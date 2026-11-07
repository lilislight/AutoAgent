"""Persistence contract tests shared by async SQLite and PostgreSQL backends."""

from __future__ import annotations

import asyncio
import gc
import json
import os
import tempfile
import unittest
import weakref
from dataclasses import replace
from pathlib import Path
from uuid import uuid4
from tests.postgres import POSTGRES_TEST_URL

try:
    from sqlalchemy import select
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    raise unittest.SkipTest(
        "Install autoagent[persistence] to run Persistence tests."
    ) from exc
from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    Await,
    ConditionContext,
    Edge,
    Node,
    Operator,
    Spawn,
    Wait,
    Workflow,
)
from autoagent.core.runtime import RuntimeEvent, SessionOpened
from autoagent.core.runtime.user_events import UserEvent
from autoagent.host.persistence import (
    CapacityError,
    ClosedError,
    ConflictError,
    DatabaseSink,
    PersistenceConfig,
    PersistenceError,
)
from autoagent.host.persistence.codec import EncodingMemo, assign, encode
from autoagent.host.persistence.schema import meta


class Value(TypedDict):
    blob: str


def identity(value: Value) -> Value:
    return value


def fail(value: Value) -> Value:
    raise ValueError("operator failed: " + value["blob"])


def never(value: ConditionContext) -> bool:
    return False


def shorten(value: Value) -> Value:
    return {"blob": value["blob"][1:]}


def again(value: ConditionContext) -> bool:
    return bool(value.output["blob"])


def finished(value: ConditionContext) -> bool:
    return not value.output["blob"]


class Capture:
    def __init__(self):
        self.events = []

    async def append(self, event):
        self.events.append(event)


class PersistenceContracts:
    async def test_source_membership_reopens_and_preserves_cross_stream_conflicts(self):
        """Startup loads persisted IDs, including collisions across different streams."""
        sink=await self.sink()
        event=RuntimeEvent('membership',1,SessionOpened({}))
        await sink.append(event)
        await sink.close()
        reopened=await DatabaseSink.open(self.url,config=PersistenceConfig())
        self.sinks.append(reopened)
        identities=reopened._writer.storage.identities
        self.assertTrue(identities.ready)
        self.assertTrue(identities.may_contain('runtime_event',event.id))
        await reopened.append(event)
        with self.assertRaises(ConflictError):
            await reopened.append(replace(event,session_id='other-stream'))
        self.assertEqual(len(await self.rows(reopened,'runtime_events')),1)

    async def test_saturated_source_filter_preserves_new_events_and_conflicts(self):
        """A full filter falls back to SQL without false rejections or lost writes."""
        sink=await self.sink(writer_metadata_cache_bytes=64)
        writer=sink._writer
        async def saturate():
            writer.storage.identities.bits[:]=b'\xff'*len(writer.storage.identities.bits)
        await writer.run(saturate)
        event=RuntimeEvent('saturated',1,SessionOpened({}))
        await sink.append(event)
        await sink.append(event)
        with self.assertRaises(ConflictError):
            await sink.append(replace(event,payload=SessionOpened({'changed':True})))
        self.assertEqual(len(await self.rows(sink,'runtime_events')),1)
        self.assertLessEqual(writer.storage.metadata_cache.budget+len(writer.storage.identities.bits),64)

    async def test_fresh_sources_and_commits_skip_redundant_position_queries(self):
        """Known-new source IDs and the single Writer position need no preflight SQL."""
        from sqlalchemy import event as sql_event
        sink=await self.sink()
        statements=[]
        def sql(conn,cursor,statement,parameters,context,executemany):
            statements.append(statement)
        engine=sink._writer.storage.engine.sync_engine
        sql_event.listen(engine,'before_cursor_execute',sql)
        try:
            for index in range(3):
                await sink.append(RuntimeEvent(f'fresh-{index}',1,SessionOpened({})))
        finally:
            sql_event.remove(engine,'before_cursor_execute',sql)
        self.assertFalse(any(q.startswith('SELECT runtime_events.')for q in statements))
        self.assertFalse(any(q.startswith('SELECT persistence_meta.last_store_order')for q in statements))
        self.assertEqual(sorted(r['store_order']for r in await self.rows(sink,'runtime_events')),[1,2,3])

    async def test_user_tail_survives_lost_commit_ack_and_maintenance(self):
        """Retry a committed User Event once and reload sequence state after maintenance."""
        from sqlalchemy.exc import OperationalError
        sink=await self.sink(retry_delay_ms=0)
        storage=sink._writer.storage;original=storage.write;lost=False
        async def lose_ack(entries):
            nonlocal lost
            result=await original(entries)
            if not lost and any(e.kind=='user_event'for e in entries):
                lost=True
                raise OperationalError(None,None,OSError('lost acknowledgment'),connection_invalidated=True)
            return result
        storage.write=lose_ack
        first=UserEvent('s','i',1,'progress',{})
        await sink.append_user_event(first)
        await sink.append_user_event(first)
        await sink.rebuild_projections()
        await sink.append_user_event(UserEvent('s','i',2,'progress',{}))
        with self.assertRaises(ConflictError):
            await sink.append_user_event(UserEvent('s','i',4,'progress',{}))
        self.assertTrue(lost)
        self.assertEqual([r['sequence']for r in await self.rows(sink,'user_events')],[1,2])

    async def test_terminal_buffered_batch_cannot_leave_stale_ordinal_on_recovery(self):
        """Rebuild ordinals after a buffered failure without Writer projection state."""
        from autoagent import Recovery, RecoverySource
        calls = 0

        def flaky(value: Value) -> Value:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ValueError('recover the second occurrence')
            return {'blob': value['blob'][1:]}

        capture = Capture()
        workflow = Workflow('ordinal-recovery', nodes=[Node('entry', identity), Node('step', flaky, recovery_mode=Recovery('replay_safe')), Node('finish', identity)],
                            edges=[Edge('entry', 'step'), Edge('step', 'step', condition=again), Edge('step', 'finish', condition=finished)])
        app = AutoAgentApp(runtime_event_sink=capture)
        self.apps.append(app)
        failed = await app.ainvoke(workflow, {'blob': 'xx'}, entry_node_id='entry')
        self.assertEqual(failed.status, 'failed')
        sink = await self.sink(mode='buffered', batch_delay_ms=60_000)
        await sink.register_workflow(app.workflow_definition_snapshot(workflow.id))
        first = True
        for event in capture.events:
            await sink.append(event)
            if first and event.event_name == 'node_occurrence.started' and event.payload.occurrence_id.startswith('step'):
                first = False
                await sink.flush()
                cached = sink._writer.storage.metadata_cache.get(('ordinals', (failed.ref.invocation_id, 'step')))
                self.assertIsNone(cached)
                projected = await sink.list_nodes(failed.ref.invocation_id)
                self.assertEqual(max(r['ordinal'] for r in projected.items if r['node_id']=='step'), 1)
        await sink.flush()
        boundary = len(capture.events)
        recovered = await app.arecover(failed.ref, source=RecoverySource.from_events(tuple(capture.events)))
        self.assertEqual(recovered.status, 'completed')
        for event in capture.events[boundary:]:
            await sink.append(event)
        await sink.flush()
        ordinals = [row['ordinal'] for row in await self.rows(sink, 'node_executions') if row['node_id'] == 'step']
        self.assertEqual(sorted(ordinals), [1, 2, 3])

    async def test_batch_source_writes_are_chunked_and_lossless(self):
        """Bulk writes cross the parameter chunk boundary without losing ordered facts."""
        from sqlalchemy import event as sql_event
        sink = await self.sink(mode='buffered', batch_count=1024, batch_delay_ms=60_000)
        statements = []

        def sql(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith('INSERT INTO runtime_events'):
                statements.append(statement)

        engine = sink._writer.storage.engine.sync_engine
        sql_event.listen(engine, 'before_cursor_execute', sql)
        try:
            for index in range(507):
                await sink.append(RuntimeEvent(str(index), 1, SessionOpened({'shared': 'x' * 1024})))
            await sink.flush()
        finally:
            sql_event.remove(engine, 'before_cursor_execute', sql)
        rows = await self.rows(sink, 'runtime_events')
        self.assertEqual(len(rows), 507)
        self.assertEqual(sorted(row['store_order'] for row in rows), list(range(1, 508)))
        self.assertEqual(len(statements), 2)
        self.assertEqual(len(await self.rows(sink, 'sessions')), 507)

    async def test_committed_metadata_is_not_published_before_commit(self):
        """Rollback clears Writer metadata without exposing an uncommitted frontier."""
        sink = await self.sink()
        writer = sink._writer
        event = RuntimeEvent('metadata-session', 1, SessionOpened({}))
        original = writer.storage.projections

        async def broken(conn, batch):
            await original(conn, batch)
            self.assertIsNone(writer.storage.metadata_cache.get(('sessions', ('metadata-session',))))
            raise RuntimeError('rollback metadata')

        writer.storage.projections = broken
        with self.assertRaises(PersistenceError):
            await sink.append(event)
        self.assertFalse(writer.storage.metadata_cache.entries)
        self.assertIsNone(await sink.get_session('metadata-session'))

    async def test_metadata_eviction_preserves_frontiers_and_rebuild(self):
        """Eviction and maintenance preserve committed rows and historical state."""
        sink = await self.sink(writer_metadata_cache_bytes=4096)
        app, workflow, _ = await self.registered(sink)
        result = await app.ainvoke(workflow, {'blob': 'cache'})
        await sink.flush()
        cache = sink._writer.storage.metadata_cache
        self.assertLessEqual(cache.bytes, 4096)
        self.assertGreater(cache.evictions, 0)
        before = await self.rows(sink, 'node_executions')
        await sink.rebuild_projections()
        self.assertFalse(cache.entries)
        self.assertEqual(await self.rows(sink, 'node_executions'), before)
        self.assertEqual(result.status, 'completed')

    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.sinks, self.apps = [], []
        self.url = await self.database_url()

    async def asyncTearDown(self):
        for app in self.apps:
            app.close()
        for sink in self.sinks:
            try:
                await sink.close()
            except PersistenceError:
                pass
        await self.remove_database()
        self.directory.cleanup()

    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(
            Path(self.directory.name) / "persistence.sqlite"
        )

    async def remove_database(self):
        pass

    async def sink(self, **config):
        result = await DatabaseSink.open(self.url, config=PersistenceConfig(**config))
        self.sinks.append(result)
        return result

    def workflow(self, *, handler=identity, branch=False, sink=None):
        operator = Operator(handler, id="echo", version="1")
        nodes = [Node("a", operator), Node("b", operator)]
        if branch:
            edges = [Edge("a", "b", condition=never)]
        else:
            edges = [Edge("a", "b")]
        workflow = Workflow("host-persistence", nodes=nodes, edges=edges)
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(workflow)
        return app, workflow, operator

    async def registered(self, sink, *, handler=identity, branch=False):
        app, workflow, operator = self.workflow(
            handler=handler, branch=branch, sink=sink
        )
        await sink.register_operator(operator)
        await sink.register_workflow(app.workflow_definition_snapshot(workflow.id))
        return app, workflow, operator

    async def rows(self, sink, name):
        async def query():
            async with sink._queries.view() as (conn, _, _, _):
                return [
                    dict(row)
                    for row in (
                        await conn.execute(select(meta.tables[name]))
                    ).mappings()
                ]

        return await sink._writer.run(query)

    async def test_real_core_roundtrip_details_and_rebuild(self):
        """Preserve real Core facts and historical details through projection rebuild."""
        sink = await self.sink()
        app, workflow, operator = await self.registered(sink)
        result = await app.ainvoke(workflow, {"blob": "hello"})
        self.assertEqual(result.status, "completed")
        invocation = await sink.get_invocation(result.ref.invocation_id)
        self.assertEqual(invocation["status"], "completed")
        nodes = (await sink.list_nodes(result.ref.invocation_id)).items
        self.assertEqual([n["status"] for n in nodes], ["completed", "completed"])
        detail = await sink.node_detail(result.ref.invocation_id, "a@root")
        self.assertEqual(detail["input"]["value"], {"blob": "hello"})
        self.assertEqual(detail["output"]["value"], {"blob": "hello"})
        self.assertEqual(detail["definition"]["id"], "a")
        call = (await sink.list_calls(result.ref.invocation_id)).items[0]
        cd = await sink.call_detail(result.ref.invocation_id, call["call_id"])
        self.assertEqual(cd["definition"]["operator_id"], operator.id)
        self.assertGreaterEqual(cd["execution_duration_ns"], 0)
        before = (await sink.graph_changes(result.ref.invocation_id)).items
        self.assertTrue(
            any(
                r["entity_type"] == "edge" and r["change"].get("selected")
                for r in before
            )
        )
        await sink.rebuild_projections()
        self.assertEqual(
            (await sink.graph_changes(result.ref.invocation_id)).items, before
        )
        self.assertEqual(
            (await sink.node_detail(result.ref.invocation_id, "a@root"))["output"],
            detail["output"],
        )
        self.assertEqual(
            sum(row["count"] for row in await sink.statistics("operator")), 2
        )

    async def test_new_event_facts_append_without_reading_and_survive_lost_ack(self):
        """New facts skip existence reads and remain unique after committed retries."""
        from sqlalchemy import event
        from sqlalchemy.exc import OperationalError

        sink = await self.sink(retry_delay_ms=0)
        workflow = Workflow('append-facts', nodes=[Node('a', identity), Node('b', identity)],
                            edges=[Edge('a', 'b', condition=again)])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(workflow)
        await sink.register_workflow(app.workflow_definition_snapshot(workflow.id))
        storage = sink._writer.storage
        original = storage.write
        lost = False
        statements = []

        async def lose_ack(entries):
            nonlocal lost
            order = await original(entries)
            if not lost and any(e.light.get('event_name') == 'routing.resolved' for e in entries):
                lost = True
                raise OperationalError(None, None, OSError('lost commit acknowledgment'),
                                       connection_invalidated=True)
            return order

        def inspect_sql(conn, cursor, statement, parameters, context, executemany):
            if any(name in statement for name in ('graph_changes', 'edge_evaluations')):
                statements.append(statement)

        storage.write = lose_ack
        engine = storage.engine.sync_engine
        event.listen(engine, 'before_cursor_execute', inspect_sql)
        try:
            result = await app.ainvoke(workflow, {'blob': 'facts'})
            await sink.flush()
        finally:
            event.remove(engine, 'before_cursor_execute', inspect_sql)
            storage.write = original
        self.assertEqual(result.status, 'completed')
        self.assertTrue(lost)
        self.assertFalse(any(q.startswith('SELECT graph_changes.') for q in statements))
        self.assertFalse(any(q.startswith('SELECT edge_evaluations.') and 'JOIN' not in q for q in statements))
        inserts = [q for q in statements if q.startswith(('INSERT INTO graph_changes', 'INSERT INTO edge_evaluations'))]
        self.assertFalse(inserts)
        self.assertTrue(all('ON CONFLICT' not in q for q in inserts))
        before = {name: sorted(await self.rows(sink, name), key=lambda r: (r['event_id'], r.get('change_index', r.get('result_index'))))
                  for name in ('graph_changes', 'edge_evaluations')}
        routing = next(r for r in await self.rows(sink, 'runtime_events') if r['event_name'] == 'routing.resolved')
        await sink.append(await sink.get_runtime_event(routing['event_id']))
        await sink.rebuild_projections()
        for name, expected in before.items():
            actual = sorted(await self.rows(sink, name), key=lambda r: (r['event_id'], r.get('change_index', r.get('result_index'))))
            self.assertEqual(actual, expected)

    async def test_parent_child_projection_writes_each_node_once_per_batch(self):
        """Child ownership persists without writing any historical Node projection."""
        sink = await self.sink(mode='buffered', batch_delay_ms=1000)
        child = Workflow('append-child', nodes=[Node('work', identity)])
        root = Workflow('append-root', nodes=[Node('spawn', Spawn(child, 'work')), Node('await', Await())],
                        edges=[Edge('spawn', 'await')])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(root)
        for workflow in (child, root):
            await sink.register_workflow(app.workflow_definition_snapshot(workflow.id))
        storage = sink._writer.storage
        original_project, original_upsert = storage.projections, storage.upsert
        seen = set()
        writes = 0

        async def project(conn, batch):
            seen.clear()
            return await original_project(conn, batch)

        async def upsert(conn, name, rows):
            nonlocal writes
            if name == 'node_executions':
                for row in rows:
                    key = (row['invocation_id'], row['occurrence_id'])
                    self.assertNotIn(key, seen)
                    seen.add(key)
                    writes += 1
            return await original_upsert(conn, name, rows)

        storage.projections, storage.upsert = project, upsert
        try:
            result = await app.ainvoke(root, {'blob': 'child'})
            await sink.flush()
        finally:
            storage.projections, storage.upsert = original_project, original_upsert
        self.assertEqual(result.status, 'completed')
        self.assertEqual(writes, 0)
        rows = await self.rows(sink, 'invocations')
        self.assertEqual(len(rows), 2)
        self.assertTrue(any(row['parent_invocation_id'] for row in rows))

    async def test_writer_graph_cache_skips_unconditional_reads_and_survives_eviction(self):
        """Execution skips display graph reads; demand queries use a bounded graph cache."""
        from sqlalchemy import event
        sink = await self.sink(workflow_graph_cache_bytes=1500)
        storage = sink._writer.storage
        queries = []

        def sql(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith('SELECT'):
                queries.append(statement)

        event.listen(storage.engine.sync_engine, 'before_cursor_execute', sql)
        results = []
        try:
            app = AutoAgentApp(runtime_event_sink=sink)
            self.apps.append(app)
            for i in range(3):
                workflow = Workflow(f'cached-{i}', nodes=[Node('a', identity), Node('b', identity)],
                                    edges=[Edge('a', 'b')])
                app.register_workflow(workflow)
                await sink.register_workflow(app.workflow_definition_snapshot(workflow.id))
                for _ in range(2):
                    result = await app.ainvoke(workflow, {'blob': 'metadata'})
                    self.assertEqual(result.output, {'blob': 'metadata'})
                    results.append(result)
        finally:
            event.remove(storage.engine.sync_engine, 'before_cursor_execute', sql)
        self.assertFalse(any('FROM edge_evaluations' in q for q in queries))
        # One full definition read per revision; admission uses only its PK.
        self.assertEqual(sum('workflow_revisions.snapshot_text' in q for q in queries), 0)
        for result in results:
            await sink.graph_changes(result.ref.invocation_id)
        self.assertGreater(storage.graph_cache.hits, 0)
        self.assertGreaterEqual(storage.graph_cache.evictions, 2)
        self.assertLessEqual(storage.graph_cache.bytes, 1500)
        before = (await sink.graph_changes(results[-1].ref.invocation_id)).items
        await sink.rebuild_projections()
        self.assertEqual(storage.graph_cache.entries, {})
        self.assertEqual((await sink.graph_changes(results[-1].ref.invocation_id)).items, before)
        await sink.close()
        self.assertEqual(storage.graph_cache.bytes, 0)

    async def test_graph_cache_clears_after_rollback_and_definition_maintenance(self):
        """Do not retain graph metadata across rollback, deletion or re-registration."""
        sink = await self.sink()
        app, workflow, _ = self.workflow()
        snapshot = app.workflow_definition_snapshot(workflow.id)
        await sink.register_workflow(snapshot)
        revision = snapshot.workflow_revision_id
        storage = sink._writer.storage
        original = storage.write_transaction

        async def broken(conn, entries):
            await storage.workflow_graph(conn, revision)
            self.assertTrue(storage.graph_cache.entries)
            raise RuntimeError('rollback graph load')

        async def rollback():
            storage.write_transaction = broken
            try:
                with self.assertRaisesRegex(RuntimeError, 'rollback graph load'):
                    await storage.write([])
            finally:
                storage.write_transaction = original
            self.assertFalse(storage.graph_cache.entries)
            async with storage.engine.connect() as conn:
                await storage.workflow_graph(conn, revision)

        await sink._writer.run(rollback)
        await sink.delete_workflow(revision)
        self.assertEqual(storage.graph_cache.bytes, 0)
        await sink.register_workflow(snapshot)
        self.assertIsNotNone(await sink.get_workflow(revision))

    async def test_projection_preserves_missing_conditions_other_status_and_skipped_edges(self):
        """Never invent conditional decisions or omit unselected and skipped routes."""
        from autoagent.host.persistence.projections import ProjectionBatch
        sink = await self.sink()
        snapshot = {'definition': {'edges': [
            dict(id=f'{status}-{kind}', source='source', target='target', on=status,
                 condition={} if kind == 'conditional' else None)
            for status in ('complete', 'error') for kind in ('plain', 'conditional')
        ] + [dict(id='unrelated', source='other', target='target', on='complete', condition=None)]}}

        async def check():
            async with sink._queries.view() as (conn, _, _, _):
                async def load(*args):
                    return snapshot
                for status, decision in [('completed', None), ('completed', False),
                                         ('completed', True), ('failed', True), ('skipped', None)]:
                    batch = ProjectionBatch(conn, load)
                    batch.rows['sessions'][('s',)] = {'session_id': 's'}
                    batch.rows['invocations'][('i',)] = {'invocation_id': 'i', 'workflow_revision_id': 'r'}
                    payload = dict(occurrence_id='source@root', output=None, metrics=None,
                                   error={'type': 'Failure', 'code': 'TEST'})
                    record = dict(id=uuid4().hex, session_id='s', invocation_id='i', sequence=1,
                                  occurred_at_us=1, event_name='node_occurrence.failed' if status == 'failed'
                                  else 'node_occurrence.completed', payload=payload)
                    node = dict(node_id='source', scope='root', status=status)
                    if status == 'skipped':
                        record['event_name'] = 'scheduler.updated'
                        record['delta'] = {'operations': [dict(op='set',
                            path=['invocation','scheduler','occurrences','source@root'], value=node)]}
                    else:
                        batch.rows['node_executions'][('i', 'source@root')] = node
                    source_status = 'error' if status == 'failed' else 'complete'
                    if decision is not None:
                        batch.rows['edge_evaluations'][('evaluation', 0)] = dict(
                            invocation_id='i', source_occurrence_id='source@root',
                            source_status=source_status, edge_id=source_status+'-conditional',
                            condition_result=decision)
                    await batch.runtime(record, 1)
                    actual = {r['change']['edge_id']: r['change']['selected']
                              for r in batch.changed('graph_changes') if r['entity_type'] == 'edge'}
                    expected = {edge['id']: False for edge in snapshot['definition']['edges']
                                if edge['source'] == 'source'}
                    if status != 'skipped':
                        expected[source_status+'-plain'] = True
                        if decision is None:
                            del expected[source_status+'-conditional']
                        else:
                            expected[source_status+'-conditional'] = decision
                    self.assertEqual(actual, expected, (status, decision))
        await sink._writer.run(check)

    async def test_fact_schema_creates_empty_optional_read_models(self):
        """Database initialization creates empty detailed projection tables."""
        from sqlalchemy import inspect
        from autoagent.host.persistence.schema import DETAIL_TABLES
        sink = await self.sink()
        async def tables():
            async with sink._writer.storage.engine.connect() as conn:
                return await conn.run_sync(lambda sync: inspect(sync).get_table_names())
        names = await sink._writer.run(tables)
        self.assertTrue(set(DETAIL_TABLES).issubset(names))
        self.assertTrue({'sessions','invocations','active_nodes','active_waits'}.issubset(names))

    async def test_artifacts_deduplicate_event_delta_and_checkpoint(self):
        """Store a shared MiB value once across original facts and checkpoints."""
        sink = await self.sink()
        app, workflow, _ = await self.registered(sink)
        value = {"blob": "x" * 1048576}
        result = await app.ainvoke(workflow, value)
        checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
        key = await sink.save_checkpoint(checkpoint)
        self.assertEqual(
            (await sink.get_checkpoint(key)).to_record(), checkpoint.to_record()
        )
        artifacts = await self.rows(sink, "artifacts")
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0]["value_type"], "string")
        nodes = await self.rows(sink, "node_executions")
        self.assertNotIn("x" * 100, json.dumps(nodes))
        facts = (await sink.runtime_events(result.ref.session_id)).items
        for record in facts:
            self.assertEqual(
                (await sink.get_runtime_event(record["id"])).to_record(), record
            )
        self.assertEqual(
            (await sink.node_detail(result.ref.invocation_id, "a@root"))["output"][
                "value"
            ],
            value,
        )

    async def test_subthreshold_business_values_remain_inline(self):
        """Do not externalize structural Event wrappers containing small values."""
        sink = await self.sink()
        app, workflow, _ = await self.registered(sink)
        await app.ainvoke(workflow, {"blob": "x" * 4096})
        self.assertEqual(await self.rows(sink, "artifacts"), [])

    async def test_idempotency_and_conflicting_event_identity(self):
        """Accept retries once and reject changed content or a sequence gap."""
        sink = await self.sink()
        event = RuntimeEvent("session", 1, SessionOpened({}))
        await sink.append(event)
        await sink.append(event)
        self.assertEqual(len((await sink.runtime_events("session")).items), 1)
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, payload=SessionOpened({"changed": True})))
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, id=str(uuid4())))
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, sequence=3, id=str(uuid4())))
        self.assertEqual((await sink.health()).state, "open")

    async def test_operator_binding_survives_registry_update_and_retry(self):
        """Pin historical Call bindings even when the current Operator changes."""
        sink = await self.sink()
        app, workflow, operator = await self.registered(sink)
        result = await app.ainvoke(workflow, {"blob": "old"})
        before = (await sink.list_calls(result.ref.invocation_id)).items[0][
            "operator_definition_id"
        ]
        updated = await sink.register_operator(
            Operator(identity, id=operator.id, version="2")
        )
        self.assertNotEqual(updated, before)
        records = (await sink.runtime_events(result.ref.session_id)).items
        started = next(r for r in records if r["event_name"] == "operator_call.started")
        await sink.append(RuntimeEvent.from_record(started))
        await sink.rebuild_projections()
        self.assertEqual(
            (await sink.list_calls(result.ref.invocation_id)).items[0][
                "operator_definition_id"
            ],
            before,
        )
        with self.assertRaises(ConflictError):
            await sink.append_runtime_event(
                RuntimeEvent.from_record(started), operator_definition_id=updated
            )

    async def test_buffered_ack_queue_capacity_and_memory_release(self):
        """ACK memory acceptance, block at capacity, and release committed objects."""
        sink = await self.sink(mode="buffered", queue_count=2, batch_delay_ms=0)
        original = sink._writer.storage.write

        async def install():
            gate = asyncio.Event()
            entered = asyncio.Event()

            async def delayed(entries):
                entered.set()
                await gate.wait()
                return await original(entries)

            sink._writer.storage.write = delayed
            return gate, entered

        gate, entered = await sink._writer.run(install)
        a = RuntimeEvent("a", 1, SessionOpened({"blob": "shared"}))
        b = RuntimeEvent("b", 1, SessionOpened({}))
        c = RuntimeEvent("c", 1, SessionOpened({}))
        await sink.append(a)

        async def wait_entered():
            await entered.wait()

        await sink._writer.run(wait_entered)
        await asyncio.wait_for(sink.append(b), 2)
        task = asyncio.create_task(sink.append(c))
        await asyncio.sleep(0.05)
        self.assertFalse(task.done())
        health = await sink.health()
        self.assertEqual(health.queued_count, 2)
        self.assertGreater(health.retained_bytes, 0)

        async def release():
            gate.set()

        await sink._writer.run(release)
        await asyncio.wait_for(task, 2)
        await sink.flush()
        health = await sink.health()
        self.assertEqual((health.queued_count, health.retained_bytes), (0, 0))
        self.assertEqual(health.accepted_frontier, health.durable_frontier)

    async def test_durable_does_not_ack_before_commit(self):
        """A durable append waits for its actual transaction completion."""
        sink = await self.sink(batch_delay_ms=0)
        original = sink._writer.storage.write

        async def install():
            gate = asyncio.Event()
            entered = asyncio.Event()

            async def delayed(entries):
                entered.set()
                await gate.wait()
                return await original(entries)

            sink._writer.storage.write = delayed
            return gate, entered

        gate, entered = await sink._writer.run(install)
        task = asyncio.create_task(sink.append(RuntimeEvent("s", 1, SessionOpened({}))))

        async def wait():
            await entered.wait()

        await sink._writer.run(wait)
        self.assertFalse(task.done())

        async def release():
            gate.set()

        await sink._writer.run(release)
        await task
        self.assertEqual(len((await sink.runtime_events("s")).items), 1)

    async def test_inflight_indexes_preserve_duplicates_tail_and_cancellation(self):
        """Keep in-flight identities and newer stream tails until their own ACKs."""
        sink = await self.sink(queue_count=2, batch_count=1)
        writer = sink._writer
        original = writer.storage.write

        async def scenario():
            gate, entered = asyncio.Event(), asyncio.Event()
            next_gate, next_entered = asyncio.Event(), asyncio.Event()

            async def delayed(entries):
                entered.set()
                await gate.wait()
                if entries[0].identity == second.id:
                    next_entered.set()
                    await next_gate.wait()
                return await original(entries)

            writer.storage.write = delayed
            first = UserEvent("s", "i", 1, "progress", {"value": 1})
            second = UserEvent("s", "i", 2, "progress", {"value": 2})
            a = asyncio.create_task(writer.accept("user_event", first.id, first))
            await asyncio.wait_for(entered.wait(), 2)
            b = asyncio.create_task(writer.accept("user_event", second.id, second))
            while writer.pending_count != 2:
                await asyncio.sleep(0)
            duplicate = asyncio.create_task(
                writer.accept("user_event", first.id, replace(first))
            )
            third = UserEvent("s", "i", 3, "progress", {})
            blocked = asyncio.create_task(writer.accept("user_event", third.id, third))
            try:
                # Wait until the producer has finished SQL/codec validation and
                # is actually blocked at the queue budget.
                async def at_capacity():
                    while not writer.condition._waiters:
                        await asyncio.sleep(0)

                await asyncio.wait_for(at_capacity(), 2)
                duplicate.cancel()
                blocked.cancel()
                await asyncio.gather(duplicate, blocked, return_exceptions=True)
                self.assertFalse(a.done())
                self.assertEqual(len(writer.pending_by_identity), 2)
                self.assertIs(writer.pending_tails[("user_event", "i")].value, second)
                self.assertNotIn(("user_event", third.id), writer.pending_by_identity)
                gate.set()
                await asyncio.wait_for(next_entered.wait(), 2)
                self.assertEqual(len(writer.pending_by_identity), 1)
                self.assertIs(writer.pending_tails[("user_event", "i")].value, second)
            finally:
                gate.set()
                next_gate.set()
                await asyncio.gather(a, b)
                await writer.flush()
            self.assertEqual(writer.pending_by_identity, {})
            self.assertEqual(writer.pending_tails, {})
            self.assertEqual(writer.objects, {})

        await asyncio.wait_for(writer.run(scenario), 10)
        self.assertEqual([e["sequence"] for e in (await sink.user_events("i")).items], [1, 2])
        # Independent producers still share the same serialized admission order.
        await asyncio.gather(*(
            sink.append(RuntimeEvent(f"producer-{i}", 1, SessionOpened({})))
            for i in range(6)
        ))
        await sink.flush()

    async def test_admission_sql_does_not_block_commit_ack(self):
        """Allow the worker to ACK and release capacity during a slow admission read."""
        sink = await self.sink(mode="buffered", queue_count=1, batch_delay_ms=0)
        writer = sink._writer
        original_write, original_source = writer.storage.write, writer.storage.source_header

        async def scenario():
            write_gate, writing = asyncio.Event(), asyncio.Event()
            read_gate, reading = asyncio.Event(), asyncio.Event()
            first = UserEvent("s", "i", 1, "progress", {})
            second = UserEvent("s", "i", 2, "progress", {})

            async def delayed_write(entries):
                writing.set()
                await write_gate.wait()
                return await original_write(entries)

            async def delayed_read(conn, kind, identity):
                result = await original_source(conn, kind, identity)
                if identity == second.id:
                    reading.set()
                    await read_gate.wait()
                return result

            writer.storage.write = delayed_write
            writer.storage.source_header = delayed_read
            await writer.accept("user_event", first.id, first)
            await asyncio.wait_for(writing.wait(), 2)
            second_call = asyncio.create_task(
                writer.accept("user_event", second.id, second)
            )
            try:
                await asyncio.wait_for(reading.wait(), 2)
                completion = writer.pending_by_identity[("user_event", first.id)].done
                write_gate.set()
                await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(completion)), 2)
                self.assertFalse(second_call.done())
                self.assertEqual(writer.pending_count, 0)
                self.assertEqual(writer.pending_tails, {})
            finally:
                read_gate.set()
                write_gate.set()
                await second_call
            await writer.flush()
            self.assertEqual(writer.pending_by_identity, {})
            self.assertEqual(writer.pending_tails, {})

        await asyncio.wait_for(writer.run(scenario), 10)
        self.assertEqual([e["sequence"] for e in (await sink.user_events("i")).items], [1, 2])

    async def test_cancelled_durable_waiter_cannot_cancel_the_write(self):
        """Cancellation after acceptance leaves the real writer running."""
        sink = await self.sink(durable_batch_delay_ms=200)
        event = RuntimeEvent("s", 1, SessionOpened({}))
        task = asyncio.create_task(sink.append(event))
        for _ in range(30):
            if (await sink.health()).queued_count:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await sink.flush()
        self.assertEqual(
            (await sink.get_runtime_event(event.id)).to_record(), event.to_record()
        )

    async def test_buffered_queue_and_byte_thresholds_bypass_large_window(self):
        """Queue utilization and batch bytes commit promptly despite a long buffer window."""
        for options in ({'queue_count': 10, 'batch_queue_ratio': .2}, {'batch_bytes': 1024}):
            with self.subTest(options=options):
                sink = await self.sink(mode='buffered', batch_delay_ms=5000, **options)
                prefix = uuid4().hex
                first = RuntimeEvent(prefix + '-a', 1, SessionOpened({'blob': 'x' * 4096}))
                await sink.append(first)
                await sink.append(RuntimeEvent(prefix + '-b', 1, SessionOpened({})))
                async def committed():
                    while await sink.get_runtime_event(first.id) is None:
                        await asyncio.sleep(.01)
                await asyncio.wait_for(committed(), 2)
                # A small remainder may wait for the next normal batch window.
                await sink.flush()
                self.assertEqual((await sink.health()).retained_bytes, 0)
                await sink.close()

    async def test_buffer_window_does_not_delay_durable_append(self):
        """Durable writes use their own window rather than the buffered delay."""
        sink = await self.sink(batch_delay_ms=5000, durable_batch_delay_ms=0)
        await asyncio.wait_for(sink.append(RuntimeEvent('durable', 1, SessionOpened({}))), 2)
        self.assertEqual((await sink.health()).queued_count, 0)

    async def test_oversized_record_rejected_before_ack(self):
        """Reject a record exceeding the explicit retained-data budget."""
        sink = await self.sink(mode="buffered", queue_bytes=4096)
        with self.assertRaises(CapacityError):
            await sink.append(RuntimeEvent("s", 1, SessionOpened({"blob": "x" * 8192})))
        self.assertEqual((await sink.health()).accepted_frontier, 0)

    async def test_failed_batch_stops_tail_and_close_reports_failure(self):
        """Do not persist later records past a failed buffered batch."""
        sink = await self.sink(mode="buffered", batch_delay_ms=100, batch_count=1)

        async def install():
            async def broken(entries):
                raise ValueError("injected permanent failure")

            sink._writer.storage.write = broken

        await sink._writer.run(install)
        await sink.append(RuntimeEvent("a", 1, SessionOpened({})))
        with self.assertRaises(PersistenceError):
            await sink.flush()
        with self.assertRaises(PersistenceError):
            await sink.append(RuntimeEvent("b", 1, SessionOpened({})))
        self.assertEqual((await sink.health()).state, "faulted")
        async def retained_indexes():
            self.assertEqual(len(sink._writer.pending_by_identity), 1)
            self.assertEqual(len(sink._writer.pending_tails), 1)
        await sink._writer.run(retained_indexes)
        self.assertEqual((await sink.runtime_events("a")).items, ())
        with self.assertRaises(PersistenceError):
            await sink.close()
        self.assertFalse(sink._writer.thread.is_alive())
        self.assertEqual(sink._writer.pending_by_identity, {})
        self.assertEqual(sink._writer.pending_tails, {})

    async def test_checkpoint_cannot_advance_beyond_event_frontier(self):
        """Reject a normal checkpoint whose Event prefix was never persisted."""
        sink = await self.sink()
        app, wf, _ = self.workflow()
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {"blob": "missing-history"})
        checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
        with self.assertRaises(PersistenceError):
            await sink.save_checkpoint(checkpoint)
        self.assertEqual(await self.rows(sink, "checkpoints"), [])

    async def test_import_anchor_rebuild_and_retention_guard(self):
        """Import a complete checkpoint without fabricating historical Call data."""
        sink = await self.sink()
        app, wf, _ = self.workflow()
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {"blob": "checkpoint-only"})
        checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
        key = await sink.import_checkpoint(checkpoint)
        sessions = (await sink.list_sessions()).items
        self.assertEqual(
            sessions[0]["history_start_sequence"], checkpoint.sessions[0].sequence + 1
        )
        self.assertEqual((await sink.runtime_events(result.ref.session_id)).items, ())
        inv = await sink.get_invocation(result.ref.invocation_id)
        self.assertEqual(
            (await sink.resolve(inv["output_ref"]))["value"],
            {"blob": "checkpoint-only"},
        )
        await sink.rebuild_projections()
        self.assertEqual(
            (await sink.get_invocation(result.ref.invocation_id))["status"], "completed"
        )
        with self.assertRaises(ConflictError):
            await sink.delete_checkpoint(key)

    async def test_ordered_graph_pages_never_split_one_event(self):
        """Graph cursors return whole Event groups in deterministic sequence order."""
        sink = await self.sink()
        app, workflow, _ = await self.registered(sink)
        result = await app.ainvoke(workflow, {"blob": "pagination"})
        all_rows = (await sink.graph_changes(result.ref.invocation_id)).items
        rows, after = [], 0
        while True:
            page = await sink.graph_changes(
                result.ref.invocation_id, after_sequence=after, limit=1
            )
            rows.extend(page.items)
            if page.next_cursor is None:
                break
            after = page.next_cursor[0]
        self.assertEqual(tuple(rows), all_rows)

    async def test_user_events_are_independent_and_ordered(self):
        """User Events preserve a separate sequence and idempotency boundary."""
        sink = await self.sink()
        event = UserEvent(
            "s", "i", 1, "progress", {"value": None, "blob": "x" * 1048576}
        )
        await sink.append_user_event(event)
        await sink.append_user_event(event)
        self.assertEqual(
            (await sink.get_user_event(event.id)).to_record(), event.to_record()
        )
        self.assertEqual(len((await sink.user_events("i")).items), 1)
        self.assertEqual((await sink.runtime_events("s")).items, ())
        with self.assertRaises(ConflictError):
            await sink.append_user_event(replace(event, sequence=3, id=str(uuid4())))
        self.assertEqual(
            await sink.resolve(
                {"kind": "user_event", "id": event.id, "path": ["payload", "value"]}
            ),
            {"available": True, "value": None},
        )
        self.assertEqual(await sink.resolve(None), {"available": False, "value": None})

    async def test_failed_operator_retains_error_and_node_classification(self):
        """Persist failure details without duplicating large errors in projections."""
        sink = await self.sink()
        app, wf, _ = await self.registered(sink, handler=fail)
        result = await app.ainvoke(wf, {"blob": "failure"})
        self.assertEqual(result.status, "failed")
        node = await sink.node_detail(result.ref.invocation_id, "a@root")
        self.assertEqual(node["error_type"], "ValueError")
        self.assertIn("operator failed", node["error"]["value"]["message"])
        self.assertTrue(
            any(r["status"] == "failed" for r in await sink.statistics("operator"))
        )

    async def test_purge_removes_facts_projections_and_unreachable_artifacts(self):
        """Delete a stopped whole Graph atomically and GC its unreferenced data."""
        sink = await self.sink()
        app, wf, _operator = await self.registered(sink)
        result = await app.ainvoke(wf, {"blob": "x" * 1048576})
        cp = app.unload_session(result.ref, capture_checkpoint=True)
        await sink.save_checkpoint(cp)
        with self.assertRaises(ConflictError):
            await sink.delete_workflow(result.ref.workflow_revision_id)
        definition = (await sink.list_calls(result.ref.invocation_id)).items[0][
            "operator_definition_id"
        ]
        with self.assertRaises(ConflictError):
            await sink.delete_operator(definition)
        await sink.purge_root_graph(result.ref.invocation_id)
        self.assertIsNone(await sink.get_invocation(result.ref.invocation_id))
        self.assertEqual((await sink.runtime_events(result.ref.session_id)).items, ())
        self.assertEqual(await self.rows(sink, "artifacts"), [])
        await sink.delete_workflow(result.ref.workflow_revision_id)
        await sink.delete_operator(definition)

    async def test_single_writer_and_reopen_preserve_identity(self):
        """Exclude another writer and preserve durable facts after reopening."""
        sink = await self.sink()
        with self.assertRaises(ConflictError):
            await DatabaseSink.open(self.url)
        event = RuntimeEvent("s", 1, SessionOpened({}))
        await sink.append(event)
        await sink.close()
        reopened = await self.sink()
        await reopened.append(event)
        self.assertEqual(len((await reopened.runtime_events("s")).items), 1)
        with self.assertRaises(ClosedError):
            await sink.append(event)

    async def test_live_core_can_call_sink_from_different_event_loops(self):
        """Support Core's synchronous adapter without crossing SQLAlchemy loops."""
        sink = await self.sink()
        app, wf, _ = await self.registered(sink)
        result = await asyncio.to_thread(app.invoke, wf, {"blob": "thread"})
        self.assertEqual(result.status, "completed")
        self.assertEqual(
            (await sink.get_invocation(result.ref.invocation_id))["status"], "completed"
        )

    async def test_wait_checkpoint_reload_continues_same_identity(self):
        """Resume a persisted Wait after Core reload without replacing its Call."""
        sink = await self.sink()
        wf = Workflow("persist-wait", nodes=[Node("wait", Wait(Value, Value))])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        waiting = await app.ainvoke(wf, {"blob": "request"})
        self.assertEqual(waiting.status, "waiting")
        before = (await sink.list_calls(waiting.ref.invocation_id)).items
        cp = app.unload_session(waiting.ref, capture_checkpoint=True)
        key = await sink.save_checkpoint(cp)
        await app.aload_checkpoint(await sink.get_checkpoint(key))
        await app.arecover(waiting.ref)
        final = await app.aresume(
            waiting.ref, waiting.waits[0].id, {"blob": "response"}
        )
        self.assertEqual(final.status, "completed")
        wait = await sink.wait_detail(waiting.ref.invocation_id, waiting.waits[0].id)
        self.assertEqual(wait["status"], "resumed")
        self.assertEqual(wait["request"]["value"], {"blob": "request"})
        self.assertEqual(wait["response"]["value"], {"blob": "response"})
        after = (await sink.list_calls(waiting.ref.invocation_id)).items
        self.assertEqual([c["call_id"] for c in before], [c["call_id"] for c in after])

    async def test_child_graph_ownership_checkpoint_rebuild_and_purge(self):
        """Persist real Parent-Child plans and compacted results as one Graph."""
        sink = await self.sink(mode="buffered")
        child = Workflow("persist-child", nodes=[Node("work", identity)])
        root = Workflow(
            "persist-root",
            nodes=[Node("spawn", Spawn(child, "work")), Node("await", Await())],
            edges=[Edge("spawn", "await")],
        )
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(root)
        for wf in (child, root):
            await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(root, {"blob": "child"})
        self.assertEqual(result.status, "completed")
        await sink.flush()
        invs = (
            await sink.list_invocations(
                root_invocation_id=result.ref.invocation_id, include_planned=True
            )
        ).items
        self.assertEqual(len(invs), 2)
        child_row = next(r for r in invs if r["parent_invocation_id"])
        self.assertEqual(child_row["parent_occurrence_id"], "spawn@root")
        self.assertEqual(child_row["ownership_phase"], "terminal")
        changes = (
            await sink.graph_changes(result.ref.invocation_id, root_graph=True)
        ).items
        self.assertEqual(
            {r["invocation_id"] for r in changes}, {r["invocation_id"] for r in invs}
        )
        cp = app.unload_session(result.ref, capture_checkpoint=True)
        key = await sink.save_checkpoint(cp)
        self.assertEqual((await sink.get_checkpoint(key)).to_record(), cp.to_record())
        await sink.rebuild_projections()
        self.assertEqual(
            (
                await sink.list_invocations(
                    root_invocation_id=result.ref.invocation_id, include_planned=True
                )
            ).items,
            invs,
        )
        await sink.purge_root_graph(result.ref.invocation_id)
        self.assertEqual((await sink.list_sessions()).items, ())

    async def test_loop_occurrences_and_conditions_remain_distinct(self):
        """Separate repeated Node occurrences and actual condition evaluations."""
        sink = await self.sink()
        wf = Workflow(
            "persist-loop",
            nodes=[
                Node("entry", identity),
                Node("step", shorten),
                Node("exit", identity),
            ],
            edges=[
                Edge("entry", "step"),
                Edge("step", "step", again, id="back"),
                Edge("step", "exit", finished, id="exit"),
            ],
        )
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {"blob": "abcd"})
        self.assertEqual(result.status, "completed")
        steps = [
            n
            for n in (await sink.list_nodes(result.ref.invocation_id)).items
            if n["node_id"] == "step"
        ]
        self.assertEqual(len(steps), 4)
        self.assertEqual(len({n["occurrence_id"] for n in steps}), 4)
        edge = await sink.edge_detail(result.ref.invocation_id, "back")
        self.assertEqual(len(edge["evaluations"]), 4)
        self.assertTrue(
            any(r["change"].get("selected") is False for r in edge["changes"])
        )
        await sink.rebuild_projections()
        self.assertEqual(
            (await sink.edge_detail(result.ref.invocation_id, "back"))["evaluations"],
            edge["evaluations"],
        )

    async def test_false_condition_is_a_committed_unselected_route(self):
        """Keep condition results separate from final routes and skipped Nodes."""
        sink = await self.sink()
        app, wf, _ = await self.registered(sink, branch=True)
        result = await app.ainvoke(wf, {"blob": "branch"})
        nodes = (await sink.list_nodes(result.ref.invocation_id)).items
        self.assertEqual(
            next(n for n in nodes if n["node_id"] == "b")["status"], "skipped"
        )
        edges = (await sink.get_workflow(result.ref.workflow_revision_id))[
            "definition"
        ]["edges"]
        detail = await sink.edge_detail(result.ref.invocation_id, edges[0]["id"])
        self.assertFalse(detail["evaluations"][0]["condition_result"])
        self.assertTrue(
            any(r["change"].get("selected") is False for r in detail["changes"])
        )

    async def test_accepted_view_reads_pending_graph_and_releases_tail(self):
        """Expose a fixed accepted frontier before commit and merge it once."""
        sink = await self.sink(mode="buffered", batch_delay_ms=5000, batch_count=1000)
        app, wf, _ = await self.registered(sink)
        result = await app.ainvoke(wf, {"blob": "tail"})
        self.assertIsNone(await sink.get_invocation(result.ref.invocation_id))
        inv = await sink.get_invocation(result.ref.invocation_id, view="accepted")
        self.assertEqual(inv["status"], "completed")
        detail = await sink.node_detail(
            result.ref.invocation_id, "a@root", view="accepted"
        )
        self.assertEqual(detail["output"]["value"], {"blob": "tail"})
        before = (
            await sink.graph_changes(result.ref.invocation_id, view="accepted")
        ).items
        await sink.flush()
        self.assertEqual(
            (await sink.graph_changes(result.ref.invocation_id)).items, before
        )
        self.assertEqual((await sink.health()).retained_bytes, 0)

    async def test_shared_artifact_gc_retains_other_graph_and_nested_dependencies(self):
        """Collect one Root without deleting a shared or indirectly used Artifact."""
        sink = await self.sink(artifact_threshold_bytes=1024)
        app, wf, _ = await self.registered(sink)
        value = {"blob": "x" * 3000}
        a = await app.ainvoke(wf, value)
        b = await app.ainvoke(wf, value)
        app.unload_session(a.ref)
        app.unload_session(b.ref)
        await sink.purge_root_graph(a.ref.invocation_id)
        self.assertTrue(await self.rows(sink, "artifacts"))
        self.assertEqual(
            (await sink.node_detail(b.ref.invocation_id, "a@root"))["output"]["value"],
            value,
        )
        user = UserEvent(
            "user-s",
            "user-i",
            1,
            "data",
            {
                "rows": [
                    {"small": "a" * 80, "big": "y" * 3000 if i == 0 else ""}
                    for i in range(40)
                ]
            },
        )
        await sink.append_user_event(user)
        self.assertTrue(await self.rows(sink, "artifact_dependencies"))
        await sink.gc_artifacts()
        self.assertEqual(
            (await sink.get_user_event(user.id)).to_record(), user.to_record()
        )

    async def test_byte_capacity_waits_and_shared_objects_are_counted_once(self):
        """Apply byte backpressure while retaining shared values only once."""
        sink = await self.sink(mode="buffered", queue_bytes=16000, batch_delay_ms=5000)
        shared = {"blob": "x" * 9000}
        a = RuntimeEvent("a", 1, SessionOpened(shared))
        b = RuntimeEvent("b", 1, SessionOpened(shared))
        await sink.append(a)
        first = (await sink.health()).retained_bytes
        await sink.append(b)
        second = (await sink.health()).retained_bytes
        self.assertLess(second - first, 4000)
        task = asyncio.create_task(
            sink.append(RuntimeEvent("c", 1, SessionOpened({"blob": "y" * 9000})))
        )
        await asyncio.sleep(0.03)
        self.assertFalse(task.done())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await sink.flush()
        self.assertEqual((await sink.runtime_events("c")).items, ())

    async def test_idempotency_preserves_json_number_and_boolean_types(self):
        """Distinguish equal Python scalars with different canonical JSON types."""
        sink = await self.sink(mode="buffered", batch_delay_ms=5000)
        event = RuntimeEvent("types", 1, SessionOpened({"value": 1}))
        await sink.append(event)
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, payload=SessionOpened({"value": True})))
        await sink.flush()
        with self.assertRaises(ConflictError):
            await sink.append(replace(event, payload=SessionOpened({"value": 1.0})))

    async def test_commit_releases_source_objects_while_writer_remains_open(self):
        """Release actual source references after commit, beyond queue accounting."""

        class Blob(str):
            pass

        sink = await self.sink(mode="buffered")
        blob = Blob("x" * 1048576)
        reference = weakref.ref(blob)
        event = RuntimeEvent("memory", 1, SessionOpened({"blob": blob}))
        await sink.append(event)
        del blob, event
        await sink.flush()
        gc.collect()
        self.assertIsNone(reference())
        self.assertEqual((await sink.health()).state, "open")

    async def test_cancelled_close_still_drains_and_releases_writer(self):
        """Keep shutdown alive after cancellation and reject capacity waiters."""
        sink = await self.sink(mode="buffered", queue_count=1, batch_delay_ms=0)
        original = sink._writer.storage.write

        async def install():
            gate, entered = asyncio.Event(), asyncio.Event()

            async def delayed(entries):
                entered.set()
                await gate.wait()
                return await original(entries)

            sink._writer.storage.write = delayed
            return gate, entered

        gate, entered = await sink._writer.run(install)
        await sink.append(RuntimeEvent("a", 1, SessionOpened({})))

        async def started():
            await entered.wait()

        await sink._writer.run(started)
        blocked = asyncio.create_task(
            sink.append(RuntimeEvent("b", 1, SessionOpened({})))
        )
        await asyncio.sleep(0.03)
        closing = asyncio.create_task(sink.close())
        with self.assertRaises(ClosedError):
            await asyncio.wait_for(blocked, 2)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        sink._writer.loop.call_soon_threadsafe(gate.set)
        await asyncio.wait_for(asyncio.wrap_future(sink._writer._close_future), 2)
        await sink.close()
        self.assertFalse(sink._writer.thread.is_alive())
        reopened = await self.sink()
        self.assertEqual(len((await reopened.runtime_events("a")).items), 1)
        self.assertEqual((await reopened.runtime_events("b")).items, ())

    async def test_edge_evaluation_pages_follow_runtime_sequence(self):
        """Page actual condition results chronologically rather than by UUID."""
        sink = await self.sink()
        wf = Workflow(
            "edge-pagination",
            nodes=[
                Node("entry", identity),
                Node("step", shorten),
                Node("exit", identity),
            ],
            edges=[
                Edge("entry", "step"),
                Edge("step", "step", again, id="back"),
                Edge("step", "exit", finished, id="exit"),
            ],
        )
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {"blob": "abcd"})
        whole = await sink.edge_detail(result.ref.invocation_id, "back")
        values, cursor = [], None
        while True:
            page = await sink.edge_detail(
                result.ref.invocation_id, "back", limit=1, evaluations_cursor=cursor
            )
            values.extend(page["evaluations"])
            cursor = page["evaluations_cursor"]
            if cursor is None:
                break
        self.assertEqual(tuple(values), whole["evaluations"])
        self.assertEqual(
            [r["sequence"] for r in values], sorted(r["sequence"] for r in values)
        )

    async def test_settling_output_reference_is_cleared_on_cancellation(self):
        """Do not expose a discarded success result after cancellation wins."""
        sink = await self.sink()
        child = Workflow("cancel-child", nodes=[Node("wait", Wait(Value, Value))])
        root = Workflow("cancel-root", nodes=[Node("spawn", Spawn(child, "wait"))])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(root)
        for wf in (root, child):
            await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(root, {"blob": "wait"})
        self.assertEqual(result.status, "settling")
        self.assertIsNotNone(
            (await sink.get_invocation(result.ref.invocation_id))["output_ref"]
        )
        cancelled = await app.acancel(result.ref, "stop")
        self.assertEqual(cancelled.status, "cancelled")
        self.assertIsNone(
            (await sink.get_invocation(result.ref.invocation_id))["output_ref"]
        )


class SQLitePersistenceTests(PersistenceContracts, unittest.IsolatedAsyncioTestCase):
    async def test_memory_connection_serializes_admission_and_mutation(self):
        """Keep admission SQL off the single SQLite connection while a write owns it."""
        sink = await DatabaseSink.open(
            "sqlite+aiosqlite:///:memory:",
            config=PersistenceConfig(mode="buffered", batch_delay_ms=0),
        )
        self.sinks.append(sink)
        writer = sink._writer
        original_write, original_source = writer.storage.write, writer.storage.source_header

        async def scenario():
            gate, entered = asyncio.Event(), asyncio.Event()
            reads, writing = 0, False

            async def delayed_write(entries):
                nonlocal writing
                writing = True
                entered.set()
                await gate.wait()
                try:
                    return await original_write(entries)
                finally:
                    writing = False

            async def checked_read(*args):
                nonlocal reads
                self.assertFalse(writing)
                self.assertTrue(writer.mutation.locked())
                reads += 1
                return await original_source(*args)

            writer.storage.write, writer.storage.source_header = delayed_write, checked_read
            first = UserEvent("memory", "first", 1, "progress", {})
            await writer.accept("user_event", first.id, first)
            await asyncio.wait_for(entered.wait(), 2)
            others = [UserEvent("memory", f"i-{i}", 1, "progress", {}) for i in range(6)]
            tasks = [asyncio.create_task(writer.accept("user_event", e.id, e)) for e in others]
            try:
                await asyncio.sleep(.02)
                self.assertEqual(reads, 1)
                self.assertTrue(all(not task.done() for task in tasks))
            finally:
                gate.set()
                await asyncio.gather(*tasks)
            await writer.flush()
            self.assertEqual(writer.pending_by_identity, {})
            self.assertEqual(writer.pending_tails, {})
            self.assertEqual(writer.objects, {})

        await asyncio.wait_for(writer.run(scenario), 10)
        self.assertEqual(len(await self.rows(sink, "user_events")), 7)


@unittest.skipUnless(
    POSTGRES_TEST_URL,
    "isolated PostgreSQL URL not configured",
)
class PostgreSQLPersistenceTests(
    PersistenceContracts, unittest.IsolatedAsyncioTestCase
):
    async def database_url(self):
        import asyncpg
        from sqlalchemy.engine import make_url

        self.admin_url = make_url(POSTGRES_TEST_URL)
        self.database_name = "autoagent_test_" + uuid4().hex
        conn = await asyncpg.connect(
            self.admin_url.set(drivername="postgresql").render_as_string(
                hide_password=False
            )
        )
        try:
            await conn.execute('CREATE DATABASE "' + self.database_name + '"')
        finally:
            await conn.close()
        return self.admin_url.set(
            drivername="postgresql+asyncpg", database=self.database_name
        ).render_as_string(hide_password=False)

    async def remove_database(self):
        import asyncpg

        conn = await asyncpg.connect(
            self.admin_url.set(drivername="postgresql").render_as_string(
                hide_password=False
            )
        )
        try:
            await conn.execute(
                'DROP DATABASE "' + self.database_name + '" WITH (FORCE)'
            )
        finally:
            await conn.close()


class ArtifactCodecTests(unittest.TestCase):
    def roundtrip(self, value, *, threshold=1024):
        result = encode({"payload": value}, "user_event", threshold, EncodingMemo(4096))

        def load(digest):
            artifact = result.artifacts[digest]
            body = json.loads(artifact["body"])
            for path, child in artifact["deps"]:
                body = assign(body, path, load(child))
            return body

        body = result.record
        for path, digest in result.refs:
            body = assign(body, path, load(digest))
        self.assertEqual(body, {"payload": value})
        return result

    def test_user_keys_unicode_and_nested_manifests(self):
        """Handle normal user keys and nested manifests without sentinel collisions."""
        value = {
            "op": ["replace"],
            "path": ["value"],
            "artifact_id": "user",
            "unicode": "中文" * 1000,
            "rows": [
                {"small": "a" * 80, "index": i, "large": "z" * 2000 if i == 0 else ""}
                for i in range(40)
            ],
        }
        result = self.roundtrip(value)
        self.assertTrue(any(a["deps"] for a in result.artifacts.values()))

    def test_threshold_and_content_hash_order(self):
        """Apply the canonical-byte threshold and ignore dictionary key order."""
        self.assertEqual(len(self.roundtrip("x" * 1021).artifacts), 0)
        self.assertEqual(len(self.roundtrip("x" * 1022).artifacts), 1)
        a = self.roundtrip({"a": "x" * 600, "b": "y" * 600})
        b = self.roundtrip({"b": "y" * 600, "a": "x" * 600})
        self.assertEqual(set(a.artifacts), set(b.artifacts))

    def test_escaped_surrogates_roundtrip_as_json(self):
        """Preserve escaped surrogate values allowed by Core JSON records."""
        self.roundtrip({"value": "\ud800" * 2000})
