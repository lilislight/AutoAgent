"""Database projections remain optional and never become execution authority."""
import asyncio
from contextlib import asynccontextmanager
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sqlalchemy import select

from autoagent.host.persistence import DatabaseSink, PersistenceConfig
from autoagent.host.persistence.schema import DETAIL_TABLES, meta
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_host_persistence import PersistenceContracts


class ProjectionSchedulingTests(unittest.IsolatedAsyncioTestCase):
    def worker(self):
        from autoagent.host.persistence.projection_worker import ProjectionWorker
        return ProjectionWorker(SimpleNamespace(config=PersistenceConfig(), pending_count=0,
                                                storage=SimpleNamespace(last_write=-100)))

    async def test_busy_round_waits_ten_seconds(self):
        """Busy scheduling waits ten seconds without opening a database snapshot."""
        worker = self.worker()
        delays = []
        async def sleep(delay):
            delays.append(delay)
            if len(delays) == 2:
                worker.stop.set()
        with patch.object(worker, 'busy', return_value=True), patch(
                'autoagent.host.persistence.projection_worker.asyncio.sleep', sleep):
            await worker._idle(None)
        self.assertEqual(delays, [5, 10])
        self.assertEqual(worker.busy_rounds, 1)

    async def test_finished_round_waits_five_seconds(self):
        """The next idle delay starts after a completed projection round."""
        worker = self.worker()
        actions = []
        conn = SimpleNamespace(execute=AsyncMock())
        @asynccontextmanager
        async def transaction():
            yield conn
        conn.begin = transaction
        storage = SimpleNamespace(sqlite=True, query_engine=SimpleNamespace(connect=transaction))
        async def prepare(*args):
            actions.append('computed')
            return SimpleNamespace(after=1, order=1)
        async def sleep(delay):
            actions.append(delay)
            if len(actions) == 3:
                worker.stop.set()
        with patch.object(worker, 'busy', return_value=False), patch(
                'autoagent.host.persistence.projection_worker.prepare', prepare), patch(
                'autoagent.host.persistence.projection_worker.asyncio.sleep', sleep):
            await worker._idle(storage)
        self.assertEqual(actions, [5, 'computed', 5])

    async def test_cpu_gate_excludes_projection_computation(self):
        """Worker CPU does not cause it to repeatedly discard its own prepared work."""
        worker = self.worker()
        worker.cores = 1
        worker._cpu_stamp = (0, 0, 0)
        clock = SimpleNamespace(monotonic=lambda: 1, process_time=lambda: .9, thread_time=lambda: .9)
        with patch('autoagent.host.persistence.projection_worker.time', clock):
            self.assertFalse(worker.busy())
            clock.monotonic = lambda: 2
            clock.process_time = lambda: 1.9
            self.assertTrue(worker.busy())


class DatabaseProjectionContracts:
    def assert_trace_equal(self, left, right):
        self.assertEqual({key: value for key, value in left.items() if key != 'observed_at_us'},
                         {key: value for key, value in right.items() if key != 'observed_at_us'})
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.apps, self.sinks = [], []
        self.url = await self.database_url()

    async def asyncTearDown(self):
        for app in self.apps:
            app.close()
        for sink in self.sinks:
            if sink._writer.state == 'faulted':
                from autoagent.host.persistence import PersistenceError
                with self.assertRaises(PersistenceError):
                    await sink.close()
            else:
                await sink.close()
        await self.remove_database()
        self.directory.cleanup()

    async def database_url(self):
        return 'sqlite+aiosqlite:///' + str(Path(self.directory.name) / 'projection.sqlite')

    async def remove_database(self):
        pass

    sink = PersistenceContracts.sink
    workflow = PersistenceContracts.workflow
    registered = PersistenceContracts.registered
    rows = PersistenceContracts.rows

    async def _transactional_contract(self, method):
        original = self.sink
        async def sink(**config):
            return await original(projection_mode='transactional', projection_batch_count=8, **config)
        self.sink = sink
        await method(self)

    async def test_transactional_child_rebuild_and_purge(self):
        """Detailed foreign keys survive Child ownership rebuild and history purge."""
        await self._transactional_contract(PersistenceContracts.test_child_graph_ownership_checkpoint_rebuild_and_purge)

    async def test_transactional_imported_anchor(self):
        """Imported Checkpoints seed detailed projections before continuation Events."""
        await self._transactional_contract(PersistenceContracts.test_import_anchor_rebuild_and_retention_guard)

    async def test_transactional_wait_continuation(self):
        """Wait resumption and recovery retain references across small SQL batches."""
        await self._transactional_contract(PersistenceContracts.test_wait_checkpoint_reload_continues_same_identity)

    async def test_sql_prefix_same_invocation_tail(self):
        """A running Invocation combines its persisted prefix with resumed Event tail."""
        from autoagent import Node, Wait, Workflow
        from autoagent.core.app import AutoAgentApp
        from tests.test_host_persistence import Value
        sink = await self.sink(projection_worker_enabled=False, projection_cache_enabled=False)
        wf = Workflow('prefix-wait', nodes=[Node('approval', Wait(Value, Value))])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result = await app.ainvoke(wf, {'blob': 'waiting'})
        await sink.build_query_projections()
        prefix = (await sink.query_projection_status())['processed_order']
        await app.aresume(result.ref, result.waits[0].id, {'blob': 'approved'})
        await sink.flush()
        rows = await sink.list_waits(result.invocation_id)
        self.assertEqual(rows.items[0]['status'], 'resumed')
        before = sink._queries.projection_cache.built_events
        waits = await sink.wait_detail(result.invocation_id, result.waits[0].id)
        self.assertEqual(waits['status'], 'resumed')
        self.assertEqual((await sink.query_projection_status())['processed_order'], prefix)
        self.assertGreater(sink._queries.projection_cache.built_events, before)

    async def test_fork_graph_and_value_references_in_both_modes(self):
        """Fork lineage reads pending same-batch graph rows and original value references."""
        from autoagent import Edge, Node, Wait, Workflow
        from autoagent.host import AutoAgentHost, HostSettings
        from tests.test_forks import Value, echo, selected
        wf = Workflow('db-fork-values', nodes=[Node('echo', echo), Node('ask', Wait(Value, Value))],
                      edges=[Edge('echo', 'ask', selected, id='next')])
        for mode in ('background', 'transactional'):
            with self.subTest(mode=mode):
                config = PersistenceConfig(projection_mode=mode, projection_worker_enabled=False,
                    projection_cache_enabled=False, projection_batch_count=10000)
                async with AutoAgentHost(workflows=[wf], settings=HostSettings(database_url=self.url,
                    persistence=config, local_projections=False), root=self.directory.name, environment={}) as host:
                    ref = await host.execute(wf.id, {'text': 'original'}, entry_node_id='echo')
                    await host.join(ref)
                    events = (await host.list_execution_events(ref.invocation_id,
                        names=['node_occurrence.waiting'], limit=100)).items
                    branch = await host.fork(ref.invocation_id, sequence=events[-1]['sequence'])
                    await host.join(branch['invocation_id'])
                    before = await host.get_execution_trace(branch['invocation_id'])
                    await host.build_query_projections()
                    self.assert_trace_equal(await host.get_execution_trace(branch['invocation_id']), before)
                    calls = (await host.list_operator_calls(branch['invocation_id'])).items
                    call = next(row for row in calls if row['operator_id'] == 'echo')
                    value = await host.get_execution_value(branch['invocation_id'], 'call', call['call_id'], 'output')
                    self.assertEqual(value['value'], {'text': 'original'})
                    self.assertEqual(next(row for row in before['edges'] if row['edge_id'] == 'next')['evaluations'], 1)

    async def test_manual_metadata_build_never_reads_business_artifacts(self):
        """Offline metadata generation never decodes Node/Call input or output bodies."""
        sink, _, _, _ = await self.exercise(projection_worker_enabled=False)
        from autoagent.host.persistence.storage import Storage
        original = Storage.row
        async def no_business(storage, conn, name, **identity):
            self.assertNotEqual(name, 'artifacts', 'Projection decoded a business Artifact')
            return await original(storage, conn, name, **identity)
        with patch.object(Storage, 'row', no_business):
            await sink.build_query_projections()

    async def test_purge_keeps_unrelated_database_projections(self):
        """Purging one Root preserves other SQL projections and their coverage frontier."""
        sink, app, wf, first = await self.exercise(projection_worker_enabled=False, projection_cache_enabled=False)
        second = await app.ainvoke(wf, {'blob': 'keep'})
        await sink.flush()
        await sink.build_query_projections()
        await sink.purge_root_graph(first.invocation_id)
        self.assertEqual((await sink.query_projection_status())['lag_events'], 0)
        self.assertTrue(await self.raw(sink, 'node_executions'))
        from autoagent.host.persistence.read_models import ProjectionCache
        with patch.object(ProjectionCache, 'build', side_effect=AssertionError('Unrelated SQL projection was lost')):
            self.assertEqual(len((await sink.list_nodes(second.invocation_id)).items), 2)

    async def raw(self, sink, name):
        async def read():
            async with sink._writer.storage.engine.connect() as conn:
                return [dict(row) for row in (await conn.execute(select(meta.tables[name]))).mappings()]
        return await sink._writer.run(read)

    async def exercise(self, **config):
        sink = await self.sink(**config)
        app, wf, _ = await self.registered(sink)
        result = await app.ainvoke(wf, {'blob': 'business-value' * 1000})
        await sink.flush()
        return sink, app, wf, result

    async def test_schema_initially_empty(self):
        """Every query table exists at creation, before any Event or Worker work."""
        sink = await self.sink(projection_worker_enabled=False)
        for name in (*DETAIL_TABLES, 'query_projection_state', 'query_projection_entities'):
            self.assertEqual(await self.raw(sink, name), [])

    async def test_transactional_matches_event_reconstruction(self):
        """Transactional metadata and references match pure Event reconstruction."""
        sink, _, _, result = await self.exercise(projection_mode=PersistenceConfig().projection_mode, projection_cache_enabled=False)
        self.assertEqual(sink.config.projection_mode, 'transactional')
        self.assertIsNone(sink._projection_worker)
        for name in ('node_executions', 'operator_calls', 'graph_changes'):
            self.assertTrue(await self.raw(sink, name), name)
        status = await sink.query_projection_status()
        self.assertEqual(status['processed_order'], status['durable_order'])
        before = await sink.execution_trace(result.invocation_id)
        calls = await sink.list_calls(result.invocation_id)
        expected_input = await sink.trace_value(result.invocation_id, 'call', calls.items[0]['call_id'], 'input')
        self.assertIn('business-value', str(expected_input))
        from autoagent.host.persistence.query_projections import reset
        async def clear():
            async with sink._writer.storage.engine.begin() as conn:
                await reset(conn)
        await sink._writer.run(clear)
        after = await sink.execution_trace(result.invocation_id)
        self.assert_trace_equal(before, after)

    async def test_manual_build_read_only_and_rebuild(self):
        """Build/rebuild from SQL facts keeps values lazy and read-only readers use SQL."""
        sink, _, _, result = await self.exercise(projection_worker_enabled=False)
        expected = await sink.execution_trace(result.invocation_id)
        self.assertIsNone((await sink.query_projection_status())['processed_order'])
        await sink.build_query_projections()
        self.assertEqual((await sink.query_projection_status())['lag_events'], 0)
        async def clear_cache():
            sink._queries.projection_cache.clear()
        await sink._writer.run(clear_cache)
        from autoagent.host.persistence.read_models import ProjectionCache
        with patch.object(ProjectionCache, 'build', side_effect=AssertionError('should use persisted SQL')):
            self.assert_trace_equal(await sink.execution_trace(result.invocation_id), expected)
            reader = await DatabaseSink.open(self.url, read_only=True)
            self.sinks.append(reader)
            self.assert_trace_equal(await reader.execution_trace(result.invocation_id), expected)
            self.assertFalse((await reader.query_projection_status())['worker']['running'])
        await sink.build_query_projections(rebuild=True)
        self.assert_trace_equal(await sink.execution_trace(result.invocation_id), expected)

    async def test_database_prefix_folds_only_event_tail(self):
        """Lagging SQL projection seeds a cold query before newer facts are folded."""
        sink, app, wf, first = await self.exercise(projection_worker_enabled=False, projection_cache_enabled=False)
        await sink.build_query_projections()
        second = await app.ainvoke(wf, {'blob': 'second'})
        await sink.flush()
        before = sink._queries.projection_cache.built_events
        rows = await sink.list_nodes(first.invocation_id)
        self.assertEqual(len(rows.items), 2)
        self.assertEqual(sink._queries.projection_cache.built_events, before)
        rows = await sink.list_nodes(second.invocation_id)
        self.assertEqual(len(rows.items), 2)
        self.assertGreater(sink._queries.projection_cache.built_events, before)

    async def test_failed_transaction_does_not_publish_projection(self):
        """Projection failure rolls back facts and frontier with the transaction."""
        sink = await self.sink(projection_mode='transactional')
        app, wf, _ = await self.registered(sink)
        from autoagent.host.persistence import query_projections
        original = query_projections.publish
        async def fail(storage, conn, prepared):
            await original(storage, conn, prepared)
            raise RuntimeError('projection transaction failed')
        with patch.object(query_projections, 'publish', fail):
            with self.assertRaises(Exception):
                await app.ainvoke(wf, {'blob': 'rollback'})
        self.assertEqual(await self.raw(sink, 'runtime_events'), [])
        self.assertEqual(await self.raw(sink, 'node_executions'), [])

    async def test_transactional_writes_do_not_read_new_facts_back(self):
        """A current projection cut folds prepared metadata without Event or reference reads."""
        from sqlalchemy import event
        sink, app, wf, _ = await self.exercise(projection_mode='transactional')
        statements = []
        def collect(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith('SELECT'):
                statements.append(statement)
        engine = sink._writer.storage.engine.sync_engine
        event.listen(engine, 'before_cursor_execute', collect)
        try:
            result = await app.ainvoke(wf, {'blob': 'prepared-second-call'})
            await sink.flush()
        finally:
            event.remove(engine, 'before_cursor_execute', collect)
        self.assertEqual(result.status, 'completed')
        self.assertFalse([sql for sql in statements if 'FROM runtime_events' in sql or 'FROM artifact_refs' in sql])
        self.assertEqual((await sink.query_projection_status())['lag_events'], 0)

    async def test_projection_cache_is_published_only_after_commit(self):
        """A failed COMMIT leaves no advanced cached cursor and the same facts can retry."""
        from autoagent.core.runtime import RuntimeEvent, SessionOpened
        from autoagent.host.persistence.query_projections import FRONTIER_KEY
        from autoagent.host.persistence.storage import prepare
        sink = await self.sink(projection_mode='transactional')
        await sink.append(RuntimeEvent('projection-before-commit', 1, SessionOpened({})))
        following = RuntimeEvent('projection-after-commit', 1, SessionOpened({}))
        async def scenario():
            storage = sink._writer.storage
            encoded = await sink._writer.cpu(prepare, [SimpleNamespace(kind='runtime_event',
                identity=following.id, value=following, binding=None, anchor=False)], sink.config)
            old = storage.metadata_cache.get(FRONTIER_KEY)
            original = storage.transaction
            @asynccontextmanager
            async def abort():
                async with original() as conn:
                    yield conn
                    self.assertIsNotNone(storage._committing_projection)
                    self.assertEqual(storage.metadata_cache.get(FRONTIER_KEY), old)
                    raise RuntimeError('abort before COMMIT')
            async with sink._writer.mutation:
                with patch.object(storage, 'transaction', abort):
                    with self.assertRaisesRegex(RuntimeError, 'abort before COMMIT'):
                        await storage.write(encoded)
                self.assertIsNone(storage.metadata_cache.get(FRONTIER_KEY))
                self.assertIsNone(storage._committing_projection)
                async with storage.engine.connect() as conn:
                    self.assertEqual(await conn.scalar(select(meta.tables['persistence_meta'].c.last_store_order)), 1)
                await storage.write(encoded)
                self.assertEqual(storage.metadata_cache.get(FRONTIER_KEY)[1], 2)
                self.assertLessEqual(storage.metadata_cache.bytes, storage.metadata_cache.budget)
        await sink._writer.run(scenario)
        self.assertEqual(len(await self.raw(sink, 'runtime_events')), 2)

    async def test_chunked_projection_cache_eviction_preserves_loop_ordinals(self):
        """Small projection pages and cache eviction keep repeated Node ordinals and replay identical."""
        from autoagent import Edge, Node, ResourcePolicy, Workflow
        from autoagent.core.app import AutoAgentApp
        from tests.test_host_persistence import again, finished, identity, shorten
        for budget in (4 * 1024 * 1024, 64):
            with self.subTest(budget=budget):
                async with await self.sink(projection_mode='transactional', mode='buffered',
                        batch_delay_ms=60000, projection_batch_count=2, writer_metadata_cache_bytes=budget) as sink:
                    wf = Workflow('projection-cache-loop', nodes=[Node('entry', identity), Node('step', shorten,
                        resource=ResourcePolicy(max_executions_per_invocation=8)), Node('finish', identity)],
                        edges=[Edge('entry', 'step'), Edge('step', 'step', condition=again), Edge('step', 'finish', condition=finished)])
                    app = AutoAgentApp(runtime_event_sink=sink)
                    self.apps.append(app)
                    app.register_workflow(wf)
                    await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
                    session = None
                    for _ in range(2):
                        result = await app.ainvoke(wf, {'blob': 'abc'}, entry_node_id='entry', session_id=session)
                        session = result.ref.session_id
                        await sink.flush()
                        nodes = (await sink.list_nodes(result.invocation_id)).items
                        self.assertEqual(sorted(row['ordinal'] for row in nodes if row['node_id'] == 'step'), [1, 2, 3])
                        before = await sink.execution_trace(result.invocation_id)
                        await sink.build_query_projections(rebuild=True)
                        self.assert_trace_equal(await sink.execution_trace(result.invocation_id), before)
                        self.assertLessEqual(sink._writer.storage.metadata_cache.bytes, sink._writer.storage.metadata_cache.budget)

    async def test_stale_publish_after_reset_is_rejected(self):
        """Maintenance epoch changes cannot resurrect a prepared stale projection."""
        sink, _, _, _ = await self.exercise(projection_worker_enabled=False)
        from autoagent.host.persistence.query_projections import prepare, publish
        async def scenario():
            storage = sink._writer.storage
            async with storage.query_engine.connect() as conn:
                prepared = await prepare(storage, conn, sink.config)
            from sqlalchemy import update
            async with storage.engine.begin() as conn:
                await conn.execute(update(meta.tables['persistence_meta']).values(store_id='new-history'))
            async with storage.engine.begin() as conn:
                self.assertFalse(await publish(storage, conn, prepared))
        await sink._writer.run(scenario)
        self.assertEqual(await self.raw(sink, 'node_executions'), [])

    async def test_maintenance_takes_precedence_over_projection_publish(self):
        """Background publication yields to a foreground transaction before row deletion."""
        from sqlalchemy.exc import DBAPIError
        from autoagent.host.persistence.projection_worker import ProjectionWorker
        from autoagent.host.persistence.query_projections import prepare, publish
        sink, _, _, _ = await self.exercise(projection_worker_enabled=False)
        async def control(conn):
            await conn.scalar(select(meta.tables['persistence_meta'].c.last_store_order))
            worker_storage = ProjectionWorker(sink._writer)._storage()
            try:
                async with worker_storage.query_engine.connect() as reader:
                    prepared = await prepare(worker_storage, reader, sink.config)
                with self.assertRaises(DBAPIError):
                    async with worker_storage.engine.begin() as publisher:
                        await publish(worker_storage, publisher, prepared)
                from sqlalchemy import update
                await conn.execute(update(meta.tables['persistence_meta']).values(
                    last_store_order=meta.tables['persistence_meta'].c.last_store_order))
            finally:
                await worker_storage.close()
        await sink._writer.run(sink._maintenance.run, control)
        self.assertEqual(await self.raw(sink, 'node_executions'), [])

    async def test_background_owns_thread_and_reads_committed_facts(self):
        """Idle Worker uses its own thread and never projects uncommitted queue data."""
        sink = await self.sink(mode='buffered', batch_delay_ms=60000,
            projection_idle_seconds=.03, projection_busy_seconds=.03, projection_quiet_seconds=.01,
            projection_work_seconds=1, projection_batch_count=64)
        app, wf, _ = await self.registered(sink)
        result = await app.ainvoke(wf, {'blob': 'queued'})
        await asyncio.sleep(.1)
        self.assertEqual(await self.raw(sink, 'node_executions'), [])
        await sink.flush()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            status = await sink.query_projection_status()
            if status['processed_order'] == status['durable_order']:
                break
            await asyncio.sleep(.05)
        self.assertEqual(status['lag_events'], 0, status)
        self.assertTrue(await self.raw(sink, 'node_executions'))
        worker = sink._projection_worker
        self.assertNotEqual(worker.thread.ident, sink._writer.thread.ident)
        self.assertNotEqual(worker.thread.ident, threading.get_ident())
        self.assertEqual(worker.failures, 0, worker.error)
        self.assertEqual((await sink.get_invocation(result.invocation_id))['status'], 'completed')

    async def test_transactional_start_catches_up_background_history(self):
        """Changing modes catches up durable history before accepting new Events."""
        sink, _, _, result = await self.exercise(projection_worker_enabled=False)
        await sink.close()
        reopened = await DatabaseSink.open(self.url, config=PersistenceConfig(projection_mode='transactional'))
        self.sinks.append(reopened)
        self.assertEqual((await reopened.query_projection_status())['lag_events'], 0)
        self.assertEqual(len((await reopened.list_nodes(result.invocation_id)).items), 2)


class SQLiteDatabaseProjectionTests(DatabaseProjectionContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL is not configured')
class PostgreSQLDatabaseProjectionTests(DatabaseProjectionContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        self.pg = PostgreSQLDatabase()
        return await self.pg.create()

    async def remove_database(self):
        await self.pg.close()
