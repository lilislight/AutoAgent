"""Local Server browsing, borrowed Host ownership, and UI hosting contracts."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import os
import sys
import tempfile
import threading
from pathlib import Path
import unittest
from urllib.parse import quote
from unittest.mock import patch
from uuid import uuid4
from tests.postgres import POSTGRES_TEST_URL

try:
    import httpx
    from fastapi import FastAPI
    from autoagent.server import ServerSettings, create_app, create_router, mount_ui
except ModuleNotFoundError as exc:
    raise unittest.SkipTest("Install autoagent[server] and httpx to run Server tests.") from exc

from typing_extensions import TypedDict
from autoagent import (AggregationContext, ConditionContext, ContextOperation, ContextPatch,
                       Edge, InputMappingContext, Map, Node, OutputBindingContext, Workflow)
from autoagent.host import AutoAgentHost, HostSettings


class Value(TypedDict):
    text: str


def identity(value: Value) -> Value:
    return value


def workflow(name="catalog", version="1"):
    return Workflow(name, version=version, nodes=[Node("identity", identity)])


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.host_settings = HostSettings(database_url="sqlite+aiosqlite:///" + str(self.root / "runtime.db"))
        self.ui = self.root / "ui"
        self.ui.mkdir()
        (self.ui / "index.html").write_text('<html><head><!--AUTOAGENT_CONFIG--></head><body>workflow-browser</body></html>')
        (self.ui / "assets").mkdir()
        (self.ui / "assets" / "app.js").write_text("/* asset */")

    async def asyncTearDown(self):
        # Project imports intentionally retain identity for a running Host.
        # Remove this fixture's modules so the next isolated project can load.
        for name, module in tuple(sys.modules.items()):
            origin = getattr(module, "__file__", None)
            if origin and Path(origin).resolve().is_relative_to(self.root):
                del sys.modules[name]
        self.temp.cleanup()

    def host(self, workflows=None):
        return AutoAgentHost(workflows=workflows if workflows is not None else [workflow()], settings=self.host_settings, root=self.root, environment={})

    def client(self, app):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")

    @asynccontextmanager
    async def database_connection(self, host, *, write=False):
        """Inspect or seed fixtures through a connection owned by the test's loop."""
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(host._persistence._writer.storage.url)
        try:
            async with (engine.begin() if write else engine.connect()) as connection:
                yield connection
        finally:
            await engine.dispose()

    async def test_trace_dataset_downloads_all_metadata_without_replay_or_business_values(self):
        """Paged exports include every timing/header record while raw values and Replay remain unread."""
        from autoagent.host.persistence.reading import SourceReader
        def step(value: Value) -> Value:
            return {'text': value['text'][1:]}
        def again(ctx: ConditionContext) -> bool:
            return bool(ctx.output['text'])
        def finish(ctx: ConditionContext) -> bool:
            return not ctx.output['text']
        wf = Workflow('export-loop', nodes=[Node('entry', identity),Node('step', step),Node('done',identity)], edges=[Edge('entry','step'),Edge('step','step',again),Edge('step','done',finish)])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text':'x'*120}, entry_node_id='entry')
            await host.join(root)
            async with self.client(create_app(host, ui=False)) as client:
                with patch.object(SourceReader,'read',side_effect=AssertionError('No business values')), patch('autoagent.host.persistence.datasets.replay_events',side_effect=AssertionError('Replay must be lazy')):
                    response = await client.get('/api/v2/trace-dataset',params=dict(invocation_id=root.invocation_id))
                    self.assertEqual(response.status_code,200,response.text)
                    manifest=response.json()
                    self.assertFalse(manifest['replay'])
                    self.assertIsNone(manifest['initial_trace'])
                    self.assertEqual(manifest['sections']['node']['count'],122)
                    self.assertEqual(manifest['sections']['call']['count'],122)
                    self.assertGreater(manifest['sections']['event']['count'],500)
                    rows=[]
                    for page in range(manifest['sections']['event']['pages']):
                        response=await client.get('/api/v2/trace-dataset-page',params=dict(snapshot_id=manifest['snapshot_id'],section='event',page=page))
                        self.assertEqual(response.status_code,200,response.text)
                        rows.extend(response.json()['items'])
                    self.assertEqual(len(rows),manifest['sections']['event']['count'])
                    self.assertEqual(len({row['event_id'] for row in rows}),len(rows))
                    required = {'event_id','event_name','sequence','occurred_at_us'}
                    allowed = required | {'control_state','invocation_status'}
                    self.assertTrue(all(required <= set(row) <= allowed for row in rows))
                    self.assertTrue(any(row.get('invocation_status') == 'completed' for row in rows))
                response=await client.get('/api/v2/trace-dataset',params=dict(invocation_id=root.invocation_id,replay='true',snapshot_id=manifest['snapshot_id']))
                self.assertEqual(response.status_code,200,response.text)
                replay=response.json()
                self.assertTrue(replay['history_complete'])
                self.assertEqual(replay['sections']['event']['count'],len(rows))
                self.assertEqual(replay['initial_trace']['nodes'],[])
                invalid=await client.get('/api/v2/trace-dataset-page',params=dict(snapshot_id=manifest['snapshot_id'],section='event',page=999999))
                self.assertEqual(invalid.status_code,400)
                self.assertEqual((await client.get('/api/v2/trace-dataset',params=dict(invocation_id='missing'))).status_code,404)

    async def test_trace_dataset_keeps_wait_snapshot_immutable_and_merges_incremental_upserts(self):
        """A running export stays frozen across resume; incremental timing updates reconstruct a fresh snapshot."""
        from autoagent import Wait
        wf=Workflow('export-wait',nodes=[Node('approval',Wait(Value,Value))])
        async with self.host([wf]) as host:
            root=await host.execute(wf.id,{'text':'ask'},entry_node_id='approval')
            await host.join(root)
            before=await host.get_trace_dataset(root.invocation_id)
            async def rows(manifest,kind):
                items=[]
                for page in range(manifest['sections'][kind]['pages']):
                    items.extend((await host.get_trace_dataset_page(manifest['snapshot_id'],kind,page))['items'])
                return items
            old={kind:await rows(before,kind) for kind in ('node','call','wait','event')}
            wait=(await host.list_execution_waits(root.invocation_id)).items[0]
            await host.respond(root,wait['wait_id'],{'text':'approved'})
            await host.join(root)
            self.assertEqual(await rows(before,'wait'),old['wait'])
            self.assertEqual(old['wait'][0]['status'],'waiting')
            delta=await host.get_trace_dataset(root.invocation_id,since=before['snapshot_id'])
            self.assertEqual(delta['base_version'],before['version'])
            fresh=await host.get_trace_dataset(root.invocation_id)
            for kind in ('node','call','wait','event'):
                key='event_id' if kind=='event' else 'id'
                merged={row[key]:row for row in old[kind]}
                merged.update({row[key]:row for row in await rows(delta,kind)})
                self.assertEqual(merged,{row[key]:row for row in await rows(fresh,kind)},kind)
            replay=await host.get_trace_dataset(root.invocation_id,replay=True,snapshot_id=before['snapshot_id'])
            self.assertEqual(replay['through_sequence'],before['through_sequence'])
            self.assertTrue(all(event['sequence']<=before['through_sequence'] for event in await rows(replay,'event')))
            # Cached browser metadata remains useful after the temporary export expires.
            host._persistence._datasets.entries[before['snapshot_id']]['used']=-1e30
            expired=await host.get_trace_dataset(root.invocation_id,replay=True,snapshot_id=before['snapshot_id'],through_sequence=before['through_sequence'])
            self.assertEqual(expired['through_sequence'],before['through_sequence'])
            self.assertTrue(expired['history_complete'])
            spool=host._persistence._datasets.entries[fresh['snapshot_id']]['file']
        self.assertTrue(spool.closed)

    async def test_trace_dataset_capacity_failure_is_explicit_and_leaves_no_partial_cache(self):
        """An oversized export returns a capacity error rather than retaining a partial snapshot."""
        host=self.host()
        host.settings=replace(host.settings,persistence=replace(host.settings.persistence,trace_snapshot_bytes=32))
        async with host:
            root=await host.execute('catalog',{'text':'large-'+'x'*1048576},entry_node_id='identity')
            await host.join(root)
            async with self.client(create_app(host,ui=False)) as client:
                response=await client.get('/api/v2/trace-dataset',params=dict(invocation_id=root.invocation_id))
                self.assertEqual(response.status_code,503,response.text)
            self.assertEqual(host._persistence._datasets.bytes,0)
            self.assertFalse(host._persistence._datasets.entries)

    async def test_complete_event_rail_groups_and_details_preserve_all_headers_without_values(self):
        """A bounded rail represents every Event, freezes its frontier and lazily exposes exact headers."""
        from sqlalchemy import select, event
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader
        def step(value: Value) -> Value:
            return {'text': value['text'][1:]}
        def again(ctx: ConditionContext) -> bool:
            return bool(ctx.output['text'])
        def finish(ctx: ConditionContext) -> bool:
            return not ctx.output['text']
        wf = Workflow('rail-loop', nodes=[Node('entry', identity), Node('step', step), Node('done', identity)],
            edges=[Edge('entry', 'step'), Edge('step', 'step', again), Edge('step', 'done', finish)])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text':'x' * 120}, entry_node_id='entry')
            await host.join(root)
            storage = host._persistence._writer.storage
            e = meta.tables['runtime_events']
            async with self.database_connection(host) as conn:
                headers = [dict(row) for row in (await conn.execute(select(e.c.event_id, e.c.event_name, e.c.sequence, e.c.occurred_at_us)
                    .where(e.c.invocation_id == root.invocation_id).order_by(e.c.sequence))).mappings()]
            self.assertGreater(len(headers), 500)
            start, end = min(row['occurred_at_us'] for row in headers), max(row['occurred_at_us'] for row in headers) + 1
            upper = headers[-1]['sequence']
            statements = []
            def inspect_sql(conn, cursor, statement, params, context, many):
                statements.append(statement)
            event.listen(storage.engine.sync_engine, 'before_cursor_execute', inspect_sql)
            try:
                with patch.object(SourceReader, 'read', side_effect=AssertionError('Rail must never read values')):
                    async with self.client(create_app(host, ui=False)) as client:
                        params = dict(invocation_id=root.invocation_id, start_us=start, end_us=end, through_sequence=upper)
                        for buckets in (1, 7, 32, 512):
                            response = await client.get('/api/v2/timeline-event-groups', params=dict(params, buckets=buckets))
                            self.assertEqual(response.status_code, 200, response.text)
                            result = response.json()
                            self.assertEqual(result['total_count'], len(headers))
                            self.assertLessEqual(len(result['items']), buckets)
                            self.assertEqual(sum(row['count'] for row in result['items']), len(headers))
                            self.assertTrue(all(sum(row['names'].values()) == row['count'] for row in result['items']))
                            for row in result['items']:
                                self.assertLessEqual(row['start_us'], row['occurred_at_us'])
                                self.assertLess(row['occurred_at_us'], row['end_us'])
                                if row['count'] == 1:
                                    self.assertIn(row['marker'], headers)
                        frozen = (await client.get('/api/v2/timeline-event-groups', params=dict(params, through_sequence=headers[219]['sequence']))).json()
                        self.assertEqual(frozen['total_count'], 220)
                        one = (await client.get('/api/v2/timeline-event-groups', params=dict(params, buckets=1))).json()['items'][0]
                        details, cursor = [], None
                        while True:
                            page = (await client.get('/api/v2/timeline-markers', params=dict(params, start_us=one['start_us'], end_us=one['end_us'], limit=100,
                                **({'cursor':cursor} if cursor else {})))).json()
                            details.extend(page['items'])
                            cursor = page['next_cursor']
                            if not cursor:
                                break
                        self.assertEqual(details, headers)
                        self.assertEqual((await client.get('/api/v2/timeline-event-groups', params=dict(params,buckets=513))).status_code, 422)
                        self.assertEqual((await client.get('/api/v2/timeline-event-groups', params=dict(params,end_us=start))).status_code, 400)
                        self.assertEqual((await client.get('/api/v2/timeline-event-groups', params=dict(params,invocation_id='missing'))).status_code, 404)
                self.assertFalse(any('payload_text' in statement or 'delta_text' in statement for statement in statements))
            finally:
                event.remove(storage.engine.sync_engine, 'before_cursor_execute', inspect_sql)

    async def test_map_call_counts_replay_at_start_boundaries_without_reading_inputs(self):
        """Graph and bounded Replay count only Calls started by the selected Event, including across batches."""
        from sqlalchemy import select
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader
        def mapped(ctx: InputMappingContext) -> list[Value]:
            return [ctx.invocation_input] * 12
        wf = Workflow('map-counts', nodes=[Node('mapped', identity, input_mapping=mapped, map=Map(max_parallelism=3))])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text':'payload'}, entry_node_id='mapped')
            await host.join(root)
            e = meta.tables['runtime_events']
            async with self.database_connection(host) as conn:
                starts = list((await conn.execute(select(e.c.sequence).where(e.c.invocation_id == root.invocation_id,
                    e.c.event_name == 'operator_call.started').order_by(e.c.sequence))).scalars())
            self.assertEqual(len(starts), 12)
            with patch.object(SourceReader, 'read', side_effect=AssertionError('Counts must never read inputs')):
                current = await host.get_execution_trace(root.invocation_id)
                self.assertEqual(current['nodes'][0]['call_count'], 12)
                for index, sequence in enumerate(starts):
                    before = await host.get_timeline_frame(root.invocation_id, sequence=sequence-1)
                    after = await host.get_timeline_frame(root.invocation_id, sequence=sequence)
                    self.assertEqual(before['trace']['nodes'][0]['call_count'], index)
                    self.assertEqual(after['trace']['nodes'][0]['call_count'], index+1)
                    window = await host.get_timeline_replay(root.invocation_id, sequence=sequence, limit=4)
                    count = next((row['call_count'] for row in window['baseline']['trace']['nodes'] if row['node_id']=='mapped'), 0)
                    for item in window['events']:
                        if item['sequence'] <= sequence:
                            count += sum(row['count'] for row in item['call_starts'] if row['node_id']=='mapped')
                    self.assertEqual(count,index+1)

    async def test_replay_windows_are_atomic_bounded_and_frozen_without_values(self):
        """Replay windows keep full Event deltas, a compact baseline and a fixed upper boundary."""
        from sqlalchemy import select, update
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader
        async with self.host() as host:
            root = await host.execute('catalog', {'text': 'large-' + 'x' * (2 * 1024 * 1024)}, entry_node_id='identity')
            await host.join(root)
            storage = host._persistence._writer.storage
            events, changes = meta.tables['runtime_events'], meta.tables['graph_changes']
            async with self.database_connection(host) as conn:
                headers = (await conn.execute(select(events.c.event_id, events.c.sequence).where(events.c.invocation_id == root.invocation_id).order_by(events.c.sequence))).mappings().all()
            upper = headers[-2]['sequence']
            with patch.object(SourceReader, 'read', side_effect=AssertionError('Replay must not read values')):
                async with self.client(create_app(host, ui=False)) as client:
                    response = await client.get('/api/v2/timeline-replay', params=dict(invocation_id=root.invocation_id, sequence=headers[3]['sequence'], through_sequence=upper, limit=4))
                    self.assertEqual(response.status_code, 200, response.text)
                    window = response.json()
                    self.assertTrue(window['history_complete'])
                    self.assertLessEqual(len(window['events']), 4)
                    self.assertEqual(window['last']['sequence'], upper)
                    self.assertEqual(window['boundary']['sequence'], headers[3]['sequence'])
                    self.assertNotIn('large-', response.text)
                    self.assertLess(len(response.content), 15000)
                    for item in window['events']:
                        async with self.database_connection(host) as conn:
                            rows = (await conn.execute(select(changes.c.entity_type, changes.c.entity_id, changes.c.change).where(changes.c.event_id == item['event_id']))).mappings().all()
                        expected = [(r['entity_type'], r['entity_id'], r['change']['status']) for r in rows if r['entity_type'] in ('node', 'invocation') and 'status' in r['change']]
                        actual = [(r['entity_type'], r['entity_id'], r['change']['status']) for r in item['changes'] if 'status' in r['change']]
                        self.assertEqual(actual, expected)
                    baseline = await host.get_timeline_frame(root.invocation_id, sequence=window['previous']['sequence'])
                    self.assertEqual(window['baseline']['trace']['nodes'], baseline['trace']['nodes'])
                    first = (await client.get('/api/v2/timeline-replay', params=dict(invocation_id=root.invocation_id, sequence=0, limit=4))).json()
                    self.assertEqual(first['baseline']['trace']['nodes'], [])
                    self.assertIsNone(first['previous'])
                    self.assertEqual((await client.get('/api/v2/timeline-replay', params=dict(invocation_id=root.invocation_id, sequence=1, at_us=1))).status_code, 400)
                    self.assertEqual((await client.get('/api/v2/timeline-replay', params=dict(invocation_id=root.invocation_id, limit=201))).status_code, 422)
                    self.assertEqual((await client.get('/api/v2/timeline-replay', params=dict(invocation_id='missing'))).status_code, 404)
            async with self.database_connection(host, write=True) as conn:
                await conn.execute(update(events).where(events.c.event_id == headers[0]['event_id']).values(event_name='history.truncated'))
            incomplete = await host.get_timeline_replay(root.invocation_id)
            self.assertFalse(incomplete['history_complete'])
            self.assertIsNone(incomplete['baseline'])

    async def test_execution_pages_tied_timestamps_in_descending_order(self):
        """Default newest-first pagination visits tied Session and Invocation timestamps exactly once."""
        from sqlalchemy import update
        from autoagent.host.persistence.schema import meta
        async with self.host() as host:
            roots = []
            for sid in ('sort-a', 'sort-b', 'sort-c', 'sort-a'):
                root = await host.execute('catalog', {'text': sid}, entry_node_id='identity', session_id=sid)
                await host.join(root)
                roots.append(root)
            async with self.database_connection(host, write=True) as conn:
                await conn.execute(update(meta.tables['runtime_events']).where(meta.tables['runtime_events'].c.invocation_id.in_([r.invocation_id for r in roots])).values(occurred_at_us=100))
                await conn.execute(update(meta.tables['invocations']).values(started_at_us=100))
            async with self.client(create_app(host, ui=False)) as client:
                for endpoint, extra, key, expected in (
                    ('execution-sessions', {}, 'session_id', ['sort-a','sort-b','sort-c']),
                    ('session-invocations', {'session_id':'sort-a'}, 'invocation_id', sorted([roots[0].invocation_id, roots[3].invocation_id])),
                ):
                    items, cursor = [], None
                    while True:
                        params = dict(extra, limit=1)
                        if cursor: params['cursor'] = cursor
                        response = await client.get('/api/v2/' + endpoint, params=params)
                        self.assertEqual(response.status_code, 200, response.text)
                        page = response.json()
                        items.extend(row[key] for row in page['items'])
                        cursor = page['next_cursor']
                        if not cursor: break
                    self.assertEqual(items, list(reversed(expected)))

    async def test_trace_snapshot_times_scope_saved_event_headers_and_read_clock(self):
        """Snapshot age uses this Invocation's saved Event, not the global store watermark."""
        from sqlalchemy import select
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader
        async with self.host() as host:
            first = await host.execute('catalog', {'text': 'first'}, entry_node_id='identity', session_id='snapshot-session')
            await host.join(first)
            before = await host.get_execution_trace(first.invocation_id)
            second = await host.execute('catalog', {'text': 'large-' + 'x' * (2 * 1024 * 1024)}, entry_node_id='identity', session_id='snapshot-session')
            await host.join(second)
            events = meta.tables['runtime_events']
            async with self.database_connection(host) as conn:
                expected = (await conn.execute(select(events.c.event_id, events.c.event_name, events.c.sequence, events.c.occurred_at_us)
                    .where(events.c.invocation_id == first.invocation_id).order_by(events.c.sequence.desc()).limit(1))).mappings().one()
            with patch.object(SourceReader, 'read', side_effect=AssertionError('Snapshot metadata must not read values')):
                async with self.client(create_app(host, ui=False)) as client:
                    response = await client.get('/api/v2/execution-trace', params={'invocation_id': first.invocation_id})
                    self.assertEqual(response.status_code, 200, response.text)
                    trace = response.json()
                    self.assertEqual(trace['last_event'], dict(expected))
                    self.assertEqual(trace['last_event'], before['last_event'])
                    self.assertGreater(trace['frontier'], before['frontier'])
                    self.assertGreaterEqual(trace['observed_at_us'], before['observed_at_us'])
                    self.assertNotIn('large-', response.text)
                    self.assertLess(len(response.content), 4096)
                    later = (await client.get('/api/v2/execution-trace', params={'invocation_id': second.invocation_id})).json()
                    self.assertGreater(later['last_event']['sequence'], trace['last_event']['sequence'])
                    self.assertNotEqual(later['last_event']['event_id'], trace['last_event']['event_id'])

    async def test_exact_record_position_seeks_tied_calls_and_repeated_node_pages(self):
        """Direct seeks preserve exact IDs across pages, ties, filters and foreign executions."""
        from sqlalchemy import insert, update
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader

        def mapped(ctx: InputMappingContext) -> list[Value]:
            return [ctx.invocation_input] * 65
        def aggregated(ctx: AggregationContext) -> Value:
            return ctx.outputs[0]
        wf = Workflow('position-map', nodes=[Node('mapped', identity, input_mapping=mapped, map=Map(aggregate=aggregated))])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'large-' + 'x' * (2 * 1024 * 1024)}, entry_node_id='mapped')
            self.assertEqual((await host.join(root)).status, 'completed')
            calls = (await host.list_operator_calls(root.invocation_id, limit=100)).items
            target = sorted(calls, key=lambda row: row['call_id'])[54]
            storage = host._persistence._writer.storage
            async with self.database_connection(host, write=True) as conn:
                original = await storage.row(conn, 'node_executions', invocation_id=root.invocation_id, occurrence_id='mapped@root')
                await conn.execute(update(meta.tables['node_executions']).where(meta.tables['node_executions'].c.invocation_id == root.invocation_id).values(ready_at_us=0, started_at_us=0, completed_at_us=1000))
                await conn.execute(update(meta.tables['operator_calls']).where(meta.tables['operator_calls'].c.invocation_id == root.invocation_id).values(started_at_us=120, completed_at_us=300))
                for index in range(75):
                    await conn.execute(insert(meta.tables['node_executions']).values(dict(original,
                        occurrence_id=f'mapped@loop:{index:03}', ordinal=index + 2, ready_at_us=100 + index * 10,
                        started_at_us=100 + index * 10, completed_at_us=108 + index * 10,
                        metrics_ref=None, input_ref=None, output_ref=None)))
            async with self.client(create_app(host, ui=False)) as client:
                with patch.object(SourceReader, 'read', side_effect=AssertionError('Position must not read values')):
                    common = dict(invocation_id=root.invocation_id, start_us=0, end_us=1001)
                    request = dict(common, kind='call', identity=target['call_id'], occurrence_id='mapped@root', limit=20)
                    response = await client.get('/api/v2/timeline-position', params=request)
                    self.assertEqual(response.status_code, 200, response.text)
                    position = response.json()
                    self.assertEqual(position['page_index'], 2)
                    page = (await client.get('/api/v2/timeline-items', params=dict(common, kind='call', occurrence_id='mapped@root', limit=20, cursor=position['cursor']))).json()
                    self.assertIn(target['call_id'], [row['id'] for row in page['items']])
                    earlier = (await client.get('/api/v2/timeline-position', params=dict(request, page_index=1))).json()
                    previous = (await client.get('/api/v2/timeline-items', params=dict(common, kind='call', occurrence_id='mapped@root', limit=20, cursor=earlier['cursor']))).json()
                    self.assertFalse(set(row['id'] for row in previous['items']) & set(row['id'] for row in page['items']))
                    node_id = 'mapped@loop:054'
                    node_position = (await client.get('/api/v2/timeline-position', params=dict(common, kind='node', identity=node_id, limit=50))).json()
                    self.assertEqual(node_position['page_index'], 1)
                    node_page = (await client.get('/api/v2/timeline-items', params=dict(common, kind='node', limit=50, cursor=node_position['cursor']))).json()
                    self.assertIn(node_id, [row['id'] for row in node_page['items']])
                    call = (await client.get('/api/v2/execution-record', params=dict(invocation_id=root.invocation_id, kind='call', identity=target['call_id']))).json()
                    self.assertEqual(call['node_id'], 'mapped')
                    node = (await client.get('/api/v2/execution-record', params=dict(invocation_id=root.invocation_id, kind='node', identity=node_id))).json()
                    self.assertEqual(node['ordinal'], 56)
                    self.assertEqual((await client.get('/api/v2/timeline-position', params=dict(request, occurrence_id=node_id))).status_code, 404)
                    self.assertEqual((await client.get('/api/v2/timeline-position', params=dict(request, end_us=119))).status_code, 404)
                    self.assertEqual((await client.get('/api/v2/timeline-position', params=dict(request, invocation_id='foreign'))).status_code, 404)
                    self.assertEqual((await client.get('/api/v2/timeline-position', params=dict(request, page_index=9007199254740991))).status_code, 400)
                    invalid_cursor = await client.get('/api/v2/timeline-items', params=dict(common, kind='call', occurrence_id=node_id, limit=20, cursor=position['cursor']))
                    self.assertEqual(invalid_cursor.status_code, 400)

    async def test_timeline_windows_page_metadata_and_preserve_exact_parallel_call_times(self):
        """Windowed timing pages avoid business values and preserve tied parallel Calls."""
        from sqlalchemy import event, update
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader

        def mapped(ctx: InputMappingContext) -> list[Value]:
            return [ctx.invocation_input] * 3

        def aggregated(ctx: AggregationContext) -> Value:
            return ctx.outputs[0]

        wf = Workflow('timeline-map', nodes=[Node('mapped', identity, input_mapping=mapped, map=Map(aggregate=aggregated))])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'x' * (2 * 1024 * 1024)}, entry_node_id='mapped')
            await host.join(root)
            node = (await host.list_node_executions(root.invocation_id, 'mapped')).items[0]
            calls = (await host.list_operator_calls(root.invocation_id, occurrence_id=node['occurrence_id'])).items
            storage = host._persistence._writer.storage
            async with self.database_connection(host, write=True) as conn:
                await conn.execute(update(meta.tables['invocations']).where(meta.tables['invocations'].c.invocation_id == root.invocation_id).values(started_at_us=100, completed_at_us=400))
                await conn.execute(update(meta.tables['node_executions']).where(meta.tables['node_executions'].c.invocation_id == root.invocation_id).values(ready_at_us=100, started_at_us=110, completed_at_us=300))
                for call, start, end in zip(calls, (120, 120, 140), (250, 220, 280)):
                    await conn.execute(update(meta.tables['operator_calls']).where(meta.tables['operator_calls'].c.call_id == call['call_id']).values(started_at_us=start, completed_at_us=end, queue_duration_ns=123000, execution_duration_ns=456000))
            statements = []
            def inspect_sql(conn, cursor, statement, params, context, many):
                statements.append(statement)
            event.listen(storage.engine.sync_engine, 'before_cursor_execute', inspect_sql)
            try:
                async with self.client(create_app(host, ui=False)) as client, asyncio.timeout(10):
                    with patch.object(SourceReader, 'read', side_effect=AssertionError('Timeline must not read values')):
                        overview = (await client.get('/api/v2/execution-timeline', params={'invocation_id': root.invocation_id})).json()
                        self.assertEqual((overview['start_at_us'], overview['end_at_us']), (100, 400))
                        self.assertEqual(overview['counts'], {'node': 1, 'call': 3, 'wait': 0})
                        self.assertEqual(len(overview['node_start_bins']), 64)
                        self.assertEqual(sum(overview['node_start_bins']), 1)
                        params = dict(invocation_id=root.invocation_id, start_us=150, end_us=200)
                        response = await client.get('/api/v2/timeline-items', params=params)
                        self.assertEqual(response.status_code, 200, response.text)
                        item = response.json()['items'][0]
                        self.assertEqual((item['start_at_us'], item['end_at_us']), (110, 300))
                        self.assertEqual(item['call_count'], 3)
                        self.assertEqual(len(item['preview_calls']), 3)
                        self.assertNotIn('input_ref', item)
                        self.assertNotIn('value_keys', item)
                        self.assertLess(len(response.content), 2000)
                        cp = dict(params, kind='call', occurrence_id=node['occurrence_id'], limit=1)
                        rows, cursor = [], None
                        while True:
                            response = await client.get('/api/v2/timeline-items', params=dict(cp, **({'cursor': cursor} if cursor else {})))
                            self.assertEqual(response.status_code, 200, response.text)
                            page = response.json()
                            rows.extend(page['items'])
                            cursor = page['next_cursor']
                            if cursor is None:
                                break
                        self.assertEqual(len({r['id'] for r in rows}), 3)
                        self.assertEqual(sorted((r['start_at_us'], r['end_at_us']) for r in rows), [(120, 220), (120, 250), (140, 280)])
                        self.assertTrue(all(r['queue_duration_ns'] == 123000 and r['execution_duration_ns'] == 456000 for r in rows))
                        self.assertTrue(all(r['wait'] is None for r in rows))
                        # Half-open windows omit spans ending at the left edge.
                        empty = (await client.get('/api/v2/timeline-items', params=dict(params, start_us=300, end_us=400))).json()
                        self.assertEqual(empty['items'], [])
                    self.assertFalse(any('payload_text' in sql or 'delta_text' in sql for sql in statements))
                    selected = await client.get('/api/v2/execution-record', params=dict(invocation_id=root.invocation_id, kind='node', identity=node['occurrence_id']))
                    self.assertEqual(selected.status_code, 200, selected.text)
                    self.assertIn('input', selected.json()['values'])
                    invalid = await client.get('/api/v2/execution-record', params=dict(invocation_id='other', kind='node', identity=node['occurrence_id']))
                    self.assertEqual(invalid.status_code, 404)
                    for args, status in ((dict(start_us=200, end_us=150), 400), (dict(start_us=-1), 422), (dict(kind='call'), 400), (dict(invocation_id='missing'), 404)):
                        self.assertEqual((await client.get('/api/v2/timeline-items', params=dict(params, **args))).status_code, status)
            finally:
                event.remove(storage.engine.sync_engine, 'before_cursor_execute', inspect_sql)

    async def test_timeline_loop_pagination_keeps_ordinals_and_scopes_cursors_to_window(self):
        """Large Loop pages preserve every Occurrence and reject cursor reuse in another window."""
        def step(value: Value) -> Value:
            return {'text': value['text'][1:]}
        def again(ctx: ConditionContext) -> bool:
            return bool(ctx.output['text'])
        def finish(ctx: ConditionContext) -> bool:
            return not ctx.output['text']
        wf = Workflow('timeline-loop', nodes=[Node('entry', identity), Node('step', step), Node('done', identity)], edges=[Edge('entry', 'step'), Edge('step', 'step', again), Edge('step', 'done', finish)])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'x' * 55}, entry_node_id='entry')
            await host.join(root)
            overview = await host.get_execution_timeline(root.invocation_id)
            self.assertEqual(overview['counts']['node'], 57)
            self.assertEqual(sum(overview['node_start_bins']), 57)
            async with self.client(create_app(host, ui=False)) as client:
                params = dict(invocation_id=root.invocation_id, start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1, limit=50)
                response = await client.get('/api/v2/timeline-items', params=params)
                self.assertEqual(response.status_code, 200, response.text)
                first = response.json()
                self.assertEqual(len(first['items']), 50)
                last = (await client.get('/api/v2/timeline-items', params=dict(params, cursor=first['next_cursor']))).json()
                self.assertEqual(len(last['items']), 7)
                self.assertIsNone(last['next_cursor'])
                rows = first['items'] + last['items']
                self.assertEqual(len({r['id'] for r in rows}), 57)
                self.assertEqual([r['ordinal'] for r in rows if r['node_id'] == 'step'], list(range(1, 56)))
                wrong = await client.get('/api/v2/timeline-items', params=dict(params, start_us=params['start_us'] + 1, cursor=first['next_cursor']))
                self.assertEqual(wrong.status_code, 400)

    async def test_timeline_call_previews_are_bounded_and_follow_the_visible_window(self):
        """Collapsed previews remain bounded while expanded pages retain every visible Call."""
        from sqlalchemy import update
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader

        def mapped(ctx: InputMappingContext) -> list[Value]:
            return [ctx.invocation_input] * 9

        def aggregated(ctx: AggregationContext) -> Value:
            return ctx.outputs[0]

        wf = Workflow('preview-map', nodes=[Node('mapped', identity, input_mapping=mapped, map=Map(aggregate=aggregated))])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'preview'}, entry_node_id='mapped')
            await host.join(root)
            node = (await host.list_node_executions(root.invocation_id, 'mapped')).items[0]
            calls = (await host.list_operator_calls(root.invocation_id, occurrence_id=node['occurrence_id'])).items
            async with self.database_connection(host, write=True) as conn:
                n, c = meta.tables['node_executions'], meta.tables['operator_calls']
                await conn.execute(update(n).where(n.c.invocation_id == root.invocation_id).values(ready_at_us=100, started_at_us=100, completed_at_us=1100))
                for index, call in enumerate(calls):
                    await conn.execute(update(c).where(c.c.invocation_id == root.invocation_id, c.c.call_id == call['call_id']).values(started_at_us=110 + index * 100, completed_at_us=190 + index * 100))
            with patch.object(SourceReader, 'read', side_effect=AssertionError('Previews must not load values')):
                page = await host.list_timeline_items(root.invocation_id, start_us=100, end_us=1101)
                preview = page.items[0]['preview_calls']
                self.assertEqual(len(preview), 6)
                self.assertEqual(page.items[0]['call_count'], 9)
                self.assertTrue(all(row['values'] == [] and 'input_ref' not in row for row in preview))
                later = await host.list_timeline_items(root.invocation_id, start_us=710, end_us=1101)
                self.assertEqual([row['start_at_us'] for row in later.items[0]['preview_calls']], [710, 810, 910])
                cursor, found = None, []
                while True:
                    page = await host.list_timeline_items(root.invocation_id, kind='call', occurrence_id=node['occurrence_id'], start_us=100, end_us=1101, limit=2, cursor=cursor)
                    found.extend(page.items)
                    cursor = page.next_cursor
                    if cursor is None:
                        break
                self.assertEqual(len({row['id'] for row in found}), 9)

    async def test_timeline_wait_pause_resume_cancel_and_skipped_nodes_use_real_boundaries(self):
        """Waits stay inside their Calls; skipping filters pages without deleting Graph history."""
        from autoagent import Wait
        from autoagent.host.persistence.reading import SourceReader
        def skip(ctx: ConditionContext) -> bool:
            return False
        wf = Workflow('timeline-wait', nodes=[Node('entry', identity), Node('approval', Wait(Value, Value)), Node('skip', identity)], edges=[Edge('entry', 'approval'), Edge('entry', 'skip', skip)])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'approve'}, entry_node_id='entry')
            await host.join(root)
            node = (await host.list_node_executions(root.invocation_id, 'approval')).items[0]
            wait = (await host.list_execution_waits(root.invocation_id, occurrence_id=node['occurrence_id'])).items[0]
            await host.pause(root)
            overview = await host.get_execution_timeline(root.invocation_id)
            self.assertEqual(overview['control_state'], 'paused')
            page = await host.list_timeline_items(root.invocation_id, kind='wait', occurrence_id=node['occurrence_id'], start_us=wait['created_at_us'] + 1, end_us=overview['end_at_us'] + 1)
            self.assertEqual(page.items[0]['start_at_us'], wait['created_at_us'])
            self.assertIsNone(page.items[0]['end_at_us'])
            with patch.object(SourceReader, 'read', side_effect=AssertionError('Nested timing must not load values')):
                calls = await host.list_timeline_items(root.invocation_id, kind='call', occurrence_id=node['occurrence_id'], start_us=wait['created_at_us'] + 1, end_us=overview['end_at_us'] + 1)
            nested = calls.items[0]['wait']
            self.assertEqual(nested['id'], calls.items[0]['call_id'])
            self.assertEqual(nested['start_at_us'], wait['created_at_us'])
            self.assertEqual(nested['status'], 'waiting')
            self.assertIsNone(nested['end_at_us'])
            self.assertEqual(nested['values'], [])
            self.assertNotIn('request_ref', nested)
            await host.recover(root)
            await host.respond(root, wait['wait_id'], {'text': 'yes'})
            await host.join(root)
            finished = (await host.list_execution_waits(root.invocation_id, occurrence_id=node['occurrence_id'])).items[0]
            overview = await host.get_execution_timeline(root.invocation_id)
            page = await host.list_timeline_items(root.invocation_id, kind='wait', occurrence_id=node['occurrence_id'], start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1)
            self.assertEqual(page.items[0]['end_at_us'], finished['resumed_at_us'])
            self.assertEqual(overview['counts']['node'], 2)
            self.assertEqual(sum(overview['node_start_bins']), 2)
            params = dict(start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1)
            cursor, rows = None, []
            while True:
                nodes = await host.list_timeline_items(root.invocation_id, limit=1, cursor=cursor, **params)
                self.assertEqual(len(nodes.items), 1)
                rows.extend(nodes.items)
                cursor = nodes.next_cursor
                if cursor is None:
                    break
            self.assertEqual({row['node_id'] for row in rows}, {'entry', 'approval'})
            self.assertEqual((await host.list_timeline_items(root.invocation_id, node_id='skip', **params)).items, ())
            skipped = (await host.list_node_executions(root.invocation_id, 'skip')).items[0]
            self.assertEqual(skipped['status'], 'skipped')
            self.assertEqual(next(row for row in (await host.get_execution_trace(root.invocation_id))['nodes'] if row['node_id'] == 'skip')['status'], 'skipped')
            markers = await host.list_timeline_markers(root.invocation_id, **params)
            # Skipping is recorded as part of the routing boundary, not a
            # separate Node-skipped Runtime Event.
            self.assertIn('routing.resolved', {row['event_name'] for row in markers.items})
            calls = await host.list_timeline_items(root.invocation_id, kind='call', occurrence_id=node['occurrence_id'], **params)
            self.assertEqual(calls.items[0]['wait'], page.items[0])
            nodes = await host.list_timeline_items(root.invocation_id, **params)
            embedded = next(row for row in nodes.items if row['node_id'] == 'approval')['preview_calls'][0]
            self.assertEqual(embedded['wait'], page.items[0])
            self.assertLessEqual(calls.items[0]['start_at_us'], page.items[0]['start_at_us'])
            self.assertGreaterEqual(calls.items[0]['end_at_us'], page.items[0]['end_at_us'])
            other = await host.execute(wf.id, {'text': 'cancel'}, entry_node_id='entry')
            await host.join(other)
            oid = (await host.list_node_executions(other.invocation_id, 'approval')).items[0]['occurrence_id']
            await host.cancel(other)
            overview = await host.get_execution_timeline(other.invocation_id)
            cancelled = (await host.list_timeline_items(other.invocation_id, kind='wait', occurrence_id=oid, start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1)).items[0]
            self.assertEqual(cancelled['status'], 'cancelled')
            self.assertIsNotNone(cancelled['end_at_us'])
            self.assertLessEqual(cancelled['end_at_us'], overview['end_at_us'])
            calls = await host.list_timeline_items(other.invocation_id, kind='call', occurrence_id=oid, start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1)
            self.assertEqual(calls.items[0]['wait'], cancelled)

    async def test_timeline_timer_uses_recorded_awakening_instead_of_scheduled_deadline(self):
        """Timer blocks end at their actual awakening timestamp, including scheduling delay."""
        from autoagent import Timer
        import time
        async with self.host([Workflow('timeline-timer', nodes=[Node('timer', Timer())])]) as host:
            deadline = time.time_ns() // 1000 + 300000
            root = await host.execute('timeline-timer', {'deadline_at_us': deadline}, entry_node_id='timer')
            for _ in range(50):
                result = await host.join(root)
                if result.status == 'completed':
                    break
                await asyncio.sleep(.02)
            self.assertEqual(result.status, 'completed')
            overview = await host.get_execution_timeline(root.invocation_id)
            node = (await host.list_node_executions(root.invocation_id, 'timer')).items[0]
            wait = (await host.list_execution_waits(root.invocation_id, occurrence_id=node['occurrence_id'])).items[0]
            row = (await host.list_timeline_items(root.invocation_id, kind='wait', occurrence_id=node['occurrence_id'], start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1)).items[0]
            self.assertEqual(row['start_at_us'], wait['created_at_us'])
            self.assertEqual(row['end_at_us'], wait['resumed_at_us'])
            self.assertGreaterEqual(row['end_at_us'], deadline)
            calls = await host.list_timeline_items(root.invocation_id, kind='call', occurrence_id=node['occurrence_id'], start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1)
            self.assertEqual(calls.items[0]['wait'], row)

    async def test_timeline_replay_reconstructs_loop_states_and_routes_without_values(self):
        """Read-only frames follow Event sequence, hide future Loop runs and avoid business values."""
        from sqlalchemy import event, select
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader
        def step(value: Value) -> Value:
            return {'text': value['text'][1:]}
        def again(ctx: ConditionContext) -> bool:
            return bool(ctx.output['text'])
        def finish(ctx: ConditionContext) -> bool:
            return not ctx.output['text']
        wf = Workflow('replay-loop', nodes=[Node('entry', identity), Node('step', step), Node('done', identity)],
            edges=[Edge('entry', 'step'), Edge('step', 'step', again), Edge('step', 'done', finish)])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'xxx'}, entry_node_id='entry')
            await host.join(root)
            storage = host._persistence._writer.storage
            e = meta.tables['runtime_events']
            async with self.database_connection(host) as conn:
                headers = (await conn.execute(select(e.c.event_name, e.c.sequence, e.c.occurred_at_us)
                    .where(e.c.invocation_id == root.invocation_id).order_by(e.c.sequence))).mappings().all()
            starts = [row for row in headers if row['event_name'] == 'node_occurrence.started']
            current = await host.get_execution_trace(root.invocation_id)
            statements = []
            def inspect_sql(conn, cursor, statement, params, context, many):
                statements.append(statement)
            event.listen(storage.engine.sync_engine, 'before_cursor_execute', inspect_sql)
            try:
                with patch.object(SourceReader, 'read', side_effect=AssertionError('Replay must not resolve values')):
                    for index, header in enumerate(starts[1:-1], 1):
                        frame = await host.get_timeline_frame(root.invocation_id, sequence=header['sequence'])
                        self.assertTrue(frame['history_complete'])
                        self.assertEqual(frame['boundary']['sequence'], header['sequence'])
                        step_state = next(row for row in frame['trace']['nodes'] if row['node_id'] == 'step')
                        self.assertEqual((step_state['status'], step_state['occurrences']), ('running', index))
                        self.assertFalse(any(row['node_id'] == 'done' for row in frame['trace']['nodes']))
                        self.assertTrue(any(row['target_occurrence_id'] == step_state['occurrence_id'] for row in frame['trace']['active_routes']))
                        next_frame = await host.get_timeline_frame(root.invocation_id, sequence=header['sequence'], direction=1)
                        self.assertEqual(next_frame['previous']['sequence'], header['sequence'])
                    routing = next(row for row in headers if row['event_name'] == 'routing.resolved'
                        and starts[1]['sequence'] < row['sequence'] < starts[2]['sequence'])
                    routed = await host.get_timeline_frame(root.invocation_id, sequence=routing['sequence'])
                    evaluations = [row['evaluation'] for row in routed['trace']['edges'] if row.get('evaluation')]
                    self.assertEqual(len(evaluations), 2)
                    self.assertTrue(all(row['evaluations'] == 1 and row['sequence'] == routing['sequence'] for row in evaluations))
                    last = await host.get_timeline_frame(root.invocation_id)
                    self.assertEqual(last['trace']['nodes'], current['nodes'])
                    self.assertEqual(sorted([{k:v for k,v in row.items() if k != 'evaluation'} for row in last['trace']['edges']], key=lambda r: r['edge_id']), sorted(current['edges'], key=lambda r: r['edge_id']))
                    self.assertEqual(last['trace']['invocation']['status'], 'completed')
                    self.assertIsNone(last['next'])
                    at_time = await host.get_timeline_frame(root.invocation_id, at_us=starts[2]['occurred_at_us'])
                    self.assertEqual(at_time['boundary']['sequence'], starts[2]['sequence'])
            finally:
                event.remove(storage.engine.sync_engine, 'before_cursor_execute', inspect_sql)
            self.assertFalse(any('payload_text' in sql or 'delta_text' in sql for sql in statements))
            # Only the small Call source pointer is joined for exact start identity.
            # No business reference is selected or resolved.
            self.assertFalse(any('_ref' in sql.partition('FROM')[0] for sql in statements))

    async def test_timeline_markers_and_node_filter_have_bounded_scoped_pages(self):
        """Header and Node pages stay bounded and reject cursors from a different filter."""
        async with self.host() as host:
            root = await host.execute('catalog', {'text': 'small'}, entry_node_id='identity')
            await host.join(root)
            overview = await host.get_execution_timeline(root.invocation_id)
            async with self.client(create_app(host, ui=False)) as client:
                params = dict(invocation_id=root.invocation_id, start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1, limit=2)
                rows, cursor = [], None
                while True:
                    response = await client.get('/api/v2/timeline-markers', params=dict(params, **({'cursor': cursor} if cursor else {})))
                    self.assertEqual(response.status_code, 200, response.text)
                    page = response.json()
                    self.assertLessEqual(len(page['items']), 2)
                    self.assertTrue(all(set(row) == {'event_id', 'event_name', 'sequence', 'occurred_at_us'} for row in page['items']))
                    rows.extend(page['items'])
                    if cursor is None:
                        wrong = await client.get('/api/v2/timeline-markers', params=dict(params, start_us=params['start_us'] + 1, cursor=page['next_cursor']))
                        self.assertEqual(wrong.status_code, 400)
                    cursor = page['next_cursor']
                    if not cursor:
                        break
                self.assertEqual(len({row['event_id'] for row in rows}), len(rows))
                self.assertEqual([row['sequence'] for row in rows], sorted(row['sequence'] for row in rows))
                for node_id, count in [('identity', 1), ('missing', 0)]:
                    response = await client.get('/api/v2/timeline-items', params=dict(params, node_id=node_id))
                    self.assertEqual(len(response.json()['items']), count)
                for invalid in ({'sequence': 1, 'at_us': 1}, {'direction': 1}, {'sequence': -1}):
                    response = await client.get('/api/v2/timeline-frame', params=dict(invocation_id=root.invocation_id, **invalid))
                    self.assertIn(response.status_code, (400, 422))
                self.assertEqual((await client.get('/api/v2/timeline-frame', params={'invocation_id': 'missing'})).status_code, 404)

    async def test_timeline_replay_retains_paused_wait_and_rejects_incomplete_history(self):
        """Pause frames survive later resumption; missing InvocationStarted prevents invented replay."""
        from autoagent import Wait
        from sqlalchemy import delete
        from autoagent.host.persistence.schema import meta
        wf = Workflow('replay-wait', nodes=[Node('approval', Wait(Value, Value))])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'ask'}, entry_node_id='approval')
            await host.join(root)
            await host.pause(root)
            paused = await host.get_timeline_frame(root.invocation_id)
            self.assertEqual(paused['trace']['invocation']['control_state'], 'paused')
            node = (await host.list_node_executions(root.invocation_id, 'approval')).items[0]
            overview = await host.get_execution_timeline(root.invocation_id)
            nodes = await host.list_timeline_items(root.invocation_id, start_us=overview['start_at_us'], end_us=overview['end_at_us'] + 1)
            self.assertEqual(nodes.items[0]['wait_count'], 1)
            wait = (await host.list_execution_waits(root.invocation_id)).items[0]
            await host.recover(root)
            await host.respond(root, wait['wait_id'], {'text': 'approved'})
            await host.join(root)
            replay = await host.get_timeline_frame(root.invocation_id, sequence=paused['boundary']['sequence'])
            self.assertEqual(replay['trace']['invocation']['control_state'], 'paused')
            self.assertEqual(replay['trace']['nodes'][0]['status'], 'waiting')
            self.assertEqual(replay['trace']['nodes'][0]['occurrence_id'], node['occurrence_id'])
            storage = host._persistence._writer.storage
            async with self.database_connection(host, write=True) as conn:
                # Emulate an imported/truncated history without touching Runtime State.
                await conn.execute(delete(meta.tables['graph_changes']).where(meta.tables['graph_changes'].c.event_id == paused['first']['event_id']))
                await conn.execute(delete(meta.tables['runtime_events']).where(meta.tables['runtime_events'].c.event_id == paused['first']['event_id']))
            missing = await host.get_timeline_frame(root.invocation_id)
            self.assertFalse(missing['history_complete'])
            self.assertIsNone(missing['trace'])

    async def test_inspector_cancellation_reason_is_lazy_scoped_and_survives_reopen(self):
        """Cancelled Invocation reasons remain canonical lazy values, including Artifact-backed text."""
        from autoagent import Wait
        from autoagent.host.persistence.reading import SourceReader

        wf = Workflow('cancel-reason', nodes=[Node('approval', Wait(Value, Value))])
        reason = 'Review cancelled: ' + 'x' * (2 * 1024 * 1024)
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'request'}, entry_node_id='approval')
            await host.join(root)
            await host.cancel(root, reason=reason)
            other = await host.execute(wf.id, {'text': 'other'}, entry_node_id='approval')
            await host.join(other)
            await host.cancel(other)
            wait = (await host.list_execution_waits(root.invocation_id)).items[0]
        async with self.host([wf]) as host, self.client(create_app(host, ui=False)) as client:
            with patch.object(SourceReader, 'read', side_effect=AssertionError('Summary must not hydrate cancellation reason')):
                trace = await client.get('/api/v2/execution-trace', params={'invocation_id': root.invocation_id})
                self.assertEqual(trace.status_code, 200, trace.text)
                self.assertNotIn('Review cancelled:', trace.text)
                self.assertLess(len(trace.content), 20_000)
            params = dict(invocation_id=root.invocation_id, kind='invocation', identity=root.invocation_id, field='cancel_reason')
            response = await client.get('/api/v2/execution-value', params=params)
            self.assertEqual(response.status_code, 200, response.text[:1000])
            self.assertEqual(response.json(), {'available': True, 'value': reason})
            self.assertEqual((await client.get('/api/v2/execution-value', params=dict(params, identity=other.invocation_id))).status_code, 404)
            absent = await client.get('/api/v2/execution-value', params=dict(params, invocation_id=other.invocation_id, identity=other.invocation_id))
            self.assertEqual(absent.json(), {'available': False, 'value': None})
            wrong_kind = await client.get('/api/v2/execution-value', params=dict(params, kind='wait', identity=wait['wait_id']))
            self.assertEqual(wrong_kind.status_code, 400)

    async def test_inspector_reads_one_wait_without_resolving_values_and_preserves_cancellation(self):
        """Command child Inspectors read exact Wait metadata, with real cancellation time and lazy values."""
        from autoagent import Wait
        from autoagent.host.persistence.reading import SourceReader
        wf = Workflow('inspector-wait', nodes=[Node('approval', Wait(Value, Value))])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'large request'}, entry_node_id='approval')
            await host.join(root)
            wait = (await host.list_execution_waits(root.invocation_id)).items[0]
            async with self.client(create_app(host, ui=False)) as client:
                params = dict(invocation_id=root.invocation_id, kind='wait', identity=wait['wait_id'])
                with patch.object(SourceReader, 'read', side_effect=AssertionError('Wait summary must not read business values')):
                    response = await client.get('/api/v2/execution-record', params=params)
                    self.assertEqual(response.status_code, 200, response.text)
                    result = response.json()
                    self.assertEqual(result['wait_id'], wait['wait_id'])
                    self.assertEqual(result['values'], ['request'])
                    self.assertNotIn('request', result)
                    self.assertIsNone(result['end_at_us'])
                    self.assertEqual((await client.get('/api/v2/execution-record', params=dict(params, invocation_id='other'))).status_code, 404)
                await host.cancel(root)
                cancelled = (await client.get('/api/v2/execution-record', params=params)).json()
                self.assertEqual(cancelled['status'], 'cancelled')
                self.assertIsNotNone(cancelled['end_at_us'])
                self.assertGreaterEqual(cancelled['end_at_us'], cancelled['created_at_us'])

    async def test_inspector_edge_evaluations_are_scoped_to_the_selected_loop_occurrence(self):
        """An Edge child Inspector cannot silently display another Loop iteration's evaluation."""
        def step(value: Value) -> Value:
            return {'text': value['text'][1:]}
        def again(ctx: ConditionContext) -> bool:
            return bool(ctx.output['text'])
        def finish(ctx: ConditionContext) -> bool:
            return not ctx.output['text']
        wf = Workflow('inspector-loop', nodes=[Node('entry', identity), Node('step', step), Node('done', identity)],
            edges=[Edge('entry', 'step'), Edge('step', 'step', again), Edge('step', 'done', finish)])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'xxx'}, entry_node_id='entry')
            await host.join(root)
            nodes = (await host.list_node_executions(root.invocation_id, 'step')).items
            trace = await host.get_execution_trace(root.invocation_id)
            edge_id = trace['edges'][0]['edge_id']
            all_rows = (await host.list_edge_evaluations(root.invocation_id, edge_id)).items
            async with self.client(create_app(host, ui=False)) as client:
                for node in nodes:
                    params = dict(invocation_id=root.invocation_id, kind='edge', edge_id=edge_id, occurrence_id=node['occurrence_id'])
                    response = await client.get('/api/v2/execution-items', params=params)
                    self.assertEqual(response.status_code, 200, response.text)
                    expected = [row for row in all_rows if row['source_occurrence_id'] == node['occurrence_id']]
                    self.assertEqual(response.json()['items'], list(expected))
                    self.assertEqual(len(expected), 1)
                    selected = expected[0]
                    identity_key = f"{selected['event_id']}:{selected['result_index']}"
                    exact = await client.get('/api/v2/execution-record', params=dict(invocation_id=root.invocation_id, kind='edge', identity=identity_key))
                    self.assertEqual(exact.status_code, 200, exact.text)
                    self.assertEqual(exact.json()['source_occurrence_id'], node['occurrence_id'])
                    self.assertEqual(exact.json()['node_id'], 'step')
                    self.assertIsInstance(exact.json()['ordinal'], int)
                    self.assertEqual((await client.get('/api/v2/execution-record', params=dict(invocation_id='foreign', kind='edge', identity=identity_key))).status_code, 404)
                    self.assertEqual((await client.get('/api/v2/execution-record', params=dict(invocation_id=root.invocation_id, kind='edge', identity='invalid'))).status_code, 400)


    async def test_start_time_range_filters_invocations_and_matching_sessions(self):
        """Inclusive start bounds retain whole-Session counts and scope pagination cursors."""
        from sqlalchemy import update
        from autoagent.host.persistence.schema import meta
        async with self.host() as host:
            runs = []
            for sid in ('shared', 'shared', 'shared', 'other'):
                call = await host.execute('catalog', {'text': sid}, session_id=sid, entry_node_id='identity')
                runs.append(await host.join(call))
            storage = host._persistence._writer.storage
            async with self.database_connection(host, write=True) as conn:
                for run, stamp in zip(runs, (100, 200, 300, 400)):
                    await conn.execute(update(meta.tables['invocations']).where(meta.tables['invocations'].c.invocation_id == run.invocation_id).values(started_at_us=stamp))
            async with self.client(create_app(host, ui=False)) as client:
                bounds = {'started_after_us': 200, 'started_before_us': 300}
                page = (await client.get('/api/v2/execution-sessions', params=bounds)).json()
                self.assertEqual([(r['session_id'], r['invocation_count'], r['matching_count']) for r in page['items']], [('shared', 3, 2)])
                self.assertEqual(page['total_count'], 1)
                params = dict(bounds, session_id='shared', limit=1)
                response = await client.get('/api/v2/session-invocations', params=params)
                self.assertEqual(response.status_code, 200, response.text)
                first = response.json()
                second = (await client.get('/api/v2/session-invocations', params=dict(params, cursor=first['next_cursor']))).json()
                self.assertEqual([first['items'][0]['invocation_id'], second['items'][0]['invocation_id']], [runs[2].invocation_id, runs[1].invocation_id])
                self.assertIsNone(second['next_cursor'])
                wrong = await client.get('/api/v2/session-invocations', params=dict(params, started_after_us=201, cursor=first['next_cursor']))
                self.assertEqual(wrong.status_code, 400)
                for endpoint in ('execution-sessions', 'session-invocations'):
                    invalid = await client.get('/api/v2/' + endpoint, params={'session_id': 'shared', 'started_after_us': 300, 'started_before_us': 200})
                    self.assertEqual(invalid.status_code, 400)
                    invalid = await client.get('/api/v2/' + endpoint, params={'session_id': 'shared', 'started_after_us': -1})
                    self.assertEqual(invalid.status_code, 422)

    async def test_invocation_status_filters_sessions_counts_and_scoped_pages(self):
        """Status unions filter metadata pages, preserve Session totals and distinguish paused work."""
        from sqlalchemy import update
        from autoagent.host.persistence.schema import meta
        from autoagent.host.persistence.reading import SourceReader
        async with self.host() as host:
            seeded = []
            states = [('shared', 'completed', 'active'), ('shared', 'cancelled', 'active'),
                ('shared', 'failed', 'active'), ('shared', 'running', 'paused'),
                ('shared', 'waiting', 'active'), ('shared', 'settling', 'active'),
                ('running-only', 'running', 'active'), ('completed-only', 'completed', 'active'),
                ('paused-only', 'running', 'paused')]
            for sid, status, control in states:
                ref = await host.execute('catalog', {'text': sid}, session_id=sid, entry_node_id='identity')
                await host.join(ref)
                seeded.append((ref, status, control))
            async with self.database_connection(host, write=True) as conn:
                for ref, status, control in seeded:
                    await conn.execute(update(meta.tables['invocations']).where(meta.tables['invocations'].c.invocation_id == ref.invocation_id).values(status=status, control_state=control))
            async with self.client(create_app(host, ui=False)) as client:
                with patch.object(SourceReader, 'read', side_effect=AssertionError('Status filters must remain metadata-only')):
                    both = {'status': ['completed', 'cancelled']}
                    sessions = (await client.get('/api/v2/execution-sessions', params=both)).json()
                    self.assertEqual(sessions['total_count'], 2)
                    shared = next(row for row in sessions['items'] if row['session_id'] == 'shared')
                    self.assertEqual((shared['invocation_count'], shared['matching_count']), (6, 2))
                    params = dict(both, session_id='shared', limit=1)
                    first = (await client.get('/api/v2/session-invocations', params=params)).json()
                    self.assertEqual(len(first['items']), 1)
                    second = (await client.get('/api/v2/session-invocations', params=dict(params, status=['cancelled', 'completed', 'completed'], cursor=first['next_cursor']))).json()
                    self.assertEqual({first['items'][0]['status'], second['items'][0]['status']}, {'completed', 'cancelled'})
                    self.assertIsNone(second['next_cursor'])
                    wrong = await client.get('/api/v2/session-invocations', params=dict(params, status=['failed'], cursor=first['next_cursor']))
                    self.assertEqual(wrong.status_code, 400)
                    session_first = (await client.get('/api/v2/execution-sessions', params=dict(both, limit=1))).json()
                    wrong = await client.get('/api/v2/execution-sessions', params=dict(status='waiting', limit=1, cursor=session_first['next_cursor']))
                    self.assertEqual(wrong.status_code, 400)
                    running = (await client.get('/api/v2/execution-sessions', params={'status':'running'})).json()
                    self.assertEqual([row['session_id'] for row in running['items']], ['running-only'])
                    paused = (await client.get('/api/v2/execution-sessions', params={'status':'paused'})).json()
                    self.assertEqual({row['session_id'] for row in paused['items']}, {'shared', 'paused-only'})
                    paused_inv = (await client.get('/api/v2/session-invocations', params={'status':'paused', 'session_id':'shared'})).json()
                    self.assertEqual([row['invocation_id'] for row in paused_inv['items']], [seeded[3][0].invocation_id])
                    combined = (await client.get('/api/v2/session-invocations', params={'status':['paused','waiting'], 'session_id':'shared', 'workflow_id':'catalog'})).json()
                    self.assertEqual(len(combined['items']), 2)
                    narrowed = (await client.get('/api/v2/execution-sessions', params={'status':'paused', 'workflow_id':'missing-workflow'})).json()
                    self.assertEqual((narrowed['total_count'], narrowed['items']), (0, []))
                    cleared = (await client.get('/api/v2/session-invocations', params={'session_id':'shared'})).json()
                    self.assertEqual(len(cleared['items']), 6)
                    for endpoint in ('execution-sessions', 'session-invocations'):
                        invalid = await client.get('/api/v2/'+endpoint, params={'session_id':'shared', 'status':'unknown'})
                        self.assertEqual(invalid.status_code, 422)

    async def test_occurrence_stages_are_lazy_paged_and_scoped_to_exact_execution(self):
        """Stage summaries avoid Artifact reads; values hydrate only the selected scoped field."""
        from autoagent.host.persistence.reading import SourceReader

        def mapped(ctx: InputMappingContext) -> list[Value]:
            return [ctx.invocation_input, ctx.invocation_input]

        def aggregated(ctx: AggregationContext) -> Value:
            return ctx.outputs[0]

        def bound(ctx: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation=(ContextOperation.set('result', ctx.output),))

        def condition(ctx: ConditionContext) -> bool:
            return True

        definition = Workflow('stages', nodes=[Node('mapped', identity, input_mapping=mapped, map=Map(aggregate=aggregated), output_binding=bound), Node('finish', identity)], edges=[Edge('mapped', 'finish', id='done', condition=condition)])
        payload = {'text': 'stage-data-' + 'x' * (2 * 1024 * 1024)}
        async with self.host([definition]) as host:
            run = await host.execute('stages', payload, entry_node_id='mapped')
            result = await host.join(run)
            iid = result.invocation_id
            oid = (await host.list_node_executions(iid, 'mapped')).items[0]['occurrence_id']
            other = (await host.list_node_executions(iid, 'finish')).items[0]['occurrence_id']
            async with self.client(create_app(host, ui=False)) as client:
                params = {'invocation_id': iid, 'occurrence_id': oid, 'limit': 1}
                items, cursor = [], None
                with patch.object(SourceReader, 'read', side_effect=AssertionError('Summaries must not read stage values')):
                    while True:
                        response = await client.get('/api/v2/execution-stages', params=dict(params, **({'cursor': cursor} if cursor else {})))
                        self.assertEqual(response.status_code, 200, response.text)
                        page = response.json()
                        items.extend(page['items'])
                        cursor = page['next_cursor']
                        if cursor is None:
                            break
                self.assertEqual([r['event_name'] for r in items], ['input.mapped', 'node.aggregated', 'output.bound', 'routing.resolved'])
                self.assertEqual(len({r['event_id'] for r in items}), 4)
                node = (await host.list_node_executions(iid, 'mapped')).items[0]
                self.assertEqual(node['value_keys']['input'], items[0]['value_keys']['mapped_input'])
                self.assertEqual([r['sequence'] for r in items], sorted(r['sequence'] for r in items))
                for item in items:
                    self.assertNotIn('payload', item)
                    self.assertNotIn('output', item)
                    self.assertGreaterEqual(item['duration_ns'], 0)
                for index, field in ((0, 'mapped_input'), (1, 'output'), (2, 'patch'), (3, 'conditions')):
                    response = await client.get('/api/v2/execution-stage-value', params=dict(invocation_id=iid, occurrence_id=oid, event_id=items[index]['event_id'], field=field))
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertTrue(response.json()['available'])
                    if field == 'output':
                        self.assertEqual(response.json()['value'], payload)
                    if field == 'mapped_input':
                        self.assertEqual(response.json()['value'], [payload, payload])
                wrong = await client.get('/api/v2/execution-stage-value', params=dict(invocation_id=iid, occurrence_id=other, event_id=items[0]['event_id'], field='mapped_input'))
                self.assertEqual(wrong.status_code, 404)
                wrong = await client.get('/api/v2/execution-stage-value', params=dict(invocation_id=iid, occurrence_id=oid, event_id=items[0]['event_id'], field='output'))
                self.assertEqual(wrong.status_code, 400)
                missing = await client.get('/api/v2/execution-stages', params=dict(invocation_id=iid, occurrence_id='missing'))
                self.assertEqual(missing.status_code, 404)

    async def test_failed_input_mapping_is_a_stage_without_operator_calls(self):
        """A hook failure is visible even when no Operator Call was created."""

        def broken(ctx: InputMappingContext) -> Value:
            raise ValueError('mapping failed')

        async with self.host([Workflow('broken-stage', nodes=[Node('mapped', identity, input_mapping=broken)])]) as host:
            run = await host.execute('broken-stage', {'text': 'test'}, entry_node_id='mapped')
            result = await host.join(run)
            oid = (await host.list_node_executions(result.invocation_id, 'mapped')).items[0]['occurrence_id']
            page = await host.list_execution_stages(result.invocation_id, oid)
            self.assertEqual(len(page.items), 1)
            item = page.items[0]
            self.assertEqual((item['event_name'], item['status'], item['error_phase']), ('node.faulted', 'failed', 'input_mapping'))
            value = await host.get_execution_stage_value(result.invocation_id, oid, item['event_id'], 'error')
            self.assertTrue(value['available'])
            self.assertEqual(value['value']['type'], 'ValueError')
            self.assertEqual((await host.list_operator_calls(result.invocation_id, occurrence_id=oid)).items, ())

    async def test_session_directory_spans_workflows_revisions_without_hydrating_values(self):
        """Session searches retain full counts and per-Invocation Revision identities."""
        payload = {"text": "directory-payload-" + "x" * (2 * 1024 * 1024)}
        async with self.host([workflow("review"), workflow("build")]) as host:
            first = await host.execute("review", payload, session_id="shared-session", entry_node_id="identity")
            await host.join(first)
            second = await host.execute("build", payload, session_id="shared-session", entry_node_id="identity")
            await host.join(second)
            old = (await host.get_workflow("review"))["workflow_revision_id"]
        async with self.host([workflow("review", "2"), workflow("build")]) as host:
            third = await host.execute("review", payload, session_id="shared-session", entry_node_id="identity")
            await host.join(third)
            other = await host.execute("build", {"text":"other"}, session_id="other-session", entry_node_id="identity")
            await host.join(other)
        async with self.host([workflow("unrelated")]) as host:
            with patch.object(host._persistence._writer.storage, "source", side_effect=AssertionError("No source hydration")):
                async with self.client(create_app(host, ui=False)) as client:
                    sessions = await client.get("/api/v2/execution-sessions", params={"q":"rvw"})
                    self.assertEqual(sessions.status_code, 200, sessions.text)
                    row = sessions.json()["items"][0]
                    self.assertEqual((row["session_id"], row["invocation_count"], row["workflow_count"], row["matching_count"]), ("shared-session", 3, 2, 2))
                    self.assertGreater(row["updated_at_us"], 0)
                    prefix = old.split(":")[-1][:16]
                    revision = (await client.get("/api/v2/execution-sessions", params={"q":prefix})).json()["items"][0]
                    self.assertEqual(revision["matching_count"], 1)
                    page = await client.get("/api/v2/session-invocations", params={"session_id":"shared-session"})
                    self.assertEqual(page.status_code, 200, page.text)
                    rows = page.json()["items"]
                    self.assertEqual([r["invocation_id"] for r in rows], [third.invocation_id, second.invocation_id, first.invocation_id])
                    self.assertEqual({r["workflow_id"] for r in rows}, {"review", "build"})
                    self.assertEqual(len({r["workflow_revision_id"] for r in rows}), 3)
                    for response in (sessions, page):
                        self.assertLess(len(response.content), 8192)
                        self.assertNotIn("directory-payload-", response.text)
                        self.assertNotIn("_ref", response.text)
                    filtered = (await client.get("/api/v2/session-invocations", params={"session_id":"shared-session", "revision_id":old})).json()["items"]
                    self.assertEqual([r["invocation_id"] for r in filtered], [first.invocation_id])
                    for term in ("%", "_", "\\"):
                        empty = await client.get("/api/v2/execution-sessions", params={"q":term})
                        self.assertEqual(empty.json()["items"], [])
                    by_id = (await client.get("/api/v2/execution-sessions", params={"q":second.invocation_id})).json()["items"][0]
                    self.assertEqual(by_id["matching_count"], 1)
                    pinned = (await client.get("/api/v2/execution-sessions", params={"q":"no-match", "session_id":"shared-session"})).json()["items"][0]
                    self.assertEqual((pinned["invocation_count"], pinned["matching_count"]), (3, 0))
                    self.assertEqual(host._runtime.resident, {})

    async def test_execution_filters_combine_selections_and_preserve_session_context(self):
        """Multi-select filters intersect search terms, page consistently, and avoid value hydration."""
        async with self.host([workflow("alpha"), workflow("beta"), workflow("gamma")]) as host:
            first = await host.execute("alpha", {"text": "first"}, session_id="shared-filter-session", entry_node_id="identity")
            await host.join(first)
            second = await host.execute("beta", {"text": "second"}, session_id="shared-filter-session", entry_node_id="identity")
            await host.join(second)
            third = await host.execute("gamma", {"text": "third"}, session_id="shared-filter-session", entry_node_id="identity")
            await host.join(third)
            old_alpha = (await host.get_workflow("alpha"))["workflow_revision_id"]
            old_beta = (await host.get_workflow("beta"))["workflow_revision_id"]
        async with self.host([workflow("alpha", "2"), workflow("beta"), workflow("gamma")]) as host:
            for wid, sid in (("alpha", "shared-filter-session"), ("alpha", "alpha-only"), ("beta", "beta-only")):
                item = await host.execute(wid, {"text": sid}, session_id=sid, entry_node_id="identity")
                await host.join(item)
            with patch.object(host._persistence._writer.storage, "source", side_effect=AssertionError("Metadata filters must not read values")):
                async with self.client(create_app(host, ui=False)) as client:
                    choices = [("workflow_id", "alpha"), ("workflow_id", "beta")]
                    sessions = (await client.get("/api/v2/execution-sessions", params=choices)).json()["items"]
                    self.assertEqual({row["session_id"] for row in sessions}, {"shared-filter-session", "alpha-only", "beta-only"})
                    shared = next(row for row in sessions if row["session_id"] == "shared-filter-session")
                    self.assertEqual((shared["invocation_count"], shared["workflow_count"], shared["matching_count"]), (4, 3, 3))
                    revisions = [("revision_id", old_alpha), ("revision_id", old_beta)]
                    params = choices + revisions + [("session_id", "shared-filter-session")]
                    invocations = (await client.get("/api/v2/session-invocations", params=params)).json()["items"]
                    self.assertEqual({row["invocation_id"] for row in invocations}, {first.invocation_id, second.invocation_id})
                    self.assertTrue(all("input" in row["values"] and "input_ref" not in row for row in invocations))
                    searched = (await client.get("/api/v2/session-invocations", params=params + [("q", second.invocation_id[:16])])).json()["items"]
                    self.assertEqual([row["invocation_id"] for row in searched], [second.invocation_id])
                    mixed = (await client.get("/api/v2/session-invocations", params=[("session_id", "shared-filter-session"), ("workflow_id", "gamma"), *revisions])).json()["items"]
                    self.assertEqual(mixed, [])
                    narrowed = (await client.get("/api/v2/execution-sessions", params=choices + revisions)).json()["items"]
                    self.assertEqual({row["session_id"] for row in narrowed}, {"shared-filter-session", "beta-only"})
                    pinned = (await client.get("/api/v2/execution-sessions", params=choices + revisions + [("session_id", "alpha-only")])).json()["items"][0]
                    self.assertEqual((pinned["invocation_count"], pinned["matching_count"]), (1, 0))
                    page = (await client.get("/api/v2/execution-sessions", params=choices + [("limit", "1")])).json()
                    tail = await client.get("/api/v2/execution-sessions", params=list(reversed(choices)) + [("workflow_id", "alpha"), ("limit", "1"), ("cursor", page["next_cursor"])])
                    self.assertEqual(tail.status_code, 200, tail.text)
                    self.assertNotEqual(tail.json()["items"][0]["session_id"], page["items"][0]["session_id"])
                    changed = await client.get("/api/v2/execution-sessions", params=choices + [("workflow_id", "gamma"), ("cursor", page["next_cursor"])])
                    self.assertEqual(changed.status_code, 400)
                    calls = (await client.get("/api/v2/session-invocations", params=params + [("limit", "1")])).json()
                    call_tail = await client.get("/api/v2/session-invocations", params=list(reversed(params)) + [("cursor", calls["next_cursor"])])
                    self.assertEqual(call_tail.status_code, 200, call_tail.text)
                    self.assertNotEqual(call_tail.json()["items"][0]["invocation_id"], calls["items"][0]["invocation_id"])
                    changed_revision = await client.get("/api/v2/session-invocations", params=choices + [("session_id", "shared-filter-session"), ("revision_id", old_alpha), ("cursor", calls["next_cursor"])])
                    self.assertEqual(changed_revision.status_code, 400)
                    for invalid in ([ ("workflow_id", "") ], [ ("workflow_id", "x" * 513) ], [ ("revision_id", "x" * 4097) ], [("workflow_id", f"wf-{i}") for i in range(65)]):
                        response = await client.get("/api/v2/execution-sessions", params=invalid)
                        self.assertEqual(response.status_code, 422, response.text)

    async def test_session_and_invocation_pagination_validate_search_scope(self):
        """Session and Invocation cursors reject reuse across searches or Sessions."""
        async with self.host() as host:
            results = []
            for sid in ("one", "one", "two"):
                item = await host.execute("catalog", {"text":sid}, session_id=sid, entry_node_id="identity")
                results.append(await host.join(item))
            async with self.client(create_app(host, ui=False)) as client:
                page = (await client.get("/api/v2/execution-sessions", params={"limit":1})).json()
                self.assertEqual(page["items"][0]["session_id"], "two")
                tail = (await client.get("/api/v2/execution-sessions", params={"limit":1,"cursor":page["next_cursor"]})).json()
                self.assertEqual(tail["items"][0]["session_id"], "one")
                self.assertIsNone(tail["next_cursor"])
                wrong = await client.get("/api/v2/execution-sessions", params={"q":"catalog", "cursor":page["next_cursor"]})
                self.assertEqual(wrong.status_code, 400)
                calls = (await client.get("/api/v2/session-invocations", params={"session_id":"one", "limit":1})).json()
                tail = (await client.get("/api/v2/session-invocations", params={"session_id":"one", "limit":1, "cursor":calls["next_cursor"]})).json()
                self.assertEqual([calls["items"][0]["invocation_id"], tail["items"][0]["invocation_id"]], [results[1].invocation_id, results[0].invocation_id])
                self.assertIsNone(tail["next_cursor"])
                wrong = await client.get("/api/v2/session-invocations", params={"session_id":"two", "cursor":calls["next_cursor"]})
                self.assertEqual(wrong.status_code, 400)
                self.assertEqual((await client.get("/api/v2/session-invocations", params={"session_id":"missing"})).status_code, 404)
                self.assertEqual((await client.get("/api/v2/execution-sessions?limit=0")).status_code, 422)

    async def test_graph_and_object_queries_do_not_expand_unrelated_artifacts(self):
        """Graphs omit heavy definitions and an object read hydrates only its subtree."""
        import json
        from autoagent.core.compiler.snapshot import WorkflowDefinitionSnapshot, definition_digest
        async with self.host() as host:
            record = await host.get_workflow("catalog")
            record.pop("runnable")
            node = record["definition"]["nodes"][0]
            node["large_schema"] = "x" * (2 * 1024 * 1024)
            node["map"] = {"metadata":node["large_schema"]}
            other = dict(node, id="other", large_schema="y" * (2 * 1024 * 1024))
            record["definition"]["nodes"].append(other)
            record["definition_hash"] = definition_digest(record["definition"])
            record["workflow_revision_id"] = "catalog:" + record["definition_hash"]
            await host._persistence.register_workflow(WorkflowDefinitionSnapshot.from_record(record))
            storage = host._persistence._writer.storage
            original = storage.row
            read_artifacts = []
            async def capture(conn, name, **identity):
                row = await original(conn, name, **identity)
                if name == "artifacts":
                    read_artifacts.append(json.loads(row["body"]))
                return row
            with patch.object(storage, "row", side_effect=capture), patch.object(storage, "source", side_effect=AssertionError("Full source hydration is forbidden")):
                async with self.client(create_app(host, ui=False)) as client:
                    graph = await client.get("/api/v2/workflow-graph", params={"revision_id":record["workflow_revision_id"]})
                    self.assertEqual(graph.status_code, 200, graph.text)
                    self.assertLess(len(graph.content), 4096)
                    self.assertEqual(set(graph.json()["definition"]["nodes"][0]), {"id", "executable", "mapped"})
                    self.assertTrue(graph.json()["definition"]["nodes"][0]["mapped"])
                    self.assertEqual(read_artifacts, [])
                    selected = await client.get("/api/v2/workflow-object", params={"revision_id":record["workflow_revision_id"], "kind":"node", "identity":"identity"})
                    self.assertEqual(selected.status_code, 200)
                    self.assertEqual(selected.json()["large_schema"], node["large_schema"])
                    self.assertEqual(read_artifacts, [node["large_schema"]])

    async def test_execution_overviews_omit_business_values_and_load_call_input_on_demand(self):
        """Node counters are inline while MiB inputs and outputs remain lazy."""
        from autoagent.host.persistence.reading import SourceReader
        payload = {"text":"large-value-" + "z" * (2 * 1024 * 1024)}
        async with self.host() as host:
            result = await host.execute("catalog", payload, entry_node_id="identity")
            result = await host.join(result)
            iid = result.invocation_id
            storage = host._persistence._writer.storage
            with patch.object(storage, "source", side_effect=AssertionError("Overview must not hydrate sources")):
                async with self.client(create_app(host, ui=False)) as client:
                    overview = await client.get("/api/v2/execution-trace", params={"invocation_id":iid})
                    self.assertEqual(overview.status_code, 200, overview.text)
                    self.assertEqual(overview.json()["nodes"][0]["status"], "completed")
                    with patch.object(SourceReader, 'read', side_effect=AssertionError('Counters must not hydrate Event or Artifact values')):
                        nodes = await client.get("/api/v2/execution-items", params={"invocation_id":iid,"kind":"node","node_id":"identity"})
                    node = nodes.json()["items"][0]
                    occurrence = node["occurrence_id"]
                    self.assertEqual(node['call_count'], 1)
                    self.assertEqual(node['peak_parallelism'], 1)
                    self.assertNotIn('metrics', node['values'])
                    self.assertNotIn('metrics', node['value_keys'])
                    calls = await client.get("/api/v2/execution-items", params={"invocation_id":iid,"kind":"call","occurrence_id":occurrence})
                    roots = await client.get("/api/v2/executions")
                    for response in (overview, nodes, calls, roots):
                        self.assertEqual(response.status_code, 200, response.text)
                        self.assertLess(len(response.content), 8192)
                        self.assertNotIn("large-value-", response.text)
                        self.assertNotIn("_ref", response.text)
                    call = calls.json()["items"][0]
                    self.assertIn("input", call["values"])
                    value = await client.get("/api/v2/execution-value", params={"invocation_id":iid,"kind":"call","identity":call["call_id"],"field":"input"})
                    self.assertEqual(value.json(), {"available":True,"value":payload})
                    output = await client.get("/api/v2/execution-value", params={"invocation_id":iid,"kind":"invocation","identity":iid,"field":"output"})
                    self.assertEqual(output.json(), {"available":True,"value":payload})

    async def test_execution_pagination_history_and_missing_objects(self):
        """Recent Root pages are stable and historical reads never activate Sessions."""
        async with self.host() as host:
            first = await host.execute("catalog", {"text":"one"}, session_id="shared", entry_node_id="identity")
            await host.join(first)
            second = await host.execute("catalog", {"text":"two"}, session_id="shared", entry_node_id="identity")
            await host.join(second)
            old_revision = (await host.get_workflow("catalog"))["workflow_revision_id"]
        async with self.host([workflow(version="2")]) as host:
            async with self.client(create_app(host, ui=False)) as client:
                page = (await client.get("/api/v2/executions?limit=1")).json()
                self.assertEqual(page["items"][0]["invocation_id"], second.invocation_id)
                tail = (await client.get("/api/v2/executions", params={"limit":1,"cursor":page["next_cursor"]})).json()
                self.assertEqual(tail["items"][0]["invocation_id"], first.invocation_id)
                self.assertIsNone(tail["next_cursor"])
                self.assertEqual((await client.get("/api/v2/executions", params={"q":"shared","cursor":page["next_cursor"]})).status_code, 400)
                trace = (await client.get("/api/v2/execution-trace", params={"invocation_id":first.invocation_id})).json()
                self.assertEqual(trace["invocation"]["workflow_revision_id"], old_revision)
                self.assertEqual(host._runtime.resident, {})
                self.assertEqual((await client.get("/api/v2/execution-trace?invocation_id=missing")).status_code, 404)
                self.assertEqual((await client.get("/api/v2/execution-items?invocation_id=missing&kind=child")).status_code, 404)
                self.assertEqual((await client.get("/api/v2/execution-value", params={"invocation_id":first.invocation_id,"kind":"call","identity":"missing","field":"input"})).status_code, 404)
                self.assertEqual((await client.get("/api/v2/executions?status=invalid")).status_code, 400)

    async def test_parent_child_navigation_and_values_are_scoped(self):
        """Root lists exclude children and foreign Call IDs cannot be read under a parent."""
        from autoagent import Spawn
        child = workflow("child")
        parent = Workflow("parent", nodes=[Node("spawn", Spawn(child, "identity"))])
        async with self.host([parent]) as host:
            root = await host.execute("parent", {"text":"child input"}, entry_node_id="spawn")
            await host.join(root)
            async with self.client(create_app(host, ui=False)) as client:
                roots = (await client.get("/api/v2/executions")).json()["items"]
                self.assertEqual([r["invocation_id"] for r in roots], [root.invocation_id])
                overview = (await client.get("/api/v2/execution-trace", params={"invocation_id":root.invocation_id})).json()
                self.assertEqual(overview["child_count"], 1)
                children = (await client.get("/api/v2/execution-items", params={"invocation_id":root.invocation_id,"kind":"child"})).json()["items"]
                iid = children[0]["invocation_id"]
                self.assertEqual(children[0]["parent_invocation_id"], root.invocation_id)
                child_trace = (await client.get("/api/v2/execution-trace", params={"invocation_id":iid})).json()
                self.assertEqual(child_trace["invocation"]["root_invocation_id"], root.invocation_id)
                child_sid = child_trace["invocation"]["session_id"]
                sessions = (await client.get("/api/v2/execution-sessions", params={"workflow_id":"child"})).json()["items"]
                self.assertEqual([row["session_id"] for row in sessions], [child_sid])
                history = (await client.get("/api/v2/session-invocations", params={"session_id":child_sid})).json()["items"]
                self.assertEqual([row["invocation_id"] for row in history], [iid])
                self.assertEqual(history[0]["parent_invocation_id"], root.invocation_id)
                call = (await client.get("/api/v2/execution-items", params={"invocation_id":iid,"kind":"call"})).json()["items"][0]
                foreign = await client.get("/api/v2/execution-value", params={"invocation_id":root.invocation_id,"kind":"call","identity":call["call_id"],"field":"input"})
                self.assertEqual(foreign.status_code, 404)

    async def test_execution_family_and_map_call_children_are_bounded_metadata(self):
        """Nested ancestry and Map Unit navigation use metadata, exact ownership and scoped pages."""
        from autoagent import Spawn
        from autoagent.host.persistence.reading import SourceReader
        from sqlalchemy import event

        def mapped(ctx: InputMappingContext) -> list[Value]:
            return [ctx.invocation_input] * 3

        leaf = workflow('family-leaf')
        child = Workflow('family-child', nodes=[Node('nested', Spawn(leaf, 'identity'))])
        parent = Workflow('family-parent', nodes=[Node('spawn', Spawn(child, 'nested'),
            input_mapping=mapped, map=Map())])
        async with self.host([parent]) as host:
            root = await host.execute(parent.id, {'text': 'large-' + 'x' * (2 * 1024 * 1024)}, entry_node_id='spawn')
            self.assertEqual((await host.join(root)).status, 'completed')
            children = (await host.list_child_executions(root.invocation_id)).items
            self.assertEqual(len(children), 3)
            grandchild = (await host.list_child_executions(children[0]['invocation_id'])).items[0]
            calls = (await host.list_operator_calls(root.invocation_id)).items
            foreign = (await host.list_operator_calls(children[0]['invocation_id'])).items[0]
            statements = []
            def inspect_sql(conn, cursor, statement, params, context, many):
                statements.append(statement)
            engine = host._persistence._writer.storage.engine.sync_engine
            event.listen(engine, 'before_cursor_execute', inspect_sql)
            try:
                with patch.object(SourceReader, 'read', side_effect=AssertionError('Navigation must not read values')):
                    async with self.client(create_app(host, ui=False)) as client:
                        response = await client.get('/api/v2/execution-family', params={'invocation_id': grandchild['invocation_id']})
                        self.assertEqual(response.status_code, 200, response.text)
                        family = response.json()
                        self.assertEqual([row['invocation_id'] for row in family['ancestors']], [root.invocation_id, children[0]['invocation_id']])
                        self.assertEqual(family['root']['invocation_id'], root.invocation_id)
                        self.assertTrue(family['ancestry_complete'])
                        self.assertEqual(family['current']['invocation_id'], grandchild['invocation_id'])
                        self.assertLess(len(response.content), 8000)
                        for call in calls:
                            response = await client.get('/api/v2/execution-items', params={'invocation_id': root.invocation_id, 'kind': 'child', 'call_id': call['call_id']})
                            self.assertEqual(response.status_code, 200, response.text)
                            rows = response.json()['items']
                            self.assertEqual(len(rows), 1)
                            self.assertEqual(rows[0]['unit_index'], call['unit_index'])
                            self.assertEqual(rows[0]['parent_occurrence_id'], call['occurrence_id'])
                            self.assertNotIn('large-', response.text)
                        first = await client.get('/api/v2/execution-items', params={'invocation_id': root.invocation_id, 'kind': 'child', 'limit': 1})
                        cursor = first.json()['next_cursor']
                        second = await client.get('/api/v2/execution-items', params={'invocation_id': root.invocation_id, 'kind': 'child', 'limit': 1, 'cursor': cursor})
                        self.assertNotEqual(first.json()['items'][0]['invocation_id'], second.json()['items'][0]['invocation_id'])
                        invalid = await client.get('/api/v2/execution-items', params={'invocation_id': root.invocation_id, 'kind': 'child', 'call_id': calls[0]['call_id'], 'cursor': cursor})
                        self.assertEqual(invalid.status_code, 400)
                        ordered = []
                        ascending_cursor = None
                        for _ in range(3):
                            response = await client.get('/api/v2/execution-items', params={
                                'invocation_id': root.invocation_id, 'kind': 'child', 'order': 'asc', 'limit': 1,
                                **({'cursor': ascending_cursor} if ascending_cursor else {}),
                            })
                            self.assertEqual(response.status_code, 200, response.text)
                            ordered.extend(response.json()['items'])
                            ascending_cursor = response.json()['next_cursor']
                        self.assertEqual(len({row['invocation_id'] for row in ordered}), 3)
                        self.assertTrue(all(type(row['created_sequence']) is int and row['created_sequence'] > 0 for row in ordered))
                        self.assertEqual([row['created_sequence'] for row in ordered], sorted(row['created_sequence'] for row in ordered))
                        opposite_order = await client.get('/api/v2/execution-items', params={
                            'invocation_id': root.invocation_id, 'kind': 'child', 'order': 'asc', 'cursor': cursor,
                        })
                        self.assertEqual(opposite_order.status_code, 400)
                        first_frame = (await client.get('/api/v2/timeline-frame', params={
                            'invocation_id': root.invocation_id, 'sequence': ordered[0]['created_sequence'] - 1,
                        })).json()
                        self.assertEqual(first_frame['trace']['child_count'], 0)
                        created_frame = (await client.get('/api/v2/timeline-frame', params={
                            'invocation_id': root.invocation_id, 'sequence': ordered[0]['created_sequence'],
                        })).json()
                        self.assertGreater(created_frame['trace']['child_count'], 0)
                        foreign_response = await client.get('/api/v2/execution-items', params={'invocation_id': root.invocation_id, 'kind': 'child', 'call_id': foreign['call_id']})
                        self.assertEqual(foreign_response.status_code, 404)
                        mismatch = await client.get('/api/v2/execution-items', params={'invocation_id': root.invocation_id, 'kind': 'child', 'call_id': calls[0]['call_id'], 'occurrence_id': 'missing'})
                        self.assertEqual(mismatch.json()['items'], [])
                        self.assertEqual((await client.get('/api/v2/execution-family', params={'invocation_id': 'missing'})).status_code, 404)
                        root_family = (await client.get('/api/v2/execution-family', params={'invocation_id': root.invocation_id})).json()
                        self.assertEqual(root_family['ancestors'], [])
                        self.assertEqual(root_family['root']['invocation_id'], root.invocation_id)
                ancestry_statements = [statement for statement in statements if 'ancestry' in statement]
                self.assertTrue(ancestry_statements)
                self.assertFalse(any('payload_text' in statement or 'checkpoint' in statement for statement in ancestry_statements))
            finally:
                event.remove(engine, 'before_cursor_execute', inspect_sql)

    async def test_failed_execution_error_and_call_definition_are_loaded_separately(self):
        """Error messages and Operator contracts are absent from summaries and lazy-readable."""
        from tests.test_host_persistence import fail
        wf = Workflow("failed", nodes=[Node("fail", fail)])
        async with self.host([wf]) as host:
            root = await host.execute("failed", {"blob":"failure-message-" + "e" * (2 * 1024 * 1024)}, entry_node_id="fail")
            result = await host.join(root)
            self.assertEqual(result.status, "failed")
            async with self.client(create_app(host, ui=False)) as client:
                overview = await client.get("/api/v2/execution-trace", params={"invocation_id":root.invocation_id})
                self.assertLess(len(overview.content), 4096)
                self.assertNotIn("failure-message-", overview.text)
                self.assertEqual(overview.json()["invocation"]["error_type"], "ValueError")
                row = (await client.get("/api/v2/execution-items", params={"invocation_id":root.invocation_id,"kind":"call"})).json()["items"][0]
                params = {"invocation_id":root.invocation_id,"kind":"call","identity":row["call_id"],"field":"error"}
                value = (await client.get("/api/v2/execution-value", params=params)).json()
                self.assertTrue(value["available"])
                self.assertIn("failure-message-", value["value"]["message"])
                params["field"] = "definition"
                definition = (await client.get("/api/v2/execution-value", params=params)).json()
                self.assertTrue(definition["available"])
                self.assertIn("handler", definition["value"]["definition"])

    async def test_active_routes_use_consumed_activations_without_loading_business_values(self):
        """Only the chosen input Edge is active across multiple source Nodes."""
        from autoagent.host.persistence.reading import SourceReader
        started, release = threading.Event(), threading.Event()

        async def blocked(value: Value) -> Value:
            started.set()
            await asyncio.to_thread(release.wait, 5)
            return value

        def chosen(ctx: ConditionContext) -> bool:
            return True

        def rejected(ctx: ConditionContext) -> bool:
            return False

        def input_mapping(ctx: InputMappingContext) -> Value:
            return {'text': ctx.incoming['chosen']['text']}

        wf = Workflow('active-routes', nodes=[Node('entry', identity), Node('left', identity), Node('right', identity), Node('work', blocked, input_mapping=input_mapping), Node('unused', identity)], edges=[
            Edge('entry', 'left', id='start-left'), Edge('entry', 'right', id='start-right'), Edge('left', 'work', chosen, id='chosen'), Edge('right', 'work', rejected, id='not-chosen'), Edge('right', 'unused', rejected, id='unused')])
        async with self.host([wf]) as host:
            root = await host.execute('active-routes', {'text': 'large-route-' + 'x' * (2 * 1024 * 1024)}, entry_node_id='entry')
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 3))
                with patch.object(SourceReader, 'read', side_effect=AssertionError('Active routes are metadata only')):
                    async with self.client(create_app(host, ui=False)) as client:
                        response = await client.get('/api/v2/execution-trace', params={'invocation_id': root.invocation_id})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertLess(len(response.content), 8192)
                trace = response.json()
                self.assertNotIn('large-route-', response.text)
                work = next(node for node in trace['nodes'] if node['node_id'] == 'work')
                self.assertEqual(work['status'], 'running')
                self.assertEqual({route['edge_id'] for route in trace['active_routes']}, {'chosen'})
                route = trace['active_routes'][0]
                self.assertEqual(route['target_occurrence_id'], work['occurrence_id'])
                self.assertEqual(route['target_node_id'], 'work')
                self.assertEqual(route['status'], 'running')
                self.assertEqual(route['source_occurrence_id'], next(node for node in trace['nodes'] if node['node_id'] == 'left')['occurrence_id'])
            finally:
                release.set()
                await asyncio.wait_for(host.join(root), 10)
            self.assertEqual((await host.get_execution_trace(root.invocation_id))['active_routes'], [])

    async def test_active_loop_route_belongs_to_current_occurrence(self):
        """A running second iteration animates its Loop input instead of the original entry Edge."""
        started, release = threading.Event(), threading.Event()
        calls = 0

        async def step(value: Value) -> Value:
            nonlocal calls
            calls += 1
            if calls == 2:
                started.set()
                await asyncio.to_thread(release.wait, 5)
            return {'text': value['text'][1:]}

        def again(ctx: ConditionContext) -> bool:
            return bool(ctx.output['text'])

        def finished(ctx: ConditionContext) -> bool:
            return not ctx.output['text']

        wf = Workflow('active-loop', nodes=[Node('entry', identity), Node('step', step), Node('exit', identity)], edges=[
            Edge('entry', 'step', id='enter'), Edge('step', 'step', again, id='back'), Edge('step', 'exit', finished, id='finish')])
        async with self.host([wf]) as host:
            root = await host.execute('active-loop', {'text': 'abc'}, entry_node_id='entry')
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 3))
                trace = await host.get_execution_trace(root.invocation_id)
                current = next(node for node in trace['nodes'] if node['node_id'] == 'step')
                self.assertEqual(current['occurrences'], 2)
                self.assertEqual(current['status'], 'running')
                self.assertEqual({route['edge_id'] for route in trace['active_routes']}, {'back'})
                self.assertEqual(trace['active_routes'][0]['target_occurrence_id'], current['occurrence_id'])
                self.assertNotEqual(trace['active_routes'][0]['source_occurrence_id'], current['occurrence_id'])
                self.assertTrue(next(edge for edge in trace['edges'] if edge['edge_id'] == 'enter')['condition_result'])
            finally:
                release.set()
                await asyncio.wait_for(host.join(root), 10)
            terminal = await host.get_execution_trace(root.invocation_id)
            self.assertEqual(terminal['active_routes'], [])
            self.assertTrue(next(edge for edge in terminal['edges'] if edge['edge_id'] == 'enter')['condition_result'])
            self.assertFalse(next(edge for edge in terminal['edges'] if edge['edge_id'] == 'back')['condition_result'])

    async def test_waiting_and_paused_routes_keep_underlying_occurrence_state(self):
        """Pausing a Wait changes Invocation control while preserving its real input Activation."""
        from autoagent import Wait
        wf = Workflow('waiting-route', nodes=[Node('entry', identity), Node('approval', Wait(Value, Value))], edges=[Edge('entry', 'approval', id='approve')])
        async with self.host([wf]) as host:
            root = await host.execute('waiting-route', {'text': 'approval'}, entry_node_id='entry')
            await host.join(root)
            trace = await host.get_execution_trace(root.invocation_id)
            self.assertEqual(trace['active_routes'][0]['edge_id'], 'approve')
            self.assertEqual(trace['active_routes'][0]['status'], 'waiting')
            await host.pause(root)
            paused = await host.get_execution_trace(root.invocation_id)
            self.assertEqual(paused['invocation']['control_state'], 'paused')
            self.assertEqual(paused['active_routes'], trace['active_routes'])
            await host.recover(root)
            waiting = await host.get_execution(root)
            await host.respond(root, waiting.waits[0].id, {'text': 'approved'})
            await host.join(root)
            self.assertEqual((await host.get_execution_trace(root.invocation_id))['active_routes'], [])

    async def test_wait_details_are_lazy_and_durable_view_does_not_flush_buffer(self):
        """Buffered facts stay invisible until flushed; Wait requests are fetched separately."""
        from autoagent import Wait
        from autoagent.host.persistence import PersistenceConfig
        self.host_settings = replace(self.host_settings, persistence=PersistenceConfig(mode="buffered", batch_delay_ms=10_000))
        wf = Workflow("approval", nodes=[Node("wait", Wait(Value, Value))])
        async with self.host([wf]) as host:
            root = await host.execute("approval", {"text":"approval-" + "q" * (2 * 1024 * 1024)}, entry_node_id="wait")
            result = await host.join(root)
            self.assertEqual(result.status, "waiting")
            async with self.client(create_app(host, ui=False)) as client:
                page = await client.get("/api/v2/executions")
                self.assertEqual(page.json()["items"], [])
                self.assertEqual((await client.get("/api/v2/execution-sessions")).json()["items"], [])
                self.assertEqual((await client.get("/api/v2/session-invocations", params={"session_id":root.session_id})).status_code, 404)
                self.assertEqual((await client.get("/api/v2/execution-trace", params={"invocation_id":root.invocation_id})).status_code, 404)
                await host._persistence.flush()
                trace = (await client.get("/api/v2/execution-trace", params={"invocation_id":root.invocation_id})).json()
                self.assertEqual(trace["nodes"][0]["status"], "waiting")
                row = (await client.get("/api/v2/execution-items", params={"invocation_id":root.invocation_id,"kind":"wait"})).json()["items"][0]
                self.assertIn("request", row["values"])
                self.assertNotIn("request_ref", row)
                self.assertNotIn("approval-", str(row))
                response = (await client.get("/api/v2/execution-value", params={"invocation_id":root.invocation_id,"kind":"wait","identity":row["wait_id"],"field":"request"})).json()
                self.assertTrue(response["available"])
                self.assertTrue(response["value"]["text"].startswith("approval-"))

    async def test_repeated_node_occurrences_and_edge_evaluations_are_bounded(self):
        """Loops retain distinct occurrences and latest Edge decisions with paging."""
        from autoagent import Edge
        from tests.test_host_persistence import identity as echo, shorten, again, finished
        wf = Workflow("loop", nodes=[Node("entry", echo), Node("step", shorten), Node("exit", echo)], edges=[Edge("entry", "step"), Edge("step", "step", again, id="back"), Edge("step", "exit", finished, id="exit")])
        async with self.host([wf]) as host:
            root = await host.execute("loop", {"blob":"abcd"}, entry_node_id="entry")
            await host.join(root)
            async with self.client(create_app(host, ui=False)) as client:
                trace = (await client.get("/api/v2/execution-trace", params={"invocation_id":root.invocation_id})).json()
                step = next(n for n in trace["nodes"] if n["node_id"] == "step")
                self.assertEqual(step["occurrences"], 4)
                self.assertEqual(step["status"], "completed")
                self.assertFalse(next(e for e in trace["edges"] if e["edge_id"] == "back")["condition_result"])
                params = {"invocation_id":root.invocation_id,"kind":"node","node_id":"step","limit":1}
                rows = []
                while True:
                    response = (await client.get("/api/v2/execution-items", params=params)).json()
                    rows.extend(response["items"])
                    if not response["next_cursor"]: break
                    params["cursor"] = response["next_cursor"]
                self.assertEqual(len({r["occurrence_id"] for r in rows}), 4)
                response = (await client.get("/api/v2/execution-items", params={"invocation_id":root.invocation_id,"kind":"edge","edge_id":"back","limit":1})).json()
                self.assertFalse(response["items"][0]["condition_result"])
                self.assertIsNotNone(response["next_cursor"])

    async def test_directory_paginates_current_definitions_without_loading_sessions(self):
        """Catalog pages are scoped, bounded metadata and do not load Runtime Sessions."""
        async with self.host([workflow("a"), workflow("b")]) as host:
            async with self.client(create_app(host, ui=False)) as client:
                first = (await client.get("/api/v2/workflows?limit=1")).json()
                self.assertEqual([r["workflow_id"] for r in first["items"]], ["a"])
                self.assertTrue(first["items"][0]["runnable"])
                self.assertNotIn("definition", first["items"][0])
                second = (await client.get("/api/v2/workflows", params={"limit": 1, "cursor": first["next_cursor"]})).json()
                self.assertEqual([r["workflow_id"] for r in second["items"]], ["b"])
                self.assertIsNone(second["next_cursor"])
                for params in ({"cursor": first["next_cursor"], "include_history": True}, {"cursor": first["next_cursor"], "workflow_id": "a"}):
                    self.assertEqual((await client.get("/api/v2/workflows", params=params)).status_code, 400)
                self.assertEqual(host._runtime.resident, {})

    async def test_historical_revision_is_available_without_compatible_registered_code(self):
        """The exact persisted definition remains readable after registration changes."""
        async with self.host([workflow(version="1")]) as old_host:
            old = await old_host.get_workflow("catalog")
        async with self.host([workflow(version="2")]) as host:
            async with self.client(create_app(host, ui=False)) as client:
                page = (await client.get("/api/v2/workflows?include_history=true&workflow_id=catalog&limit=1")).json()
                rows = list(page["items"])
                while page["next_cursor"]:
                    page = (await client.get("/api/v2/workflows", params={"include_history": True, "workflow_id": "catalog", "limit": 1, "cursor": page["next_cursor"]})).json()
                    rows.extend(page["items"])
                self.assertEqual(len(rows), 2)
                self.assertEqual(sum(row["runnable"] for row in rows), 1)
                response = await client.get("/api/v2/workflow-revisions/" + quote(old["workflow_revision_id"], safe=""))
                self.assertEqual(response.status_code, 200)
                self.assertFalse(response.json()["runnable"])
                self.assertEqual(response.json()["definition"], old["definition"])

    async def test_encoded_workflow_ids_and_missing_revision_errors(self):
        """Encoded slash and Unicode IDs resolve; missing revisions return 404."""
        async with self.host([workflow("目录/示例")]) as host:
            row = await host.get_workflow("目录/示例")
            async with self.client(create_app(host, ui=False)) as client:
                response = await client.get("/api/v2/workflow-revisions/" + quote(row["workflow_revision_id"], safe=""))
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["workflow_id"], "目录/示例")
                self.assertEqual((await client.get("/api/v2/workflow-revisions/catalog:missing")).status_code, 404)
                self.assertEqual((await client.get("/api/v2/workflow-revisions/malformed")).status_code, 400)

    async def test_catalog_registration_time_sort_is_stable_across_pages_and_search(self):
        """Registration times sort groups newest first with stable ties and cursor pages."""
        from sqlalchemy import case, update
        from autoagent.host.persistence.schema import meta

        async with self.host([workflow("saved-flow"), workflow("z-flow", "1")]):
            pass
        async with self.host([workflow("z-flow", "2"), workflow("a-flow"), workflow("b-flow")]) as host:
            table = meta.tables["workflow_revisions"]
            async with self.database_connection(host, write=True) as conn:
                await conn.execute(update(table).values(registered_at_us=case(
                    (table.c.workflow_id == "saved-flow", 600),
                    (table.c.workflow_id == "a-flow", 300),
                    (table.c.workflow_id == "b-flow", 200),
                    (table.c.workflow_version == "2", 300),
                    else_=100,
                )))
            async with self.client(create_app(host, ui=False)) as client:
                for search in ("", "flow"):
                    params = {"q": search, "limit": 1}
                    rows = []
                    while True:
                        response = await client.get("/api/v2/workflow-catalog", params=params)
                        self.assertEqual(response.status_code, 200, response.text)
                        page = response.json()
                        rows.extend(page["items"])
                        if not page["next_cursor"]:
                            break
                        params["cursor"] = page["next_cursor"]
                    self.assertEqual([row["workflow_id"] for row in rows], ["a-flow", "z-flow", "b-flow", "saved-flow"])
                    self.assertEqual(rows[1]["workflow_version"], "2")
                    self.assertEqual(len({row["workflow_id"] for row in rows}), 4)

    async def test_registered_catalog_filters_before_paging_and_preserves_directory_order(self):
        """Run choices exclude unregistered groups before paging and retain directory search/order."""
        async with self.host([workflow("archived-flow"), workflow("order-processing", "1")]) as host:
            old = await host.get_workflow("order-processing")
        async with self.host([workflow("order-processing", "2"), workflow("other-flow")]) as host:
            async with self.client(create_app(host, ui=False)) as client:
                directory = (await client.get("/api/v2/workflow-catalog")).json()["items"]
                params = {"registered_only": "true", "limit": 1}
                first = (await client.get("/api/v2/workflow-catalog", params=params)).json()
                rows = list(first["items"])
                page = first
                while page["next_cursor"]:
                    page = (await client.get("/api/v2/workflow-catalog", params=dict(params, cursor=page["next_cursor"]))).json()
                    rows.extend(page["items"])
                self.assertEqual([row["workflow_id"] for row in rows], [row["workflow_id"] for row in directory if row["current_revision_id"]])
                self.assertEqual(len(rows), 2)
                for search in ("ORDPROC", old["definition_hash"][:12]):
                    matches = (await client.get("/api/v2/workflow-catalog", params=dict(params, q=search))).json()["items"]
                    self.assertEqual([row["workflow_id"] for row in matches], ["order-processing"])
                    self.assertNotEqual(matches[0]["current_revision_id"], old["workflow_revision_id"])
                self.assertEqual((await client.get("/api/v2/workflow-catalog", params=dict(params, q="archived"))).json()["items"], [])
                response = await client.get("/api/v2/workflow-catalog", params={"cursor": first["next_cursor"]})
                self.assertEqual(response.status_code, 400)
                full = (await client.get("/api/v2/workflow-catalog", params={"limit": 1})).json()
                response = await client.get("/api/v2/workflow-catalog", params=dict(params, cursor=full["next_cursor"]))
                self.assertEqual(response.status_code, 400)
            page = await host._persistence.workflow_catalog(registered_ids=(), registered_only=True)
            self.assertEqual(page.items, ())
            self.assertIsNone(page.next_cursor)

    async def test_catalog_groups_revisions_and_keeps_registered_workflows_first(self):
        """Grouped pages retain current registration and include saved-only Workflows."""
        async with self.host([workflow("a-archived"), workflow("z-live", "1")]):
            pass
        async with self.host([workflow("z-live", "2"), workflow("b-live")]) as host:
            async with self.client(create_app(host, ui=False)) as client:
                page = (await client.get("/api/v2/workflow-catalog?limit=1")).json()
                rows = list(page["items"])
                first_cursor = page["next_cursor"]
                while page["next_cursor"]:
                    page = (await client.get("/api/v2/workflow-catalog", params={"limit": 1, "cursor": page["next_cursor"]})).json()
                    rows.extend(page["items"])
                self.assertEqual([row["workflow_id"] for row in rows], ["b-live", "z-live", "a-archived"])
                self.assertEqual(rows[1]["workflow_version"], "2")
                self.assertIsNotNone(rows[1]["current_revision_id"])
                self.assertIsNone(rows[2]["current_revision_id"])
                self.assertNotIn("definition", rows[1])
                self.assertEqual(host._runtime.resident, {})
                response = await client.get("/api/v2/workflow-catalog", params={"q": "live", "cursor": first_cursor})
                self.assertEqual(response.status_code, 400)

    async def test_catalog_search_matches_abbreviated_ids_and_historical_hash_prefixes(self):
        """Search covers unloaded older Revisions and treats SQL wildcards literally."""
        async with self.host([workflow("order-processing", "1"), workflow("percent%pipe")]) as host:
            old = await host.get_workflow("order-processing")
        async with self.host([workflow("order-processing", "2"), workflow("other")]) as host:
            async with self.client(create_app(host, ui=False)) as client:
                for search in ("processing", "ORDPROC", "order proc"):
                    rows = (await client.get("/api/v2/workflow-catalog", params={"q": search})).json()["items"]
                    self.assertEqual([row["workflow_id"] for row in rows], ["order-processing"])
                for search in (old["definition_hash"][:12].upper(), old["workflow_revision_id"][:len("order-processing:") + 12]):
                    rows = (await client.get("/api/v2/workflow-catalog", params={"q": search})).json()["items"]
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0]["matched_revision_id"], old["workflow_revision_id"])
                    self.assertNotEqual(rows[0]["current_revision_id"], old["workflow_revision_id"])
                rows = (await client.get("/api/v2/workflow-catalog", params={"q": "%"})).json()["items"]
                self.assertEqual([row["workflow_id"] for row in rows], ["percent%pipe"])
                self.assertEqual((await client.get("/api/v2/workflow-catalog?q=unknown")).json()["items"], [])

    async def test_workflow_revision_pages_are_newest_first_and_search_scoped(self):
        """Revision paging is scoped to Workflow and prefix without definition loading."""
        async with self.host([workflow("目录/workflow", "1")]) as host:
            old = await host.get_workflow("目录/workflow")
        async with self.host([workflow("目录/workflow", "2")]) as host:
            current = await host.get_workflow("目录/workflow")
            async with self.client(create_app(host, ui=False)) as client:
                params = {"workflow_id": "目录/workflow", "limit": 1}
                first = (await client.get("/api/v2/workflow-revisions", params=params)).json()
                self.assertEqual(first["items"][0]["revision_id"], current["workflow_revision_id"])
                self.assertNotIn("snapshot_text", first["items"][0])
                second = (await client.get("/api/v2/workflow-revisions", params=dict(params, cursor=first["next_cursor"]))).json()
                self.assertEqual(second["items"][0]["revision_id"], old["workflow_revision_id"])
                self.assertIsNone(second["next_cursor"])
                for prefix in (old["definition_hash"][:12], old["workflow_revision_id"][:len("目录/workflow:") + 12]):
                    filtered = (await client.get("/api/v2/workflow-revisions", params=dict(params, prefix=prefix))).json()
                    self.assertEqual([row["revision_id"] for row in filtered["items"]], [old["workflow_revision_id"]])
                for changes in ({"workflow_id": "other"}, {"prefix": "new"}):
                    response = await client.get("/api/v2/workflow-revisions", params=dict(params, cursor=first["next_cursor"], **changes))
                    self.assertEqual(response.status_code, 400)

    async def test_invalid_cursors_and_limits_do_not_reach_host(self):
        """Malformed pagination is rejected before a Host query is admitted."""
        async with self.host() as host:
            async with self.client(create_app(host, ui=False)) as client:
                for cursor in ("not-json", "!!!!", "e30", "W10"):
                    response = await client.get("/api/v2/workflows", params={"cursor": cursor})
                    self.assertEqual(response.status_code, 400)
                for limit in (0, 201, "bad"):
                    self.assertEqual((await client.get("/api/v2/workflows", params={"limit": limit})).status_code, 422)

    async def test_borrowed_app_does_not_close_host(self):
        """A borrowed Host survives the FastAPI application's lifespan."""
        async with self.host([workflow()]) as host:
            app = create_app(host, ui=False)
            async with app.router.lifespan_context(app):
                async with self.client(app) as client:
                    self.assertEqual((await client.get("/api/v2/workflows")).status_code, 200)
            self.assertEqual(len((await host.list_workflows()).items), 1)

    async def test_owned_project_host_uses_env_and_closes_after_lifespan(self):
        """Standalone startup loads project env and owns exactly one Host context."""
        (self.root / "workflows.py").write_text('from autoagent import Workflow, Node\nfrom autoagent.host import workflow\nfrom tests.test_server import identity\n@workflow\ndef example():\n return Workflow("example", nodes=[Node("identity", identity)])\n')
        (self.root / ".env").write_text('AUTOAGENT_SERVER_PAGE_SIZE=7\nAUTOAGENT_SERVER_PORT=8999\nAUTOAGENT_DATABASE_URL=' + self.host_settings.database_url + '\n')
        app = create_app(project=self.root, ui=False)
        host = app.state.autoagent_host
        self.assertEqual(app.state.autoagent_settings.page_size, 7)
        self.assertEqual(app.state.autoagent_settings.port, 8999)
        async with app.router.lifespan_context(app):
            async with self.client(app) as client:
                self.assertEqual((await client.get("/api/v2/workflows")).status_code, 200)
        self.assertEqual(host._state, "closed")

    async def test_embedded_router_ui_prefix_and_proxy_root_preserve_business_routes(self):
        """Embedded UI targets its API prefix including reverse-proxy root paths."""
        async with self.host([workflow()]) as host:
            settings = ServerSettings(api_prefix="/custom/api", ui_path="/tools/autoagent", ui_directory=self.ui)
            app = FastAPI(root_path="/proxy")

            @app.get("/business")
            async def business():
                return {"business": True}

            app.include_router(create_router(host, settings=settings))
            mount_ui(app, settings=settings)
            async with self.client(app) as client:
                self.assertEqual((await client.get("/proxy/business")).json(), {"business": True})
                self.assertEqual((await client.get("/proxy/custom/api/workflows")).status_code, 200)
                response = await client.get("/proxy/tools/autoagent/workflows")
                self.assertEqual(response.status_code, 200)
                self.assertIn('"apiPrefix": "/proxy/custom/api"', response.text)
                self.assertIn('<base href="/proxy/tools/autoagent/">', response.text)
                self.assertNotIn("<!--AUTOAGENT_CONFIG-->", response.text)

    async def test_ui_assets_and_api_errors_are_not_replaced_with_spa_html(self):
        """SPA fallback serves page links while missing API routes and assets stay errors."""
        async with self.host() as host:
            settings = ServerSettings(ui_directory=self.ui)
            async with self.client(create_app(host, settings=settings)) as client:
                response = await client.get("/")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers["cache-control"], "no-store")
                self.assertEqual((await client.get("/workflows/revision?workflow=catalog")).status_code, 200)
                self.assertEqual((await client.get("/assets/app.js")).text, "/* asset */")
                self.assertEqual((await client.get("/assets/missing.js")).status_code, 404)
                response = await client.get("/api/v2/missing")
                self.assertEqual(response.status_code, 404)
                self.assertIn("application/json", response.headers["content-type"])
                self.assertEqual((await client.post("/api/v2/missing", json={})).status_code, 404)
                self.assertEqual((await client.post("/api/v2/executions", json={})).status_code, 422)

    async def test_unopened_host_and_query_timeout_are_reported(self):
        """Unavailable and timed-out queries are distinct from missing definitions."""
        host = self.host()
        async with self.client(create_app(host, ui=False)) as client:
            self.assertEqual((await client.get("/api/v2/workflows")).status_code, 503)
        async with host:
            async def slow(**kwargs):
                await asyncio.sleep(10)
            settings = ServerSettings(query_timeout_seconds=0.01)
            with patch.object(host, "list_workflows", slow):
                async with self.client(create_app(host, settings=settings, ui=False)) as client:
                    self.assertEqual((await client.get("/api/v2/workflows")).status_code, 504)


class ServerSettingsTests(unittest.TestCase):
    def test_env_example_covers_every_server_setting(self):
        """Document every environment setting parsed by the Server."""
        names = {
            value for value in ServerSettings.from_environment.__func__.__code__.co_consts
            if isinstance(value, str) and value.startswith("AUTOAGENT_SERVER_")
        }
        documented = {
            line.split("=", 1)[0]
            for line in (Path(__file__).resolve().parents[1] / ".env.example").read_text().splitlines()
            if line.startswith("AUTOAGENT_SERVER_")
        }
        self.assertEqual(names, documented)

    def test_settings_resolve_env_and_validate_prefixes(self):
        """Server settings reject conflicting paths and invalid resource limits."""
        settings = ServerSettings.from_environment({"AUTOAGENT_SERVER_UI_DIRECTORY": "frontend"}, root=Path("/tmp/project"))
        self.assertEqual(settings.ui_directory, Path("/tmp/project/frontend"))
        for changes in ({"port": 0}, {"ui_path": "/api/v2/ui"}, {"api_prefix": "/api/../v2"}, {"page_size": 201}, {"query_timeout_seconds": 0},
                        {"shutdown_timeout_seconds": 0}, {"shutdown_timeout_seconds": float('inf')}, {"shutdown_timeout_seconds": True}):
            with self.assertRaises(ValueError):
                replace(settings, **changes)

    def test_missing_ui_is_reported_instead_of_silently_disabling_it(self):
        """Default API-plus-UI mode fails clearly if its assets were not built."""
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                mount_ui(FastAPI(), settings=ServerSettings(ui_directory=Path(directory)))


@unittest.skipUnless(POSTGRES_TEST_URL, "isolated PostgreSQL URL not configured")
class PostgreSQLServerTests(ServerTests):
    async def asyncSetUp(self):
        import asyncpg
        from sqlalchemy.engine import make_url

        await super().asyncSetUp()
        self.admin_url = make_url(POSTGRES_TEST_URL)
        self.database_name = "autoagent_server_" + uuid4().hex
        conn = await asyncpg.connect(self.admin_url.set(drivername="postgresql").render_as_string(hide_password=False))
        try:
            await conn.execute('CREATE DATABASE "' + self.database_name + '"')
        finally:
            await conn.close()
        self.host_settings = replace(self.host_settings, database_url=self.admin_url.set(drivername="postgresql+asyncpg", database=self.database_name).render_as_string(hide_password=False))

    async def asyncTearDown(self):
        import asyncpg

        conn = await asyncpg.connect(self.admin_url.set(drivername="postgresql").render_as_string(hide_password=False))
        try:
            await conn.execute('DROP DATABASE "' + self.database_name + '" WITH (FORCE)')
        finally:
            await conn.close()
            await super().asyncTearDown()
