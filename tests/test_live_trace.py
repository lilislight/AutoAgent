"""Committed live tracing, durable cursors and bounded subscription lifetimes."""
from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import httpx
from autoagent import ConditionContext, Edge, Node, Spawn, Wait, Workflow
from autoagent.host import AutoAgentHost, HostCapacityError, HostSettings
from autoagent.host.persistence.datasets import TraceDatasets
from autoagent.host.persistence.reading import SourceReader
from autoagent.server import ServerSettings, create_app, create_router
from autoagent.server.watching import TraceWatchHub
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_server import Value, identity


class LiveTraceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.url = 'sqlite+aiosqlite:///' + str(Path(self.temp.name) / 'live.db')

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self, workflow, **settings):
        return AutoAgentHost(workflows=[workflow], settings=HostSettings(database_url=self.url, **settings), environment={})

    def client(self, host):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)), base_url='http://test')

    async def rows(self, host, manifest, section):
        rows = []
        for page in range(manifest['sections'].get(section, {}).get('pages', 0)):
            rows.extend((await host.get_trace_dataset_page(manifest['snapshot_id'], section, page))['items'])
        return rows

    async def test_released_exports_resume_by_durable_cursor_and_fetch_only_changed_occurrences(self):
        """A completed loop is not retransmitted when its final approval resumes, even after export eviction."""
        def step(value: Value) -> Value:
            return {'text': value['text'][1:]}
        def again(ctx: ConditionContext) -> bool:
            return bool(ctx.output['text'])
        def done(ctx: ConditionContext) -> bool:
            return not ctx.output['text']
        wf = Workflow('live-loop', nodes=[Node('entry', identity), Node('step', step), Node('approval', Wait(Value, Value))], edges=[Edge('entry','step'), Edge('step','step',again), Edge('step','approval',done)])
        async with self.host(wf) as host:
            root = await host.execute(wf.id, {'text':'x'*55}, entry_node_id='entry')
            await host.join(root)
            before = await host.get_trace_dataset(root.invocation_id)
            self.assertEqual(before['sections']['node']['count'], 57)
            old = {kind: await self.rows(host, before, kind) for kind in ('node','call','wait','event')}
            spool = host._persistence._datasets.entries[before['snapshot_id']]['file']
            await host.release_trace_dataset(before['snapshot_id'])
            self.assertTrue(spool.closed)
            # Replacing the export manager simulates Server/temp-cache restart.
            await host._persistence._writer.run(host._persistence._datasets.close)
            host._persistence._datasets = TraceDatasets(host._persistence._queries, host.settings.persistence, self.url)
            wait = (await host.list_execution_waits(root.invocation_id)).items[0]
            await host.respond(root, wait['wait_id'], {'text':'approved'})
            await host.join(root)
            with patch.object(SourceReader, 'read', side_effect=AssertionError('No business values')), patch('autoagent.host.persistence.datasets.TraceQueries._execution', side_effect=AssertionError('No full Graph scan')), patch('autoagent.host.persistence.datasets.TimelineQueries._overview', side_effect=AssertionError('No full Timeline scan')):
                delta = await host.get_trace_dataset(root.invocation_id, cursor=before['cursor'])
            self.assertTrue(delta['delta'])
            self.assertEqual(delta['base_cursor'], before['cursor'])
            self.assertEqual(delta['sections']['node']['count'], 1)
            fresh = await host.get_trace_dataset(root.invocation_id)
            for kind in ('node','call','wait','event'):
                key = 'event_id' if kind == 'event' else 'id'
                merged = {r[key]:r for r in old[kind]}
                merged.update({r[key]:r for r in await self.rows(host, delta, kind)})
                self.assertEqual(merged, {r[key]:r for r in await self.rows(host, fresh, kind)}, kind)
            replay = await host.get_trace_dataset(root.invocation_id, replay=True, through_sequence=before['through_sequence'])
            self.assertEqual(replay['through_sequence'], before['through_sequence'])
            self.assertTrue(replay['history_complete'])

    async def test_commit_notification_excludes_buffered_events_and_does_not_flush(self):
        """A watch wakes after the transaction commits; accepted queue data remains invisible until then."""
        from autoagent.host.persistence import PersistenceConfig
        wf = Workflow('live-buffer', nodes=[Node('approval',Wait(Value,Value))])
        config = PersistenceConfig(mode='buffered', batch_delay_ms=5000)
        async with self.host(wf, persistence=config) as host:
            # Execute schedules Core in its own loop. Wait until it reaches the
            # command boundary without join/checkpoint implicitly flushing it.
            root = await host.execute(wf.id, {'text':'ask'}, entry_node_id='approval')
            sink = host._persistence
            for _ in range(200):
                health = await sink.health()
                if health.queued_count >= 3:
                    break
                await asyncio.sleep(.005)
            self.assertGreater(health.queued_count, 0)
            with self.assertRaises(Exception) as missing:
                await host.get_trace_head(root.invocation_id)
            self.assertIn('Unknown persisted execution', str(missing.exception))
            revision = sink._writer.commit_revision
            waiting = asyncio.create_task(host.wait_trace_commit(revision, timeout=.3))
            await asyncio.sleep(.03)
            self.assertFalse(waiting.done())
            await sink.flush()
            self.assertGreater(await waiting, revision)
            head = await host.get_trace_head(root.invocation_id)
            self.assertTrue(head['active'])
            self.assertGreater(head['invocation_version'], 0)
            self.assertEqual((await sink.health()).queued_count, 0)

    async def test_subscriptions_coalesce_and_release_without_consuming_host_slots(self):
        """Slow clients retain one head each and closing the final subscription removes its producer."""
        wf = Workflow('live-watch',nodes=[Node('approval',Wait(Value,Value))])
        async with self.host(wf) as host:
            root = await host.execute(wf.id, {'text':'ask'}, entry_node_id='approval')
            await host.join(root)
            head = await host.get_trace_head(root.invocation_id)
            hub = TraceWatchHub(host, ServerSettings(trace_max_subscribers=2, trace_coalesce_ms=0))
            first = hub.subscribe(root.invocation_id, head)
            second = hub.subscribe(root.invocation_id, head)
            with self.assertRaises(HostCapacityError):
                hub.subscribe(root.invocation_id, head)
            group = hub.groups[root.invocation_id]
            for index in range(500):
                hub._publish(group, 'update', dict(head, family_version=index))
            self.assertEqual(first.qsize(),1)
            self.assertEqual(second.qsize(),1)
            self.assertEqual((await first.get())[1]['family_version'],499)
            self.assertFalse(host._operations)
            await hub.unsubscribe(root.invocation_id, first)
            self.assertEqual(hub.count,1)
            await hub.unsubscribe(root.invocation_id, second)
            self.assertEqual(hub.groups,{})
            self.assertTrue(group['task'].done())
            await hub.close()

    async def test_sse_delivers_terminal_commit_and_disconnect_cleans_up(self):
        """SSE carries metadata-only heads and finishes only after the last persisted family state."""
        wf=Workflow('live-sse',nodes=[Node('approval',Wait(Value,Value))])
        async with self.host(wf) as host:
            root=await host.execute(wf.id,{'text':'ask'},entry_node_id='approval')
            await host.join(root)
            router=create_router(host,settings=ServerSettings(trace_coalesce_ms=0))
            endpoint=next(route.endpoint for route in router.routes if route.path.endswith('/trace-watch'))
            response=await endpoint(root.invocation_id)
            iterator=response.body_iterator
            self.assertIn('retry:',await anext(iterator))
            first=await anext(iterator)
            self.assertIn('event: update',first)
            self.assertNotIn('ask',first)
            first_head=json.loads(first.split('data: ',1)[1])
            self.assertTrue(first_head['active'])
            wait=(await host.list_execution_waits(root.invocation_id)).items[0]
            await host.respond(root,wait['wait_id'],{'text':'approved'})
            await host.join(root)
            for _ in range(20):
                last=await asyncio.wait_for(anext(iterator),2)
                body=json.loads(last.split('data: ',1)[1])
                if not body['active']:
                    break
            self.assertFalse(body['active'])
            self.assertEqual(body['status'],'completed')
            self.assertGreater(body['invocation_version'],first_head['invocation_version'])
            with self.assertRaises(StopAsyncIteration):
                await anext(iterator)
            # A second subscription can be disconnected before any updates.
            response=await endpoint(root.invocation_id)
            await anext(response.body_iterator)
            await response.body_iterator.aclose()

    async def test_generation_reset_and_cursor_scope_are_explicit(self):
        """Rebuilt stores require a fresh baseline, while malformed or cross-Invocation cursors are rejected."""
        wf=Workflow('live-reset',nodes=[Node('identity',identity)])
        async with self.host(wf) as host:
            first=await host.execute(wf.id,{'text':'first'},entry_node_id='identity')
            second=await host.execute(wf.id,{'text':'second'},entry_node_id='identity')
            await host.join(first);await host.join(second)
            before=await host.get_trace_dataset(first.invocation_id)
            async with self.client(host) as client:
                wrong=await client.get('/api/v2/trace-dataset',params=dict(invocation_id=second.invocation_id,cursor=before['cursor']))
                self.assertEqual(wrong.status_code,400)
                await host._persistence.rebuild_projections()
                stale=await client.get('/api/v2/trace-dataset',params=dict(invocation_id=first.invocation_id,cursor=before['cursor']))
                self.assertEqual(stale.status_code,409,stale.text)
                self.assertEqual(stale.json()['detail']['code'],'trace_reset')

    async def test_settling_parent_keeps_observing_waiting_children_and_child_can_navigate_family(self):
        """A settling Root stays live until its Child ends, including navigation from Child back to Root."""
        child=Workflow('live-child',nodes=[Node('approval',Wait(Value,Value))])
        parent=Workflow('live-parent',nodes=[Node('spawn',Spawn(child,'approval'))])
        async with self.host(parent) as host:
            root=await host.execute(parent.id,{'text':'child approval'},entry_node_id='spawn')
            await host.join(root)
            head=await host.get_trace_head(root.invocation_id)
            self.assertEqual(head['status'],'settling')
            self.assertTrue(head['active'])
            rows=(await host.list_child_executions(root.invocation_id)).items
            self.assertEqual(len(rows),1)
            iid=rows[0]['invocation_id']
            child_head=await host.get_trace_head(iid)
            self.assertEqual(child_head['root_invocation_id'],root.invocation_id)
            self.assertTrue(child_head['active'])
            async with self.client(host) as client:
                family=(await client.get('/api/v2/execution-family',params={'invocation_id':iid})).json()
                self.assertEqual(family['root']['invocation_id'],root.invocation_id)
                self.assertEqual([r['invocation_id'] for r in family['ancestors']],[root.invocation_id])
            self.assertEqual(len((await host.list_execution_waits(iid)).items),1)
            # Host control is Root-scoped; cancellation settles the owned tree.
            await host.cancel(root)
            await host.join(root)
            final=await host.get_trace_head(root.invocation_id)
            self.assertFalse(final['active'])
            self.assertEqual(final['status'],'cancelled')
            self.assertGreater(final['invocation_version'],head['invocation_version'])
            self.assertGreater(final['family_version'],head['family_version'])
            # A planned Unit abandoned before starting has no Invocation status.
            # Such durable metadata must not keep an otherwise finished watch alive.
            from sqlalchemy import update
            from autoagent.host.persistence.schema import meta
            async def abandon():
                table=meta.tables['invocations']
                async with host._persistence._writer.storage.engine.begin() as conn:
                    await conn.execute(update(table).where(table.c.invocation_id==iid).values(status=None,ownership_phase='abandoned'))
            await host._persistence._writer.run(abandon)
            self.assertFalse((await host.get_trace_head(root.invocation_id))['active'])

    async def test_idle_exports_are_reclaimed_without_a_followup_request(self):
        """Idle pagination files expire on a timer, including after a disconnected download."""
        from autoagent.host.persistence import PersistenceConfig
        wf=Workflow('live-expire',nodes=[Node('identity',identity)])
        async with self.host(wf,persistence=PersistenceConfig(trace_snapshot_ttl_seconds=.05)) as host:
            root=await host.execute(wf.id,{'text':'once'},entry_node_id='identity');await host.join(root)
            capture=await host.get_trace_dataset(root.invocation_id)
            spool=host._persistence._datasets.entries[capture['snapshot_id']]['file']
            for _ in range(100):
                if spool.closed:break
                await asyncio.sleep(.01)
            self.assertTrue(spool.closed)


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL test database is not configured.')
class PostgreSQLLiveTraceTests(LiveTraceTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database=PostgreSQLDatabase()
        self.url=await self.database.create()

    async def asyncTearDown(self):
        await self.database.close()
        await super().asyncTearDown()
