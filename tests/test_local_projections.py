"""Proactive local UI projections remain optional, bounded and snapshot-correct."""

import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import select

from autoagent import Node, Spawn, Wait, Workflow
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.persistence import PersistenceConfig
from autoagent.host.persistence.schema import DETAIL_TABLES, meta
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_host import Value, identity


class LocalProjectionContracts:
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.url = await self.database_url()

    async def asyncTearDown(self):
        await self.remove_database()
        self.directory.cleanup()

    def host(self, workflows, **options):
        return AutoAgentHost(workflows=workflows, root=self.directory.name,
            settings=HostSettings(database_url=self.url, local_projection_interval_seconds=.005, **options))

    async def wait_live(self, host, predicate):
        async def read():
            service = host._local_projections
            return predicate(service, service.cache)
        async with asyncio.timeout(10):
            while not await host._persistence._writer.run(read):
                await asyncio.sleep(.005)

    async def cached(self, host, root):
        async def read():
            service = host._local_projections
            entry = service.cache.entries[service.key(root)]
            return json.loads(entry['data'])
        return await host._persistence._writer.run(read)

    async def test_events_warm_the_ui_cache_without_any_detail_query(self):
        """Events proactively build metadata while large business values stay outside the cache."""
        wf = Workflow('live-projection', nodes=[Node('work', identity)])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text': 'private-body-' + 'x' * 20000}, entry_node_id='work')
            await host.join(ref)
            await host._persistence.flush()
            await self.wait_live(host, lambda s, c: s.observed_order == host._persistence._writer.store_order
                and s.key(ref.invocation_id) in c.entries)
            data = await self.cached(host, ref.invocation_id)
            self.assertEqual(data['node_executions'][0]['status'], 'completed')
            self.assertEqual(data['operator_calls'][0]['status'], 'completed')
            self.assertNotIn('private-body-', json.dumps(data))
            async def check():
                cache = host._local_projections.cache
                before = cache.built_events
                rows = (await host._persistence._queries.list_rows('node_executions',
                    {'invocation_id': ref.invocation_id}, 100, None, 'durable')).items
                self.assertEqual(cache.built_events, before)
                return rows
            self.assertEqual(len(await host._persistence._writer.run(check)), 1)
            value = await host._persistence.node_detail(ref.invocation_id, data['node_executions'][0]['occurrence_id'])
            self.assertTrue(value['input']['value']['text'].startswith('private-body-'))
        self.assertIsNone(host._local_projections.task)

    async def test_buffered_projection_tracks_acceptance_without_publishing_future_history(self):
        """An uncommitted live generation cannot leak into durable reads or survive cache loss incorrectly."""
        wf = Workflow('live-buffered', nodes=[Node('ask', Wait(Value, Value))])
        config = PersistenceConfig(mode='buffered', batch_count=10000, batch_delay_ms=60000)
        async with self.host([wf], persistence=config) as host:
            ref = await host.execute(wf.id, {'text': 'request'}, entry_node_id='ask')
            waiting = await host.join(ref)
            await self.wait_live(host, lambda s, c: s.key(ref.invocation_id) in c.entries)
            service = host._local_projections
            before = await self.cached(host, ref.invocation_id)
            self.assertEqual(before['waits'][0]['status'], 'waiting')
            self.assertGreater(service.observed_order, host._persistence._writer.store_order)
            self.assertIsNone(await host._persistence.get_invocation(ref.invocation_id))
            accepted = await host._persistence.list_nodes(ref.invocation_id, view='accepted')
            self.assertEqual(accepted.items[0]['status'], 'waiting')
            await host._persistence.flush()
            durable = await host._persistence.list_nodes(ref.invocation_id)
            self.assertEqual(durable.items[0],
                {**dict.fromkeys(durable.items[0]), **accepted.items[0]})
            await host.respond(ref, waiting.waits[0].id, {'text': 'approved'})
            await host.join(ref)
            await self.wait_live(host, lambda s, c: s.revision is not None
                and s.revision[0] >= host._persistence._writer.accepted_ticket and any(
                b'"status":"completed"' in e['data'] for e in c.entries.values()))
            completed = await self.cached(host, ref.invocation_id)
            self.assertEqual(completed['waits'][0]['status'], 'resumed')
            self.assertEqual((await host._persistence.list_nodes(ref.invocation_id)).items[0]['status'], 'waiting')
            built = service.cache.built_events
            await host._persistence.list_nodes(ref.invocation_id)
            self.assertEqual(service.cache.built_events, built)
            await host._persistence.flush()
            hot = (await host._persistence.list_nodes(ref.invocation_id)).items
            async def clear():
                service.cache.clear()
            await host._persistence._writer.run(clear)
            cold = (await host._persistence.list_nodes(ref.invocation_id)).items
            self.assertEqual(hot, cold)

    async def test_child_family_projection_matches_cold_event_reconstruction(self):
        """A Root cache includes Child Events and produces the same detail rows as cold replay."""
        child = Workflow('live-child', nodes=[Node('work', identity)])
        parent = Workflow('live-parent', nodes=[Node('spawn', Spawn(child, 'work'))])
        config = PersistenceConfig(mode='buffered', batch_count=10000, batch_delay_ms=60000)
        async with self.host([parent], persistence=config) as host:
            ref = await host.execute(parent.id, {'text': 'child'}, entry_node_id='spawn')
            await host.join(ref)
            # Spawn ownership can outlive the Parent Operator result.
            async with asyncio.timeout(10):
                while not (await host.get_execution_state(ref.invocation_id))['settled']:
                    await asyncio.sleep(.005)
            accepted_ticket = host._persistence._writer.accepted_ticket
            await self.wait_live(host, lambda s, c: s.revision[0] >= accepted_ticket
                and s.key(ref.invocation_id) in c.entries
                and len(json.loads(c.entries[s.key(ref.invocation_id)]['data'])['invocations']) == 2
                and all(row['status'] == 'completed' for row in
                    json.loads(c.entries[s.key(ref.invocation_id)]['data'])['node_executions']))
            hot = await self.cached(host, ref.invocation_id)
            self.assertGreater(host._local_projections.observed_order, host._persistence._writer.store_order)
            await host._persistence.flush()
            self.assertEqual(len(hot['invocations']), 2)
            async def cold():
                q = host._persistence._queries
                q.projection_cache.clear()
                async with q.view(invocation_id=ref.invocation_id) as (conn, _, _, frontier):
                    return await q.projection_cache.build(conn, ref.invocation_id, frontier, host._local_projections.store_id)
            rebuilt = await host._persistence._writer.run(cold)
            for name in DETAIL_TABLES:
                self.assertCountEqual(hot[name], list(rebuilt[name].values()), name)

    async def test_evicted_live_root_rebuilds_its_prefix_on_the_next_event(self):
        """An active cache miss restores earlier Events before applying a resumed Wait."""
        wf = Workflow('live-eviction', nodes=[Node('ask', Wait(Value, Value))])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text': 'request'}, entry_node_id='ask')
            waiting = await host.join(ref)
            await self.wait_live(host, lambda s, c: s.key(ref.invocation_id) in c.entries)
            async def clear():
                host._local_projections.cache.clear()
            await host._persistence._writer.run(clear)
            await host.respond(ref, waiting.waits[0].id, {'text': 'done'})
            await host.join(ref)
            await host._persistence.flush()
            await self.wait_live(host, lambda s, c: s.key(ref.invocation_id) in c.entries
                and c.entries[s.key(ref.invocation_id)]['order'] == host._persistence._writer.store_order)
            data = await self.cached(host, ref.invocation_id)
            self.assertEqual(len(data['node_executions']), 1)
            self.assertEqual(data['node_executions'][0]['status'], 'completed')
            self.assertEqual(data['waits'][0]['status'], 'resumed')

    async def test_fork_live_projection_matches_event_reconstruction(self):
        """Fork lineage and inherited metadata remain correct in proactively warmed caches."""
        wf = Workflow('live-fork', nodes=[Node('work', identity)])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text': 'source'}, entry_node_id='work')
            await host.join(ref)
            await host._persistence.flush()
            events = (await host.list_execution_events(ref.invocation_id, names=['node_occurrence.completed'])).items
            fork = await host.fork(ref.invocation_id, sequence=events[-1]['sequence'])
            await host.join(fork['invocation_id'])
            await host._persistence.flush()
            await self.wait_live(host, lambda s, c: s.key(fork['invocation_id']) in c.entries
                and c.entries[s.key(fork['invocation_id'])]['order'] == host._persistence._writer.store_order)
            hot = await self.cached(host, fork['invocation_id'])
            async def rebuild():
                q = host._persistence._queries
                q.projection_cache.clear()
                async with q.view(invocation_id=fork['invocation_id']) as (conn, _, _, frontier):
                    return await q.projection_cache.build(conn, fork['invocation_id'], frontier, host._local_projections.store_id)
            cold = await host._persistence._writer.run(rebuild)
            for name in DETAIL_TABLES:
                self.assertCountEqual(hot[name], list(cold[name].values()), name)

    async def test_disabled_or_failed_service_does_not_affect_controls(self):
        """Workers can omit local observation, and an observer failure never breaks Wait response or persistence."""
        wf = Workflow('live-optional', nodes=[Node('ask', Wait(Value, Value))])
        async with self.host([wf], local_projections=False) as host:
            self.assertIsNone(host._local_projections)
            ref = await host.execute(wf.id, {'text': 'request'}, entry_node_id='ask')
            waiting = await host.join(ref)
            await host.respond(ref, waiting.waits[0].id, {'text': 'done'})
            self.assertEqual((await host.join(ref)).output, {'text': 'done'})
            self.assertFalse((await host._persistence.projection_cache_statistics())['local']['enabled'])
        async with self.host([wf]) as host:
            with patch.object(host._local_projections.cache, 'build', side_effect=RuntimeError('observer failed')):
                ref = await host.execute(wf.id, {'text': 'request'}, entry_node_id='ask')
                waiting = await host.join(ref)
                await self.wait_live(host, lambda s, c: s.failures > 0)
                await host.respond(ref, waiting.waits[0].id, {'text': 'done'})
                self.assertEqual((await host.join(ref)).output, {'text': 'done'})
                await host._persistence.flush()
                self.assertIsNone((await host._persistence.health()).fault)

    async def test_oversized_roots_are_deferred_instead_of_rebuilt_for_each_event(self):
        """An uncachable Root observes a bounded cooldown while Event-backed queries remain correct."""
        wf = Workflow('live-oversized', nodes=[Node('ask', Wait(Value, Value))])
        config = PersistenceConfig(projection_cache_entry_bytes=1)
        async with self.host([wf], persistence=config) as host:
            ref = await host.execute(wf.id, {'text': 'request'}, entry_node_id='ask')
            waiting = await host.join(ref)
            await self.wait_live(host, lambda s, c: ref.invocation_id in s.blocked)
            cache = host._local_projections.cache
            before = cache.built_events
            await host.respond(ref, waiting.waits[0].id, {'text': 'done'})
            await host.join(ref)
            await host._persistence.flush()
            await self.wait_live(host, lambda s, c: s.observed_order == host._persistence._writer.store_order)
            self.assertEqual(cache.built_events, before)
            self.assertLessEqual(cache.bytes, config.projection_cache_bytes)
            self.assertEqual((await host._persistence.list_nodes(ref.invocation_id)).items[0]['status'], 'completed')


class SQLiteLocalProjectionTests(LocalProjectionContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        return 'sqlite+aiosqlite:///' + str(Path(self.directory.name) / 'runtime.db')

    async def remove_database(self):
        pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL not configured')
class PostgreSQLLocalProjectionTests(LocalProjectionContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        self.database = PostgreSQLDatabase()
        return await self.database.create()

    async def remove_database(self):
        await self.database.close()


class LocalProjectionSettingsTests(unittest.TestCase):
    def test_local_observation_configuration_is_strict_and_environment_driven(self):
        """Host owns the optional observer setting without adding it to Core or SQL format settings."""
        settings = HostSettings.from_environment({'AUTOAGENT_LOCAL_PROJECTIONS': 'false',
            'AUTOAGENT_LOCAL_PROJECTION_INTERVAL_SECONDS': '.05'})
        self.assertFalse(settings.local_projections)
        self.assertEqual(settings.local_projection_interval_seconds, .05)
        for changes in ({'local_projections': 1}, {'local_projection_interval_seconds': 0},
                        {'local_projection_interval_seconds': float('nan')}):
            with self.assertRaises(ValueError):
                replace(settings, **changes)
