"""Checkpoint management contract shared by SQLite and PostgreSQL."""

import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import httpx
from typing_extensions import TypedDict

from autoagent import AutoAgentApp, Await, Edge, Node, Spawn, Wait, Workflow, RuntimeTransitionError
from autoagent.host import AutoAgentHost, HostRunConflict, HostSettings
from autoagent.host.persistence.reading import SourceReader
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class Value(TypedDict):
    text: str


def identity(value: Value) -> Value:
    return value


class CheckpointManagementTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.settings = HostSettings(database_url='sqlite+aiosqlite:///' + str(Path(self.temp.name) / 'cp.db'))

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self, workflows=None, **settings):
        return AutoAgentHost(workflows=workflows or [Workflow('work', nodes=[Node('work', identity)])],
            settings=replace(self.settings, **settings), environment={})

    async def run_work(self, host, session_id='session', text='value'):
        ref = await host.execute('work', {'text': text}, session_id=session_id, entry_node_id='work')
        await host.join(ref)
        return ref

    async def test_capture_deduplicates_without_reading_large_payload(self):
        """Concurrent fresh captures reuse one graph version and can upgrade its Pin."""
        async with self.host() as host:
            ref = await self.run_work(host)
            graphs = [await host._runtime.app.acapture_checkpoint(ref) for _ in range(2)]
            self.assertNotEqual(graphs[0].to_record()['digest'], graphs[1].to_record()['digest'])
            first = await host.create_checkpoint(ref.session_id)
            with patch.object(SourceReader, 'read', side_effect=AssertionError('Snapshot body read')):
                second = await host._persistence.save_checkpoint(graphs[1], pinned=True)
                pages = await host.list_checkpoints(ref.session_id)
                info = await host.get_checkpoint(second)
                self.assertEqual(pages.total_count, 1)
                self.assertEqual(first['checkpoint_id'], second)
                self.assertTrue(info['pinned'])
                self.assertNotIn('checkpoint_text', info)
            ids = await asyncio.gather(*(host.create_checkpoint(ref.session_id) for _ in range(4)))
            self.assertEqual({row['checkpoint_id'] for row in ids}, {second})

    async def test_historical_sequence_preserves_live_state_and_current_recovery_anchor(self):
        """An old Invocation's exact Event prefix never replaces the latest Session state or anchor."""
        async with self.host() as host:
            first = await self.run_work(host, text='first')
            events = (await host._persistence.runtime_events(first.session_id, limit=100)).items
            target = next(event for event in events if event['event_name'] == 'node_occurrence.started')
            latest = await self.run_work(host, text='second')
            current = await host.create_checkpoint(latest.session_id)
            before = await host._persistence.get_session(latest.session_id)
            old = await host.create_checkpoint(first.session_id, sequence=target['sequence'], pinned=True)
            self.assertEqual(old['members'][0]['sequence'], target['sequence'])
            self.assertEqual(old['members'][0]['invocation_id'], first.invocation_id)
            self.assertNotEqual(old['checkpoint_id'], current['checkpoint_id'])
            self.assertEqual(await host._persistence.get_session(latest.session_id), before)
            bundle = await host._persistence.read_recovery_bundle(latest.invocation_id)
            self.assertEqual(bundle['checkpoint']['sessions'][0]['sequence'], before['durable_sequence'])
            self.assertEqual((await host.create_checkpoint(first.session_id, sequence=target['sequence']))['checkpoint_id'], old['checkpoint_id'])
            await host.create_checkpoint(latest.session_id)
            self.assertEqual((await host.list_checkpoints(latest.session_id)).total_count, 2)

    async def test_historical_child_position_uses_causal_graph_cut(self):
        """A Child Event selects the whole Root graph at its persisted causal position."""
        child = Workflow('history-child', nodes=[Node('wait', Wait(Value, Value))])
        parent = Workflow('history-parent', nodes=[Node('spawn', Spawn(child, 'wait')), Node('await', Await())], edges=[Edge('spawn', 'await')])
        async with self.host([parent]) as host:
            ref = await host.execute(parent.id, {'text': 'ask'}, entry_node_id='spawn')
            before = await host.join(ref)
            info = await host.create_checkpoint(ref.session_id)
            child_member = next(member for member in info['members'] if member['session_id'] != ref.session_id)
            sid = child_member['session_id']
            events = (await host._persistence.runtime_events(sid, limit=100)).items
            target = next(event for event in events if event['event_name'] == 'invocation.started')
            old = await host.create_checkpoint(sid, sequence=target['sequence'], pinned=True)
            self.assertEqual(next(m['sequence'] for m in old['members'] if m['session_id'] == sid), target['sequence'])
            self.assertEqual(old['root_session_id'], ref.session_id)
            self.assertEqual(len(old['members']), 2)
            self.assertEqual((await host.join(ref)).status, before.status)

    async def test_historical_capture_rejects_unavailable_and_pre_invocation_positions(self):
        """Invalid or unavailable positions fail explicitly instead of falling back to latest."""
        async with self.host() as host:
            ref = await self.run_work(host)
            for sequence in (0, -1, True, 1.5, 9007199254740992, 99999, 1):
                with self.subTest(sequence=sequence), self.assertRaises((ValueError, HostRunConflict)):
                    await host.create_checkpoint(ref.session_id, sequence=sequence)

    async def test_historical_new_invocation_can_start_from_previous_imported_anchor(self):
        """Imported Context anchors remain usable when the selected Event belongs to a later Invocation."""
        wf = Workflow('imported-history', nodes=[Node('work', identity)])
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(wf, {'text': 'imported'})
            graph = await app.acapture_checkpoint(result.ref)
        finally:
            await app.aclose()
        async with self.host([wf]) as host:
            await host._persistence.import_checkpoint(graph)
            ref = await host.execute(wf.id, {'text': 'new'}, session_id=result.ref.session_id, entry_node_id='work')
            await host.join(ref)
            events = (await host._persistence.runtime_events(ref.session_id, limit=100)).items
            target = next(event for event in events if event['event_name'] == 'node_occurrence.started')
            saved = await host.create_checkpoint(ref.session_id, sequence=target['sequence'], pinned=True)
            member = next(item for item in saved['members'] if item['session_id'] == ref.session_id)
            self.assertEqual(member['invocation_id'], ref.invocation_id)
            self.assertEqual(member['sequence'], target['sequence'])

    async def test_retention_keeps_latest_versions_and_pins(self):
        """Latest-N cleanup preserves Pins and leaves Event history intact."""
        async with self.host(checkpoint_keep_latest=2) as host:
            pinned = None
            for i in range(5):
                ref = await self.run_work(host, text=str(i))
                row = await host.create_checkpoint(ref.session_id, pinned=i == 0)
                if i == 0:
                    pinned = row['checkpoint_id']
            page = await host.list_checkpoints('session', limit=2)
            self.assertEqual(page.total_count, 3)
            self.assertEqual(len(page.items), 2)
            tail = await host.list_checkpoints('session', limit=2, cursor=page.next_cursor)
            self.assertEqual(tail.total_count, 3)
            self.assertEqual(len(tail.items), 1)
            self.assertTrue((await host.get_checkpoint(pinned))['pinned'])
            await host.set_checkpoint_pinned(pinned, False)
            await host.create_checkpoint('session')
            self.assertEqual((await host.list_checkpoints('session')).total_count, 2)
            self.assertEqual((await host._persistence.runtime_events('session', limit=1)).items[0]['sequence'], 1)

    async def test_default_retention_and_shutdown_do_not_duplicate(self):
        """Automatic shutdown reuses a saved version and defaults to one unpinned snapshot."""
        async with self.host() as host:
            ref = await self.run_work(host)
            await host.create_checkpoint(ref.session_id)
            await self.run_work(host, text='new')
            final = await host.create_checkpoint(ref.session_id)
            self.assertEqual((await host.list_checkpoints(ref.session_id)).total_count, 1)
        async with self.host() as reopened:
            page = await reopened.list_checkpoints(ref.session_id)
            self.assertEqual(page.total_count, 1)
            self.assertEqual(page.items[0]['checkpoint_id'], final['checkpoint_id'])
            captured = await reopened.create_checkpoint(ref.session_id)
            self.assertEqual(captured['checkpoint_id'], final['checkpoint_id'])
            self.assertEqual(reopened._runtime.resident, {})

    async def test_deleting_all_snapshots_preserves_wait_recovery(self):
        """Full Event histories can reconstruct an unloaded Wait after every snapshot is deleted."""
        wf = Workflow('approval', nodes=[Node('wait', Wait(Value, Value))])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text': 'ask'}, entry_node_id='wait')
            waiting = await host.join(ref)
            row = await host.create_checkpoint(ref.session_id, pinned=True)
        async with self.host([wf]) as host:
            await host.delete_checkpoint(row['checkpoint_id'])
            self.assertEqual((await host.list_checkpoints(ref.session_id)).total_count, 0)
            # Metadata capture reconstructs from Events without loading or running Core.
            rebuilt = await host.create_checkpoint(ref.session_id)
            self.assertEqual(rebuilt['checkpoint_id'], row['checkpoint_id'])
            self.assertEqual(host._runtime.resident, {})
            await host.delete_checkpoint(rebuilt['checkpoint_id'])
            await host.respond(ref, waiting.waits[0].id, {'text': 'approved'})
            self.assertEqual((await host.join(ref)).output, {'text': 'approved'})

    async def test_child_positions_and_sequence_vector(self):
        """Child-only progress changes identity even when the Root Sequence is unchanged."""
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        parent = Workflow('parent', nodes=[Node('spawn', Spawn(child, 'wait')), Node('await', Await())], edges=[Edge('spawn', 'await')])
        async with self.host([parent], checkpoint_keep_latest=3) as host:
            ref = await host.execute(parent.id, {'text': 'ask'}, entry_node_id='spawn')
            await host.join(ref)
            first = await host.create_checkpoint(ref.session_id)
            child_member = next(m for m in first['members'] if m['session_id'] != ref.session_id)
            page = await host.list_checkpoints(child_member['session_id'])
            self.assertEqual(page.items[0]['sequence'], child_member['sequence'])
            self.assertEqual(page.total_count, 1)
            # Exercise the version key independently of Root progress; saving
            # a new snapshot still validates its actual Event frontiers.
            from autoagent.host.persistence.checkpoints import checkpoint_key
            from autoagent import RuntimeGraphCheckpoint, SessionCheckpoint
            graph = await host._runtime.app.acapture_checkpoint(ref)
            state = next(s for s in graph.sessions if s.session_id == child_member['session_id'])
            advanced = SessionCheckpoint.from_state(replace(state.state, sequence=state.sequence + 1))
            changed = RuntimeGraphCheckpoint(graph.root_session_id, tuple(advanced if s.session_id == advanced.session_id else s for s in graph.sessions))
            self.assertNotEqual(checkpoint_key(graph), checkpoint_key(changed))
            again = await host.create_checkpoint(child_member['session_id'])
            self.assertEqual(again['checkpoint_id'], first['checkpoint_id'])

    async def test_imported_anchor_protection_is_independent_of_pin(self):
        """Unpinning an imported recovery anchor cannot bypass its system deletion dependency."""
        wf = Workflow('imported', nodes=[Node('wait', Wait(Value, Value))])
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(wf, {'text': 'ask'})
            graph = await app.acapture_checkpoint(result.ref)
        finally:
            await app.aclose()
        async with self.host([wf]) as host:
            key = await host._persistence.import_checkpoint(graph)
            row = await host.get_checkpoint(key)
            self.assertFalse(row['deletable'])
            self.assertIn('required_anchor', row['delete_blockers'])
            await host.set_checkpoint_pinned(key, True)
            await host.set_checkpoint_pinned(key, False)
            with self.assertRaises(HostRunConflict) as error:
                await host.delete_checkpoint(key)
            self.assertEqual(error.exception.code, 'checkpoint_required')
            self.assertIsNotNone(await host.get_checkpoint(key))

    async def test_imported_details_remain_protected_with_newer_anchor(self):
        """Retention keeps imported detail sources after a newer Checkpoint can anchor recovery."""
        wf = Workflow('imported', nodes=[Node('wait', Wait(Value, Value))])
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(wf, {'text': 'ask'})
            graph = await app.acapture_checkpoint(result.ref)
        finally:
            await app.aclose()
        async with self.host([wf]) as host:
            original = await host._persistence.import_checkpoint(graph)
            await host.respond(result.ref, result.waits[0].id, {'text': 'approved'})
            await host.join(result.ref)
            latest = await host.create_checkpoint(result.ref.session_id)
            self.assertNotEqual(latest['checkpoint_id'], original)
            protected = await host.get_checkpoint(original)
            self.assertIn('referenced_details', protected['delete_blockers'])
            self.assertEqual((await host.list_checkpoints(result.ref.session_id)).total_count, 2)
            with self.assertRaises(HostRunConflict):
                await host.delete_checkpoint(original)

    async def test_live_capture_does_not_pause_running_operator(self):
        """A manual snapshot rejects an unsafe live boundary without controlling the task."""
        entered, release = threading.Event(), threading.Event()
        async def work(value: Value) -> Value:
            entered.set()
            await asyncio.to_thread(release.wait)
            return value
        wf = Workflow('busy', nodes=[Node('work', work)])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text': 'busy'}, entry_node_id='work')
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 3))
                with self.assertRaises(RuntimeTransitionError):
                    await host.create_checkpoint(ref.session_id)
                self.assertEqual((await host.get_execution(ref)).control_state, 'active')
            finally:
                release.set()
            await host.join(ref)
            self.assertIsNotNone(await host.create_checkpoint(ref.session_id))

    async def test_server_checkpoint_api_and_invalid_requests(self):
        """HTTP metadata, capture, Pin and deletion use bounded queries and explicit errors."""
        async with self.host() as host:
            ref = await self.run_work(host)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)), base_url='http://test') as client:
                base = '/api/v2'
                response = await client.post(base + '/session-checkpoints', json={'session_id':ref.session_id,'pinned':True})
                self.assertEqual(response.status_code, 200, response.text)
                row = response.json()
                key = row['checkpoint_id']
                with patch.object(SourceReader, 'read', side_effect=AssertionError('No snapshot data')):
                    response = await client.get(base + '/session-checkpoints', params={'session_id':ref.session_id})
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json()['total_count'], 1)
                    response = await client.get(base + '/checkpoints/' + key)
                    self.assertEqual(response.status_code, 200)
                response = await client.put(base + '/checkpoints/' + key + '/pin', json={'pinned':False})
                self.assertFalse(response.json()['pinned'])
                response = await client.delete(base + '/checkpoints/' + key)
                self.assertEqual(response.status_code, 204)
                self.assertEqual((await client.get(base + '/checkpoints/' + key)).status_code, 404)
                self.assertEqual((await client.delete(base + '/checkpoints/' + key)).status_code, 404)
                self.assertEqual((await client.get(base + '/session-checkpoints', params={'session_id':'missing'})).status_code, 404)
                self.assertEqual((await client.get(base + '/session-checkpoints', params={'session_id':ref.session_id,'cursor':'invalid'})).status_code, 400)
                self.assertEqual((await client.post(base + '/session-checkpoints', json={'session_id':ref.session_id,'pinned':'true'})).status_code, 422)
                events = (await host._persistence.runtime_events(ref.session_id, limit=100)).items
                target = next(event for event in events if event['event_name'] == 'node_occurrence.started')
                response = await client.post(base + '/session-checkpoints', json={
                    'session_id':ref.session_id, 'sequence':target['sequence'], 'pinned':True})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['members'][0]['sequence'], target['sequence'])
                for invalid in (0, -1, True, 1.5, 9007199254740992):
                    response = await client.post(base + '/session-checkpoints', json={'session_id':ref.session_id, 'sequence':invalid})
                    self.assertEqual(response.status_code, 422)

    def test_retention_settings_require_positive_integer(self):
        """Retention is configured through environment and rejects ambiguous or unbounded counts."""
        self.assertEqual(HostSettings.from_environment({'AUTOAGENT_HOST_CHECKPOINT_KEEP_LATEST':'4'}).checkpoint_keep_latest, 4)
        for invalid in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                replace(self.settings, checkpoint_keep_latest=invalid)


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL is not configured')
class PostgreSQLCheckpointManagementTests(CheckpointManagementTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database = PostgreSQLDatabase()
        self.settings = replace(self.settings, database_url=await self.database.create())

    async def asyncTearDown(self):
        try:
            await self.database.close()
        finally:
            await super().asyncTearDown()
