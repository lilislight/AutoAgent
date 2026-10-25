"""Adaptive caching, Root APIs and unloaded graph wakeups with real SQLite."""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from tests.postgres import POSTGRES_TEST_URL

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp, AwaitSignal, InputMappingContext, Map, Node, SignalEndpoint,
    Spawn, Timer, TimerCase, SignalCase, Select, Wait, Workflow, RuntimeTransitionError,
)
from autoagent.host import AutoAgentHost, HostSettings, HostError, HostCapacityError
from autoagent.host.persistence import PersistenceConfig


class Value(TypedDict):
    text: str


def identity(value: Value) -> Value:
    return value


async def completed(host, value=None, *, session_id=None):
    return await host.join(await host.execute('work', value or {'text': 'ok'}, session_id=session_id))


async def eventually(predicate, *, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if hasattr(result, '__await__'):
            result = await result
        if result:
            return result
        await asyncio.sleep(0.01)
    raise AssertionError('Condition did not become true before timeout.')


class CoreResidencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_capture_keeps_graph_and_conditional_unload_rejects_new_progress(self):
        """A saved snapshot cannot evict a graph advanced by external approval."""
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow('wait', nodes=[Node('wait', Wait(Value, Value))]), {'text': 'ask'})
            checkpoint = await app.acapture_checkpoint(result.ref)
            self.assertEqual(await app.aresident_invocations(), (result.ref,))
            await app.aresume(result.ref, result.waits[0].id, {'text': 'answer'})
            with self.assertRaises(RuntimeTransitionError) as error:
                await app.aunload_session(result.ref, expected_version=checkpoint.version)
            self.assertEqual(error.exception.code, 'CHECKPOINT_GRAPH_CHANGED')
            self.assertEqual((await app.astatus(result.ref)).output, {'text': 'answer'})
            fresh = await app.acapture_checkpoint(result.ref)
            await app.aunload_session(result.ref, expected_version=fresh.version)
            self.assertEqual(await app.aresident_invocations(), ())
        finally:
            await app.aclose()

    async def test_capture_rejects_live_graph_and_estimate_does_not_capture(self):
        """Memory inspection does not serialize State or allow live-task eviction."""
        started, release = threading.Event(), threading.Event()
        async def slow(value: Value) -> Value:
            started.set()
            await asyncio.to_thread(release.wait, 3)
            return value
        app = AutoAgentApp()
        try:
            submission = await app.asubmit_invoke(Workflow('slow', nodes=[Node('work', slow)]), {'text': 'x' * 8192})
            self.assertTrue(await asyncio.to_thread(started.wait, 3))
            with patch.object(app, '_capture_graph_locked', side_effect=AssertionError('must not capture')):
                info = await app.aresidency_info(submission.ref)
            self.assertFalse(info.can_unload)
            self.assertGreater(info.estimated_bytes, 8192)
            with self.assertRaises(RuntimeTransitionError):
                await app.acapture_checkpoint(submission.ref)
            release.set()
            await app.ajoin(submission.ref)
        finally:
            release.set()
            await app.aclose()


class AdaptiveHostTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.settings = HostSettings(
            database_url='sqlite+aiosqlite:///' + str(self.root / 'runtime.db'),
            residency_interval_seconds=0.02,
            min_residency_seconds=0,
        )
        self.workflow = Workflow('work', nodes=[Node('work', identity)])

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self, *, workflows=None, **settings):
        return AutoAgentHost(workflows=workflows or [self.workflow], settings=replace(self.settings, **settings))

    async def test_directory_history_and_explicit_entry_parameters(self):
        """Expose current/historical Revisions and pass explicit admission options."""
        host = self.host()
        self.assertTrue((await host.check_workflows()).ok)
        self.assertFalse((self.root / 'runtime.db').exists())
        async with host:
            old = await host.get_workflow('work')
            self.assertTrue(old['runnable'])
            ref = await host.execute('work', {'text': 'first'}, revision_id=old['workflow_revision_id'], session_id='chosen', session_context={'shared': 'context'}, entry_node_id='work')
            await host.join(ref)
            self.assertEqual(ref.session_id, 'chosen')
            for obsolete in ('run', 'submit', 'wait', 'resume', 'runtime_events', 'graph_changes', 'statistics', 'load_checkpoint', 'flush'):
                self.assertFalse(hasattr(host, obsolete))
            with self.assertRaises(RuntimeTransitionError):
                await host.execute('work', {'text': 'bad'}, session_id='chosen', session_context={})
        new = Workflow('work', nodes=[Node('work', identity)], version='2')
        async with self.host(workflows=[new]) as host:
            current = (await host.list_workflows()).items
            history = (await host.list_workflows(include_history=True)).items
            self.assertEqual(len(current), 1)
            self.assertEqual(len(history), 2)
            old_record = await host.get_workflow('work', revision_id=old['workflow_revision_id'])
            self.assertFalse(old_record['runnable'])
            with self.assertRaises(HostError):
                await host.execute('work', {'text': 'old'}, revision_id=old['workflow_revision_id'])

    async def test_warm_session_reuses_large_context_without_completion_snapshots(self):
        """Repeated Invocations reuse resident Context and do not save per-result Checkpoints."""
        async with self.host() as host:
            with patch.object(host._persistence, 'save_checkpoint', wraps=host._persistence.save_checkpoint) as saves:
                first = await host.execute('work', {'text': 'first'}, session_id='warm', session_context={'blob': 'x' * (1024 * 1024)})
                await host.join(first)
                for _ in range(5):
                    await completed(host, session_id='warm')
                self.assertEqual(saves.call_count, 0)
                ref = host._runtime.resident['warm']
                checkpoint = await host._runtime.app.acapture_checkpoint(ref)
                state = next(s.state for s in checkpoint.sessions if s.session_id == 'warm')
                self.assertEqual(len(state.session.context['blob']), 1024 * 1024)
                self.assertEqual(len(host._runtime.resident), 1)
                self.assertEqual(host._runtime.locks, {})

    async def test_idle_eviction_and_same_session_load_preserve_context(self):
        """Cold eviction persists State before a later Invocation reuses the Session."""
        async with self.host(cold_after_seconds=0.04, max_idle_seconds=0.06) as host:
            first = await host.execute('work', {'text': 'first'}, session_id='cold', session_context={'counter': 7})
            await host.join(first)
            await eventually(lambda: 'cold' not in host._runtime.resident)
            self.assertEqual(host._runtime.owners, {})
            self.assertEqual(host._runtime.locks, {})
            self.assertEqual((await host.get_execution(first)).output, {'text': 'first'})
            second = await completed(host, session_id='cold')
            checkpoint = await host._runtime.app.acapture_checkpoint(second.ref)
            self.assertEqual(checkpoint.sessions[0].state.session.context['counter'], 7)
            self.assertNotEqual(first.invocation_id, second.invocation_id)
            with self.assertRaises(HostError):
                await host.cancel(first)

    async def test_pressure_reclaims_cold_state_before_expensive_hot_state(self):
        """Memory pressure uses reclaimable bytes and recent reuse to choose victims."""
        async with self.host(memory_budget_bytes=1024 * 1024 * 1024, unload_concurrency=1) as host:
            cold = await completed(host, {'text': 'x' * 32768}, session_id='cold')
            await completed(host, session_id='hot')
            await completed(host, session_id='hot')
            policy = host._runtime.residency
            policy.budget = 50000
            policy.baseline = 0
            policy.memory_usage = lambda: 41000
            await policy.cycle(activate=False)
            await eventually(lambda: 'cold' not in host._runtime.resident)
            self.assertIn('hot', host._runtime.resident)
            self.assertEqual((await host.get_execution(cold.ref)).status, 'completed')

    async def test_root_slot_limit_reclaims_completed_graphs_under_low_pressure(self):
        """A hard graph cap reuses slots without requiring memory pressure."""
        async with self.host(max_resident_graphs=2) as host:
            ids = []
            for i in range(7):
                result = await completed(host, session_id=f's{i}')
                ids.append(result.ref)
                self.assertLessEqual(len(host._runtime.resident), 2)
            for ref in ids:
                self.assertEqual((await host.get_execution(ref)).status, 'completed')
            self.assertLessEqual(len(host._runtime.residency.entries), 4)

    async def test_long_timer_unloads_and_automatically_wakes(self):
        """A Timer continues through database wake scheduling after graph eviction."""
        workflow = Workflow('timer', nodes=[Node('timer', Timer())])
        async with self.host(workflows=[workflow], cold_after_seconds=0, max_idle_seconds=0, short_timer_seconds=0) as host:
            ref = await host.execute('timer', {'delay_us': 300000})
            waiting = await host.join(ref)
            self.assertEqual(waiting.status, 'waiting')
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            async def finished():
                return (await host.get_execution(ref)).status == 'completed'
            await eventually(finished)
            self.assertEqual(host._runtime.residency.fault, None)

    async def test_short_timer_stays_resident_despite_idle_expiration(self):
        """Near-term Timers retain their graph below critical memory pressure."""
        workflow = Workflow('timer', nodes=[Node('timer', Timer())])
        async with self.host(workflows=[workflow], cold_after_seconds=0, max_idle_seconds=0, short_timer_seconds=60) as host:
            ref = await host.execute('timer', {'delay_us': 400000})
            await host.join(ref)
            await asyncio.sleep(0.08)
            self.assertIn(ref.session_id, host._runtime.resident)
            await eventually(lambda: host._persistence.get_invocation(ref.invocation_id))
            async def finished():
                return (await host.get_execution(ref)).status == 'completed'
            await eventually(finished)

    async def test_unloaded_select_timer_wakes_and_results_keep_json_shape(self):
        """A Select deadline is indexed, recovered and queried without changing output types."""
        workflow = Workflow('select', nodes=[Node('select', Select({'message': SignalCase('mail'), 'timeout': TimerCase(delay_us=500000)}))], signal_endpoints=[SignalEndpoint('mail', Value)])
        async with self.host(workflows=[workflow], cold_after_seconds=0, max_idle_seconds=0, short_timer_seconds=0) as host:
            ref = await host.execute('select', None)
            await host.join(ref)
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            async def finished():
                result = await host.get_execution(ref)
                return result if result.status == 'completed' else None
            result = await eventually(finished)
            self.assertEqual(result.output['case'], 'timeout')
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            self.assertEqual((await host.get_execution(ref)).output, result.output)

    async def test_timer_restarts_from_durable_deadline(self):
        """Host startup discovers sleeping Timers without loading all waiting Sessions."""
        workflow = Workflow('timer', nodes=[Node('timer', Timer())])
        async with self.host(workflows=[workflow]) as host:
            ref = await host.execute('timer', {'delay_us': 300000})
            await host.join(ref)
        async with self.host(workflows=[workflow]) as host:
            async def finished():
                return (await host.get_execution(ref)).status == 'completed'
            await eventually(finished)

    async def test_parent_child_timer_graph_recovers_as_one_unit(self):
        """A Child Timer deadline wakes an unloaded Root and settles parent ownership."""
        child = Workflow('child', nodes=[Node('timer', Timer())])
        parent = Workflow('parent', nodes=[Node('spawn', Spawn(child, 'timer'))])
        async with self.host(workflows=[parent], cold_after_seconds=0, max_idle_seconds=0, short_timer_seconds=0) as host:
            ref = await host.execute('parent', {'delay_us': 300000})
            await host.join(ref)
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            async def finished():
                return (await host.get_execution(ref)).status == 'completed'
            await eventually(finished)
            self.assertEqual(host._runtime.residency.fault, None)

    async def test_paused_evicted_approval_does_not_implicitly_continue(self):
        """Respond, query and join keep a restored approval graph paused."""
        workflow = Workflow('approval', nodes=[Node('wait', Wait(Value, Value))])
        async with self.host(workflows=[workflow], cold_after_seconds=0, max_idle_seconds=0) as host:
            ref = await host.execute('approval', {'text': 'ask'})
            result = await host.join(ref)
            await host.pause(ref)
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            self.assertEqual((await host.get_execution(ref)).control_state, 'paused')
            await host.respond(ref, result.waits[0].id, {'text': 'approved'})
            self.assertEqual((await host.join(ref)).control_state, 'paused')
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            result = await host.recover(ref)
            self.assertEqual(result.status, 'completed')
            self.assertEqual(result.output, {'text': 'approved'})

    async def test_paused_evicted_signal_stays_paused_until_continue(self):
        """Signal acceptance survives eviction while dispatch remains paused."""
        workflow = Workflow('signal', nodes=[Node('receive', AwaitSignal('mail'))], signal_endpoints=[SignalEndpoint('mail', Value)])
        async with self.host(workflows=[workflow], cold_after_seconds=0, max_idle_seconds=0) as host:
            ref = await host.execute('signal', None)
            await host.join(ref)
            await host.pause(ref)
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            receipt = await host.signal(ref, 'mail', {'text': 'message'}, source_id='client', sequence=1)
            self.assertTrue(receipt.message_id)
            self.assertEqual((await host.join(ref)).control_state, 'paused')
            result = await host.recover(ref)
            self.assertEqual(result.status, 'completed')

    async def test_new_request_cancels_eviction_during_snapshot_io(self):
        """Saving an old snapshot cannot evict a Session used again during I/O."""
        async with self.host() as host:
            first = await completed(host, session_id='race')
            entered, release = asyncio.Event(), asyncio.Event()
            original = host._persistence.save_checkpoint
            async def delayed(checkpoint, **options):
                entered.set()
                await release.wait()
                return await original(checkpoint, **options)
            host._persistence.save_checkpoint = delayed
            item = host._runtime.residency.entries['race']
            info = await host._runtime.app.aresidency_info(first.ref)
            host._runtime.residency._snapshot(item, info, evict=True)
            await asyncio.wait_for(entered.wait(), 3)
            second = await completed(host, {'text': 'second'}, session_id='race')
            release.set()
            await eventually(lambda: item.job is None)
            self.assertIn('race', host._runtime.resident)
            self.assertEqual((await host.get_execution(second.ref)).output, {'text': 'second'})
            self.assertIsNone(host._runtime.residency.fault)

    async def test_snapshot_failure_keeps_state_and_blocks_new_admission(self):
        """A failed persistence barrier retains Core State and prevents accumulating more work."""
        async with self.host() as host:
            result = await completed(host)
            item = host._runtime.residency.entries[result.session_id]
            info = await host._runtime.app.aresidency_info(result.ref)
            original = host._persistence.save_checkpoint
            async def fail(checkpoint, **options):
                raise OSError('database unavailable')
            with self.assertLogs('autoagent.host.residency', level='ERROR'):
                host._persistence.save_checkpoint = fail
                host._runtime.residency._snapshot(item, info, evict=True)
                await eventually(lambda: item.job is None)
            self.assertIn(result.session_id, host._runtime.resident)
            self.assertEqual((await host.get_execution(result.ref)).status, 'completed')
            with self.assertRaises(HostCapacityError):
                await host.execute('work', {'text': 'new'})
            with self.assertRaises(HostCapacityError):
                await host.execute('work', {'text': 'reuse'}, session_id=result.session_id)
            host._persistence.save_checkpoint = original

    async def test_raw_memory_limit_cannot_be_bypassed_by_reclaimed_credit(self):
        """Freed allocator arenas cannot authorize new work above the actual RSS limit."""
        async with self.host(memory_budget_bytes=1024 * 1024 * 1024) as host:
            first = await completed(host, session_id='warm')
            policy = host._runtime.residency
            policy.budget = 10000
            policy.baseline = 0
            policy.last_rss = 9500
            policy.memory_usage = lambda: 9500
            policy.reclaimed_credit = 9000
            with self.assertRaises(HostCapacityError):
                await host.execute('work', {'text': 'reject'})
            with self.assertRaises(HostCapacityError):
                await host.execute('work', {'text': 'reject'}, session_id='warm')
            self.assertEqual((await host.get_execution(first.ref)).status, 'completed')

    async def test_paused_parent_child_approval_is_restored_without_dispatch(self):
        """Deliver a Child approval through its paused Root after whole-graph eviction."""
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        parent = Workflow('parent', nodes=[Node('spawn', Spawn(child, 'wait'))])
        async with self.host(workflows=[parent], cold_after_seconds=0, max_idle_seconds=0) as host:
            ref = await host.execute('parent', {'text': 'ask'})
            waiting = await host.join(ref)
            await host.pause(ref)
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            await host.respond(ref, waiting.waits[0].id, {'text': 'approved'})
            self.assertEqual((await host.join(ref)).control_state, 'paused')
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            self.assertEqual((await host.recover(ref)).status, 'completed')
            graph = (await host._persistence.list_invocations(root_invocation_id=ref.invocation_id)).items
            children = [row for row in graph if row['parent_invocation_id'] == ref.invocation_id]
            self.assertEqual(children[0]['status'], 'completed')

    async def test_unopened_child_session_cannot_be_admitted_as_root(self):
        """Persisted planned Children retain ownership even while their Root is unloaded."""
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        def inputs(context: InputMappingContext) -> list[Value]:
            return [{'text': str(i)} for i in range(3)]
        parent = Workflow('parent', nodes=[Node('spawn', Spawn(child, 'wait'), map=Map(max_parallelism=1), input_mapping=inputs)])
        async with self.host(workflows=[parent], cold_after_seconds=0, max_idle_seconds=0) as host:
            ref = await host.execute('parent', None)
            async def planned_child():
                graph = (await host._persistence.list_invocations(root_invocation_id=ref.invocation_id, include_planned=True)).items
                return next((row for row in graph if row['parent_invocation_id'] and row['status'] is None), None)
            child_row = await eventually(planned_child)
            info = await host._runtime.app.aresidency_info(ref)
            self.assertFalse(info.can_unload)
            await host.pause(ref, timeout=3)
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            with self.assertRaises(HostError):
                await host.execute('child', {'text': 'wrong'}, session_id=child_row['session_id'])
            self.assertEqual((await host.get_execution(ref)).control_state, 'paused')
            self.assertIsNone(host._runtime.residency.fault)

    async def test_periodic_snapshot_bounds_tail_without_unloading(self):
        """Tail limits save a dirty safe graph while retaining warm Session State."""
        async with self.host(checkpoint_tail_events=1) as host:
            result = await completed(host, session_id='warm')
            async def saved():
                return bool((await host._persistence.list_checkpoints('warm')).items)
            await eventually(saved)
            self.assertIn('warm', host._runtime.resident)
            item = host._runtime.residency.entries['warm']
            await eventually(lambda: item.job is None)
            version = (await host._runtime.app.aresidency_info(result.ref)).version
            self.assertEqual(item.saved_version, version)
            with patch.object(host._persistence, 'save_checkpoint', wraps=host._persistence.save_checkpoint) as saves:
                for _ in range(3):
                    await host._runtime.residency.cycle(activate=False)
                self.assertEqual(saves.call_count, 0)

    async def test_live_graph_cannot_be_evicted_and_capacity_is_bounded(self):
        """At the graph cap, a live Operator is retained and further admission is rejected."""
        started, release = threading.Event(), threading.Event()
        async def slow(value: Value) -> Value:
            started.set()
            await asyncio.to_thread(release.wait, 3)
            return value
        workflow = Workflow('work', nodes=[Node('work', slow)])
        try:
            async with self.host(workflows=[workflow], max_resident_graphs=1) as host:
                ref = await host.execute('work', {'text': 'first'})
                self.assertTrue(await asyncio.to_thread(started.wait, 3))
                with self.assertRaises(HostCapacityError):
                    await host.execute('work', {'text': 'second'})
                self.assertIn(ref.session_id, host._runtime.resident)
                release.set()
                await host.join(ref)
        finally:
            release.set()

    async def test_blocked_join_cannot_exhaust_control_capacity(self):
        """A full ordinary request budget retains separate bounded cancellation capacity."""
        started, release = threading.Event(), threading.Event()
        async def slow(value: Value) -> Value:
            started.set()
            await asyncio.to_thread(release.wait, 3)
            return value
        workflow = Workflow('work', nodes=[Node('work', slow)])
        try:
            async with self.host(workflows=[workflow], max_requests=1, control_request_reserve=1) as host:
                ref = await host.execute('work', {'text': 'first'})
                self.assertTrue(await asyncio.to_thread(started.wait, 3))
                waiter = asyncio.create_task(host.join(ref))
                await eventually(lambda: bool(host._operations))
                with self.assertRaises(HostCapacityError):
                    await host.get_execution(ref)
                self.assertEqual((await host.cancel(ref, reason='stop')).status, 'cancelled')
                self.assertEqual((await waiter).status, 'cancelled')
                release.set()
        finally:
            release.set()

    async def test_buffered_eviction_flushes_before_releasing_residency(self):
        """Even a large buffer window cannot discard State before its Checkpoint is committed."""
        settings = replace(self.settings, persistence=PersistenceConfig(mode='buffered', batch_delay_ms=5000), cold_after_seconds=0, max_idle_seconds=0)
        async with AutoAgentHost(workflows=[self.workflow], settings=settings) as host:
            ref = await host.execute('work', {'text': 'buffered'})
            await host.join(ref)
            await eventually(lambda: ref.session_id not in host._runtime.resident)
            health = await host._persistence.health()
            self.assertEqual(health.queued_count, 0)
            self.assertEqual(health.retained_bytes, 0)
            row = await host._persistence.get_invocation(ref.invocation_id)
            self.assertEqual(row['status'], 'completed')
            self.assertEqual((await host.get_execution(ref)).output, {'text': 'buffered'})

    async def test_concurrent_cold_load_is_single_and_existing_session_stays_exclusive(self):
        """Concurrent messages share one load; a waiting Session rejects a new Invocation."""
        workflow = Workflow('signal', nodes=[Node('wait', Wait(Value, Value))], signal_endpoints=[SignalEndpoint('mail', Value)])
        async with self.host(workflows=[workflow], cold_after_seconds=0, max_idle_seconds=0) as host:
            ref = await host.execute('signal', {'text': 'ask'}, session_id='single')
            await host.join(ref)
            await eventually(lambda: 'single' not in host._runtime.resident)
            original = host._runtime.app.aload_checkpoint
            with patch.object(host._runtime.app, 'aload_checkpoint', wraps=original) as loads:
                await asyncio.gather(*(host.signal(ref, 'mail', {'text': str(i)}, source_id=f'c{i}', sequence=1) for i in range(4)))
                self.assertEqual(loads.call_count, 1)
            with self.assertRaises(RuntimeTransitionError):
                await host.execute('signal', {'text': 'new'}, session_id='single')
            self.assertEqual(host._runtime.locks, {})

    async def test_request_byte_limit_rejects_before_creating_session(self):
        """Oversized queued input is rejected without Runtime or database mutation."""
        async with self.host(max_request_bytes=1024) as host:
            with self.assertRaises(HostCapacityError):
                await host.execute('work', {'text': 'x' * 8192})
            self.assertEqual(host._runtime.resident, {})
            self.assertEqual((await host._persistence.list_sessions()).items, ())


@unittest.skipUnless(POSTGRES_TEST_URL, 'isolated PostgreSQL URL not configured')
class PostgreSQLAdaptiveHostTests(AdaptiveHostTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        import asyncpg
        from sqlalchemy.engine import make_url
        self.admin_url = make_url(POSTGRES_TEST_URL)
        self.database_name = 'autoagent_residency_' + uuid4().hex
        conn = await asyncpg.connect(self.admin_url.set(drivername='postgresql').render_as_string(hide_password=False))
        try:
            await conn.execute('CREATE DATABASE "' + self.database_name + '"')
        finally:
            await conn.close()
        self.settings = replace(self.settings, database_url=self.admin_url.set(drivername='postgresql+asyncpg', database=self.database_name).render_as_string(hide_password=False))

    async def asyncTearDown(self):
        import asyncpg
        conn = await asyncpg.connect(self.admin_url.set(drivername='postgresql').render_as_string(hide_password=False))
        try:
            await conn.execute('DROP DATABASE "' + self.database_name + '" WITH (FORCE)')
        finally:
            await conn.close()
            await super().asyncTearDown()


class ResidencyConfigurationTests(unittest.TestCase):
    def test_invalid_watermarks_intervals_and_budget_are_rejected(self):
        """Reject unstable policy combinations and non-finite deployment values."""
        for values in ({'memory_target_ratio': .9}, {'memory_high_ratio': float('nan')}, {'memory_budget_bytes': -1}, {'residency_interval_seconds': 0}, {'cold_after_seconds': 2000}, {'short_timer_seconds': float('inf')}, {'background_batch_size': 1001}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                HostSettings(**values)

    def test_environment_defaults_and_overrides_are_valid(self):
        """The complete example config parses, and deployment overrides affect policy."""
        example = Path(__file__).resolve().parents[1] / '.env.example'
        env = dict(line.split('=', 1) for line in example.read_text().splitlines() if line.startswith('AUTOAGENT_'))
        self.assertEqual(HostSettings.from_environment(env), HostSettings())
        settings = HostSettings.from_environment(dict(env, AUTOAGENT_HOST_COLD_AFTER_SECONDS='17', AUTOAGENT_HOST_CHECKPOINT_TAIL_EVENTS='99', AUTOAGENT_BATCH_DELAY_MS='500', AUTOAGENT_WORKFLOW_GRAPH_CACHE_BYTES='12345'))
        self.assertEqual(settings.cold_after_seconds, 17)
        self.assertEqual(settings.checkpoint_tail_events, 99)
        self.assertEqual(settings.persistence.batch_delay_ms, 500)
        self.assertEqual(settings.persistence.workflow_graph_cache_bytes, 12345)
        for invalid in ('0', '-1', 'nan'):
            with self.subTest(graph_cache_bytes=invalid), self.assertRaises(ValueError):
                HostSettings.from_environment(dict(env, AUTOAGENT_WORKFLOW_GRAPH_CACHE_BYTES=invalid))
