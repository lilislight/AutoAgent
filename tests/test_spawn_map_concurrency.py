"""Spawn admission concurrency is independent of Child execution lifetime."""
import asyncio
from collections import Counter
import threading
import time
import unittest

from typing_extensions import TypedDict

from autoagent import AutoAgentApp, InputMappingContext, Map, Node, Spawn, Workflow, RuntimeInfrastructureError
from tests.graph_fixtures import child_refs, load_graph
from tests.test_child_sink_integrity import Collector, checkpoint_from_events


class Value(TypedDict):
    value: int


def inputs(context: InputMappingContext) -> list[Value]:
    return [{'value': index} for index in range(context.invocation_input['value'])]


def wait_until(predicate, timeout=3):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.001)
    raise AssertionError('Runtime did not reach the expected boundary')


class SpawnMapConcurrencyTests(unittest.TestCase):
    def read(self, app, callback):
        async def run():
            return callback()
        return app._runtime_loop.run(run())

    def test_spawn_map_one_admits_all_children_before_any_child_finishes(self):
        """Map=1 releases its Spawn permit on Handle acceptance, allowing all Child Nodes to run."""
        release = threading.Event()
        entered = []
        async def block(value: Value) -> Value:
            entered.append(value['value'])
            while not release.is_set():
                await asyncio.sleep(.001)
            return value
        child = Workflow('spawn-independent-child', nodes=[Node('work', block)])
        parent = Workflow('spawn-independent-parent', nodes=[Node('spawn', Spawn(child, 'work'),
            input_mapping=inputs, map=Map(max_parallelism=1))])
        sink = Collector()
        app = AutoAgentApp(max_operator_concurrency=8, runtime_event_sink=sink)
        try:
            root = app.submit_invoke(parent, {'value': 6}).ref
            wait_until(lambda: len(entered) == 6)
            def snapshot():
                inv = app._repository.state(root.session_id).invocation
                return [call.status for call in inv.scheduler.operator_calls.values()], [
                    app._repository.state(unit.session_id).invocation.scheduler.occurrences
                    for plan in inv.child_plans.values() for unit in plan.units]
            statuses, children = self.read(app, snapshot)
            self.assertEqual(statuses, ['completed'] * 6)
            self.assertTrue(all(next(iter(nodes.values())).started_at_us is not None for nodes in children))
            self.assertFalse(any(e.event_name == 'operator_call.completed' and e.session_id != root.session_id for e in sink.events))
            release.set()
            result = app.join(root, timeout=3)
            self.assertEqual(result.status, 'completed')
            self.assertEqual([h.invocation_id for h in result.output], [h.invocation_id for h in child_refs(app, root)])
        finally:
            release.set()
            app.close()

    def test_children_share_global_operator_capacity_across_spawn_nodes(self):
        """Multiple Spawn Maps can admit all Children while their Operators share one global limit."""
        releases = [threading.Event() for _ in range(6)]
        entered = []
        active = peak = 0
        async def block(value: Value) -> Value:
            nonlocal active, peak
            index = value['value']
            entered.append(index)
            active += 1
            peak = max(peak, active)
            try:
                while not releases[index].is_set():
                    await asyncio.sleep(.001)
                return value
            finally:
                active -= 1
        child = Workflow('global-spawn-child', nodes=[Node('work', block)])
        parent = Workflow('global-spawn-parent', nodes=[Node('spawn', Spawn(child, 'work'),
            input_mapping=inputs, map=Map(max_parallelism=1))])
        sink = Collector()
        app = AutoAgentApp(max_operator_concurrency=2, runtime_event_sink=sink)
        try:
            # Two Root graphs compete for the same NodeExecutor capacity.
            roots = [app.submit_invoke(parent, {'value': 3}).ref for _ in range(2)]
            wait_until(lambda: len([e for e in sink.events if e.event_name == 'operator_call.completed'
                and e.session_id in {root.session_id for root in roots}]) == 6)
            self.assertEqual(len(entered), 2)
            self.assertEqual(peak, 2)
            events = list(sink.events)
            self.assertEqual(len([e for e in events if e.event_name == 'node_occurrence.started'
                and e.session_id not in {root.session_id for root in roots}]), 6)
            self.assertEqual(len([e for e in events if e.event_name == 'operator_call.started'
                and e.session_id not in {root.session_id for root in roots}]), 2)
            for release in releases:
                release.set()
            for root in roots:
                self.assertEqual(app.join(root, timeout=3).status, 'completed')
            self.assertEqual(peak, 2)
            self.assertEqual(len(entered), 6)
        finally:
            for release in releases:
                release.set()
            app.close()

    def test_map_limits_inflight_admissions_and_records_each_call_boundary(self):
        """Blocked Child admission uses Map permits; Call completion precedes unrelated Child completion."""
        admission_release = threading.Event()
        work_release = threading.Event()
        openings = []
        class Sink(Collector):
            async def append(self, event):
                if event.event_name == 'session.opened' and event.session_id != 'root':
                    openings.append(event.session_id)
                    while not admission_release.is_set():
                        await asyncio.sleep(.001)
                await super().append(event)
        async def block(value: Value) -> Value:
            while not work_release.is_set():
                await asyncio.sleep(.001)
            return value
        child = Workflow('admission-child', nodes=[Node('work', block)])
        parent = Workflow('admission-parent', nodes=[Node('spawn', Spawn(child, 'work'),
            input_mapping=inputs, map=Map(max_parallelism=2))])
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            root = app.submit_invoke(parent, {'value': 6}, session_id='root').ref
            wait_until(lambda: len(openings) == 2)
            self.assertEqual(len([e for e in sink.events if e.session_id == 'root' and e.event_name == 'operator_call.started']), 2)
            admission_release.set()
            wait_until(lambda: len([e for e in sink.events if e.session_id == 'root' and e.event_name == 'operator_call.completed']) == 6)
            events = list(sink.events)
            active = peak = 0
            for event in events:
                if event.session_id != 'root':
                    continue
                if event.event_name == 'operator_call.started':
                    active += 1
                    peak = max(peak, active)
                elif event.event_name == 'operator_call.completed':
                    active -= 1
            self.assertEqual((active, peak), (0, 2))
            plans = [e for e in events if e.event_name == 'child_invocation.planned']
            self.assertEqual(len(plans), 1)
            starts = {e.payload.unit_index: e for e in events if e.session_id == 'root' and e.event_name == 'operator_call.started'}
            completions = {e.payload.call_id: e for e in events if e.session_id == 'root' and e.event_name == 'operator_call.completed'}
            for unit in plans[0].payload.units:
                start = starts[unit.unit_index]
                opened = next(e for e in events if e.session_id == unit.child_session_id and e.event_name == 'invocation.started')
                accepted = next(e for e in events if e.event_name == 'child_invocation.phase_changed'
                    and e.payload.unit_index == unit.unit_index and e.payload.phase == 'accepted')
                complete = completions[start.payload.call_id]
                self.assertLess(events.index(start), events.index(opened))
                self.assertLess(events.index(accepted), events.index(complete))
                self.assertLessEqual(start.occurred_at_us, opened.occurred_at_us)
                self.assertLessEqual(accepted.occurred_at_us, complete.occurred_at_us)
            work_release.set()
            self.assertEqual(app.join(root, timeout=3).status, 'completed')
        finally:
            admission_release.set()
            work_release.set()
            app.close()

    def test_recovery_reuses_partial_spawn_plan_after_lost_call_completion_ack(self):
        """A lost Spawn completion ACK restores the same Children without repeating accepted Calls."""
        for limit in (1, 3):
            with self.subTest(map_limit=limit):
                release = threading.Event()
                class Sink(Collector):
                    failed = False
                    async def append(self, event):
                        await super().append(event)
                        if not self.failed and event.session_id == 'root' and event.event_name == 'operator_call.completed':
                            self.failed = True
                            raise OSError('Lost Spawn completion ACK')
                async def block(value: Value) -> Value:
                    while not release.is_set():
                        await asyncio.sleep(.001)
                    return value
                child = Workflow('recover-admission-child', nodes=[Node('work', block)])
                parent = Workflow('recover-admission-parent', nodes=[Node('spawn', Spawn(child, 'work'),
                    input_mapping=inputs, map=Map(max_parallelism=limit))])
                sink = Sink()
                source = AutoAgentApp(runtime_event_sink=sink)
                try:
                    with self.assertRaises(RuntimeInfrastructureError):
                        source.invoke(parent, {'value': 6}, session_id='root')
                    prefix = tuple(sink.events)
                    checkpoint = checkpoint_from_events(prefix)
                    planned = next(e.payload for e in prefix if e.event_name == 'child_invocation.planned')
                    completed = {e.payload.call_id for e in prefix if e.session_id == 'root' and e.event_name == 'operator_call.completed'}
                finally:
                    release.set()
                    source.close()
                restored_sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=restored_sink)
                try:
                    restored.register_workflow(parent)
                    ref = next(r for r in load_graph(restored, checkpoint).invocations if r.session_id == 'root')
                    result = restored.recover(ref)
                    self.assertEqual(result.status, 'completed', result.error)
                    self.assertEqual([h.invocation_id for h in result.output], [u.child_invocation_id for u in planned.units])
                    births = Counter(e.invocation_id for e in (*prefix, *restored_sink.events) if e.event_name == 'invocation.started' and e.session_id != 'root')
                    self.assertEqual(births, Counter(u.child_invocation_id for u in planned.units))
                    repeated = [e for e in restored_sink.events if e.event_name == 'operator_call.completed' and e.payload.call_id in completed]
                    self.assertEqual(repeated, [])
                finally:
                    restored.close()
