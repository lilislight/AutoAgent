"""Canonical Event, Delta, Repository and stage recovery boundaries."""
from __future__ import annotations

from autoagent import RuntimeGraphCheckpoint

from tests.graph_fixtures import (
    load_graph,
    root_snapshot,
)

import asyncio
import json
import unittest
from dataclasses import replace
from unittest.mock import patch
from typing_extensions import TypedDict

from autoagent import (AutoAgentApp, ContextOperation, ContextPatch, Edge, Map, Node,
    Workflow, InputMappingContext, OutputBindingContext, ConditionContext, AggregationContext,
    RuntimeInfrastructureError, RuntimeTransitionError)
from autoagent.core.runtime import (RuntimeEvent, RuntimeState, StateReducer, StateDelta,
    StateOperation, RuntimeRepository, SessionOpened,
    InvocationStarted, InputMapped, OperatorCallStarted, OperatorCallCompleted,
    Aggregated, OutputBound, RoutingResolved, NodeStarted, NodeCompleted, SessionCheckpoint,
    TransitionPlanner, RecoveryApplied)
from autoagent.core.runtime.operations import apply_runtime_delta


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


class RecordingSink:
    def __init__(self):
        self.events = []

    async def append(self, event):
        if not any(item.id == event.id for item in self.events):
            self.events.append(event)


class RuntimeReducerTests(unittest.TestCase):
    def history(self):
        sink = RecordingSink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(Workflow("linear", nodes=[Node("a", identity), Node("b", identity)],
                edges=[Edge("a", "b")]), {"value": 2})
            self.assertEqual(result.status, "completed", result.error)
            return tuple(sink.events), app._repository.state(result.session_id)
        finally:
            app.close()

    def test_every_prefix_replays_and_round_trips(self):
        """Every semantic boundary has a canonical round trip and identical replay."""
        events, expected = self.history()
        state = RuntimeState()
        for index, event in enumerate(events, 1):
            restored = RuntimeEvent.from_record(json.loads(json.dumps(event.to_record())))
            self.assertEqual(event, restored)
            state = StateReducer().apply(state, restored)
            self.assertEqual(state, StateReducer().reduce(events[:index]))
            self.assertEqual(state.sequence, index)
        self.assertEqual(state, expected)
        self.assertEqual([event.event_name for event in events[:2]], ["session.opened", "invocation.started"])

    def test_checkpoint_suffix_equals_full_replay(self):
        """A standalone checkpoint plus Event suffix reproduces full history."""
        events, expected = self.history()
        for index in range(2, len(events)):
            state = StateReducer().reduce(events[:index])
            checkpoint = SessionCheckpoint.from_state(state)
            restored = SessionCheckpoint.from_record(checkpoint.to_record())
            self.assertEqual(restored.sequence, index)
            self.assertEqual(StateReducer().reduce(events[index:], restored.state), expected)

    def test_replay_does_not_plan_or_read_clocks(self):
        """Persisted replay applies only Delta even when live planning is unavailable."""
        events, expected = self.history()
        with patch.object(TransitionPlanner, "plan", side_effect=AssertionError("planner called")), \
             patch("time.time_ns", side_effect=AssertionError("clock called")):
            self.assertEqual(StateReducer().reduce(events), expected)

    def test_delta_is_atomic_when_later_operation_fails(self):
        """A rejected trailing operation cannot expose preceding State mutations."""
        events, _ = self.history()
        state = StateReducer().reduce(events[:2])
        before = state.to_record()
        delta = StateDelta((StateOperation("replace", ("session", "context"), {"changed": True}),
            StateOperation("replace", ("invocation", "missing"), 1)))
        with self.assertRaises(RuntimeTransitionError):
            apply_runtime_delta(state, delta)
        self.assertEqual(state.to_record(), before)

    def test_delta_cannot_change_event_order(self):
        """Sequence metadata belongs exclusively to the Event envelope."""
        events, _ = self.history()
        state = StateReducer().reduce(events[:2])
        changed = replace(events[2], delta=StateDelta((StateOperation("replace", ("sequence",), 99),)))
        with self.assertRaisesRegex(RuntimeTransitionError, "METADATA"):
            StateReducer().apply(state, changed)

    def test_sequence_gap_duplicate_and_session_mismatch_rejected(self):
        """Reducer rejects gaps, duplicate applications and cross-Session history."""
        events, _ = self.history()
        state = StateReducer().reduce(events[:2])
        for event in (events[1], replace(events[2], sequence=99), replace(events[2], session_id="other")):
            with self.assertRaises(RuntimeTransitionError):
                StateReducer().apply(state, event)

    def test_wall_clock_regression_does_not_change_order(self):
        """Backward wall-clock adjustments remain replayable and checkpointable."""
        clock = iter(range(1000, 0, -1))
        sink = RecordingSink()
        app = AutoAgentApp(clock_us=lambda: next(clock), runtime_event_sink=sink)
        try:
            result = app.invoke(Workflow("clock", nodes=[Node("node", identity)]), {"value":1})
            self.assertEqual(result.status, "completed", result.error)
            state = StateReducer().reduce(tuple(sink.events))
            self.assertEqual(state.sequence, len(sink.events))
            SessionCheckpoint.from_record(SessionCheckpoint.from_state(state).to_record())
        finally:
            app.close()

    def test_event_has_one_delta_and_no_parallel_version_or_log(self):
        """Canonical records expose one semantic event and at most one Delta."""
        events, state = self.history()
        for event in events:
            self.assertIsInstance(event.delta, StateDelta)
            self.assertFalse({"logs", "operation_batches", "from_state_version", "to_state_version"} & event.to_record().keys())
        self.assertNotIn("state_version", state.to_record())

    def test_parallel_ready_is_one_delta_but_starts_are_independent(self):
        """One completion readies several successors while each start is independent."""
        sink = RecordingSink(); app = AutoAgentApp(runtime_event_sink=sink)
        try:
            workflow = Workflow("fanout", nodes=[Node(name, identity) for name in ("a", "b", "c", "d")],
                edges=[Edge("a", name) for name in ("b", "c", "d")])
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "completed", result.error)
            completion = next(e for e in sink.events if isinstance(e.payload, NodeCompleted) and e.payload.occurrence_id == "a@root")
            state = StateReducer().reduce(tuple(e for e in sink.events if e.sequence <= completion.sequence))
            self.assertEqual(set(state.invocation.scheduler.ready), {"b@root", "c@root", "d@root"})
            starts = [e for e in sink.events if isinstance(e.payload, NodeStarted)]
            self.assertEqual(len(starts), 4)
            self.assertEqual(len({e.sequence for e in starts}), 4)
        finally:
            app.close()


class RepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_append_failure_keeps_visible_state_and_retries_same_event(self):
        """Ambiguous append retries preserve Event identity and hide candidate State."""
        class Store(RecordingSink):
            fail = True
            async def append(self, event):
                if self.events:
                    assert self.events[0] is event
                await super().append(event)
                if self.fail:
                    self.fail = False
                    raise OSError("ack lost")
        store = Store(); repository = RuntimeRepository(sink=store)
        with self.assertRaises(RuntimeInfrastructureError):
            await repository.commit(session_id="s", invocation_id=None, payload=SessionOpened({}))
        self.assertEqual(repository.state("s").sequence, 0)
        self.assertNotIn("s", repository._execution_indexes)
        await repository.settle("s")
        self.assertEqual(repository.state("s").sequence, 1)
        self.assertEqual(len(store.events), 1)

    async def test_concurrent_same_session_commit_uses_one_order(self):
        """Concurrent commits reserve contiguous sequence numbers under the Session lock."""
        sink = RecordingSink()
        repository = RuntimeRepository(sink=sink)
        await repository.commit(session_id="s", invocation_id=None, payload=SessionOpened({}))
        from autoagent.core.runtime import SchedulerDelta, OccurrencePlan
        await repository.commit(session_id="s", invocation_id="i",
            payload=InvocationStarted("w", "r", "entry", {}),
            scheduler_delta=SchedulerDelta(ready=(OccurrencePlan("entry@root", "entry", ()),)))
        await asyncio.gather(*(repository.commit(session_id="s", invocation_id="i", payload=RecoveryApplied()) for _ in range(20)))
        events = sink.events
        self.assertEqual([e.sequence for e in events], list(range(1, 23)))
        self.assertEqual(repository.state("s"), StateReducer().reduce(events))

    async def test_acknowledged_event_is_released(self):
        """Core retains State but releases acknowledged Events with or without a sink."""
        import weakref
        import gc
        class TrackedEvent(RuntimeEvent):
            __slots__ = ("__weakref__",)
        class Sink:
            async def append(self, event):
                self.reference = weakref.ref(event)
        for sink in (None, Sink()):
            repository = RuntimeRepository(sink=sink)
            with patch("autoagent.core.runtime.repository.RuntimeEvent", TrackedEvent):
                event = await repository.commit(session_id="s", invocation_id=None, payload=SessionOpened({}))
            reference = weakref.ref(event)
            del event
            gc.collect()
            self.assertIsNone(reference())
            self.assertEqual(repository.state("s").sequence, 1)
            self.assertEqual(repository._pending, {})

    async def test_cancelled_append_does_not_publish_state(self):
        """Cancellation while storage is blocked preserves the last committed prefix."""
        entered = asyncio.Event(); release = asyncio.Event()
        class Store(RecordingSink):
            async def append(self, event):
                entered.set(); await release.wait()
                await super().append(event)
        repository = RuntimeRepository(sink=Store())
        task = asyncio.create_task(repository.commit(session_id="s", invocation_id=None, payload=SessionOpened({})))
        await entered.wait(); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertIsNone(repository.state("s").session)
        release.set(); await repository.settle("s")
        self.assertEqual(repository.state("s").sequence, 1)


class StageRecoveryTests(unittest.TestCase):
    def test_successful_stages_are_not_reexecuted_after_checkpoint_restore(self):
        """Recover each successful hook/call boundary using its saved intermediate results."""
        for boundary in (InputMapped, OperatorCallCompleted, Aggregated, OutputBound, RoutingResolved):
            with self.subTest(boundary=boundary.__name__):
                counts = {"mapping":0, "operator":0, "aggregate":0, "binding":0, "condition":0}
                def mapped(context: InputMappingContext) -> list[Value]:
                    counts["mapping"] += 1
                    return [{"value":context.invocation_input["value"]}]
                def operator(value: Value) -> Value:
                    counts["operator"] += 1
                    return {"value":value["value"]+1}
                def aggregate(context: AggregationContext) -> Value:
                    counts["aggregate"] += 1
                    return context.outputs[0]
                def bound(context: OutputBindingContext) -> ContextPatch:
                    counts["binding"] += 1
                    return ContextPatch(invocation=(ContextOperation.set("saved", context.output),))
                def route(context: ConditionContext) -> bool:
                    counts["condition"] += 1
                    return context.invocation_context["saved"]["value"] == 2
                workflow = Workflow("stages", nodes=[Node("work", operator, input_mapping=mapped,
                    map=Map(aggregate=aggregate), output_binding=bound), Node("end", identity)],
                    edges=[Edge("work", "end", condition=route)])
                sink = RecordingSink(); source = AutoAgentApp(runtime_event_sink=sink)
                try:
                    result = source.invoke(workflow, {"value":1})
                    self.assertEqual(result.status, "completed", result.error)
                    target = next(e for e in sink.events if isinstance(e.payload, boundary))
                    prefix = tuple(e for e in sink.events if e.sequence <= target.sequence)
                    checkpoint = SessionCheckpoint.from_state(StateReducer().reduce(prefix))
                finally:
                    source.close()
                for key in counts: counts[key] = 0
                restored = AutoAgentApp()
                try:
                    restored.register_workflow(workflow)
                    loaded = load_graph(restored, SessionCheckpoint.from_record(checkpoint.to_record()))
                    result = restored.recover(loaded.invocations[0])
                    self.assertEqual(result.status, "completed", result.error)
                    self.assertEqual(result.output, {"value":2})
                    order = ["mapping", "operator", "aggregate", "binding", "condition"]
                    position = (InputMapped, OperatorCallCompleted, Aggregated, OutputBound, RoutingResolved).index(boundary)
                    self.assertEqual({key: counts[key] for key in order[:position+1]}, {key:0 for key in order[:position+1]})
                finally:
                    restored.close()

    def test_operator_handler_runs_only_after_started_append(self):
        """A blocked Started append prevents any physical Operator side effect."""
        invoked = []
        class RejectStarted(RecordingSink):
            async def append(self, event):
                if isinstance(event.payload, OperatorCallStarted):
                    raise OSError("not durable")
                await super().append(event)
        def operator(value: Value) -> Value:
            invoked.append(value); return value
        sink = RejectStarted(); app = AutoAgentApp(runtime_event_sink=sink)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(Workflow("write-ahead", nodes=[Node("node", operator)]), {"value":1})
            self.assertEqual(invoked, [])
        finally:
            # Permit the preserved append intent to settle during shutdown.
            app._repository.sink = None
            app.close()

    def test_partial_map_recovery_reuses_completed_units(self):
        """Recovery reruns only unfinished Map units and preserves output index order."""
        from autoagent import Recovery
        counts = {0: 0, 1: 0}
        async def operator(value: Value) -> Value:
            counts[value["value"]] += 1
            if value["value"] == 1:
                await asyncio.sleep(0.02)
            return value
        def mapped(context: InputMappingContext) -> list[Value]:
            return [{"value": 0}, {"value": 1}]
        workflow = Workflow("partial-map", nodes=[Node("map", operator,
            input_mapping=mapped, map=Map(max_parallelism=2), recovery_mode=Recovery("replay_safe"))])
        sink = RecordingSink(); source = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = source.invoke(workflow, {"value": 0})
            self.assertEqual(result.status, "completed", result.error)
            boundary = next(e for e in sink.events if isinstance(e.payload, OperatorCallCompleted))
            state = StateReducer().reduce(tuple(e for e in sink.events if e.sequence <= boundary.sequence))
            checkpoint = SessionCheckpoint.from_state(state)
        finally:
            source.close()
        counts.update({0: 0, 1: 0})
        restored = AutoAgentApp()
        try:
            restored.register_workflow(workflow)
            loaded = load_graph(restored, checkpoint)
            result = restored.recover(loaded.invocations[0])
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, [{"value": 0}, {"value": 1}])
            self.assertEqual(counts, {0: 0, 1: 1})
        finally:
            restored.close()

    def test_capability_recovery_reuses_recorded_operator_identity(self):
        """Capability continuation uses the saved Operator without calling its resolver."""
        from autoagent import Capability, Operator
        from autoagent.core.runtime import CapabilityResolved
        operator = Operator(identity, id="chosen")
        workflow = Workflow("dispatch", nodes=[Node("node", Capability("choice", operator.contract))])
        sink = RecordingSink(); source = AutoAgentApp(runtime_event_sink=sink)
        try:
            source.register_operator(operator, capability_id="choice")
            result = source.invoke(workflow, {"value": 7})
            self.assertEqual(result.status, "completed", result.error)
            boundary = next(e for e in sink.events if isinstance(e.payload, CapabilityResolved))
            checkpoint = SessionCheckpoint.from_state(StateReducer().reduce(tuple(
                e for e in sink.events if e.sequence <= boundary.sequence)))
        finally:
            source.close()
        def unavailable_resolver(*args):
            raise AssertionError("Resolver must not run during continuation")
        restored = AutoAgentApp(capability_resolver=unavailable_resolver)
        try:
            restored.register_operator(operator, capability_id="choice")
            restored.register_workflow(workflow)
            loaded = load_graph(restored, checkpoint)
            result = restored.recover(loaded.invocations[0])
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output, {"value": 7})
        finally:
            restored.close()

    def test_queue_and_execution_durations_are_separate(self):
        """Contending physical calls record queue delay separately from handler time."""
        async def slow(value: Value) -> Value:
            await asyncio.sleep(0.02)
            return value
        workflow = Workflow("timing", nodes=[Node("start", identity), Node("b", slow), Node("c", slow)],
            edges=[Edge("start", "b"), Edge("start", "c")])
        sink = RecordingSink(); app = AutoAgentApp(runtime_event_sink=sink, max_operator_concurrency=1)
        try:
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "completed", result.error)
            starts = [e.payload for e in sink.events if isinstance(e.payload, OperatorCallStarted)
                and e.payload.occurrence_id != "start@root"]
            self.assertEqual(len(starts), 2)
            self.assertGreater(max(e.queue_duration_ns for e in starts), 10_000_000)
            completions = [e.payload for e in sink.events if isinstance(e.payload, OperatorCallCompleted)]
            self.assertTrue(all(e.execution_duration_ns > 0 for e in completions))
        finally:
            app.close()

    def test_failed_operator_boundary_recovers_without_repeating_handler(self):
        """A committed call failure resumes error routing instead of invoking it again."""
        from autoagent.core.runtime import OperatorCallFailed
        calls = 0
        def fail(value: Value) -> Value:
            nonlocal calls
            calls += 1
            raise ValueError("known failure")
        workflow = Workflow("failed-stage", nodes=[Node("node", fail)])
        sink = RecordingSink(); source = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = source.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "failed")
            boundary = next(e for e in sink.events if isinstance(e.payload, OperatorCallFailed))
            checkpoint = SessionCheckpoint.from_state(StateReducer().reduce(tuple(
                e for e in sink.events if e.sequence <= boundary.sequence)))
        finally:
            source.close()
        restored = AutoAgentApp()
        try:
            restored.register_workflow(workflow)
            loaded = load_graph(restored, checkpoint)
            result = restored.recover(loaded.invocations[0])
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.message, "known failure")
            self.assertEqual(calls, 1)
        finally:
            restored.close()


class RuntimeClockUnitTests(unittest.TestCase):
    def test_wall_clock_records_use_integer_microseconds(self):
        """Events, State, User Events and Checkpoints use the same Unix microseconds."""
        from autoagent.core.runtime import UserEvent
        timestamp_ns = 1_789_139_862_143_250_999
        timestamp_us = timestamp_ns // 1_000
        sink = RecordingSink()
        with patch('time.time_ns', return_value=timestamp_ns):
            app = AutoAgentApp(runtime_event_sink=sink)
            try:
                result = app.invoke(Workflow('clock-units', nodes=[Node('identity', identity)]), {'value': 2})
                self.assertEqual(result.status, 'completed')
                checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
                self.assertEqual(root_snapshot(checkpoint).captured_at_us, timestamp_us)
                self.assertEqual(root_snapshot(checkpoint).state.session.created_at_us, timestamp_us)
                self.assertEqual(root_snapshot(checkpoint).state.invocation.started_at_us, timestamp_us)
                self.assertEqual(root_snapshot(checkpoint).state.invocation.completed_at_us, timestamp_us)
                for event in sink.events:
                    self.assertEqual(event.occurred_at_us, timestamp_us)
                    self.assertEqual(event, RuntimeEvent.from_record(event.to_record()))
                    self.assertEqual(set(event.to_record()), {
                        'schema_version', 'id', 'session_id', 'invocation_id', 'sequence',
                        'event_name', 'payload', 'delta', 'occurred_at_us',
                    })
                self.assertEqual(checkpoint, RuntimeGraphCheckpoint.from_record(checkpoint.to_record()))
                user = UserEvent(result.session_id, result.invocation_id, 1, 'clock', {})
                self.assertEqual(user.occurred_at_us, timestamp_us)
                self.assertEqual(user, UserEvent.from_record(user.to_record()))
            finally:
                app.close()

    def test_old_clock_schema_is_rejected(self):
        """Earlier schema versions cannot silently reinterpret timestamp units."""
        event = RuntimeEvent('session', 1, SessionOpened({}))
        record = event.to_record()
        record['schema_version'] = 4
        with self.assertRaises(ValueError):
            RuntimeEvent.from_record(record)

    def test_hours_of_execution_preserve_nanosecond_duration(self):
        """A six-hour Operator duration round-trips exactly without changing units."""
        duration = 6 * 60 * 60 * 1_000_000_000
        event = RuntimeEvent('session', 1, OperatorCallCompleted('call', {'value': 2}, duration), 'invocation')
        restored = RuntimeEvent.from_record(json.loads(json.dumps(event.to_record())))
        self.assertEqual(restored.payload.execution_duration_ns, duration)
        self.assertLess(duration, 2**53)
