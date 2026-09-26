"""Core Child admission and completion integrity at the external Sink boundary."""
from __future__ import annotations

from tests.graph_fixtures import (
    graph_bundle,
    join_observed,
    load_graph,
    session_checkpoints,
)

import asyncio
import unittest
from typing_extensions import TypedDict
from autoagent import (AppCheckpoint, AutoAgentApp, InputMappingContext, Map, Node,
                       Recovery, RuntimeInfrastructureError, Workflow)
from autoagent.core.runtime import RuntimeEvent, RuntimeRepository, SessionCheckpoint, SessionOpened, StateReducer


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def map_items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input['items']


class Collector:
    def __init__(self):
        self.events = []
        self.by_sequence = {}

    async def append(self, event):
        key = event.session_id, event.sequence
        if key in self.by_sequence:
            if self.by_sequence[key] != event:
                raise RuntimeError('Runtime Event sequence fork')
            return
        self.by_sequence[key] = event
        self.events.append(event)


def checkpoint_from_events(events):
    return graph_bundle(tuple(SessionCheckpoint.from_state(StateReducer().reduce(tuple(
        RuntimeEvent.from_record(event.to_record()) for event in events if event.session_id == sid)))
        for sid in dict.fromkeys(event.session_id for event in events)))


class ChildSinkIntegrityTests(unittest.TestCase):
    def test_failed_append_cannot_publish_partial_admission(self):
        """Core publishes admission State only after the Sink acknowledges the exact Event."""
        class Sink(Collector):
            fail = True
            async def append(self, event):
                await super().append(event)
                if self.fail:
                    raise OSError('ambiguous append')
        sink = Sink()
        repository = RuntimeRepository(sink=sink)
        async def exercise():
            with self.assertRaises(RuntimeInfrastructureError):
                await repository.commit(session_id='admission', invocation_id=None, payload=SessionOpened({}))
            self.assertIsNone(repository.state('admission').session)
            event = repository._pending['admission'][0]
            sink.fail = False
            self.assertIs(await repository.settle('admission'), event)
            self.assertEqual(repository.state('admission').sequence, 1)
            self.assertEqual(len(sink.events), 1)
        asyncio.run(exercise())

    def test_child_handle_has_self_contained_checkpoint(self):
        """A completed Handle retains its result in a compact Session checkpoint."""
        app = AutoAgentApp()
        self.addCleanup(app.close)
        child = Workflow('handle-child', nodes=[Node('work', identity)])
        parent = Workflow('handle-parent', nodes=[Node('spawn', child, execution_mode='spawn')])
        result = app.invoke(parent, {'value': 1})
        self.assertEqual(result.status, 'completed', result.error)
        child_result = join_observed(app, result.output, timeout=2)
        self.assertEqual(child_result.output, {'value': 1})
        graph = app.unload_session(result.ref, capture_checkpoint=True)
        checkpoint = next(s for s in graph.sessions if s.session_id == result.output.child_session_id)
        restored = SessionCheckpoint.from_record(checkpoint.to_record())
        self.assertEqual(restored.state.invocation.id, result.output.child_invocation_id)
        from autoagent.core import ChildResult
        self.assertIsInstance(restored.state.invocation, ChildResult)
        self.assertEqual(restored.state.invocation.output, {"value": 1})

    def test_spawn_map_admits_children_before_parent_acceptance(self):
        """Each accepted parent unit has a Child Invocation in the acknowledged Event prefix."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close)
        child = Workflow('map-child', nodes=[Node('work', identity)])
        parent = Workflow('map-parent', nodes=[Node('spawn', child, execution_mode='spawn',
            input_mapping=map_items, map=Map(max_parallelism=2))])
        result = app.invoke(parent, {'items': [{'value': i} for i in range(3)]}, session_id='parent')
        self.assertEqual(result.status, 'completed', result.error)
        for handle in result.output:
            self.assertEqual(join_observed(app, handle, timeout=2).status, 'completed')
        states = {}
        accepted = 0
        from autoagent.core.runtime import RuntimeState
        for event in sink.events:
            if event.event_name == 'child_invocation.phase_changed' and event.payload.phase == 'accepted':
                plan = states[event.session_id].invocation.child_plans[event.payload.creation_id]
                unit = plan.units[event.payload.unit_index]
                child_state = states[unit.session_id]
                self.assertEqual(child_state.invocation.id, unit.invocation_id)
                self.assertTrue(child_state.invocation.scheduler.initialized)
                accepted += 1
            states[event.session_id] = StateReducer().apply(states.get(event.session_id, RuntimeState()), event)
        self.assertEqual(accepted, 3)

    def test_child_completion_is_acknowledged_before_parent_terminal_phase(self):
        """A rejected Child completion leaves a recoverable prefix without false parent completion."""
        class Sink(Collector):
            fail = True
            async def append(self, event):
                if self.fail and event.session_id != 'parent' and event.event_name == 'invocation.completed':
                    raise OSError('Child completion not acknowledged')
                await super().append(event)
        sink = Sink()
        child = Workflow('terminal-child', nodes=[Node('work', identity, recovery_mode=Recovery('replay_safe'))])
        parent = Workflow('terminal-parent', nodes=[Node('child', child)])
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                source.invoke(parent, {'value': 1}, session_id='parent')
            checkpoint = checkpoint_from_events(tuple(sink.events))
            root = next(s.state.invocation for s in session_checkpoints(checkpoint) if s.session_id == 'parent')
            self.assertNotEqual(root.status, 'completed')
            self.assertNotEqual(next(iter(root.child_plans.values())).units[0].phase, 'terminal')
        finally:
            sink.fail = False
            source.close()
        restored = AutoAgentApp()
        self.addCleanup(restored.close)
        restored.register_workflow(parent)
        ref = next(ref for ref in load_graph(restored, checkpoint).invocations if ref.session_id == 'parent')
        result = restored.recover(ref)
        self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(result.output, {'value': 1})
