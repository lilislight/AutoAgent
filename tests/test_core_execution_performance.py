"""Correctness boundaries for derived indexes and on-demand input views."""

from tests.graph_fixtures import (
    child_refs,
    join_observed,
    load_graph,
    resume_graph_wait,
)
import asyncio
import unittest
import threading
from unittest.mock import patch

from autoagent import Edge, InputMappingContext, Map, Node, Workflow
from autoagent.core.app import AutoAgentApp
from autoagent.core.runtime import RuntimeRepository, ChildResult
from autoagent.core.runtime._execution_index import ExecutionIndex
from tests.benchmarks.benchmark_core_execution import Value, identity, shrink, loop_workflow


def assert_index(repository, session_id):
    if isinstance(repository.state(session_id).invocation, ChildResult):
        assert session_id not in repository._execution_indexes
        assert session_id not in repository._context_indexes
        return
    actual = repository.execution_index(session_id)
    expected = ExecutionIndex(repository.state(session_id))
    assert actual.child_remaining == expected.child_remaining
    assert actual.started_count == expected.started_count
    assert actual.waiting_count == expected.waiting_count
    assert {k:v for k,v in actual.occurrence_counts.items() if v} == expected.occurrence_counts
    assert actual.running == expected.running
    assert actual.occurrences_by_scope == expected.occurrences_by_scope
    assert actual.calls_by_occurrence == expected.calls_by_occurrence
    assert actual.waits_by_occurrence == expected.waits_by_occurrence


class CheckingRepository(RuntimeRepository):
    async def _settle_pending(self, session_id):
        await super()._settle_pending(session_id)
        if session_id in self._states:
            assert_index(self, session_id)

    def install_states(self, states):
        super().install_states(states)
        for session_id in states:
            assert_index(self, session_id)


class ExecutionPerformanceTests(unittest.TestCase):
    def test_sparse_call_cleanup_replays_like_whole_call_replacement(self):
        """Releasing values preserves Call metadata and matches dense replay at every boundary."""
        from dataclasses import replace
        from autoagent.core.runtime import RuntimeState, StateReducer, StateDelta, StateOperation
        events = []
        class Sink:
            async def append(self, event):
                events.append(event)
        def inputs(context: InputMappingContext) -> list[Value]:
            return [{'value': index} for index in range(30)]
        app = AutoAgentApp(runtime_event_sink=Sink())
        try:
            result = app.invoke(Workflow('compact-calls', nodes=[Node('map', identity,
                input_mapping=inputs, map=Map(max_parallelism=8))]), {})
            self.assertEqual(result.status, 'completed')
            state, reducer, cleared = RuntimeState(), StateReducer(), 0
            for event in events:
                replacements, operations = {}, []
                for operation in event.delta.operations:
                    path = operation.path
                    if (event.event_name in ('node.aggregated', 'node_occurrence.completed', 'node_occurrence.failed')
                            and len(path) == 5 and path[:3] == ('invocation', 'scheduler', 'operator_calls')
                            and path[-1] in ('input', 'output') and operation.value is None):
                        call = replacements.get(path[3], state.invocation.scheduler.operator_calls[path[3]])
                        replacements[path[3]] = replace(call, **{path[-1]: None})
                        cleared += 1
                    else:
                        operations.append(operation)
                    if event.event_name == 'operator_call.completed':
                        self.assertNotEqual(path[-1], 'error')
                operations.extend(StateOperation._from_owned('replace',
                    ('invocation', 'scheduler', 'operator_calls', key), value)
                    for key, value in replacements.items())
                sparse = reducer.apply(state, event)
                dense = reducer.apply(state, replace(event, delta=StateDelta(tuple(operations))))
                self.assertEqual(sparse, dense)
                state = sparse
            self.assertGreater(cleared, 0)
            for call in state.invocation.scheduler.operator_calls.values():
                self.assertEqual(call.status, 'completed')
                self.assertIsNone(call.input)
                self.assertIsNone(call.output)
                self.assertIsNotNone(call.completed_at_us)
            self.assertEqual(state, app._repository.state(result.session_id))
        finally:
            app.close()

    def test_map_call_starts_do_not_repeat_workspace_and_events_replay_exactly(self):
        """Map starts change phase once and preserve the acknowledged State on replay."""
        from autoagent.core.runtime import RuntimeState, StateReducer
        events = []

        class Sink:
            async def append(self, event):
                events.append(event)

        def inputs(context: InputMappingContext) -> list[Value]:
            return [{'value': index} for index in range(30)]

        app = AutoAgentApp(runtime_event_sink=Sink())
        try:
            result = app.invoke(Workflow('sparse-workspace', nodes=[Node('map', identity,
                input_mapping=inputs, map=Map(max_parallelism=8))]), {})
            self.assertEqual(result.status, 'completed')
            starts = [event for event in events if event.event_name == 'operator_call.started']
            self.assertEqual(len(starts), 30)
            phases = []
            for event in starts:
                for operation in event.delta.operations:
                    self.assertNotEqual(operation.path[-1], 'execution')
                    if operation.path[-2:] == ('execution', 'phase'):
                        phases.append(operation.value)
            self.assertEqual(phases, ['executing'])
            state = RuntimeState()
            for event in events:
                state = StateReducer().apply(state, event)
            self.assertEqual(state, app._repository.state(result.session_id))
        finally:
            app.close()
    def test_loop_and_map_indexes_match_state_at_every_ack(self):
        """Incremental lookups match full State scans across loops and parallel calls."""
        def inputs(context: InputMappingContext) -> list[Value]:
            return [{'value': i} for i in context.invocation_input['rows']]
        app = AutoAgentApp(runtime_repository=CheckingRepository())
        try:
            self.assertEqual(app.invoke(loop_workflow(), {'value':0}).output, {'value':100})
            result = app.invoke(Workflow('map-index', nodes=[Node('map', identity,
                input_mapping=inputs, map=Map(max_parallelism=4))]), {'rows':list(range(20))})
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(len(result.output), 20)
        finally:
            app.close()

    def test_wait_child_restore_indexes_match_every_ack(self):
        """Wait/resume, Child admission and checkpoint installation rebuild correct indexes."""
        from examples.core_workflow_api_demo import build_workflow
        app = AutoAgentApp(runtime_repository=CheckingRepository())
        try:
            result = app.invoke(build_workflow(), {'order_id':'index','amount':600,'stock':3})
            self.assertEqual(result.status, 'waiting')
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            self.assertNotIn(result.session_id, app._repository._execution_indexes)
            loaded = load_graph(app, checkpoint)
            result = app.recover(loaded.invocations[0])
            result = resume_graph_wait(app, result.ref, result.waits[0].id, {'approved':True})
            self.assertEqual(result.status, 'completed', result.error)
            child = child_refs(app, result.ref)[0]
            self.assertEqual(join_observed(app, child, timeout=2).status, 'completed')
        finally:
            app.close()

    def test_default_downstream_does_not_materialize_unused_input(self):
        """Large invocation input is detached once for default entry mapping only."""
        from autoagent.core.runtime import thaw
        seen=[]
        def observe(value):
            if isinstance(value, dict) or hasattr(value, 'keys'):
                if 'rows' in value:
                    seen.append(value)
            return thaw(value)
        app=AutoAgentApp()
        try:
            workflow=Workflow('lazy-input',nodes=[Node('a',shrink),Node('b',identity),Node('c',identity)],
                edges=[Edge('a','b'),Edge('b','c')])
            with patch('autoagent.core.executor.workflow_executor.thaw',observe):
                result=app.invoke(workflow,{'rows':list(range(1000))})
            self.assertEqual(result.output,{'value':1000})
            self.assertEqual(len(seen),1)
        finally:
            app.close()

    def test_custom_mapper_keeps_isolated_original_input(self):
        """Custom downstream hooks still see an isolated complete invocation input."""
        def mapped(context: InputMappingContext) -> Value:
            context.invocation_input['rows'].append(999)
            return {'value':len(context.invocation_input['rows'])}
        app=AutoAgentApp()
        value={'rows':[1,2]}
        try:
            result=app.invoke(Workflow('custom-input',nodes=[Node('a',shrink),Node('b',identity,input_mapping=mapped)],
                edges=[Edge('a','b')]),value)
            self.assertEqual(result.output,{'value':3})
            self.assertEqual(value,{'rows':[1,2]})
            self.assertEqual(app._repository.state(result.session_id).invocation.input['rows'],(1,2))
        finally:
            app.close()

    def test_cancel_indexes_match_acknowledged_state(self):
        """Cancellation updates running occurrence and call indexes with terminal State."""
        async def run():
            entered=threading.Event()
            async def block(value: Value) -> Value:
                entered.set()
                await asyncio.sleep(1)
                return value
            app=AutoAgentApp(runtime_repository=CheckingRepository())
            try:
                task=asyncio.create_task(app.ainvoke(Workflow('cancel-index',nodes=[Node('a',block)]),{'value':1},session_id='cancel-index'))
                for _ in range(200):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.01)
                self.assertTrue(entered.is_set())
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                for _ in range(200):
                    if all(app._repository.state(sid).invocation.terminal for sid in app._repository.session_ids()):
                        break
                    await asyncio.sleep(0.01)
                for sid in app._repository.session_ids():
                    assert_index(app._repository,sid)
                    if not isinstance(app._repository.state(sid).invocation, ChildResult):
                        self.assertFalse(app._repository.execution_index(sid).running)
            finally:
                await app.aclose()
        asyncio.run(run())
