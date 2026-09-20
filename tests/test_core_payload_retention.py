"""Payload lifetime across graph consumers, durable recovery and ACK failures."""
from __future__ import annotations

from autoagent import RuntimeGraphCheckpoint

from tests.graph_fixtures import (
    load_graph,
    resume_graph_wait,
)

import unittest
from typing_extensions import TypedDict

from autoagent import (AggregationContext, AutoAgentApp, ConditionContext, ContextOperation, ContextPatch, Edge,
                       InputMappingContext, Map, Node, OutputBindingContext,
                       Recovery, RuntimeInfrastructureError, Wait, Workflow)
from autoagent.core.runtime import RuntimeEvent, RuntimeState, SessionCheckpoint, StateReducer
from autoagent.core.runtime._execution_index import ExecutionIndex
from autoagent.core.runtime.state import validate_runtime_state


class Data(TypedDict):
    value: int
    blob: str


def identity(value: Data) -> Data:
    return value


def produce(value: Data) -> Data:
    return {'value': value['value'] + 1, 'blob': 'x' * 65536}


class Collector:
    def __init__(self):
        self.events = []

    async def append(self, event):
        self.events.append(event)


class PayloadRetentionTests(unittest.TestCase):
    def assert_payloads_released(self, state):
        scheduler = state.invocation.scheduler
        self.assertTrue(all(c.input is None and c.output is None for c in scheduler.operator_calls.values()))
        self.assertTrue(all(o.output is None for o in scheduler.occurrences.values()))
        self.assertTrue(all(w.request is None and w.response is None for w in scheduler.waits.values()))

    def test_chain_replay_preserves_events_and_only_retains_live_outputs(self):
        """Every prefix replays and rebuilds the same consumer index without history payloads."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close)
        workflow = Workflow('retention-chain', nodes=[Node(n, produce) for n in ('a', 'b', 'c')],
                            edges=[Edge('a', 'b'), Edge('b', 'c')])
        result = app.invoke(workflow, {'value': 0, 'blob': ''})
        self.assertEqual(result.status, 'completed', result.error)
        state = RuntimeState()
        index = ExecutionIndex(state)
        for original in sink.events:
            event = RuntimeEvent.from_record(original.to_record())
            after = StateReducer().apply(state, event)
            validate_runtime_state(after)
            index = index.advance(state, after, event.delta)
            rebuilt = ExecutionIndex(after)
            self.assertEqual(index.output_consumers, rebuilt.output_consumers)
            self.assertEqual(index.calls_by_occurrence, rebuilt.calls_by_occurrence)
            if event.event_name == 'node_occurrence.completed':
                scheduler = after.invocation.scheduler
                self.assertTrue(all(c.input is None and c.output is None
                                    for c in scheduler.operator_calls.values()))
                self.assertEqual(sum(o.output is not None for o in scheduler.occurrences.values()), 1)
                self.assertEqual(len(event.payload.output['blob']), 65536)
            state = after
        self.assertEqual(state, app._repository.state(result.session_id))
        self.assert_payloads_released(state)
        self.assertEqual(len(result.output['blob']), 65536)

    def test_fanout_join_and_wait_checkpoint_keep_unconsumed_sources(self):
        """A delayed fanout consumer and an unresolved Join retain inputs through reload/resume."""
        def join(context: InputMappingContext) -> Data:
            values = list(context.incoming.values())
            self.assertEqual(len(values), 2)
            self.assertTrue(all(len(v['blob']) == 65536 for v in values))
            return {'value': sum(v['value'] for v in values), 'blob': values[0]['blob']}

        workflow = Workflow('retention-join', nodes=[Node('source', produce), Node('fast', identity),
            Node('wait', Wait(Data, Data)), Node('join', identity, input_mapping=join)],
            edges=[Edge('source', 'fast'), Edge('source', 'wait'), Edge('fast', 'join'), Edge('wait', 'join')])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close)
        waiting = app.invoke(workflow, {'value': 0, 'blob': ''})
        self.assertEqual(waiting.status, 'waiting', waiting.error)
        state = app._repository.state(waiting.session_id)
        for node in ('source', 'fast'):
            self.assertEqual(len(state.invocation.scheduler.occurrences[node + '@root'].output['blob']), 65536)
        self.assertTrue(state.invocation.scheduler.resolutions)
        self.assertEqual(StateReducer().reduce(tuple(sink.events)), state)
        checkpoint = app.unload_session(waiting.ref, capture_checkpoint=True)
        restored = AutoAgentApp()
        self.addCleanup(restored.close)
        restored.register_workflow(workflow)
        load_graph(restored, RuntimeGraphCheckpoint.from_record(checkpoint.to_record()))
        result = resume_graph_wait(restored, waiting.ref, waiting.waits[0].id, {'value': 5, 'blob': 'y' * 65536})
        self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(result.output['value'], 6)
        self.assert_payloads_released(restored._repository.state(result.session_id))

    def test_map_prefix_recovery_reuses_completed_units_and_aggregate(self):
        """Partial Map and post-aggregate prefixes recover without losing or repeating accepted work."""
        called, aggregated = [], []

        def mapped(context: InputMappingContext) -> list[Data]:
            return [{'value': i, 'blob': ''} for i in range(3)]

        def unit(value: Data) -> Data:
            called.append(value['value'])
            return produce(value)

        def aggregate(context: AggregationContext) -> Data:
            aggregated.append(True)
            return {'value': sum(v['value'] for v in context.outputs), 'blob': ''}

        workflow = Workflow('retention-map', nodes=[Node('map', unit, input_mapping=mapped,
            map=Map(max_parallelism=1, aggregate=aggregate), recovery_mode=Recovery('replay_safe'))])
        sink = Collector()
        source = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(source.close)
        self.assertEqual(source.invoke(workflow, {'value': 0, 'blob': ''}).status, 'completed')
        prefixes = []
        for position, event in enumerate(sink.events):
            if event.event_name in {'operator_call.completed', 'node.aggregated', 'node_occurrence.completed'}:
                prefixes.append(sink.events[:position + 1])
        self.assertGreaterEqual(len(prefixes), 5)
        for prefix in prefixes:
            with self.subTest(last=prefix[-1].event_name, sequence=prefix[-1].sequence):
                called.clear()
                aggregated.clear()
                state = StateReducer().reduce(tuple(RuntimeEvent.from_record(e.to_record()) for e in prefix))
                completed = {c.unit_index for c in state.invocation.scheduler.operator_calls.values()
                             if c.status == 'completed'}
                app = AutoAgentApp()
                try:
                    app.register_workflow(workflow)
                    loaded = load_graph(app, SessionCheckpoint.from_state(state))
                    result = app.recover(loaded.invocations[0])
                    self.assertEqual(result.status, 'completed', result.error)
                    self.assertEqual(result.output, {'value': 6, 'blob': ''})
                    self.assertEqual(set(called), set(range(3)) - completed)
                    self.assertEqual(len(aggregated), int(prefix[-1].event_name == 'operator_call.completed'))
                    self.assert_payloads_released(app._repository.state(result.session_id))
                finally:
                    app.close()

    def test_failed_ack_does_not_release_committed_state_until_exact_retry(self):
        """An ambiguous completion ACK keeps old ownership and retries the same immutable Event."""
        class Sink(Collector):
            fail = True

            async def append(self, event):
                self.events.append(event)
                if self.fail and event.event_name == 'node_occurrence.completed' and event.payload.occurrence_id == 'b@root':
                    raise OSError('ambiguous append')

        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close)
        workflow = Workflow('retention-ack', nodes=[Node('a', produce), Node('b', identity)], edges=[Edge('a', 'b')])
        with self.assertRaises(RuntimeInfrastructureError):
            app.invoke(workflow, {'value': 0, 'blob': ''}, session_id='ack')
        repository = app._repository
        before = repository.state('ack')
        event, candidate = repository._pending['ack']
        self.assertIsNotNone(before.invocation.scheduler.occurrences['a@root'].output)
        self.assertIsNone(candidate.invocation.scheduler.occurrences['a@root'].output)
        self.assertEqual(repository.execution_index('ack').output_consumers, ExecutionIndex(before).output_consumers)
        sink.fail = False
        retried = app._runtime_loop.run(repository.settle('ack'))
        self.assertIs(retried, event)
        self.assertIs(sink.events[-1], event)
        self.assertIs(repository.state('ack'), candidate)
        self.assertIsNotNone(before.invocation.scheduler.occurrences['a@root'].output)
        self.assertEqual(repository.execution_index('ack').output_consumers, ExecutionIndex(candidate).output_consumers)

    def test_context_and_external_event_owners_keep_their_values(self):
        """Releasing Runtime State payloads cannot delete explicit Context or external Event data."""
        def bind(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation=(ContextOperation.set('saved', context.output),))

        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close)
        workflow = Workflow('retention-context', nodes=[Node('a', produce, output_binding=bind)])
        result = app.invoke(workflow, {'value': 0, 'blob': ''})
        state = app._repository.state(result.session_id)
        self.assert_payloads_released(state)
        event = next(e for e in sink.events if e.event_name == 'operator_call.completed')
        self.assertIs(state.invocation.context['saved'], event.payload.output)
        self.assertIs(state.invocation.output, event.payload.output)

    def test_cancel_wait_releases_internal_payloads_and_keeps_lifecycle(self):
        """Cancelling a waiting graph drops payloads without corrupting Wait or Call status."""
        app = AutoAgentApp()
        self.addCleanup(app.close)
        workflow = Workflow('retention-cancel', nodes=[Node('a', produce), Node('wait', Wait(Data, Data))],
                            edges=[Edge('a', 'wait')])
        waiting = app.invoke(workflow, {'value': 0, 'blob': ''})
        result = app.cancel(waiting.ref)
        self.assertEqual(result.status, 'cancelled')
        state = app._repository.state(result.session_id)
        validate_runtime_state(state)
        self.assert_payloads_released(state)

    def test_multiple_exits_keep_early_result_while_other_exit_waits(self):
        """A completed Exit stays available until all public Workflow results can be assembled."""
        app = AutoAgentApp()
        self.addCleanup(app.close)
        workflow = Workflow('retention-exits', nodes=[Node('a', produce), Node('early', identity),
            Node('late', Wait(Data, Data))], edges=[Edge('a', 'early'), Edge('a', 'late')])
        waiting = app.invoke(workflow, {'value': 0, 'blob': ''})
        state = app._repository.state(waiting.session_id)
        self.assertEqual(state.invocation.scheduler.occurrences['early@root'].output['value'], 1)
        result = resume_graph_wait(app, waiting.ref, waiting.waits[0].id, {'value': 2, 'blob': 'late'})
        self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(result.output['early']['value'], 1)
        self.assertEqual(result.output['late']['value'], 2)
        self.assert_payloads_released(app._repository.state(result.session_id))

    def test_nested_loop_completion_prefixes_recover_with_compacted_outputs(self):
        """Nested loop back/exit transfers preserve their source until the new scope consumes it."""
        def inner_again(context: ConditionContext) -> bool:
            return context.output['value'] % 3 != 0

        def inner_exit(context: ConditionContext) -> bool:
            return context.output['value'] % 3 == 0

        def outer_again(context: ConditionContext) -> bool:
            return context.output['value'] < 6

        def outer_exit(context: ConditionContext) -> bool:
            return context.output['value'] >= 6

        workflow = Workflow('retention-nested', nodes=[
            Node(name, produce if name == 'body' else identity, recovery_mode=Recovery('replay_safe'))
            for name in ('start', 'outer', 'inner', 'body', 'latch', 'finish')], edges=[
                Edge('start', 'outer'), Edge('outer', 'inner'), Edge('inner', 'body'),
                Edge('body', 'inner', inner_again), Edge('body', 'latch', inner_exit),
                Edge('latch', 'outer', outer_again), Edge('latch', 'finish', outer_exit)])
        sink = Collector()
        source = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(source.close)
        result = source.invoke(workflow, {'value': 0, 'blob': ''})
        self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(result.output['value'], 6)
        state = RuntimeState()
        index = ExecutionIndex(state)
        for original in sink.events:
            event = RuntimeEvent.from_record(original.to_record())
            after = StateReducer().apply(state, event)
            index = index.advance(state, after, event.delta)
            self.assertEqual(index.output_consumers, ExecutionIndex(after).output_consumers)
            state = after
            if event.event_name != 'node_occurrence.completed':
                continue
            self.assertLessEqual(sum(o.output is not None for o in state.invocation.scheduler.occurrences.values()), 1)
            with self.subTest(sequence=event.sequence):
                app = AutoAgentApp()
                try:
                    app.register_workflow(workflow)
                    loaded = load_graph(app, SessionCheckpoint.from_state(state))
                    resumed = app.recover(loaded.invocations[0])
                    self.assertEqual(resumed.status, 'completed', resumed.error)
                    self.assertEqual(resumed.output['value'], 6)
                    self.assert_payloads_released(app._repository.state(resumed.session_id))
                finally:
                    app.close()
