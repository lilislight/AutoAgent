"""Safety of transient output discard, plain records and aggregate-stage release."""
from __future__ import annotations
import unittest
from unittest.mock import patch
from enum import Enum
from typing_extensions import TypedDict
from pydantic import BaseModel, ConfigDict, Field, field_serializer
from autoagent import (AggregationContext, AutoAgentApp, ContextPatch, InputMappingContext,
                       Map, Node, OutputBindingContext, Recovery, RuntimeInfrastructureError, Workflow)
from autoagent.core import NodeExecutor, ValueContract, WorkflowCompiler
from autoagent.core.runtime import RuntimeEvent, SessionCheckpoint, StateReducer
from tests.test_core_payload_retention import Collector, Data, produce


class Rows(TypedDict):
    rows: list[dict[str, int]]


class Numbers(TypedDict):
    number: float
    integer: int
    text: str


class Tag(str, Enum):
    A = 'a'


class Rich(BaseModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=True)
    label: str = Field(alias='wire_label')
    coordinates: tuple[int, int]
    tag: Tag

    @field_serializer('label')
    def serialize_label(self, value):
        return value.upper()


def inputs(context: InputMappingContext) -> list[Data]:
    return [{'value': i, 'blob': ''} for i in range(3)]


def aggregate(context: AggregationContext) -> Data:
    # Existing aggregators still receive mutable Python-domain values.
    assert all(type(value) is dict for value in context.outputs)
    context.outputs[0]['blob'] = 'mutated by aggregator'
    return {'value': sum(value['value'] for value in context.outputs), 'blob': ''}


class TransientMemoryTests(unittest.TestCase):
    def test_plain_record_encoding_remains_detached_from_external_containers(self):
        """Skipping identity serialization cannot alias caller-owned mutable containers."""
        contract = ValueContract.create(Rows, location='test')
        original = {'rows': [{'id': 1}]}
        expected = contract._adapter.dump_python(contract.validate(original), mode='json', round_trip=True, by_alias=True)
        with patch.object(contract._adapter, 'dump_python', side_effect=AssertionError('redundant dump')):
            record = contract.to_record(original)
            restored = contract._restore_internal(record)
        self.assertEqual(record, expected)
        restored['rows'][0]['id'] = 3
        original['rows'][0]['id'] = 2
        self.assertEqual(record, {'rows': [{'id': 1}]})
        with self.assertRaises(TypeError):
            contract._restore_internal({'rows': [], 'extra': 1})

    def test_model_alias_serializer_enum_and_tuple_keep_record_encoding(self):
        """Custom model serialization stays outside the identity-record optimization."""
        contract = ValueContract.create(Rich, location='test')
        self.assertFalse(contract._python_record_equivalent)
        value = Rich(label='hello', coordinates=(1, 2), tag=Tag.A)
        record = contract.to_record(value)
        self.assertEqual(record, {'wire_label': 'HELLO', 'coordinates': [1, 2], 'tag': 'a'})
        restored = contract.restore(record)
        self.assertIsInstance(restored, Rich)
        self.assertIs(restored.tag, Tag.A)
        self.assertEqual(restored.coordinates, (1, 2))

    def test_plain_encoding_keeps_numeric_and_unicode_boundaries(self):
        """Plain record reuse preserves large integers and strings but rejects non-finite floats."""
        contract = ValueContract.create(Numbers, location='test')
        for number in (0.0, -0.0, 1.5):
            value = {'number': number, 'integer': 2**90, 'text': '\ud800'}
            expected = contract._adapter.dump_python(contract.validate(value), mode='json', round_trip=True, by_alias=True)
            self.assertEqual(contract.to_record(value), expected)
        for number in (float('nan'), float('inf'), -float('inf')):
            with self.assertRaisesRegex(TypeError, 'finite'):
                contract.to_record({'number': number, 'integer': 1, 'text': 'ok'})

    def test_aggregation_cannot_clear_a_still_running_call(self):
        """An invalid early aggregate transition leaves live Call input and State unchanged."""
        from autoagent.core.runtime import Aggregated, OperatorCallStarted
        from autoagent import RuntimeTransitionError
        from tests.test_phase3_scheduler import Harness
        harness = Harness(Workflow('aggregate-active', nodes=[Node('work', produce)]), entry='work')
        harness.start('work@root')
        harness.emit(OperatorCallStarted('call', 'work@root', 'produce', 0, {'value': 0, 'blob': 'input'}))
        before = harness.state
        with self.assertRaisesRegex(RuntimeTransitionError, 'AGGREGATION_CALLS_ACTIVE'):
            harness.emit(Aggregated('work@root', {'value': 1, 'blob': ''}, 0))
        self.assertIs(harness.state, before)

    def test_no_aggregate_map_recovers_accepted_outputs_without_reexecution(self):
        """Discarding transient results never discards accepted records needed by recovery."""
        called = []
        def unit(value: Data) -> Data:
            called.append(value['value'])
            return produce(value)
        workflow = Workflow('transient-no-aggregate', nodes=[Node('map', unit, input_mapping=inputs,
            map=Map(max_parallelism=1), recovery_mode=Recovery('replay_safe'))])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close)
        original = app.invoke(workflow, {'value': 0, 'blob': ''})
        self.assertEqual(original.status, 'completed', original.error)
        prefix_end = next(i for i, e in enumerate(sink.events) if e.event_name == 'operator_call.completed')
        state = StateReducer().reduce(tuple(sink.events[:prefix_end + 1]))
        called.clear()
        restored = AutoAgentApp()
        self.addCleanup(restored.close)
        restored.register_workflow(workflow)
        loaded = restored.load_checkpoint(SessionCheckpoint.from_state(state))
        result = restored.recover(loaded.invocations[0])
        self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(result.output, original.output)
        self.assertEqual(called, [1, 2])

    def test_aggregate_ack_releases_calls_before_binding_and_preserves_event_data(self):
        """Binding sees compact State, while external Call Events remain unmodified by aggregation."""
        sink = Collector()
        observed = []
        def binding(context: OutputBindingContext) -> ContextPatch:
            inv = app._repository.state('aggregate').invocation
            observed.append(True)
            self.assertTrue(all(c.input is None and c.output is None for c in inv.scheduler.operator_calls.values()))
            self.assertIsNone(inv.scheduler.occurrences['map@root'].execution.mapped_input)
            return ContextPatch()
        workflow = Workflow('transient-aggregate', nodes=[Node('map', produce, input_mapping=inputs,
            map=Map(max_parallelism=1, aggregate=aggregate), output_binding=binding)])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close)
        result = app.invoke(workflow, {'value': 0, 'blob': ''}, session_id='aggregate')
        self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(observed, [True])
        call_event = next(e for e in sink.events if e.event_name == 'operator_call.completed')
        self.assertEqual(call_event.payload.output['blob'], 'x' * 65536)
        self.assertEqual(StateReducer().reduce(tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)),
                         app._repository.state('aggregate'))

    def test_failed_aggregate_ack_preserves_old_calls_until_exact_retry(self):
        """An ambiguous aggregate append cannot prematurely clear acknowledged Call results."""
        class Sink(Collector):
            fail = True
            async def append(self, event):
                self.events.append(event)
                if self.fail and event.event_name == 'node.aggregated':
                    raise OSError('ambiguous aggregate')
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close)
        workflow = Workflow('aggregate-retry', nodes=[Node('map', produce, input_mapping=inputs,
            map=Map(max_parallelism=1, aggregate=aggregate), recovery_mode=Recovery('replay_safe'))])
        with self.assertRaises(RuntimeInfrastructureError):
            app.invoke(workflow, {'value': 0, 'blob': ''}, session_id='aggregate')
        repository = app._repository
        before = repository.state('aggregate')
        event, after = repository._pending['aggregate']
        self.assertTrue(all(c.output is not None for c in before.invocation.scheduler.operator_calls.values()))
        self.assertTrue(all(c.output is None for c in after.invocation.scheduler.operator_calls.values()))
        self.assertIsNotNone(before.invocation.scheduler.occurrences['map@root'].execution.mapped_input)
        sink.fail = False
        self.assertIs(app._runtime_loop.run(repository.settle('aggregate')), event)
        self.assertIs(sink.events[-1], event)
        restored = AutoAgentApp()
        self.addCleanup(restored.close)
        restored.register_workflow(workflow)
        loaded = restored.load_checkpoint(SessionCheckpoint.from_state(after))
        result = restored.recover(loaded.invocations[0])
        self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(result.output, {'value': 6, 'blob': ''})


class ExecutorContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_discard_mode_still_validates_recovered_call_output(self):
        """Discard mode cannot bypass the contract check for externally restored Call records."""
        from types import SimpleNamespace
        executor = NodeExecutor()
        self.addCleanup(executor.close)
        workflow = WorkflowCompiler().compile_or_raise(Workflow('invalid-recovered', nodes=[Node('work', produce)]))
        with self.assertRaises(TypeError):
            await executor.execute(workflow.node('work'), 'work@root', {'value': 0, 'blob': ''},
                completed_calls={0: SimpleNamespace(output={'value': 'invalid', 'blob': ''})},
                aggregate=False, _retain_outputs=False)

    async def test_direct_executor_default_still_returns_raw_results(self):
        """The internal discard option does not change direct NodeExecutor result contracts."""
        executor = NodeExecutor()
        self.addCleanup(executor.close)
        workflow = WorkflowCompiler().compile_or_raise(Workflow('raw-default', nodes=[
            Node('map', produce, input_mapping=inputs, map=Map(max_parallelism=1))]))
        node = workflow.node('map')
        result = await executor.execute(node, 'map@root', [{'value': 0, 'blob': ''}])
        self.assertEqual(result.output, [{'value': 1, 'blob': 'x' * 65536}])
        with self.assertRaises(ValueError):
            await executor.execute(node, 'map@root', [], _retain_outputs=False)
