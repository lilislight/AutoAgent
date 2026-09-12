"""JSON-mode equivalence and ownership boundaries for nullable/Literal records."""
import copy
import random
import unittest
from enum import Enum
from typing import Annotated, Literal, Optional
from typing_extensions import TypedDict
from pydantic import BeforeValidator, ConfigDict

from autoagent.core.operators.contract import ValueContract
from tests.benchmarks.benchmark_core_contracts import (
    NullableDocument, ModelDocument, CustomDocument, UnionDocument)
from tests.test_core_performance_fixes import ModeSensitive


class NestedOptional(TypedDict):
    payload: Optional[NullableDocument]
    choices: list[Literal[1, 2, True, None]]
    values: dict[str, list[int | None]] | None


class Tag(str, Enum):
    A = 'a'


class EnumLiteral(TypedDict):
    tag: Literal[Tag.A]


class TupleDocument(TypedDict):
    values: tuple[int, ...] | None


class ConfiguredDocument(TypedDict):
    __pydantic_config__ = ConfigDict(str_to_lower=True)
    value: str | None


def mode_value(value, info):
    return value + (1 if info.mode == 'python' else 0)


class ValidatedDocument(TypedDict):
    value: Annotated[int, BeforeValidator(mode_value)] | None


async def mutate_nullable(value: NullableDocument) -> NullableDocument:
    value['rows'][0]['value'] = 99
    return value


class ContractFastPathTests(unittest.TestCase):
    def assert_equivalent(self, contract, record):
        before = copy.deepcopy(record)
        try:
            expected = contract.restore(record)
        except (TypeError, ValueError):
            with self.assertRaises((TypeError, ValueError)):
                contract._restore_internal(record)
        else:
            actual = contract._restore_internal(record)
            self.assertEqual(actual, expected)
            self.assertEqual(type(actual), type(expected))
            self.assertEqual(contract.to_record(actual), contract.to_record(expected))
        self.assertEqual(record, before)

    def test_random_nullable_records_match_json_acceptance_and_values(self):
        """Valid, missing, extra and incorrectly typed nested fields match strict JSON."""
        contract = ValueContract.create(NullableDocument, location='test')
        self.assertTrue(contract._python_record_equivalent)
        rng = random.Random(88)
        values = [None, 1, -1, True, 1.5, '1', {}, [], 2**90]
        for _ in range(300):
            rows = []
            for _ in range(rng.randrange(5)):
                row = {'value': rng.choice(values), 'label': rng.choice(['a', 'b', 'c', 1, None])}
                if rng.random() < .4:
                    row['note'] = rng.choice(values)
                if rng.random() < .1:
                    row.pop('value')
                if rng.random() < .1:
                    row['extra'] = 1
                rows.append(row)
            self.assert_equivalent(contract, {'rows': rows})
        self.assert_equivalent(contract, {'rows': [{'value': None, 'label': 'a'}]})

    def test_nested_nullable_literals_and_numeric_boundaries(self):
        """Nested nullable containers and ambiguous primitive literals retain JSON results."""
        contract = ValueContract.create(NestedOptional, location='test')
        self.assertTrue(contract._python_record_equivalent)
        for choice in (1, 2, True, False, None, 1.0, '1', 3):
            for value in (None, 0, -(2**63), 2**90, True, 1.0, '1'):
                self.assert_equivalent(contract, {'payload': None, 'choices': [choice], 'values': {'x': [value]}})
        self.assert_equivalent(contract, {'payload': {'rows': []}, 'choices': [], 'values': None})

    def test_custom_and_non_equivalent_contracts_stay_on_json_path(self):
        """Models, validators, config, tuple/enum and general unions remain excluded."""
        for annotation, record in (
            (ModelDocument, {'rows': []}), (CustomDocument, {'rows': []}),
            (ModeSensitive, {'value': 1}), (UnionDocument, {'rows': [1, 'a']}),
            (EnumLiteral, {'tag': 'a'}), (TupleDocument, {'values': [1]}),
            (ConfiguredDocument, {'value': 'A'}), (ValidatedDocument, {'value': 1}),
        ):
            contract = ValueContract.create(annotation, location='test')
            self.assertFalse(contract._python_record_equivalent, annotation)
            self.assert_equivalent(contract, record)

    def test_restoration_creates_independent_mutable_inputs(self):
        """Two recoveries do not share mutable nested values with each other or State records."""
        contract = ValueContract.create(NullableDocument, location='test')
        record = {'rows': [{'value': 1, 'label': 'a'}]}
        first = contract._restore_internal(record)
        second = contract._restore_internal(record)
        first['rows'][0]['value'] = 99
        first['rows'].append({'value': None, 'label': 'b'})
        self.assertEqual(second, record)
        self.assertEqual(record, {'rows': [{'value': 1, 'label': 'a'}]})

    def test_non_json_values_remain_rejected(self):
        """Fast-path eligibility does not admit tuple, non-finite numbers or foreign objects."""
        contract = ValueContract.create(NullableDocument, location='test')
        for value in (float('nan'), float('inf'), object(), (1,), {1: 2}):
            record = {'rows': [{'value': value, 'label': 'a'}]}
            with self.assertRaises((TypeError, ValueError)):
                contract.restore(record)
            with self.assertRaises((TypeError, ValueError)):
                contract._restore_internal(record)
        self.assert_equivalent(contract, {'rows': [{'value': 1, 'label': 'a', 'note': '\ud800'}]})

    def test_live_operator_mutation_preserves_input_state_and_event_replay(self):
        """A real fast-path Operator gets detached data and produces replayable Events."""
        from autoagent import AutoAgentApp, Node, Workflow
        from autoagent.core.runtime import StateReducer
        class Sink:
            def __init__(self):
                self.events = []
            async def append(self, event):
                self.events.append(event)
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        original = {'rows': [{'value': 1, 'label': 'a'}]}
        try:
            result = app.invoke(Workflow('nullable-mutation', nodes=[Node('entry', mutate_nullable)]), original)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(result.output['rows'][0]['value'], 99)
            self.assertEqual(original['rows'][0]['value'], 1)
            state = app._repository.state(result.session_id)
            self.assertEqual(state.invocation.input['rows'][0]['value'], 1)
            self.assertEqual(StateReducer().reduce(tuple(sink.events)).to_record(), state.to_record())
        finally:
            app.close()
