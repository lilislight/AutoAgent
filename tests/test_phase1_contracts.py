from __future__ import annotations

import unittest
from dataclasses import dataclass
from typing import Any, Iterator
from unittest.mock import patch
from typing_extensions import TypedDict

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core import Operator, ValueContract, Wait


class Value(TypedDict):
    value: int


class SameShape(TypedDict):
    value: int


class ModelValue(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int


class AliasedModelValue(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int = Field(alias="v")


class UnsafeAny(TypedDict):
    value: Any


class UnsafeObject(BaseModel):
    value: object


def identity(value: Value) -> Value:
    return value


def no_input() -> Value:
    return {"value": 1}


def no_output(value: Value) -> None:
    del value


def stream(value: Value) -> Iterator[Value]:
    yield value


class CallableOperator:
    def __call__(self, value: Value) -> Value:
        return value


class ContractTests(unittest.TestCase):
    def test_typed_dict_and_pydantic_contracts_validate(self) -> None:
        """Verify typed dict and pydantic contracts validate."""
        typed = ValueContract.create(Value, location="test")
        model = ValueContract.create(ModelValue, location="test")
        self.assertEqual(typed.validate({"value": 1}), {"value": 1})
        self.assertEqual(model.validate({"value": 2}), ModelValue(value=2))
        with self.assertRaises(TypeError):
            typed.validate({"value": "1"})

    def test_pydantic_alias_uses_one_reversible_durable_record(self) -> None:
        """Verify model aliases serialize and restore with the same canonical key."""

        contract = ValueContract.create(AliasedModelValue, location="alias")
        record = contract.to_record(AliasedModelValue(v=2))
        self.assertEqual(record, {"v": 2})
        self.assertEqual(contract.restore(record), AliasedModelValue(v=2))

    def test_contract_reuses_its_compiled_type_adapter(self) -> None:
        """Verify hot contract boundaries do not rebuild Pydantic adapters."""

        contract = ValueContract.create(Value, location="cached")
        with patch(
            "autoagent.core.operators.contract.TypeAdapter",
            side_effect=AssertionError("TypeAdapter rebuilt"),
        ):
            self.assertEqual(contract.validate({"value": 1}), {"value": 1})
            self.assertEqual(contract.to_record({"value": 2}), {"value": 2})
            self.assertEqual(contract.restore({"value": 3}), {"value": 3})

    def test_contracts_are_nominal(self) -> None:
        """Verify contracts are nominal."""
        self.assertFalse(
            ValueContract.create(Value, location="a").same_as(
                ValueContract.create(SameShape, location="b")
            )
        )

    def test_scalar_dataclass_and_arbitrary_model_are_rejected(self) -> None:
        """Verify scalar dataclass and arbitrary model are rejected."""
        @dataclass
        class Data:
            value: int

        class Arbitrary(BaseModel):
            model_config = ConfigDict(arbitrary_types_allowed=True)
            value: object

        for annotation in (int, Data, Arbitrary, UnsafeAny, UnsafeObject):
            with self.subTest(annotation=annotation):
                with self.assertRaises(TypeError):
                    ValueContract.create(annotation, location="bad")

    def test_operator_allows_zero_or_one_business_input(self) -> None:
        """Verify operator allows zero or one business input."""
        self.assertIsNone(Operator(no_input).contract.input.annotation)
        self.assertIsNone(Operator(no_output).contract.output.annotation)

        def invalid(left: Value, right: Value) -> Value:
            return left or right

        with self.assertRaisesRegex(TypeError, "zero or one"):
            Operator(invalid)

        self.assertIs(Operator(CallableOperator()).contract.output.annotation, Value)

        def missing(value):
            return value

        with self.assertRaisesRegex(TypeError, "explicit contract"):
            Operator(missing)

    def test_stream_operator_declares_chunk_contract(self) -> None:
        """Verify stream operator declares chunk contract."""
        operator = Operator(stream)
        self.assertIsNone(operator.contract.output)
        self.assertIs(operator.contract.stream_chunk.annotation, Value)

    def test_wait_validates_custom_request_and_response(self) -> None:
        """Verify wait validates custom request and response."""
        wait = Wait(Value, ModelValue)
        self.assertEqual(wait.input_contract.validate({"value": 1}), {"value": 1})
        self.assertEqual(wait.output_contract.validate({"value": 2}), ModelValue(value=2))


if __name__ == "__main__":
    unittest.main()
