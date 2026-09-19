from __future__ import annotations

import unittest
from enum import Enum

from pydantic import BaseModel, ConfigDict
from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    Edge,
    InputMappingContext,
    Map,
    Node,
    Wait,
    Workflow,
)
from autoagent.core import ValueContract


class Mode(str, Enum):
    FIRST = "first"
    SECOND = "second"


class RichValue(TypedDict):
    mode: Mode
    coordinates: tuple[int, int]


class FloatValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: float


def identity(value: RichValue) -> RichValue:
    return value


def nonfinite_output(_value: FloatValue) -> FloatValue:
    return FloatValue(value=float("nan"))


def map_float_items(context: InputMappingContext) -> list[FloatValue]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


def _value(mode: Mode = Mode.FIRST) -> RichValue:
    return {"mode": mode, "coordinates": (3, 5)}


def _record(mode: str = "first") -> dict[str, object]:
    return {"mode": mode, "coordinates": [3, 5]}


class DurableContractRoundTripTests(unittest.TestCase):
    def test_contract_rejects_nonfinite_json_output_record(self) -> None:
        """Verify durable output records reject JSON non-finite floats."""

        contract = ValueContract.create(FloatValue, location="non-finite")
        with self.assertRaisesRegex(TypeError, "floats must be finite"):
            contract.to_record(FloatValue(value=float("nan")))

    def test_output_record_failure_closes_operator_and_occurrence_as_failed(
        self,
    ) -> None:
        """Verify output serialization failure preserves a valid failed lifecycle."""

        app = AutoAgentApp()
        try:
            result = app.invoke(
                Workflow(
                    "non-finite-output",
                    nodes=[Node("node", nonfinite_output)],
                ),
                {"value": 1.0},
            )
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "TypeError")
            self.assertIn("floats must be finite", result.error.message)

            state = app._repository.state(result.session_id)
            invocation = state.invocation
            self.assertIsNotNone(invocation)
            assert invocation is not None
            self.assertEqual(
                {item.status for item in invocation.scheduler.occurrences.values()},
                {"failed"},
            )
            self.assertEqual(
                {item.status for item in invocation.scheduler.operator_calls.values()},
                {"failed"},
            )
        finally:
            app.close()

    def test_map_output_record_failure_settles_every_started_call(self) -> None:
        """Verify a mapped serialization failure leaves no live Operator Call."""

        app = AutoAgentApp()
        try:
            result = app.invoke(
                Workflow(
                    "non-finite-map-output",
                    nodes=[
                        Node(
                            "node",
                            nonfinite_output,
                            input_mapping=map_float_items,
                            map=Map(max_parallelism=2),
                        )
                    ],
                ),
                {"items": [{"value": 1.0}, {"value": 2.0}]},
            )
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "TypeError")
            self.assertIn("floats must be finite", result.error.message)

            state = app._repository.state(result.session_id)
            invocation = state.invocation
            self.assertIsNotNone(invocation)
            assert invocation is not None
            calls = tuple(invocation.scheduler.operator_calls.values())
            self.assertTrue(calls)
            self.assertTrue(all(item.status == "failed" for item in calls))
            occurrence = next(iter(invocation.scheduler.occurrences.values()))
            self.assertEqual(occurrence.status, "failed")
        finally:
            app.close()

    def test_contract_distinguishes_domain_validation_from_record_restore(self) -> None:
        """Verify Enum/tuple domain values round-trip through canonical JSON records."""

        contract = ValueContract.create(RichValue, location="round-trip")
        record = contract.to_record(_value())
        self.assertEqual(record, _record())
        self.assertEqual(contract.restore(record), _value())
        with self.assertRaises(TypeError):
            contract.validate(record)
        with self.assertRaises(TypeError):
            contract.restore(_value())

    def test_implicit_edge_restores_the_target_operator_input(self) -> None:
        """Verify a durable Node output is restored before an implicit Edge call."""

        app = AutoAgentApp()
        try:
            result = app.invoke(
                Workflow(
                    "rich-edge",
                    nodes=[Node("first", identity), Node("second", identity)],
                    edges=[Edge("first", "second")],
                ),
                _value(),
            )
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, _record())
        finally:
            app.close()

    def test_wait_restores_its_durable_response_before_completion(self) -> None:
        """Verify Wait request/response records preserve Enum and tuple contracts."""

        app = AutoAgentApp()
        try:
            waiting = app.invoke(
                Workflow(
                    "rich-wait",
                    nodes=[Node("wait", Wait(RichValue, RichValue))],
                ),
                _value(),
            )
            self.assertEqual(waiting.status, "waiting")
            self.assertEqual(waiting.waits[0].request, _record())
            completed = app.resume(
                waiting.ref,
                waiting.waits[0].id,
                _value(Mode.SECOND),
            )
            self.assertEqual(completed.status, "completed")
            self.assertEqual(completed.output, _record("second"))
        finally:
            app.close()

    def test_child_workflow_round_trips_its_independent_runtime_input(self) -> None:
        """Verify Child admission and output restore one canonical contract value."""

        child = Workflow("rich-child", nodes=[Node("child", identity)])
        parent = Workflow("rich-parent", nodes=[Node("child", child)])
        app = AutoAgentApp()
        try:
            result = app.invoke(parent, _value())
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, _record())
            self.assertIn(result.ref, app.resident_invocations())
        finally:
            app.close()

    def test_checkpoint_load_restores_wait_contract_before_resume(self) -> None:
        """Verify a loaded Wait checkpoint preserves its durable rich value schema."""

        workflow = Workflow(
            "rich-checkpoint",
            nodes=[Node("wait", Wait(RichValue, RichValue))],
        )
        source = AutoAgentApp()
        waiting = source.invoke(workflow, _value())
        checkpoint = source.unload_session(waiting.ref, capture_checkpoint=True)
        source.close()

        restored = AutoAgentApp()
        try:
            restored.register_workflow(workflow)
            restored.load_checkpoint(checkpoint)
            completed = restored.resume(
                waiting.ref,
                waiting.waits[0].id,
                _value(Mode.SECOND),
            )
            self.assertEqual(completed.status, "completed")
            self.assertEqual(completed.output, _record("second"))
        finally:
            restored.close()


if __name__ == "__main__":
    unittest.main()
