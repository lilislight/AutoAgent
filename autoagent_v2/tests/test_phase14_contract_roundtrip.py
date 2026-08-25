from __future__ import annotations

import unittest
from enum import Enum

from typing_extensions import TypedDict

from autoagent import AutoAgentApp, Edge, Node, Wait, Workflow
from autoagent.core import ValueContract


class Mode(str, Enum):
    FIRST = "first"
    SECOND = "second"


class RichValue(TypedDict):
    mode: Mode
    coordinates: tuple[int, int]


def identity(value: RichValue) -> RichValue:
    return value


def _value(mode: Mode = Mode.FIRST) -> RichValue:
    return {"mode": mode, "coordinates": (3, 5)}


def _record(mode: str = "first") -> dict[str, object]:
    return {"mode": mode, "coordinates": [3, 5]}


class DurableContractRoundTripTests(unittest.TestCase):
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
            self.assertEqual(len(result.checkpoint.states), 2)
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
        checkpoint = waiting.checkpoint
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
