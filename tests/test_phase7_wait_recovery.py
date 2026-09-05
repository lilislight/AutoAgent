from __future__ import annotations

import unittest
from typing_extensions import TypedDict

from autoagent.core import (
    Edge,
    InvocationCancelled,
    InvocationRecoveryRequested,
    Node,
    NodeOccurrenceWaiting,
    OperatorCallStarted,
    RuntimeEvent,
    RuntimeTransitionError,
    Wait,
    WaitResumed,
    Workflow,
)

from tests.test_phase3_scheduler import Harness


class Request(TypedDict):
    question: str


class Response(TypedDict):
    answer: str


def finish(value: Response) -> Response:
    return value


class WaitRecoveryTests(unittest.TestCase):
    def test_wait_binds_occurrence_and_resume_returns_same_occurrence_to_ready(self) -> None:
        """Verify wait binds occurrence and resume returns same occurrence to ready."""
        harness = Harness(
            Workflow(
                "wait",
                nodes=[
                    Node("approval", Wait(Request, Response)),
                    Node("finish", finish),
                ],
                edges=[Edge("approval", "finish")],
            ),
            entry="approval",
        )
        harness.start("approval@root")
        wait = harness.workflow.node("approval").executable
        request = wait.input_contract.validate({"question": "continue?"})
        harness.emit(NodeOccurrenceWaiting("approval@root", "wait-1", request))
        state = harness.state
        self.assertEqual(state.invocation.scheduler.occurrences["approval@root"].status, "waiting")
        self.assertEqual(state.invocation.scheduler.waits["wait-1"].request, request)

        response = wait.output_contract.validate({"answer": "yes"})
        harness.emit(WaitResumed("wait-1", response))
        state = harness.state
        self.assertEqual(state.invocation.scheduler.ready, ("approval@root",))
        self.assertEqual(state.invocation.scheduler.waits["wait-1"].response, response)
        harness.start("approval@root")
        harness.complete("approval@root", {"approval->finish"})
        self.assertEqual(state.invocation.scheduler.waits["wait-1"].status, "resumed")

    def test_invalid_response_and_duplicate_resume_do_not_change_state(self) -> None:
        """Verify invalid response and duplicate resume do not change state."""
        harness = Harness(
            Workflow("wait", nodes=[Node("approval", Wait(Request, Response))]),
            entry="approval",
        )
        harness.start("approval@root")
        harness.emit(
            NodeOccurrenceWaiting(
                "approval@root", "wait-1", {"question": "continue?"}
            )
        )
        wait = harness.workflow.node("approval").executable
        with self.assertRaises(TypeError):
            wait.output_contract.validate({"answer": 1})
        before = harness.state.to_record()
        harness.emit(WaitResumed("wait-1", {"answer": "yes"}))
        resumed = harness.state.to_record()
        with self.assertRaisesRegex(RuntimeTransitionError, "WAIT_NOT_WAITING"):
            harness.emit(WaitResumed("wait-1", {"answer": "again"}))
        self.assertNotEqual(before, resumed)
        self.assertEqual(harness.state.to_record(), resumed)

    def test_recovery_marks_running_calls_lost_and_requeues_occurrence(self) -> None:
        """Verify recovery marks running calls lost and requeues occurrence."""
        harness = Harness(
            Workflow("recover", nodes=[Node("node", finish)]), entry="node"
        )
        harness.start("node@root")
        harness.emit(
            OperatorCallStarted(
                "call-1", "node@root", "finish", 0, {"answer": "pending"}
            )
        )
        harness.emit(InvocationRecoveryRequested())
        scheduler = harness.state.invocation.scheduler
        self.assertEqual(scheduler.operator_calls["call-1"].status, "lost")
        self.assertEqual(scheduler.occurrences["node@root"].status, "ready")
        self.assertEqual(scheduler.occurrences["node@root"].recovery_attempts, 1)
        self.assertEqual(scheduler.ready, ("node@root",))

    def test_recovery_keeps_waits_waiting_and_cancel_converges_all_work(self) -> None:
        """Verify recovery keeps waits waiting and cancel converges all work."""
        harness = Harness(
            Workflow("wait", nodes=[Node("approval", Wait(Request, Response))]),
            entry="approval",
        )
        harness.start("approval@root")
        harness.emit(
            NodeOccurrenceWaiting(
                "approval@root", "wait-1", {"question": "continue?"}
            )
        )
        harness.emit(InvocationRecoveryRequested())
        self.assertEqual(
            harness.state.invocation.scheduler.waits["wait-1"].status, "waiting"
        )
        harness.emit(InvocationCancelled("user requested"))
        state = harness.state
        self.assertEqual(state.invocation.status, "cancelled")
        self.assertEqual(state.invocation.scheduler.waits["wait-1"].status, "cancelled")
        self.assertEqual(
            state.invocation.scheduler.occurrences["approval@root"].status,
            "cancelled",
        )

    def test_wait_and_recovery_events_round_trip(self) -> None:
        """Verify wait and recovery events round trip."""
        payloads = (
            NodeOccurrenceWaiting("node@root", "wait", {"question": "q"}),
            WaitResumed("wait", {"answer": "a"}),
            InvocationRecoveryRequested(),
        )
        for sequence, payload in enumerate(payloads, 1):
            event = RuntimeEvent(
                "session", sequence, payload, invocation_id="invocation"
            )
            self.assertEqual(RuntimeEvent.from_record(event.to_record()), event)


if __name__ == "__main__":
    unittest.main()
