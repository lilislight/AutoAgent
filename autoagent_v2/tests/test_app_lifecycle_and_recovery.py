from __future__ import annotations

import asyncio
import copy
import threading
import time
import unittest
from dataclasses import replace
from typing import Any
from uuid import uuid4

from autoagent.core import (
    AdmissionRejectedError,
    AutoAgentApp,
    Edge,
    EventMode,
    InvocationConflictError,
    InvocationState,
    InvocationStateError,
    Node,
    RecoveryError,
    RuntimeSink,
    WaitOperator,
    Workflow,
    WorkflowNotRegisteredError,
)
from tests.helpers import decode_checkpoint, decode_events


TestInput = int | dict[str, list[int]]


def identity(value: TestInput) -> TestInput:
    return value


def workflow(workflow_id: str = "workflow") -> Workflow:
    return Workflow(workflow_id, nodes=[Node("node", identity)])


class CaptureSink:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self.checkpoints: list[Any] = []

    async def wait_until_admissible(self) -> None:
        return None

    async def submit_events(self, events: tuple[Any, ...]) -> None:
        self.events.extend(decode_events(events))

    def offer_checkpoint(self, checkpoint: Any) -> None:
        self.checkpoints.append(decode_checkpoint(checkpoint))


class BrokenAdmissionSink(CaptureSink):
    async def wait_until_admissible(self) -> None:
        raise ConnectionError("closed")


class AppLifecycleTests(unittest.TestCase):
    def test_unregistered_workflow_object_and_id_are_rejected(self) -> None:
        app = AutoAgentApp()
        value = workflow()
        with self.assertRaises(WorkflowNotRegisteredError):
            app.invoke(value, 1)
        with self.assertRaises(WorkflowNotRegisteredError):
            app.invoke("missing", 1)
        app.close()

    def test_equivalent_but_distinct_source_object_is_not_registered(self) -> None:
        app = AutoAgentApp()
        registered = workflow()
        equivalent = workflow()
        app.register_workflow(registered)
        with self.assertRaises(WorkflowNotRegisteredError):
            app.invoke(equivalent, 1)
        self.assertEqual(app.invoke("workflow", 2).result(), {"node": 2})
        app.close()

    def test_closed_app_rejects_registration_and_execution(self) -> None:
        app = AutoAgentApp()
        value = workflow()
        app.register_workflow(value)
        app.close()
        app.close()
        with self.assertRaises(RuntimeError):
            app.register_workflow(workflow("other"))
        with self.assertRaises(RuntimeError):
            app.invoke(value, 1)

    def test_invalid_event_mode_leaves_no_session(self) -> None:
        app = AutoAgentApp()
        value = workflow()
        app.register_workflow(value)
        with self.assertRaises(ValueError):
            app.invoke(value, 1, session_id="invalid", event_mode="unknown")
        self.assertNotIn("invalid", app._sessions)
        app.close()

    def test_session_id_cannot_move_between_workflows(self) -> None:
        app = AutoAgentApp()
        first = workflow("first")
        second = workflow("second")
        app.register_workflow(first)
        app.register_workflow(second)
        app.invoke(first, 1, session_id="shared")
        with self.assertRaises(InvocationConflictError):
            app.invoke(second, 2, session_id="shared")
        app.close()

    def test_result_before_completion_and_wait_timeout_raise(self) -> None:
        release = threading.Event()

        def slow(value: int) -> int:
            release.wait(1)
            return value

        app = AutoAgentApp()
        value = Workflow("slow", nodes=[Node("slow", slow)])
        app.register_workflow(value)
        invocation = app.submit_invoke(value, 1)
        with self.assertRaises(InvocationStateError):
            invocation.result()
        with self.assertRaises(TimeoutError):
            invocation.wait(0.001)
        release.set()
        invocation.wait(2)
        app.close()

    def test_failed_and_cancelled_results_raise(self) -> None:
        failed_app = AutoAgentApp()
        def fail(_value: int) -> int:
            raise ZeroDivisionError("failed")

        failed_workflow = Workflow("failed", nodes=[Node("node", fail)])
        failed_app.register_workflow(failed_workflow)
        failed = failed_app.invoke(failed_workflow, 1)
        with self.assertRaises(InvocationStateError):
            failed.result()
        failed_app.close()

        started = threading.Event()

        async def slow(value: int) -> int:
            started.set()
            await asyncio.sleep(60)
            return value

        cancelled_app = AutoAgentApp()
        cancelled_workflow = Workflow("cancelled", nodes=[Node("node", slow)])
        cancelled_app.register_workflow(cancelled_workflow)
        cancelled = cancelled_app.submit_invoke(cancelled_workflow, 1)
        self.assertTrue(started.wait(1))
        cancelled_app.cancel(cancelled)
        with self.assertRaises(InvocationStateError):
            cancelled.result()
        cancelled_app.close()

    def test_cancel_rejects_terminal_or_unknown_invocation(self) -> None:
        app = AutoAgentApp()
        value = workflow()
        app.register_workflow(value)
        completed = app.invoke(value, 1)
        with self.assertRaises(InvocationStateError):
            app.cancel(completed)
        with self.assertRaises(InvocationStateError):
            app.cancel(uuid4())
        app.close()

    def test_admission_exception_is_a_core_infrastructure_error(self) -> None:
        app = AutoAgentApp(runtime_sink=BrokenAdmissionSink())
        value = workflow()
        app.register_workflow(value)
        with self.assertRaises(AdmissionRejectedError) as captured:
            app.invoke(value, 1, session_id="not-created")
        self.assertIsInstance(captured.exception.__cause__, ConnectionError)
        self.assertNotIn("not-created", app._sessions)
        app.close()

    def test_generated_session_id_is_a_nonempty_string(self) -> None:
        app = AutoAgentApp()
        value = workflow()
        app.register_workflow(value)
        invocation = app.invoke(value, 1)
        self.assertIsInstance(invocation.session_id, str)
        self.assertTrue(invocation.session_id)
        app.close()

    def test_terminal_snapshot_retains_latest_checkpoint(self) -> None:
        app = AutoAgentApp()
        value = workflow()
        app.register_workflow(value)
        invocation = app.invoke(value, {"nested": [1]})
        snapshot = invocation.snapshot()
        snapshot.output["node"]["nested"].append(2)  # type: ignore[index]
        self.assertEqual(invocation.output, {"node": {"nested": [1]}})
        self.assertIsNotNone(invocation.latest_checkpoint)
        self.assertEqual(
            invocation.latest_checkpoint.invocation_state,  # type: ignore[union-attr]
            InvocationState.COMPLETED.value,
        )
        app.close()


class RecoveryValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.value = Workflow(
            "recover",
            nodes=[Node("wait", WaitOperator(str, str))],
        )
        sink = CaptureSink()
        source = AutoAgentApp(runtime_sink=sink)
        source.register_workflow(self.value)
        waiting = source.invoke(self.value, "question", session_id="session")
        self.checkpoint = waiting.latest_checkpoint
        source.close()

    def recover_app(self) -> AutoAgentApp:
        app = AutoAgentApp()
        app.register_workflow(self.value)
        return app

    def test_rejects_unknown_schema_version(self) -> None:
        app = self.recover_app()
        with self.assertRaisesRegex(RecoveryError, "schema version"):
            app.recover(replace(self.checkpoint, schema_version=2))
        app.close()

    def test_rejects_revision_mismatch(self) -> None:
        app = self.recover_app()
        with self.assertRaisesRegex(RecoveryError, "Revision"):
            app.recover(
                replace(self.checkpoint, workflow_revision_id="recover:missing")
            )
        app.close()

    def test_terminal_checkpoint_restores_session_and_result_without_execution(self) -> None:
        source = AutoAgentApp()
        value = workflow()
        source.register_workflow(value)
        completed = source.invoke(value, {"value": 1}, session_id="terminal")
        checkpoint = completed.latest_checkpoint
        self.assertIsNotNone(checkpoint)
        source.close()

        restored_app = AutoAgentApp()
        restored_app.register_workflow(value)
        restored = restored_app.recover(checkpoint)  # type: ignore[arg-type]
        self.assertEqual(restored.state, InvocationState.COMPLETED)
        self.assertEqual(restored.result(), {"node": 1})
        self.assertNotIn(restored.id, restored_app._active)
        restored_app.close()

    def test_rejects_negative_sequences(self) -> None:
        app = self.recover_app()
        with self.assertRaisesRegex(RecoveryError, "cannot be negative"):
            app.recover(replace(self.checkpoint, runtime_event_sequence=-1))
        app.close()

    def test_waiting_checkpoint_requires_at_least_one_wait(self) -> None:
        app = self.recover_app()
        with self.assertRaisesRegex(RecoveryError, "Waiting Checkpoint"):
            app.recover(replace(self.checkpoint, waits=()))
        app.close()

    def test_rejects_unknown_ready_node_and_edge(self) -> None:
        app = self.recover_app()
        scheduler = self.checkpoint.scheduler_state
        request = scheduler.ready[0] if scheduler.ready else self.checkpoint.waits[0].request
        unknown_node = replace(request, node_id="missing")
        with self.assertRaisesRegex(RecoveryError, "unknown ready Node"):
            app.recover(
                replace(
                    self.checkpoint,
                    invocation_state=InvocationState.RUNNING.value,
                    waits=(),
                    scheduler_state=replace(scheduler, ready=(unknown_node,)),
                )
            )
        app.close()

    def test_rejects_duplicate_active_recovery(self) -> None:
        app = self.recover_app()
        first = app.recover(self.checkpoint)
        self.assertEqual(first.state, InvocationState.WAITING)
        with self.assertRaises(InvocationConflictError):
            app.recover(self.checkpoint)
        app.cancel(first)
        app.close()

    def test_recovered_waiting_does_not_consume_a_response(self) -> None:
        app = self.recover_app()
        recovered = app.recover(self.checkpoint)
        self.assertEqual(recovered.state, InvocationState.WAITING)
        self.assertFalse(recovered.done())
        app.cancel(recovered)
        app.close()


if __name__ == "__main__":
    unittest.main()
