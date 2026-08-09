from __future__ import annotations

import asyncio
import threading
import time
import unittest
from collections.abc import Iterator
from typing import Any

from autoagent.core import (
    AdmissionRejectedError,
    AutoAgentApp,
    ContextPatch,
    Edge,
    EventMode,
    ExecutionContext,
    InvocationConflictError,
    InvocationState,
    Node,
    NodePolicy,
    RecoveryPolicy,
    RuntimeEvent,
    StreamPolicy,
    WaitOperator,
    Workflow,
    WorkflowRegistrationError,
    UserEventMapping,
)
from tests.helpers import decode_checkpoint, decode_events
from tests.helpers import double, identity_int


class TextReducer:
    def __init__(self) -> None:
        self.parts: list[str] = []

    def add(self, chunk: str) -> None:
        self.parts.append(chunk)

    def finish(self) -> str:
        return "".join(self.parts)


def stream_letters(_value: None) -> Iterator[str]:
    return iter(("a", "b", "c"))


def string_event(value: str) -> str:
    return value


def answer_event(value: int) -> dict[str, int]:
    return {"answer": value}


class RecordingSink:
    def __init__(self, *, admissible: bool = True) -> None:
        self.admissible = admissible
        self.events: list[RuntimeEvent] = []
        self.checkpoints: list[Any] = []
        self.admission_calls = 0
        self._event_gate: asyncio.Event | None = None
        self.block_status: str | None = None
        self.block_event_name: str | None = None

    async def wait_until_admissible(self) -> None:
        self.admission_calls += 1
        if not self.admissible:
            await asyncio.Event().wait()

    async def submit_events(self, events: tuple[Any, ...]) -> None:
        decoded = decode_events(events)
        if self.block_status is not None and any(
            getattr(event, "status", None) == self.block_status
            and (
                self.block_event_name is None
                or getattr(event, "event_name", None) == self.block_event_name
            )
            for event in decoded
        ):
            if self._event_gate is None:
                self._event_gate = asyncio.Event()
            await self._event_gate.wait()
        self.events.extend(decoded)

    def offer_checkpoint(self, checkpoint: Any) -> None:
        self.checkpoints.append(decode_checkpoint(checkpoint))

    def release(self, app: AutoAgentApp) -> None:
        gate = self._event_gate
        if gate is not None:
            app._runtime.run(self._release(gate))

    @staticmethod
    async def _release(gate: asyncio.Event) -> None:
        gate.set()


def linear_workflow(*, suffix: str = "") -> Workflow:
    def prepare(value: int) -> int:
        return value + 1

    def finish(value: int) -> int:
        return value * 2

    return Workflow(
        id="linear",
        version=1,
        nodes=[
            Node(id="prepare", operator=prepare, hook_version=f"1{suffix}"),
            Node(id="finish", operator=finish),
        ],
        edges=[Edge(source="prepare", target="finish")],
    )


class CompilerAndRegistryTests(unittest.TestCase):
    def test_revision_does_not_depend_on_callable_import_module_name(self) -> None:
        def operator(value: int) -> int:
            return value

        workflow = Workflow(id="identity", nodes=[Node("node", operator)])
        first = AutoAgentApp()._compiler.compile(workflow)
        original_module = operator.__module__
        operator.__module__ = "__main__"
        try:
            second = AutoAgentApp()._compiler.compile(workflow)
        finally:
            operator.__module__ = original_module
        self.assertEqual(first.workflow_revision_id, second.workflow_revision_id)

    def test_registration_compiles_once_and_source_mutation_does_not_change_ir(self) -> None:
        app = AutoAgentApp()
        workflow = linear_workflow()
        app.register_workflow(workflow)
        revision = app._workflow_registry[workflow.id].workflow_revision_id

        workflow.nodes.clear()
        invocation = app.invoke(workflow, 2)

        self.assertEqual(invocation.result(), {"finish": 6})
        self.assertEqual(invocation.workflow_revision_id, revision)
        app.close()

    def test_one_app_rejects_second_revision_for_same_workflow_id(self) -> None:
        app = AutoAgentApp()
        app.register_workflow(linear_workflow())
        with self.assertRaises(WorkflowRegistrationError):
            app.register_workflow(linear_workflow(suffix="-changed"))
        app.close()


class ExecutionTests(unittest.TestCase):
    def test_parallel_batch_joins_and_applies_distinct_context_patches(self) -> None:
        def start(value: int) -> int:
            return value

        def left(value: int) -> int:
            return value + 1

        def right(value: int) -> int:
            return value + 2

        def left_binding(_context: ExecutionContext, value: int) -> ContextPatch:
            return ContextPatch(invocation={"left": value})

        def right_binding(_context: ExecutionContext, value: int) -> ContextPatch:
            return ContextPatch(invocation={"right": value})

        def join(values: dict[str, int]) -> int:
            return values["left"] + values["right"]

        workflow = Workflow(
            id="parallel",
            nodes=[
                Node("start", start),
                Node("left", left, output_binding=left_binding),
                Node("right", right, output_binding=right_binding),
                Node("join", join),
            ],
            edges=[
                Edge("start", "left"),
                Edge("start", "right"),
                Edge("left", "join"),
                Edge("right", "join"),
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 3, event_mode=EventMode.FULL)
        self.assertEqual(invocation.result(), {"join": 9})
        app.close()

    def test_same_session_rejects_a_second_active_invocation(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def slow(value: int) -> int:
            started.set()
            release.wait(2)
            return value

        workflow = Workflow(id="slow", nodes=[Node("slow", slow)])
        app = AutoAgentApp()
        app.register_workflow(workflow)
        first = app.submit_invoke(workflow, 1, session_id="session")
        self.assertTrue(started.wait(1))
        with self.assertRaises(InvocationConflictError):
            app.submit_invoke(workflow, 2, session_id="session")
        release.set()
        first.wait(2)
        app.close()

    def test_wait_resume_and_checkpoint_recovery(self) -> None:
        def finish(value: str) -> str:
            return value.upper()

        workflow = Workflow(
            id="waiting",
            nodes=[
                Node("ask", WaitOperator(str, str)),
                Node(
                    "finish",
                    finish,
                    policy=NodePolicy(
                        recovery=RecoveryPolicy(mode="replay_safe")
                    ),
                ),
            ],
            edges=[Edge("ask", "finish")],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        waiting = app.invoke(workflow, "name", session_id="conversation")
        self.assertEqual(waiting.state, InvocationState.WAITING)
        checkpoint = waiting.latest_checkpoint
        self.assertIsNotNone(checkpoint)
        app.close()

        recovered_app = AutoAgentApp()
        recovered_app.register_workflow(workflow)
        recovered = recovered_app.recover(checkpoint)  # type: ignore[arg-type]
        self.assertEqual(recovered.state, InvocationState.WAITING)
        completed = recovered_app.resume(recovered, recovered.waits[0].id, "alice")
        self.assertEqual(completed.result(), {"finish": "ALICE"})
        recovered_app.close()

    def test_failure_marks_other_running_nodes_cancelled_and_pending_nodes_skipped(self) -> None:
        async def fail(_value: int) -> int:
            raise ValueError("broken")

        async def sibling(value: int) -> int:
            await asyncio.sleep(0.01)
            return value

        workflow = Workflow(
            id="failure",
            nodes=[
                Node("start", identity_int),
                Node("fail", fail),
                Node("sibling", sibling),
                Node("after", identity_int),
            ],
            edges=[
                Edge("start", "fail"),
                Edge("start", "sibling"),
                Edge("fail", "after"),
                Edge("sibling", "after"),
            ],
        )
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1)
        self.assertEqual(invocation.state, InvocationState.FAILED)
        statuses = {
            (event.subject_id, event.status)
            for event in sink.events
            if event.event_name == "node_state_changed"
        }
        self.assertIn(("fail", "failed"), statuses)
        self.assertIn(("sibling", "cancelled"), statuses)
        self.assertIn(("after", "skipped"), statuses)
        app.close()


class AsyncExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_and_sync_entrypoints_share_one_runtime(self) -> None:
        workflow = linear_workflow()
        app = AutoAgentApp()
        app.register_workflow(workflow)

        async_invocation = await app.ainvoke(workflow, 4, session_id="async")
        sync_invocation = await asyncio.to_thread(
            app.invoke, workflow, 5, session_id="sync"
        )

        self.assertEqual(async_invocation.result(), {"finish": 10})
        self.assertEqual(sync_invocation.result(), {"finish": 12})
        await app.aclose()


class SinkAndStreamTests(unittest.TestCase):
    def test_streaming_operator_reduces_output_and_emits_transient_chunks(self) -> None:
        workflow = Workflow(
            id="streaming-operator",
            nodes=[
                Node(
                    "stream",
                    stream_letters,
                    policy=NodePolicy(stream=StreamPolicy(TextReducer)),
                    stream_user_event_mappings=(
                        UserEventMapping("message_delta", string_event),
                    ),
                    user_event_mappings=(
                        UserEventMapping("message_completed", string_event),
                    ),
                )
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        stream = app.stream_invoke(workflow, None)
        events = list(stream)
        self.assertEqual(
            [(event.type, event.data) for event in events],
            [
                ("message_delta", "a"),
                ("message_delta", "b"),
                ("message_delta", "c"),
                ("message_completed", "abc"),
            ],
        )
        self.assertEqual(stream.invocation.result(), {"stream": "abc"})
        app.close()

    def test_admission_failure_creates_no_session(self) -> None:
        sink = RecordingSink(admissible=False)
        app = AutoAgentApp(runtime_sink=sink, admission_timeout=0.01)
        workflow = linear_workflow()
        app.register_workflow(workflow)
        with self.assertRaises(AdmissionRejectedError):
            app.invoke(workflow, 1, session_id="rejected")
        self.assertNotIn("rejected", app._sessions)
        self.assertEqual(sink.admission_calls, 1)
        app.close()

    def test_strict_stream_advances_only_when_caller_pulls(self) -> None:
        app = AutoAgentApp()
        workflow = linear_workflow()
        app.register_workflow(workflow)
        stream = app.stream_invoke(
            workflow, 2, event_channel="runtime", event_mode=EventMode.STANDARD
        )
        self.assertEqual(stream.invocation.state, InvocationState.CREATED)

        first = next(stream)
        self.assertEqual(first.sequence, 1)
        time.sleep(0.02)
        self.assertEqual(stream.invocation.state, InvocationState.RUNNING)

        remaining = list(stream)
        self.assertTrue(remaining)
        self.assertEqual(stream.invocation.result(), {"finish": 6})
        app.close()

    def test_default_stream_emits_user_events_independent_of_runtime_mode(self) -> None:
        workflow = Workflow(
            id="user-events",
            nodes=[
                Node(
                    "answer",
                    double,
                    user_event_mappings=(
                        UserEventMapping(
                            type="agent_output",
                            transform=answer_event,
                        ),
                    ),
                )
            ],
        )
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        app.register_workflow(workflow)
        stream = app.stream_invoke(
            workflow, 3, event_mode=EventMode.MINIMAL
        )
        event = next(stream)
        self.assertEqual(event.type, "agent_output")
        self.assertEqual(event.data, {"answer": 6})
        self.assertEqual(list(stream), [])
        self.assertEqual(stream.invocation.result(), {"answer": 6})
        self.assertTrue(any(getattr(item, "type", None) == "agent_output" for item in sink.events))
        app.close()

    def test_stream_ends_at_waiting_boundary_without_user_events(self) -> None:
        workflow = Workflow(
            id="stream-wait",
            nodes=[Node("wait", WaitOperator(str, str))],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        stream = app.stream_invoke(workflow, "question")
        self.assertEqual(list(stream), [])
        self.assertEqual(stream.invocation.state, InvocationState.WAITING)
        app.cancel(stream.invocation)
        app.close()

    def test_closing_stream_early_cancels_without_leaving_receiver_blocked(self) -> None:
        app = AutoAgentApp()
        workflow = linear_workflow()
        app.register_workflow(workflow)
        stream = app.stream_invoke(workflow, 2, event_channel="runtime")
        next(stream)
        stream.close()
        self.assertEqual(stream.invocation.state, InvocationState.CANCELLED)
        app.close()

    def test_app_close_cancels_a_stream_that_was_never_pulled(self) -> None:
        app = AutoAgentApp()
        workflow = linear_workflow()
        app.register_workflow(workflow)
        stream = app.stream_invoke(workflow, 2, event_channel="runtime")

        app.close()

        self.assertEqual(stream.invocation.state, InvocationState.CANCELLED)

    def test_cancel_during_sink_backpressure_remains_cancelled(self) -> None:
        sink = RecordingSink()
        sink.block_status = "running"
        sink.block_event_name = "invocation_state_changed"
        app = AutoAgentApp(runtime_sink=sink)
        workflow = linear_workflow()
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, 2)

        for _ in range(100):
            if sink._event_gate is not None:
                break
            time.sleep(0.01)
        self.assertIsNotNone(sink._event_gate)

        finished = threading.Event()
        thread = threading.Thread(
            target=lambda: (app.cancel(invocation), finished.set())
        )
        thread.start()
        time.sleep(0.02)
        self.assertFalse(finished.is_set())

        sink.release(app)
        thread.join(2)
        self.assertTrue(finished.is_set())
        self.assertEqual(invocation.state, InvocationState.CANCELLED)
        self.assertTrue(
            any(
                event.event_name == "invocation_state_changed"
                and event.status == "cancelled"
                for event in sink.events
            )
        )
        app.close()

    def test_cancel_returns_after_sink_accepts_cancel_event(self) -> None:
        started = threading.Event()

        async def slow(value: int) -> int:
            started.set()
            await asyncio.sleep(60)
            return value

        sink = RecordingSink()
        sink.block_status = "cancelled"
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(id="cancel", nodes=[Node("slow", slow)])
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, 1)
        self.assertTrue(started.wait(1))

        finished = threading.Event()

        def cancel() -> None:
            app.cancel(invocation)
            finished.set()

        thread = threading.Thread(target=cancel)
        thread.start()
        for _ in range(100):
            if invocation.state is InvocationState.CANCELLED:
                break
            time.sleep(0.01)
        self.assertEqual(invocation.state, InvocationState.CANCELLED)
        self.assertFalse(finished.is_set())
        sink.release(app)
        thread.join(2)
        self.assertTrue(finished.is_set())
        self.assertTrue(
            any(event.status == "cancelled" for event in sink.events)
        )
        app.close()

    def test_wait_result_returns_only_after_terminal_event_is_accepted(self) -> None:
        sink = RecordingSink()
        sink.block_status = "completed"
        sink.block_event_name = "invocation_state_changed"
        app = AutoAgentApp(runtime_sink=sink)
        workflow = linear_workflow()
        app.register_workflow(workflow)
        returned = threading.Event()
        result: list[Any] = []

        def invoke() -> None:
            result.append(app.invoke(workflow, 2))
            returned.set()

        thread = threading.Thread(target=invoke)
        thread.start()
        for _ in range(100):
            session = next(iter(app._sessions.values()), None)
            if session is not None and session.invocation is not None:
                if session.invocation.state is InvocationState.COMPLETED:
                    break
            time.sleep(0.01)
        self.assertFalse(returned.is_set())
        sink.release(app)
        thread.join(2)
        self.assertTrue(returned.is_set())
        self.assertEqual(result[0].result(), {"finish": 6})
        app.close()


if __name__ == "__main__":
    unittest.main()
