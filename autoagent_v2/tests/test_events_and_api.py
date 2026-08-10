from __future__ import annotations

import asyncio
import copy
import json
import threading
import time
import unittest
from dataclasses import dataclass
from typing import Any

from autoagent.core import (
    AdmissionRejectedError,
    AutoAgentApp,
    ContextPatch,
    Edge,
    EventMode,
    InputMappingContext,
    OutputBindingContext,
    InvocationConflictError,
    InvocationState,
    Node,
    NodePolicy,
    RecoveryPolicy,
    UserEventMapping,
    WaitOperator,
    Workflow,
)
from tests.helpers import (
    always_true,
    context_input_int,
    double,
    identity_int,
    identity_str,
    increment,
    uppercase,
    decode_checkpoint,
    decode_events,
)


@dataclass(frozen=True)
class StructuredValue:
    city: str
    temperatures: tuple[int, ...]


def bind_invocation_value(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation={"value": context.output})


def int_user_event(value: int) -> dict[str, int]:
    return {"value": value}


def string_user_event(value: str) -> str:
    return value


def structured_value(_input: None) -> StructuredValue:
    return StructuredValue("Paris", (18, 19))


def identity_none(value: None) -> None:
    return value


def bind_name(_context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation={"profile": {"name": "Ada"}})


def bind_age(_context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation={"profile": {"age": 37}})


class RecordingSink:
    def __init__(self, *, admissible: bool = True) -> None:
        self.admissible = admissible
        self.events: list[Any] = []
        self.checkpoints: list[Any] = []

    async def wait_until_admissible(self) -> None:
        if not self.admissible:
            await asyncio.Event().wait()

    async def submit_events(self, events: tuple[Any, ...]) -> None:
        self.events.extend(decode_events(events))

    def offer_checkpoint(self, checkpoint: Any) -> None:
        self.checkpoints.append(decode_checkpoint(checkpoint))


class FailingSink(RecordingSink):
    async def submit_events(self, events: tuple[Any, ...]) -> None:
        raise ConnectionError("sink closed")


def workflow() -> Workflow:
    return Workflow(
        "events",
        nodes=[
            Node(
                "start",
                increment,
                input_mapping=context_input_int,
                output_binding=bind_invocation_value,
                user_event_mappings=(
                    UserEventMapping("agent_output", int_user_event),
                ),
            ),
            Node("finish", double),
        ],
        edges=[Edge("start", "finish", condition=always_true)],
    )


class EventModeTests(unittest.TestCase):
    def _events(self, mode: EventMode) -> list[Any]:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        value = workflow()
        app.register_workflow(value)
        app.invoke(value, 2, event_mode=mode)
        app.close()
        return sink.events

    def test_minimal_standard_and_full_have_distinct_runtime_detail(self) -> None:
        minimal = self._events(EventMode.MINIMAL)
        standard = self._events(EventMode.STANDARD)
        full = self._events(EventMode.FULL)

        self.assertTrue(all(getattr(event, "subject_type", None) == "invocation" or hasattr(event, "type") for event in minimal))
        self.assertTrue(any(getattr(event, "subject_type", None) == "node" for event in standard))
        self.assertTrue(any(getattr(event, "subject_type", None) == "node_phase" for event in standard))
        self.assertTrue(any(getattr(event, "subject_type", None) == "node_phase" for event in full))
        self.assertTrue(any(getattr(event, "operations", ()) for event in full if hasattr(event, "operations")))
        for events in (minimal, standard, full):
            runtime_sequences = [event.sequence for event in events if hasattr(event, "event_name")]
            user_sequences = [event.sequence for event in events if hasattr(event, "type")]
            self.assertEqual(runtime_sequences, list(range(1, len(runtime_sequences) + 1)))
            self.assertEqual(user_sequences, list(range(1, len(user_sequences) + 1)))

    def test_user_event_contains_child_workflow_path(self) -> None:
        child = Workflow(
            "child",
            nodes=[
                Node(
                    "answer",
                    identity_str,
                    user_event_mappings=(UserEventMapping("agent_output", string_user_event),),
                )
            ],
        )
        parent = Workflow("parent", nodes=[Node("agent", child)])
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        app.register_workflow(parent)
        app.invoke(parent, "ok")
        event = next(event for event in sink.events if hasattr(event, "type"))
        self.assertEqual(event.node_id, "agent/answer")
        self.assertEqual(event.workflow_path, ("agent",))
        app.close()

    def test_events_and_checkpoint_have_json_round_trip_records(self) -> None:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        value = workflow()
        app.register_workflow(value)
        invocation = app.invoke(value, 2, event_mode="full")

        for event in sink.events:
            record = event.to_record()
            json.dumps(record)
            restored = type(event).from_record(record)
            self.assertEqual(restored.invocation_id, event.invocation_id)
            self.assertEqual(restored.sequence, event.sequence)
        checkpoint = sink.checkpoints[-1]
        record = checkpoint.to_record()
        json.dumps(record)
        restored = type(checkpoint).from_record(record)
        self.assertEqual(restored.invocation_id, checkpoint.invocation_id)
        self.assertEqual(restored.scheduler_state, checkpoint.scheduler_state)
        app.close()

    def test_checkpoint_record_restores_structured_runtime_values(self) -> None:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        value = Workflow(
            "structured-checkpoint",
            nodes=[Node("value", structured_value)],
        )
        app.register_workflow(value)
        app.invoke(value, None)
        checkpoint = sink.checkpoints[-1]
        restored = type(checkpoint).from_record(checkpoint.to_record())
        output = next(iter(restored.required_outputs.values()))
        self.assertEqual(output, StructuredValue("Paris", (18, 19)))
        app.close()

    def test_full_operations_match_recursive_context_patch_semantics(self) -> None:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        value = Workflow(
            "nested-context-operations",
            nodes=[
                Node(
                    "first",
                    identity_none,
                    output_binding=bind_name,
                ),
                Node(
                    "second",
                    identity_none,
                    output_binding=bind_age,
                ),
            ],
            edges=[Edge("first", "second")],
        )
        app.register_workflow(value)
        app.invoke(value, None, event_mode="full")

        context_operations = [
            operation
            for event in sink.events
            for operation in getattr(event, "operations", ())
            if operation.path[:2] == ("invocation", "context")
        ]
        self.assertEqual(
            [(item.op, item.path, item.value) for item in context_operations],
            [
                (
                    "add",
                    ("invocation", "context", "profile"),
                    {"name": "Ada"},
                ),
                (
                    "add",
                    ("invocation", "context", "profile", "age"),
                    37,
                ),
            ],
        )
        app.close()

    def test_operator_contract_restores_typed_input_and_validates_output(self) -> None:
        received: list[StructuredValue] = []

        def typed(value: StructuredValue) -> StructuredValue:
            received.append(value)
            return value

        app = AutoAgentApp()
        workflow_value = Workflow("typed", nodes=[Node("typed", typed)])
        app.register_workflow(workflow_value)
        invocation = app.invoke(
            workflow_value,
            {"city": "Paris", "temperatures": [18, 19]},
        )
        self.assertEqual(received, [StructuredValue("Paris", (18, 19))])
        self.assertEqual(
            invocation.result(), {"typed": StructuredValue("Paris", (18, 19))}
        )
        app.close()


class AppApiTests(unittest.TestCase):
    def test_different_sessions_execute_concurrently_but_same_session_is_exclusive(self) -> None:
        entered = threading.Barrier(3)
        release = threading.Event()

        def slow(value: int) -> int:
            entered.wait(timeout=2)
            release.wait(timeout=2)
            return value

        value = Workflow("concurrent", nodes=[Node("slow", slow)])
        app = AutoAgentApp(max_executor_concurrency=2)
        app.register_workflow(value)
        first = app.submit_invoke(value, 1, session_id="one")
        second = app.submit_invoke(value, 2, session_id="two")
        entered.wait(timeout=2)
        with self.assertRaises(InvocationConflictError):
            app.submit_invoke(value, 3, session_id="one")
        release.set()
        first.wait(2)
        second.wait(2)
        self.assertEqual(first.result(), {"slow": 1})
        self.assertEqual(second.result(), {"slow": 2})
        app.close()

    def test_admission_rejection_leaves_no_session_or_invocation(self) -> None:
        sink = RecordingSink(admissible=False)
        app = AutoAgentApp(runtime_sink=sink, admission_timeout=0.01)
        value = Workflow("rejected", nodes=[Node("node", identity_int)])
        app.register_workflow(value)
        with self.assertRaises(AdmissionRejectedError):
            app.invoke(value, 1, session_id="never-created")
        self.assertNotIn("never-created", app._sessions)
        self.assertFalse(app._active)
        app.close()

    def test_sink_protocol_failure_does_not_change_business_result(self) -> None:
        app = AutoAgentApp(runtime_sink=FailingSink())
        value = Workflow("sink-failure", nodes=[Node("node", identity_int)])
        app.register_workflow(value)
        invocation = app.invoke(value, 1)
        self.assertEqual(invocation.state, InvocationState.COMPLETED)
        self.assertEqual(invocation.result(), {"node": 1})
        self.assertIsNone(invocation.error)
        app.close()

    def test_session_reuses_identity_after_terminal_and_keeps_old_handle_valid(self) -> None:
        app = AutoAgentApp()
        value = Workflow("reuse", nodes=[Node("node", identity_int)])
        app.register_workflow(value)
        first = app.invoke(value, 1, session_id="session")
        second = app.invoke(value, 2, session_id="session")
        self.assertNotEqual(first.id, second.id)
        self.assertEqual(first.result(), {"node": 1})
        self.assertEqual(second.result(), {"node": 2})
        self.assertIs(app._sessions["session"].invocation, second)
        app.close()

    def test_session_context_is_shared_by_sequential_invocations_only(self) -> None:
        def mapping(context: InputMappingContext) -> int:
            return int(context.session_context.get("count", 0)) + 1

        def binding(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(session={"count": context.output})

        app = AutoAgentApp()
        value = Workflow(
            "session-context",
            nodes=[Node("count", identity_int, input_mapping=mapping, output_binding=binding)],
        )
        app.register_workflow(value)
        self.assertEqual(app.invoke(value, None, session_id="same").result(), {"count": 1})
        self.assertEqual(app.invoke(value, None, session_id="same").result(), {"count": 2})
        self.assertEqual(app.invoke(value, None, session_id="other").result(), {"count": 1})
        app.close()

    def test_resume_wait_stream_and_submit_styles_share_one_execution_path(self) -> None:
        value = Workflow(
            "resume-styles",
            nodes=[
                Node("ask", WaitOperator(str, str)),
                Node("finish", uppercase),
            ],
            edges=[Edge("ask", "finish")],
        )
        app = AutoAgentApp()
        app.register_workflow(value)

        submitted = app.invoke(value, "first", session_id="submit")
        same_handle = app.submit_resume(submitted, submitted.waits[0].id, "one")
        self.assertIs(same_handle, submitted)
        submitted.wait(2)
        self.assertEqual(submitted.result(), {"finish": "ONE"})

        streamed = app.invoke(value, "second", session_id="stream")
        stream = app.stream_resume(
            streamed, streamed.waits[0].id, "two", event_channel="runtime"
        )
        runtime_events = list(stream)
        self.assertTrue(runtime_events)
        self.assertIs(stream.invocation, streamed)
        self.assertEqual(stream.invocation.result(), {"finish": "TWO"})
        app.close()

    def test_recover_wait_stream_and_submit_styles_share_one_execution_path(self) -> None:
        value = Workflow(
            "recover-styles",
            nodes=[
                Node(
                    "run",
                    increment,
                    policy=NodePolicy(
                        recovery=RecoveryPolicy(mode="replay_safe")
                    ),
                )
            ],
        )
        source_sink = RecordingSink()
        source = AutoAgentApp(runtime_sink=source_sink)
        source.register_workflow(value)
        source.invoke(value, 1)
        checkpoint = source_sink.checkpoints[0]
        source.close()

        submitted_app = AutoAgentApp()
        submitted_app.register_workflow(value)
        submitted = submitted_app.submit_recover(checkpoint)
        submitted.wait(2)
        self.assertEqual(submitted.result(), {"run": 2})
        submitted_app.close()

        streamed_app = AutoAgentApp()
        streamed_app.register_workflow(value)
        stream = streamed_app.stream_recover(
            checkpoint, event_channel="runtime"
        )
        events = list(stream)
        self.assertTrue(events)
        self.assertEqual(stream.invocation.result(), {"run": 2})
        streamed_app.close()


class AsyncStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_stream_is_pull_driven_and_closing_cancels(self) -> None:
        async def operator(value: int) -> int:
            await asyncio.sleep(0)
            return value

        value = Workflow("async-stream", nodes=[Node("node", operator)])
        app = AutoAgentApp()
        app.register_workflow(value)
        stream = await app.astream_invoke(
            value, 1, event_channel="runtime", event_mode="standard"
        )
        self.assertEqual(stream.invocation.state, InvocationState.CREATED)
        event = await stream.__anext__()
        self.assertEqual(event.sequence, 1)
        await stream.aclose()
        self.assertEqual(stream.invocation.state, InvocationState.CANCELLED)
        await app.aclose()

    async def test_async_submit_and_wait_share_the_runtime_loop(self) -> None:
        value = Workflow("async-submit", nodes=[Node("node", increment)])
        app = AutoAgentApp()
        app.register_workflow(value)
        invocation = await app.asubmit_invoke(value, 1)
        await invocation.await_done(2)
        self.assertEqual(invocation.result(), {"node": 2})
        await app.aclose()

    async def test_async_resume_wait_and_stream_styles(self) -> None:
        value = Workflow(
            "async-resume",
            nodes=[
                Node("wait", WaitOperator(str, str)),
                Node("finish", uppercase),
            ],
            edges=[Edge("wait", "finish")],
        )
        app = AutoAgentApp()
        app.register_workflow(value)

        submitted = await app.ainvoke(value, "question", session_id="submitted")
        resumed = await app.asubmit_resume(
            submitted, submitted.waits[0].id, "answer"
        )
        await resumed.await_done(2)
        self.assertEqual(resumed.result(), {"finish": "ANSWER"})

        streamed = await app.ainvoke(value, "question", session_id="streamed")
        stream = await app.astream_resume(
            streamed,
            streamed.waits[0].id,
            "stream answer",
            event_channel="runtime",
        )
        events = [event async for event in stream]
        self.assertTrue(events)
        self.assertEqual(stream.invocation.result(), {"finish": "STREAM ANSWER"})
        await app.aclose()

    async def test_async_cancel_returns_the_same_handle(self) -> None:
        # User callables execute on the App Runtime Loop, not this test's
        # caller Loop.  ``asyncio.Event`` is not a cross-loop notification
        # primitive, so use the thread-safe equivalent and poll without
        # blocking the caller Loop.
        started = threading.Event()

        async def slow(value: int) -> int:
            started.set()
            await asyncio.sleep(60)
            return value

        value = Workflow("async-cancel", nodes=[Node("node", slow)])
        app = AutoAgentApp()
        app.register_workflow(value)
        invocation = await app.asubmit_invoke(value, 1)
        async with asyncio.timeout(1):
            while not started.is_set():
                await asyncio.sleep(0.001)
        cancelled = await app.acancel(invocation)
        self.assertIs(cancelled, invocation)
        self.assertEqual(cancelled.state, InvocationState.CANCELLED)
        await app.aclose()


if __name__ == "__main__":
    unittest.main()
