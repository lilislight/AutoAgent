from __future__ import annotations

import asyncio
import threading
import time
import unittest
from typing import Any
from unittest.mock import patch

from autoagent.core import (
    AdmissionRejectedError,
    AutoAgentApp,
    EventMode,
    InputMappingContext,
    ItemSelectorContext,
    InvocationState,
    MapPolicy,
    Node,
    NodePolicy,
    ResourcePolicy,
    RuntimeEvent,
    SerializedCheckpoint,
    SerializedEvent,
    UserEventMapping,
    Workflow,
)


def identity_int(value: int) -> int:
    return value


ACCOUNTING_CALLS: list[int] = []


def counted_identity(value: int) -> int:
    ACCOUNTING_CALLS.append(value)
    return value


def identity_dict(value: dict[str, list[int]]) -> dict[str, list[int]]:
    return value


def select_ints(context: ItemSelectorContext) -> list[int]:
    return context.input


def user_value(value: int) -> dict[str, int]:
    return {"value": value}


class EnvelopeSink:
    def __init__(self) -> None:
        self.events: list[SerializedEvent] = []
        self.checkpoints: list[SerializedCheckpoint] = []

    async def wait_until_admissible(self) -> None:
        return None

    async def submit_events(self, events: tuple[SerializedEvent, ...]) -> None:
        self.events.extend(events)

    def offer_checkpoint(self, checkpoint: SerializedCheckpoint) -> None:
        self.checkpoints.append(checkpoint)


class NeverAdmissibleSink(EnvelopeSink):
    async def wait_until_admissible(self) -> None:
        await asyncio.Event().wait()


class BlockingSubmissionSink(EnvelopeSink):
    def __init__(self) -> None:
        super().__init__()
        self.blocked = threading.Event()
        self.release = threading.Event()
        self.submissions = 0

    async def submit_events(self, events: tuple[SerializedEvent, ...]) -> None:
        self.submissions += 1
        if self.submissions == 2:
            self.blocked.set()
            await asyncio.to_thread(self.release.wait)
        await super().submit_events(events)


class SinkContractTests(unittest.TestCase):
    def test_core_owns_admission_timeout(self) -> None:
        app = AutoAgentApp(
            runtime_sink=NeverAdmissibleSink(),
            admission_timeout=0.01,
        )
        workflow = Workflow("admission-timeout", nodes=[Node("node", identity_int)])
        app.register_workflow(workflow)

        started = time.perf_counter()
        with self.assertRaises(AdmissionRejectedError):
            app.invoke(workflow, 1, session_id="not-created")

        self.assertLess(time.perf_counter() - started, 0.5)
        self.assertNotIn("not-created", app._sessions)
        app.close()

    def test_negative_admission_timeout_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            AutoAgentApp(admission_timeout=-0.1)

    def test_sink_receives_immutable_serialized_events_and_checkpoints(self) -> None:
        sink = EnvelopeSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow("envelopes", nodes=[Node("node", identity_dict)])
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, {"values": [1, 2]}, event_mode="full")

        self.assertEqual(invocation.state, InvocationState.COMPLETED)
        self.assertTrue(sink.events)
        self.assertTrue(sink.checkpoints)
        self.assertTrue(all(isinstance(item.payload, bytes) for item in sink.events))
        self.assertTrue(all(item.size_bytes == len(item.payload) for item in sink.events))
        self.assertTrue(
            all(isinstance(item.payload, bytes) for item in sink.checkpoints)
        )

        event = next(
            item
            for item in sink.events
            if item.channel == "runtime" and item.event_type == "node_state_changed"
        )
        first = event.decode()
        if isinstance(first.payload, dict):
            first.payload["mutated"] = True
        second = event.decode()
        self.assertNotIn("mutated", second.payload or {})
        app.close()

    def test_full_sink_blocks_execution_only_at_acceptance_boundary(self) -> None:
        sink = BlockingSubmissionSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow("sink-backpressure", nodes=[Node("node", identity_int)])
        app.register_workflow(workflow)

        invocation = app.submit_invoke(workflow, 1, event_mode="standard")
        self.assertTrue(sink.blocked.wait(1))
        self.assertFalse(invocation.done())
        self.assertEqual(invocation.state, InvocationState.RUNNING)

        sink.release.set()
        invocation.wait(2)
        self.assertEqual(invocation.result(), {"node": 1})
        app.close()

    def test_attached_stream_event_was_accepted_by_sink_before_delivery(self) -> None:
        sink = EnvelopeSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "stream-order",
            nodes=[
                Node(
                    "node",
                    identity_int,
                    user_event_mappings=(UserEventMapping("agent_output", user_value),),
                )
            ],
        )
        app.register_workflow(workflow)

        stream = app.stream_invoke(workflow, 3, event_mode="minimal")
        event = next(stream)
        self.assertIn(event.id, {item.id for item in sink.events})
        self.assertEqual(event.data, {"value": 3})
        self.assertEqual(list(stream), [])
        app.close()

    def test_runtime_event_capture_failure_does_not_fail_business_execution(self) -> None:
        app = AutoAgentApp(runtime_sink=EnvelopeSink())
        workflow = Workflow("capture-failure", nodes=[Node("node", identity_int)])
        app.register_workflow(workflow)

        with (
            patch.object(
                SerializedEvent,
                "from_event",
                side_effect=TypeError("cannot serialize trace"),
            ),
            self.assertLogs(
                "autoagent.core.executor.workflow_executor", level="ERROR"
            ),
        ):
            invocation = app.invoke(workflow, 7, event_mode="full")

        self.assertEqual(invocation.state, InvocationState.COMPLETED)
        self.assertEqual(invocation.result(), {"node": 7})
        app.close()

    def test_full_event_capture_failure_does_not_fail_business_execution(self) -> None:
        app = AutoAgentApp(runtime_sink=EnvelopeSink())
        workflow = Workflow("operation-capture-failure", nodes=[Node("node", identity_int)])
        app.register_workflow(workflow)

        with (
            patch.object(
                RuntimeEvent,
                "detached",
                side_effect=TypeError("cannot capture event"),
            ),
            self.assertLogs(
                "autoagent.core.executor.workflow_executor", level="ERROR"
            ),
        ):
            invocation = app.invoke(workflow, 8, event_mode="full")

        self.assertEqual(invocation.state, InvocationState.COMPLETED)
        self.assertEqual(invocation.result(), {"node": 8})
        self.assertGreater(invocation.latest_checkpoint.state_version, 0)
        app.close()

    def test_checkpoint_capture_failure_only_degrades_recoverability(self) -> None:
        sink = EnvelopeSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow("checkpoint-failure", nodes=[Node("node", identity_int)])
        app.register_workflow(workflow)

        with (
            patch.object(
                SerializedCheckpoint,
                "from_checkpoint",
                side_effect=TypeError("cannot serialize checkpoint"),
            ),
            self.assertLogs(
                "autoagent.core.executor.workflow_executor", level="ERROR"
            ),
        ):
            invocation = app.invoke(workflow, 9)

        self.assertEqual(invocation.state, InvocationState.COMPLETED)
        self.assertEqual(invocation.result(), {"node": 9})
        # Local recovery remains available even when this particular Sink
        # offer cannot be serialized or accepted.
        self.assertEqual(invocation.latest_checkpoint.invocation_state, "completed")
        self.assertTrue(
            any(
                event.decode().event_name == "invocation_state_changed"
                and event.decode().status == "completed"
                for event in sink.events
                if event.channel == "runtime"
            )
        )
        app.close()

    def test_checkpoint_sink_exception_does_not_detach_event_delivery(self) -> None:
        sink = EnvelopeSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow("checkpoint-offer-failure", nodes=[Node("node", identity_int)])
        app.register_workflow(workflow)

        with (
            patch.object(
                sink,
                "offer_checkpoint",
                side_effect=RuntimeError("checkpoint queue unavailable"),
            ),
            self.assertLogs(
                "autoagent.core.executor.workflow_executor", level="ERROR"
            ),
        ):
            invocation = app.invoke(workflow, 10, event_mode="full")

        self.assertEqual(invocation.result(), {"node": 10})
        decoded = [
            event.decode() for event in sink.events if event.channel == "runtime"
        ]
        self.assertTrue(decoded)
        self.assertEqual(decoded[-1].event_name, "invocation_state_changed")
        self.assertEqual(decoded[-1].status, "completed")
        self.assertEqual(invocation.latest_checkpoint.invocation_state, "completed")
        app.close()


class ExecutionAccountingTests(unittest.TestCase):
    def test_operator_attempt_limit_is_identical_in_every_event_mode(self) -> None:
        workflow = Workflow(
            "mode-independent-attempt-limit",
            nodes=[
                Node(
                    "mapped",
                    counted_identity,
                    policy=NodePolicy(
                        map=MapPolicy(item_selector=select_ints, max_parallelism=1),
                        resource=ResourcePolicy(
                            max_operator_attempts_per_invocation=2
                        ),
                    ),
                )
            ],
        )

        for mode in EventMode:
            with self.subTest(mode=mode):
                ACCOUNTING_CALLS.clear()
                app = AutoAgentApp()
                app.register_workflow(workflow)
                invocation = app.invoke(workflow, [1, 2, 3], event_mode=mode)
                self.assertEqual(invocation.state, InvocationState.FAILED)
                self.assertIn("Operator attempt limit exceeded", invocation.error.message)
                self.assertEqual(ACCOUNTING_CALLS, [1, 2])
                app.close()

    def test_runtime_event_sequences_are_monotonic_after_serialization(self) -> None:
        sink = EnvelopeSink()
        app = AutoAgentApp(runtime_sink=sink, max_executor_concurrency=4)
        workflow = Workflow(
            "serialized-sequence",
            nodes=[Node("one", identity_int), Node("two", identity_int)],
        )
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1, event_mode="full")

        sequences = [
            event.sequence
            for event in sink.events
            if event.channel == "runtime" and event.invocation_id == invocation.id
        ]
        self.assertEqual(sequences, list(range(1, len(sequences) + 1)))
        self.assertEqual(len(sequences), len(set(sequences)))
        app.close()


if __name__ == "__main__":
    unittest.main()
