from __future__ import annotations

import asyncio
import copy
import threading
import time
import unittest
from typing import Any

from autoagent.core import (
    AutoAgentApp,
    Edge,
    EventMode,
    ExecutionContext,
    InvocationState,
    MapPolicy,
    Node,
    NodePolicy,
    RecoveryPolicy,
    RuntimeEvent,
    RecoveryCheckpoint,
    UserEventMapping,
    WaitOperator,
    Workflow,
    WorkflowCompileError,
)
from autoagent.core.executor import NodeExecutor
from autoagent.core.workflow import BackoffPolicy
from tests.helpers import decode_checkpoint, decode_events


def identity_text(value: str) -> str:
    return value


def user_number(value: int) -> int:
    return value


def select_numbers(
    context: ExecutionContext, values: list[int]
) -> list[int]:
    if context.node_id != "mapped":
        raise AssertionError("selector received the wrong Node location")
    return values


def aggregate_numbers(context: ExecutionContext, values: list[int]) -> int:
    if context.node_id != "mapped":
        raise AssertionError("aggregator received the wrong Node location")
    return sum(values)


def double(value: int) -> int:
    return value * 2


def idempotent_operation(value: int, idempotency_key: str) -> int:
    _IDEMPOTENCY_KEYS.append(idempotency_key)
    return value


def non_idempotent_operation(value: int) -> int:
    return value


_IDEMPOTENCY_KEYS: list[str] = []


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

class IncrementalCoordinatorTests(unittest.TestCase):
    def test_fast_branch_schedules_downstream_before_slow_sibling_finishes(self) -> None:
        release_slow = asyncio.Event()
        downstream_started = threading.Event()

        async def slow(value: str) -> str:
            await release_slow.wait()
            return value

        def after_fast(value: str) -> str:
            downstream_started.set()
            return value

        workflow = Workflow(
            "incremental",
            nodes=[
                Node("start", identity_text),
                Node("fast", identity_text),
                Node("slow", slow),
                Node("after_fast", after_fast),
            ],
            edges=[
                Edge("start", "fast"),
                Edge("start", "slow"),
                Edge("fast", "after_fast"),
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, "value")
        self.assertTrue(downstream_started.wait(1))
        self.assertEqual(invocation.state, InvocationState.RUNNING)
        app._runtime.run(self._set_event(release_slow))
        invocation.wait(2)
        self.assertEqual(
            invocation.result(), {"slow": "value", "after_fast": "value"}
        )
        app.close()

    @staticmethod
    async def _set_event(event: asyncio.Event) -> None:
        event.set()

    def test_multiple_waits_have_independent_ids_and_resume_independently(self) -> None:
        workflow = Workflow(
            "multiple-waits",
            nodes=[
                Node("approval", WaitOperator(str, str)),
                Node("confirmation", WaitOperator(str, str)),
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, "question")
        self.assertEqual(invocation.state, InvocationState.WAITING)
        self.assertEqual(len(invocation.waits), 2)
        self.assertEqual(len({wait.id for wait in invocation.waits}), 2)
        checkpoint = invocation.latest_checkpoint
        assert checkpoint is not None
        checkpoint = RecoveryCheckpoint.from_record(checkpoint.to_record())
        self.assertEqual(
            {wait.id for wait in checkpoint.waits},
            {wait.id for wait in invocation.waits},
        )
        app.close()

        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.recover(checkpoint)

        waits_by_node = {wait.node_id: wait for wait in invocation.waits}
        approval = waits_by_node["approval"]
        confirmation = waits_by_node["confirmation"]
        app.resume(invocation, approval.id, "approved")
        self.assertEqual(invocation.state, InvocationState.WAITING)
        self.assertEqual(
            {wait.id for wait in invocation.waits}, {confirmation.id}
        )
        app.resume(invocation, confirmation.id, "confirmed")
        self.assertEqual(
            invocation.result(),
            {"approval": "approved", "confirmation": "confirmed"},
        )
        app.close()

    def test_wait_can_be_resumed_while_another_branch_is_running(self) -> None:
        release_slow = asyncio.Event()

        async def slow(value: str) -> str:
            await release_slow.wait()
            return value

        workflow = Workflow(
            "resume-running",
            nodes=[
                Node("start", identity_text),
                Node("wait", WaitOperator(str, str)),
                Node("slow", slow),
            ],
            edges=[Edge("start", "wait"), Edge("start", "slow")],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, "question")
        deadline = time.monotonic() + 1
        while not invocation.waits and time.monotonic() < deadline:
            time.sleep(0.001)
        self.assertEqual(invocation.state, InvocationState.RUNNING)
        self.assertEqual(len(invocation.waits), 1)
        app.submit_resume(invocation, invocation.waits[0].id, "answer")
        deadline = time.monotonic() + 1
        while invocation.waits and time.monotonic() < deadline:
            time.sleep(0.001)
        self.assertEqual(invocation.waits, ())
        self.assertEqual(invocation.state, InvocationState.RUNNING)
        app._runtime.run(self._set_event(release_slow))
        invocation.wait(2)
        self.assertEqual(invocation.result(), {"wait": "answer", "slow": "question"})
        app.close()


class PolicyAndEventContractTests(unittest.TestCase):
    def test_idempotent_recovery_requires_key_and_reuses_it_after_recovery(self) -> None:
        invalid = Workflow(
            "invalid-idempotent",
            nodes=[
                Node(
                    "node",
                    non_idempotent_operation,
                    policy=NodePolicy(recovery=RecoveryPolicy(mode="idempotent")),
                )
            ],
        )
        app = AutoAgentApp()
        with self.assertRaisesRegex(WorkflowCompileError, "idempotency_key: str"):
            app.register_workflow(invalid)
        app.close()

        _IDEMPOTENCY_KEYS.clear()
        sink = CaptureSink()
        workflow = Workflow(
            "idempotent",
            nodes=[
                Node(
                    "node",
                    idempotent_operation,
                    policy=NodePolicy(recovery=RecoveryPolicy(mode="idempotent")),
                )
            ],
        )
        first = AutoAgentApp(runtime_sink=sink)
        first.register_workflow(workflow)
        self.assertEqual(first.invoke(workflow, {"value": 7}).result(), {"node": 7})
        created = sink.checkpoints[0]
        first.close()

        second = AutoAgentApp()
        second.register_workflow(workflow)
        self.assertEqual(second.recover(created).result(), {"node": 7})
        self.assertEqual(len(_IDEMPOTENCY_KEYS), 2)
        self.assertEqual(_IDEMPOTENCY_KEYS[0], _IDEMPOTENCY_KEYS[1])
        second.close()

    def test_event_modes_and_parallel_calls_follow_the_new_contract(self) -> None:
        workflow = Workflow(
            "modes",
            nodes=[
                Node(
                    "mapped",
                    double,
                    policy=NodePolicy(
                        map=MapPolicy(
                            item_selector=select_numbers,
                            output_aggregator=aggregate_numbers,
                        )
                    ),
                    user_event_mappings=(UserEventMapping("answer", user_number),),
                )
            ],
        )
        for mode, expected_runtime in (
            (EventMode.MINIMAL, 0),
            (EventMode.STANDARD, 7),
            (EventMode.FULL, 9),
        ):
            sink = CaptureSink()
            app = AutoAgentApp(runtime_sink=sink)
            app.register_workflow(workflow)
            invocation = app.invoke(workflow, [1, 2, 3], event_mode=mode)
            self.assertEqual(invocation.result(), {"mapped": 12})
            runtime = [event for event in sink.events if isinstance(event, RuntimeEvent)]
            self.assertEqual(len(runtime), expected_runtime)
            calls = [event for event in runtime if event.subject_type == "operator_call"]
            self.assertEqual(len(calls), 0 if mode is EventMode.MINIMAL else 3)
            if mode is EventMode.STANDARD:
                self.assertTrue(
                    all("input" not in event.payload and "output" not in event.payload for event in calls)
                )
            if mode is EventMode.FULL:
                self.assertTrue(all("input" in event.payload for event in calls))
            app.close()

    def test_backoff_formulas_and_jitter_bounds(self) -> None:
        fixed = BackoffPolicy(mode="fixed", initial_delay_ms=100)
        linear = BackoffPolicy(mode="linear", initial_delay_ms=100)
        exponential = BackoffPolicy(
            mode="exponential", initial_delay_ms=100, multiplier=3
        )
        self.assertEqual(NodeExecutor._retry_delay(fixed, 3), 0.1)
        self.assertEqual(NodeExecutor._retry_delay(linear, 3), 0.3)
        self.assertEqual(NodeExecutor._retry_delay(exponential, 3), 0.9)
        with self.assertRaisesRegex(ValueError, "backoff mode"):
            BackoffPolicy(mode="unknown")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "jitter"):
            BackoffPolicy(jitter="unknown")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "less than"):
            BackoffPolicy(initial_delay_ms=100, max_delay_ms=99)


if __name__ == "__main__":
    unittest.main()
