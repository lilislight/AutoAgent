from __future__ import annotations

import asyncio
import threading
import unittest

from typing_extensions import TypedDict

from autoagent import (
    AggregationContext,
    AutoAgentApp,
    ConditionContext,
    ContextPatch,
    Edge,
    InputMappingContext,
    Map,
    Node,
    OutputBindingContext,
    Recovery,
    RuntimeInfrastructureError,
    Wait,
    Workflow,
)
from autoagent.core import RuntimeCheckpointBundle, RuntimeEvent, StateReducer


class Value(TypedDict):
    value: int


class Batch(TypedDict):
    items: list[Value]


def identity(value: Value) -> Value:
    return value


def map_items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


class _CommitThenFailSink:
    """Model a durable database commit whose acknowledgement is lost."""

    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []
        self.fail_event_name: str | None = None

    async def append(self, event: RuntimeEvent) -> None:
        if all(existing.id != event.id for existing in self.events):
            self.events.append(event)
        if (
            self.fail_event_name is not None
            and event.session_id == "root"
            and any(
                log.event_name == self.fail_event_name for log in event.logs
            )
        ):
            raise RuntimeError("database acknowledgement was lost")


def _checkpoint_from_prefix(
    events: tuple[RuntimeEvent, ...],
) -> RuntimeCheckpointBundle:
    states = {
        session_id: StateReducer().reduce(
            tuple(event for event in events if event.session_id == session_id)
        )
        for session_id in {event.session_id for event in events}
    }
    return RuntimeCheckpointBundle.from_states("root", states)


class ChildRecoveryIntegrityTests(unittest.TestCase):
    def test_created_root_admission_recovers_before_scheduler_initialization(self) -> None:
        """Verify recovery preflight does not reject an unopened Scheduler."""

        workflow = Workflow(
            "created-root-recovery",
            nodes=[Node("work", identity)],
        )
        sink = _CommitThenFailSink()
        sink.fail_event_name = "invocation.opened"
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                source.invoke(workflow, {"value": 1}, session_id="root")
            prefix = tuple(sink.events)
        finally:
            sink.fail_event_name = None
            source.close(timeout=1)

        checkpoint = _checkpoint_from_prefix(prefix)
        invocation = checkpoint.state("root").invocation
        assert invocation is not None
        self.assertEqual(invocation.status, "created")
        self.assertFalse(invocation.scheduler.initialized)

        restored = AutoAgentApp()
        try:
            restored.register_workflow(workflow)
            ref = restored.load_checkpoint(checkpoint).roots[0]
            result = restored.recover(ref)
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 1})
        finally:
            restored.close(timeout=1)

    def test_cancelled_root_recovery_cancels_running_spawn_child(self) -> None:
        """Verify a persisted Root cancellation never replays a spawn Child."""

        child_started = threading.Event()
        child_calls = 0

        async def child_work(value: Value) -> Value:
            nonlocal child_calls
            child_calls += 1
            child_started.set()
            await asyncio.Event().wait()
            return value

        child = Workflow("cancel-spawn-child", nodes=[Node("work", child_work)])
        parent = Workflow(
            "cancel-spawn-parent",
            nodes=[
                Node("start", identity),
                Node("spawn", child, execution_mode="spawn"),
                Node("approval", Wait(Value, Value)),
            ],
            edges=[Edge("start", "spawn"), Edge("start", "approval")],
        )
        sink = _CommitThenFailSink()
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            waiting = source.invoke(parent, {"value": 1}, session_id="root")
            self.assertEqual(waiting.status, "waiting")
            self.assertTrue(child_started.wait(1))
            sink.fail_event_name = "invocation.cancelled"
            with self.assertRaises(RuntimeInfrastructureError):
                source.cancel(waiting.ref, "stop")
            prefix = tuple(sink.events)
        finally:
            sink.fail_event_name = None
            source.close(timeout=1)

        checkpoint = _checkpoint_from_prefix(prefix)
        root = checkpoint.state("root").invocation
        assert root is not None
        child_session_id = next(iter(root.child_plans.values())).units[0].session_id
        self.assertEqual(root.status, "cancelled")
        self.assertEqual(
            checkpoint.state(child_session_id).invocation.status,  # type: ignore[union-attr]
            "running",
        )

        restored = AutoAgentApp()
        try:
            restored.register_workflow(parent)
            ref = restored.load_checkpoint(checkpoint).roots[0]
            result = restored.recover(ref)
            handle = restored.child_handles(ref)[0]
            self.assertEqual(result.status, "cancelled")
            self.assertEqual(restored.child_status(handle).status, "cancelled")
            self.assertEqual(child_calls, 1)
        finally:
            restored.close(timeout=1)

    def test_failed_root_recovery_cancels_running_spawn_child(self) -> None:
        """Verify a persisted Root failure never replays a spawn Child."""

        child_started = threading.Event()
        child_calls = 0

        async def child_work(value: Value) -> Value:
            nonlocal child_calls
            child_calls += 1
            child_started.set()
            await asyncio.Event().wait()
            return value

        async def fail_after_child_started(_value: Value) -> Value:
            while not child_started.is_set():
                await asyncio.sleep(0)
            raise RuntimeError("parent failed")

        child = Workflow(
            "fail-spawn-child",
            nodes=[
                Node(
                    "work",
                    child_work,
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        parent = Workflow(
            "fail-spawn-parent",
            nodes=[
                Node("start", identity),
                Node("spawn", child, execution_mode="spawn"),
                Node("fail", fail_after_child_started),
            ],
            edges=[Edge("start", "spawn"), Edge("start", "fail")],
        )
        sink = _CommitThenFailSink()
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            sink.fail_event_name = "invocation.failed"
            with self.assertRaises(RuntimeInfrastructureError):
                source.invoke(parent, {"value": 1}, session_id="root")
            prefix = tuple(sink.events)
        finally:
            sink.fail_event_name = None
            source.close(timeout=1)

        checkpoint = _checkpoint_from_prefix(prefix)
        root = checkpoint.state("root").invocation
        assert root is not None
        child_session_id = next(iter(root.child_plans.values())).units[0].session_id
        self.assertEqual(root.status, "failed")
        self.assertEqual(
            checkpoint.state(child_session_id).invocation.status,  # type: ignore[union-attr]
            "running",
        )

        restored = AutoAgentApp()
        try:
            restored.register_workflow(parent)
            ref = restored.load_checkpoint(checkpoint).roots[0]
            result = restored.recover(ref)
            handle = restored.child_handles(ref)[0]
            self.assertEqual(result.status, "failed")
            self.assertEqual(restored.child_status(handle).status, "cancelled")
            self.assertEqual(child_calls, 1)
        finally:
            restored.close(timeout=1)

    def test_persisted_fail_fast_decision_prevents_spawn_child_replay(self) -> None:
        """Verify a failed Node is resolved before recovering detached work."""

        child_started = threading.Event()
        child_calls = 0

        async def child_work(value: Value) -> Value:
            nonlocal child_calls
            child_calls += 1
            child_started.set()
            await asyncio.Event().wait()
            return value  # pragma: no cover

        async def fail_after_child_started(_value: Value) -> Value:
            while not child_started.is_set():
                await asyncio.sleep(0)
            raise RuntimeError("parent branch failed")

        child = Workflow(
            "fail-fast-prefix-child",
            nodes=[
                Node(
                    "work",
                    child_work,
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        parent = Workflow(
            "fail-fast-prefix-parent",
            nodes=[
                Node("start", identity),
                Node("spawn", child, execution_mode="spawn"),
                Node("fail", fail_after_child_started),
            ],
            edges=[Edge("start", "spawn"), Edge("start", "fail")],
        )
        sink = _CommitThenFailSink()
        sink.fail_event_name = "node_occurrence.failed"
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                source.invoke(parent, {"value": 1}, session_id="root")
            prefix = tuple(sink.events)
        finally:
            sink.fail_event_name = None
            source.close(timeout=1)

        checkpoint = _checkpoint_from_prefix(prefix)
        root = checkpoint.state("root").invocation
        assert root is not None
        self.assertEqual(root.status, "running")
        self.assertTrue(
            any(item.status == "failed" for item in root.scheduler.occurrences.values())
        )

        restored = AutoAgentApp()
        try:
            restored.register_workflow(parent)
            ref = restored.load_checkpoint(checkpoint).roots[0]
            result = restored.recover(ref)
            self.assertEqual(result.status, "failed")
            handle = restored.child_handles(ref)[0]
            self.assertEqual(restored.child_status(handle).status, "cancelled")
            self.assertEqual(child_calls, 1)
        finally:
            restored.close(timeout=1)

    def test_cancelled_root_recovery_cancels_waiting_await_child(self) -> None:
        """Verify a cancelled Root settles an awaited Child instead of retaining its Wait."""

        child = Workflow(
            "cancel-await-child",
            nodes=[Node("approval", Wait(Value, Value))],
        )
        parent = Workflow("cancel-await-parent", nodes=[Node("child", child)])
        sink = _CommitThenFailSink()
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            waiting = source.invoke(parent, {"value": 1}, session_id="root")
            self.assertEqual(waiting.status, "waiting")
            sink.fail_event_name = "invocation.cancelled"
            with self.assertRaises(RuntimeInfrastructureError):
                source.cancel(waiting.ref, "stop")
            prefix = tuple(sink.events)
        finally:
            sink.fail_event_name = None
            source.close(timeout=1)

        checkpoint = _checkpoint_from_prefix(prefix)
        restored = AutoAgentApp()
        try:
            restored.register_workflow(parent)
            ref = restored.load_checkpoint(checkpoint).roots[0]
            result = restored.recover(ref)
            handle = restored.child_handles(ref)[0]
            child_result = restored.child_status(handle)
            self.assertEqual(result.status, "cancelled")
            self.assertEqual(child_result.status, "cancelled")
            root = child_result.checkpoint.state("root").invocation
            assert root is not None
            plan = next(iter(root.child_plans.values()))
            self.assertEqual(plan.units[0].phase, "terminal")
        finally:
            restored.close(timeout=1)

    def test_child_map_aggregate_obeys_default_recovery_policy(self) -> None:
        """Verify a Child plan cannot make an interrupted aggregate replay-safe."""

        entered = threading.Event()
        calls = 0

        async def aggregate(context: AggregationContext) -> Batch:
            nonlocal calls
            calls += 1
            entered.set()
            await asyncio.Event().wait()
            return {"items": list(context.outputs)}  # pragma: no cover

        child = Workflow("policy-aggregate-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "policy-aggregate-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(aggregate=aggregate),
                )
            ],
        )
        checkpoint = self._close_during_hook(
            parent, {"items": [{"value": 1}]}, entered
        )
        result = self._recover(parent, checkpoint)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error.type, "RecoveryNotAllowed")  # type: ignore[union-attr]
        self.assertEqual(calls, 1)

    def test_unrecoverable_parent_is_rejected_before_replaying_its_child(self) -> None:
        """Verify ancestor recovery preflight prevents every Child side effect."""

        entered = threading.Event()
        child_calls = 0

        async def child_work(value: Value) -> Value:
            nonlocal child_calls
            child_calls += 1
            entered.set()
            await asyncio.Event().wait()
            return value  # pragma: no cover

        def bind_output(_context: OutputBindingContext) -> ContextPatch | None:
            return None

        child = Workflow(
            "preflight-child",
            nodes=[
                Node(
                    "work",
                    child_work,
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        parent = Workflow(
            "preflight-parent",
            nodes=[Node("child", child, output_binding=bind_output)],
        )
        source = AutoAgentApp()
        try:
            source.submit_invoke(parent, {"value": 1}, session_id="root")
            self.assertTrue(entered.wait(1))
            checkpoint = source.close(timeout=1).roots[0]
        finally:
            if not source._closed:
                source.close(timeout=1)

        restored = AutoAgentApp()
        try:
            restored.register_workflow(parent)
            ref = restored.load_checkpoint(checkpoint).roots[0]
            result = restored.recover(ref)
            self.assertEqual(result.status, "failed")
            self.assertEqual(
                result.error.type,  # type: ignore[union-attr]
                "RecoveryNotAllowed",
            )
            handle = restored.child_handles(ref)[0]
            self.assertEqual(restored.child_status(handle).status, "cancelled")
            self.assertEqual(child_calls, 1)
        finally:
            restored.close(timeout=1)

    def test_child_output_binding_obeys_default_recovery_policy(self) -> None:
        """Verify a durable Child plan does not implicitly replay Output Binding."""

        entered = threading.Event()
        calls = 0

        async def bind_output(
            _context: OutputBindingContext,
        ) -> ContextPatch | None:
            nonlocal calls
            calls += 1
            entered.set()
            await asyncio.Event().wait()
            return None  # pragma: no cover

        child = Workflow("policy-binding-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "policy-binding-parent",
            nodes=[Node("child", child, output_binding=bind_output)],
        )
        checkpoint = self._close_during_hook(parent, {"value": 1}, entered)
        result = self._recover(parent, checkpoint)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error.type, "RecoveryNotAllowed")  # type: ignore[union-attr]
        self.assertEqual(calls, 1)

    def test_child_condition_obeys_default_recovery_policy(self) -> None:
        """Verify a durable Child plan does not implicitly replay an Edge Condition."""

        entered = threading.Event()
        calls = 0

        async def route(_context: ConditionContext) -> bool:
            nonlocal calls
            calls += 1
            entered.set()
            await asyncio.Event().wait()
            return True  # pragma: no cover

        child = Workflow("policy-condition-child", nodes=[Node("work", identity)])
        parent = Workflow(
            "policy-condition-parent",
            nodes=[Node("child", child), Node("done", identity)],
            edges=[Edge("child", "done", condition=route)],
        )
        checkpoint = self._close_during_hook(parent, {"value": 1}, entered)
        result = self._recover(parent, checkpoint)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error.type, "RecoveryNotAllowed")  # type: ignore[union-attr]
        self.assertEqual(calls, 1)

    def _close_during_hook(
        self,
        workflow: Workflow,
        value: object,
        entered: threading.Event,
    ) -> RuntimeCheckpointBundle:
        source = AutoAgentApp()
        try:
            source.submit_invoke(workflow, value, session_id="root")
            self.assertTrue(entered.wait(1))
            return source.close(timeout=1).roots[0]
        finally:
            if not source._closed:
                source.close(timeout=1)

    def _recover(
        self, workflow: Workflow, checkpoint: RuntimeCheckpointBundle
    ):
        restored = AutoAgentApp()
        try:
            restored.register_workflow(workflow)
            ref = restored.load_checkpoint(checkpoint).roots[0]
            return restored.recover(ref)
        finally:
            restored.close(timeout=1)


if __name__ == "__main__":
    unittest.main()
