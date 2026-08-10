from __future__ import annotations

import asyncio
import copy
import threading
import time
import unittest
from typing import Any

from autoagent.core import (
    AggregationContext,
    AutoAgentApp,
    BackoffPolicy,
    ContextPatch,
    Edge,
    EdgeConditionContext,
    EventMode,
    InputMappingContext,
    ItemSelectorContext,
    InvocationState,
    MapPolicy,
    Node,
    NodePolicy,
    Operator,
    OutputBindingContext,
    RecoveryPolicy,
    RetryPolicy,
    RuntimeEvent,
    WaitOperator,
    Workflow,
)
from autoagent.core.runtime import SerializedCheckpoint, SerializedEvent


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []
        self.checkpoints: list[Any] = []

    async def wait_until_admissible(self) -> None:
        return None

    async def submit_events(self, events: tuple[SerializedEvent, ...]) -> None:
        self.events.extend(
            event.decode()
            for event in events
            if event.channel == "runtime"
        )

    def offer_checkpoint(self, checkpoint: SerializedCheckpoint) -> None:
        self.checkpoints.append(checkpoint.decode())


def identity(value: int) -> int:
    return value


class CheckpointBoundaryTests(unittest.TestCase):
    def test_parallel_wait_checkpoint_recovers_running_sibling_before_stable_wait(self) -> None:
        slow_started = threading.Event()
        release_slow = threading.Event()
        slow_calls = 0

        def slow(value: str) -> str:
            nonlocal slow_calls
            slow_calls += 1
            slow_started.set()
            release_slow.wait(2)
            return value

        workflow = Workflow(
            "parallel-wait-recovery",
            nodes=[
                Node("approval", WaitOperator(str, str)),
                Node(
                    "slow",
                    slow,
                    policy=NodePolicy(recovery=RecoveryPolicy("replay_safe")),
                ),
            ],
        )
        sink = RecordingSink()
        original_app = AutoAgentApp(runtime_sink=sink)
        original_app.register_workflow(workflow)
        original = original_app.submit_invoke(workflow, "request")
        self.assertTrue(slow_started.wait(1))

        deadline = time.monotonic() + 1
        checkpoint = None
        while time.monotonic() < deadline:
            candidate = original.latest_checkpoint
            if candidate is not None and candidate.waits:
                checkpoint = copy.deepcopy(candidate)
                break
            time.sleep(0.001)
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint.invocation_state, "running")
        self.assertTrue(
            any(item.node_id == "slow" and item.state == "running" for item in checkpoint.node_states)
        )

        release_slow.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and original.state is not InvocationState.WAITING:
            time.sleep(0.001)
        self.assertEqual(original.state, InvocationState.WAITING)
        original_app.close()

        recovered_app = AutoAgentApp()
        recovered_app.register_workflow(workflow)
        recovered = recovered_app.recover(checkpoint)

        self.assertEqual(slow_calls, 2)
        self.assertEqual(recovered.state, InvocationState.WAITING)
        self.assertEqual(len(recovered.waits), 1)
        recovered_app.resume(recovered, recovered.waits[0].id, "approved")
        self.assertEqual(
            recovered.result(),
            {"approval": "approved", "slow": "request"},
        )
        recovered_app.close()

    def test_resume_output_binding_reads_latest_context_after_other_branch_finishes(self) -> None:
        writer_started = threading.Event()
        release_writer = threading.Event()

        def writer(value: str) -> str:
            writer_started.set()
            release_writer.wait(2)
            return value

        def write_context(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"written_while_waiting": True})

        def bind_resume(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(
                invocation={
                    "resume_saw_latest": context.invocation_context.get(
                        "written_while_waiting", False
                    )
                }
            )

        workflow = Workflow(
            "resume-latest-context",
            nodes=[
                Node(
                    "approval",
                    WaitOperator(str, str),
                    output_binding=bind_resume,
                ),
                Node("writer", writer, output_binding=write_context),
            ],
        )
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        app.register_workflow(workflow)
        invocation = app.submit_invoke(
            workflow, "request", event_mode=EventMode.FULL
        )
        self.assertTrue(writer_started.wait(1))
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline and not invocation.waits:
            time.sleep(0.001)
        self.assertTrue(invocation.waits)

        release_writer.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and invocation.state is not InvocationState.WAITING:
            time.sleep(0.001)
        self.assertEqual(invocation.state, InvocationState.WAITING)
        app.resume(invocation, invocation.waits[0].id, "approved")

        self.assertTrue(
            invocation.latest_checkpoint.invocation_context["resume_saw_latest"]
        )
        binding = next(
            event
            for event in sink.events
            if event.event_name == "output_binding_finished"
            and event.subject_id
            == str(
                next(
                    item.execution_id
                    for item in invocation.latest_checkpoint.node_states
                    if item.node_id == "approval"
                )
            )
        )
        self.assertTrue(
            any(
                operation.path[:2] == ("invocation", "context")
                for batch in binding.operation_batches
                for operation in batch.operations
            )
        )
        app.close()

    def test_serial_node_and_terminal_boundaries_are_identical_in_every_mode(self) -> None:
        workflow = Workflow(
            "serial-checkpoint-boundaries",
            nodes=[Node("first", identity), Node("second", identity)],
            edges=[Edge("first", "second")],
        )
        for mode in EventMode:
            with self.subTest(mode=mode):
                sink = RecordingSink()
                app = AutoAgentApp(runtime_sink=sink)
                app.register_workflow(workflow)

                invocation = app.invoke(workflow, 3, event_mode=mode)

                self.assertEqual(invocation.result(), {"second": 3})
                self.assertEqual(
                    [item.invocation_state for item in sink.checkpoints],
                    ["created", "running", "running", "completed"],
                )
                node_states = [
                    {item.node_id: item.state for item in checkpoint.node_states}
                    for checkpoint in sink.checkpoints
                ]
                self.assertNotIn("first", node_states[0])
                self.assertEqual(node_states[1].get("first"), "completed")
                self.assertEqual(node_states[2].get("second"), "completed")
                app.close()

    def test_minimal_emits_no_runtime_events_but_keeps_three_core_boundaries(self) -> None:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow("minimal-checkpoints", nodes=[Node("node", identity)])
        app.register_workflow(workflow)

        invocation = app.invoke(workflow, 3, event_mode=EventMode.MINIMAL)

        self.assertEqual(sink.events, [])
        self.assertEqual(
            [item.invocation_state for item in sink.checkpoints],
            ["created", "running", "completed"],
        )
        self.assertEqual(invocation.latest_checkpoint.invocation_state, "completed")
        app.close()

    def test_resume_acceptance_adds_no_checkpoint(self) -> None:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "resume-boundaries",
            nodes=[Node("approval", WaitOperator(str, str))],
        )
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, "approve")
        self.assertEqual(invocation.state, InvocationState.WAITING)
        self.assertEqual(
            [item.invocation_state for item in sink.checkpoints],
            ["created", "waiting"],
        )

        app.resume(invocation, invocation.waits[0].id, "yes")

        self.assertEqual(invocation.result(), {"approval": "yes"})
        self.assertEqual(
            [item.invocation_state for item in sink.checkpoints],
            ["created", "waiting", "running", "completed"],
        )
        self.assertFalse(
            any(event.event_name == "invocation_recovered" for event in sink.events)
        )
        approval_states = [
            event.status
            for event in sink.events
            if event.event_name == "node_state_changed"
            and event.subject_id == "approval"
        ]
        self.assertEqual(
            approval_states,
            ["running", "waiting", "running", "completed"],
        )
        app.close()

    def test_cancel_acceptance_waits_for_node_and_invocation_terminal_boundaries(self) -> None:
        started = threading.Event()

        async def slow(value: int) -> int:
            started.set()
            await asyncio.sleep(60)
            return value

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow("cancel-boundaries", nodes=[Node("slow", slow)])
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, 1)
        self.assertTrue(started.wait(1))
        before = len(sink.checkpoints)

        app.cancel(invocation)

        added = sink.checkpoints[before:]
        self.assertEqual(invocation.state, InvocationState.CANCELLED)
        self.assertEqual(len(added), 2)
        self.assertTrue(all(item.invocation_state == "cancelled" for item in added))
        self.assertEqual(
            [event.event_name for event in sink.events if event.status == "cancelled"],
            ["operator_call_finished", "node_state_changed", "invocation_state_changed"],
        )
        app.close()

    def test_completed_node_checkpoint_replays_edge_without_rerunning_node(self) -> None:
        source_calls = 0
        condition_calls = 0
        edge_started = threading.Event()
        release_edge = threading.Event()

        def source(value: int) -> int:
            nonlocal source_calls
            source_calls += 1
            return value + 1

        def condition(_context: EdgeConditionContext) -> bool:
            nonlocal condition_calls
            condition_calls += 1
            edge_started.set()
            release_edge.wait(2)
            return True

        policy = NodePolicy(recovery=RecoveryPolicy("replay_safe"))
        workflow = Workflow(
            "edge-replay",
            nodes=[Node("source", source), Node("target", identity, policy=policy)],
            edges=[Edge("source", "target", condition=condition)],
        )
        sink = RecordingSink()
        source_app = AutoAgentApp(runtime_sink=sink)
        source_app.register_workflow(workflow)
        original = source_app.submit_invoke(workflow, 1)
        self.assertTrue(edge_started.wait(1))
        checkpoint = copy.deepcopy(original.latest_checkpoint)
        self.assertIsNotNone(checkpoint)
        self.assertEqual(
            [item.state for item in checkpoint.node_states if item.node_id == "source"],
            ["completed"],
        )
        self.assertTrue(checkpoint.pending_advances)
        release_edge.set()
        original.wait(2)
        source_app.close()

        recovered_app = AutoAgentApp()
        recovered_app.register_workflow(workflow)
        recovered = recovered_app.recover(checkpoint)

        self.assertEqual(recovered.result(), {"target": 2})
        self.assertEqual(source_calls, 1)
        self.assertEqual(condition_calls, 2)
        recovered_app.close()

    def test_edge_evaluation_is_not_a_checkpoint_boundary(self) -> None:
        edge_started = threading.Event()
        release_edge = threading.Event()

        def condition(_context: EdgeConditionContext) -> bool:
            edge_started.set()
            release_edge.wait(2)
            return True

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "no-edge-checkpoint",
            nodes=[Node("source", identity), Node("target", identity)],
            edges=[Edge("source", "target", condition=condition)],
        )
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, 1)
        self.assertTrue(edge_started.wait(1))

        self.assertEqual(len(sink.checkpoints), 2)
        self.assertEqual(
            [item.invocation_state for item in sink.checkpoints],
            ["created", "running"],
        )
        self.assertTrue(sink.checkpoints[-1].pending_advances)

        release_edge.set()
        invocation.wait(2)
        self.assertEqual(
            [item.invocation_state for item in sink.checkpoints],
            ["created", "running", "running", "completed"],
        )
        app.close()

    def test_failed_and_skipped_nodes_create_node_terminal_boundaries(self) -> None:
        def fail(_value: int) -> int:
            raise ValueError("broken")

        failed_sink = RecordingSink()
        failed_app = AutoAgentApp(runtime_sink=failed_sink)
        failed_workflow = Workflow("failed-boundaries", nodes=[Node("failed", fail)])
        failed_app.register_workflow(failed_workflow)
        failed = failed_app.invoke(failed_workflow, 1)
        self.assertEqual(failed.state, InvocationState.FAILED)
        self.assertEqual(
            [item.invocation_state for item in failed_sink.checkpoints],
            ["created", "running", "failed"],
        )
        self.assertEqual(failed_sink.checkpoints[1].node_states[0].state, "failed")
        failed_app.close()

        skipped_sink = RecordingSink()
        skipped_app = AutoAgentApp(runtime_sink=skipped_sink)

        def reject(_context: EdgeConditionContext) -> bool:
            return False

        skipped_workflow = Workflow(
            "skipped-boundaries",
            nodes=[Node("source", identity), Node("skipped", identity)],
            edges=[Edge("source", "skipped", condition=reject)],
        )
        skipped_app.register_workflow(skipped_workflow)
        skipped = skipped_app.invoke(skipped_workflow, 1)
        self.assertEqual(skipped.state, InvocationState.COMPLETED)
        self.assertEqual(
            [item.invocation_state for item in skipped_sink.checkpoints],
            ["created", "running", "running", "completed"],
        )
        self.assertTrue(
            any(
                key == "skipped" or key.startswith("skipped@")
                for key in skipped_sink.checkpoints[2].scheduler_state.skipped
            )
        )
        skipped_app.close()

    def test_parallel_checkpoint_replays_sibling_from_its_start_context(self) -> None:
        slow_started = threading.Event()
        release_slow = threading.Event()
        slow_calls = 0

        def fast(value: int) -> int:
            return value

        def write_fast(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"fast_committed": True})

        def slow(value: int) -> int:
            nonlocal slow_calls
            slow_calls += 1
            slow_started.set()
            release_slow.wait(2)
            return value

        def record_slow_start(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(
                invocation={
                    "slow_saw_fast": bool(
                        context.invocation_context.get("fast_committed", False)
                    )
                }
            )

        workflow = Workflow(
            "parallel-restart-baseline",
            nodes=[
                Node("fast", fast, output_binding=write_fast),
                Node(
                    "slow",
                    slow,
                    output_binding=record_slow_start,
                    policy=NodePolicy(recovery=RecoveryPolicy("replay_safe")),
                ),
            ],
        )
        sink = RecordingSink()
        original_app = AutoAgentApp(runtime_sink=sink)
        original_app.register_workflow(workflow)
        original = original_app.submit_invoke(workflow, 1)
        self.assertTrue(slow_started.wait(1))

        deadline = time.monotonic() + 1
        checkpoint = None
        while time.monotonic() < deadline:
            candidate = original.latest_checkpoint
            if candidate is not None and any(
                item.node_id == "fast" and item.state == "completed"
                for item in candidate.node_states
            ):
                checkpoint = candidate
                break
            time.sleep(0.001)
        self.assertIsNotNone(checkpoint)
        slow_state = next(
            item for item in checkpoint.node_states if item.node_id == "slow"
        )
        self.assertEqual(slow_state.state, "running")
        self.assertNotIn("fast_committed", slow_state.restart_invocation_context)

        release_slow.set()
        original.wait(2)
        self.assertFalse(
            original.latest_checkpoint.invocation_context["slow_saw_fast"]
        )
        original_app.close()

        recovered_app = AutoAgentApp()
        recovered_app.register_workflow(workflow)
        recovered = recovered_app.recover(checkpoint)
        self.assertEqual(recovered.state, InvocationState.COMPLETED)
        self.assertFalse(
            recovered.latest_checkpoint.invocation_context["slow_saw_fast"]
        )
        self.assertEqual(slow_calls, 2)
        recovered_app.close()

    def test_failed_and_cancelled_terminal_checkpoints_restore_without_execution(self) -> None:
        calls = 0

        def fail(_value: int) -> int:
            nonlocal calls
            calls += 1
            raise ValueError("terminal failure")

        failed_app = AutoAgentApp()
        failed_workflow = Workflow("restore-failed-terminal", nodes=[Node("node", fail)])
        failed_app.register_workflow(failed_workflow)
        failed = failed_app.invoke(failed_workflow, 1)
        failed_checkpoint = failed.latest_checkpoint
        self.assertEqual(calls, 1)
        failed_app.close()

        restored_failed_app = AutoAgentApp()
        restored_failed_app.register_workflow(failed_workflow)
        restored_failed = restored_failed_app.recover(failed_checkpoint)
        self.assertEqual(restored_failed.state, InvocationState.FAILED)
        self.assertEqual(calls, 1)
        restored_failed_app.close()

        started = threading.Event()

        async def block(value: int) -> int:
            started.set()
            await asyncio.sleep(60)
            return value

        cancelled_app = AutoAgentApp()
        cancelled_workflow = Workflow(
            "restore-cancelled-terminal",
            nodes=[Node("node", block)],
        )
        cancelled_app.register_workflow(cancelled_workflow)
        cancelled = cancelled_app.submit_invoke(cancelled_workflow, 1)
        self.assertTrue(started.wait(1))
        cancelled_app.cancel(cancelled)
        cancelled_checkpoint = cancelled.latest_checkpoint
        cancelled_app.close()

        restored_cancelled_app = AutoAgentApp()
        restored_cancelled_app.register_workflow(cancelled_workflow)
        restored_cancelled = restored_cancelled_app.recover(cancelled_checkpoint)
        self.assertEqual(restored_cancelled.state, InvocationState.CANCELLED)
        restored_cancelled_app.close()


class RuntimeEventContractTests(unittest.TestCase):
    def test_output_binding_event_is_finalized_only_after_atomic_context_commit(self) -> None:
        operator_barrier = threading.Barrier(2)

        def concurrent(value: int) -> int:
            operator_barrier.wait(timeout=2)
            return value

        def conflicting_binding(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"shared": context.output})

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "binding-commit-order",
            nodes=[
                Node("left", concurrent, output_binding=conflicting_binding),
                Node("right", concurrent, output_binding=conflicting_binding),
            ],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(workflow, 7, event_mode=EventMode.FULL)

        self.assertEqual(invocation.state, InvocationState.FAILED)
        bindings = [
            event
            for event in sink.events
            if event.event_name == "output_binding_finished"
        ]
        self.assertEqual(
            sorted(event.status for event in bindings),
            ["completed", "failed"],
        )
        completed = next(event for event in bindings if event.status == "completed")
        self.assertTrue(completed.operation_batches)
        self.assertTrue(
            any(
                operation.path[:2] == ("invocation", "context")
                for batch in completed.operation_batches
                for operation in batch.operations
            )
        )
        node_events = [
            event
            for event in sink.events
            if event.event_name == "node_state_changed"
        ]
        self.assertFalse(
            any(
                operation.path[:2]
                in {("session", "context"), ("invocation", "context")}
                for event in node_events
                for batch in event.operation_batches
                for operation in batch.operations
            )
        )
        app.close()

    def test_fail_fast_skipped_checkpoint_has_converged_scheduler_state(self) -> None:
        def fail(_value: int) -> int:
            raise ValueError("broken")

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "terminal-convergence-checkpoint",
            nodes=[Node("failed", fail), Node("never_runs", identity)],
            edges=[Edge("failed", "never_runs")],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(workflow, 1, event_mode=EventMode.FULL)

        self.assertEqual(invocation.state, InvocationState.FAILED)
        skipped_event_index = next(
            index
            for index, event in enumerate(sink.events)
            if event.event_name == "node_state_changed"
            and event.subject_id == "never_runs"
            and event.status == "skipped"
        )
        self.assertGreaterEqual(skipped_event_index, 0)
        converged = [
            checkpoint
            for checkpoint in sink.checkpoints
            if any(
                key == "never_runs" or key.startswith("never_runs@")
                for key in checkpoint.scheduler_state.skipped
            )
        ]
        self.assertTrue(converged)
        for checkpoint in converged:
            self.assertFalse(
                any(request.node_id == "never_runs" for request in checkpoint.scheduler_state.ready)
            )
        app.close()

    @staticmethod
    def _workflow() -> Workflow:
        def map_input(context: InputMappingContext) -> int:
            time.sleep(0.001)
            return int(context.invocation_input)

        def bind(context: OutputBindingContext) -> ContextPatch:
            time.sleep(0.001)
            return ContextPatch(invocation={"answer": context.output})

        def condition(_context: EdgeConditionContext) -> bool:
            time.sleep(0.001)
            return True

        return Workflow(
            "timed-events",
            nodes=[
                Node("first", identity, input_mapping=map_input, output_binding=bind),
                Node("last", identity),
            ],
            edges=[Edge("first", "last", condition=condition)],
        )

    def _run(self, mode: EventMode) -> list[RuntimeEvent]:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = self._workflow()
        app.register_workflow(workflow)
        app.invoke(workflow, 5, event_mode=mode)
        app.close()
        return sink.events

    def test_standard_and_full_share_semantic_event_boundaries(self) -> None:
        standard = self._run(EventMode.STANDARD)
        full = self._run(EventMode.FULL)
        self.assertEqual(
            [(item.event_name, item.subject_type, item.status) for item in standard],
            [(item.event_name, item.subject_type, item.status) for item in full],
        )
        self.assertTrue(all(not item.operation_batches for item in standard))
        self.assertTrue(any(item.operation_batches for item in full))

    def test_standard_and_full_match_for_wait_resume_retry_and_fallback(self) -> None:
        def run(mode: EventMode) -> list[RuntimeEvent]:
            def primary(value: str) -> str:
                raise ValueError(f"retry {value}")

            def fallback(value: str) -> str:
                return value.upper()

            sink = RecordingSink()
            app = AutoAgentApp(runtime_sink=sink)
            workflow = Workflow(
                "mode-parity-control-flow",
                nodes=[
                    Node("approval", WaitOperator(str, str)),
                    Node(
                        "work",
                        Operator(primary, id="primary"),
                        fallback_operators=(Operator(fallback, id="fallback"),),
                        policy=NodePolicy(
                            retry=RetryPolicy(
                                max_attempts=2,
                                backoff=BackoffPolicy(initial_delay_ms=0),
                            )
                        ),
                    ),
                ],
                edges=[Edge("approval", "work")],
            )
            app.register_workflow(workflow)
            invocation = app.invoke(workflow, "question", event_mode=mode)
            app.resume(invocation, invocation.waits[0].id, "answer")
            self.assertEqual(invocation.result(), {"work": "ANSWER"})
            events = sink.events
            app.close()
            return events

        standard = run(EventMode.STANDARD)
        full = run(EventMode.FULL)
        semantic = lambda event: (
            event.event_name,
            event.subject_type,
            event.status,
        )
        self.assertEqual(
            [semantic(event) for event in standard],
            [semantic(event) for event in full],
        )
        self.assertEqual(
            [
                event.status
                for event in full
                if event.event_name == "operator_call_finished"
            ],
            ["failed", "failed", "completed"],
        )
        self.assertEqual(
            [
                event.status
                for event in full
                if event.event_name == "node_state_changed"
                and event.subject_id == "approval"
            ],
            ["running", "waiting", "running", "completed"],
        )

    def test_state_events_have_only_occurrence_time(self) -> None:
        events = self._run(EventMode.FULL)
        state_events = [
            event
            for event in events
            if event.event_name in {"invocation_state_changed", "node_state_changed"}
        ]
        self.assertTrue(state_events)
        for event in state_events:
            self.assertIsInstance(event.occurred_at_ms, int)
            self.assertIsNone(event.started_at_ms)
            self.assertIsNone(event.completed_at_ms)
            self.assertIsNone(event.duration_ns)

    def test_measured_events_have_wall_bounds_monotonic_duration_and_breakdown(self) -> None:
        events = self._run(EventMode.FULL)
        measured = [
            event
            for event in events
            if event.event_name
            in {
                "edge_evaluated",
                "input_mapping_finished",
                "operator_call_finished",
                "output_binding_finished",
            }
        ]
        self.assertTrue(measured)
        for event in measured:
            self.assertIsNotNone(event.started_at_ms)
            self.assertIsNotNone(event.completed_at_ms)
            self.assertLessEqual(event.started_at_ms, event.completed_at_ms)
            self.assertIsNotNone(event.duration_ns)
            self.assertGreaterEqual(event.duration_ns, 0)
            timing = event.payload["timing"]
            self.assertTrue(all(value >= 0 for value in timing.values()))
            self.assertLessEqual(
                sum(timing.values()),
                event.duration_ns + timing.get("stream_delivery_ns", 0),
            )

    def test_standard_removes_heavy_phase_values_but_keeps_timing(self) -> None:
        events = self._run(EventMode.STANDARD)
        phases = [event for event in events if event.subject_type == "node_phase"]
        self.assertTrue(phases)
        for event in phases:
            self.assertIn("timing", event.payload)
            self.assertTrue(
                {"input", "output", "items", "patch"}.isdisjoint(event.payload)
            )

    def test_runtime_event_vocabulary_contains_no_control_specific_events(self) -> None:
        names = {event.event_name for event in self._run(EventMode.FULL)}
        self.assertTrue(
            names
            <= {
                "invocation_state_changed",
                "node_state_changed",
                "edge_evaluated",
                "input_mapping_finished",
                "item_selection_finished",
                "operator_call_finished",
                "aggregation_finished",
                "output_binding_finished",
            }
        )

    def test_default_phases_are_omitted_but_unconditional_edges_are_observable(self) -> None:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "default-phase-boundaries",
            nodes=[Node("first", identity), Node("last", identity)],
            edges=[Edge("first", "last")],
        )
        app.register_workflow(workflow)
        app.invoke(workflow, 1, event_mode=EventMode.FULL)

        names = [event.event_name for event in sink.events]
        self.assertNotIn("input_mapping_finished", names)
        self.assertNotIn("item_selection_finished", names)
        self.assertNotIn("aggregation_finished", names)
        self.assertNotIn("output_binding_finished", names)
        edges = [event for event in sink.events if event.event_name == "edge_evaluated"]
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0].status, "selected")
        self.assertEqual(
            edges[0].payload["timing"],
            {
                "executor_wait_ns": 0,
                "thread_pool_wait_ns": 0,
                "handler_ns": 0,
            },
        )
        app.close()

    def test_failed_edge_records_timing_before_invocation_failure(self) -> None:
        def broken_condition(_context: EdgeConditionContext) -> bool:
            time.sleep(0.002)
            raise ValueError("edge failure")

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "failed-edge-timing",
            nodes=[Node("first", identity), Node("last", identity)],
            edges=[Edge("first", "last", condition=broken_condition)],
        )
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1, event_mode=EventMode.FULL)

        edge = next(
            event for event in sink.events if event.event_name == "edge_evaluated"
        )
        self.assertEqual(edge.status, "failed")
        self.assertLessEqual(edge.started_at_ms, edge.completed_at_ms)
        self.assertGreater(edge.duration_ns, 0)
        self.assertGreater(edge.payload["timing"]["handler_ns"], 0)
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertLess(
            sink.events.index(edge),
            next(
                index
                for index, event in enumerate(sink.events)
                if event.event_name == "invocation_state_changed"
                and event.status == "failed"
            ),
        )
        app.close()

    def test_selector_aggregation_and_each_physical_call_are_measured(self) -> None:
        def mapped_identity(value: int) -> int:
            time.sleep(0.002)
            return value

        def select(context: ItemSelectorContext) -> list[int]:
            time.sleep(0.001)
            return list(context.input)

        def aggregate(context: AggregationContext) -> int:
            time.sleep(0.001)
            return sum(context.operator_outputs)

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "all-measured-map-events",
            nodes=[
                Node(
                    "mapped",
                    mapped_identity,
                    policy=NodePolicy(
                        map=MapPolicy(
                            item_selector=select,
                            output_aggregator=aggregate,
                            max_parallelism=1,
                        )
                    ),
                )
            ],
        )
        app.register_workflow(workflow)

        result = app.invoke(workflow, [1, 2, 3], event_mode=EventMode.FULL)

        self.assertEqual(result.result(), {"mapped": 6})
        selected = [
            event
            for event in sink.events
            if event.event_name == "item_selection_finished"
        ]
        aggregated = [
            event
            for event in sink.events
            if event.event_name == "aggregation_finished"
        ]
        calls = [
            event
            for event in sink.events
            if event.event_name == "operator_call_finished"
        ]
        self.assertEqual(len(selected), 1)
        self.assertEqual(len(aggregated), 1)
        self.assertEqual(len(calls), 3)
        self.assertEqual(
            sorted(event.payload["unit_index"] for event in calls),
            [0, 1, 2],
        )
        self.assertGreater(
            max(event.payload["timing"]["dispatch_wait_ns"] for event in calls),
            0,
        )
        for event in [*selected, *calls, *aggregated]:
            self.assertIsNotNone(event.started_at_ms)
            self.assertIsNotNone(event.completed_at_ms)
            self.assertGreaterEqual(event.duration_ns, 0)
        app.close()

    def test_failed_hook_keeps_actual_timing_and_then_fails_node(self) -> None:
        def broken_mapping(_context: InputMappingContext) -> int:
            time.sleep(0.002)
            raise ValueError("bad mapping")

        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "failed-hook-timing",
            nodes=[Node("node", identity, input_mapping=broken_mapping)],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(workflow, 1, event_mode=EventMode.FULL)

        phase = next(
            event
            for event in sink.events
            if event.event_name == "input_mapping_finished"
        )
        self.assertEqual(phase.status, "failed")
        self.assertGreater(phase.duration_ns, 0)
        self.assertGreater(phase.payload["timing"]["handler_ns"], 0)
        node_states = [
            event.status
            for event in sink.events
            if event.event_name == "node_state_changed"
            and event.subject_id == "node"
        ]
        self.assertEqual(node_states, ["running", "failed"])
        self.assertEqual(invocation.state, InvocationState.FAILED)
        app.close()


if __name__ == "__main__":
    unittest.main()
    MapPolicy,
