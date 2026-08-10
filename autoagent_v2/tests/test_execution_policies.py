from __future__ import annotations

import asyncio
import threading
import time
import unittest
from typing import Any

from autoagent.core import (
    AutoAgentApp,
    BackoffPolicy,
    ContextPatch,
    Edge,
    EventMode,
    InputMappingContext,
    AggregationContext,
    OutputBindingContext,
    FailurePolicy,
    MapPolicy,
    Node,
    NodePolicy,
    Operator,
    RecoveryPolicy,
    ReplicationPolicy,
    RetryPolicy,
    TimeoutPolicy,
    Workflow,
    WorkflowPolicy,
)
from tests.helpers import always_false, decode_checkpoint, decode_events, identity_int, increment


def sum_values(context: AggregationContext) -> int:
    return sum(context.operator_outputs)


def identity_inputs(values: dict[str, int]) -> dict[str, int]:
    return values


class _Sink:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self.checkpoints: list[Any] = []

    async def wait_until_admissible(self) -> None:
        return None

    async def submit_events(self, events: tuple[Any, ...]) -> None:
        self.events.extend(decode_events(events))

    def offer_checkpoint(self, checkpoint: Any) -> None:
        self.checkpoints.append(decode_checkpoint(checkpoint))

class ExecutionPolicyTests(unittest.TestCase):
    def test_full_operator_event_captures_actual_input_before_mutation(self) -> None:
        seen_keys: list[str] = []

        def mutate(payload: list[int], idempotency_key: str) -> list[int]:
            seen_keys.append(idempotency_key)
            payload.append(99)
            return payload

        sink = _Sink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "operator-input-snapshot",
            nodes=[
                Node(
                    "call",
                    mutate,
                    policy=NodePolicy(recovery=RecoveryPolicy(mode="idempotent")),
                )
            ],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(
            workflow,
            {"payload": [1]},
            event_mode=EventMode.FULL,
        )

        call = next(
            event for event in sink.events if event.subject_type == "operator_call"
        )
        self.assertEqual(call.status, "completed")
        self.assertEqual(call.payload["input"]["payload"], [1])
        self.assertEqual(call.payload["output"], [1, 99])
        self.assertEqual(
            call.payload["input"]["idempotency_key"],
            call.payload["idempotency_key"],
        )
        self.assertEqual(seen_keys, [call.payload["idempotency_key"]])
        self.assertLessEqual(call.started_at_ms, call.completed_at_ms)
        self.assertGreater(call.duration_ns, 0)
        self.assertEqual(invocation.result(), {"call": [1, 99]})
        app.close()

    def test_failed_operator_event_preserves_pre_call_input(self) -> None:
        def mutate_then_fail(payload: list[int]) -> int:
            payload.append(99)
            raise ValueError("business failure")

        sink = _Sink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "failed-operator-input-snapshot",
            nodes=[Node("call", mutate_then_fail)],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(
            workflow,
            {"payload": [1]},
            event_mode=EventMode.FULL,
        )

        call_events = [
            event for event in sink.events if event.subject_type == "operator_call"
        ]
        self.assertEqual(len(call_events), 1)
        self.assertEqual(call_events[0].status, "failed")
        self.assertEqual(call_events[0].payload["input"], {"payload": [1]})
        self.assertIsNone(call_events[0].payload["output"])
        self.assertEqual(invocation.state, "failed")
        app.close()

    def test_retry_and_fallback_each_receive_fresh_pre_call_input(self) -> None:
        seen: list[tuple[str, list[int]]] = []

        def primary(payload: list[int]) -> int:
            seen.append(("primary", list(payload)))
            payload.append(99)
            raise ValueError("retry with a clean input")

        def fallback(payload: list[int]) -> int:
            seen.append(("fallback", list(payload)))
            payload.append(7)
            return sum(payload)

        sink = _Sink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "isolated-retry-inputs",
            nodes=[
                Node(
                    "call",
                    Operator(primary, id="primary"),
                    fallback_operators=(Operator(fallback, id="fallback"),),
                    policy=NodePolicy(
                        retry=RetryPolicy(
                            max_attempts=2,
                            backoff=BackoffPolicy(initial_delay_ms=0),
                        )
                    ),
                )
            ],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(
            workflow,
            {"payload": [1]},
            event_mode=EventMode.FULL,
        )

        self.assertEqual(
            seen,
            [("primary", [1]), ("primary", [1]), ("fallback", [1])],
        )
        call_events = [
            event for event in sink.events if event.subject_type == "operator_call"
        ]
        self.assertEqual(len(call_events), 3)
        self.assertTrue(
            all(event.payload["input"] == {"payload": [1]} for event in call_events)
        )
        self.assertEqual(invocation.result(), {"call": 8})
        app.close()

    def test_standard_operator_event_omits_input_and_output(self) -> None:
        sink = _Sink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow("standard-call-detail", nodes=[Node("call", identity_int)])
        app.register_workflow(workflow)

        app.invoke(workflow, 3, event_mode=EventMode.STANDARD)

        call = next(
            event for event in sink.events if event.subject_type == "operator_call"
        )
        self.assertNotIn("input", call.payload)
        self.assertNotIn("output", call.payload)
        self.assertLessEqual(call.started_at_ms, call.completed_at_ms)
        app.close()

    def test_timed_out_operator_finalizes_exactly_one_event(self) -> None:
        async def slow(value: int) -> int:
            await asyncio.sleep(1)
            return value

        sink = _Sink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "timed-out-call-event",
            nodes=[Node("call", slow, policy=NodePolicy(timeout=TimeoutPolicy(5)))],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(workflow, 1, event_mode=EventMode.STANDARD)

        call_events = [
            event for event in sink.events if event.subject_type == "operator_call"
        ]
        self.assertEqual(len(call_events), 1)
        self.assertEqual(call_events[0].status, "timed_out")
        self.assertLessEqual(
            call_events[0].started_at_ms,
            call_events[0].completed_at_ms,
        )
        self.assertGreater(call_events[0].duration_ns, 0)
        self.assertTrue(
            {
                "dispatch_wait_ns",
                "executor_wait_ns",
                "thread_pool_wait_ns",
                "handler_ns",
                "stream_ns",
                "stream_delivery_ns",
            }
            <= call_events[0].payload["timing"].keys()
        )
        self.assertGreater(call_events[0].payload["timing"]["handler_ns"], 0)
        self.assertEqual(invocation.state, "failed")
        app.close()

    def test_cancelled_operator_finalizes_event_before_invocation_cancel(self) -> None:
        started = threading.Event()

        async def blocking(value: int) -> int:
            started.set()
            await asyncio.sleep(10)
            return value

        sink = _Sink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow("cancelled-call-event", nodes=[Node("call", blocking)])
        app.register_workflow(workflow)
        invocation = app.submit_invoke(workflow, 1, event_mode=EventMode.FULL)
        self.assertTrue(started.wait(1))

        cancelled = app.cancel(invocation)

        call_events = [
            event for event in sink.events if event.subject_type == "operator_call"
        ]
        self.assertEqual(len(call_events), 1)
        self.assertEqual(call_events[0].status, "cancelled")
        self.assertEqual(call_events[0].payload["input"], 1)
        self.assertIsNone(call_events[0].payload["output"])
        self.assertLessEqual(
            call_events[0].started_at_ms,
            call_events[0].completed_at_ms,
        )
        self.assertGreater(call_events[0].duration_ns, 0)
        self.assertGreater(call_events[0].payload["timing"]["handler_ns"], 0)
        invocation_cancel_index = next(
            index
            for index, event in enumerate(sink.events)
            if event.subject_type == "invocation" and event.status == "cancelled"
        )
        self.assertLess(sink.events.index(call_events[0]), invocation_cancel_index)
        self.assertEqual(cancelled.state, "cancelled")
        app.close()

    def test_all_skipped_path_completes_with_empty_output(self) -> None:
        workflow = Workflow(
            "dead-end",
            nodes=[Node("start", identity_int), Node("exit", identity_int)],
            edges=[Edge("start", "exit", condition=always_false)],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1)
        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result(), {})
        app.close()

    def test_retry_then_fallback_records_every_ordinary_call(self) -> None:
        calls: list[str] = []

        def primary(value: int) -> int:
            calls.append("primary")
            raise OSError("offline")

        def fallback(value: int) -> int:
            calls.append("fallback")
            return value + 10

        sink = _Sink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "fallback",
            nodes=[
                Node(
                    "call",
                    Operator(primary, id="primary"),
                    fallback_operators=(Operator(fallback, id="fallback"),),
                    policy=NodePolicy(
                        retry=RetryPolicy(
                            max_attempts=2,
                            backoff=BackoffPolicy(initial_delay_ms=0),
                        )
                    ),
                )
            ],
        )
        app.register_workflow(workflow)

        invocation = app.invoke(workflow, 2, event_mode=EventMode.FULL)

        self.assertEqual(invocation.result(), {"call": 12})
        self.assertEqual(calls, ["primary", "primary", "fallback"])
        operator_events = [
            event for event in sink.events if getattr(event, "subject_type", None) == "operator_call"
        ]
        self.assertEqual([event.status for event in operator_events], ["failed", "failed", "completed"])
        app.close()

    def test_map_and_replication_record_each_call_with_bounded_parallelism(self) -> None:
        lock = threading.Lock()
        active = 0
        peak = 0

        def work(value: int) -> int:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.01)
            with lock:
                active -= 1
            return value * 2

        sink = _Sink()
        app = AutoAgentApp(runtime_sink=sink)
        workflow = Workflow(
            "map",
            nodes=[
                Node(
                    "mapped",
                    work,
                    policy=NodePolicy(
                        map=MapPolicy(max_parallelism=2, output_aggregator=sum_values)
                    ),
                )
            ],
        )
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, [1, 2, 3, 4], event_mode="full")
        self.assertEqual(invocation.result(), {"mapped": 20})
        self.assertLessEqual(peak, 2)
        calls = [
            event
            for event in sink.events
            if getattr(event, "subject_type", None) == "operator_call"
        ]
        self.assertEqual(len(calls), 4)
        self.assertEqual(
            sorted(event.payload["unit_index"] for event in calls), [0, 1, 2, 3]
        )
        self.assertTrue(all("output" in event.payload for event in calls))
        app.close()

        replication = Workflow(
            "replication",
            nodes=[
                Node(
                    "replicas",
                    increment,
                    policy=NodePolicy(replication=ReplicationPolicy(count=3)),
                )
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(replication)
        self.assertEqual(app.invoke(replication, 1).result(), {"replicas": [2, 2, 2]})
        app.close()

    def test_timeout_fails_node_and_invocation(self) -> None:
        async def slow(value: int) -> int:
            await asyncio.sleep(0.1)
            return value

        app = AutoAgentApp()
        workflow = Workflow(
            "timeout",
            nodes=[Node("slow", slow, policy=NodePolicy(timeout=TimeoutPolicy(5)))],
        )
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1)
        self.assertEqual(invocation.state, "failed")
        self.assertIn("timeout", invocation.error.type.lower())
        app.close()

    def test_continue_active_branches_runs_healthy_branch_then_fails_invocation(self) -> None:
        reached: list[int] = []

        def fail(value: int) -> int:
            raise ValueError("bad branch")

        def healthy(value: int) -> int:
            return value + 1

        def finish(value: int) -> int:
            reached.append(value)
            return value

        workflow = Workflow(
            "continue",
            policy=WorkflowPolicy(FailurePolicy("continue_active_branches")),
            nodes=[
                Node("start", identity_int),
                Node("fail", fail),
                Node("healthy", healthy),
                Node("finish", finish),
            ],
            edges=[
                Edge("start", "fail"),
                Edge("start", "healthy"),
                Edge("fail", "finish"),
                Edge("healthy", "finish"),
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 3)
        self.assertEqual(reached, [4])
        self.assertEqual(invocation.state, "failed")
        app.close()

    def test_parallel_nested_context_paths_conflict_but_distinct_paths_commit(self) -> None:
        def binding_a(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"shared": {"a": context.output}})

        def binding_b(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"shared": {"b": context.output}})

        workflow = Workflow(
            "nested-distinct",
            nodes=[
                Node("start", identity_int),
                Node("a", identity_int, output_binding=binding_a),
                Node("b", identity_int, output_binding=binding_b),
                Node("join", identity_inputs),
            ],
            edges=[Edge("start", "a"), Edge("start", "b"), Edge("a", "join"), Edge("b", "join")],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        self.assertEqual(app.invoke(workflow, 1).state, "completed")
        app.close()

        def conflict(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"shared": {"a": context.output + 1}})

        workflow.nodes[2].output_binding = conflict
        conflicting = AutoAgentApp()
        conflicting.register_workflow(workflow)
        invocation = conflicting.invoke(workflow, 1)
        self.assertEqual(invocation.state, "failed")
        self.assertIn("same Context path", invocation.error.message)
        conflicting.close()


if __name__ == "__main__":
    unittest.main()
