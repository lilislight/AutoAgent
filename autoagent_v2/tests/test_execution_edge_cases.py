from __future__ import annotations

import asyncio
import threading
import time
import unittest
from collections.abc import AsyncIterator, Iterator
from typing import Any

from autoagent.core import (
    AutoAgentApp,
    BackoffPolicy,
    ContextPatch,
    Edge,
    EventMode,
    ExecutionContext,
    InvocationState,
    MapPolicy,
    Node,
    NodePolicy,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    SinkPressure,
    StreamPolicy,
    UserEventMapping,
    WaitOperator,
    Workflow,
    WorkflowCompileError,
)
from tests.helpers import identity_int, identity_str


class IntSumReducer:
    def __init__(self) -> None:
        self.values: list[int] = []

    def add(self, chunk: int) -> None:
        self.values.append(chunk)

    def finish(self) -> int:
        return sum(self.values)


class FailingTextReducer:
    def add(self, _chunk: str) -> None:
        raise ValueError("bad chunk")

    def finish(self) -> str:
        return "unused"


def identity_none(value: None) -> None:
    return value


def empty_items(_context: ExecutionContext, _value: None) -> list[None]:
    return []


def single_text(_context: ExecutionContext, value: str) -> list[str]:
    return [value]


class CaptureSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def wait_until_admissible(self, timeout: float | None) -> bool:
        return True

    async def submit_events(self, events: tuple[Any, ...]) -> None:
        self.events.extend(events)

    def offer_checkpoint(self, checkpoint: Any) -> None:
        pass

    def pressure(self) -> SinkPressure:
        return SinkPressure(True)


class NodePhaseFailureTests(unittest.TestCase):
    def test_input_mapping_failure_marks_node_and_invocation_failed(self) -> None:
        def fail(_context: ExecutionContext) -> int:
            raise ValueError("mapping failed")

        sink = CaptureSink()
        app = AutoAgentApp(runtime_sink=sink)
        value = Workflow(
            "input-failure",
            nodes=[Node("node", identity_int, input_mapping=fail)],
        )
        app.register_workflow(value)
        invocation = app.invoke(value, 1, event_mode="full")
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertTrue(
            any(
                event.event_name == "input_mapping_completed"
                and event.status == "failed"
                for event in sink.events
            )
        )
        self.assertTrue(
            any(
                event.event_name == "node_state_changed"
                and event.status == "failed"
                for event in sink.events
            )
        )
        app.close()

    def test_output_binding_failure_rolls_back_the_entire_patch(self) -> None:
        def fail(_context: ExecutionContext, _value: int) -> ContextPatch:
            patch = ContextPatch(session={"should_not_exist": True})
            raise ValueError(f"binding failed: {patch}")

        app = AutoAgentApp()
        value = Workflow(
            "binding-failure",
            nodes=[Node("node", identity_int, output_binding=fail)],
        )
        app.register_workflow(value)
        invocation = app.invoke(value, 1, session_id="session", event_mode="full")
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertEqual(app._sessions["session"].context, {})
        app.close()

    def test_edge_condition_failure_fails_invocation(self) -> None:
        def condition(_context: ExecutionContext) -> bool:
            raise LookupError("condition failed")

        sink = CaptureSink()
        app = AutoAgentApp(runtime_sink=sink)
        value = Workflow(
            "condition-failure",
            nodes=[Node("start", identity_int), Node("finish", identity_int)],
            edges=[Edge("start", "finish", condition=condition)],
        )
        app.register_workflow(value)
        invocation = app.invoke(value, 1)
        self.assertEqual(invocation.state, InvocationState.FAILED)
        edge = next(event for event in sink.events if event.event_name == "edge_evaluated")
        self.assertEqual(edge.status, "failed")
        self.assertIn("condition failed", edge.payload["error"])
        app.close()

    def test_user_event_mapping_failure_is_isolated_from_business_result(self) -> None:
        def fail(_value: int) -> dict[str, str]:
            raise ValueError("presentation failed")

        sink = CaptureSink()
        app = AutoAgentApp(runtime_sink=sink)
        value = Workflow(
            "mapping-isolation",
            nodes=[
                Node(
                    "node",
                    identity_int,
                    user_event_mappings=(UserEventMapping("agent_output", fail),),
                )
            ],
        )
        app.register_workflow(value)
        invocation = app.invoke(value, 1)
        self.assertEqual(invocation.result(), {"node": 1})
        event = next(event for event in sink.events if hasattr(event, "type"))
        self.assertEqual(event.type, "user_event_mapping_failed")
        self.assertIn("presentation failed", event.data["error"])
        app.close()

    def test_output_user_event_is_not_emitted_after_binding_failure(self) -> None:
        sink = CaptureSink()

        def fail(_context: ExecutionContext, _value: int) -> ContextPatch | None:
            raise ValueError("binding failed")

        app = AutoAgentApp(runtime_sink=sink)
        value = Workflow(
            "no-output-event",
            nodes=[
                Node(
                    "node",
                    identity_int,
                    output_binding=fail,
                    user_event_mappings=(UserEventMapping("agent_output", identity_int),),
                )
            ],
        )
        app.register_workflow(value)
        app.invoke(value, 1)
        self.assertFalse(any(hasattr(event, "type") for event in sink.events))
        app.close()


class StreamingAndParallelTests(unittest.TestCase):
    def test_streaming_operators_require_stream_policy(self) -> None:
        def sync_generator(_value: None) -> Iterator[str]:
            yield "chunk"

        async def async_generator(_value: None) -> AsyncIterator[str]:
            yield "chunk"

        for workflow_id, operator in (
            ("raw-sync", sync_generator),
            ("raw-async", async_generator),
        ):
            app = AutoAgentApp()
            value = Workflow(workflow_id, nodes=[Node("node", operator)])
            with self.assertRaisesRegex(WorkflowCompileError, "requires StreamPolicy"):
                app.register_workflow(value)
            app.close()

    def test_async_stream_produces_one_final_output(self) -> None:
        async def operator(_value: None) -> AsyncIterator[int]:
            for value in (1, 2, 3):
                await asyncio.sleep(0)
                yield value

        app = AutoAgentApp()
        value = Workflow(
            "async-stream-result",
            nodes=[Node("node", operator, policy=NodePolicy(stream=StreamPolicy(IntSumReducer)))],
        )
        app.register_workflow(value)
        self.assertEqual(app.invoke(value, None).result(), {"node": 6})
        app.close()

    def test_stream_reducer_failure_enters_retry(self) -> None:
        attempts = 0

        def operator(_value: None) -> Iterator[str]:
            nonlocal attempts
            attempts += 1
            return iter(["x"])

        app = AutoAgentApp()
        value = Workflow(
            "stream-retry",
            nodes=[
                Node(
                    "node",
                    operator,
                    policy=NodePolicy(
                        retry=RetryPolicy(max_attempts=2),
                        stream=StreamPolicy(FailingTextReducer),
                    ),
                )
            ],
        )
        app.register_workflow(value)
        invocation = app.invoke(value, None)
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertEqual(attempts, 2)
        app.close()

    def test_empty_map_aggregates_to_an_empty_list_without_operator_calls(self) -> None:
        calls = 0

        def operator(value: None) -> None:
            nonlocal calls
            calls += 1
            return value

        app = AutoAgentApp()
        value = Workflow(
            "empty-map",
            nodes=[
                Node(
                    "node",
                    operator,
                    policy=NodePolicy(map=MapPolicy(item_selector=empty_items)),
                )
            ],
        )
        app.register_workflow(value)
        self.assertEqual(app.invoke(value, None).result(), {"node": []})
        self.assertEqual(calls, 0)
        app.close()

    def test_replication_without_aggregator_returns_all_outputs_in_order(self) -> None:
        app = AutoAgentApp()
        value = Workflow(
            "replicas",
            nodes=[
                Node(
                    "node",
                    identity_str,
                    policy=NodePolicy(replication=ReplicationPolicy(count=3)),
                )
            ],
        )
        app.register_workflow(value)
        self.assertEqual(app.invoke(value, "x").result(), {"node": ["x", "x", "x"]})
        app.close()

    def test_wait_operator_rejects_map_and_replication_policies(self) -> None:
        for workflow_id, policy in (
            ("map-wait", NodePolicy(map=MapPolicy(item_selector=single_text))),
            ("replica-wait", NodePolicy(replication=ReplicationPolicy(count=2))),
        ):
            app = AutoAgentApp()
            value = Workflow(
                workflow_id,
                nodes=[Node("node", WaitOperator(str, str), policy=policy)],
            )
            with self.assertRaisesRegex(WorkflowCompileError, "cannot define NodePolicy"):
                app.register_workflow(value)
            app.close()

    def test_wait_node_can_coexist_with_parallel_work(self) -> None:
        app = AutoAgentApp()
        value = Workflow(
            "parallel-wait",
            nodes=[
                Node("start", identity_str),
                Node("wait", WaitOperator(str, str)),
                Node("other", identity_str),
            ],
            edges=[Edge("start", "wait"), Edge("start", "other")],
        )
        app.register_workflow(value)
        invocation = app.invoke(value, "value")
        self.assertEqual(invocation.state, InvocationState.WAITING)
        self.assertEqual(len(invocation.waits), 1)
        completed = app.resume(invocation, invocation.waits[0].id, "answer")
        self.assertEqual(completed.result(), {"wait": "answer", "other": "value"})
        app.close()


class ResourceAndRetryTests(unittest.TestCase):
    def test_node_execution_limit_accumulates_across_loop_iterations(self) -> None:
        def increment(value: int) -> int:
            return value + 1

        def continue_loop(context: ExecutionContext) -> bool:
            return context.incoming["body"] < 3

        def exit_loop(context: ExecutionContext) -> bool:
            return context.incoming["body"] >= 3

        app = AutoAgentApp()
        value = Workflow(
            "node-limit",
            nodes=[
                Node("start", identity_int),
                Node(
                    "header",
                    identity_int,
                    policy=NodePolicy(
                        resource=ResourcePolicy(max_node_executions_per_invocation=1)
                    ),
                ),
                Node("body", increment),
                Node("finish", identity_int),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "body"),
                Edge("body", "header", continue_loop),
                Edge("body", "finish", exit_loop),
            ],
        )
        app.register_workflow(value)
        invocation = app.invoke(value, 0)
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertIn("execution limit", invocation.error.message)
        app.close()

    def test_operator_attempt_limit_counts_retry_and_fallback(self) -> None:
        def fail(value: str) -> str:
            raise ValueError(value)

        app = AutoAgentApp()
        value = Workflow(
            "attempt-limit",
            nodes=[
                Node(
                    "node",
                    fail,
                    fallback_operators=(fail,),
                    policy=NodePolicy(
                        retry=RetryPolicy(max_attempts=2),
                        resource=ResourcePolicy(max_operator_attempts_per_invocation=2),
                    ),
                )
            ],
        )
        app.register_workflow(value)
        invocation = app.invoke(value, "error")
        self.assertEqual(invocation.state, InvocationState.FAILED)
        self.assertIn("attempt limit", invocation.error.message)
        app.close()

    def test_fixed_backoff_delays_retry(self) -> None:
        calls = 0

        def flaky(value: int) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("retry")
            return value

        app = AutoAgentApp()
        value = Workflow(
            "fixed-backoff",
            nodes=[
                Node(
                    "node",
                    flaky,
                    policy=NodePolicy(
                        retry=RetryPolicy(
                            max_attempts=2,
                            backoff=BackoffPolicy(mode="fixed", initial_delay_ms=10),
                        )
                    ),
                )
            ],
        )
        app.register_workflow(value)
        started = time.perf_counter()
        self.assertEqual(app.invoke(value, 1).result(), {"node": 1})
        self.assertGreaterEqual(time.perf_counter() - started, 0.008)
        app.close()


if __name__ == "__main__":
    unittest.main()
