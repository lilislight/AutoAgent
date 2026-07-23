from __future__ import annotations

import asyncio
import threading
import time
import unittest
from unittest.mock import patch

from autoagent import AutoAgentApp
from autoagent.core.executor.node_executor import _retry_delay_seconds
from autoagent.core.workflow import (
    BackoffPolicy,
    CapabilityRef,
    NodePolicy,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    TimeoutPolicy,
    Workflow,
)


class BackoffPolicyTests(unittest.TestCase):
    def test_fixed_linear_and_exponential_delays(self) -> None:
        fixed = BackoffPolicy(
            mode="fixed",
            initial_delay_ms=100,
            multiplier=2,
        )
        linear = BackoffPolicy(
            mode="linear",
            initial_delay_ms=100,
            multiplier=2,
        )
        exponential = BackoffPolicy(
            mode="exponential",
            initial_delay_ms=100,
            multiplier=2,
        )

        self.assertEqual(
            [_retry_delay_seconds(fixed, index) for index in range(3)],
            [0.1, 0.1, 0.1],
        )
        self.assertEqual(
            [_retry_delay_seconds(linear, index) for index in range(3)],
            [0.1, 0.3, 0.5],
        )
        self.assertEqual(
            [_retry_delay_seconds(exponential, index) for index in range(3)],
            [0.1, 0.2, 0.4],
        )

    def test_max_delay_is_applied_before_jitter(self) -> None:
        policy = BackoffPolicy(
            mode="exponential",
            initial_delay_ms=100,
            multiplier=3,
            max_delay_ms=250,
            jitter="full",
        )

        with patch(
            "autoagent.core.executor.node_executor.random.uniform",
            return_value=125,
        ) as uniform:
            delay = _retry_delay_seconds(policy, retry_index=2)

        uniform.assert_called_once_with(0, 250)
        self.assertEqual(delay, 0.125)

    def test_equal_jitter_uses_upper_half_of_computed_delay(self) -> None:
        policy = BackoffPolicy(
            mode="fixed",
            initial_delay_ms=200,
            jitter="equal",
        )

        with patch(
            "autoagent.core.executor.node_executor.random.uniform",
            return_value=150,
        ) as uniform:
            delay = _retry_delay_seconds(policy, retry_index=0)

        uniform.assert_called_once_with(100, 200)
        self.assertEqual(delay, 0.15)


class ExecutorPolicyBoundaryTests(unittest.TestCase):
    def test_async_operator_timeout_is_retried_and_cancels_each_call(self) -> None:
        async def scenario() -> None:
            started = 0
            cancelled = 0

            async def slow() -> str:
                nonlocal started, cancelled
                started += 1
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled += 1
                    raise

            workflow = Workflow(id="async_timeout_retry")
            workflow.add_node(
                slow,
                node_id="slow",
                policy=NodePolicy(
                    retry=RetryPolicy(max_attempts=2),
                    timeout=TimeoutPolicy(timeout_ms=5),
                ),
            )

            invocation = await AutoAgentApp().ainvoke(workflow)
            calls = invocation.latest_node_execution("slow").operator_executions

            self.assertEqual(invocation.state, "failed")
            self.assertEqual(invocation.error.code, "OPERATOR_TIMEOUT")
            self.assertEqual(started, 2)
            self.assertEqual(cancelled, 2)
            self.assertEqual([call.reason for call in calls], ["normal", "retry"])
            self.assertTrue(
                all(call.error.code == "OPERATOR_TIMEOUT" for call in calls)
            )
            self.assertTrue(
                all(
                    call.error.detail["execution_may_continue"] is False
                    for call in calls
                )
            )

        asyncio.run(scenario())

    def test_sync_timeout_late_result_cannot_mutate_runtime(self) -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def slow() -> str:
            started.set()
            release.wait(timeout=1)
            finished.set()
            return "late"

        workflow = Workflow(id="sync_timeout_late_result")
        workflow.add_node(
            slow,
            node_id="slow",
            policy=NodePolicy(timeout=TimeoutPolicy(timeout_ms=5)),
        )
        app = AutoAgentApp()

        invocation = app.invoke(workflow, session_id="session")
        self.assertTrue(started.is_set())
        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "OPERATOR_TIMEOUT")
        self.assertTrue(invocation.error.detail["execution_may_continue"])

        release.set()
        self.assertTrue(finished.wait(timeout=1))
        time.sleep(0.01)
        stored = app.runtime_store.find_session(
            namespace=app.namespace,
            workflow_id=workflow.id,
            session_key="session",
        ).get_current_invocation()
        execution = stored.latest_node_execution("slow")

        self.assertEqual(stored.state, "failed")
        self.assertEqual(execution.state, "failed")
        self.assertIsNone(execution.output)
        self.assertEqual(len(execution.operator_executions), 1)
        self.assertIsNone(execution.operator_executions[0].output)
        self.assertFalse(stored.execution_mailbox.has_pending())

    def test_sync_operator_late_result_after_cancellation_is_discarded(self) -> None:
        async def scenario() -> None:
            started = threading.Event()
            release = threading.Event()
            finished = threading.Event()

            def slow() -> str:
                started.set()
                release.wait(timeout=1)
                finished.set()
                return "late"

            workflow = Workflow(id="sync_cancel_late_result")
            workflow.add_node(slow, node_id="slow")
            app = AutoAgentApp()
            task = asyncio.create_task(
                app.ainvoke(workflow, session_id="session")
            )
            while not started.is_set():
                await asyncio.sleep(0.001)

            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

            release.set()
            while not finished.is_set():
                await asyncio.sleep(0.001)
            await asyncio.sleep(0.01)
            stored = app.runtime_store.find_session(
                namespace=app.namespace,
                workflow_id=workflow.id,
                session_key="session",
            ).get_current_invocation()
            execution = stored.latest_node_execution("slow")

            self.assertEqual(stored.state, "cancelled")
            self.assertEqual(execution.state, "cancelled")
            self.assertIsNone(execution.output)
            self.assertEqual(execution.operator_executions, [])
            self.assertFalse(stored.execution_mailbox.has_pending())

        asyncio.run(scenario())

    def test_runtime_limit_accumulates_across_loop_executions(self) -> None:
        calls = 0

        def iterative() -> int:
            nonlocal calls
            calls += 1
            time.sleep(0.015)
            return calls

        workflow = Workflow(id="accumulated_runtime_limit")
        workflow.add_node(lambda: None, node_id="start", entry=True)
        workflow.add_node(
            iterative,
            node_id="loop",
            input_mapping=lambda _ctx: {},
            policy=NodePolicy(
                resource=ResourcePolicy(max_runtime_ms_per_invocation=20)
            ),
        )
        workflow.add_node(lambda value: value, node_id="done")
        workflow.add_edge("start", "loop")
        workflow.add_edge(
            "loop",
            "loop",
            condition=lambda ctx: ctx.source_output < 3,
        )
        workflow.add_edge(
            "loop",
            "done",
            condition=lambda ctx: ctx.source_output >= 3,
        )

        invocation = AutoAgentApp().invoke(workflow)
        executions = [
            execution
            for execution in invocation.node_executions
            if execution.node_id == "loop"
        ]

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "RESOURCE_LIMIT_EXCEEDED")
        self.assertEqual(invocation.error.detail["resource"], "runtime_ms")
        self.assertGreater(invocation.error.detail["actual"], 20)
        self.assertEqual(calls, 2)
        self.assertEqual(
            [execution.state for execution in executions],
            ["completed", "failed"],
        )

    def test_aggregation_failure_does_not_retry_or_fallback(self) -> None:
        primary_calls = 0
        fallback_calls = 0

        app = AutoAgentApp()

        @app.capability("sample", operator_id="primary")
        def primary() -> str:
            nonlocal primary_calls
            primary_calls += 1
            return "sample"

        @app.operator("fallback", capability="sample")
        def fallback() -> str:
            nonlocal fallback_calls
            fallback_calls += 1
            return "fallback"

        def broken_aggregator(_outputs: list[str]) -> str:
            raise ValueError("cannot aggregate")

        workflow = Workflow(id="aggregation_failure_boundary")
        workflow.add_node(
            CapabilityRef(id="sample"),
            node_id="sample",
            policy=NodePolicy(
                retry=RetryPolicy(max_attempts=3),
                replication=ReplicationPolicy(
                    count=2,
                    output_aggregator=broken_aggregator,
                ),
            ),
        )

        invocation = app.invoke(workflow)
        calls = invocation.latest_node_execution("sample").operator_executions

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "OUTPUT_AGGREGATION_FAILED")
        self.assertEqual(primary_calls, 2)
        self.assertEqual(fallback_calls, 0)
        self.assertEqual(1, len(calls))
        self.assertEqual("replication", calls[0].kind)
        self.assertEqual(("primary",), calls[0].operator_ids)
        self.assertEqual(2, calls[0].summary.call_count)
        self.assertEqual(2, calls[0].summary.attempt_count)


if __name__ == "__main__":
    unittest.main()
