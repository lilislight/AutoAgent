from __future__ import annotations

import asyncio
import threading
import time
import unittest
from collections.abc import AsyncIterator, Iterator
from unittest.mock import patch

from autoagent import (
    AutoAgentApp,
    StreamingResult,
    UserEventMapping,
    streaming_result,
)
from autoagent.core.compiler import workflow_revision_id
from autoagent.core.executor.node_executor import _retry_delay_seconds
from autoagent.core.workflow import (
    BackoffPolicy,
    CapabilityRef,
    MapPolicy,
    NodePolicy,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    TimeoutPolicy,
    Workflow,
)
from tests.helpers import started_app


def registered_revision_id(app: AutoAgentApp, workflow: Workflow) -> str:
    snapshot = app.register_workflow(workflow).workflow_snapshot
    return workflow_revision_id(
        snapshot.workflow_id,
        snapshot.definition_hash,
    )


class TextReducer:
    def __init__(self) -> None:
        self.parts: list[str] = []

    def add(self, chunk: str) -> None:
        self.parts.append(chunk)

    def finish(self) -> str:
        return "".join(self.parts)


class FailingReducer(TextReducer):
    def add(self, chunk: str) -> None:
        raise ValueError(f"cannot reduce {chunk}")


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
    def test_stream_and_output_user_events_are_mode_independent(self) -> None:
        def chunks():
            yield "hello"
            yield " world"

        def stream() -> StreamingResult[str, str]:
            return streaming_result(chunks(), reducer=TextReducer())

        workflow = Workflow(id="stream_user_events")
        workflow.add_node(
            stream,
            node_id="stream",
            stream_user_event_mapping=UserEventMapping(
                type="text_delta",
                transform=lambda chunk: {"delta": chunk},
            ),
            user_event_mapping=UserEventMapping(
                type="answer_completed",
                transform=lambda output: {"answer": output},
            ),
        )

        app = started_app()
        invocation = app.invoke(workflow, event_mode="minimal")
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(
            [event.type for event in events],
            ["text_delta", "text_delta", "answer_completed"],
        )
        self.assertEqual([event.sequence for event in events], [1, 2, 3])
        self.assertEqual(events[0].data, {"delta": "hello"})
        self.assertEqual(events[-1].data, {"answer": "hello world"})
        self.assertEqual(events[0].node_execution_id, events[-1].node_execution_id)
        self.assertIsNotNone(events[0].operator_call_id)
        self.assertEqual(
            events[0].operator_call_id,
            events[-1].operator_call_id,
        )
        self.assertEqual(app.runtime_store.runtime_events[invocation.id], [])

    def test_stream_user_event_transport_batches_without_changing_events(
        self,
    ) -> None:
        def stream() -> StreamingResult[str, str]:
            return streaming_result(
                (str(index) for index in range(70)),
                reducer=TextReducer(),
            )

        workflow = Workflow(id="batched_stream_user_events")
        workflow.add_node(
            stream,
            node_id="stream",
            stream_user_event_mapping=UserEventMapping(
                type="text_delta",
                transform=lambda chunk: {"delta": chunk},
            ),
        )
        app = started_app()
        original = app.runtime_store._record_user_events
        try:
            with patch.object(
                app.runtime_store,
                "_record_user_events",
                wraps=original,
            ) as record_batch:
                invocation = app.invoke(workflow, event_mode="minimal")
            events = app.runtime_store.list_user_events(
                invocation_id=invocation.id,
                limit=100,
            )
        finally:
            app.close()

        batch_sizes = [
            len(call.kwargs["specs"])
            for call in record_batch.call_args_list
        ]
        self.assertEqual(invocation.state, "completed")
        self.assertEqual(len(events), 70)
        self.assertEqual([event.sequence for event in events], list(range(1, 71)))
        self.assertEqual(
            [event.data["delta"] for event in events],
            [str(index) for index in range(70)],
        )
        self.assertEqual(sum(batch_sizes), 70)
        self.assertLess(len(batch_sizes), 70)
        self.assertLessEqual(max(batch_sizes), 32)

    def test_partial_user_event_batch_flushes_while_stream_is_running(
        self,
    ) -> None:
        first_chunk = threading.Event()
        release_stream = threading.Event()
        result: list[object] = []

        def chunks():
            first_chunk.set()
            yield "first"
            release_stream.wait(timeout=1)
            yield "second"

        def stream() -> StreamingResult[str, str]:
            return streaming_result(chunks(), reducer=TextReducer())

        workflow = Workflow(id="timed_user_event_batch")
        workflow.add_node(
            stream,
            node_id="stream",
            stream_user_event_mapping=UserEventMapping(
                type="text_delta",
                transform=lambda chunk: {"delta": chunk},
            ),
        )
        app = started_app()
        execution = threading.Thread(
            target=lambda: result.append(
                app.invoke(workflow, event_mode="minimal")
            )
        )
        execution.start()
        try:
            self.assertTrue(first_chunk.wait(timeout=1))
            deadline = time.monotonic() + 1
            visible = ()
            while time.monotonic() < deadline:
                invocations = tuple(app.runtime_store.invocations.values())
                if invocations:
                    visible = app.runtime_store.list_user_events(
                        invocation_id=invocations[0].id,
                    )
                    if visible:
                        break
                time.sleep(0.005)
            self.assertEqual(
                [event.data for event in visible],
                [{"delta": "first"}],
            )
            self.assertTrue(execution.is_alive())
        finally:
            release_stream.set()
            execution.join(timeout=2)
            app.close()

        self.assertFalse(execution.is_alive())
        self.assertEqual(result[0].state, "completed")

    def test_user_event_mapping_failure_does_not_fail_node(self) -> None:
        def complete() -> str:
            return "business output"

        def fail_mapping(value: str) -> dict[str, str]:
            raise ValueError(f"cannot map {value}")

        workflow = Workflow(id="failed_user_event_mapping")
        workflow.add_node(
            complete,
            node_id="complete",
            user_event_mapping=UserEventMapping(
                type="business_completed",
                transform=fail_mapping,
            ),
        )

        app = started_app()
        invocation = app.invoke(workflow)
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": "business output"})
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "user_event_mapping_failed")
        self.assertEqual(events[0].data["mapping_type"], "business_completed")
        self.assertEqual(events[0].data["source"], "output")

    def test_user_event_serialization_failure_does_not_fail_node(self) -> None:
        def complete() -> str:
            return "business output"

        workflow = Workflow(id="unserializable_user_event")
        workflow.add_node(
            complete,
            node_id="complete",
            user_event_mapping=UserEventMapping(
                type="business_completed",
                transform=lambda output: {"raw": b"not-json"},
            ),
        )

        app = started_app()
        invocation = app.invoke(workflow)
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(events[0].type, "user_event_mapping_failed")
        self.assertEqual(events[0].data["source"], "serialization")

    def test_batched_serialization_failure_preserves_valid_sibling_order(
        self,
    ) -> None:
        def stream() -> StreamingResult[str, str]:
            return streaming_result(
                iter(("valid", "invalid", "after")),
                reducer=TextReducer(),
            )

        workflow = Workflow(id="batched_serialization_failure")
        workflow.add_node(
            stream,
            node_id="stream",
            stream_user_event_mapping=UserEventMapping(
                type="text_delta",
                transform=lambda chunk: {
                    "delta": b"not-json" if chunk == "invalid" else chunk
                },
            ),
        )
        app = started_app()
        try:
            invocation = app.invoke(workflow, event_mode="minimal")
            events = app.runtime_store.list_user_events(
                invocation_id=invocation.id,
            )
        finally:
            app.close()

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(
            [event.type for event in events],
            ["text_delta", "user_event_mapping_failed", "text_delta"],
        )
        self.assertEqual([event.sequence for event in events], [1, 2, 3])
        self.assertEqual(events[0].data, {"delta": "valid"})
        self.assertEqual(events[1].data["source"], "serialization")
        self.assertEqual(events[2].data, {"delta": "after"})

    def test_message_stream_abort_is_emitted_without_persisting_chunks(
        self,
    ) -> None:
        async def chunks():
            yield "partial"
            raise RuntimeError("provider disconnected")

        async def stream() -> StreamingResult[str, str]:
            return streaming_result(chunks(), reducer=TextReducer())

        workflow = Workflow(id="aborted_message_stream")
        workflow.add_node(
            stream,
            node_id="stream",
            metadata={"_autoagent_user_event_stream": "message"},
            stream_user_event_mapping=UserEventMapping(
                type="message_delta",
                transform=lambda chunk: {"delta": chunk},
            ),
        )

        app = started_app()
        invocation = app.invoke(workflow, event_mode="minimal")
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(
            [event.type for event in events],
            ["message_delta", "message_aborted", "agent_failed"],
        )
        self.assertEqual(
            events[-2].data["error_type"],
            "RuntimeError",
        )

    def test_output_user_event_requires_successful_output_binding(self) -> None:
        def complete() -> str:
            return "output"

        def fail_binding(ctx) -> None:
            raise ValueError("binding failed")

        workflow = Workflow(id="binding_before_user_event")
        workflow.add_node(
            complete,
            node_id="complete",
            output_binding=fail_binding,
            user_event_mapping=UserEventMapping(
                type="business_completed",
                transform=lambda output: {"output": output},
            ),
        )

        app = started_app()
        invocation = app.invoke(workflow)

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(
            app.runtime_store.list_user_events(invocation_id=invocation.id),
            (),
        )

    def test_sync_streaming_result_reduces_to_normal_node_output(self) -> None:
        def chunks():
            yield "hello"
            yield " "
            yield "world"

        def stream() -> StreamingResult[str, str]:
            return streaming_result(chunks(), reducer=TextReducer())

        workflow = Workflow(id="sync_stream")
        workflow.add_node(stream, node_id="stream")

        app = started_app()
        invocation = app.invoke(workflow, event_mode="full")
        execution = invocation.latest_node_execution("stream")
        call = execution.operator_executions[0]
        operator_event = next(
            event
            for event in app.runtime_store.runtime_events[invocation.id]
            if event.event_name == "operator_call.completed"
        )

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": "hello world"})
        self.assertTrue(call.streaming)
        self.assertEqual(call.stream_chunk_count, 3)
        self.assertGreater(call.resource_usage.stream_consumption_ns, 0)
        self.assertGreater(call.resource_usage.stream_reduction_ns, 0)
        self.assertEqual(operator_event.output, "hello world")
        self.assertEqual(operator_event.payload["stream_chunk_count"], 3)
        self.assertNotIn("chunks", operator_event.payload)
        self.assertGreater(operator_event.timing["stream_consumption_ns"], 0)
        self.assertGreater(operator_event.timing["stream_reduction_ns"], 0)

    def test_async_streaming_result_reduces_to_normal_node_output(self) -> None:
        async def scenario() -> None:
            async def chunks():
                yield "async"
                await asyncio.sleep(0)
                yield " stream"

            async def stream() -> StreamingResult[str, str]:
                return streaming_result(chunks(), reducer=TextReducer())

            workflow = Workflow(id="async_stream")
            workflow.add_node(stream, node_id="stream")

            invocation = await started_app().ainvoke(workflow)
            call = invocation.latest_node_execution(
                "stream"
            ).operator_executions[0]

            self.assertEqual(invocation.state, "completed")
            self.assertEqual(
                invocation.result,
                {"output": "async stream"},
            )
            self.assertTrue(call.streaming)
            self.assertEqual(call.stream_chunk_count, 2)

        asyncio.run(scenario())

    def test_raw_sync_generator_contract_is_rejected(self) -> None:
        def stream() -> Iterator[str]:
            yield "not wrapped"

        workflow = Workflow(id="raw_sync_stream")
        workflow.add_node(stream, node_id="stream")

        with self.assertRaisesRegex(ValueError, "non-serializable"):
            started_app().invoke(workflow)

    def test_raw_async_generator_contract_is_rejected(self) -> None:
        async def scenario() -> None:
            async def stream() -> AsyncIterator[str]:
                yield "not wrapped"

            workflow = Workflow(id="raw_async_stream")
            workflow.add_node(stream, node_id="stream")

            with self.assertRaisesRegex(ValueError, "non-serializable"):
                await started_app().ainvoke(workflow)

        asyncio.run(scenario())

    def test_stream_reducer_failure_uses_specific_error(self) -> None:
        def stream() -> StreamingResult[str, str]:
            return streaming_result(["broken"], reducer=FailingReducer())

        workflow = Workflow(id="stream_reducer_failure")
        workflow.add_node(stream, node_id="stream")

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "STREAM_REDUCTION_FAILED")

    def test_stream_failure_is_retried_with_a_new_source(self) -> None:
        calls = 0

        def stream() -> StreamingResult[str, str]:
            nonlocal calls
            calls += 1
            current = calls

            def chunks():
                yield f"attempt-{current}"
                if current == 1:
                    raise ValueError("stream disconnected")

            return streaming_result(chunks(), reducer=TextReducer())

        workflow = Workflow(id="stream_retry")
        workflow.add_node(
            stream,
            node_id="stream",
            policy=NodePolicy(retry=RetryPolicy(max_attempts=2)),
        )

        invocation = started_app().invoke(workflow)
        attempts = invocation.latest_node_execution(
            "stream"
        ).operator_executions

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": "attempt-2"})
        self.assertEqual(calls, 2)
        self.assertEqual([call.reason for call in attempts], ["normal", "retry"])
        self.assertEqual(
            attempts[0].error.code,
            "STREAM_CONSUMPTION_FAILED",
        )
        self.assertTrue(attempts[0].streaming)
        self.assertEqual(attempts[0].stream_chunk_count, 1)
        self.assertTrue(attempts[1].streaming)

    def test_stream_failure_uses_fallback_operator(self) -> None:
        app = started_app()

        @app.capability("stream_text", operator_id="primary")
        def primary() -> StreamingResult[str, str]:
            def chunks():
                yield "partial"
                raise ValueError("primary stream failed")

            return streaming_result(chunks(), reducer=TextReducer())

        @app.operator("fallback", capability="stream_text")
        def fallback() -> StreamingResult[str, str]:
            return streaming_result(
                iter(("fallback", " result")),
                reducer=TextReducer(),
            )

        workflow = Workflow(id="stream_fallback")
        workflow.add_node(
            CapabilityRef(id="stream_text"),
            node_id="stream",
        )

        invocation = app.invoke(workflow)
        attempts = invocation.latest_node_execution(
            "stream"
        ).operator_executions

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(
            invocation.result,
            {"output": "fallback result"},
        )
        self.assertEqual(
            [attempt.reason for attempt in attempts],
            ["normal", "fallback"],
        )
        self.assertEqual(
            attempts[0].error.code,
            "STREAM_CONSUMPTION_FAILED",
        )
        self.assertTrue(attempts[1].streaming)

    def test_async_stream_timeout_closes_the_source(self) -> None:
        async def scenario() -> None:
            closed = asyncio.Event()

            async def chunks():
                try:
                    yield "first"
                    await asyncio.Event().wait()
                finally:
                    closed.set()

            async def stream() -> StreamingResult[str, str]:
                return streaming_result(chunks(), reducer=TextReducer())

            workflow = Workflow(id="async_stream_timeout")
            workflow.add_node(
                stream,
                node_id="stream",
                policy=NodePolicy(timeout=TimeoutPolicy(timeout_ms=5)),
            )

            invocation = await started_app().ainvoke(workflow)

            self.assertEqual(invocation.state, "failed")
            self.assertEqual(invocation.error.code, "OPERATOR_TIMEOUT")
            self.assertTrue(closed.is_set())
            attempt = invocation.latest_node_execution(
                "stream"
            ).operator_executions[0]
            self.assertTrue(attempt.streaming)
            self.assertEqual(attempt.stream_chunk_count, 1)

        asyncio.run(scenario())

    def test_invocation_cancel_closes_async_stream(self) -> None:
        async def scenario() -> None:
            started = asyncio.Event()
            closed = asyncio.Event()

            async def chunks():
                try:
                    started.set()
                    yield "first"
                    await asyncio.Event().wait()
                finally:
                    closed.set()

            async def stream() -> StreamingResult[str, str]:
                return streaming_result(chunks(), reducer=TextReducer())

            workflow = Workflow(id="cancel_async_stream")
            workflow.add_node(
                stream,
                node_id="stream",
                metadata={"_autoagent_user_event_stream": "message"},
                stream_user_event_mapping=UserEventMapping(
                    type="message_delta",
                    transform=lambda chunk: {"delta": chunk},
                ),
            )
            app = started_app()
            task = asyncio.create_task(app.ainvoke(workflow))
            await started.wait()

            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

            self.assertTrue(closed.is_set())
            stored = next(iter(app.runtime_store.invocations.values()))
            self.assertEqual(stored.state, "cancelled")
            self.assertEqual(
                [
                    event.type
                    for event in app.runtime_store.list_user_events(
                        invocation_id=stored.id,
                    )
                ],
                ["message_delta", "message_aborted"],
            )

        asyncio.run(scenario())

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

            invocation = await started_app().ainvoke(workflow)
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
        app = started_app()

        invocation = app.invoke(workflow, session_id="session")
        self.assertTrue(started.is_set())
        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "OPERATOR_TIMEOUT")
        self.assertTrue(invocation.error.detail["execution_may_continue"])

        release.set()
        self.assertTrue(finished.wait(timeout=1))
        time.sleep(0.01)
        stored = app.runtime_store.find_session(
            workflow_revision_id=invocation.workflow_revision_id,
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
            app = started_app()
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
                workflow_revision_id=registered_revision_id(app, workflow),
                session_key="session",
            ).get_current_invocation()
            execution = stored.latest_node_execution("slow")

            self.assertEqual(stored.state, "cancelled")
            self.assertEqual(execution.state, "cancelled")
            self.assertIsNone(execution.output)
            self.assertEqual(execution.operator_executions, [])
            self.assertFalse(stored.execution_mailbox.has_pending())

        asyncio.run(scenario())

    def test_replication_consumes_each_stream_before_aggregation(self) -> None:
        def stream() -> StreamingResult[str, str]:
            return streaming_result(
                iter(("a", "b")),
                reducer=TextReducer(),
            )

        workflow = Workflow(id="replicated_stream")
        workflow.add_node(
            stream,
            node_id="stream",
            policy=NodePolicy(
                replication=ReplicationPolicy(count=3),
            ),
        )

        invocation = started_app().invoke(workflow)
        execution = invocation.latest_node_execution("stream")
        parallel_call = execution.operator_executions[0]
        summary = parallel_call.summary

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(
            invocation.result,
            {"output": ["ab", "ab", "ab"]},
        )
        self.assertEqual(parallel_call.kind, "replication")
        self.assertEqual(summary.streaming_call_count, 3)
        self.assertEqual(summary.stream_chunk_count, 6)
        self.assertGreater(summary.stream_consumption_ns, 0)
        self.assertGreater(summary.stream_reduction_ns, 0)

    def test_map_consumes_each_stream_before_aggregation(self) -> None:
        def source() -> list[int]:
            return [1, 2, 3]

        def stream(value: int) -> StreamingResult[str, str]:
            return streaming_result(
                iter((str(value), "!")),
                reducer=TextReducer(),
            )

        workflow = Workflow(id="mapped_stream")
        workflow.add_node(
            source,
            node_id="source",
        )
        workflow.add_node(
            stream,
            node_id="stream",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": value} for value in ctx.input
                    ],
                    max_parallelism=2,
                )
            ),
        )
        workflow.add_edge(
            "source",
            "stream",
        )

        invocation = started_app().invoke(workflow)
        execution = invocation.latest_node_execution("stream")
        parallel_call = execution.operator_executions[0]
        summary = parallel_call.summary

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(
            invocation.result,
            {"output": ["1!", "2!", "3!"]},
        )
        self.assertEqual(parallel_call.kind, "map")
        self.assertEqual(summary.streaming_call_count, 3)
        self.assertEqual(summary.stream_chunk_count, 6)
        self.assertLessEqual(summary.peak_parallelism, 2)

    def test_runtime_limit_accumulates_across_loop_executions(self) -> None:
        calls = 0

        def start() -> None:
            return None

        def done(value: int) -> int:
            return value

        def iterative() -> int:
            nonlocal calls
            calls += 1
            time.sleep(0.015)
            return calls

        workflow = Workflow(id="accumulated_runtime_limit")
        workflow.add_node(start, node_id="start", entry=True)
        workflow.add_node(
            iterative,
            node_id="loop",
            input_mapping=lambda _ctx: {},
            policy=NodePolicy(
                resource=ResourcePolicy(max_runtime_ms_per_invocation=25)
            ),
        )
        workflow.add_node(done, node_id="done")
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

        invocation = started_app().invoke(workflow)
        executions = [
            execution
            for execution in invocation.node_executions
            if execution.node_id == "loop"
        ]

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "RESOURCE_LIMIT_EXCEEDED")
        self.assertEqual(invocation.error.detail["resource"], "runtime_ms")
        self.assertGreater(invocation.error.detail["actual"], 25)
        self.assertEqual(calls, 2)
        self.assertEqual(
            [execution.state for execution in executions],
            ["completed", "failed"],
        )

    def test_aggregation_failure_does_not_retry_or_fallback(self) -> None:
        primary_calls = 0
        fallback_calls = 0

        app = started_app()

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

        def broken_aggregator(_ctx) -> str:
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
