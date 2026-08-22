from __future__ import annotations

import asyncio
import unittest
from collections.abc import AsyncIterator, Iterator
from typing_extensions import TypedDict

from autoagent.core import (
    AggregationContext,
    Backoff,
    InputMappingContext,
    Map,
    Node,
    NodeExecutor,
    Operator,
    OperatorPolicy,
    OperatorCallCompleted,
    OperatorCallFailed,
    OperatorCallStarted,
    RuntimeEvent,
    Retry,
    Stream,
    StreamContext,
    Workflow,
    WorkflowCompiler,
)

from tests.test_phase3_scheduler import Harness


class Value(TypedDict):
    value: int


class Total(TypedDict):
    total: int


class Chunk(TypedDict):
    value: int


class StreamState(TypedDict):
    total: int


def identity(value: Value) -> Value:
    return value


def map_inputs(_context: InputMappingContext) -> list[Value]:
    return []


def aggregate(context: AggregationContext) -> Total:
    return {"total": sum(item["value"] for item in context.outputs)}  # type: ignore[index]


def sync_stream(value: Value) -> Iterator[Chunk]:
    for number in range(value["value"]):
        yield {"value": number}


async def async_stream(value: Value) -> AsyncIterator[Chunk]:
    for number in range(value["value"]):
        await asyncio.sleep(0)
        yield {"value": number}


class SumReducer:
    def initial(self, _context: StreamContext) -> StreamState:
        return {"total": 0}

    def add(
        self, _context: StreamContext, state: StreamState, chunk: Chunk
    ) -> StreamState:
        return {"total": state["total"] + chunk["value"]}

    def finish(self, _context: StreamContext, state: StreamState) -> Total:
        return {"total": state["total"]}


class ExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_backoff_fallback_and_timeout_are_call_scoped(self) -> None:
        """Verify retry backoff fallback and timeout are call scoped."""
        attempts = 0

        async def flaky(value: Value) -> Value:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError("retry")
            return value

        events: list[object] = []

        async def record(payload: object) -> None:
            events.append(payload)

        retry_node = WorkflowCompiler().compile_or_raise(
            Workflow(
                "retry",
                nodes=[
                    Node(
                        "node",
                        flaky,
                        operator_policy=OperatorPolicy(
                            retry=Retry(3, Backoff("fixed", 0))
                        ),
                    )
                ],
            )
        ).node("node")
        retried = await self.executor.execute(
            retry_node,
            "node@root",
            {"value": 1},
            on_call_event=record,  # type: ignore[arg-type]
        )
        self.assertEqual(retried.metrics.call_count, 3)
        starts = [item for item in events if isinstance(item, OperatorCallStarted)]
        self.assertEqual([item.reason for item in starts], ["normal", "retry", "retry"])

        def primary(_value: Value) -> Value:
            raise RuntimeError("primary")

        def fallback(value: Value) -> Value:
            return value

        fallback_events: list[object] = []

        async def record_fallback(payload: object) -> None:
            fallback_events.append(payload)

        fallback_node = WorkflowCompiler().compile_or_raise(
            Workflow(
                "fallback",
                nodes=[
                    Node(
                        "node",
                        primary,
                        operator_policy=OperatorPolicy(
                            fallback=(Operator(fallback, id="fallback"),)
                        ),
                    )
                ],
            )
        ).node("node")
        recovered = await self.executor.execute(
            fallback_node,
            "node@root",
            {"value": 2},
            on_call_event=record_fallback,  # type: ignore[arg-type]
        )
        self.assertEqual(recovered.output, {"value": 2})
        fallback_starts = [
            item for item in fallback_events if isinstance(item, OperatorCallStarted)
        ]
        self.assertEqual([item.reason for item in fallback_starts], ["normal", "fallback"])

        async def slow(value: Value) -> Value:
            await asyncio.sleep(1)
            return value

        timeout_node = WorkflowCompiler().compile_or_raise(
            Workflow(
                "timeout",
                nodes=[
                    Node(
                        "node",
                        slow,
                        operator_policy=OperatorPolicy(
                            timeout_ms=5,
                            retry=Retry(2),
                        ),
                    )
                ],
            )
        ).node("node")
        timeout_events: list[object] = []

        async def record_timeout(payload: object) -> None:
            timeout_events.append(payload)

        with self.assertRaises(TimeoutError):
            await self.executor.execute(
                timeout_node,
                "node@root",
                {"value": 1},
                on_call_event=record_timeout,  # type: ignore[arg-type]
            )
        self.assertEqual(
            len([item for item in timeout_events if isinstance(item, OperatorCallFailed)]),
            2,
        )

    async def asyncSetUp(self) -> None:
        self.executor = NodeExecutor(max_operator_concurrency=8)

    async def asyncTearDown(self) -> None:
        self.executor.close()

    async def test_sync_operator_records_durable_call_lifecycle(self) -> None:
        """Verify sync operator records durable call lifecycle."""
        harness = Harness(Workflow("one", nodes=[Node("node", identity)]), entry="node")
        harness.start("node@root")
        events: list[object] = []

        async def record(payload: object) -> None:
            events.append(payload)
            harness.emit(payload)

        result = await self.executor.execute(
            harness.workflow.node("node"),
            "node@root",
            {"value": 3},
            on_call_event=record,  # type: ignore[arg-type]
        )
        self.assertEqual(result.output, {"value": 3})
        self.assertEqual(result.metrics.call_count, 1)
        self.assertIsInstance(events[0], OperatorCallStarted)
        self.assertIsInstance(events[1], OperatorCallCompleted)
        call = next(iter(harness.state.invocation.scheduler.operator_calls.values()))
        self.assertEqual(call.status, "completed")
        self.assertEqual(call.output, {"value": 3})

    async def test_map_honors_parallel_limit_and_preserves_input_order(self) -> None:
        """Verify map honors parallel limit and preserves input order."""
        active = 0
        peak = 0

        async def delayed(value: Value) -> Value:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep((5 - value["value"]) * 0.002)
            active -= 1
            return value

        workflow = Workflow(
            "map",
            nodes=[
                Node(
                    "map",
                    delayed,
                    input_mapping=map_inputs,
                    map=Map(max_parallelism=2),
                )
            ],
        )
        node = WorkflowCompiler().compile_or_raise(workflow).node("map")
        result = await self.executor.execute(
            node,
            "map@root",
            [{"value": number} for number in range(5)],
        )
        self.assertEqual(result.output, [{"value": number} for number in range(5)])
        self.assertEqual(peak, 2)
        self.assertEqual(result.metrics.peak_parallelism, 2)

    async def test_map_aggregation_and_empty_map(self) -> None:
        """Verify map aggregation and empty map."""
        node = WorkflowCompiler().compile_or_raise(
            Workflow(
                "aggregate",
                nodes=[
                    Node(
                        "map",
                        identity,
                        input_mapping=map_inputs,
                        map=Map(aggregate=aggregate, max_parallelism=3),
                    )
                ],
            )
        ).node("map")
        result = await self.executor.execute(
            node, "map@root", [{"value": 2}, {"value": 4}]
        )
        self.assertEqual(result.output, {"total": 6})
        empty = await self.executor.execute(node, "map@root", [])
        self.assertEqual(empty.output, {"total": 0})

    async def test_map_failure_cancels_and_settles_every_started_call(self) -> None:
        """Verify map failure cancels and settles every started call."""
        started = 0
        all_started = asyncio.Event()

        async def fail_one(value: Value) -> Value:
            nonlocal started
            started += 1
            if started == 3:
                all_started.set()
            await all_started.wait()
            if value["value"] == 0:
                raise RuntimeError("map failed")
            await asyncio.sleep(10)
            return value

        harness = Harness(
            Workflow(
                "map-failure",
                nodes=[
                    Node(
                        "map",
                        fail_one,
                        input_mapping=map_inputs,
                        map=Map(max_parallelism=3),
                    )
                ],
            ),
            entry="map",
        )
        harness.start("map@root")

        async def record(payload: object) -> None:
            harness.emit(payload)

        with self.assertRaisesRegex(RuntimeError, "map failed"):
            await self.executor.execute(
                harness.workflow.node("map"),
                "map@root",
                [{"value": number} for number in range(3)],
                on_call_event=record,  # type: ignore[arg-type]
            )
        calls = harness.state.invocation.scheduler.operator_calls.values()
        self.assertEqual(len(tuple(calls)), 3)
        self.assertFalse(any(call.status == "running" for call in calls))

    async def test_sync_and_async_stream_reduce_to_one_output(self) -> None:
        """Verify sync and async stream reduce to one output."""
        for handler in (sync_stream, async_stream):
            node = WorkflowCompiler().compile_or_raise(
                Workflow(
                    f"stream-{handler.__name__}",
                    nodes=[Node("stream", handler, stream=Stream(SumReducer()))],
                )
            ).node("stream")
            result = await self.executor.execute(
                node, "stream@root", {"value": 4}
            )
            self.assertEqual(result.output, {"total": 6})

    async def test_stream_source_is_closed_when_reduction_fails(self) -> None:
        """Verify stream source is closed when reduction fails."""
        closed = asyncio.Event()

        async def source(_value: Value) -> AsyncIterator[Chunk]:
            try:
                yield {"value": 1}
                await asyncio.sleep(10)
            finally:
                closed.set()

        class FailingReducer(SumReducer):
            def add(
                self, _context: StreamContext, _state: StreamState, _chunk: Chunk
            ) -> StreamState:
                raise RuntimeError("reducer failed")

        node = WorkflowCompiler().compile_or_raise(
            Workflow(
                "stream-close",
                nodes=[Node("stream", source, stream=Stream(FailingReducer()))],
            )
        ).node("stream")
        with self.assertRaisesRegex(RuntimeError, "reducer failed"):
            await self.executor.execute(node, "stream@root", {"value": 1})
        self.assertTrue(closed.is_set())

    async def test_stream_chunks_are_observable_without_entering_runtime_state(self) -> None:
        """Verify stream chunks are observable without entering runtime state."""
        node = WorkflowCompiler().compile_or_raise(
            Workflow(
                "stream-chunks",
                nodes=[Node("stream", sync_stream, stream=Stream(SumReducer()))],
            )
        ).node("stream")
        chunks: list[object] = []

        async def observe(chunk: object) -> None:
            chunks.append(chunk)

        result = await self.executor.execute(
            node,
            "stream@root",
            {"value": 3},
            on_stream_chunk=observe,
        )
        self.assertEqual(chunks, [{"value": 0}, {"value": 1}, {"value": 2}])
        self.assertEqual(result.output, {"total": 3})

    async def test_failure_emits_failed_call_and_does_not_complete_it(self) -> None:
        """Verify failure emits failed call and does not complete it."""
        def fail(_value: Value) -> Value:
            raise RuntimeError("broken")

        harness = Harness(Workflow("failure", nodes=[Node("node", fail)]), entry="node")
        harness.start("node@root")

        async def record(payload: object) -> None:
            harness.emit(payload)

        with self.assertRaisesRegex(RuntimeError, "broken"):
            await self.executor.execute(
                harness.workflow.node("node"),
                "node@root",
                {"value": 1},
                on_call_event=record,  # type: ignore[arg-type]
            )
        call = next(iter(harness.state.invocation.scheduler.operator_calls.values()))
        self.assertEqual(call.status, "failed")
        self.assertEqual(call.error.message, "broken")
        self.assertIsInstance(harness.journal.events("session")[-1].payload, OperatorCallFailed)

    async def test_runtime_event_round_trip_keeps_operator_call_payload(self) -> None:
        """Verify runtime event round trip keeps operator call payload."""
        payloads = (
            OperatorCallStarted("call", "node@root", "operator", 0, {"value": 1}),
            OperatorCallCompleted("call", {"value": 1}),
        )
        for sequence, payload in enumerate(payloads, 1):
            event = RuntimeEvent("session", sequence, payload, invocation_id="invocation")
            self.assertEqual(RuntimeEvent.from_record(event.to_record()), event)


if __name__ == "__main__":
    unittest.main()
