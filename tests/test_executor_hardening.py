from __future__ import annotations

import asyncio
import threading
import unittest
from collections.abc import Iterator

from pydantic import BaseModel, ConfigDict
from typing_extensions import TypedDict

from autoagent import (
    Node,
    Stream,
    StreamContext,
    Workflow,
    WorkflowCompiler,
)
from autoagent.core import (
    NodeExecutor,
    OperatorCallCompleted,
    OperatorCallFailed,
    ValueContract,
)


class Value(TypedDict):
    value: int


class Chunk(TypedDict):
    value: int


class StreamState(TypedDict):
    total: int


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int


class PermissiveModel(BaseModel):
    value: int


class NestedPermissiveModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    nested: PermissiveModel


def zero_input() -> Value:
    return {"value": 7}


class SumReducer:
    def initial(self, _context: StreamContext) -> StreamState:
        return {"total": 0}

    def add(
        self,
        _context: StreamContext,
        state: StreamState,
        chunk: Chunk,
    ) -> StreamState:
        return {"total": state["total"] + chunk["value"]}

    def finish(
        self, _context: StreamContext, state: StreamState
    ) -> StreamState:
        return state


class OperatorBoundaryTests(unittest.TestCase):
    def test_pydantic_contract_requires_forbid_extra_at_every_level(self) -> None:
        """Verify every Pydantic contract explicitly rejects unknown fields."""

        contract = ValueContract.create(StrictModel, location="strict")
        with self.assertRaises(TypeError):
            contract.validate({"value": 1, "unknown": 2})
        for annotation in (PermissiveModel, NestedPermissiveModel):
            with self.subTest(annotation=annotation):
                with self.assertRaisesRegex(TypeError, "extra='forbid'"):
                    ValueContract.create(annotation, location="model")


class NodeExecutorHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.executor = NodeExecutor(max_operator_concurrency=2)

    async def asyncTearDown(self) -> None:
        self.executor.close()

    async def test_zero_input_operator_is_called_without_a_positional_value(self) -> None:
        """Verify a zero-input Operator is invoked with no positional argument."""

        node = WorkflowCompiler().compile_or_raise(
            Workflow("zero-input", nodes=[Node("node", zero_input)])
        ).node("node")
        result = await self.executor.execute(node, "node@root", None)
        self.assertEqual(result.output, {"value": 7})

    async def test_completed_emit_failure_propagates_after_one_handler_call(self) -> None:
        """Verify a completed-call emit failure follows one successful call."""

        calls = 0

        def counted(value: Value) -> Value:
            nonlocal calls
            calls += 1
            return value

        async def fail_completed(payload: object) -> None:
            if isinstance(payload, OperatorCallCompleted):
                raise RuntimeError("runtime emit failed")

        node = WorkflowCompiler().compile_or_raise(
            Workflow(
                "emit-after-success",
                nodes=[Node("node", counted)],
            )
        ).node("node")
        with self.assertRaisesRegex(RuntimeError, "runtime emit failed"):
            await self.executor.execute(
                node,
                "node@root",
                {"value": 1},
                on_call_event=fail_completed,  # type: ignore[arg-type]
            )
        self.assertEqual(calls, 1)

    async def test_failed_emit_failure_replaces_the_operator_error(self) -> None:
        """Verify failed-call emission errors remain Runtime infrastructure errors."""

        calls = 0

        def failing(_value: Value) -> Value:
            nonlocal calls
            calls += 1
            raise ValueError("operator failed")

        async def fail_failed(payload: object) -> None:
            if isinstance(payload, OperatorCallFailed):
                raise RuntimeError("runtime emit failed")

        node = WorkflowCompiler().compile_or_raise(
            Workflow(
                "emit-after-failure",
                nodes=[Node("node", failing)],
            )
        ).node("node")
        with self.assertRaisesRegex(RuntimeError, "runtime emit failed"):
            await self.executor.execute(
                node,
                "node@root",
                {"value": 1},
                on_call_event=fail_failed,  # type: ignore[arg-type]
            )
        self.assertEqual(calls, 1)

    async def test_stream_emit_failure_is_not_an_operator_failure(self) -> None:
        """Verify a chunk emit error escapes without re-executing the Operator."""

        calls = 0

        def source(_value: Value) -> Iterator[Chunk]:
            nonlocal calls
            calls += 1
            yield {"value": 1}

        async def fail_chunk(_chunk: object) -> None:
            raise RuntimeError("runtime stream emit failed")

        events: list[object] = []

        async def observe_event(event: object) -> None:
            events.append(event)

        node = self._stream_node("emit-stream-failure", source, SumReducer())
        with self.assertRaisesRegex(RuntimeError, "runtime stream emit failed"):
            await self.executor.execute(
                node,
                "stream@root",
                {"value": 1},
                on_call_event=observe_event,  # type: ignore[arg-type]
                on_stream_chunk=fail_chunk,
            )
        self.assertEqual(calls, 1)
        self.assertFalse(
            any(isinstance(event, OperatorCallFailed) for event in events)
        )

    async def test_cancelled_sync_stream_next_keeps_operator_capacity(self) -> None:
        """Verify a blocked sync next call retains its physical Operator lease."""

        entered = threading.Event()
        release = threading.Event()

        def source(_value: Value) -> Iterator[Chunk]:
            entered.set()
            release.wait()
            yield {"value": 1}

        node = self._stream_node("blocked-next", source, SumReducer())
        await self._assert_cancelled_stage_keeps_capacity(node, entered, release)

    async def test_cancelled_sync_stream_reducer_keeps_operator_capacity(self) -> None:
        """Verify a blocked sync reducer retains its physical Operator lease."""

        entered = threading.Event()
        release = threading.Event()

        def source(_value: Value) -> Iterator[Chunk]:
            yield {"value": 1}

        class BlockingReducer(SumReducer):
            def add(
                self,
                context: StreamContext,
                state: StreamState,
                chunk: Chunk,
            ) -> StreamState:
                entered.set()
                release.wait()
                return super().add(context, state, chunk)

        node = self._stream_node("blocked-reducer", source, BlockingReducer())
        await self._assert_cancelled_stage_keeps_capacity(node, entered, release)

    async def test_cancelled_sync_stream_close_keeps_operator_capacity(self) -> None:
        """Verify a blocked sync close retains its physical Operator lease."""

        entered = threading.Event()
        release = threading.Event()

        class ClosingIterator(Iterator[Chunk]):
            def __init__(self) -> None:
                self._returned = False

            def __next__(self) -> Chunk:
                if self._returned:
                    raise StopIteration
                self._returned = True
                return {"value": 1}

            def close(self) -> None:
                entered.set()
                release.wait()

        def source(_value: Value) -> Iterator[Chunk]:
            return ClosingIterator()

        node = self._stream_node("blocked-close", source, SumReducer())
        await self._assert_cancelled_stage_keeps_capacity(node, entered, release)

    def _stream_node(self, workflow_id: str, source, reducer):
        return WorkflowCompiler().compile_or_raise(
            Workflow(
                workflow_id,
                nodes=[Node("stream", source, stream=Stream(reducer))],
            )
        ).node("stream")

    async def _assert_cancelled_stage_keeps_capacity(
        self,
        stream_node,
        entered: threading.Event,
        release: threading.Event,
    ) -> None:
        blocked = asyncio.create_task(
            self.executor.execute(stream_node, "stream@root", {"value": 1})
        )
        await self._wait_for_thread_event(entered)
        blocked.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(blocked, timeout=0.5)

        started = 0
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        finish = asyncio.Event()

        async def following(value: Value) -> Value:
            nonlocal started
            started += 1
            if started == 1:
                first_started.set()
            else:
                second_started.set()
            await finish.wait()
            return value

        following_node = WorkflowCompiler().compile_or_raise(
            Workflow("following", nodes=[Node("node", following)])
        ).node("node")
        followers = tuple(
            asyncio.create_task(
                self.executor.execute(
                    following_node,
                    f"following-{index}@root",
                    {"value": index},
                )
            )
            for index in range(2)
        )
        try:
            await asyncio.wait_for(first_started.wait(), timeout=0.5)
            await asyncio.sleep(0.03)
            self.assertEqual(started, 1)
            self.assertFalse(second_started.is_set())

            release.set()
            await asyncio.wait_for(second_started.wait(), timeout=0.5)
            finish.set()
            results = await asyncio.gather(*followers)
            self.assertEqual(
                [item.output for item in results],
                [{"value": 0}, {"value": 1}],
            )
        finally:
            release.set()
            finish.set()
            for task in followers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*followers, return_exceptions=True)

    async def _wait_for_thread_event(self, event: threading.Event) -> None:
        deadline = asyncio.get_running_loop().time() + 1
        while not event.is_set():
            if asyncio.get_running_loop().time() >= deadline:
                self.fail("Timed out waiting for synchronous Operator work.")
            await asyncio.sleep(0.005)


if __name__ == "__main__":
    unittest.main()
