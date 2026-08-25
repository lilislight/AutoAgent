"""Transient execution of one NodeOccurrence.

Python Tasks, generators and thread-pool work remain in this module. Durable
progress is exposed only as semantic OperatorCall payloads.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterator
from concurrent.futures import Executor, Future
from uuid import uuid4

from ..operators import Operator, ValueContract, is_stream_value
from ..runtime.events import (
    OperatorCallCompleted,
    OperatorCallFailed,
    OperatorCallStarted,
    RuntimeErrorInfo,
)
from ..runtime.values import freeze
from ..workflow import (
    AggregationContext,
    Capability,
    ConditionContext,
    ContextPatch,
    EdgeIR,
    ErrorInfo,
    InputMappingContext,
    NodeIR,
    OutputBindingContext,
    StreamContext,
    WorkflowIR,
)
from .future import await_concurrent_future
from .result import ExecutionMetrics, NodeExecutionResult


CallEvent = OperatorCallStarted | OperatorCallCompleted | OperatorCallFailed
CallEventHandler = Callable[[CallEvent], Awaitable[None]]
StreamChunkHandler = Callable[[object], Awaitable[None]]


class UserCallableCancelledError(RuntimeError):
    """A user callable raised CancelledError without Task cancellation."""


async def _ignore_event(_event: CallEvent) -> None:
    return None


async def _ignore_chunk(_chunk: object) -> None:
    return None


class NodeExecutor:
    """Execute Operator, Map and Stream semantics without owning Runtime State."""

    def __init__(self, *, max_operator_concurrency: int = 32) -> None:
        if (
            not isinstance(max_operator_concurrency, int)
            or isinstance(max_operator_concurrency, bool)
            or max_operator_concurrency < 1
        ):
            raise ValueError("max_operator_concurrency must be positive.")
        self._max_operator_concurrency = max_operator_concurrency
        self._operator_capacity = asyncio.Semaphore(max_operator_concurrency)
        self._pool = _BurstThreadPool(max_operator_concurrency)
        self._settlement_tasks: set[asyncio.Task[None]] = set()
        self._closed = False

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            # Python cannot terminate a non-cooperative synchronous handler.
            # Workers are daemon threads, so App shutdown cancels queued work
            # and does not wait forever for an external blocking call.
            self._pool.shutdown(wait=False, cancel_futures=True)

    @property
    def max_operator_concurrency(self) -> int:
        return self._max_operator_concurrency

    async def call_hook(
        self, handler: Callable[..., object], *args: object
    ) -> object:
        if self._closed:
            raise RuntimeError("NodeExecutor is closed.")
        return await _invoke(self._pool, handler, *args)

    async def map_input(
        self,
        node: NodeIR,
        *,
        invocation_input: object,
        incoming: dict[str, object],
        invocation_context: object,
        session_context: object,
    ) -> object:
        if node.input_mapping is None:
            if not incoming:
                value = invocation_input
            elif len(incoming) == 1:
                value = next(iter(incoming.values()))
            else:
                raise RuntimeError("Multi-input Node requires Input Mapping.")
        else:
            value = await _invoke(
                self._pool,
                node.input_mapping,
                InputMappingContext(
                    invocation_context=_mapping(invocation_context),
                    session_context=_mapping(session_context),
                    invocation_input=invocation_input,
                    incoming=incoming,
                ),
            )
        if node.map is None and node.input_contract is not None:
            value = (
                node.input_contract.restore(value)
                if node.input_mapping is None
                else node.input_contract.validate(value)
            )
        return value

    async def bind_output(
        self,
        node: NodeIR,
        output: object,
        *,
        invocation_context: object,
        session_context: object,
    ) -> ContextPatch:
        if node.output_binding is None:
            return ContextPatch()
        patch = await _invoke(
            self._pool,
            node.output_binding,
            OutputBindingContext(
                invocation_context=_mapping(invocation_context),
                session_context=_mapping(session_context),
                output=freeze(output),
            ),
        )
        if patch is None:
            return ContextPatch()
        if not isinstance(patch, ContextPatch):
            raise TypeError("Output Binding must return ContextPatch or None.")
        return patch

    async def select_edges(
        self,
        edges: tuple[EdgeIR, ...],
        *,
        source_status: str,
        source_node_id: str,
        output: object | None,
        error: ErrorInfo | None,
        invocation_context: object,
        session_context: object,
    ) -> set[str]:
        selected: set[str] = set()
        context = ConditionContext(
            invocation_context=_mapping(invocation_context),
            session_context=_mapping(session_context),
            source_node_id=source_node_id,
            output=freeze(output),
            error=error,
        )
        for edge in edges:
            if edge.on != source_status:
                continue
            if edge.condition is None:
                selected.add(edge.id)
                continue
            decision = await _invoke(self._pool, edge.condition, context)
            if type(decision) is not bool:
                raise TypeError("Edge Condition must return bool.")
            if decision:
                selected.add(edge.id)
        return selected

    async def execute(
        self,
        node: NodeIR,
        occurrence_id: str,
        value: object,
        *,
        invocation_context: object = None,
        session_context: object = None,
        on_call_event: CallEventHandler = _ignore_event,
        on_stream_chunk: StreamChunkHandler = _ignore_chunk,
    ) -> NodeExecutionResult:
        if self._closed:
            raise RuntimeError("NodeExecutor is closed.")
        if isinstance(node.executable, (Capability, WorkflowIR)):
            raise TypeError(
                f"Node {node.id!r} requires a dispatcher owned by AutoAgentApp."
            )
        if not isinstance(node.executable, Operator):
            raise TypeError(f"Node {node.id!r} is not directly executable.")

        started = time.perf_counter_ns()
        peak = 0
        active = 0
        lock = asyncio.Lock()
        async def run_unit(index: int, item: object) -> tuple[object, int]:
            nonlocal active, peak
            async with lock:
                active += 1
                peak = max(peak, active)
            try:
                return await self._call(
                    node,
                    occurrence_id,
                    index,
                    item,
                    invocation_context,
                    session_context,
                    on_call_event,
                    on_stream_chunk,
                )
            finally:
                async with lock:
                    active -= 1

        if node.map is None:
            unit_results = [await run_unit(0, value)]
        else:
            if not isinstance(value, list):
                raise TypeError("Map Node input must be a list of Operator inputs.")
            limit = min(
                len(value) or 1,
                node.map.max_parallelism or self._max_operator_concurrency,
                self._max_operator_concurrency,
            )
            unit_results: list[tuple[object, int] | None] = [None] * len(value)
            next_index = 0
            index_lock = asyncio.Lock()

            async def worker() -> None:
                nonlocal next_index
                while True:
                    async with index_lock:
                        if next_index >= len(value):
                            return
                        index = next_index
                        next_index += 1
                    unit_results[index] = await run_unit(index, value[index])

            tasks = tuple(asyncio.create_task(worker()) for _ in range(limit))
            try:
                await asyncio.gather(*tasks)
            except BaseException:
                # A Map is one NodeOccurrence. It cannot become terminal while
                # physical calls from the same occurrence are still live.
                for task in tasks:
                    if not task.done():
                        task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                raise
            unit_results = [item for item in unit_results if item is not None]

        outputs = [item[0] for item in unit_results]
        call_count = sum(item[1] for item in unit_results)
        output: object
        if node.map is None:
            output = outputs[0]
        elif node.map.aggregate is None:
            output = outputs
        else:
            context = AggregationContext(
                invocation_context=_mapping(invocation_context),
                session_context=_mapping(session_context),
                inputs=tuple(value),  # type: ignore[arg-type]
                outputs=tuple(outputs),
            )
            output = await _invoke(self._pool, node.map.aggregate, context)
        return NodeExecutionResult(
            output,
            ExecutionMetrics(
                duration_ns=max(0, time.perf_counter_ns() - started),
                call_count=call_count,
                peak_parallelism=peak,
            ),
        )

    async def _call(
        self,
        node: NodeIR,
        occurrence_id: str,
        unit_index: int,
        value: object,
        invocation_context: object,
        session_context: object,
        on_call_event: CallEventHandler,
        on_stream_chunk: StreamChunkHandler,
    ) -> tuple[object, int]:
        operator = node.executable
        assert isinstance(operator, Operator)
        output = await self._call_once(
            node,
            operator,
            occurrence_id,
            unit_index,
            value,
            invocation_context,
            session_context,
            on_call_event,
            on_stream_chunk,
        )
        return output, 1

    async def _call_once(
        self,
        node: NodeIR,
        operator: Operator,
        occurrence_id: str,
        unit_index: int,
        value: object,
        invocation_context: object,
        session_context: object,
        on_call_event: CallEventHandler,
        on_stream_chunk: StreamChunkHandler,
    ) -> object:
        await self._operator_capacity.acquire()
        lease = _OperatorCapacityLease(
            self._pool,
            self._operator_capacity,
            self._settlement_tasks,
        )

        try:
            return await self._call_once_admitted(
                node,
                operator,
                occurrence_id,
                unit_index,
                value,
                invocation_context,
                session_context,
                on_call_event,
                on_stream_chunk,
                lease,
            )
        finally:
            lease.close()

    async def _call_once_admitted(
        self,
        node: NodeIR,
        operator: Operator,
        occurrence_id: str,
        unit_index: int,
        value: object,
        invocation_context: object,
        session_context: object,
        on_call_event: CallEventHandler,
        on_stream_chunk: StreamChunkHandler,
        lease: "_OperatorCapacityLease",
    ) -> object:
        validated = (
            operator.contract.input.validate(value)
            if operator.contract.accepts_input
            else None
        )
        call_id = str(uuid4())
        await on_call_event(
            OperatorCallStarted(
                call_id,
                occurrence_id,
                operator.id,
                unit_index,
                (
                    operator.contract.input.to_record(validated)
                    if operator.contract.accepts_input
                    else None
                ),
            )
        )

        async def invoke_and_validate() -> tuple[object, ValueContract]:
            returned = await _invoke_handler(
                self._pool, operator, validated, lease.defer
            )
            if node.stream is not None:
                returned = await self._reduce_stream(
                    node,
                    operator,
                    returned,
                    value,
                    invocation_context,
                    session_context,
                    on_stream_chunk,
                    lease,
                )
            elif is_stream_value(returned):
                raise TypeError("Streaming Operator requires Node.stream.")
            contract = (
                node.output_contract
                if node.stream is not None
                else operator.contract.output
            )
            if contract is None:
                raise TypeError("Operator output contract is unavailable.")
            return contract.validate(returned), contract

        try:
            output, contract = await invoke_and_validate()
        except _RuntimeEmitFailure as failure:
            raise failure.error from failure
        except UserCallableCancelledError:
            error = RuntimeErrorInfo(
                "CancelledError", "User callable raised asyncio.CancelledError."
            )
            await on_call_event(OperatorCallFailed(call_id, error))
            raise
        except asyncio.CancelledError as cancelled:
            error = RuntimeErrorInfo("CancelledError", "Operator Call was cancelled.")
            if _task_is_cancelling():
                await asyncio.shield(on_call_event(OperatorCallFailed(call_id, error)))
                raise
            user_error = UserCallableCancelledError(
                "User callable raised asyncio.CancelledError."
            )
            await on_call_event(OperatorCallFailed(call_id, error))
            raise user_error from cancelled
        except BaseException as exc:
            error = RuntimeErrorInfo(type(exc).__name__, str(exc) or type(exc).__name__)
            await on_call_event(OperatorCallFailed(call_id, error))
            raise
        await on_call_event(OperatorCallCompleted(call_id, contract.to_record(output)))
        return output

    async def _reduce_stream(
        self,
        node: NodeIR,
        operator: Operator,
        returned: object,
        value: object,
        invocation_context: object,
        session_context: object,
        on_stream_chunk: StreamChunkHandler,
        lease: "_OperatorCapacityLease",
    ) -> object:
        if not is_stream_value(returned):
            raise TypeError("Stream Node Operator must return Iterator or AsyncIterator.")
        assert node.stream is not None
        context = StreamContext(
            invocation_context=_mapping(invocation_context),
            session_context=_mapping(session_context),
            input=value,
        )
        reducer = node.stream.reducer
        state: object | None = None
        stream_error: BaseException | None = None
        try:
            state = await _invoke(
                self._pool,
                reducer.initial,
                context,
                defer_release=lease.defer,
            )
            if hasattr(returned, "__anext__"):
                async for chunk in returned:  # type: ignore[union-attr]
                    if operator.contract.stream_chunk is not None:
                        chunk = operator.contract.stream_chunk.validate(
                            chunk
                        )
                        emitted_chunk = operator.contract.stream_chunk.to_record(
                            chunk
                        )
                    else:
                        emitted_chunk = chunk
                    await _emit_stream_chunk(on_stream_chunk, emitted_chunk)
                    state = await _invoke(
                        self._pool,
                        reducer.add,
                        context,
                        state,
                        chunk,
                        defer_release=lease.defer,
                    )
            else:
                iterator = iter(returned)  # type: ignore[arg-type]
                while True:
                    present, chunk = await _run_sync(
                        self._pool,
                        _next_item,
                        iterator,
                        defer_release=lease.defer,
                    )
                    if not present:
                        break
                    if operator.contract.stream_chunk is not None:
                        chunk = operator.contract.stream_chunk.validate(
                            chunk
                        )
                        emitted_chunk = operator.contract.stream_chunk.to_record(
                            chunk
                        )
                    else:
                        emitted_chunk = chunk
                    await _emit_stream_chunk(on_stream_chunk, emitted_chunk)
                    state = await _invoke(
                        self._pool,
                        reducer.add,
                        context,
                        state,
                        chunk,
                        defer_release=lease.defer,
                    )
        except BaseException as error:
            stream_error = error
            raise
        finally:
            if lease.has_deferred_work:
                lease.close_stream_later(returned)
            else:
                try:
                    await _close_stream_source(
                        self._pool,
                        returned,
                        defer_release=lease.defer,
                    )
                except BaseException:
                    if stream_error is None:
                        raise
        return await _invoke(
            self._pool,
            reducer.finish,
            context,
            state,
            defer_release=lease.defer,
        )


async def _invoke_handler(
    pool: Executor,
    operator: Operator,
    value: object,
    defer_release: Callable[[Future[object]], None],
) -> object:
    try:
        handler = operator.handler
        if inspect.iscoroutinefunction(handler) or inspect.isasyncgenfunction(handler):
            returned = (
                handler()
                if not operator.contract.accepts_input
                else handler(value)
            )
        else:
            returned = await _run_sync(
                pool,
                handler if not operator.contract.accepts_input else lambda: handler(value),
                defer_release=defer_release,
            )
        if inspect.isawaitable(returned):
            return await returned
        return returned
    except asyncio.CancelledError as cancelled:
        if _task_is_cancelling():
            raise
        raise UserCallableCancelledError(
            "User callable raised asyncio.CancelledError."
        ) from cancelled


async def _invoke(
    pool: Executor,
    handler: Callable[..., object],
    *args: object,
    defer_release: Callable[[Future[object]], None] | None = None,
) -> object:
    try:
        if inspect.iscoroutinefunction(handler):
            return await handler(*args)  # type: ignore[misc]
        returned = await _run_sync(
            pool,
            handler,
            *args,
            defer_release=defer_release,
        )
        if inspect.isawaitable(returned):
            return await returned
        return returned
    except asyncio.CancelledError as cancelled:
        if _task_is_cancelling():
            raise
        raise UserCallableCancelledError(
            "User callable raised asyncio.CancelledError."
        ) from cancelled


def _task_is_cancelling() -> bool:
    current = asyncio.current_task()
    return current is not None and current.cancelling() > 0


async def _run_sync(
    pool: Executor,
    handler: Callable[..., object],
    *args: object,
    defer_release: Callable[[Future[object]], None] | None = None,
) -> object:
    future = pool.submit(handler, *args)
    # This cancels queued work when the caller is cancelled. Python cannot
    # forcibly interrupt a sync callable that has already entered user code.
    try:
        return await await_concurrent_future(future)
    except asyncio.CancelledError:
        if defer_release is not None and not future.done():
            defer_release(future)
        raise


class _RuntimeEmitFailure(Exception):
    """A Runtime callback failed outside physical Operator execution."""

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


async def _emit_stream_chunk(
    handler: StreamChunkHandler,
    chunk: object,
) -> None:
    try:
        await handler(chunk)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        raise _RuntimeEmitFailure(error) from error


class _OperatorCapacityLease:
    """Hold one Operator permit until all escaped sync work has settled."""

    def __init__(
        self,
        pool: Executor,
        capacity: asyncio.Semaphore,
        settlement_tasks: set[asyncio.Task[None]],
    ) -> None:
        self._pool = pool
        self._capacity = capacity
        self._settlement_tasks = settlement_tasks
        self._futures: list[Future[object]] = []
        self._stream_sources: list[object] = []
        self._closed = False

    @property
    def has_deferred_work(self) -> bool:
        return bool(self._futures)

    def defer(self, future: Future[object]) -> None:
        if future not in self._futures:
            self._futures.append(future)

    def close_stream_later(self, source: object) -> None:
        if all(existing is not source for existing in self._stream_sources):
            self._stream_sources.append(source)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._futures and not self._stream_sources:
            self._capacity.release()
            return
        task = asyncio.create_task(self._settle())
        self._settlement_tasks.add(task)
        task.add_done_callback(self._settlement_tasks.discard)
        task.add_done_callback(_consume_background_exception)

    async def _settle(self) -> None:
        try:
            for future in self._futures:
                try:
                    await await_concurrent_future(future)
                except BaseException:
                    pass
            for source in self._stream_sources:
                try:
                    await _close_stream_source(self._pool, source)
                except BaseException:
                    pass
        finally:
            self._capacity.release()


def _consume_background_exception(task: asyncio.Task[object]) -> None:
    if not task.cancelled():
        task.exception()


async def _close_stream_source(
    pool: Executor,
    source: object,
    *,
    defer_release: Callable[[Future[object]], None] | None = None,
) -> None:
    close = getattr(source, "aclose", None)
    if not callable(close):
        close = getattr(source, "close", None)
    if callable(close):
        await _invoke(pool, close, defer_release=defer_release)


class _BurstThreadPool(Executor):
    """Lazy bounded workers that exit when the current work burst is drained.

    Workers never wait for a future notification: they drain queued work and
    exit. This keeps idle resource use bounded and isolates worker lifecycle
    from Node execution semantics.
    """

    def __init__(self, max_workers: int) -> None:
        self._max_workers = max_workers
        self._jobs: deque[tuple[Future[object], Callable[[], object]]] = deque()
        self._lock = threading.Lock()
        self._threads: set[threading.Thread] = set()
        self._active_workers = 0
        self._closed = False

    def submit(self, fn, /, *args, **kwargs):
        future: Future[object] = Future()
        with self._lock:
            if self._closed:
                raise RuntimeError("Operator thread pool is closed.")
            self._jobs.append((future, lambda: fn(*args, **kwargs)))
            if self._active_workers < self._max_workers:
                thread = threading.Thread(
                    target=self._worker,
                    name=f"autoagent-operator-{self._active_workers}",
                    daemon=True,
                )
                self._active_workers += 1
                self._threads.add(thread)
                thread.start()
        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if cancel_futures:
                while self._jobs:
                    future, _call = self._jobs.popleft()
                    future.cancel()
            threads = tuple(self._threads)
        if wait:
            for thread in threads:
                thread.join()

    def _worker(self) -> None:
        while True:
            with self._lock:
                if not self._jobs:
                    self._active_workers -= 1
                    self._threads.discard(threading.current_thread())
                    return
                job = self._jobs.popleft()
            future, call = job
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(call())
            except BaseException as error:
                future.set_exception(error)


def _next_item(iterator: Iterator[object]) -> tuple[bool, object | None]:
    try:
        return True, next(iterator)
    except StopIteration:
        return False, None


def _mapping(value: object):
    from collections.abc import Mapping

    return value if isinstance(value, Mapping) else {}
