"""Policy-aware execution of one logical Node occurrence."""

from __future__ import annotations

import asyncio
import inspect
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID, uuid4

from ..operators import Operator, WaitOperator, is_stream_value
from ..runtime import NodeExecution, OperatorCallRecord
from ..runtime.serialization import RuntimeValueCodec
from ..workflow import (
    AggregationContext,
    ContextPatch,
    InputMappingContext,
    ItemSelectorContext,
    NodeIR,
    OutputBindingContext,
)
from .result import NodeExecutionResult, NodePhaseResult


ProgressCallback = Callable[[str, Any], Awaitable[None]]
StreamCallback = Callable[[Any], Awaitable[None]]


class _TimedHookFailure(Exception):
    """Internal carrier preserving timing without changing public Hook errors."""

    def __init__(self, cause: BaseException, timing: dict[str, int]) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.timing = timing


@dataclass(slots=True)
class _AttemptTiming:
    """Mutable timing markers that survive timeout and cancellation."""

    executor_wait_ns: int = 0
    thread_pool_wait_ns: int = 0
    handler_ns: int = 0
    stream_ns: int = 0
    stream_delivery_ns: int = 0
    handler_started_ns: int | None = None


@dataclass(frozen=True, slots=True)
class _OperatorCallDraft:
    """Call identity and immutable pre-call data finalized into one record."""

    id: UUID
    node_execution_id: UUID
    node_id: str
    operator_id: str
    unit_kind: str
    unit_index: int
    attempt: int
    started_at_ms: int
    started_perf_ns: int
    input: Any
    idempotency_key: str | None

    def finalize(
        self,
        *,
        status: Literal["completed", "failed", "timed_out", "cancelled"],
        output: Any,
        error: str | None,
        dispatch_wait_ns: int,
        executor_wait_ns: int,
        thread_pool_wait_ns: int,
        handler_ns: int,
        stream_ns: int,
        stream_delivery_ns: int,
    ) -> OperatorCallRecord:
        return OperatorCallRecord(
            id=self.id,
            node_execution_id=self.node_execution_id,
            node_id=self.node_id,
            operator_id=self.operator_id,
            unit_kind=self.unit_kind,
            unit_index=self.unit_index,
            attempt=self.attempt,
            status=status,
            started_at_ms=self.started_at_ms,
            completed_at_ms=time.time_ns() // 1_000_000,
            duration_ns=max(0, time.perf_counter_ns() - self.started_perf_ns),
            dispatch_wait_ns=dispatch_wait_ns,
            executor_wait_ns=executor_wait_ns,
            thread_pool_wait_ns=thread_pool_wait_ns,
            handler_ns=handler_ns,
            stream_ns=stream_ns,
            stream_delivery_ns=stream_delivery_ns,
            input=self.input,
            output=output,
            error=error,
            idempotency_key=self.idempotency_key,
        )


class NodeExecutor:
    """Run hooks and Operators without mutating shared Runtime state."""

    def __init__(self, *, max_executor_concurrency: int = 8) -> None:
        if max_executor_concurrency < 1:
            raise ValueError("max_executor_concurrency must be positive.")
        self._thread_pool = ThreadPoolExecutor(
            max_workers=max_executor_concurrency,
            thread_name_prefix="autoagent-executor",
        )
        # Every user-defined Hook and Operator shares one App-wide execution
        # budget. Sync callables use the same bounded pool; async callables run
        # on the Runtime Loop while holding the same semaphore slot.
        self._executor_semaphore = asyncio.Semaphore(max_executor_concurrency)
        self._max_executor_concurrency = max_executor_concurrency

    async def execute(
        self,
        *,
        node: NodeIR,
        execution: NodeExecution,
        default_input: Any,
        context: InputMappingContext,
        progress: ProgressCallback,
        stream_chunk: StreamCallback,
        idempotency_key: str | None = None,
        capture_operator_io: bool = False,
    ) -> NodeExecutionResult:
        result = NodeExecutionResult()
        try:
            await self._execute(
                node, execution, default_input, context, result, progress,
                stream_chunk, idempotency_key, capture_operator_io
            )
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            result.error = error
        return result

    async def bind_output(
        self,
        node: NodeIR,
        context: OutputBindingContext,
    ) -> ContextPatch:
        if node.output_binding is None:
            return ContextPatch()
        patch = await self._call_hook(node.output_binding, context)
        if patch is None:
            return ContextPatch()
        if not isinstance(patch, ContextPatch):
            raise TypeError("Output Binding must return ContextPatch or None.")
        RuntimeValueCodec.encode(dict(patch.session))
        RuntimeValueCodec.encode(dict(patch.invocation))
        return patch

    async def _execute(
        self,
        node: NodeIR,
        execution: NodeExecution,
        default_input: Any,
        context: InputMappingContext,
        result: NodeExecutionResult,
        progress: ProgressCallback,
        stream_chunk: StreamCallback,
        idempotency_key: str | None,
        capture_operator_io: bool,
    ) -> None:
        result.mapped_input = await self._phase(
            "input_mapping_finished",
            node.input_mapping,
            (context,),
            default=default_input,
            result=result,
            progress=progress,
            payload_name="input",
        )

        if isinstance(node.operator, WaitOperator):
            result.wait_payload = node.operator.request_contract.validate(
                result.mapped_input
            )
            RuntimeValueCodec.encode(result.wait_payload)
            result.waiting = True
            return

        units, unit_kind = await self._prepare_units(
            node, result.mapped_input, context, result, progress
        )
        outputs = await self._execute_units(
            node=node,
            execution=execution,
            units=units,
            unit_kind=unit_kind,
            result=result,
            progress=progress,
            stream_chunk=stream_chunk,
            idempotency_key=idempotency_key,
            capture_operator_io=capture_operator_io,
        )
        if unit_kind == "normal":
            output = outputs[0]
        else:
            policy = node.policy
            aggregator = (
                policy.map.output_aggregator
                if policy and policy.map
                else policy.replication.output_aggregator
                if policy and policy.replication
                else None
            )
            aggregation_context = self._derived_context(
                AggregationContext,
                context,
                input=result.mapped_input,
                operator_outputs=outputs,
            )
            output = await self._phase(
                "aggregation_finished",
                aggregator,
                (aggregation_context,),
                default=outputs,
                result=result,
                progress=progress,
                payload_name="output",
            )
        output = node.output_contract.validate(output)
        RuntimeValueCodec.encode(output)
        result.output = output

        binding_context = self._derived_context(
            OutputBindingContext,
            context,
            input=result.mapped_input,
            output=output,
        )
        patch = await self._phase(
            "output_binding_finished",
            node.output_binding,
            (binding_context,),
            default=ContextPatch(),
            result=result,
            progress=progress,
            payload_name="patch",
        )
        if patch is None:
            patch = ContextPatch()
        if not isinstance(patch, ContextPatch):
            raise TypeError("Output Binding must return ContextPatch or None.")
        RuntimeValueCodec.encode(dict(patch.session))
        RuntimeValueCodec.encode(dict(patch.invocation))
        result.patch = patch

    async def _prepare_units(
        self,
        node: NodeIR,
        mapped_input: Any,
        context: InputMappingContext,
        result: NodeExecutionResult,
        progress: ProgressCallback,
    ) -> tuple[list[Any], str]:
        policy = node.policy
        if policy and policy.map:
            selector_context = self._derived_context(
                ItemSelectorContext, context, input=mapped_input
            )
            selected = await self._phase(
                "item_selection_finished",
                policy.map.item_selector,
                (selector_context,),
                default=mapped_input,
                result=result,
                progress=progress,
                payload_name="items",
            )
            if not isinstance(selected, list):
                raise TypeError("Map item selection must return list[ItemInput].")
            return selected, "map_item"
        if policy and policy.replication:
            # Units only reference the immutable-by-ownership mapped input.
            # Each physical Operator attempt isolates it immediately before
            # invocation, avoiding an eager copy for every Replica here.
            return [mapped_input] * policy.replication.count, "replica"
        return [mapped_input], "normal"

    async def _execute_units(
        self,
        *,
        node: NodeIR,
        execution: NodeExecution,
        units: list[Any],
        unit_kind: str,
        result: NodeExecutionResult,
        progress: ProgressCallback,
        stream_chunk: StreamCallback,
        idempotency_key: str | None,
        capture_operator_io: bool,
    ) -> list[Any]:
        if not units:
            return []
        policy = node.policy
        configured = (
            policy.map.max_parallelism
            if policy and policy.map
            else policy.replication.max_parallelism
            if policy and policy.replication
            else None
        )
        limit = min(
            len(units),
            configured or self._max_executor_concurrency,
            self._max_executor_concurrency,
        )
        queue: asyncio.Queue[tuple[int, Any, int, int] | None] = asyncio.Queue()
        for index, value in enumerate(units):
            queue.put_nowait(
                (index, value, time.time_ns() // 1_000_000, time.perf_counter_ns())
            )
        for _ in range(max(1, limit)):
            queue.put_nowait(None)
        outputs: list[Any] = [None] * len(units)

        async def worker() -> None:
            while True:
                item = await queue.get()
                if item is None:
                    return
                index, value, scheduled_at_ms, scheduled_perf_ns = item
                outputs[index] = await self._execute_unit(
                    node=node,
                    execution=execution,
                    # ``_execute_unit`` isolates the baseline separately for
                    # every physical Retry/Fallback attempt. Copying here as
                    # well would traverse every unit input twice.
                    value=value,
                    unit_kind=unit_kind,
                    unit_index=index,
                    scheduled_at_ms=scheduled_at_ms,
                    scheduled_perf_ns=scheduled_perf_ns,
                    result=result,
                    progress=progress,
                    stream_chunk=stream_chunk,
                    idempotency_key=idempotency_key,
                    capture_operator_io=capture_operator_io,
                )

        tasks = [asyncio.create_task(worker()) for _ in range(max(1, limit))]
        try:
            await asyncio.gather(*tasks)
            return outputs
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _execute_unit(
        self,
        *,
        node: NodeIR,
        execution: NodeExecution,
        value: Any,
        unit_kind: str,
        unit_index: int,
        scheduled_at_ms: int,
        scheduled_perf_ns: int,
        result: NodeExecutionResult,
        progress: ProgressCallback,
        stream_chunk: StreamCallback,
        idempotency_key: str | None,
        capture_operator_io: bool,
    ) -> Any:
        retry = node.policy.retry if node.policy and node.policy.retry else None
        max_attempts = retry.max_attempts if retry else 1
        assert isinstance(node.operator, Operator)
        operators = (node.operator, *node.fallback_operators)
        last_error: BaseException | None = None
        first_physical_attempt = True
        for operator in operators:
            for attempt in range(1, max_attempts + 1):
                call_id = uuid4()
                await progress("operator_call_started", call_id)
                call_started_perf_ns = time.perf_counter_ns()
                call_started_at_ms = (
                    scheduled_at_ms
                    if first_physical_attempt
                    else time.time_ns() // 1_000_000
                )
                call_duration_start_ns = (
                    scheduled_perf_ns if first_physical_attempt else call_started_perf_ns
                )
                dispatch_wait_ns = (
                    max(0, call_started_perf_ns - scheduled_perf_ns)
                    if first_physical_attempt
                    else 0
                )
                effective_idempotency_key = (
                    f"{idempotency_key}:{unit_kind}:{unit_index}"
                    if idempotency_key is not None
                    else None
                )
                call_input = self._with_idempotency_key(
                    # Every physical attempt owns a fresh value. A failed
                    # primary Call may mutate its arguments, but that must not
                    # alter a Retry or Fallback input for the same unit.
                    RuntimeValueCodec.isolate(value),
                    idempotency_key=effective_idempotency_key,
                    operator=operator,
                )
                draft = _OperatorCallDraft(
                    id=call_id,
                    node_execution_id=execution.id,
                    node_id=node.id,
                    operator_id=operator.id,
                    unit_kind=unit_kind,
                    unit_index=unit_index,
                    attempt=attempt,
                    started_at_ms=call_started_at_ms,
                    started_perf_ns=call_duration_start_ns,
                    input=(
                        RuntimeValueCodec.isolate(call_input)
                        if capture_operator_io
                        else None
                    ),
                    idempotency_key=effective_idempotency_key,
                )
                status: Literal["completed", "failed", "timed_out", "cancelled"] = (
                    "completed"
                )
                error_text = None
                executor_wait_ns = 0
                thread_pool_wait_ns = 0
                handler_ns = 0
                stream_ns = 0
                stream_delivery_ns = 0
                output = None
                cancellation: asyncio.CancelledError | None = None
                attempt_timing = _AttemptTiming()
                try:
                    output = await self._invoke_attempt(
                        operator, call_input, node, stream_chunk, attempt_timing
                    )
                except asyncio.TimeoutError:
                    status = "timed_out"
                    last_error = TimeoutError(
                        f"Operator {operator.id!r} exceeded its timeout."
                    )
                    error_text = str(last_error)
                except asyncio.CancelledError as error:
                    status = "cancelled"
                    cancellation = error
                    error_text = "Operator call was cancelled."
                except BaseException as error:
                    status = "failed"
                    last_error = error
                    error_text = f"{type(error).__name__}: {error}"
                executor_wait_ns = attempt_timing.executor_wait_ns
                thread_pool_wait_ns = attempt_timing.thread_pool_wait_ns
                handler_ns = attempt_timing.handler_ns
                stream_ns = attempt_timing.stream_ns
                stream_delivery_ns = attempt_timing.stream_delivery_ns
                record = draft.finalize(
                    status=status,
                    dispatch_wait_ns=dispatch_wait_ns,
                    executor_wait_ns=executor_wait_ns,
                    thread_pool_wait_ns=thread_pool_wait_ns,
                    handler_ns=handler_ns,
                    stream_ns=stream_ns,
                    stream_delivery_ns=stream_delivery_ns,
                    output=(
                        output
                        if capture_operator_io and status == "completed"
                        else None
                    ),
                    error=error_text,
                )
                first_physical_attempt = False
                execution.operator_attempts += 1
                await progress("operator_call", record)
                if cancellation is not None:
                    raise cancellation
                if status == "completed":
                    return output
                if attempt < max_attempts:
                    await asyncio.sleep(
                        self._retry_delay(retry.backoff if retry else None, attempt)
                    )
        assert last_error is not None
        raise last_error

    @staticmethod
    def _with_idempotency_key(
        value: Any, *, idempotency_key: str | None, operator: Operator
    ) -> Any:
        if idempotency_key is None:
            return value
        if not any(
            parameter.name == "idempotency_key"
            for parameter in operator.contract.parameters
        ):
            return value
        if not isinstance(value, Mapping):
            raise TypeError(
                "An idempotent Operator input must be a mapping so Core can inject "
                "idempotency_key."
            )
        enriched = dict(value)
        supplied = enriched.get("idempotency_key")
        if supplied is not None and supplied != idempotency_key:
            raise ValueError("Input cannot override Core's idempotency_key.")
        enriched["idempotency_key"] = idempotency_key
        return enriched

    async def _invoke_attempt(
        self,
        operator: Operator,
        value: Any,
        node: NodeIR,
        stream_chunk: StreamCallback,
        timing: _AttemptTiming,
    ) -> Any:
        async def call() -> Any:
            admission_started = time.perf_counter_ns()
            acquired = False
            handler = operator.handler
            try:
                try:
                    await self._executor_semaphore.acquire()
                    acquired = True
                finally:
                    timing.executor_wait_ns = max(
                        0, time.perf_counter_ns() - admission_started
                    )
                if inspect.iscoroutinefunction(handler):
                    started = time.perf_counter_ns()
                    timing.handler_started_ns = started
                    try:
                        output = await self._invoke_handler(operator, value)
                    finally:
                        timing.handler_ns = max(
                            0, time.perf_counter_ns() - started
                        )
                else:
                    submitted = time.perf_counter_ns()
                    loop = asyncio.get_running_loop()
                    output = await self._await_thread_future(
                        loop.run_in_executor(
                            self._thread_pool,
                            self._invoke_handler_tracked,
                            operator,
                            value,
                            submitted,
                            timing,
                        )
                    )
                    if inspect.isawaitable(output):
                        await_started = time.perf_counter_ns()
                        try:
                            output = await output
                        finally:
                            timing.handler_ns += max(
                                0, time.perf_counter_ns() - await_started
                            )
                stream_policy = node.policy.stream if node.policy else None
                if stream_policy is not None:
                    if not is_stream_value(output):
                        raise TypeError(
                            "A Node with StreamPolicy must return Iterator or AsyncIterator."
                        )
                    stream_started = time.perf_counter_ns()
                    try:
                        output, timing.stream_delivery_ns = await self._consume_stream(
                            output, operator, node, stream_chunk
                        )
                    finally:
                        timing.stream_ns = max(
                            0,
                            time.perf_counter_ns()
                            - stream_started
                            - timing.stream_delivery_ns,
                        )
                elif is_stream_value(output):
                    raise TypeError(
                        "A streaming Operator requires NodePolicy(stream=StreamPolicy(...))."
                    )
                else:
                    assert operator.contract.output is not None
                    output = operator.contract.output.validate(output)
                    RuntimeValueCodec.encode(output)
                return output
            finally:
                if timing.handler_started_ns is not None and timing.handler_ns == 0:
                    timing.handler_ns = max(
                        0, time.perf_counter_ns() - timing.handler_started_ns
                    )
                if acquired:
                    self._executor_semaphore.release()

        timeout = (
            node.policy.timeout.timeout_ms / 1000
            if node.policy and node.policy.timeout
            else None
        )
        return await asyncio.wait_for(call(), timeout) if timeout else await call()

    @staticmethod
    def _invoke_handler_tracked(
        operator: Operator,
        value: Any,
        submitted_ns: int,
        timing: _AttemptTiming,
    ) -> Any:
        started = time.perf_counter_ns()
        timing.thread_pool_wait_ns = max(0, started - submitted_ns)
        timing.handler_started_ns = started
        try:
            return NodeExecutor._invoke_handler_sync(operator, value)
        finally:
            timing.handler_ns = max(0, time.perf_counter_ns() - started)

    @staticmethod
    async def _invoke_handler(operator: Operator, value: Any) -> Any:
        output = NodeExecutor._invoke_handler_sync(operator, value)
        return await output if inspect.isawaitable(output) else output

    @staticmethod
    def _invoke_handler_sync(operator: Operator, value: Any) -> Any:
        arguments, keywords = operator.contract.prepare_call(value)
        return operator.handler(*arguments, **keywords)

    async def _consume_stream(
        self,
        source: object,
        operator: Operator,
        node: NodeIR,
        stream_chunk: StreamCallback,
    ) -> tuple[Any, int]:
        policy = node.policy.stream if node.policy else None
        assert policy is not None and operator.contract.stream_chunk is not None
        reducer = policy.reducer()
        delivery_ns = 0
        if hasattr(source, "__aiter__"):
            async for raw_chunk in source:  # type: ignore[union-attr]
                chunk = operator.contract.stream_chunk.validate(raw_chunk)
                RuntimeValueCodec.encode(chunk)
                value = reducer.add(chunk)
                if value is not None:
                    raise TypeError("StreamReducer.add() must return None.")
                started = time.perf_counter_ns()
                await stream_chunk(chunk)
                delivery_ns += max(0, time.perf_counter_ns() - started)
        else:
            iterator = iter(source)  # type: ignore[arg-type]
            while True:
                exists, raw_chunk = await self._await_thread_future(
                    asyncio.get_running_loop().run_in_executor(
                        self._thread_pool, self._next, iterator
                    )
                )
                if not exists:
                    break
                chunk = operator.contract.stream_chunk.validate(raw_chunk)
                RuntimeValueCodec.encode(chunk)
                value = reducer.add(chunk)
                if value is not None:
                    raise TypeError("StreamReducer.add() must return None.")
                started = time.perf_counter_ns()
                await stream_chunk(chunk)
                delivery_ns += max(0, time.perf_counter_ns() - started)
        output = reducer.finish()
        if inspect.isawaitable(output):
            raise TypeError("StreamReducer.finish() must be synchronous.")
        output = node.output_contract.validate(output)
        RuntimeValueCodec.encode(output)
        return output, delivery_ns

    @staticmethod
    def _next(iterator: Any) -> tuple[bool, Any]:
        try:
            return True, next(iterator)
        except StopIteration:
            return False, None

    async def _phase(
        self,
        name: str,
        function: Callable[..., Any] | None,
        arguments: tuple[Any, ...],
        *,
        default: Any,
        result: NodeExecutionResult,
        progress: ProgressCallback,
        payload_name: str,
    ) -> Any:
        if function is None:
            return default
        started_at_ms = time.time_ns() // 1_000_000
        started = time.perf_counter_ns()
        try:
            value, timing = await self.call_hook_timed(function, *arguments)
        except _TimedHookFailure as failure:
            error = failure.cause
            completed_at_ms = time.time_ns() // 1_000_000
            duration = max(0, time.perf_counter_ns() - started)
            phase = NodePhaseResult(
                name=name,
                status="failed",
                started_at_ms=started_at_ms,
                completed_at_ms=completed_at_ms,
                duration_ns=duration,
                executor_wait_ns=failure.timing["executor_wait_ns"],
                thread_pool_wait_ns=failure.timing["thread_pool_wait_ns"],
                handler_ns=failure.timing["handler_ns"],
                payload={"error": f"{type(error).__name__}: {error}"},
            )
            if name == "output_binding_finished":
                result.deferred_phases.append(phase)
            else:
                await progress("phase", phase)
            raise
        completed_at_ms = time.time_ns() // 1_000_000
        duration = max(0, time.perf_counter_ns() - started)
        phase = NodePhaseResult(
            name=name,
            status="completed",
            started_at_ms=started_at_ms,
            completed_at_ms=completed_at_ms,
            duration_ns=duration,
            executor_wait_ns=timing["executor_wait_ns"],
            thread_pool_wait_ns=timing["thread_pool_wait_ns"],
            handler_ns=timing["handler_ns"],
            payload={payload_name: value},
        )
        if name == "output_binding_finished":
            result.deferred_phases.append(phase)
        else:
            await progress("phase", phase)
        return value

    async def _call_hook(self, function: Callable[..., Any], *arguments: Any) -> Any:
        return await self.call_hook(function, *arguments)

    async def call_hook(self, function: Callable[..., Any], *arguments: Any) -> Any:
        """Run one Hook under the same global execution budget as Operators."""

        try:
            value, _ = await self.call_hook_timed(function, *arguments)
        except _TimedHookFailure as failure:
            raise failure.cause
        return value

    async def call_hook_timed(
        self, function: Callable[..., Any], *arguments: Any
    ) -> tuple[Any, dict[str, int]]:
        """Run a Hook and report waits separately from user handler time."""

        admission_started = time.perf_counter_ns()
        await self._executor_semaphore.acquire()
        executor_wait_ns = max(0, time.perf_counter_ns() - admission_started)
        try:
            if inspect.iscoroutinefunction(function):
                handler_started = time.perf_counter_ns()
                try:
                    value = await function(*arguments)
                except BaseException as error:
                    raise _TimedHookFailure(
                        error,
                        {
                            "executor_wait_ns": executor_wait_ns,
                            "thread_pool_wait_ns": 0,
                            "handler_ns": max(
                                0, time.perf_counter_ns() - handler_started
                            ),
                        },
                    ) from error
                return value, {
                    "executor_wait_ns": executor_wait_ns,
                    "thread_pool_wait_ns": 0,
                    "handler_ns": max(0, time.perf_counter_ns() - handler_started),
                }
            loop = asyncio.get_running_loop()
            submitted = time.perf_counter_ns()
            ok, value, worker_started, worker_finished = await self._await_thread_future(
                loop.run_in_executor(
                    self._thread_pool,
                    self._call_hook_sync_timed,
                    function,
                    *arguments,
                )
            )
            # Callable instances and ordinary functions may return an Awaitable
            # even when inspect cannot identify them as coroutine functions.
            handler_ns = max(0, worker_finished - worker_started)
            timing = {
                "executor_wait_ns": executor_wait_ns,
                "thread_pool_wait_ns": max(0, worker_started - submitted),
                "handler_ns": handler_ns,
            }
            if not ok:
                assert isinstance(value, BaseException)
                raise _TimedHookFailure(value, timing) from value
            if inspect.isawaitable(value):
                await_started = time.perf_counter_ns()
                try:
                    value = await value
                except BaseException as error:
                    timing["handler_ns"] += max(
                        0, time.perf_counter_ns() - await_started
                    )
                    raise _TimedHookFailure(error, timing) from error
                handler_ns += max(0, time.perf_counter_ns() - await_started)
            return value, {
                "executor_wait_ns": executor_wait_ns,
                "thread_pool_wait_ns": max(0, worker_started - submitted),
                "handler_ns": handler_ns,
            }
        finally:
            self._executor_semaphore.release()

    @staticmethod
    def _call_hook_sync_timed(
        function: Callable[..., Any], *arguments: Any
    ) -> tuple[bool, Any, int, int]:
        started = time.perf_counter_ns()
        try:
            value = function(*arguments)
        except BaseException as error:
            return False, error, started, time.perf_counter_ns()
        return True, value, started, time.perf_counter_ns()

    @staticmethod
    async def _await_thread_future(future: asyncio.Future[Any]) -> Any:
        """Bound lost selector wakeups only while thread-pool work is active."""

        while not future.done():
            await asyncio.sleep(0.001)
        return future.result()

    @staticmethod
    def _derived_context(
        context_type: type[Any], context: InputMappingContext, **values: Any
    ) -> Any:
        if context_type is ItemSelectorContext:
            values["input"] = RuntimeValueCodec.isolate(values["input"])
        elif context_type is OutputBindingContext:
            values["input"] = RuntimeValueCodec.isolate(values["input"])
            values["output"] = RuntimeValueCodec.isolate(values["output"])
        elif context_type is AggregationContext:
            values["input"] = RuntimeValueCodec.isolate(values["input"])
            # operator_outputs is already a private, unit-index ordered working
            # list for this Aggregator and is intentionally writable.
        return context_type(
            workflow_id=context.workflow_id,
            workflow_revision_id=context.workflow_revision_id,
            workflow_path=context.workflow_path,
            session_id=context.session_id,
            invocation_id=context.invocation_id,
            session_context=context.session_context,
            invocation_context=context.invocation_context,
            invocation_input=context.invocation_input,
            node_id=context.node_id,
            node_execution_id=context.node_execution_id,
            execution_scope=context.execution_scope,
            incoming=context.incoming,
            **values,
        )

    @staticmethod
    def _retry_delay(backoff: Any, retry_index: int) -> float:
        if backoff is None:
            return 0.0
        delay = float(backoff.initial_delay_ms)
        if backoff.mode == "linear":
            delay *= retry_index
        elif backoff.mode == "exponential":
            delay *= backoff.multiplier ** (retry_index - 1)
        if backoff.max_delay_ms is not None:
            delay = min(delay, backoff.max_delay_ms)
        if backoff.jitter == "full":
            delay = random.uniform(0, delay)
        elif backoff.jitter == "equal":
            delay = delay / 2 + random.uniform(0, delay / 2)
        return delay / 1000

    def close(self) -> None:
        self._thread_pool.shutdown(wait=False, cancel_futures=True)
