"""Policy-aware execution of one logical Node occurrence."""

from __future__ import annotations

import asyncio
import inspect
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

from ..operators import Operator, WaitOperator, is_stream_value
from ..runtime import NodeExecution, OperatorCallRecord
from ..runtime.serialization import encode_runtime_value
from ..workflow import ContextPatch, ExecutionContext, NodeIR
from .result import NodeExecutionResult, NodePhaseResult


ProgressCallback = Callable[[str, Any], Awaitable[None]]
StreamCallback = Callable[[Any], Awaitable[None]]


class NodeExecutor:
    """Run hooks and Operators without mutating shared Runtime state."""

    def __init__(self, *, max_thread_workers: int = 8, max_parallel_units: int = 8) -> None:
        if max_thread_workers < 1 or max_parallel_units < 1:
            raise ValueError("Executor worker limits must be positive.")
        self._thread_pool = ThreadPoolExecutor(
            max_workers=max_thread_workers, thread_name_prefix="autoagent-operator"
        )
        self._max_parallel_units = max_parallel_units
        self._node_semaphores: dict[tuple[str, str], asyncio.Semaphore] = {}

    async def execute(
        self,
        *,
        workflow_revision_id: str,
        node: NodeIR,
        execution: NodeExecution,
        default_input: Any,
        context: ExecutionContext,
        progress: ProgressCallback,
        stream_chunk: StreamCallback,
        idempotency_key: str | None = None,
    ) -> NodeExecutionResult:
        result = NodeExecutionResult()
        limit = node.policy.max_concurrency if node.policy else None
        semaphore = None
        if limit is not None:
            semaphore = self._node_semaphores.setdefault(
                (workflow_revision_id, node.id), asyncio.Semaphore(limit)
            )
        try:
            if semaphore is None:
                await self._execute(
                    node, execution, default_input, context, result, progress,
                    stream_chunk, idempotency_key
                )
            else:
                async with semaphore:
                    await self._execute(
                        node, execution, default_input, context, result, progress,
                        stream_chunk, idempotency_key
                    )
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            result.error = error
        return result

    async def bind_output(
        self,
        node: NodeIR,
        context: ExecutionContext,
        output: Any,
    ) -> ContextPatch:
        if node.output_binding is None:
            return ContextPatch()
        patch = await self._call_hook(node.output_binding, context, output)
        if patch is None:
            return ContextPatch()
        if not isinstance(patch, ContextPatch):
            raise TypeError("Output Binding must return ContextPatch or None.")
        encode_runtime_value(dict(patch.session))
        encode_runtime_value(dict(patch.invocation))
        return patch

    async def _execute(
        self,
        node: NodeIR,
        execution: NodeExecution,
        default_input: Any,
        context: ExecutionContext,
        result: NodeExecutionResult,
        progress: ProgressCallback,
        stream_chunk: StreamCallback,
        idempotency_key: str | None,
    ) -> None:
        result.mapped_input = await self._phase(
            "input_mapping_completed",
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
            encode_runtime_value(result.wait_payload)
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
            output = await self._phase(
                "aggregation_completed",
                aggregator,
                (context, outputs),
                default=outputs,
                result=result,
                progress=progress,
                payload_name="output",
            )
        output = node.output_contract.validate(output)
        encode_runtime_value(output)
        result.output = output

        patch = await self._phase(
            "output_binding_completed",
            node.output_binding,
            (context, output),
            default=ContextPatch(),
            result=result,
            progress=progress,
            payload_name="patch",
        )
        if patch is None:
            patch = ContextPatch()
        if not isinstance(patch, ContextPatch):
            raise TypeError("Output Binding must return ContextPatch or None.")
        encode_runtime_value(dict(patch.session))
        encode_runtime_value(dict(patch.invocation))
        result.patch = patch

    async def _prepare_units(
        self,
        node: NodeIR,
        mapped_input: Any,
        context: ExecutionContext,
        result: NodeExecutionResult,
        progress: ProgressCallback,
    ) -> tuple[list[Any], str]:
        policy = node.policy
        if policy and policy.map:
            selected = await self._phase(
                "item_selection_completed",
                policy.map.item_selector,
                (context, mapped_input),
                default=mapped_input,
                result=result,
                progress=progress,
                payload_name="items",
            )
            if not isinstance(selected, list):
                raise TypeError("Map item selection must return list[ItemInput].")
            return selected, "map_item"
        if policy and policy.replication:
            return [mapped_input for _ in range(policy.replication.count)], "replica"
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
        limit = min(len(units), configured or self._max_parallel_units, self._max_parallel_units)
        semaphore = asyncio.Semaphore(max(1, limit))

        async def run(index: int, value: Any) -> Any:
            async with semaphore:
                output = await self._execute_unit(
                    node=node,
                    execution=execution,
                    value=value,
                    unit_kind=unit_kind,
                    unit_index=index,
                    result=result,
                    progress=progress,
                    stream_chunk=stream_chunk,
                    idempotency_key=idempotency_key,
                )
                return output

        tasks = [asyncio.create_task(run(index, value)) for index, value in enumerate(units)]
        try:
            return list(await asyncio.gather(*tasks))
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
        result: NodeExecutionResult,
        progress: ProgressCallback,
        stream_chunk: StreamCallback,
        idempotency_key: str | None,
    ) -> Any:
        retry = node.policy.retry if node.policy and node.policy.retry else None
        max_attempts = retry.max_attempts if retry else 1
        assert isinstance(node.operator, Operator)
        operators = (node.operator, *node.fallback_operators)
        last_error: BaseException | None = None
        for operator in operators:
            for attempt in range(1, max_attempts + 1):
                await progress("operator_call_started", None)
                started = time.perf_counter_ns()
                status = "completed"
                error_text = None
                queue_wait_ns = 0
                handler_ns = 0
                stream_ns = 0
                stream_delivery_ns = 0
                output = None
                try:
                    call_input = self._with_idempotency_key(
                        value,
                        idempotency_key=(
                            f"{idempotency_key}:{unit_kind}:{unit_index}"
                            if idempotency_key is not None
                            else None
                        ),
                        operator=operator,
                    )
                    output, timing = await self._invoke_attempt(
                        operator, call_input, node, stream_chunk
                    )
                    queue_wait_ns = timing["queue_wait_ns"]
                    handler_ns = timing["handler_ns"]
                    stream_ns = timing["stream_ns"]
                    stream_delivery_ns = timing["stream_delivery_ns"]
                except asyncio.TimeoutError as error:
                    status = "timed_out"
                    last_error = TimeoutError(
                        f"Operator {operator.id!r} exceeded its timeout."
                    )
                    error_text = str(last_error)
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
                    status = "failed"
                    last_error = error
                    error_text = f"{type(error).__name__}: {error}"
                duration = max(0, time.perf_counter_ns() - started)
                record = OperatorCallRecord(
                    id=uuid4(),
                    node_execution_id=execution.id,
                    node_id=node.id,
                    operator_id=operator.id,
                    unit_kind=unit_kind,
                    unit_index=unit_index,
                    attempt=attempt,
                    status=status,  # type: ignore[arg-type]
                    duration_ns=duration,
                    queue_wait_ns=queue_wait_ns,
                    handler_ns=handler_ns,
                    stream_ns=stream_ns,
                    stream_delivery_ns=stream_delivery_ns,
                    input=value,
                    output=output if status == "completed" else None,
                    error=error_text,
                    idempotency_key=(
                        f"{idempotency_key}:{unit_kind}:{unit_index}"
                        if idempotency_key is not None
                        else None
                    ),
                )
                result.operator_calls.append(record)
                execution.operator_attempts += 1
                await progress("operator_call", record)
                if status == "completed":
                    return output
                if attempt < max_attempts:
                    await asyncio.sleep(self._retry_delay(retry.backoff if retry else None, attempt))
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
    ) -> tuple[Any, dict[str, int]]:
        async def call() -> tuple[Any, dict[str, int]]:
            handler = operator.handler
            if inspect.iscoroutinefunction(handler):
                started = time.perf_counter_ns()
                output = await self._invoke_handler(operator, value)
                queue_wait_ns = 0
                handler_ns = max(0, time.perf_counter_ns() - started)
            else:
                submitted = time.perf_counter_ns()
                loop = asyncio.get_running_loop()
                output, worker_started, worker_finished = await loop.run_in_executor(
                    self._thread_pool,
                    self._invoke_handler_timed,
                    operator,
                    value,
                )
                queue_wait_ns = max(0, worker_started - submitted)
                handler_ns = max(0, worker_finished - worker_started)
                if inspect.isawaitable(output):
                    await_started = time.perf_counter_ns()
                    output = await output
                    handler_ns += max(0, time.perf_counter_ns() - await_started)
            stream_ns = 0
            stream_delivery_ns = 0
            stream_policy = node.policy.stream if node.policy else None
            if stream_policy is not None:
                if not is_stream_value(output):
                    raise TypeError(
                        "A Node with StreamPolicy must return Iterator or AsyncIterator."
                    )
                stream_started = time.perf_counter_ns()
                output, stream_delivery_ns = await self._consume_stream(
                    output,
                    operator,
                    node,
                    stream_chunk,
                )
                stream_ns = max(
                    0,
                    time.perf_counter_ns()
                    - stream_started
                    - stream_delivery_ns,
                )
            elif is_stream_value(output):
                raise TypeError(
                    "A streaming Operator requires NodePolicy(stream=StreamPolicy(...))."
                )
            else:
                assert operator.contract.output is not None
                output = operator.contract.output.validate(output)
                encode_runtime_value(output)
            return output, {
                "queue_wait_ns": queue_wait_ns,
                "handler_ns": handler_ns,
                "stream_ns": stream_ns,
                "stream_delivery_ns": stream_delivery_ns,
            }

        timeout = node.policy.timeout.timeout_ms / 1000 if node.policy and node.policy.timeout else None
        return await asyncio.wait_for(call(), timeout) if timeout else await call()

    @staticmethod
    def _invoke_handler_timed(
        operator: Operator, value: Any
    ) -> tuple[Any, int, int]:
        started = time.perf_counter_ns()
        output = NodeExecutor._invoke_handler_sync(operator, value)
        return output, started, time.perf_counter_ns()

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
                encode_runtime_value(chunk)
                value = reducer.add(chunk)
                if value is not None:
                    raise TypeError("StreamReducer.add() must return None.")
                started = time.perf_counter_ns()
                await stream_chunk(chunk)
                delivery_ns += max(0, time.perf_counter_ns() - started)
        else:
            iterator = iter(source)  # type: ignore[arg-type]
            while True:
                exists, raw_chunk = await asyncio.get_running_loop().run_in_executor(
                    self._thread_pool, self._next, iterator
                )
                if not exists:
                    break
                chunk = operator.contract.stream_chunk.validate(raw_chunk)
                encode_runtime_value(chunk)
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
        encode_runtime_value(output)
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
        started = time.perf_counter_ns()
        try:
            value = await self._call_hook(function, *arguments)
        except BaseException as error:
            duration = max(0, time.perf_counter_ns() - started)
            phase = NodePhaseResult(
                name=name, status="failed", duration_ns=duration,
                payload={"error": f"{type(error).__name__}: {error}"},
            )
            result.phases.append(phase)
            await progress("phase", phase)
            raise
        duration = max(0, time.perf_counter_ns() - started)
        phase = NodePhaseResult(
            name=name, status="completed", duration_ns=duration,
            payload={payload_name: value},
        )
        result.phases.append(phase)
        await progress("phase", phase)
        return value

    async def _call_hook(self, function: Callable[..., Any], *arguments: Any) -> Any:
        if inspect.iscoroutinefunction(function):
            return await function(*arguments)
        loop = asyncio.get_running_loop()
        value = await loop.run_in_executor(self._thread_pool, function, *arguments)
        return await value if inspect.isawaitable(value) else value

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
