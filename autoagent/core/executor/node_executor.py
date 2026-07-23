from __future__ import annotations

import asyncio
from copy import deepcopy
import inspect
import random
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from autoagent.core.compiler import NodeIR
from autoagent.core.executor.result import NodeExecutionResult
from autoagent.core.operators import (
    CapabilityRegistry,
    Operator,
    OperatorRegistry,
    OperatorResolutionError,
    OperatorResolver,
)
from autoagent.core.runtime import (
    DirectOperatorExecution,
    InvocationExecutionMailbox,
    MapAggregationContext,
    MapItemSelectionContext,
    NodeExecution,
    ParallelExecutionSummary,
    ParallelOperatorExecution,
    ResourceUsage,
    ReplicationAggregationContext,
    RuntimeErrorInfo,
    RuntimeConcurrencyController,
)
from autoagent.core.runtime.context import HookContextSnapshot
from autoagent.core.runtime.hooks import invoke_hook_async
from autoagent.core.runtime.time import utc_timestamp_ms
from autoagent.core.workflow import BackoffPolicy, MapPolicy
from autoagent.core.workflow.capability import SystemCommand, WAIT_SYSTEM_COMMAND_ID


@dataclass(frozen=True)
class NodeExecutionJob:
    """Prepared unit of work submitted by WorkflowExecutor.

    WorkflowExecutor creates NodeExecution objects and builds final node input
    before submitting jobs. NodeExecutor runs the node capability in its native
    async path or shared sync thread pool, but returns a NodeExecutionResult
    instead of mutating runtime state directly.
    """

    node_ir: NodeIR
    node_execution: NodeExecution
    input: Any
    max_operator_attempts: int | None = None
    map_policy: MapPolicy | None = None
    concurrency_key: str | None = None
    recovery: bool = False
    hook_context: HookContextSnapshot | None = None


@dataclass(frozen=True)
class ResolvedNodeExecutionJob:
    """Immutable worker job containing one concrete resolved Operator.

    Registry resolution happens on the WorkflowExecutor control path before
    submission. Operator tasks therefore run user code without mutating App
    registries, Invocation state, or persistent runtime records.
    """

    node_ir: NodeIR
    node_execution: NodeExecution
    operators: tuple[Operator, ...]
    input: Any
    max_operator_attempts: int | None = None
    map_policy: MapPolicy | None = None
    concurrency_key: str | None = None
    recovery: bool = False
    hook_context: HookContextSnapshot | None = None
    concurrency_controller: RuntimeConcurrencyController | None = None
    thread_pool: ThreadPoolExecutor | None = None


class NodeExecutor:
    """Dispatch NodeExecution jobs through one async-first execution path.

    Public contract:
      - submit_batch is non-blocking. It records asyncio Tasks and returns.
      - wait_next_completed awaits only until at least one submitted job finishes.
      - completed results are returned to WorkflowExecutor for state writes.

    Coroutine Operators run directly as Tasks. Synchronous Operators use the
    shared thread pool so they cannot block the event loop. Retry, fallback,
    map, replication, timeout, and aggregation share the same async code path.

    CapabilityRef and OperatorRef are resolved for each NodeExecution, so newly
    registered Operators can participate without recompiling Workflow IR.
    System command, process, and external-worker lanes can be added behind the
    same result contract later.
    """

    def __init__(
        self,
        *,
        max_thread_workers: int = 8,
        operator_resolver: OperatorResolver | None = None,
        concurrency_controller: RuntimeConcurrencyController | None = None,
    ) -> None:
        self.thread_pool = ThreadPoolExecutor(max_workers=max_thread_workers)
        if operator_resolver is None:
            capabilities = CapabilityRegistry()
            operators = OperatorRegistry(capabilities)
            operator_resolver = OperatorResolver(capabilities, operators)
        self.operator_resolver = operator_resolver
        self.concurrency_controller = (
            concurrency_controller or RuntimeConcurrencyController()
        )

    def submit_batch(
        self,
        jobs: list[NodeExecutionJob],
        *,
        mailbox: InvocationExecutionMailbox,
    ) -> None:
        for job in jobs:
            if isinstance(job.node_ir.capability, SystemCommand):
                mailbox.put_completed(_execute_system_command(job))
                continue

            selection_policy = (
                job.node_ir.policy.selection
                if job.node_ir.policy is not None
                else None
            )
            try:
                operators = self.operator_resolver.resolve_candidates(
                    job.node_ir.capability,
                    selection_policy,
                )
            except OperatorResolutionError as exc:
                mailbox.put_completed(
                    NodeExecutionResult(
                        node_execution_id=job.node_execution.id,
                        state="failed",
                        error=RuntimeErrorInfo(
                            code=exc.code,
                            message=exc.message,
                            detail={"node_id": job.node_ir.id, **exc.detail},
                        ),
                    )
                )
                continue

            if job.max_operator_attempts is not None:
                operators = operators[:job.max_operator_attempts]
            if not operators:
                mailbox.put_completed(
                    NodeExecutionResult(
                        node_execution_id=job.node_execution.id,
                        state="failed",
                        error=RuntimeErrorInfo(
                            code="RESOURCE_LIMIT_EXCEEDED",
                            message="Operator attempt limit exceeded.",
                            detail={"node_id": job.node_ir.id},
                        ),
                    )
                )
                continue

            resolved_job = ResolvedNodeExecutionJob(
                node_ir=job.node_ir,
                node_execution=job.node_execution,
                operators=operators,
                input=job.input,
                max_operator_attempts=job.max_operator_attempts,
                map_policy=job.map_policy,
                concurrency_key=job.concurrency_key,
                recovery=job.recovery,
                hook_context=job.hook_context,
                concurrency_controller=self.concurrency_controller,
                thread_pool=self.thread_pool,
            )
            task = asyncio.create_task(_execute_job(resolved_job))
            mailbox.track(task, job.node_execution.id)

    def has_running(self, mailbox: InvocationExecutionMailbox) -> bool:
        return mailbox.has_pending()

    async def wait_next_completed(
        self,
        mailbox: InvocationExecutionMailbox,
    ) -> list[NodeExecutionResult]:
        ready = mailbox.drain_completed()
        if ready:
            return ready
        running = mailbox.running_tasks()
        if not running:
            return []

        done, _ = await asyncio.wait(
            running,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            node_execution_id = mailbox.node_execution_id_for(task)
            mailbox.finish_task(
                task,
                self._task_result(task, node_execution_id),
            )
        return mailbox.drain_completed()

    async def abandon(self, mailbox: InvocationExecutionMailbox) -> None:
        await mailbox.abandon()

    def _task_result(
        self,
        task: asyncio.Task[NodeExecutionResult],
        node_execution_id: UUID,
    ) -> NodeExecutionResult:
        try:
            return task.result()
        except asyncio.CancelledError:
            return NodeExecutionResult(
                node_execution_id=node_execution_id,
                state="cancelled",
                error=RuntimeErrorInfo(
                    code="NODE_EXECUTION_CANCELLED",
                    message="Node execution task was cancelled.",
                ),
            )
        except Exception as exc:  # pragma: no cover - defensive lane failure guard.
            return NodeExecutionResult(
                node_execution_id=node_execution_id,
                state="failed",
                error=RuntimeErrorInfo(
                    code="NODE_EXECUTOR_FAILED",
                    message=str(exc),
                ),
            )


def _execute_system_command(job: NodeExecutionJob) -> NodeExecutionResult:
    """Interpret one framework command without Operator resolution or calls."""

    command = job.node_ir.capability
    if not isinstance(command, SystemCommand) or command.id != WAIT_SYSTEM_COMMAND_ID:
        return NodeExecutionResult(
            node_execution_id=job.node_execution.id,
            state="failed",
            error=RuntimeErrorInfo(
                code="SYSTEM_COMMAND_UNSUPPORTED",
                message="NodeExecutor received an unsupported SystemCommand.",
                detail={"node_id": job.node_ir.id},
            ),
        )
    if job.map_policy is not None or job.node_ir.policy is not None:
        return NodeExecutionResult(
            node_execution_id=job.node_execution.id,
            state="failed",
            error=RuntimeErrorInfo(
                code="SYSTEM_COMMAND_POLICY_UNSUPPORTED",
                message="SystemCommand wait cannot execute with node or map policies.",
                detail={"node_id": job.node_ir.id},
            ),
        )

    try:
        if not isinstance(job.input, Mapping):
            raise TypeError("SystemCommand wait input must be a named-argument mapping.")
        arguments = job.node_ir.input_contract.validate(dict(job.input))
        wait_key = arguments.get("wait_key") or str(job.node_execution.id)
        if not wait_key.strip():
            raise ValueError("SystemCommand wait_key cannot be empty.")
    except (TypeError, ValueError, ValidationError) as exc:
        return NodeExecutionResult(
            node_execution_id=job.node_execution.id,
            state="failed",
            error=RuntimeErrorInfo(
                code="SYSTEM_COMMAND_INPUT_INVALID",
                message=str(exc),
                detail={
                    "node_id": job.node_ir.id,
                    "expected_schema": job.node_ir.input_contract.json_schema,
                },
            ),
        )

    return NodeExecutionResult(
        node_execution_id=job.node_execution.id,
        state="waiting",
        wait_key=wait_key,
        wait_type=arguments.get("wait_type") or "external",
        wait_payload=dict(arguments.get("payload") or {}),
    )


async def _execute_job(job: ResolvedNodeExecutionJob) -> NodeExecutionResult:
    limit = job.node_ir.policy.max_concurrency if job.node_ir.policy is not None else None
    key = job.concurrency_key or job.node_ir.id
    controller = job.concurrency_controller or RuntimeConcurrencyController()
    async with controller.async_slot(key, limit):
        return await _execute_job_with_slot(job)


async def _execute_job_with_slot(job: ResolvedNodeExecutionJob) -> NodeExecutionResult:
    prepared = await _prepare_units(job)
    if isinstance(prepared, RuntimeErrorInfo):
        return _failed_result(job, prepared)
    units, unit_kind = prepared
    input_error = _validate_unit_inputs(job, units)
    if input_error is not None:
        return _failed_result(job, input_error)
    budget = _CallBudget(job.max_operator_attempts)
    call_sequence = _CallSequence(
        sum(
            execution.attempt_count
            for execution in job.node_execution.operator_executions
        )
    )
    unit_results, peak_parallelism = await _execute_units(
        job,
        units,
        budget,
        call_sequence,
        unit_kind=unit_kind,
        max_parallelism=_max_parallelism(job, len(units)),
    )
    return await _aggregate_unit_results(
        job,
        unit_results,
        unit_kind=unit_kind,
        unit_count=len(units),
        peak_parallelism=peak_parallelism,
    )


@dataclass
class _UnitResult:
    index: int
    output: Any | None
    error: RuntimeErrorInfo | None
    attempts: list[DirectOperatorExecution]


class _CallBudget:
    """Operator-attempt budget shared by cooperative map/replica Tasks."""

    def __init__(self, remaining: int | None) -> None:
        self.remaining = remaining

    def consume(self) -> bool:
        # No await occurs inside this method, so one event loop updates the
        # counter atomically even when many map/replica Tasks share it.
        if self.remaining is None:
            return True
        if self.remaining <= 0:
            return False
        self.remaining -= 1
        return True


class _CallSequence:
    """Allocate unique call numbers across concurrent map/replica tasks."""

    def __init__(self, current: int = 0) -> None:
        self.current = current

    def next(self) -> int:
        # No await occurs here, so tasks on one event loop cannot interleave the
        # read/increment/write sequence.
        self.current += 1
        return self.current


class _OperatorTimedOut(TimeoutError):
    def __init__(self, message: str, *, execution_may_continue: bool) -> None:
        super().__init__(message)
        self.execution_may_continue = execution_may_continue


class _OperatorOutputInvalid(TypeError):
    pass


class _NodeOutputInvalid(TypeError):
    pass


async def _prepare_units(
    job: ResolvedNodeExecutionJob,
) -> tuple[list[tuple[int, Any]], str] | RuntimeErrorInfo:
    policy = job.node_ir.policy
    replication = policy.replication if policy is not None else None
    if job.map_policy is not None and replication is not None:
        return RuntimeErrorInfo(
            code="POLICY_COMBINATION_UNSUPPORTED",
            message="MapPolicy and ReplicationPolicy cannot apply to the same node execution.",
            detail={"node_id": job.node_ir.id},
        )

    if job.map_policy is not None:
        try:
            selected = (
                await invoke_hook_async(
                    job.map_policy.item_selector,
                    _map_selection_context(job),
                )
                if job.map_policy.item_selector is not None
                else deepcopy(job.input)
            )
            units = _map_units(job, selected)
        except Exception as exc:
            return RuntimeErrorInfo(
                code="MAP_ITEM_SELECTION_FAILED",
                message=str(exc),
                detail={"node_id": job.node_ir.id, "error_type": type(exc).__name__},
            )
        if isinstance(units, RuntimeErrorInfo):
            return units
        if (
            policy is not None
            and policy.recovery is not None
            and policy.recovery.mode == "idempotent"
        ):
            units = [
                (
                    index,
                    {
                        **value,
                        "idempotency_key": (
                            f"{job.node_execution.idempotency_key}:{index}"
                        ),
                    },
                )
                for index, value in units
            ]
        return units, "map_item"

    if replication is not None:
        units = [
            (index, deepcopy(job.input))
            for index in range(replication.count)
        ]
        if (
            policy.recovery is not None
            and policy.recovery.mode == "idempotent"
        ):
            units = [
                (
                    index,
                    {
                        **value,
                        "idempotency_key": (
                            f"{job.node_execution.idempotency_key}:{index}"
                        ),
                    },
                )
                for index, value in units
            ]
        return units, "replica"

    return [(0, deepcopy(job.input))], "normal"


def _map_units(
    job: ResolvedNodeExecutionJob,
    selected: Any,
) -> list[tuple[int, dict[str, Any]]] | RuntimeErrorInfo:
    """Materialize MapPolicy output as isolated named-argument mappings.

    The selector owns both fan-out and per-item input construction. Its outer
    result must therefore be an iterable, while every item must be a Mapping
    representing one complete OperatorExecution input. This validation runs before
    any OperatorExecution is created, so selector/data-shaping errors never enter
    retry or capability fallback.
    """

    if isinstance(selected, Mapping):
        return RuntimeErrorInfo(
            code="MAP_ITEM_SELECTION_FAILED",
            message=(
                "MapPolicy item_selector must return an iterable of argument "
                "mappings, not one argument mapping."
            ),
            detail={"node_id": job.node_ir.id},
        )
    try:
        items = list(selected)
    except TypeError as exc:
        return RuntimeErrorInfo(
            code="MAP_ITEM_SELECTION_FAILED",
            message="MapPolicy item_selector result must be iterable.",
            detail={
                "node_id": job.node_ir.id,
                "error_type": type(exc).__name__,
            },
        )

    units: list[tuple[int, dict[str, Any]]] = []
    for item_index, item in enumerate(items):
        if not isinstance(item, Mapping):
            return RuntimeErrorInfo(
                code="MAP_ITEM_INPUT_INVALID",
                message=(
                    "Each MapPolicy item must be a mapping whose keys match "
                    "the target Operator parameters."
                ),
                detail={
                    "node_id": job.node_ir.id,
                    "item_index": item_index,
                    "actual_type": type(item).__name__,
                },
            )
        units.append((item_index, dict(item)))
    return units


def _max_parallelism(job: ResolvedNodeExecutionJob, unit_count: int) -> int:
    limits = [max(1, unit_count)]
    policy = job.node_ir.policy
    if job.map_policy is not None and job.map_policy.max_parallelism is not None:
        limits.append(job.map_policy.max_parallelism)
    if (
        policy is not None
        and policy.replication is not None
        and policy.replication.max_parallelism is not None
    ):
        limits.append(policy.replication.max_parallelism)
    return max(1, min(limits))


def _validate_unit_inputs(
    job: ResolvedNodeExecutionJob,
    units: list[tuple[int, Any]],
) -> RuntimeErrorInfo | None:
    """Validate final unit inputs before creating any operator attempt.

    Node input always represents named Operator arguments. A mismatch against
    the compiled node/capability contract is a data-construction error, so
    execution stops before retry or Operator fallback can begin.
    """

    for unit_position, (unit_index, unit_input) in enumerate(units):
        try:
            if not isinstance(unit_input, Mapping):
                raise TypeError(
                    "Node input must be a mapping whose keys match Operator parameters."
                )
            arguments = dict(unit_input)
            arguments = job.node_ir.input_contract.validate(arguments)
            # Detach hook-owned/custom Mapping objects before they cross the
            # Operator boundary or are retained in runtime execution records.
            units[unit_position] = (unit_index, arguments)
        except (TypeError, ValidationError) as exc:
            is_map_item = job.map_policy is not None
            return RuntimeErrorInfo(
                code=(
                    "MAP_ITEM_INPUT_INVALID"
                    if is_map_item
                    else "INPUT_MAPPING_INVALID"
                ),
                message=str(exc),
                detail={
                    "node_id": job.node_ir.id,
                    "unit_index": unit_index,
                    "expected_schema": job.node_ir.input_contract.json_schema,
                },
            )
    return None


def _validate_operator_output(
    job: ResolvedNodeExecutionJob,
    operator: Operator,
    output: Any,
) -> None:
    """Validate one concrete call against Operator and Capability contracts."""

    contracts = [operator.contract.output, job.node_ir.operator_output_contract]
    checked: set[int] = set()
    for contract in contracts:
        if id(contract) in checked:
            continue
        try:
            contract.validate(output)
        except (TypeError, ValidationError) as exc:
            raise _OperatorOutputInvalid(
                f"Operator {operator.id} returned output that does not satisfy "
                f"contract {contract.json_schema}: {exc}"
            ) from exc
        checked.add(id(contract))


def _validate_final_output(job: ResolvedNodeExecutionJob, output: Any) -> None:
    try:
        job.node_ir.output_contract.validate(output)
    except (TypeError, ValidationError) as exc:
        raise _NodeOutputInvalid(
            f"Node {job.node_ir.id} output does not satisfy final contract "
            f"{job.node_ir.output_contract.json_schema}: {exc}"
        ) from exc


async def _execute_units(
    job: ResolvedNodeExecutionJob,
    units: list[tuple[int, Any]],
    budget: _CallBudget,
    call_sequence: _CallSequence,
    *,
    unit_kind: str,
    max_parallelism: int,
) -> tuple[list[_UnitResult], int]:
    semaphore = asyncio.Semaphore(max_parallelism)
    active = 0
    peak_parallelism = 0

    async def execute(unit_index: int, unit_input: Any) -> _UnitResult:
        nonlocal active, peak_parallelism
        async with semaphore:
            active += 1
            peak_parallelism = max(peak_parallelism, active)
            try:
                return await _execute_unit(
                    job,
                    unit_input,
                    budget,
                    call_sequence,
                    unit_kind=unit_kind,
                    unit_index=unit_index,
                )
            finally:
                active -= 1

    pending = {
        asyncio.create_task(execute(unit_index, unit_input))
        for unit_index, unit_input in units
    }
    results: list[_UnitResult] = []
    try:
        while pending:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )
            completed = [task.result() for task in done]
            results.extend(completed)
            if not any(result.error is not None for result in completed):
                continue
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            pending.clear()
            break
    finally:
        # Workflow cancellation can interrupt this function while map/replica
        # children are still active. Cancel and observe every child Task before
        # releasing the parent NodeExecution task.
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    return results, peak_parallelism


async def _execute_unit(
    job: ResolvedNodeExecutionJob,
    unit_input: Any,
    budget: _CallBudget,
    call_sequence: _CallSequence,
    *,
    unit_kind: str,
    unit_index: int,
) -> _UnitResult:
    attempts: list[DirectOperatorExecution] = []
    retry = job.node_ir.policy.retry if job.node_ir.policy is not None else None
    max_attempts = retry.max_attempts if retry is not None else 1
    for operator_index, operator in enumerate(job.operators):
        for attempt_index in range(max_attempts):
            if not budget.consume():
                return _UnitResult(
                    unit_index,
                    None,
                    _operator_budget_error(job),
                    attempts,
                )
            reason = _attempt_reason(
                operator_index,
                attempt_index,
                recovery=job.recovery,
            )
            attempt = DirectOperatorExecution(
                operator_id=operator.id,
                sequence=call_sequence.next(),
                reason=reason,
                input=deepcopy(unit_input),
            )

            started_ns = perf_counter_ns()
            try:
                output = await _invoke_operator(job, operator, unit_input)
                _validate_operator_output(job, operator, output)
                duration_ms = max(0, (perf_counter_ns() - started_ns) // 1_000_000)
                attempt.resource_usage = ResourceUsage(duration_ms=duration_ms)
                attempt.mark_completed(output)
                attempts.append(attempt)
                return _UnitResult(unit_index, output, None, attempts)
            except Exception as exc:
                duration_ms = max(0, (perf_counter_ns() - started_ns) // 1_000_000)
                error = _operator_error(job, operator, exc)
                attempt.resource_usage = ResourceUsage(duration_ms=duration_ms)
                attempt.mark_failed(error)
                attempts.append(attempt)
                if attempt_index + 1 < max_attempts:
                    await asyncio.sleep(_retry_delay_seconds(retry.backoff, attempt_index))

    return _UnitResult(unit_index, None, attempts[-1].error, attempts)


async def _invoke_operator(
    job: ResolvedNodeExecutionJob,
    operator: Operator,
    input: Any,
) -> Any:
    """Invoke one Operator without blocking the event loop.

    Native coroutine handlers stay on the current loop. Synchronous handlers
    execute in NodeExecutor's shared thread pool. Cancelling or timing out the
    latter stops waiting for its result but cannot terminate Python code that
    already started in the worker thread.
    """

    timeout = job.node_ir.policy.timeout if job.node_ir.policy is not None else None

    async def invoke() -> Any:
        if operator.is_async:
            return await operator.ainvoke(input)

        if job.thread_pool is None:  # pragma: no cover - defensive construction guard.
            raise RuntimeError("NodeExecutor thread pool is unavailable.")
        future = job.thread_pool.submit(operator.invoke, input)
        try:
            # Polling keeps the event loop responsive and works across restricted
            # runtimes where asyncio's cross-thread self-pipe wakeup is blocked.
            while not future.done():
                await asyncio.sleep(0.001)
            output = future.result()
        except asyncio.CancelledError:
            future.cancel()
            raise
        return await output if inspect.isawaitable(output) else output

    try:
        invocation = invoke()
        return (
            await asyncio.wait_for(invocation, timeout.timeout_ms / 1000)
            if timeout is not None
            else await invocation
        )
    except TimeoutError as exc:
        raise _OperatorTimedOut(
            "Operator attempt exceeded TimeoutPolicy.",
            execution_may_continue=not operator.is_async,
        ) from exc


async def _aggregate_unit_results(
    job: ResolvedNodeExecutionJob,
    unit_results: list[_UnitResult],
    *,
    unit_kind: str,
    unit_count: int,
    peak_parallelism: int,
) -> NodeExecutionResult:
    unit_results.sort(key=lambda item: item.index)
    attempts = tuple(
        sorted(
            (attempt for item in unit_results for attempt in item.attempts),
            key=lambda attempt: attempt.sequence,
        )
    )
    duration_ms = sum(
        attempt.resource_usage.duration_ms
        for attempt in attempts
    )
    is_parallel = unit_kind in {"map_item", "replica"}
    parallel_execution = (
        _parallel_execution(
            unit_kind=unit_kind,
            attempts=attempts,
            unit_results=unit_results,
            unit_count=unit_count,
            peak_parallelism=peak_parallelism,
        )
        if is_parallel
        else None
    )
    retained_executions = (
        (parallel_execution,)
        if parallel_execution is not None
        else attempts
    )
    failed = next((item for item in unit_results if item.error is not None), None)
    if failed is not None:
        if parallel_execution is not None:
            parallel_execution.state = "failed"
            parallel_execution.error = failed.error
            parallel_execution.ended_at_ms = max(
                (
                    attempt.ended_at_ms or attempt.started_at_ms
                    for attempt in attempts
                ),
                default=parallel_execution.started_at_ms,
            )
        return NodeExecutionResult(
            node_execution_id=job.node_execution.id,
            state="failed",
            error=failed.error,
            operator_executions=retained_executions,
            resource_usage=ResourceUsage(duration_ms=duration_ms),
        )

    outputs = [deepcopy(item.output) for item in unit_results]
    try:
        if unit_kind == "map_item":
            output = (
                await invoke_hook_async(
                    job.map_policy.output_aggregator,
                    _map_aggregation_context(job, outputs),
                )
                if job.map_policy is not None
                and job.map_policy.output_aggregator is not None
                else outputs
            )
        elif unit_kind == "replica":
            replication = job.node_ir.policy.replication
            output = await invoke_hook_async(
                replication.output_aggregator,
                _replication_aggregation_context(job, outputs),
            )
        else:
            output = outputs[0]
        _validate_final_output(job, output)
    except Exception as exc:
        error = RuntimeErrorInfo(
            code=(
                "NODE_OUTPUT_INVALID"
                if isinstance(exc, _NodeOutputInvalid)
                else "OUTPUT_AGGREGATION_FAILED"
            ),
            message=str(exc),
            detail={"node_id": job.node_ir.id, "error_type": type(exc).__name__},
        )
        if parallel_execution is not None:
            parallel_execution.state = "failed"
            parallel_execution.error = error
        return NodeExecutionResult(
            node_execution_id=job.node_execution.id,
            state="failed",
            error=error,
            operator_executions=retained_executions,
            resource_usage=ResourceUsage(duration_ms=duration_ms),
        )

    if parallel_execution is not None:
        parallel_execution.state = "completed"
    return NodeExecutionResult(
        node_execution_id=job.node_execution.id,
        state="completed",
        output=output,
        operator_executions=retained_executions,
        resource_usage=ResourceUsage(duration_ms=duration_ms),
    )


def _map_selection_context(
    job: ResolvedNodeExecutionJob,
) -> MapItemSelectionContext:
    common = _require_hook_context(job)
    return MapItemSelectionContext(
        invocation_input=common.invocation_input,
        invocation_context=common.invocation_context,
        session_context=common.session_context,
        outputs=common.outputs,
        node_id=job.node_ir.local_id or job.node_ir.id,
        input=deepcopy(job.input),
    )


def _map_aggregation_context(
    job: ResolvedNodeExecutionJob,
    outputs: list[Any],
) -> MapAggregationContext:
    common = _require_hook_context(job)
    return MapAggregationContext(
        invocation_input=common.invocation_input,
        invocation_context=common.invocation_context,
        session_context=common.session_context,
        outputs=common.outputs,
        node_id=job.node_ir.local_id or job.node_ir.id,
        item_outputs=deepcopy(outputs),
    )


def _replication_aggregation_context(
    job: ResolvedNodeExecutionJob,
    outputs: list[Any],
) -> ReplicationAggregationContext:
    common = _require_hook_context(job)
    return ReplicationAggregationContext(
        invocation_input=common.invocation_input,
        invocation_context=common.invocation_context,
        session_context=common.session_context,
        outputs=common.outputs,
        node_id=job.node_ir.local_id or job.node_ir.id,
        replica_outputs=deepcopy(outputs),
    )


def _require_hook_context(
    job: ResolvedNodeExecutionJob,
) -> HookContextSnapshot:
    if job.hook_context is None:
        raise RuntimeError("NodeExecutionJob is missing its Workflow hook context.")
    return job.hook_context.isolate()


def _attempt_reason(
    operator_index: int,
    attempt_index: int,
    *,
    recovery: bool,
) -> str:
    if attempt_index > 0:
        return "retry"
    if operator_index > 0:
        return "fallback"
    if recovery:
        return "recovery"
    return "normal"


def _parallel_execution(
    *,
    unit_kind: str,
    attempts: tuple[DirectOperatorExecution, ...],
    unit_results: list[_UnitResult],
    unit_count: int,
    peak_parallelism: int,
) -> ParallelOperatorExecution:
    durations = [
        attempt.resource_usage.duration_ms
        for attempt in attempts
    ]
    failures = [
        {
            "code": attempt.error.code,
            "message": attempt.error.message,
            "operator_id": attempt.operator_id,
        }
        for attempt in attempts
        if attempt.error is not None
    ][:8]
    completed_units = sum(
        1
        for result in unit_results
        if result.error is None
    )
    failed_units = sum(
        1
        for result in unit_results
        if result.error is not None
    )
    summary = ParallelExecutionSummary(
        call_count=unit_count,
        attempt_count=len(attempts),
        success_count=completed_units,
        failure_count=failed_units,
        cancelled_count=max(0, unit_count - completed_units - failed_units),
        retry_count=sum(
            1 for attempt in attempts if attempt.reason == "retry"
        ),
        fallback_count=sum(
            1 for attempt in attempts if attempt.reason == "fallback"
        ),
        total_duration_ms=sum(durations),
        min_duration_ms=min(durations) if durations else None,
        max_duration_ms=max(durations) if durations else None,
        peak_parallelism=peak_parallelism,
        failure_samples=tuple(failures),
    )
    started_at_ms = min(
        (attempt.started_at_ms for attempt in attempts),
        default=utc_timestamp_ms(),
    )
    ended_at_ms = max(
        (
            attempt.ended_at_ms or attempt.started_at_ms
            for attempt in attempts
        ),
        default=started_at_ms,
    )
    return ParallelOperatorExecution(
        kind="map" if unit_kind == "map_item" else "replication",
        summary=summary,
        operator_ids=tuple(dict.fromkeys(attempt.operator_id for attempt in attempts)),
        state="running",
        started_at_ms=started_at_ms,
        ended_at_ms=ended_at_ms,
    )


def _operator_budget_error(job: ResolvedNodeExecutionJob) -> RuntimeErrorInfo:
    return RuntimeErrorInfo(
        code="RESOURCE_LIMIT_EXCEEDED",
        message="Operator attempt limit exceeded.",
        detail={"node_id": job.node_ir.id, "resource": "operator_executions"},
    )


def _failed_result(
    job: ResolvedNodeExecutionJob,
    error: RuntimeErrorInfo,
) -> NodeExecutionResult:
    return NodeExecutionResult(
        node_execution_id=job.node_execution.id,
        state="failed",
        error=error,
    )


def _retry_delay_seconds(backoff: BackoffPolicy | None, retry_index: int) -> float:
    if backoff is None:
        return 0.0
    if backoff.mode == "fixed":
        delay = float(backoff.initial_delay_ms)
    elif backoff.mode == "linear":
        delay = backoff.initial_delay_ms * (1 + backoff.multiplier * retry_index)
    else:
        delay = backoff.initial_delay_ms * (backoff.multiplier ** retry_index)
    if backoff.max_delay_ms is not None:
        delay = min(delay, backoff.max_delay_ms)
    if backoff.jitter == "full":
        delay = random.uniform(0, delay)
    elif backoff.jitter == "equal":
        delay = random.uniform(delay / 2, delay)
    return delay / 1000


def _operator_error(
    job: ResolvedNodeExecutionJob,
    operator: Operator,
    error: Exception,
) -> RuntimeErrorInfo:
    if isinstance(error, _OperatorTimedOut):
        return RuntimeErrorInfo(
            code="OPERATOR_TIMEOUT",
            message=str(error),
            detail={
                "node_id": job.node_ir.id,
                "operator_id": operator.id,
                "execution_may_continue": error.execution_may_continue,
            },
        )
    if isinstance(error, _OperatorOutputInvalid):
        return RuntimeErrorInfo(
            code="OPERATOR_OUTPUT_INVALID",
            message=str(error),
            detail={
                "node_id": job.node_ir.id,
                "operator_id": operator.id,
            },
        )
    return RuntimeErrorInfo(
        code="OPERATOR_CALL_FAILED",
        message=str(error),
        detail={"node_id": job.node_ir.id, "operator_id": operator.id},
    )
