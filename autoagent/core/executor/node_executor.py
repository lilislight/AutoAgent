from __future__ import annotations

import asyncio
from copy import deepcopy
import inspect
import random
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from threading import Lock
from time import perf_counter_ns
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from autoagent.core.compiler import NodeIR
from autoagent.core.executor.result import (
    NodeExecutionProgress,
    NodeExecutionResult,
    NodePhaseResult,
)
from autoagent.core.operators import (
    CapabilityRegistry,
    Operator,
    OperatorRegistry,
    OperatorResolutionError,
    OperatorResolver,
    StreamingResult,
)
from autoagent.core.operators.streaming import is_raw_stream_result
from autoagent.core.runtime import (
    OperatorCall,
    IncomingOutput,
    InvocationExecutionMailbox,
    MapAggregationContext,
    MapItemSelectionContext,
    NodeExecution,
    OperatorCallSummary,
    ParallelExecutionSummary,
    ResourceUsage,
    ReplicationAggregationContext,
    RuntimeErrorInfo,
    RuntimeEventMode,
    RuntimeConcurrencyController,
    UserEventSpec,
)
from autoagent.core.runtime.context import HookContextSnapshot
from autoagent.core.runtime.hooks import invoke_hook_async
from autoagent.core.runtime.serialization import RuntimeSerializationError
from autoagent.core.runtime.time import utc_timestamp_ms
from autoagent.core.workflow import BackoffPolicy
from autoagent.core.workflow.capability import SystemCommand, WAIT_SYSTEM_COMMAND_ID
from autoagent.core.workflow.user_event import normalize_user_event_mappings


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
    incoming: tuple[IncomingOutput, ...] = ()
    max_operator_attempts: int | None = None
    concurrency_key: str | None = None
    recovery: bool = False
    hook_context: HookContextSnapshot | None = None
    event_mode: RuntimeEventMode = "standard"


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
    incoming: tuple[IncomingOutput, ...] = ()
    max_operator_attempts: int | None = None
    concurrency_key: str | None = None
    recovery: bool = False
    hook_context: HookContextSnapshot | None = None
    event_mode: RuntimeEventMode = "standard"
    concurrency_controller: RuntimeConcurrencyController | None = None
    thread_pool: ThreadPoolExecutor | None = None
    max_parallel_units: int = 8
    publish_progress: Callable[[NodeExecutionProgress], None] | None = None


_USER_EVENT_BATCH_MAX_ITEMS = 32
_USER_EVENT_BATCH_MAX_DELAY_SECONDS = 0.020


class _StreamUserEventBatcher:
    """Batch internal transport without changing individual UserEvent semantics."""

    __slots__ = (
        "_generation",
        "_job",
        "_lock",
        "_loop",
        "_mappings",
        "_operator_call_id",
        "_specs",
    )

    def __init__(
        self,
        job: ResolvedNodeExecutionJob,
        *,
        operator_call_id: UUID,
    ) -> None:
        self._job = job
        self._operator_call_id = operator_call_id
        self._mappings = normalize_user_event_mappings(
            job.node_ir.stream_user_event_mapping
        )
        self._specs: list[UserEventSpec] = []
        self._loop = asyncio.get_running_loop()
        self._lock = Lock()
        self._generation = 0

    def add(self, chunk: Any) -> None:
        if not self._mappings or self._job.publish_progress is None:
            return
        for mapping in self._mappings:
            try:
                data = mapping.transform(deepcopy(chunk))
                if inspect.isawaitable(data):
                    close = getattr(data, "close", None)
                    if callable(close):
                        close()
                    raise TypeError(
                        "UserEventMapping.transform() must be synchronous."
                    )
                if data is None:
                    continue
                spec = UserEventSpec(
                    type=mapping.type,
                    # Own the mapping result until RuntimeStore serializes it;
                    # user code may otherwise mutate a returned container while
                    # this batch is waiting to flush.
                    data=deepcopy(data),
                    node_id=self._job.node_ir.id,
                    node_execution_id=self._job.node_execution.id,
                    workflow_path=self._job.node_ir.workflow_path,
                    operator_call_id=self._operator_call_id,
                )
            except Exception as exc:
                spec = UserEventSpec(
                    type="user_event_mapping_failed",
                    data={
                        "mapping_type": mapping.type,
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                        "source": "stream",
                    },
                    node_id=self._job.node_ir.id,
                    node_execution_id=self._job.node_execution.id,
                    workflow_path=self._job.node_ir.workflow_path,
                    operator_call_id=self._operator_call_id,
                )
            self._append(spec)

    def flush(self) -> None:
        with self._lock:
            specs = self._take_locked()
        self._publish(specs)

    def _append(self, spec: UserEventSpec) -> None:
        schedule_generation: int | None = None
        with self._lock:
            if not self._specs:
                self._generation += 1
                schedule_generation = self._generation
            self._specs.append(spec)
            specs = (
                self._take_locked()
                if len(self._specs) >= _USER_EVENT_BATCH_MAX_ITEMS
                else ()
            )
        if schedule_generation is not None:
            self._loop.call_soon_threadsafe(
                self._schedule_deadline,
                schedule_generation,
            )
        self._publish(specs)

    def _schedule_deadline(self, generation: int) -> None:
        self._loop.call_later(
            _USER_EVENT_BATCH_MAX_DELAY_SECONDS,
            self._flush_generation,
            generation,
        )

    def _flush_generation(self, generation: int) -> None:
        with self._lock:
            specs = (
                self._take_locked()
                if generation == self._generation
                else ()
            )
        self._publish(specs)

    def _take_locked(self) -> tuple[UserEventSpec, ...]:
        if not self._specs:
            return ()
        specs = tuple(self._specs)
        self._specs.clear()
        self._generation += 1
        return specs

    def _publish(self, specs: tuple[UserEventSpec, ...]) -> None:
        if not specs or self._job.publish_progress is None:
            return
        self._job.publish_progress(
            NodeExecutionProgress(
                node_execution_id=self._job.node_execution.id,
                kind="user_event",
                user_event_specs=specs,
            )
        )


class NodeExecutor:
    """Dispatch NodeExecution jobs through one async-first execution path.

    Public contract:
      - submit_batch is non-blocking. It records asyncio Tasks and returns.
      - wait_next_messages wakes for internal progress or a completed job.
      - progress and terminal results return to WorkflowExecutor for state writes.

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
        max_parallel_units: int = 8,
        operator_resolver: OperatorResolver | None = None,
        concurrency_controller: RuntimeConcurrencyController | None = None,
    ) -> None:
        if max_thread_workers < 1 or max_parallel_units < 1:
            raise ValueError("Executor worker limits must be positive.")
        self.thread_pool = ThreadPoolExecutor(max_workers=max_thread_workers)
        self.max_parallel_units = max_parallel_units
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
                mailbox.put_message(_execute_system_command(job))
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
                mailbox.put_message(
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
                mailbox.put_message(
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

            loop = asyncio.get_running_loop()

            def publish_progress(
                message: NodeExecutionProgress,
                *,
                target_loop: asyncio.AbstractEventLoop = loop,
                target_mailbox: InvocationExecutionMailbox = mailbox,
            ) -> None:
                try:
                    if asyncio.get_running_loop() is target_loop:
                        target_mailbox.put_message(message)
                        return
                except RuntimeError:
                    pass
                target_loop.call_soon_threadsafe(
                    target_mailbox.put_message,
                    message,
                )

            resolved_job = ResolvedNodeExecutionJob(
                node_ir=job.node_ir,
                node_execution=job.node_execution,
                operators=operators,
                input=job.input,
                incoming=job.incoming,
                max_operator_attempts=job.max_operator_attempts,
                concurrency_key=job.concurrency_key,
                recovery=job.recovery,
                hook_context=job.hook_context,
                event_mode=job.event_mode,
                concurrency_controller=self.concurrency_controller,
                thread_pool=self.thread_pool,
                max_parallel_units=self.max_parallel_units,
                # UserEvents are independent of RuntimeEvent mode. Runtime
                # progress helpers still suppress trace-only messages in
                # minimal mode.
                publish_progress=publish_progress,
            )
            task = asyncio.create_task(_execute_job(resolved_job))
            mailbox.track(task, job.node_execution.id)
            task.add_done_callback(
                partial(
                    self._finish_task,
                    mailbox,
                    job.node_execution.id,
                )
            )

    def has_running(self, mailbox: InvocationExecutionMailbox) -> bool:
        return mailbox.has_pending()

    def close(self) -> None:
        """Stop accepting synchronous calls without waiting on user code."""

        self.thread_pool.shutdown(wait=False, cancel_futures=True)

    async def wait_next_messages(
        self,
        mailbox: InvocationExecutionMailbox,
    ) -> list[NodeExecutionProgress | NodeExecutionResult]:
        return await mailbox.wait_for_messages()

    def _finish_task(
        self,
        mailbox: InvocationExecutionMailbox,
        node_execution_id: UUID,
        task: asyncio.Task[NodeExecutionResult],
    ) -> None:
        mailbox.finish_task(
            task,
            self._task_result(task, node_execution_id),
        )

    async def abandon(
        self,
        mailbox: InvocationExecutionMailbox,
    ) -> list[NodeExecutionProgress | NodeExecutionResult]:
        return await mailbox.abandon()

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
    if job.node_ir.policy is not None:
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
        arguments = job.node_ir.input_contract.restore(dict(job.input))
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
    waiting_started_ns = perf_counter_ns()
    async with controller.async_slot(key, limit):
        concurrency_wait_ns = max(0, perf_counter_ns() - waiting_started_ns)
        result = await _execute_job_with_slot(job)
        result.resource_usage.concurrency_wait_ns += concurrency_wait_ns
        result.resource_usage.duration_ns += concurrency_wait_ns
        return result


async def _execute_job_with_slot(job: ResolvedNodeExecutionJob) -> NodeExecutionResult:
    prepared = await _prepare_units(job)
    if prepared.error is not None:
        result = _failed_result(job, prepared.error)
        result.phases = prepared.phases
        return result
    units, unit_kind = prepared.units, prepared.unit_kind
    input_error = _validate_unit_inputs(job, units)
    if input_error is not None:
        result = _failed_result(job, input_error)
        result.phases = prepared.phases
        return result
    budget = _CallBudget(job.max_operator_attempts)
    call_sequence = _CallSequence(
        job.node_execution.operator_summary.attempt_count
    )
    operator_started_ns = perf_counter_ns()
    unit_results, peak_parallelism = await _execute_units(
        job,
        units,
        budget,
        call_sequence,
        unit_kind=unit_kind,
        max_parallelism=_max_parallelism(job, len(units)),
    )
    operator_elapsed_ns = max(0, perf_counter_ns() - operator_started_ns)
    return await _aggregate_unit_results(
        job,
        unit_results,
        unit_kind=unit_kind,
        unit_count=len(units),
        peak_parallelism=peak_parallelism,
        operator_elapsed_ns=operator_elapsed_ns,
        phases=prepared.phases,
    )


@dataclass
class _UnitResult:
    index: int
    output: Any | None
    error: RuntimeErrorInfo | None
    attempts: list[OperatorCall]
    retry_backoff_ns: int = 0


@dataclass(frozen=True)
class _PreparedUnits:
    units: list[tuple[int, Any]]
    unit_kind: str
    phases: tuple[NodePhaseResult, ...] = ()
    error: RuntimeErrorInfo | None = None


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
    def __init__(
        self,
        message: str,
        *,
        execution_may_continue: bool,
        stream_metrics: _StreamMetrics | None = None,
    ) -> None:
        super().__init__(message)
        self.execution_may_continue = execution_may_continue
        self.stream_metrics = stream_metrics


class _OperatorOutputInvalid(TypeError):
    pass


class _UnsupportedStreamResult(TypeError):
    pass


@dataclass
class _StreamMetrics:
    chunk_count: int = 0
    consumption_ns: int = 0
    reduction_ns: int = 0


class _StreamExecutionFailed(RuntimeError):
    def __init__(self, message: str, metrics: _StreamMetrics) -> None:
        super().__init__(message)
        self.metrics = metrics


class _StreamConsumptionFailed(_StreamExecutionFailed):
    pass


class _StreamReductionFailed(_StreamExecutionFailed):
    pass


class _NodeOutputInvalid(TypeError):
    pass


@dataclass(frozen=True)
class _OperatorInvocationResult:
    output: Any
    execution_ns: int
    thread_pool_queue_ns: int
    streaming: bool = False
    stream_chunk_count: int = 0
    stream_consumption_ns: int = 0
    stream_reduction_ns: int = 0


def _publish_phase(
    job: ResolvedNodeExecutionJob,
    phase: NodePhaseResult | None,
) -> None:
    if (
        phase is None
        or job.event_mode == "minimal"
        or job.publish_progress is None
    ):
        return
    job.publish_progress(
        NodeExecutionProgress(
            node_execution_id=job.node_execution.id,
            kind="phase",
            phase=deepcopy(phase),
        )
    )


def _publish_operator_call(
    job: ResolvedNodeExecutionJob,
    operator_call: OperatorCall,
) -> None:
    if job.event_mode == "minimal" or job.publish_progress is None:
        return
    job.publish_progress(
        NodeExecutionProgress(
            node_execution_id=job.node_execution.id,
            kind="operator_call",
            operator_call=deepcopy(operator_call),
        )
    )


async def _prepare_units(
    job: ResolvedNodeExecutionJob,
) -> _PreparedUnits:
    policy = job.node_ir.policy
    map_policy = policy.map if policy is not None else None
    replication = policy.replication if policy is not None else None
    if map_policy is not None and replication is not None:
        return _PreparedUnits(
            units=[],
            unit_kind="normal",
            error=RuntimeErrorInfo(
                code="POLICY_COMBINATION_UNSUPPORTED",
                message="MapPolicy and ReplicationPolicy cannot apply to the same node execution.",
                detail={"node_id": job.node_ir.id},
            ),
        )

    if map_policy is not None:
        selection_started_ns = perf_counter_ns()
        try:
            selected = (
                await invoke_hook_async(
                    map_policy.item_selector,
                    _map_selection_context(job),
                )
                if map_policy.item_selector is not None
                else deepcopy(job.input)
            )
            units = _map_units(job, selected)
        except Exception as exc:
            elapsed_ns = max(0, perf_counter_ns() - selection_started_ns)
            phase = (
                NodePhaseResult(
                    name="item_selection.completed",
                    status="failed",
                    elapsed_ns=elapsed_ns,
                    timing={"execution_ns": elapsed_ns},
                )
                if (
                    job.event_mode == "full"
                    and map_policy.item_selector is not None
                )
                else None
            )
            _publish_phase(job, phase)
            return _PreparedUnits(
                units=[],
                unit_kind="map_item",
                error=RuntimeErrorInfo(
                    code="MAP_ITEM_SELECTION_FAILED",
                    message=str(exc),
                    detail={"node_id": job.node_ir.id, "error_type": type(exc).__name__},
                ),
            )
        if isinstance(units, RuntimeErrorInfo):
            elapsed_ns = max(0, perf_counter_ns() - selection_started_ns)
            phase = (
                NodePhaseResult(
                    name="item_selection.completed",
                    status="failed",
                    elapsed_ns=elapsed_ns,
                    timing={"execution_ns": elapsed_ns},
                )
                if (
                    job.event_mode == "full"
                    and map_policy.item_selector is not None
                )
                else None
            )
            _publish_phase(job, phase)
            return _PreparedUnits(
                units=[],
                unit_kind="map_item",
                error=units,
            )
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
        phase = None
        if (
            job.event_mode == "full"
            and map_policy.item_selector is not None
        ):
            elapsed_ns = max(0, perf_counter_ns() - selection_started_ns)
            phase = NodePhaseResult(
                name="item_selection.completed",
                status="completed",
                elapsed_ns=elapsed_ns,
                timing={"execution_ns": elapsed_ns},
            )
        _publish_phase(job, phase)
        return _PreparedUnits(units=units, unit_kind="map_item")

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
        return _PreparedUnits(units=units, unit_kind="replica")

    return _PreparedUnits(
        units=[(0, deepcopy(job.input))],
        unit_kind="normal",
    )


def _map_units(
    job: ResolvedNodeExecutionJob,
    selected: Any,
) -> list[tuple[int, dict[str, Any]]] | RuntimeErrorInfo:
    """Materialize MapPolicy output as isolated named-argument mappings.

    The selector owns both fan-out and per-item input construction. Its outer
    result must therefore be an iterable, while every item must be a Mapping
    representing one complete Operator Call input. This validation runs before
    any Operator Call is created, so selector/data-shaping errors never enter
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
    limits = [max(1, unit_count), job.max_parallel_units]
    policy = job.node_ir.policy
    if (
        policy is not None
        and policy.map is not None
        and policy.map.max_parallelism is not None
    ):
        limits.append(policy.map.max_parallelism)
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
            arguments = job.node_ir.input_contract.restore(arguments)
            # Detach hook-owned/custom Mapping objects before they cross into
            # an Operator or are retained in runtime execution records.
            units[unit_position] = (unit_index, arguments)
        except (TypeError, ValidationError, RuntimeSerializationError) as exc:
            is_map_item = (
                job.node_ir.policy is not None
                and job.node_ir.policy.map is not None
            )
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
        except (TypeError, ValidationError, RuntimeSerializationError) as exc:
            raise _OperatorOutputInvalid(
                f"Operator {operator.id} returned output that does not satisfy "
                f"contract {contract.json_schema}: {exc}"
            ) from exc
        checked.add(id(contract))


def _validate_final_output(job: ResolvedNodeExecutionJob, output: Any) -> Any:
    try:
        validated = job.node_ir.output_contract.validate(output)
        return validated
    except (TypeError, ValidationError, RuntimeSerializationError) as exc:
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
    unit_iterator = iter(units)
    active = 0
    peak_parallelism = 0

    async def worker() -> bool:
        nonlocal active, peak_parallelism
        while True:
            try:
                unit_index, unit_input = next(unit_iterator)
            except StopIteration:
                return False
            active += 1
            peak_parallelism = max(peak_parallelism, active)
            try:
                result = await _execute_unit(
                    job,
                    unit_input,
                    budget,
                    call_sequence,
                    unit_kind=unit_kind,
                    unit_index=unit_index,
                )
            finally:
                active -= 1
            results.append(result)
            if result.error is not None:
                return True

    results: list[_UnitResult] = []
    worker_count = min(max_parallelism, len(units))
    pending = {
        asyncio.create_task(
            worker(),
            name=f"autoagent-unit-worker-{index}",
        )
        for index in range(worker_count)
    }
    try:
        while pending:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not any(task.result() for task in done):
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
    attempts: list[OperatorCall] = []
    retry_backoff_ns = 0
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
                    retry_backoff_ns,
                )
            reason = _attempt_reason(
                operator_index,
                attempt_index,
                recovery=job.recovery,
            )
            attempt = OperatorCall(
                operator_id=operator.id,
                sequence=call_sequence.next(),
                reason=reason,
                kind=(
                    "map"
                    if unit_kind == "map_item"
                    else "replication"
                    if unit_kind == "replica"
                    else "direct"
                ),
                unit_index=(unit_index if unit_kind != "normal" else None),
                unit_attempt_no=attempt_index + 1,
                input=deepcopy(unit_input),
            )

            started_ns = perf_counter_ns()
            try:
                invocation_result = await _invoke_operator(
                    job,
                    operator,
                    unit_input,
                    operator_call_id=attempt.id,
                )
                output = invocation_result.output
                _validate_operator_output(job, operator, output)
                duration_ns = max(0, perf_counter_ns() - started_ns)
                attempt.resource_usage = ResourceUsage(
                    duration_ns=duration_ns,
                    execution_ns=invocation_result.execution_ns,
                    thread_pool_queue_ns=(
                        invocation_result.thread_pool_queue_ns
                    ),
                    stream_consumption_ns=(
                        invocation_result.stream_consumption_ns
                    ),
                    stream_reduction_ns=(
                        invocation_result.stream_reduction_ns
                    ),
                )
                attempt.streaming = invocation_result.streaming
                attempt.stream_chunk_count = (
                    invocation_result.stream_chunk_count
                )
                attempt.mark_completed(output)
                attempts.append(attempt)
                _publish_operator_call(
                    job,
                    attempt,
                )
                return _UnitResult(
                    unit_index,
                    output,
                    None,
                    attempts,
                    retry_backoff_ns,
                )
            except asyncio.CancelledError:
                duration_ns = max(0, perf_counter_ns() - started_ns)
                attempt.resource_usage = ResourceUsage(
                    duration_ns=duration_ns,
                    execution_ns=duration_ns,
                )
                attempt.mark_cancelled(
                    RuntimeErrorInfo(
                        code="OPERATOR_CALL_CANCELLED",
                        message="Operator Call was cancelled before completion.",
                        detail={
                            "node_id": job.node_ir.id,
                            "operator_id": operator.id,
                        },
                    )
                )
                attempts.append(attempt)
                _publish_operator_call(job, attempt)
                if unit_kind == "normal":
                    raise
                return _UnitResult(
                    unit_index,
                    None,
                    attempt.error,
                    attempts,
                    retry_backoff_ns,
                )
            except Exception as exc:
                duration_ns = max(0, perf_counter_ns() - started_ns)
                error = _operator_error(job, operator, exc)
                stream_metrics = (
                    exc.metrics
                    if isinstance(exc, _StreamExecutionFailed)
                    else (
                        exc.stream_metrics
                        if isinstance(exc, _OperatorTimedOut)
                        else None
                    )
                )
                attempt.resource_usage = ResourceUsage(
                    duration_ns=duration_ns,
                    execution_ns=duration_ns,
                    stream_consumption_ns=(
                        stream_metrics.consumption_ns
                        if stream_metrics is not None
                        else 0
                    ),
                    stream_reduction_ns=(
                        stream_metrics.reduction_ns
                        if stream_metrics is not None
                        else 0
                    ),
                )
                attempt.streaming = stream_metrics is not None
                attempt.stream_chunk_count = (
                    stream_metrics.chunk_count
                    if stream_metrics is not None
                    else 0
                )
                attempt.mark_failed(error)
                attempts.append(attempt)
                _publish_operator_call(
                    job,
                    attempt,
                )
                if attempt_index + 1 < max_attempts:
                    backoff_started_ns = perf_counter_ns()
                    await asyncio.sleep(
                        _retry_delay_seconds(retry.backoff, attempt_index)
                    )
                    retry_backoff_ns += max(
                        0,
                        perf_counter_ns() - backoff_started_ns,
                    )

    return _UnitResult(
        unit_index,
        None,
        attempts[-1].error,
        attempts,
        retry_backoff_ns,
    )


async def _await_thread_future(future: Any) -> Any:
    """Await a worker result with bounded wake polling."""

    wrapped = asyncio.wrap_future(future)
    while not wrapped.done():
        tick = asyncio.create_task(asyncio.sleep(0.005))
        try:
            done, _ = await asyncio.wait(
                (wrapped, tick),
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            if not tick.done():
                tick.cancel()
                await asyncio.gather(tick, return_exceptions=True)
        if wrapped in done:
            break
    return wrapped.result()


async def _invoke_operator(
    job: ResolvedNodeExecutionJob,
    operator: Operator,
    input: Any,
    *,
    operator_call_id: UUID,
) -> _OperatorInvocationResult:
    """Invoke one Operator without blocking the event loop.

    Native coroutine handlers stay on the current loop. Synchronous handlers
    execute in NodeExecutor's shared thread pool. Cancelling or timing out the
    latter stops waiting for its result but cannot terminate Python code that
    already started in the worker thread.
    """

    timeout = job.node_ir.policy.timeout if job.node_ir.policy is not None else None

    execution_may_continue = not operator.is_async
    stream_metrics: _StreamMetrics | None = None

    async def invoke() -> _OperatorInvocationResult:
        nonlocal execution_may_continue, stream_metrics
        if operator.is_async:
            started_ns = perf_counter_ns()
            output = await operator.ainvoke(input)
            execution_ns = max(0, perf_counter_ns() - started_ns)
            thread_pool_queue_ns = 0
        else:
            if job.thread_pool is None:  # pragma: no cover
                raise RuntimeError("NodeExecutor thread pool is unavailable.")
            submitted_ns = perf_counter_ns()

            def invoke_sync() -> tuple[Any, int, int]:
                started_ns = perf_counter_ns()
                output = operator.invoke(input)
                ended_ns = perf_counter_ns()
                return output, started_ns, ended_ns

            future = job.thread_pool.submit(invoke_sync)
            try:
                output, worker_started_ns, worker_ended_ns = (
                    await _await_thread_future(future)
                )
            except asyncio.CancelledError:
                future.cancel()
                raise
            execution_ns = max(0, worker_ended_ns - worker_started_ns)
            thread_pool_queue_ns = max(
                0,
                worker_started_ns - submitted_ns,
            )

        if inspect.isawaitable(output):
            awaited_started_ns = perf_counter_ns()
            output = await output
            execution_ns += max(0, perf_counter_ns() - awaited_started_ns)

        if isinstance(output, StreamingResult):
            execution_may_continue = not hasattr(output.source, "__aiter__")
            stream_metrics = _StreamMetrics()
            streamed = await _consume_streaming_result(
                job,
                output,
                stream_metrics,
                operator_call_id=operator_call_id,
            )
            return _OperatorInvocationResult(
                output=streamed.output,
                execution_ns=(
                    execution_ns
                    + streamed.stream_consumption_ns
                    + streamed.stream_reduction_ns
                ),
                thread_pool_queue_ns=(
                    thread_pool_queue_ns + streamed.thread_pool_queue_ns
                ),
                streaming=True,
                stream_chunk_count=streamed.stream_chunk_count,
                stream_consumption_ns=streamed.stream_consumption_ns,
                stream_reduction_ns=streamed.stream_reduction_ns,
            )
        if is_raw_stream_result(output):
            await _close_stream_source(output, suppress_errors=True)
            raise _UnsupportedStreamResult(
                "Operator returned a raw stream. AutoAgent cannot determine "
                "the final Operator output; wrap the stream with "
                "streaming_result(source, reducer=...)."
            )
        return _OperatorInvocationResult(
            output=output,
            execution_ns=execution_ns,
            thread_pool_queue_ns=thread_pool_queue_ns,
        )

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
            execution_may_continue=execution_may_continue,
            stream_metrics=stream_metrics,
        ) from exc


async def _consume_streaming_result(
    job: ResolvedNodeExecutionJob,
    result: StreamingResult[Any, Any],
    metrics: _StreamMetrics,
    *,
    operator_call_id: UUID,
) -> _OperatorInvocationResult:
    source = result.source
    user_events = _StreamUserEventBatcher(
        job,
        operator_call_id=operator_call_id,
    )
    try:
        if hasattr(source, "__aiter__"):
            streamed = await _consume_async_stream(
                source,
                result.reducer,
                metrics,
                user_events=user_events,
            )
        else:
            if job.thread_pool is None:  # pragma: no cover
                raise RuntimeError("NodeExecutor thread pool is unavailable.")

            submitted_ns = perf_counter_ns()
            future = job.thread_pool.submit(
                _consume_sync_stream,
                source,
                result.reducer,
                metrics,
                user_events,
            )
            try:
                streamed, worker_started_ns = await _await_thread_future(future)
            except asyncio.CancelledError:
                future.cancel()
                raise
            streamed = _OperatorInvocationResult(
                output=streamed.output,
                execution_ns=streamed.execution_ns,
                thread_pool_queue_ns=max(0, worker_started_ns - submitted_ns),
                streaming=True,
                stream_chunk_count=streamed.stream_chunk_count,
                stream_consumption_ns=streamed.stream_consumption_ns,
                stream_reduction_ns=streamed.stream_reduction_ns,
            )
        user_events.flush()
        return streamed
    except BaseException as exc:
        user_events.flush()
        _publish_stream_aborted_user_event(
            job,
            error=exc,
            operator_call_id=operator_call_id,
        )
        raise


async def _consume_async_stream(
    source: Any,
    reducer: Any,
    metrics: _StreamMetrics,
    *,
    user_events: _StreamUserEventBatcher,
) -> _OperatorInvocationResult:
    iterator: AsyncIterator[Any] | None = None
    try:
        try:
            iterator = source.__aiter__()
        except Exception as exc:
            raise _StreamConsumptionFailed(str(exc), metrics) from exc
        while True:
            started_ns = perf_counter_ns()
            try:
                chunk = await iterator.__anext__()
            except StopAsyncIteration:
                metrics.consumption_ns += max(
                    0,
                    perf_counter_ns() - started_ns,
                )
                break
            except Exception as exc:
                raise _StreamConsumptionFailed(str(exc), metrics) from exc
            metrics.consumption_ns += max(
                0,
                perf_counter_ns() - started_ns,
            )
            reduction_started_ns = perf_counter_ns()
            try:
                reduced = reducer.add(chunk)
                if inspect.isawaitable(reduced):
                    close = getattr(reduced, "close", None)
                    if callable(close):
                        close()
                    raise TypeError("StreamReducer.add() must be synchronous.")
            except Exception as exc:
                raise _StreamReductionFailed(str(exc), metrics) from exc
            metrics.reduction_ns += max(
                0,
                perf_counter_ns() - reduction_started_ns,
            )
            metrics.chunk_count += 1
            user_events.add(chunk)

        reduction_started_ns = perf_counter_ns()
        try:
            output = reducer.finish()
            if inspect.isawaitable(output):
                close = getattr(output, "close", None)
                if callable(close):
                    close()
                raise TypeError("StreamReducer.finish() must be synchronous.")
        except Exception as exc:
            raise _StreamReductionFailed(str(exc), metrics) from exc
        metrics.reduction_ns += max(
            0,
            perf_counter_ns() - reduction_started_ns,
        )
        return _OperatorInvocationResult(
            output=output,
            execution_ns=metrics.consumption_ns + metrics.reduction_ns,
            thread_pool_queue_ns=0,
            streaming=True,
            stream_chunk_count=metrics.chunk_count,
            stream_consumption_ns=metrics.consumption_ns,
            stream_reduction_ns=metrics.reduction_ns,
        )
    except BaseException:
        if iterator is not None:
            await _close_stream_source(iterator, suppress_errors=True)
        raise


def _consume_sync_stream(
    source: Any,
    reducer: Any,
    metrics: _StreamMetrics,
    user_events: _StreamUserEventBatcher,
) -> tuple[_OperatorInvocationResult, int]:
    worker_started_ns = perf_counter_ns()
    iterator: Iterator[Any] | None = None
    try:
        try:
            iterator = iter(source)
        except Exception as exc:
            raise _StreamConsumptionFailed(str(exc), metrics) from exc
        while True:
            started_ns = perf_counter_ns()
            try:
                chunk = next(iterator)
            except StopIteration:
                metrics.consumption_ns += max(
                    0,
                    perf_counter_ns() - started_ns,
                )
                break
            except Exception as exc:
                raise _StreamConsumptionFailed(str(exc), metrics) from exc
            metrics.consumption_ns += max(
                0,
                perf_counter_ns() - started_ns,
            )
            reduction_started_ns = perf_counter_ns()
            try:
                reduced = reducer.add(chunk)
                if inspect.isawaitable(reduced):
                    close = getattr(reduced, "close", None)
                    if callable(close):
                        close()
                    raise TypeError("StreamReducer.add() must be synchronous.")
            except Exception as exc:
                raise _StreamReductionFailed(str(exc), metrics) from exc
            metrics.reduction_ns += max(
                0,
                perf_counter_ns() - reduction_started_ns,
            )
            metrics.chunk_count += 1
            user_events.add(chunk)

        reduction_started_ns = perf_counter_ns()
        try:
            output = reducer.finish()
            if inspect.isawaitable(output):
                close = getattr(output, "close", None)
                if callable(close):
                    close()
                raise TypeError("StreamReducer.finish() must be synchronous.")
        except Exception as exc:
            raise _StreamReductionFailed(str(exc), metrics) from exc
        metrics.reduction_ns += max(
            0,
            perf_counter_ns() - reduction_started_ns,
        )
        return (
            _OperatorInvocationResult(
                output=output,
                execution_ns=(
                    metrics.consumption_ns + metrics.reduction_ns
                ),
                thread_pool_queue_ns=0,
                streaming=True,
                stream_chunk_count=metrics.chunk_count,
                stream_consumption_ns=metrics.consumption_ns,
                stream_reduction_ns=metrics.reduction_ns,
            ),
            worker_started_ns,
        )
    except BaseException:
        if iterator is not None:
            close = getattr(iterator, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
        raise


async def _close_stream_source(
    source: Any,
    *,
    suppress_errors: bool = False,
) -> None:
    aclose = getattr(source, "aclose", None)
    close = getattr(source, "close", None)
    try:
        if callable(aclose):
            await aclose()
            return
        if callable(close):
            close()
    except Exception:
        if not suppress_errors:
            raise


def _publish_stream_aborted_user_event(
    job: ResolvedNodeExecutionJob,
    *,
    error: BaseException,
    operator_call_id: UUID,
) -> None:
    if (
        job.publish_progress is None
        or job.node_ir.metadata.get("_autoagent_user_event_stream") != "message"
    ):
        return
    visible_error = error.__cause__ or error
    job.publish_progress(
        NodeExecutionProgress(
            node_execution_id=job.node_execution.id,
            kind="user_event",
            user_event_specs=(
                UserEventSpec(
                    type="message_aborted",
                    data={
                        "error_type": type(visible_error).__name__,
                        "message": str(error),
                    },
                    node_id=job.node_ir.id,
                    node_execution_id=job.node_execution.id,
                    workflow_path=job.node_ir.workflow_path,
                    operator_call_id=operator_call_id,
                ),
            ),
        )
    )


async def _aggregate_unit_results(
    job: ResolvedNodeExecutionJob,
    unit_results: list[_UnitResult],
    *,
    unit_kind: str,
    unit_count: int,
    peak_parallelism: int,
    operator_elapsed_ns: int,
    phases: tuple[NodePhaseResult, ...],
) -> NodeExecutionResult:
    unit_results.sort(key=lambda item: item.index)
    attempts = tuple(
        sorted(
            (attempt for item in unit_results for attempt in item.attempts),
            key=lambda attempt: attempt.sequence,
        )
    )
    execution_ns = sum(
        attempt.resource_usage.execution_ns
        for attempt in attempts
    )
    thread_pool_queue_ns = sum(
        attempt.resource_usage.thread_pool_queue_ns
        for attempt in attempts
    )
    retry_backoff_ns = sum(
        item.retry_backoff_ns
        for item in unit_results
    )
    stream_consumption_ns = sum(
        attempt.resource_usage.stream_consumption_ns
        for attempt in attempts
    )
    stream_reduction_ns = sum(
        attempt.resource_usage.stream_reduction_ns
        for attempt in attempts
    )
    resource_usage = ResourceUsage(
        duration_ns=operator_elapsed_ns,
        execution_ns=execution_ns,
        thread_pool_queue_ns=thread_pool_queue_ns,
        retry_backoff_ns=retry_backoff_ns,
        stream_consumption_ns=stream_consumption_ns,
        stream_reduction_ns=stream_reduction_ns,
    )
    is_parallel = unit_kind in {"map_item", "replica"}
    parallel_summary = (
        _parallel_summary(
            unit_kind=unit_kind,
            attempts=attempts,
            unit_results=unit_results,
            unit_count=unit_count,
            peak_parallelism=peak_parallelism,
        )
        if is_parallel
        else None
    )
    operator_summary = _operator_summary(attempts)
    failed = next(
        (
            item
            for item in unit_results
            if item.error is not None
            and item.error.code != "OPERATOR_CALL_CANCELLED"
        ),
        next((item for item in unit_results if item.error is not None), None),
    )
    if failed is not None:
        return NodeExecutionResult(
            node_execution_id=job.node_execution.id,
            state="failed",
            error=failed.error,
            operator_summary=operator_summary,
            parallel_summary=parallel_summary,
            phases=phases,
            resource_usage=resource_usage,
        )

    outputs = [item.output for item in unit_results]
    map_policy = job.node_ir.policy.map if job.node_ir.policy is not None else None
    aggregation_phase: NodePhaseResult | None = None
    aggregation_started_ns = perf_counter_ns()
    # Parallel Calls own their individual values. One aggregation Event owns
    # the resulting logical Node output, including the default list result.
    record_aggregator = is_parallel and job.event_mode == "full"
    try:
        if unit_kind == "map_item":
            output = (
                await invoke_hook_async(
                    map_policy.output_aggregator,
                    _map_aggregation_context(job, outputs),
                )
                if map_policy is not None
                and map_policy.output_aggregator is not None
                else outputs
            )
        elif unit_kind == "replica":
            replication = job.node_ir.policy.replication
            output = (
                await invoke_hook_async(
                    replication.output_aggregator,
                    _replication_aggregation_context(job, outputs),
                )
                if replication.output_aggregator is not None
                else outputs
            )
        else:
            output = outputs[0]
        output = _validate_final_output(job, output)
    except Exception as exc:
        aggregation_elapsed_ns = max(
            0,
            perf_counter_ns() - aggregation_started_ns,
        )
        if record_aggregator:
            aggregation_phase = NodePhaseResult(
                name="aggregation.completed",
                status="failed",
                elapsed_ns=aggregation_elapsed_ns,
                timing={"execution_ns": aggregation_elapsed_ns},
            )
        error = RuntimeErrorInfo(
            code=(
                "NODE_OUTPUT_INVALID"
                if isinstance(exc, _NodeOutputInvalid)
                else "OUTPUT_AGGREGATION_FAILED"
            ),
            message=str(exc),
            detail={"node_id": job.node_ir.id, "error_type": type(exc).__name__},
        )
        _publish_phase(job, aggregation_phase)
        return NodeExecutionResult(
            node_execution_id=job.node_execution.id,
            state="failed",
            error=error,
            operator_summary=operator_summary,
            parallel_summary=parallel_summary,
            phases=phases,
            resource_usage=resource_usage,
        )

    if record_aggregator:
        aggregation_elapsed_ns = max(
            0,
            perf_counter_ns() - aggregation_started_ns,
        )
        aggregation_phase = NodePhaseResult(
            name="aggregation.completed",
            status="completed",
            elapsed_ns=aggregation_elapsed_ns,
            output=deepcopy(output),
            timing={"execution_ns": aggregation_elapsed_ns},
        )
    _publish_phase(job, aggregation_phase)
    return NodeExecutionResult(
        node_execution_id=job.node_execution.id,
        state="completed",
        output=output,
        operator_summary=operator_summary,
        parallel_summary=parallel_summary,
        last_operator_call_id=(
            attempts[-1].id if attempts and not is_parallel else None
        ),
        phases=phases,
        resource_usage=resource_usage,
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
        workflow_path=job.node_ir.workflow_path,
        incoming=tuple(
            IncomingOutput(
                edge_id=item.edge_id,
                source_node_id=item.source_node_id,
                source_execution_id=item.source_execution_id,
                value=deepcopy(item.value),
            )
            for item in job.incoming
        ),
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


def _parallel_summary(
    *,
    unit_kind: str,
    attempts: tuple[OperatorCall, ...],
    unit_results: list[_UnitResult],
    unit_count: int,
    peak_parallelism: int,
) -> ParallelExecutionSummary:
    durations = [
        attempt.resource_usage.duration_ns
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
    cancelled_units = sum(
        1
        for result in unit_results
        if result.error is not None
        and result.error.code == "OPERATOR_CALL_CANCELLED"
    )
    failed_units = sum(
        1
        for result in unit_results
        if result.error is not None
        and result.error.code != "OPERATOR_CALL_CANCELLED"
    )
    summary = ParallelExecutionSummary(
        kind="map" if unit_kind == "map_item" else "replication",
        call_count=unit_count,
        attempt_count=len(attempts),
        success_count=completed_units,
        failure_count=failed_units,
        cancelled_count=max(
            cancelled_units,
            unit_count - completed_units - failed_units,
        ),
        retry_count=sum(
            1 for attempt in attempts if attempt.reason == "retry"
        ),
        fallback_count=sum(
            1 for attempt in attempts if attempt.reason == "fallback"
        ),
        total_duration_ns=sum(durations),
        min_duration_ns=min(durations) if durations else None,
        max_duration_ns=max(durations) if durations else None,
        peak_parallelism=peak_parallelism,
        streaming_call_count=sum(
            1 for attempt in attempts if attempt.streaming
        ),
        stream_chunk_count=sum(
            attempt.stream_chunk_count for attempt in attempts
        ),
        stream_consumption_ns=sum(
            attempt.resource_usage.stream_consumption_ns
            for attempt in attempts
        ),
        stream_reduction_ns=sum(
            attempt.resource_usage.stream_reduction_ns
            for attempt in attempts
        ),
        failure_samples=tuple(failures),
    )
    return summary


def _operator_summary(
    attempts: tuple[OperatorCall, ...],
) -> OperatorCallSummary:
    summary = OperatorCallSummary()
    for attempt in attempts:
        summary.record(attempt)
    return summary


def _operator_budget_error(job: ResolvedNodeExecutionJob) -> RuntimeErrorInfo:
    return RuntimeErrorInfo(
        code="RESOURCE_LIMIT_EXCEEDED",
        message="Operator attempt limit exceeded.",
        detail={"node_id": job.node_ir.id, "resource": "operator_attempts"},
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
    if isinstance(error, _UnsupportedStreamResult):
        return RuntimeErrorInfo(
            code="UNSUPPORTED_STREAM_RESULT",
            message=str(error),
            detail={
                "node_id": job.node_ir.id,
                "operator_id": operator.id,
            },
        )
    if isinstance(error, _StreamConsumptionFailed):
        return RuntimeErrorInfo(
            code="STREAM_CONSUMPTION_FAILED",
            message=str(error),
            detail={
                "node_id": job.node_ir.id,
                "operator_id": operator.id,
            },
        )
    if isinstance(error, _StreamReductionFailed):
        return RuntimeErrorInfo(
            code="STREAM_REDUCTION_FAILED",
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
