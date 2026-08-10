"""Invocation coordinator: the only writer of Runtime state and Event sequence."""

from __future__ import annotations

import asyncio
import copy
import logging
import re
import time
from collections.abc import Mapping
from dataclasses import asdict, replace
from typing import Any
from uuid import UUID, uuid4, uuid5

from ..errors import NodeExecutionLimitExceededError
from ..runtime import (
    AttachedChannel,
    Event,
    EventMode,
    Invocation,
    InvocationState,
    NodeCheckpoint,
    NodeExecution,
    RecoveryCheckpoint,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeState,
    RuntimeSink,
    SchedulerCheckpoint,
    SerializedCheckpoint,
    SerializedEvent,
    Session,
    StateOperation,
    StateOperationBatch,
    UserEvent,
    WaitCheckpoint,
    WaitSnapshot,
    context_path_from_key,
    context_path_key,
    now_ms,
    patch_paths,
    hook_context,
)
from ..runtime.serialization import RuntimeValueCodec
from ..scheduler import NodeExecutionRequest, Scheduler, occurrence_key, scope_key
from ..operators import WaitOperator
from ..workflow import (
    ContextPatch,
    EdgeConditionContext,
    IncomingActivation,
    InputMappingContext,
    NodeIR,
    OutputBindingContext,
    UserEventMapping,
    WorkflowIR,
)
from .node_executor import NodeExecutor, _TimedHookFailure
from .result import NodeExecutionResult, NodePhaseResult


logger = logging.getLogger(__name__)


class InvocationExecution:
    """All heavy mutable state for exactly one active Invocation."""

    def __init__(
        self,
        *,
        workflow: WorkflowIR,
        session: Session,
        invocation: Invocation,
        invocation_input: Any,
        event_mode: EventMode,
        sink: RuntimeSink | None,
        stream: AttachedChannel | None,
        node_executor: NodeExecutor,
        default_max_node_executions: int,
    ) -> None:
        self.workflow = workflow
        self.session = session
        self.invocation = invocation
        self.event_mode = event_mode
        self.sink = sink
        self.stream = stream
        self.node_executor = node_executor
        self.default_max_node_executions = default_max_node_executions

        self.runtime_state = RuntimeState.create(
            workflow_id=workflow.workflow_id,
            workflow_revision_id=workflow.workflow_revision_id,
            session_id=session.id,
            invocation_id=invocation.id,
            event_mode=event_mode.value,
            invocation_input=invocation_input,
            session_context=session.context,
            session_created_at_ms=session.created_at_ms,
            invocation_created_at_ms=invocation.created_at_ms,
        )
        self.session._attach_runtime_state(self.runtime_state)

        self.scheduler = Scheduler(workflow, self.runtime_state)
        self.scheduler.initialize()
        # Entry readiness is part of the Genesis Checkpoint, not a post-Genesis
        # Event delta.
        self.scheduler.take_last_batch()
        self.claimed_waits: set[UUID] = set()
        self.resume_queue: asyncio.Queue[tuple[UUID, Any]] = asyncio.Queue()

        self.boundary = asyncio.Event()
        self.terminal = asyncio.Event()
        self.task: asyncio.Task[None] | None = None
        self.worker_tasks: set[asyncio.Task[NodeExecutionResult]] = set()
        self._worker_context: dict[
            asyncio.Task[NodeExecutionResult], tuple[NodeExecutionRequest, NodeExecution]
        ] = {}
        self.pending_delivery: tuple[SerializedEvent, ...] = ()
        self._deferred_operation_batches: list[StateOperationBatch] = []
        self._event_lock = asyncio.Lock()
        self._terminal_lock = asyncio.Lock()

    def restore(self, checkpoint: RecoveryCheckpoint) -> None:
        runtime_operations = [
            StateOperation(
                "replace", ("session", "context"), checkpoint.session_context
            ),
            StateOperation(
                "replace", ("invocation", "context"), checkpoint.invocation_context
            ),
            StateOperation(
                "replace", ("invocation", "state"), checkpoint.invocation_state
            ),
            StateOperation(
                "replace", ("invocation", "output"), checkpoint.invocation_output
            ),
            StateOperation(
                "replace", ("invocation", "error"), checkpoint.invocation_error
            ),
            StateOperation(
                "replace",
                ("invocation", "runtime_event_sequence"),
                checkpoint.runtime_event_sequence,
            ),
            StateOperation(
                "replace",
                ("invocation", "user_event_sequence"),
                checkpoint.user_event_sequence,
            ),
            StateOperation(
                "replace", ("invocation", "recovery_mode"), True
            ),
        ]
        for path, version in checkpoint.session_path_revisions.items():
            runtime_operations.append(
                StateOperation(
                    "add",
                    (
                        "session",
                        "context_path_revisions",
                        context_path_key(path),
                    ),
                    version,
                )
            )
        for path, version in checkpoint.invocation_path_revisions.items():
            runtime_operations.append(
                StateOperation(
                    "add",
                    (
                        "invocation",
                        "context_path_revisions",
                        context_path_key(path),
                    ),
                    version,
                )
            )
        self.runtime_state.apply(tuple(runtime_operations))
        scheduler = checkpoint.scheduler_state
        self.scheduler.restore(
            ready=copy.deepcopy(scheduler.ready),
            resolutions=copy.deepcopy(scheduler.resolutions),
            scheduled=scheduler.scheduled,
            skipped=scheduler.skipped,
        )
        self.scheduler.take_last_batch()
        restored_executions = {
            item.execution_id: NodeExecution(
                id=item.execution_id,
                node_id=item.node_id,
                scope=tuple(
                    self._loop_iteration(region_id, iteration)
                    for region_id, iteration in item.scope
                ),
                state=item.state,  # type: ignore[arg-type]
                input=copy.deepcopy(item.input),
                error=item.error,
                idempotency_key=item.idempotency_key,
                started_state_version=item.started_state_version,
                restart_session_context=copy.deepcopy(item.restart_session_context),
                restart_invocation_context=copy.deepcopy(
                    item.restart_invocation_context
                ),
            )
            for item in checkpoint.node_states
        }
        for execution_id, output in checkpoint.required_outputs.items():
            execution = restored_executions.get(execution_id)
            if execution is not None:
                execution.output = copy.deepcopy(output)
        restored_waits = {
            wait.id: copy.deepcopy(wait) for wait in checkpoint.waits
        }
        for wait in restored_waits.values():
            self.scheduler.track_active(wait.request)
            self.scheduler.take_last_batch()
        restored_pending = dict(checkpoint.pending_advances)
        durable_operations: list[StateOperation] = [
            StateOperation(
                "replace",
                ("counters", "node_executions"),
                checkpoint.node_execution_counts,
            ),
            StateOperation(
                "replace",
                ("counters", "operator_attempts"),
                checkpoint.operator_attempt_counts,
            ),
            StateOperation(
                "replace",
                ("counters", "operator_runtime_ns"),
                checkpoint.operator_runtime_ns,
            ),
        ]
        durable_operations.extend(
            StateOperation(
                "add",
                ("node_executions", str(execution.id)),
                self._node_state_record(execution),
            )
            for execution in restored_executions.values()
        )
        durable_operations.extend(
            StateOperation(
                "add", ("waits", str(wait.id)), self._wait_state_record(wait)
            )
            for wait in restored_waits.values()
        )
        durable_operations.extend(
            StateOperation(
                "add",
                ("pending_advances", str(execution_id)),
                request.to_record(),
            )
            for execution_id, request in restored_pending.items()
        )
        self.runtime_state.apply(tuple(durable_operations))
        self.runtime_state.restore_state_version(checkpoint.state_version)
        self._publish_waits()

    @property
    def invocation_input(self) -> Any:
        return self.runtime_state.read("invocation", "input")

    @property
    def cancel_requested(self) -> bool:
        return self.runtime_state.read("invocation", "cancel_requested")

    def request_cancel(self) -> None:
        if self.cancel_requested:
            return
        self._defer_operations(
            (
                StateOperation(
                    "replace", ("invocation", "cancel_requested"), True
                ),
            )
        )

    @property
    def runtime_sequence(self) -> int:
        return self.runtime_state.read("invocation", "runtime_event_sequence")

    @property
    def user_sequence(self) -> int:
        return self.runtime_state.read("invocation", "user_event_sequence")

    @property
    def invocation_context(self) -> dict[str, Any]:
        return self.runtime_state.read("invocation", "context")

    @property
    def node_executions(self) -> dict[UUID, NodeExecution]:
        return self._execution_views()[0]

    def _execution_views(
        self,
    ) -> tuple[dict[UUID, NodeExecution], dict[UUID, Any], dict[str, UUID]]:
        executions = {
            UUID(execution_id): self._node_from_state_record(record)
            for execution_id, record in self.runtime_state.read(
                "node_executions"
            ).items()
        }
        outputs = {
            execution_id: execution.output
            for execution_id, execution in executions.items()
            if execution.state == "completed"
        }
        latest: dict[str, NodeExecution] = {}
        for execution in executions.values():
            if execution.state != "completed":
                continue
            previous = latest.get(execution.node_id)
            if (
                previous is None
                or execution.logical_occurrence > previous.logical_occurrence
            ):
                latest[execution.node_id] = execution
        return (
            executions,
            outputs,
            {node_id: execution.id for node_id, execution in latest.items()},
        )

    @property
    def outputs(self) -> dict[UUID, Any]:
        return self._execution_views()[1]

    @property
    def latest_output_ids(self) -> dict[str, UUID]:
        return self._execution_views()[2]

    @property
    def node_execution_counts(self) -> dict[str, int]:
        return self.runtime_state.read("counters", "node_executions")

    @property
    def operator_attempt_counts(self) -> dict[str, int]:
        return self.runtime_state.read("counters", "operator_attempts")

    @property
    def operator_runtime_ns(self) -> dict[str, int]:
        return self.runtime_state.read("counters", "operator_runtime_ns")

    @property
    def waits(self) -> dict[UUID, WaitCheckpoint]:
        return {
            UUID(wait_id): WaitCheckpoint(
                id=UUID(record["id"]),
                node_execution_id=UUID(record["node_execution_id"]),
                request=NodeExecutionRequest.from_record(record["request"]),
                payload=record["payload"],
            )
            for wait_id, record in self.runtime_state.read("waits").items()
        }

    @property
    def pending_advances(self) -> dict[UUID, NodeExecutionRequest]:
        return {
            UUID(execution_id): NodeExecutionRequest.from_record(record)
            for execution_id, record in self.runtime_state.read(
                "pending_advances"
            ).items()
        }

    @property
    def recovery_mode(self) -> bool:
        return self.runtime_state.read("invocation", "recovery_mode")

    @property
    def deferred_error(self) -> RuntimeErrorInfo | None:
        value = self.runtime_state.read("invocation", "deferred_error")
        return RuntimeErrorInfo(**value) if value is not None else None

    @property
    def session_path_revisions(self) -> dict[tuple[str, ...], int]:
        return {
            context_path_from_key(path): version
            for path, version in self.runtime_state.read(
                "session", "context_path_revisions"
            ).items()
        }

    @property
    def invocation_path_revisions(self) -> dict[tuple[str, ...], int]:
        return {
            context_path_from_key(path): version
            for path, version in self.runtime_state.read(
                "invocation", "context_path_revisions"
            ).items()
        }

    @staticmethod
    def _loop_iteration(region_id: str, iteration: int) -> Any:
        from ..scheduler import LoopIteration

        return LoopIteration(region_id, iteration)

    async def run(self, *, recovered: bool = False) -> None:
        try:
            if self.stream is not None:
                await self.stream.wait_started()
            if self.invocation.state is not InvocationState.RUNNING:
                await self._set_invocation_state(InvocationState.RUNNING)

            while True:
                if self.cancel_requested:
                    await self.finish_cancelled()
                    return

                if self.pending_advances:
                    await self._resume_pending_advances()
                    continue
                await self._drain_resume_queue()

                if self.scheduler.ready:
                    requests = self.scheduler.drain_ready()
                    self._defer_scheduler_batch()
                    requests = await self._filter_recovery_requests(requests)
                    executions = await self._start_batch(requests)
                    for request, execution in zip(requests, executions, strict=True):
                        task = asyncio.create_task(
                            self._run_node(request, execution),
                            name=f"autoagent-node-{request.node_id}",
                        )
                        self.worker_tasks.add(task)
                        self._worker_context[task] = (request, execution)
                    continue

                if not self.worker_tasks:
                    if self.waits:
                        await self._set_invocation_state(InvocationState.WAITING)
                        self.boundary.set()
                        if self.stream is not None:
                            await self.stream.finish()
                            self.stream = None
                        return
                    if self.deferred_error is not None:
                        await self._finish_failed_info(self.deferred_error)
                    else:
                        # Complete fan-in may settle every structural Exit as
                        # skipped. With no ready/running/waiting work and no
                        # unresolved Loop boundary, this is a valid empty-output
                        # completion rather than a dead end.
                        await self._finish_completed()
                    return

                resume_task = asyncio.create_task(self.resume_queue.get())
                done, _ = await asyncio.wait(
                    {*self.worker_tasks, resume_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if resume_task in done:
                    wait_id, response = resume_task.result()
                    await self._complete_resumed_node(wait_id, response)
                else:
                    resume_task.cancel()
                    await asyncio.gather(resume_task, return_exceptions=True)
                for task in tuple(done):
                    if task is resume_task:
                        continue
                    self.worker_tasks.discard(task)
                    request, execution = self._worker_context.pop(task)
                    result = task.result()
                    if result.waiting:
                        await self._enter_wait(request, execution, result)
                    else:
                        await self._commit_result(request, execution, result)
                if not self.worker_tasks:
                    self._prune_execution_state()
        except asyncio.CancelledError:
            if self.cancel_requested:
                await self.finish_cancelled()
            else:
                await self._finish_failed(RuntimeError("Invocation coordinator was cancelled."))
        except BaseException as error:
            await self._cancel_worker_tasks()
            await self._finish_failed(error)
        finally:
            if self.invocation.state.terminal and self.stream is not None:
                await self.stream.finish()

    async def announce_recovered_waiting(self) -> None:
        try:
            if self.stream is not None:
                await self.stream.wait_started()
        except BaseException as error:
            self.invocation._update(
                state=InvocationState.FAILED,
                error=RuntimeErrorInfo(type=type(error).__name__, message=str(error)),
                updated_at_ms=now_ms(),
            )
            self.terminal.set()
        finally:
            self.boundary.set()
            if self.stream is not None:
                await self.stream.finish()
                self.stream = None

    def has_runnable_recovery_work(self) -> bool:
        """Whether a restored Checkpoint still has control-flow work to run."""

        return bool(self.scheduler.ready or self.pending_advances)

    async def _filter_recovery_requests(
        self, requests: tuple[NodeExecutionRequest, ...]
    ) -> tuple[NodeExecutionRequest, ...]:
        if not self.recovery_mode:
            return requests
        allowed: list[NodeExecutionRequest] = []
        for request in requests:
            node = self.workflow.node(request.node_id)
            recovery = node.policy.recovery if node.policy and node.policy.recovery else None
            if recovery is not None and recovery.mode != "never" and request.recovery_attempt < recovery.max_attempts:
                allowed.append(
                    NodeExecutionRequest(
                        node_id=request.node_id,
                        scope=request.scope,
                        activations=request.activations,
                        recovery_attempt=request.recovery_attempt + 1,
                    )
                )
                continue
            info = RuntimeErrorInfo(
                type="RecoveryBlockedError",
                message=f"Node {node.id!r} does not permit crash recovery replay.",
            )
            await self._emit_skipped_occurrence(request.node_id, request.scope, "recovery_blocked")
            self.scheduler.skip_outgoing(request.node_id, request.scope)
            self._defer_scheduler_batch()
            if self.workflow.policy.failure.mode == "fail_fast":
                raise RuntimeError(info.message)
            self._set_deferred_error(info)
        return tuple(allowed)

    async def _start_batch(
        self, requests: tuple[NodeExecutionRequest, ...]
    ) -> tuple[NodeExecution, ...]:
        executions: list[NodeExecution] = []
        # Every Node admitted by one Scheduler decision observes the same
        # Context revision baseline. Runtime Event sequence updates emitted
        # while materializing the batch must not make later siblings appear
        # serial, otherwise conflicting parallel Output Bindings can escape
        # detection merely because their running Events were emitted in order.
        batch_state_version = self.runtime_state.state_version
        for request in requests:
            node = self.workflow.node(request.node_id)
            baseline = self._take_recovery_baseline(request)
            count = self.node_execution_counts.get(node.id, 0) + 1
            resource = node.policy.resource if node.policy else None
            allowed = (
                resource.max_node_executions_per_invocation
                if resource
                and resource.max_node_executions_per_invocation is not None
                else self.default_max_node_executions
            )
            if count > allowed:
                raise NodeExecutionLimitExceededError(
                    node_id=node.id,
                    attempted=count,
                    allowed=allowed,
                    scope=scope_key(request.scope),
                )
            execution = NodeExecution(
                node_id=node.id,
                scope=request.scope,
                state="running",
                started_at_ms=now_ms(),
                started_perf_ns=time.perf_counter_ns(),
                logical_occurrence=count,
                idempotency_key=str(
                    uuid5(
                        self.invocation.id,
                        f"{node.id}:{occurrence_key(node.id, request.scope)}:{count}",
                    )
                ),
                started_state_version=(
                    baseline.started_state_version
                    if baseline is not None
                    else batch_state_version
                ),
                restart_session_context=(
                    copy.deepcopy(baseline.restart_session_context)
                    if baseline is not None
                    else copy.deepcopy(self.session.context)
                ),
                restart_invocation_context=(
                    copy.deepcopy(baseline.restart_invocation_context)
                    if baseline is not None
                    else copy.deepcopy(self.invocation_context)
                ),
            )
            counter_path = ("counters", "node_executions", node.id)
            self._apply_operations(
                (
                    StateOperation(
                        "replace" if count > 1 else "add",
                        counter_path,
                        count,
                    ),
                    StateOperation(
                        "add",
                        ("node_executions", str(execution.id)),
                        self._node_state_record(execution),
                    ),
                )
            )
            executions.append(execution)
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=node.id,
                status="running",
                workflow_path=node.workflow_path,
                payload={
                    "node_execution_id": str(execution.id),
                    "scope": self._scope_value(request.scope),
                },
            )
        return tuple(executions)

    def _take_recovery_baseline(
        self, request: NodeExecutionRequest
    ) -> NodeExecution | None:
        if not self.recovery_mode:
            return None
        candidates = [
            execution
            for execution in self.node_executions.values()
            if execution.state in {"ready", "running"}
            and occurrence_key(execution.node_id, execution.scope)
            == request.occurrence
        ]
        if not candidates:
            return None
        baseline = max(
            candidates,
            key=lambda execution: (execution.logical_occurrence, str(execution.id)),
        )
        self._defer_operations(
            (
                StateOperation(
                    "remove", ("node_executions", str(baseline.id))
                ),
            )
        )
        return baseline

    async def _run_node(
        self, request: NodeExecutionRequest, execution: NodeExecution
    ) -> NodeExecutionResult:
        node = self.workflow.node(request.node_id)
        context = self._hook_context(request, execution)
        default_input = self._default_input(request)

        async def progress(kind: str, value: Any) -> None:
            resource = node.policy.resource if node.policy else None
            if kind == "operator_call_started":
                attempted = self.operator_attempt_counts.get(node.id, 0) + 1
                if (
                    resource
                    and resource.max_operator_attempts_per_invocation is not None
                    and attempted > resource.max_operator_attempts_per_invocation
                ):
                    raise RuntimeError(
                        f"Node {node.id!r} Operator attempt limit exceeded."
                    )
                path = ("counters", "operator_attempts", node.id)
                self._apply_operations(
                    (
                        StateOperation(
                            "replace" if attempted > 1 else "add",
                            path,
                            attempted,
                        ),
                    )
                )
            elif kind == "operator_call":
                total_runtime_ns = (
                    self.operator_runtime_ns.get(node.id, 0) + value.duration_ns
                )
                runtime_path = ("counters", "operator_runtime_ns", node.id)
                self._apply_operations(
                    (
                        StateOperation(
                            "replace"
                            if total_runtime_ns != value.duration_ns
                            else "add",
                            runtime_path,
                            total_runtime_ns,
                        ),
                    )
                )
                policy_error: RuntimeError | None = None
                if (
                    resource
                    and resource.max_runtime_ms_per_invocation is not None
                    and total_runtime_ns
                    > resource.max_runtime_ms_per_invocation * 1_000_000
                ):
                    policy_error = RuntimeError(
                        f"Node {node.id!r} Operator runtime limit exceeded."
                    )
                if self.event_mode is not EventMode.MINIMAL:
                    payload = {
                        "node_execution_id": str(value.node_execution_id),
                        "operator_id": value.operator_id,
                        "unit_kind": value.unit_kind,
                        "unit_index": value.unit_index,
                        "attempt": value.attempt,
                        "idempotency_key": value.idempotency_key,
                        "error": value.error,
                        "timing": {
                            "dispatch_wait_ns": value.dispatch_wait_ns,
                            "executor_wait_ns": value.executor_wait_ns,
                            "thread_pool_wait_ns": value.thread_pool_wait_ns,
                            "handler_ns": value.handler_ns,
                            "stream_ns": value.stream_ns,
                            "stream_delivery_ns": value.stream_delivery_ns,
                        },
                    }
                    if self.event_mode is EventMode.FULL:
                        payload.update({"input": value.input, "output": value.output})
                    await self._emit_runtime(
                        event_name="operator_call_finished",
                        subject_type="operator_call",
                        subject_id=str(value.id),
                        status=value.status,
                        workflow_path=node.workflow_path,
                        duration_ns=value.duration_ns,
                        started_at_ms=value.started_at_ms,
                        completed_at_ms=value.completed_at_ms,
                        payload=payload,
                    )
                if policy_error is not None:
                    raise policy_error
            elif kind == "phase":
                phase: NodePhaseResult = value
                await self._emit_phase(node, execution, phase)

        async def stream_chunk(chunk: Any) -> None:
            await self._emit_user_mappings(
                node,
                node.stream_user_event_mappings,
                node.stream_user_event_contracts,
                chunk,
            )

        return await self.node_executor.execute(
            node=node,
            execution=execution,
            default_input=default_input,
            context=context,
            progress=progress,
            stream_chunk=stream_chunk,
            idempotency_key=(
                execution.idempotency_key
                if node.policy
                and node.policy.recovery
                and node.policy.recovery.mode == "idempotent"
                else None
            ),
            capture_operator_io=self.event_mode is EventMode.FULL,
        )

    async def _commit_result(
        self,
        request: NodeExecutionRequest,
        execution: NodeExecution,
        result: NodeExecutionResult,
    ) -> None:
        if result.error is not None:
            await self._commit_failure(request, execution, result, result.error)
            return
        if result.cancelled:
            execution.state = "cancelled"
            return

        try:
            self._commit_context_patch(execution, result.patch)
        except BaseException as error:
            await self._commit_failure(request, execution, result, error)
            return
        execution.input = copy.deepcopy(result.mapped_input)
        execution.output = copy.deepcopy(result.output)
        execution.state = "completed"
        execution.completed_at_ms = now_ms()
        if execution.started_perf_ns is not None:
            execution.duration_ns = max(
                0, time.perf_counter_ns() - execution.started_perf_ns
            )
        await self._emit_deferred_output_binding(
            node=self.workflow.node(execution.node_id),
            execution=execution,
            result=result,
        )
        self._apply_operations(
            (
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "input"),
                    result.mapped_input,
                ),
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "output"),
                    result.output,
                ),
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "state"),
                    "completed",
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "restart_session_context",
                    ),
                    None,
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "restart_invocation_context",
                    ),
                    None,
                ),
                StateOperation(
                    "add",
                    ("pending_advances", str(execution.id)),
                    request.to_record(),
                ),
            )
        )
        await self._emit_runtime(
            event_name="node_state_changed",
            subject_type="node",
            subject_id=execution.node_id,
            status="completed",
            workflow_path=self.workflow.node(execution.node_id).workflow_path,
            payload={"node_execution_id": str(execution.id)},
        )
        self._offer_checkpoint(InvocationState.RUNNING)
        node = self.workflow.node(execution.node_id)
        await self._emit_user_mappings(
            node, node.user_event_mappings, node.user_event_contracts, result.output
        )
        await self._advance_completed(request, execution)

    async def _commit_failure(
        self,
        request: NodeExecutionRequest,
        execution: NodeExecution,
        result: NodeExecutionResult,
        error: BaseException,
    ) -> None:
        node = self.workflow.node(execution.node_id)
        await self._emit_deferred_output_binding(
            node=node,
            execution=execution,
            result=result,
            error=error,
        )
        execution.state = "failed"
        execution.error = f"{type(error).__name__}: {error}"
        execution.completed_at_ms = now_ms()
        self._apply_operations(
            (
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "input"),
                    result.mapped_input,
                ),
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "state"),
                    "failed",
                ),
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "error"),
                    execution.error,
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "restart_session_context",
                    ),
                    None,
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "restart_invocation_context",
                    ),
                    None,
                ),
                StateOperation(
                    "add",
                    ("pending_advances", str(execution.id)),
                    request.to_record(),
                ),
            )
        )
        await self._emit_runtime(
            event_name="node_state_changed",
            subject_type="node",
            subject_id=execution.node_id,
            status="failed",
            workflow_path=node.workflow_path,
            payload={"node_execution_id": str(execution.id), "error": execution.error},
        )
        self._offer_checkpoint(InvocationState.RUNNING)
        if self.workflow.policy.failure.mode == "fail_fast":
            self._remove_pending_advance(execution.id)
            raise error
        skipped_occurrences = self.scheduler.skip_outgoing(
            request.node_id, request.scope
        )
        self._defer_scheduler_batch()
        for skipped in skipped_occurrences:
            await self._emit_skipped_occurrence(
                skipped.node_id, skipped.scope, "upstream_failed"
            )
        self._remove_pending_advance(execution.id)
        self._set_deferred_error(
            RuntimeErrorInfo(type=type(error).__name__, message=str(error))
        )

    async def _emit_deferred_output_binding(
        self,
        *,
        node: NodeIR,
        execution: NodeExecution,
        result: NodeExecutionResult,
        error: BaseException | None = None,
    ) -> None:
        for phase in result.deferred_phases:
            if phase.name != "output_binding_finished":
                await self._emit_phase(node, execution, phase)
                continue
            if error is not None:
                phase = replace(
                    phase,
                    status="failed",
                    payload={"error": f"{type(error).__name__}: {error}"},
                )
            await self._emit_phase(
                node,
                execution,
                phase,
            )
        result.deferred_phases.clear()

    async def _emit_phase(
        self,
        node: NodeIR,
        execution: NodeExecution,
        phase: NodePhaseResult,
    ) -> None:
        await self._emit_runtime(
            event_name=phase.name,
            subject_type="node_phase",
            subject_id=str(execution.id),
            status=phase.status,
            workflow_path=node.workflow_path,
            duration_ns=phase.duration_ns,
            started_at_ms=phase.started_at_ms,
            completed_at_ms=phase.completed_at_ms,
            payload={
                **(phase.payload or {}),
                "timing": {
                    "executor_wait_ns": phase.executor_wait_ns,
                    "thread_pool_wait_ns": phase.thread_pool_wait_ns,
                    "handler_ns": phase.handler_ns,
                },
            },
        )

    async def _advance_completed(
        self, request: NodeExecutionRequest, execution: NodeExecution
    ) -> None:
        decisions: dict[str, bool] = {}
        for edge in self.workflow.outgoing(request.node_id):
            started_at_ms = now_ms()
            started = time.perf_counter_ns()
            timing = {
                "executor_wait_ns": 0,
                "thread_pool_wait_ns": 0,
                "handler_ns": 0,
            }
            try:
                selected = True
                if edge.condition is not None:
                    selected_value, timing = await self.node_executor.call_hook_timed(
                        edge.condition,
                        hook_context(
                            EdgeConditionContext,
                            workflow_id=self.workflow.workflow_id,
                            workflow_revision_id=self.workflow.workflow_revision_id,
                            workflow_path=edge.workflow_path,
                            session_id=self.session.id,
                            invocation_id=str(self.invocation.id),
                            session_context=self.session.context,
                            invocation_context=self.invocation_context,
                            invocation_input=self.invocation_input,
                            edge_id=edge.id,
                            source_node_id=execution.node_id,
                            source_execution_id=str(execution.id),
                            source_scope=execution.scope,
                            output=execution.output,
                        ),
                    )
                    if not isinstance(selected_value, bool):
                        raise TypeError("Edge condition must return bool.")
                    selected = selected_value
            except _TimedHookFailure as failure:
                error = failure.cause
                timing = failure.timing
                completed_at_ms = now_ms()
                duration = max(0, time.perf_counter_ns() - started)
                await self._emit_runtime(
                    event_name="edge_evaluated",
                    subject_type="edge",
                    subject_id=edge.id,
                    status="failed",
                    workflow_path=edge.workflow_path,
                    duration_ns=duration,
                    started_at_ms=started_at_ms,
                    completed_at_ms=completed_at_ms,
                    payload={
                        "error": f"{type(error).__name__}: {error}",
                        "timing": timing,
                    },
                )
                raise error
            except BaseException as error:
                completed_at_ms = now_ms()
                duration = max(0, time.perf_counter_ns() - started)
                await self._emit_runtime(
                    event_name="edge_evaluated",
                    subject_type="edge",
                    subject_id=edge.id,
                    status="failed",
                    workflow_path=edge.workflow_path,
                    duration_ns=duration,
                    started_at_ms=started_at_ms,
                    completed_at_ms=completed_at_ms,
                    payload={
                        "error": f"{type(error).__name__}: {error}",
                        "timing": timing,
                    },
                )
                raise
            completed_at_ms = now_ms()
            duration = max(0, time.perf_counter_ns() - started)
            decisions[edge.id] = selected
            await self._emit_runtime(
                event_name="edge_evaluated",
                subject_type="edge",
                subject_id=edge.id,
                status="selected" if selected else "not_selected",
                workflow_path=edge.workflow_path,
                duration_ns=duration,
                started_at_ms=started_at_ms,
                completed_at_ms=completed_at_ms,
                payload={
                    "source": edge.source,
                    "target": edge.target,
                    "timing": timing,
                },
            )
        skipped_occurrences = self.scheduler.resolve_outgoing(
            request, execution.id, decisions
        )
        # The complete Scheduler transition is emitted with the next semantic
        # Runtime Event after this Edge decision group.
        self._defer_scheduler_batch()
        for skipped in skipped_occurrences:
            await self._emit_skipped_occurrence(skipped.node_id, skipped.scope, "incoming_edges_not_selected")
        self._remove_pending_advance(execution.id)

    async def _resume_pending_advances(self) -> None:
        """Continue control flow from terminal Nodes captured before Edge work."""

        for execution_id, request in tuple(self.pending_advances.items()):
            execution = self.node_executions[execution_id]
            if execution.state == "completed":
                await self._advance_completed(request, execution)
                continue
            if execution.state == "failed":
                message = execution.error or f"Node {execution.node_id!r} failed."
                if self.workflow.policy.failure.mode == "fail_fast":
                    self._remove_pending_advance(execution_id)
                    raise RuntimeError(message)
                self._set_deferred_error(
                    RuntimeErrorInfo(
                        type="RecoveredNodeFailure", message=message
                    )
                )
            skipped_occurrences = self.scheduler.skip_outgoing(
                request.node_id, request.scope
            )
            self._defer_scheduler_batch()
            for skipped in skipped_occurrences:
                await self._emit_skipped_occurrence(
                    skipped.node_id, skipped.scope, "upstream_failed"
                )
            self._remove_pending_advance(execution_id)

    async def _enter_wait(
        self,
        request: NodeExecutionRequest,
        execution: NodeExecution,
        result: NodeExecutionResult,
    ) -> None:
        assert result.waiting
        execution.input = copy.deepcopy(result.mapped_input)
        execution.state = "waiting"
        wait = WaitCheckpoint(
            id=uuid4(),
            node_execution_id=execution.id,
            request=request,
            payload=copy.deepcopy(result.wait_payload),
        )
        self._apply_operations(
            (
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "input"),
                    result.mapped_input,
                ),
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "state"),
                    "waiting",
                ),
                StateOperation(
                    "add",
                    ("waits", str(wait.id)),
                    self._wait_state_record(wait),
                ),
            )
        )
        self._publish_waits()
        await self._emit_runtime(
            event_name="node_state_changed",
            subject_type="node",
            subject_id=execution.node_id,
            status="waiting",
            workflow_path=self.workflow.node(execution.node_id).workflow_path,
            payload={
                "node_execution_id": str(execution.id),
                "wait_id": str(wait.id),
                "wait_payload": result.wait_payload,
            },
        )
        checkpoint_state = (
            InvocationState.RUNNING
            if self.worker_tasks or self.scheduler.ready or self.pending_advances
            else InvocationState.WAITING
        )
        self._offer_checkpoint(checkpoint_state)
    async def _complete_resumed_node(self, wait_id: UUID, response: Any) -> None:
        wait = self.waits[wait_id]
        execution = self.node_executions[wait.node_execution_id]
        node = self.workflow.node(execution.node_id)
        if not isinstance(node.operator, WaitOperator):
            raise RuntimeError("A Wait Checkpoint must reference a WaitOperator Node.")
        output = node.operator.response_contract.validate(
            copy.deepcopy(response)
        )
        RuntimeValueCodec.encode(output)
        execution.state = "running"
        execution.restart_session_context = None
        execution.restart_invocation_context = None
        execution.started_state_version = self.runtime_state.state_version + 1
        self._apply_operations(
            (
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "state"),
                    "running",
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "restart_session_context",
                    ),
                    None,
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "restart_invocation_context",
                    ),
                    None,
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "started_state_version",
                    ),
                    execution.started_state_version,
                ),
            )
        )
        await self._emit_runtime(
            event_name="node_state_changed",
            subject_type="node",
            subject_id=node.id,
            status="running",
            workflow_path=node.workflow_path,
            payload={
                "node_execution_id": str(execution.id),
                "wait_id": str(wait_id),
                "resumed": True,
            },
        )
        binding_started_at_ms = now_ms()
        binding_started = time.perf_counter_ns()
        binding_timing = {
            "executor_wait_ns": 0,
            "thread_pool_wait_ns": 0,
            "handler_ns": 0,
        }
        try:
            # Resume is a new Node execution segment. It observes the latest
            # committed Session/Invocation Context, not the pre-Wait restart
            # baseline used only for crash replay of an in-flight Node.
            base_context = self._hook_context(wait.request, execution)
            binding_context = self.node_executor._derived_context(
                OutputBindingContext,
                base_context,
                input=execution.input,
                output=output,
            )
            if node.output_binding is None:
                patch = ContextPatch()
            else:
                patch, binding_timing = await self.node_executor.call_hook_timed(
                    node.output_binding, binding_context
                )
                if patch is None:
                    patch = ContextPatch()
                if not isinstance(patch, ContextPatch):
                    raise TypeError("Output Binding must return ContextPatch or None.")
                RuntimeValueCodec.encode(dict(patch.session))
                RuntimeValueCodec.encode(dict(patch.invocation))
            self._commit_context_patch(execution, patch)
        except _TimedHookFailure as failure:
            error = failure.cause
            binding_timing = failure.timing
            completed_at_ms = now_ms()
            duration = max(0, time.perf_counter_ns() - binding_started)
            if node.output_binding is not None:
                await self._emit_runtime(
                    event_name="output_binding_finished",
                    subject_type="node_phase",
                    subject_id=str(execution.id),
                    status="failed",
                    workflow_path=node.workflow_path,
                    duration_ns=duration,
                    started_at_ms=binding_started_at_ms,
                    completed_at_ms=completed_at_ms,
                    payload={
                        "error": f"{type(error).__name__}: {error}",
                        "timing": binding_timing,
                    },
                )
            execution.state = "failed"
            execution.error = f"{type(error).__name__}: {error}"
            self._apply_operations(
                (
                    StateOperation(
                        "replace",
                        ("node_executions", str(execution.id), "state"),
                        "failed",
                    ),
                    StateOperation(
                        "replace",
                        ("node_executions", str(execution.id), "error"),
                        execution.error,
                    ),
                    StateOperation(
                        "replace",
                        (
                            "node_executions",
                            str(execution.id),
                            "restart_session_context",
                        ),
                        None,
                    ),
                    StateOperation(
                        "replace",
                        (
                            "node_executions",
                            str(execution.id),
                            "restart_invocation_context",
                        ),
                        None,
                    ),
                )
            )
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=node.id,
                status="failed",
                workflow_path=node.workflow_path,
                payload={"node_execution_id": str(execution.id), "error": execution.error},
            )
            self._offer_checkpoint(InvocationState.RUNNING)
            raise error
        except BaseException as error:
            completed_at_ms = now_ms()
            duration = max(0, time.perf_counter_ns() - binding_started)
            if node.output_binding is not None:
                await self._emit_runtime(
                    event_name="output_binding_finished",
                    subject_type="node_phase",
                    subject_id=str(execution.id),
                    status="failed",
                    workflow_path=node.workflow_path,
                    duration_ns=duration,
                    started_at_ms=binding_started_at_ms,
                    completed_at_ms=completed_at_ms,
                    payload={
                        "error": f"{type(error).__name__}: {error}",
                        "timing": binding_timing,
                    },
                )
            execution.state = "failed"
            execution.error = f"{type(error).__name__}: {error}"
            self._apply_operations(
                (
                    StateOperation(
                        "replace",
                        ("node_executions", str(execution.id), "state"),
                        "failed",
                    ),
                    StateOperation(
                        "replace",
                        ("node_executions", str(execution.id), "error"),
                        execution.error,
                    ),
                    StateOperation(
                        "replace",
                        (
                            "node_executions",
                            str(execution.id),
                            "restart_session_context",
                        ),
                        None,
                    ),
                    StateOperation(
                        "replace",
                        (
                            "node_executions",
                            str(execution.id),
                            "restart_invocation_context",
                        ),
                        None,
                    ),
                )
            )
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=node.id,
                status="failed",
                workflow_path=node.workflow_path,
                payload={
                    "node_execution_id": str(execution.id),
                    "error": execution.error,
                },
            )
            self._offer_checkpoint(InvocationState.RUNNING)
            raise
        execution.output = output
        execution.state = "completed"
        execution.completed_at_ms = now_ms()
        self.claimed_waits.discard(wait_id)
        self._apply_operations(
            (
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "output"),
                    output,
                ),
                StateOperation(
                    "replace",
                    ("node_executions", str(execution.id), "state"),
                    "completed",
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "restart_session_context",
                    ),
                    None,
                ),
                StateOperation(
                    "replace",
                    (
                        "node_executions",
                        str(execution.id),
                        "restart_invocation_context",
                    ),
                    None,
                ),
                StateOperation("remove", ("waits", str(wait_id))),
                StateOperation(
                    "add",
                    ("pending_advances", str(execution.id)),
                    wait.request.to_record(),
                ),
            )
        )
        self._publish_waits()
        if node.output_binding is not None:
            completed_at_ms = now_ms()
            duration = max(0, time.perf_counter_ns() - binding_started)
            await self._emit_runtime(
                event_name="output_binding_finished",
                subject_type="node_phase",
                subject_id=str(execution.id),
                status="completed",
                workflow_path=node.workflow_path,
                duration_ns=duration,
                started_at_ms=binding_started_at_ms,
                completed_at_ms=completed_at_ms,
                payload={"patch": patch, "timing": binding_timing},
            )
        await self._emit_runtime(
            event_name="node_state_changed",
            subject_type="node",
            subject_id=node.id,
            status="completed",
            workflow_path=node.workflow_path,
            payload={
                "node_execution_id": str(execution.id),
                "wait_id": str(wait_id),
                "resumed": True,
            },
        )
        self._offer_checkpoint(InvocationState.RUNNING)
        await self._emit_user_mappings(
            node, node.user_event_mappings, node.user_event_contracts, output
        )
        await self._advance_completed(wait.request, execution)

    async def _drain_resume_queue(self) -> None:
        while True:
            try:
                wait_id, response = self.resume_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            await self._complete_resumed_node(wait_id, response)

    def claim_wait(self, wait_id: UUID, response: Any) -> None:
        if wait_id not in self.waits:
            raise KeyError(wait_id)
        if wait_id in self.claimed_waits:
            raise RuntimeError("Wait is already being resumed.")
        self.claimed_waits.add(wait_id)
        self.resume_queue.put_nowait((wait_id, copy.deepcopy(response)))

    def _publish_waits(self) -> None:
        snapshots = tuple(
            WaitSnapshot(
                id=wait.id,
                node_id=self.node_executions[wait.node_execution_id].node_id,
                node_execution_id=wait.node_execution_id,
                payload=copy.deepcopy(wait.payload),
            )
            for wait in self.waits.values()
        )
        self.invocation._update(waits=snapshots, updated_at_ms=now_ms())

    def _clear_waits(self) -> None:
        wait_ids = tuple(self.waits)
        self.claimed_waits.clear()
        if wait_ids:
            self._defer_operations(
                tuple(
                    StateOperation("remove", ("waits", str(wait_id)))
                    for wait_id in wait_ids
                )
            )
        self._publish_waits()

    async def _cancel_worker_tasks(self) -> None:
        pending = tuple(task for task in self.worker_tasks if not task.done())
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.worker_tasks.clear()
        self._worker_context.clear()

    async def _emit_skipped_occurrence(
        self, node_id: str, scope: Any, reason: str
    ) -> None:
        await self._emit_runtime(
            event_name="node_state_changed",
            subject_type="node",
            subject_id=node_id,
            status="skipped",
            workflow_path=self.workflow.node(node_id).workflow_path,
            payload={"scope": self._scope_value(scope), "reason": reason},
        )
        self._offer_checkpoint(self.invocation.state)

    async def _finish_completed(self) -> None:
        output = {
            node_id: copy.deepcopy(self.outputs[execution_id])
            for node_id in self.workflow.exit_node_ids
            if (execution_id := self.latest_output_ids.get(node_id)) in self.outputs
        }
        await self._set_invocation_state(InvocationState.COMPLETED, output=output)
        self.boundary.set()
        self.terminal.set()

    async def _finish_failed(self, error: BaseException) -> None:
        await self._finish_failed_info(
            RuntimeErrorInfo(type=type(error).__name__, message=str(error))
        )

    async def _finish_failed_info(self, info: RuntimeErrorInfo) -> None:
        async with self._terminal_lock:
            if self.invocation.state.terminal:
                return
            await self._finish_remaining_nodes("invocation_failed")
            self._clear_waits()
            self._clear_pending_advances()
            await self._set_invocation_state(InvocationState.FAILED, error=info)
            self.boundary.set()
            self.terminal.set()

    async def finish_cancelled(self) -> None:
        async with self._terminal_lock:
            if self.terminal.is_set():
                return
            self.request_cancel()
            for task in tuple(self.worker_tasks):
                task.cancel()
            if self.invocation.state is not InvocationState.CANCELLED:
                self.invocation._update(
                    state=InvocationState.CANCELLED,
                    updated_at_ms=now_ms(),
                )
            try:
                await self._finish_remaining_nodes("invocation_cancelled")
                self._clear_waits()
                self._clear_pending_advances()
                await self._set_invocation_state(InvocationState.CANCELLED)
            except asyncio.CancelledError:
                self._clear_waits()
            self.boundary.set()
            self.terminal.set()
            if self.stream is not None:
                self.stream.abandon()

    async def _finish_remaining_nodes(self, reason: str) -> None:
        self.scheduler.abort()
        self._defer_scheduler_batch()
        for execution in tuple(self.node_executions.values()):
            if execution.state not in {"ready", "running", "waiting"}:
                continue
            execution.state = "cancelled"
            self._apply_operations(
                (
                    StateOperation(
                        "replace",
                        ("node_executions", str(execution.id), "state"),
                        "cancelled",
                    ),
                    StateOperation(
                        "replace",
                        (
                            "node_executions",
                            str(execution.id),
                            "restart_session_context",
                        ),
                        None,
                    ),
                    StateOperation(
                        "replace",
                        (
                            "node_executions",
                            str(execution.id),
                            "restart_invocation_context",
                        ),
                        None,
                    ),
                )
            )
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=execution.node_id,
                status="cancelled",
                workflow_path=self.workflow.node(execution.node_id).workflow_path,
                payload={"node_execution_id": str(execution.id), "reason": reason},
            )
            self._offer_checkpoint(self.invocation.state)
        touched = {execution.node_id for execution in self.node_executions.values()}
        for node in self.workflow.nodes:
            already_skipped = any(
                key == node.id or key.startswith(f"{node.id}@")
                for key in self.scheduler.skipped
            )
            if node.id in touched or already_skipped:
                continue
            self.scheduler.mark_skipped(node.id)
            self._defer_scheduler_batch()
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=node.id,
                status="skipped",
                workflow_path=node.workflow_path,
                payload={"reason": reason},
            )
            self._offer_checkpoint(self.invocation.state)

    async def _set_invocation_state(
        self,
        state: InvocationState,
        *,
        output: dict[str, Any] | None = None,
        error: RuntimeErrorInfo | None = None,
    ) -> None:
        timestamp = now_ms()
        operations = [
            StateOperation("replace", ("invocation", "state"), state.value),
            StateOperation(
                "replace", ("invocation", "updated_at_ms"), timestamp
            ),
            StateOperation("replace", ("session", "updated_at_ms"), timestamp),
        ]
        if output is not None:
            operations.append(
                StateOperation("replace", ("invocation", "output"), output)
            )
        if error is not None:
            operations.append(
                StateOperation(
                    "replace", ("invocation", "error"), asdict(error)
                )
            )
        self._apply_operations(tuple(operations))
        self.session.updated_at_ms = timestamp
        if not state.terminal:
            self.invocation._update(
                state=state, output=output, error=error, updated_at_ms=timestamp
            )
        await self._emit_runtime(
            event_name="invocation_state_changed",
            subject_type="invocation",
            subject_id=str(self.invocation.id),
            status=state.value,
            payload={"output": output, "error": asdict(error) if error else None},
        )
        if state.terminal:
            self._offer_checkpoint(state)
        if state.terminal:
            self.invocation._update(
                state=state, output=output, error=error, updated_at_ms=timestamp
            )

    async def _emit_runtime(
        self,
        *,
        event_name: str,
        subject_type: str,
        subject_id: str,
        status: str | None = None,
        workflow_path: tuple[str, ...] = (),
        duration_ns: int | None = None,
        started_at_ms: int | None = None,
        completed_at_ms: int | None = None,
        payload: Any = None,
    ) -> None:
        if self.event_mode is EventMode.MINIMAL:
            return
        if self.sink is None and (
            self.stream is None
            or self.stream.event_channel not in {"runtime", "all"}
        ):
            return
        if self.event_mode is EventMode.STANDARD:
            if isinstance(payload, dict) and subject_type in {
                "node_phase",
                "operator_call",
            }:
                payload = {
                    key: value
                    for key, value in payload.items()
                    if key
                    not in {"input", "output", "items", "patch", "wait_payload"}
                }
            elif subject_type == "node" and isinstance(payload, dict):
                payload = {
                    key: value
                    for key, value in payload.items()
                    if key != "wait_payload"
                }
            elif subject_type == "invocation" and isinstance(payload, dict):
                payload = {"error": payload.get("error")}
        async with self._event_lock:
            deferred_batches = tuple(self._deferred_operation_batches)
            sequence = self.runtime_sequence + 1
            sequence_batch = self.runtime_state.apply(
                (
                    StateOperation(
                        "replace",
                        ("invocation", "runtime_event_sequence"),
                        sequence,
                    ),
                )
            )
            try:
                event_operation_batches = (
                    (
                        *deferred_batches,
                        sequence_batch,
                    )
                    if self.event_mode is EventMode.FULL
                    else ()
                )
                event = RuntimeEvent.detached(
                    workflow_id=self.workflow.workflow_id,
                    workflow_revision_id=self.workflow.workflow_revision_id,
                    session_id=self.session.id,
                    invocation_id=self.invocation.id,
                    sequence=sequence,
                    event_name=event_name,
                    subject_type=subject_type,
                    subject_id=subject_id,
                    status=status,
                    workflow_path=workflow_path,
                    duration_ns=duration_ns,
                    started_at_ms=started_at_ms,
                    completed_at_ms=completed_at_ms,
                    payload=payload,
                    operation_batches=(
                        event_operation_batches
                        if self.event_mode is EventMode.FULL
                        and event_operation_batches
                        else ()
                    ),
                )
                serialized = (
                    SerializedEvent.from_event(event) if self.sink is not None else None
                )
            except BaseException:
                if self.event_mode is EventMode.FULL:
                    # Business state was already committed. Keep every batch
                    # plus the failed Event's sequence change so a later Event
                    # can close the replay gap in exact state-version order.
                    self._deferred_operation_batches.append(sequence_batch)
                logger.exception(
                    "Runtime Event capture failed; execution continues with a sequence gap."
                )
                return
            if deferred_batches:
                del self._deferred_operation_batches[: len(deferred_batches)]
            await self._deliver(event, serialized)

    async def _emit_user_mappings(
        self,
        node: NodeIR,
        mappings: tuple[UserEventMapping, ...],
        contracts: tuple[Any, ...],
        value: Any,
    ) -> None:
        if self.sink is None and (
            self.stream is None or self.stream.event_channel not in {"user", "all"}
        ):
            return
        for mapping, contract in zip(mappings, contracts, strict=True):
            try:
                data = await self.node_executor.call_hook(
                    mapping.transform, copy.deepcopy(value)
                )
                data = contract.validate(data)
                if data is None:
                    continue
                RuntimeValueCodec.encode(data)
                event_type = mapping.type.strip()
                if re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", event_type) is None:
                    raise ValueError("UserEvent type must be lowercase snake_case.")
            except BaseException as error:
                event_type = "user_event_mapping_failed"
                data = {
                    "mapping_type": mapping.type,
                    "error": f"{type(error).__name__}: {error}",
                }
            async with self._event_lock:
                sequence = self.user_sequence + 1
                self._apply_operations(
                    (
                        StateOperation(
                            "replace",
                            ("invocation", "user_event_sequence"),
                            sequence,
                        ),
                    )
                )
                try:
                    event = UserEvent.detached(
                        workflow_id=self.workflow.workflow_id,
                        workflow_revision_id=self.workflow.workflow_revision_id,
                        session_id=self.session.id,
                        invocation_id=self.invocation.id,
                        sequence=sequence,
                        type=event_type,
                        data=data,
                        node_id=node.id,
                        workflow_path=node.workflow_path,
                    )
                    serialized = (
                        SerializedEvent.from_event(event)
                        if self.sink is not None
                        else None
                    )
                except BaseException as error:
                    logger.exception("User Event capture failed; attempting a gap Event.")
                    try:
                        event = UserEvent(
                            workflow_id=self.workflow.workflow_id,
                            workflow_revision_id=self.workflow.workflow_revision_id,
                            session_id=self.session.id,
                            invocation_id=self.invocation.id,
                            sequence=sequence,
                            type="user_event_capture_failed",
                            data={
                                "original_type": event_type,
                                "error_type": type(error).__name__,
                            },
                            node_id=node.id,
                            workflow_path=node.workflow_path,
                        )
                        serialized = (
                            SerializedEvent.from_event(event)
                            if self.sink is not None
                            else None
                        )
                    except BaseException:
                        logger.exception(
                            "User Event gap capture also failed; stream has a sequence gap."
                        )
                        continue
                await self._deliver(event, serialized)

    async def _deliver(
        self, event: Event, serialized: SerializedEvent | None
    ) -> None:
        if self.sink is not None:
            if serialized is None:
                logger.error(
                    "RuntimeSink was attached without a serialized Event; "
                    "detaching it from this Invocation."
                )
                self.sink = None
            else:
                self.pending_delivery = (*self.pending_delivery, serialized)
                try:
                    await self.sink.submit_events(self.pending_delivery)
                except asyncio.CancelledError:
                    # Cancellation is control flow, not a broken Sink.  Retain the
                    # delivery slot so cancellation convergence can submit the
                    # interrupted Event together with the ordered cancel Events.
                    raise
                except BaseException:
                    logger.exception(
                        "RuntimeSink violated the in-memory acceptance contract; "
                        "detaching it from this Invocation."
                    )
                    self.sink = None
                self.pending_delivery = ()
        if self.stream is not None:
            await self.stream.publish(event)

    def _offer_checkpoint(self, state: InvocationState) -> None:
        try:
            checkpoint = self._checkpoint(state)
            self.invocation._update(checkpoint=checkpoint, updated_at_ms=now_ms())
            if self.sink is not None:
                self.sink.offer_checkpoint(
                    SerializedCheckpoint.from_checkpoint(checkpoint)
                )
        except BaseException:
            logger.exception(
                "Checkpoint capture or acceptance failed; recoverability is degraded."
            )

    def _checkpoint(self, state: InvocationState) -> RecoveryCheckpoint:
        executions, outputs, latest_output_ids = self._execution_views()
        running_requests = tuple(
            request
            for task, (request, _) in self._worker_context.items()
            if not task.done()
        )
        latest_execution_ids = set(latest_output_ids.values())
        required_ids = set(latest_execution_ids)
        for request in (*self.scheduler.ready, *running_requests):
            required_ids.update(item.source_execution_id for item in request.activations)
        for resolution in self.scheduler.resolutions.values():
            if resolution.activation is not None:
                required_ids.add(resolution.activation.source_execution_id)
        for wait in self.waits.values():
            required_ids.add(wait.node_execution_id)
            required_ids.update(
                item.source_execution_id for item in wait.request.activations
            )
        required_ids.update(self.pending_advances)
        required_ids.update(executions)
        latest_nodes = {
            execution_id: executions[execution_id]
            for execution_id in required_ids
            if execution_id in executions
        }
        return RecoveryCheckpoint.detached(
            schema_version=1,
            workflow_id=self.workflow.workflow_id,
            workflow_revision_id=self.workflow.workflow_revision_id,
            session_id=self.session.id,
            invocation_id=self.invocation.id,
            invocation_state=state.value,
            state_version=self.runtime_state.state_version,
            runtime_event_sequence=self.runtime_sequence,
            user_event_sequence=self.user_sequence,
            invocation_input=self.invocation_input,
            invocation_output=self.runtime_state.read("invocation", "output"),
            invocation_error=self.runtime_state.read("invocation", "error"),
            session_context=self.session.context,
            invocation_context=self.invocation_context,
            session_path_revisions=self.session_path_revisions,
            invocation_path_revisions=self.invocation_path_revisions,
            scheduler_state=SchedulerCheckpoint(
                ready=(*tuple(self.scheduler.ready), *running_requests),
                resolutions=tuple(self.scheduler.resolutions.items()),
                scheduled=tuple(sorted(self.scheduler.scheduled)),
                skipped=tuple(sorted(self.scheduler.skipped)),
            ),
            node_states=tuple(
                NodeCheckpoint(
                    execution_id=execution.id,
                    node_id=execution.node_id,
                    scope=tuple(
                        (item.loop_region_id, item.iteration) for item in execution.scope
                    ),
                    state=execution.state,
                    input=execution.input,
                    error=execution.error,
                    idempotency_key=execution.idempotency_key,
                    started_state_version=execution.started_state_version,
                    restart_session_context=execution.restart_session_context,
                    restart_invocation_context=execution.restart_invocation_context,
                )
                for execution in latest_nodes.values()
            ),
            required_outputs={
                execution_id: outputs[execution_id]
                for execution_id in required_ids
                if execution_id in outputs
            },
            latest_output_ids=latest_output_ids,
            node_execution_counts=self.node_execution_counts,
            operator_attempt_counts=self.operator_attempt_counts,
            operator_runtime_ns=self.operator_runtime_ns,
            waits=tuple(self.waits.values()),
            pending_advances=tuple(self.pending_advances.items()),
            session_created_at_ms=self.session.created_at_ms,
            invocation_created_at_ms=self.invocation.created_at_ms,
            created_at_ms=now_ms(),
        )

    def _prune_execution_state(self) -> None:
        executions, _, latest_output_ids = self._execution_views()
        keep = set(latest_output_ids.values())
        for request in self.scheduler.ready:
            keep.update(item.source_execution_id for item in request.activations)
        for resolution in self.scheduler.resolutions.values():
            if resolution.activation is not None:
                keep.add(resolution.activation.source_execution_id)
        keep.update(wait.node_execution_id for wait in self.waits.values())
        keep.update(self.pending_advances)
        keep.update(
            execution.id
            for execution in executions.values()
            if execution.state in {"ready", "running", "waiting"}
        )
        remove = tuple(
            execution_id
            for execution_id in executions
            if execution_id not in keep
        )
        if remove:
            self._defer_operations(
                tuple(
                    StateOperation(
                        "remove", ("node_executions", str(execution_id))
                    )
                    for execution_id in remove
                )
            )

    def _hook_context(
        self,
        request: NodeExecutionRequest,
        execution: NodeExecution,
    ) -> InputMappingContext:
        executions, outputs, _ = self._execution_views()
        incoming = tuple(
            IncomingActivation(
                edge_id=activation.edge_id,
                source_node_id=activation.source_node_id,
                source_execution_id=str(activation.source_execution_id),
                source_scope=executions[activation.source_execution_id].scope,
                value=outputs[activation.source_execution_id],
            )
            for activation in request.activations
        )
        return hook_context(
            InputMappingContext,
            workflow_id=self.workflow.workflow_id,
            workflow_revision_id=self.workflow.workflow_revision_id,
            workflow_path=self.workflow.node(request.node_id).workflow_path,
            session_id=self.session.id,
            invocation_id=str(self.invocation.id),
            session_context=(
                execution.restart_session_context
                if execution.restart_session_context is not None
                else self.session.context
            ),
            invocation_context=(
                execution.restart_invocation_context
                if execution.restart_invocation_context is not None
                else self.invocation_context
            ),
            invocation_input=self.invocation_input,
            node_id=request.node_id,
            node_execution_id=str(execution.id),
            execution_scope=request.scope,
            incoming=incoming,
        )

    def _validate_patch_revision(
        self, execution: NodeExecution, patch: ContextPatch
    ) -> None:
        for path in sorted(patch_paths(patch)):
            root, relative = path[0], path[1:]
            revisions = (
                self.session_path_revisions
                if root == "session"
                else self.invocation_path_revisions
            )
            base = execution.started_state_version
            for changed, revision in revisions.items():
                size = min(len(relative), len(changed))
                if relative[:size] == changed[:size] and revision > base:
                    raise RuntimeError(
                        "Parallel Output Bindings modify the same Context path: "
                        f"{root}.{'/'.join(relative)}."
                    )

    def _commit_context_patch(
        self, execution: NodeExecution, patch: ContextPatch
    ) -> StateOperationBatch | None:
        self._validate_patch_revision(execution, patch)
        operations = list(self._patch_operations(patch))
        if not operations:
            return None
        next_version = self.runtime_state.state_version + 1
        for path in sorted(patch_paths(patch)):
            root, relative = path[0], path[1:]
            owner = "session" if root == "session" else "invocation"
            key = context_path_key(relative)
            revisions = self.runtime_state.read(owner, "context_path_revisions")
            operations.append(
                StateOperation(
                    "replace" if key in revisions else "add",
                    (owner, "context_path_revisions", key),
                    next_version,
                )
            )
        return self._apply_operations(tuple(operations))

    def _default_input(self, request: NodeExecutionRequest) -> Any:
        if not request.activations:
            return copy.deepcopy(self.invocation_input)
        values = copy.deepcopy(self._activation_values(request))
        return next(iter(values.values())) if len(values) == 1 else values

    def _activation_values(self, request: NodeExecutionRequest) -> dict[str, Any]:
        outputs = self.outputs
        source_ids = [item.source_node_id for item in request.activations]
        use_edge_ids = len(source_ids) != len(set(source_ids))
        return {
            (activation.edge_id if use_edge_ids else activation.source_node_id): outputs[
                activation.source_execution_id
            ]
            for activation in request.activations
        }

    def _defer_operations(
        self, operations: tuple[StateOperation, ...]
    ) -> StateOperationBatch:
        return self._apply_operations(operations)

    def _apply_operations(
        self, operations: tuple[StateOperation, ...]
    ) -> StateOperationBatch:
        """Apply one business-state batch and record its global commit order."""

        batch = self.runtime_state.apply(operations)
        if self.event_mode is EventMode.FULL and (
            self.sink is not None
            or (
                self.stream is not None
                and self.stream.event_channel in {"runtime", "all"}
            )
        ):
            self._deferred_operation_batches.append(batch)
        return batch

    def _defer_scheduler_batch(self) -> None:
        batch = self.scheduler.take_last_batch()
        if batch is None:
            return
        if self.event_mode is EventMode.FULL and (
            self.sink is not None
            or (
                self.stream is not None
                and self.stream.event_channel in {"runtime", "all"}
            )
        ):
            self._deferred_operation_batches.append(batch)

    def _remove_pending_advance(self, execution_id: UUID) -> None:
        if execution_id not in self.pending_advances:
            return
        self._defer_operations(
            (
                StateOperation(
                    "remove", ("pending_advances", str(execution_id))
                ),
            )
        )

    def _clear_pending_advances(self) -> None:
        execution_ids = tuple(self.pending_advances)
        if execution_ids:
            self._defer_operations(
                tuple(
                    StateOperation(
                        "remove", ("pending_advances", str(execution_id))
                    )
                    for execution_id in execution_ids
                )
            )

    def _set_deferred_error(self, error: RuntimeErrorInfo) -> None:
        if self.deferred_error is not None:
            return
        self._defer_operations(
            (
                StateOperation(
                    "replace",
                    ("invocation", "deferred_error"),
                    asdict(error),
                ),
            )
        )

    @classmethod
    def _node_from_state_record(cls, record: dict[str, Any]) -> NodeExecution:
        return NodeExecution(
            id=UUID(record["id"]),
            node_id=record["node_id"],
            scope=tuple(
                cls._loop_iteration(
                    frame["loop_region_id"], frame["iteration"]
                )
                for frame in record["scope"]
            ),
            state=record["state"],
            input=record["input"],
            output=record["output"],
            error=record["error"],
            logical_occurrence=record["logical_occurrence"],
            idempotency_key=record["idempotency_key"],
            started_state_version=record["started_state_version"],
            restart_session_context=record["restart_session_context"],
            restart_invocation_context=record[
                "restart_invocation_context"
            ],
        )

    @classmethod
    def _node_state_record(cls, execution: NodeExecution) -> dict[str, Any]:
        return {
            "id": str(execution.id),
            "node_id": execution.node_id,
            "scope": list(cls._scope_value(execution.scope)),
            "state": execution.state,
            "input": execution.input,
            "output": execution.output,
            "error": execution.error,
            "logical_occurrence": execution.logical_occurrence,
            "idempotency_key": execution.idempotency_key,
            "started_state_version": execution.started_state_version,
            "restart_session_context": execution.restart_session_context,
            "restart_invocation_context": execution.restart_invocation_context,
        }

    @staticmethod
    def _wait_state_record(wait: WaitCheckpoint) -> dict[str, Any]:
        return {
            "id": str(wait.id),
            "node_execution_id": str(wait.node_execution_id),
            "request": wait.request.to_record(),
            "payload": wait.payload,
        }

    def _patch_operations(self, patch: ContextPatch) -> tuple[StateOperation, ...]:
        values: list[StateOperation] = []
        for root, current, mapping in (
            ("session", self.session.context, patch.session),
            ("invocation", self.invocation_context, patch.invocation),
        ):
            self._merge_operations(values, (root, "context"), current, mapping)
        return tuple(values)

    @classmethod
    def _merge_operations(
        cls,
        operations: list[StateOperation],
        path: tuple[str | int, ...],
        current: Any,
        patch: Any,
    ) -> None:
        """Describe the same recursive merge performed by ``apply_patch``."""

        for key, value in patch.items():
            key_path = (*path, str(key))
            exists = isinstance(current, Mapping) and key in current
            previous = current.get(key) if exists else None
            if (
                isinstance(value, Mapping)
                and value
                and isinstance(previous, Mapping)
            ):
                cls._merge_operations(operations, key_path, previous, value)
                continue
            # Empty mappings and mappings replacing scalar values are complete
            # values at this path; nested merge is only valid for two mappings.
            operations.append(
                StateOperation(
                    "replace" if exists else "add",
                    key_path,
                    value,
                )
            )

    @staticmethod
    def _scope_value(scope: Any) -> tuple[dict[str, Any], ...]:
        return tuple(
            {"loop_region_id": item.loop_region_id, "iteration": item.iteration}
            for item in scope
        )
