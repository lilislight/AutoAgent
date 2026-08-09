"""Invocation coordinator: the only writer of Runtime state and Event sequence."""

from __future__ import annotations

import asyncio
import copy
import inspect
import logging
import re
import time
from collections.abc import Mapping
from dataclasses import asdict
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
    RuntimeSink,
    SchedulerCheckpoint,
    SerializedCheckpoint,
    SerializedEvent,
    Session,
    StateOperation,
    UserEvent,
    WaitCheckpoint,
    WaitSnapshot,
    apply_patch,
    now_ms,
    patch_paths,
    readonly_context,
)
from ..runtime.serialization import encode_runtime_value
from ..scheduler import NodeExecutionRequest, Scheduler, occurrence_key, scope_key
from ..operators import WaitOperator
from ..workflow import ContextPatch, NodeIR, UserEventMapping, WorkflowIR
from .node_executor import NodeExecutor
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
        self.invocation_input = copy.deepcopy(invocation_input)
        self.event_mode = event_mode
        self.sink = sink
        self.stream = stream
        self.node_executor = node_executor
        self.default_max_node_executions = default_max_node_executions

        self.scheduler = Scheduler(workflow)
        self.scheduler.initialize()
        self.invocation_context: dict[str, Any] = {}
        self.node_executions: dict[UUID, NodeExecution] = {}
        self.outputs: dict[UUID, Any] = {}
        self.latest_output_ids: dict[str, UUID] = {}
        self.node_execution_counts: dict[str, int] = {}
        self.operator_attempt_counts: dict[str, int] = {}
        self.operator_runtime_ns: dict[str, int] = {}
        self.waits: dict[UUID, WaitCheckpoint] = {}
        self.claimed_waits: set[UUID] = set()
        self.resume_queue: asyncio.Queue[tuple[UUID, Any]] = asyncio.Queue()
        self.session_context_revision = 0
        self.invocation_context_revision = 0
        self.session_path_revisions: dict[tuple[str, ...], int] = {}
        self.invocation_path_revisions: dict[tuple[str, ...], int] = {}
        self.runtime_sequence = 0
        self.user_sequence = 0
        self.recovery_mode = False
        self.deferred_error: RuntimeErrorInfo | None = None

        self.boundary = asyncio.Event()
        self.terminal = asyncio.Event()
        self.task: asyncio.Task[None] | None = None
        self.worker_tasks: set[asyncio.Task[NodeExecutionResult]] = set()
        self._worker_context: dict[
            asyncio.Task[NodeExecutionResult], tuple[NodeExecutionRequest, NodeExecution]
        ] = {}
        self.cancel_requested = False
        self.pending_delivery: tuple[SerializedEvent, ...] = ()
        self._event_lock = asyncio.Lock()
        self._terminal_lock = asyncio.Lock()

    def restore(self, checkpoint: RecoveryCheckpoint) -> None:
        self.invocation_input = copy.deepcopy(checkpoint.invocation_input)
        self.session.context = copy.deepcopy(checkpoint.session_context)
        self.invocation_context = copy.deepcopy(checkpoint.invocation_context)
        scheduler = checkpoint.scheduler_state
        self.scheduler.restore(
            ready=copy.deepcopy(scheduler.ready),
            resolutions=copy.deepcopy(scheduler.resolutions),
            scheduled=scheduler.scheduled,
            skipped=scheduler.skipped,
        )
        self.node_executions = {
            item.execution_id: NodeExecution(
                id=item.execution_id,
                node_id=item.node_id,
                scope=tuple(
                    self._loop_iteration(region_id, iteration)
                    for region_id, iteration in item.scope
                ),
                state=item.state,  # type: ignore[arg-type]
            )
            for item in checkpoint.node_states
        }
        self.outputs = copy.deepcopy(checkpoint.required_outputs)
        self.latest_output_ids = dict(checkpoint.latest_output_ids)
        self.node_execution_counts = dict(checkpoint.node_execution_counts)
        self.operator_attempt_counts = dict(checkpoint.operator_attempt_counts)
        self.operator_runtime_ns = dict(checkpoint.operator_runtime_ns)
        self.waits = {wait.id: copy.deepcopy(wait) for wait in checkpoint.waits}
        for wait in self.waits.values():
            self.scheduler.track_active(wait.request)
        self._publish_waits()
        self.runtime_sequence = checkpoint.runtime_event_sequence
        self.user_sequence = checkpoint.user_event_sequence
        self.recovery_mode = True

    @staticmethod
    def _loop_iteration(region_id: str, iteration: int) -> Any:
        from ..scheduler import LoopIteration

        return LoopIteration(region_id, iteration)

    async def run(self, *, recovered: bool = False) -> None:
        try:
            if self.stream is not None:
                await self.stream.wait_started()
            if recovered:
                self.recovery_mode = True
                await self._emit_runtime(
                    event_name="invocation_recovered",
                    subject_type="invocation",
                    subject_id=str(self.invocation.id),
                    status="running",
                    operations=self._operation(("invocation", "recovery"), True),
                )
            await self._set_invocation_state(InvocationState.RUNNING)

            while True:
                if self.cancel_requested:
                    await self.finish_cancelled()
                    return
                await self._drain_resume_queue()

                if self.scheduler.ready:
                    if not self.worker_tasks:
                        self._offer_checkpoint(InvocationState.RUNNING)
                    requests = self.scheduler.drain_ready()
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
                        self._offer_checkpoint(InvocationState.WAITING)
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
                    self._offer_checkpoint(InvocationState.RUNNING)
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
            await self._emit_runtime(
                event_name="invocation_recovered",
                subject_type="invocation",
                subject_id=str(self.invocation.id),
                status="waiting",
                operations=self._operation(("invocation", "recovery"), True),
            )
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
            if self.workflow.policy.failure.mode == "fail_fast":
                raise RuntimeError(info.message)
            self.deferred_error = self.deferred_error or info
        return tuple(allowed)

    async def _start_batch(
        self, requests: tuple[NodeExecutionRequest, ...]
    ) -> tuple[NodeExecution, ...]:
        executions: list[NodeExecution] = []
        for request in requests:
            node = self.workflow.node(request.node_id)
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
            self.node_execution_counts[node.id] = count
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
                session_context_revision=self.session_context_revision,
                invocation_context_revision=self.invocation_context_revision,
            )
            self.node_executions[execution.id] = execution
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
                operations=self._operation(
                    ("node_executions", str(execution.id), "state"), "running"
                ),
            )
        return tuple(executions)

    async def _run_node(
        self, request: NodeExecutionRequest, execution: NodeExecution
    ) -> NodeExecutionResult:
        node = self.workflow.node(request.node_id)
        context = self._hook_context(request)
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
                self.operator_attempt_counts[node.id] = attempted
            elif kind == "operator_call":
                self.operator_runtime_ns[node.id] = (
                    self.operator_runtime_ns.get(node.id, 0) + value.duration_ns
                )
                policy_error: RuntimeError | None = None
                if (
                    resource
                    and resource.max_runtime_ms_per_invocation is not None
                    and self.operator_runtime_ns[node.id]
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
                            "queue_wait_ns": value.queue_wait_ns,
                            "handler_ns": value.handler_ns,
                            "stream_ns": value.stream_ns,
                            "stream_delivery_ns": value.stream_delivery_ns,
                        },
                    }
                    if self.event_mode is EventMode.FULL:
                        payload.update({"input": value.input, "output": value.output})
                    await self._emit_runtime(
                        event_name="operator_call_completed",
                        subject_type="operator_call",
                        subject_id=str(value.id),
                        status=value.status,
                        workflow_path=node.workflow_path,
                        duration_ns=value.duration_ns,
                        payload=payload,
                    )
                if policy_error is not None:
                    raise policy_error
            elif kind == "phase" and self.event_mode is EventMode.FULL:
                phase: NodePhaseResult = value
                await self._emit_runtime(
                    event_name=phase.name,
                    subject_type="node_phase",
                    subject_id=str(execution.id),
                    status=phase.status,
                    workflow_path=node.workflow_path,
                    duration_ns=phase.duration_ns,
                    payload=phase.payload,
                )

        async def stream_chunk(chunk: Any) -> None:
            await self._emit_user_mappings(
                node,
                node.stream_user_event_mappings,
                node.stream_user_event_contracts,
                chunk,
            )

        return await self.node_executor.execute(
            workflow_revision_id=self.workflow.workflow_revision_id,
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
        )

    async def _commit_result(
        self,
        request: NodeExecutionRequest,
        execution: NodeExecution,
        result: NodeExecutionResult,
    ) -> None:
        if result.error is not None:
            execution.state = "failed"
            execution.error = f"{type(result.error).__name__}: {result.error}"
            execution.completed_at_ms = now_ms()
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=execution.node_id,
                status="failed",
                workflow_path=self.workflow.node(execution.node_id).workflow_path,
                payload={"node_execution_id": str(execution.id), "error": execution.error},
                operations=self._operation(
                    ("node_executions", str(execution.id), "state"), "failed"
                ),
            )

            if self.workflow.policy.failure.mode == "fail_fast":
                raise result.error
            for skipped in self.scheduler.skip_outgoing(request.node_id, request.scope):
                await self._emit_skipped_occurrence(skipped.node_id, skipped.scope, "upstream_failed")
            self.deferred_error = self.deferred_error or RuntimeErrorInfo(
                type=type(result.error).__name__, message=str(result.error)
            )
            return
        if result.cancelled:
            execution.state = "cancelled"
            return

        self._validate_patch_revision(execution, result.patch)
        execution.input = copy.deepcopy(result.mapped_input)
        execution.output = copy.deepcopy(result.output)
        execution.state = "completed"
        execution.completed_at_ms = now_ms()
        if execution.started_perf_ns is not None:
            execution.duration_ns = max(
                0, time.perf_counter_ns() - execution.started_perf_ns
            )
        patch_operations = self._patch_operations(result.patch)
        apply_patch(self.session.context, self.invocation_context, result.patch)
        self._record_patch_revision(result.patch)
        self.outputs[execution.id] = copy.deepcopy(result.output)
        self.latest_output_ids[execution.node_id] = execution.id
        operations = [
            StateOperation(
                op="add",
                path=("outputs", str(execution.id)),
                value=copy.deepcopy(result.output),
            )
        ]
        operations.extend(patch_operations)
        await self._emit_runtime(
            event_name="node_state_changed",
            subject_type="node",
            subject_id=execution.node_id,
            status="completed",
            workflow_path=self.workflow.node(execution.node_id).workflow_path,
            duration_ns=execution.duration_ns,
            payload={"node_execution_id": str(execution.id)},
            operations=tuple(operations),
        )
        node = self.workflow.node(execution.node_id)
        await self._emit_user_mappings(
            node, node.user_event_mappings, node.user_event_contracts, result.output
        )
        await self._advance_completed(request, execution)

    async def _advance_completed(
        self, request: NodeExecutionRequest, execution: NodeExecution
    ) -> None:
        decisions: dict[str, bool] = {}
        for edge in self.workflow.outgoing(request.node_id):
            started = time.perf_counter_ns()
            try:
                selected = True
                if edge.condition is not None:
                    selected_value = await self._call_hook(
                        edge.condition,
                        self._hook_context(
                            incoming={execution.node_id: execution.output},
                            edge_id=edge.id,
                            workflow_path=edge.workflow_path,
                        ),
                    )
                    if not isinstance(selected_value, bool):
                        raise TypeError("Edge condition must return bool.")
                    selected = selected_value
            except BaseException as error:
                duration = max(0, time.perf_counter_ns() - started)
                await self._emit_runtime(
                    event_name="edge_evaluated",
                    subject_type="edge",
                    subject_id=edge.id,
                    status="failed",
                    workflow_path=edge.workflow_path,
                    duration_ns=duration,
                    payload={"error": f"{type(error).__name__}: {error}"},
                )
                raise
            duration = max(0, time.perf_counter_ns() - started)
            decisions[edge.id] = selected
            await self._emit_runtime(
                event_name="edge_evaluated",
                subject_type="edge",
                subject_id=edge.id,
                status="selected" if selected else "not_selected",
                workflow_path=edge.workflow_path,
                duration_ns=duration,
                payload={"source": edge.source, "target": edge.target},
                operations=self._operation(("edges", edge.id, "selected"), selected),
            )
        for skipped in self.scheduler.resolve_outgoing(request, execution.id, decisions):
            await self._emit_skipped_occurrence(skipped.node_id, skipped.scope, "incoming_edges_not_selected")

    async def _enter_wait(
        self,
        request: NodeExecutionRequest,
        execution: NodeExecution,
        result: NodeExecutionResult,
    ) -> None:
        assert result.waiting
        execution.state = "waiting"
        wait = WaitCheckpoint(
            id=uuid4(),
            node_execution_id=execution.id,
            request=request,
            payload=copy.deepcopy(result.wait_payload),
        )
        self.waits[wait.id] = wait
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
            operations=self._operation(
                ("node_executions", str(execution.id), "state"), "waiting"
            ),
        )
    async def _complete_resumed_node(self, wait_id: UUID, response: Any) -> None:
        wait = self.waits[wait_id]
        execution = self.node_executions[wait.node_execution_id]
        node = self.workflow.node(execution.node_id)
        if not isinstance(node.operator, WaitOperator):
            raise RuntimeError("A Wait Checkpoint must reference a WaitOperator Node.")
        output = node.operator.response_contract.validate(
            copy.deepcopy(response)
        )
        encode_runtime_value(output)
        binding_started = time.perf_counter_ns()
        try:
            patch = await self.node_executor.bind_output(
                node, self._hook_context(wait.request), output
            )
        except BaseException as error:
            duration = max(0, time.perf_counter_ns() - binding_started)
            if node.output_binding is not None:
                await self._emit_runtime(
                    event_name="output_binding_completed",
                    subject_type="node_phase",
                    subject_id=str(execution.id),
                    status="failed",
                    workflow_path=node.workflow_path,
                    duration_ns=duration,
                    payload={"error": f"{type(error).__name__}: {error}"},
                )
            execution.state = "failed"
            execution.error = f"{type(error).__name__}: {error}"
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=node.id,
                status="failed",
                workflow_path=node.workflow_path,
                payload={"node_execution_id": str(execution.id), "error": execution.error},
                operations=self._operation(
                    ("node_executions", str(execution.id), "state"), "failed"
                ),
            )
            raise
        if node.output_binding is not None:
            duration = max(0, time.perf_counter_ns() - binding_started)
            await self._emit_runtime(
                event_name="output_binding_completed",
                subject_type="node_phase",
                subject_id=str(execution.id),
                status="completed",
                workflow_path=node.workflow_path,
                duration_ns=duration,
                payload={"patch": patch},
            )
        self._validate_patch_revision(execution, patch)
        patch_operations = self._patch_operations(patch)
        apply_patch(self.session.context, self.invocation_context, patch)
        self._record_patch_revision(patch)
        execution.output = output
        execution.state = "completed"
        execution.completed_at_ms = now_ms()
        self.outputs[execution.id] = copy.deepcopy(output)
        self.latest_output_ids[node.id] = execution.id
        self.waits.pop(wait_id)
        self.claimed_waits.discard(wait_id)
        self._publish_waits()
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
            operations=(
                *self._operation(("node_executions", str(execution.id), "state"), "completed"),
                StateOperation("add", ("outputs", str(execution.id)), copy.deepcopy(output)),
                *patch_operations,
            ),
        )
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
        self.waits.clear()
        self.claimed_waits.clear()
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
            operations=self._operation(
                ("node_occurrences", occurrence_key(node_id, scope), "state"), "skipped"
            ),
        )

    async def _finish_completed(self) -> None:
        output = {
            node_id: copy.deepcopy(self.outputs[execution_id])
            for node_id in self.workflow.exit_node_ids
            if (execution_id := self.latest_output_ids.get(node_id)) in self.outputs
        }
        await self._set_invocation_state(InvocationState.COMPLETED, output=output)
        self.invocation._clear_checkpoint(updated_at_ms=now_ms())
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
            await self._set_invocation_state(InvocationState.FAILED, error=info)
            self.invocation._clear_checkpoint(updated_at_ms=now_ms())
            self.boundary.set()
            self.terminal.set()

    async def finish_cancelled(self) -> None:
        async with self._terminal_lock:
            if self.invocation.state.terminal:
                return
            self.cancel_requested = True
            for task in tuple(self.worker_tasks):
                task.cancel()
            self.invocation._update(state=InvocationState.CANCELLED, updated_at_ms=now_ms())
            try:
                await self._finish_remaining_nodes("invocation_cancelled")
                self._clear_waits()
                await self._emit_runtime(
                    event_name="invocation_state_changed",
                    subject_type="invocation",
                    subject_id=str(self.invocation.id),
                    status="cancelled",
                    operations=self._operation(("invocation", "state"), "cancelled"),
                )
                self.invocation._clear_checkpoint(updated_at_ms=now_ms())
            except asyncio.CancelledError:
                self._clear_waits()
            self.boundary.set()
            self.terminal.set()
            if self.stream is not None:
                self.stream.abandon()

    async def _finish_remaining_nodes(self, reason: str) -> None:
        for execution in tuple(self.node_executions.values()):
            if execution.state not in {"ready", "running", "waiting"}:
                continue
            execution.state = "cancelled"
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=execution.node_id,
                status="cancelled",
                workflow_path=self.workflow.node(execution.node_id).workflow_path,
                payload={"node_execution_id": str(execution.id), "reason": reason},
                operations=self._operation(
                    ("node_executions", str(execution.id), "state"), "cancelled"
                ),
            )
        touched = {execution.node_id for execution in self.node_executions.values()}
        for node in self.workflow.nodes:
            already_skipped = any(
                key == node.id or key.startswith(f"{node.id}@")
                for key in self.scheduler.skipped
            )
            if node.id in touched or already_skipped:
                continue
            await self._emit_runtime(
                event_name="node_state_changed",
                subject_type="node",
                subject_id=node.id,
                status="skipped",
                workflow_path=node.workflow_path,
                payload={"reason": reason},
            )

    async def _set_invocation_state(
        self,
        state: InvocationState,
        *,
        output: dict[str, Any] | None = None,
        error: RuntimeErrorInfo | None = None,
    ) -> None:
        self.invocation._update(
            state=state, output=output, error=error, updated_at_ms=now_ms()
        )
        await self._emit_runtime(
            event_name="invocation_state_changed",
            subject_type="invocation",
            subject_id=str(self.invocation.id),
            status=state.value,
            payload={"output": output, "error": asdict(error) if error else None},
            operations=self._operation(("invocation", "state"), state.value),
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
        payload: Any = None,
        operations: tuple[StateOperation, ...] = (),
    ) -> None:
        if self.event_mode is EventMode.MINIMAL:
            return
        if self.sink is None and (
            self.stream is None
            or self.stream.event_channel not in {"runtime", "all"}
        ):
            return
        if self.event_mode is EventMode.STANDARD:
            operations = ()
            if subject_type == "node_phase":
                return
            if subject_type == "operator_call" and isinstance(payload, dict):
                payload = {
                    key: value
                    for key, value in payload.items()
                    if key not in {"input", "output"}
                }
        async with self._event_lock:
            self.runtime_sequence += 1
            try:
                event = RuntimeEvent.detached(
                    workflow_id=self.workflow.workflow_id,
                    workflow_revision_id=self.workflow.workflow_revision_id,
                    session_id=self.session.id,
                    invocation_id=self.invocation.id,
                    sequence=self.runtime_sequence,
                    event_name=event_name,
                    subject_type=subject_type,
                    subject_id=subject_id,
                    status=status,
                    workflow_path=workflow_path,
                    duration_ns=duration_ns,
                    payload=payload,
                    operations=(
                        operations if self.event_mode is EventMode.FULL else ()
                    ),
                )
                serialized = (
                    SerializedEvent.from_event(event) if self.sink is not None else None
                )
            except BaseException as error:
                logger.exception("Runtime Event capture failed; attempting a gap Event.")
                try:
                    event = RuntimeEvent(
                        workflow_id=self.workflow.workflow_id,
                        workflow_revision_id=self.workflow.workflow_revision_id,
                        session_id=self.session.id,
                        invocation_id=self.invocation.id,
                        sequence=self.runtime_sequence,
                        event_name="event_capture_failed",
                        subject_type=subject_type,
                        subject_id=subject_id,
                        status="failed",
                        workflow_path=workflow_path,
                        payload={
                            "original_event_name": event_name,
                            "error_type": type(error).__name__,
                        },
                    )
                    serialized = (
                        SerializedEvent.from_event(event)
                        if self.sink is not None
                        else None
                    )
                except BaseException:
                    logger.exception(
                        "Runtime Event gap capture also failed; tracing has a sequence gap."
                    )
                    return
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
                data = await self._call_hook(mapping.transform, copy.deepcopy(value))
                data = contract.validate(data)
                if data is None:
                    continue
                encode_runtime_value(data)
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
                self.user_sequence += 1
                try:
                    event = UserEvent.detached(
                        workflow_id=self.workflow.workflow_id,
                        workflow_revision_id=self.workflow.workflow_revision_id,
                        session_id=self.session.id,
                        invocation_id=self.invocation.id,
                        sequence=self.user_sequence,
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
                            sequence=self.user_sequence,
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
            self.sink = None

    def _checkpoint(self, state: InvocationState) -> RecoveryCheckpoint:
        latest_execution_ids = set(self.latest_output_ids.values())
        required_ids = set(latest_execution_ids)
        for request in self.scheduler.ready:
            required_ids.update(item.source_execution_id for item in request.activations)
        for resolution in self.scheduler.resolutions.values():
            if resolution.activation is not None:
                required_ids.add(resolution.activation.source_execution_id)
        for wait in self.waits.values():
            required_ids.add(wait.node_execution_id)
            required_ids.update(
                item.source_execution_id for item in wait.request.activations
            )
        latest_nodes = {
            execution_id: self.node_executions[execution_id]
            for execution_id in required_ids
            if execution_id in self.node_executions
        }
        return RecoveryCheckpoint.detached(
            schema_version=1,
            workflow_id=self.workflow.workflow_id,
            workflow_revision_id=self.workflow.workflow_revision_id,
            session_id=self.session.id,
            invocation_id=self.invocation.id,
            invocation_state=state.value,
            runtime_event_sequence=self.runtime_sequence,
            user_event_sequence=self.user_sequence,
            invocation_input=self.invocation_input,
            session_context=self.session.context,
            invocation_context=self.invocation_context,
            scheduler_state=SchedulerCheckpoint(
                ready=tuple(self.scheduler.ready),
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
                )
                for execution in latest_nodes.values()
            ),
            required_outputs={
                execution_id: self.outputs[execution_id]
                for execution_id in required_ids
                if execution_id in self.outputs
            },
            latest_output_ids=self.latest_output_ids,
            node_execution_counts=self.node_execution_counts,
            operator_attempt_counts=self.operator_attempt_counts,
            operator_runtime_ns=self.operator_runtime_ns,
            waits=tuple(self.waits.values()),
            created_at_ms=now_ms(),
        )

    def _prune_execution_state(self) -> None:
        keep = set(self.latest_output_ids.values())
        for request in self.scheduler.ready:
            keep.update(item.source_execution_id for item in request.activations)
        for wait in self.waits.values():
            keep.add(wait.node_execution_id)
        self.outputs = {key: value for key, value in self.outputs.items() if key in keep}
        self.node_executions = {
            key: value
            for key, value in self.node_executions.items()
            if key in keep or value.state in {"running", "waiting"}
        }

    def _hook_context(
        self,
        request: NodeExecutionRequest | None = None,
        *,
        incoming: dict[str, Any] | None = None,
        edge_id: str | None = None,
        workflow_path: tuple[str, ...] | None = None,
    ) -> Any:
        visible_outputs = {
            node_id: self.outputs[execution_id]
            for node_id, execution_id in self.latest_output_ids.items()
            if execution_id in self.outputs
        }
        exact_incoming = incoming or {}
        if request is not None:
            exact_incoming = self._activation_values(request)
        return readonly_context(
            session=self.session.context,
            invocation=self.invocation_context,
            outputs=visible_outputs,
            incoming=exact_incoming,
            invocation_input=self.invocation_input,
            node_id=request.node_id if request is not None else None,
            edge_id=edge_id,
            workflow_path=(
                workflow_path
                if workflow_path is not None
                else self.workflow.node(request.node_id).workflow_path
                if request is not None
                else ()
            ),
        )

    def _validate_patch_revision(
        self, execution: NodeExecution, patch: ContextPatch
    ) -> None:
        for path in patch_paths(patch):
            root, relative = path[0], path[1:]
            revisions = (
                self.session_path_revisions
                if root == "session"
                else self.invocation_path_revisions
            )
            base = (
                execution.session_context_revision
                if root == "session"
                else execution.invocation_context_revision
            )
            for changed, revision in revisions.items():
                size = min(len(relative), len(changed))
                if relative[:size] == changed[:size] and revision > base:
                    raise RuntimeError(
                        "Parallel Output Bindings modify the same Context path: "
                        f"{root}.{'/'.join(relative)}."
                    )

    def _record_patch_revision(self, patch: ContextPatch) -> None:
        session_paths = {
            path[1:] for path in patch_paths(patch) if path[0] == "session"
        }
        invocation_paths = {
            path[1:] for path in patch_paths(patch) if path[0] == "invocation"
        }
        if session_paths:
            self.session_context_revision += 1
            for path in session_paths:
                self.session_path_revisions[path] = self.session_context_revision
        if invocation_paths:
            self.invocation_context_revision += 1
            for path in invocation_paths:
                self.invocation_path_revisions[path] = self.invocation_context_revision

    def _default_input(self, request: NodeExecutionRequest) -> Any:
        if not request.activations:
            return copy.deepcopy(self.invocation_input)
        values = copy.deepcopy(self._activation_values(request))
        return next(iter(values.values())) if len(values) == 1 else values

    def _activation_values(self, request: NodeExecutionRequest) -> dict[str, Any]:
        source_ids = [item.source_node_id for item in request.activations]
        use_edge_ids = len(source_ids) != len(set(source_ids))
        return {
            (activation.edge_id if use_edge_ids else activation.source_node_id): self.outputs[
                activation.source_execution_id
            ]
            for activation in request.activations
        }

    async def _call_hook(self, function: Any, *arguments: Any) -> Any:
        if inspect.iscoroutinefunction(function):
            return await function(*arguments)
        value = await asyncio.to_thread(function, *arguments)
        return await value if inspect.isawaitable(value) else value

    def _operation(
        self, path: tuple[str | int, ...], value: Any
    ) -> tuple[StateOperation, ...]:
        return (StateOperation("replace", path, copy.deepcopy(value)),)

    def _patch_operations(self, patch: ContextPatch) -> tuple[StateOperation, ...]:
        values: list[StateOperation] = []
        for root, current, mapping in (
            ("session_context", self.session.context, patch.session),
            ("invocation_context", self.invocation_context, patch.invocation),
        ):
            self._merge_operations(values, (root,), current, mapping)
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
                    copy.deepcopy(value),
                )
            )

    @staticmethod
    def _scope_value(scope: Any) -> tuple[dict[str, Any], ...]:
        return tuple(
            {"loop_region_id": item.loop_region_id, "iteration": item.iteration}
            for item in scope
        )
