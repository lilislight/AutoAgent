from __future__ import annotations

import asyncio
from collections.abc import Mapping
from copy import deepcopy
import inspect
import logging
from time import perf_counter_ns
from typing import Any
from uuid import UUID

from autoagent.core.compiler import NodeIR, WorkflowIR, WorkflowVersionSnapshot
from autoagent.core.executor.node_executor import (
    NodeExecutionJob,
    NodeExecutor,
)
from autoagent.core.executor.result import (
    NodeExecutionProgress,
    NodeExecutionResult,
)
from autoagent.core.runtime import (
    InputMappingContext,
    IncomingOutput,
    Invocation,
    DirectOperatorExecution,
    EdgeEvaluation,
    NodeExecution,
    OutputBindingContext,
    ParallelOperatorExecution,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeEventSubjectType,
    RuntimeEventType,
    RuntimeStore,
    Session,
    UserEventSpec,
    build_state_operations,
)
from autoagent.core.runtime.context import capture_hook_context
from autoagent.core.runtime.scheduler import NodeExecutionRequest, node_instance_key
from autoagent.core.runtime.hooks import invoke_hook_async
from autoagent.core.runtime.time import utc_timestamp_ms
from autoagent.core.scheduler import Scheduler
from autoagent.core.workflow.user_event import normalize_user_event_mappings


_MISSING = object()
logger = logging.getLogger(__name__)


def _event_identity(event_name: str) -> tuple[RuntimeEventType, str]:
    if event_name.startswith("invocation."):
        return "state_change", event_name
    if event_name.startswith("recovery."):
        return "recovery", event_name
    if event_name.startswith("wait."):
        return "wait", event_name
    if event_name.startswith("edge.") or event_name.startswith("routing."):
        return "routing", event_name
    if event_name.startswith("operator_call."):
        return "operator_call", event_name
    if event_name.startswith("node."):
        return "state_change", event_name
    return "phase", event_name


def _event_subject(
    invocation: Invocation,
    *,
    event_type: RuntimeEventType,
    event_name: str,
    node_execution_ids: tuple[UUID, ...],
    detail: dict[str, Any] | None,
) -> tuple[RuntimeEventSubjectType, str]:
    data = detail or {}
    if event_type == "routing" and data.get("edge_id") is not None:
        return "edge", str(data["edge_id"])
    if event_type == "operator_call" and data.get("operator_call_id") is not None:
        return "operator_call", str(data["operator_call_id"])
    if event_type == "wait" and data.get("wait_key") is not None:
        return "wait", str(data["wait_key"])
    if event_type == "recovery":
        return "recovery", str(invocation.id)
    if node_execution_ids:
        return "node", str(node_execution_ids[0])
    if data.get("node_execution_id") is not None:
        return "node", str(data["node_execution_id"])
    if data.get("node_id") is not None:
        return "node", str(data["node_id"])
    return "invocation", str(invocation.id)


def _event_status(
    event_name: str,
    detail: dict[str, Any] | None,
) -> str | None:
    data = detail or {}
    if data.get("state") is not None:
        return str(data["state"])
    return event_name.rsplit(".", 1)[-1] if "." in event_name else None


class WorkflowExecutor:
    """Main control loop for one workflow invocation.

    WorkflowExecutor is the only object that connects scheduler decisions,
    node execution, runtime state mutation, and persistence. It is deliberately
    the layer that knows about running NodeExecutions; Scheduler does not.

    Loop order:
      1. Drain stable transitions and let Scheduler produce ready requests.
      2. Drain ready requests, create NodeExecutions, and submit them to
         NodeExecutor without waiting for the whole batch to finish.
      3. If no immediate work exists but nodes are running, await the first
         completed NodeExecution Task.
      4. Apply results on this main control path and persist.
      5. Return only when invocation is completed, failed, or externally waiting.
    """

    def __init__(
        self,
        *,
        scheduler: Scheduler | None = None,
        node_executor: NodeExecutor | None = None,
        runtime_store: RuntimeStore,
    ) -> None:
        self.scheduler = scheduler or Scheduler()
        self.node_executor = node_executor or NodeExecutor()
        self.runtime_store = runtime_store

    async def _record_event(
        self,
        session: Session,
        invocation: Invocation,
        event_name: str,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
        force_recovery_checkpoint: bool = False,
        detail: dict[str, Any] | None = None,
        elapsed_ns: int | None = None,
        timing: dict[str, int] | None = None,
        input: Any | None = None,
        output: Any | None = None,
        occurred_at_ms: int | None = None,
    ) -> RuntimeEvent | None:
        """Record one mode-dependent Runtime fact after its change is applied."""

        event_type, normalized_name = _event_identity(event_name)
        if invocation.event_mode == "minimal":
            if (
                normalized_name.startswith("invocation.")
                or normalized_name in {"wait.created", "wait.resumed"}
            ):
                await self.runtime_store.apersist_invocation_state(
                    session,
                    invocation,
                )
            return None

        if invocation.event_mode == "standard" and event_type == "phase":
            return None
        sequence = invocation.next_event_sequence()
        operations = (
            build_state_operations(
                self.runtime_store.reduced_state(invocation.id),
                session,
                invocation,
                node_execution_ids=node_execution_ids,
                copy_operation_values=False,
            )
            if invocation.event_mode == "full"
            else None
        )
        subject_type, subject_id = _event_subject(
            invocation,
            event_type=event_type,
            event_name=normalized_name,
            node_execution_ids=node_execution_ids,
            detail=detail,
        )
        event = RuntimeEvent(
            invocation_id=invocation.id,
            sequence=sequence,
            event_type=event_type,
            event_name=normalized_name,
            subject_type=subject_type,
            subject_id=subject_id,
            occurred_at_ms=occurred_at_ms or utc_timestamp_ms(),
            elapsed_ns=elapsed_ns,
            status=_event_status(normalized_name, detail),
            timing=timing or {},
            payload=detail or {},
            input=input if invocation.event_mode == "full" else None,
            output=output if invocation.event_mode == "full" else None,
            operations=operations,
        )
        applied = await self.runtime_store.arecord_event(
            session,
            invocation,
            event,
            node_execution_ids=node_execution_ids,
            force_recovery_checkpoint=force_recovery_checkpoint,
        )
        return applied

    async def ainvoke(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        if invocation.state == "created":
            invocation.mark_running()
            await self._record_event(
                session,
                invocation,
                "invocation.running",
                detail={"state": "running"},
            )
        skipped_before_initialization = set(
            invocation.scheduler.skipped_node_instances
        )
        self.scheduler.initialize(workflow_ir=workflow_ir, invocation=invocation)
        await self._record_new_skipped_nodes(
            session,
            invocation,
            skipped_before_initialization,
        )
        try:
            return await self._drive(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            )
        except asyncio.CancelledError:
            await self._cancel_invocation(session=session, invocation=invocation)
            raise
        except BaseException:
            await self.node_executor.abandon(invocation.execution_mailbox)
            raise

    async def afail_infrastructure(
        self,
        *,
        session: Session,
        invocation: Invocation,
        error: Exception,
    ) -> Invocation:
        """Turn an escaped framework exception into one terminal Runtime fact."""

        if invocation.state in {
            "completed",
            "failed",
            "cancelled",
            "interrupted",
        }:
            return invocation
        runtime_error = RuntimeErrorInfo(
            code="INVOCATION_INFRASTRUCTURE_ERROR",
            message=(
                "Invocation failed because execution infrastructure raised an "
                "unexpected error."
            ),
            detail={
                "exception_type": type(error).__name__,
                "message": str(error),
            },
        )
        invocation.mark_failed(runtime_error)
        try:
            await self._cancel_unfinished_nodes(
                session=session,
                invocation=invocation,
                error=runtime_error,
            )
            await self._record_event(
                session,
                invocation,
                "invocation.failed",
                detail={
                    "state": "failed",
                    "error": runtime_error.to_record(),
                },
                force_recovery_checkpoint=True,
            )
        except Exception:
            logger.exception(
                "Failed to record infrastructure RuntimeEvent; persisting the "
                "terminal Invocation state directly: invocation_id=%s",
                invocation.id,
            )
            try:
                await self.runtime_store.apersist_invocation_state(
                    session,
                    invocation,
                )
            except Exception:
                logger.exception(
                    "Failed to persist terminal infrastructure failure; the "
                    "in-memory Invocation remains failed: invocation_id=%s",
                    invocation.id,
                )
        return invocation

    async def arecover(
        self,
        *,
        workflow_ir: WorkflowIR,
        workflow_snapshot: WorkflowVersionSnapshot,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        """Automatically continue or terminate one process-interrupted Invocation.

        Recovery never restores Python stacks. It rebuilds the durable prefix,
        marks the execution mode as sticky ``recovery``, and follows only the
        path Scheduler actually selects. Each selected parallel Node batch is
        gated immediately before execution; no speculative graph scan occurs.
        """

        if invocation.state not in {"created", "running"}:
            return invocation
        if invocation.workflow_definition_hash != workflow_ir.definition_hash:
            await self._interrupt_recovery(
                session,
                invocation,
                code="WORKFLOW_DEFINITION_CHANGED",
                message="Current Workflow definition does not match interrupted work.",
            )
            return invocation
        invocation.execution_mode = "recovery"
        if invocation.state == "created":
            return await self.ainvoke(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            )

        active = [
            execution
            for execution in invocation.node_executions
            if execution.state in {"created", "ready", "running"}
        ]
        if not active:
            invocation.mark_running()
            return await self._drive(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            )

        changed_execution_ids: list[UUID] = []
        for interrupted in active:
            changed_execution_ids.append(interrupted.id)
            interrupted.mark_interrupted(
                RuntimeErrorInfo(
                    code="WORKER_LOST",
                    message="Process ended while this NodeExecution was running.",
                )
            )
            await self._record_terminal_node_events(
                session=session,
                invocation=invocation,
                executions=(interrupted,),
            )
            request = NodeExecutionRequest(
                node_id=interrupted.node_id,
                activations=interrupted.incoming_activations,
                execution_scope=interrupted.execution_scope,
            )
            node_ir = workflow_ir.nodes.get(interrupted.node_id)
            reason = self._recovery_rejection_reason(
                node_ir=node_ir,
                interrupted=interrupted,
            )
            if reason is not None:
                error = RuntimeErrorInfo(
                    code="NODE_RECOVERY_REJECTED",
                    message=reason,
                    detail={"node_id": interrupted.node_id},
                )
                if workflow_ir.policy.failure.mode == "fail_fast":
                    await self._interrupt_recovery(
                        session,
                        invocation,
                        code=error.code,
                        message=error.message,
                    )
                    return invocation
                invocation.defer_terminal(error, state="interrupted")
                self.scheduler.skip_ready_request(
                    workflow_ir=workflow_ir,
                    invocation=invocation,
                    request=request,
                )
                continue
            invocation.scheduler.enqueue_ready(
                request.node_id,
                activations=request.activations,
                execution_scope=request.execution_scope,
                idempotency_key=(
                    interrupted.idempotency_key or str(interrupted.id)
                ),
                recovery_of_execution_id=interrupted.id,
                recovery_attempt=interrupted.recovery_attempt + 1,
            )

        invocation.mark_running()
        await self._record_event(
            session,
            invocation,
            "recovery.requeued",
            node_execution_ids=tuple(changed_execution_ids),
            detail={"recovery_requeued": [str(value) for value in changed_execution_ids]},
        )
        return await self._drive(
            workflow_ir=workflow_ir,
            session=session,
            invocation=invocation,
        )

    def _recovery_rejection_reason(
        self,
        *,
        node_ir: NodeIR | None,
        interrupted: NodeExecution,
    ) -> str | None:
        if node_ir is None:
            return f"Interrupted node is absent from current Workflow: {interrupted.node_id}"
        recovery = node_ir.policy.recovery if node_ir.policy is not None else None
        if recovery is None or recovery.mode == "never":
            return f"Node {interrupted.node_id} does not permit crash recovery."
        if interrupted.recovery_attempt >= recovery.max_attempts:
            return (
                f"Node {interrupted.node_id} exhausted RecoveryPolicy.max_attempts."
            )
        return None

    async def _interrupt_recovery(
        self,
        session: Session,
        invocation: Invocation,
        *,
        code: str,
        message: str,
    ) -> None:
        error = RuntimeErrorInfo(code=code, message=message)
        changed_execution_ids = await self._interrupt_unfinished_nodes(
            session=session,
            invocation=invocation,
            error=error,
            abandon_workers=False,
        )
        invocation.mark_interrupted(error)
        await self._record_event(
            session,
            invocation,
            "recovery.interrupted",
            node_execution_ids=changed_execution_ids,
            force_recovery_checkpoint=True,
            detail={"code": code, "message": message},
        )

    async def _drive(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        """Advance one Invocation until it reaches a stable public state."""

        while True:
            if invocation.state == "failed":
                return await self._finish_failed_invocation(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                )

            transitions = invocation.scheduler.drain_transitions()
            if transitions:
                skipped_before_routing = set(
                    invocation.scheduler.skipped_node_instances
                )
                await self.scheduler.next(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    transitions=transitions,
                    on_edge_evaluated=lambda execution, evaluation: (
                        self._record_edge_event(
                            session,
                            invocation,
                            execution,
                            evaluation,
                        )
                    ),
                )
                await self._record_new_skipped_nodes(
                    session,
                    invocation,
                    skipped_before_routing,
                )
                if invocation.state == "failed":
                    return await self._finish_failed_invocation(
                        workflow_ir=workflow_ir,
                        session=session,
                        invocation=invocation,
                    )
                continue

            ready_requests = invocation.scheduler.drain_ready()
            if ready_requests:
                changed_execution_ids, jobs = await self._submit_ready_requests(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    ready_requests=ready_requests,
                )
                if jobs:
                    self.node_executor.submit_batch(
                        jobs,
                        mailbox=invocation.execution_mailbox,
                    )
                if invocation.state in {"failed", "interrupted"}:
                    if invocation.state == "interrupted":
                        return invocation
                    return await self._finish_failed_invocation(
                        workflow_ir=workflow_ir,
                        session=session,
                        invocation=invocation,
                    )
                continue

            if self.node_executor.has_running(invocation.execution_mailbox):
                messages = await self.node_executor.wait_next_messages(
                    invocation.execution_mailbox
                )
                changed_execution_ids = []
                for message in messages:
                    if isinstance(message, NodeExecutionProgress):
                        await self._apply_progress(
                            session=session,
                            invocation=invocation,
                            progress=message,
                        )
                        continue
                    changed_execution_id = await self._apply_result(
                        workflow_ir=workflow_ir,
                        session=session,
                        invocation=invocation,
                        result=message,
                    )
                    if changed_execution_id is not None:
                        changed_execution_ids.append(changed_execution_id)
                continue

            if invocation.scheduler.waiting_executions:
                invocation.mark_waiting()
                await self._record_event(
                    session,
                    invocation,
                    "wait.created",
                    node_execution_ids=tuple(
                        waiting.node_execution_id
                        for waiting in invocation.scheduler.waiting_executions.values()
                    ),
                    force_recovery_checkpoint=True,
                    detail={
                        "state": "waiting",
                        "wait_keys": sorted(
                            invocation.scheduler.waiting_executions
                        ),
                        "waits": [
                            waiting.to_record()
                            for waiting in (
                                invocation.scheduler.waiting_executions[
                                    wait_key
                                ]
                                for wait_key in sorted(
                                    invocation.scheduler.waiting_executions
                                )
                            )
                        ],
                    },
                )
                return invocation

            if invocation.deferred_terminal_state is not None:
                error = invocation.deferred_error or RuntimeErrorInfo(
                    code="DEFERRED_BRANCH_FAILURE",
                    message="A branch could not complete.",
                )
                changed_execution_ids: tuple[UUID, ...] = ()
                if invocation.deferred_terminal_state == "interrupted":
                    changed_execution_ids = (
                        await self._interrupt_unfinished_nodes(
                            session=session,
                            invocation=invocation,
                            error=error,
                        )
                    )
                    invocation.mark_interrupted(error)
                    terminal_event_name = "recovery.interrupted"
                else:
                    invocation.mark_failed(error)
                    terminal_event_name = "invocation.failed"
                if invocation.state == "failed":
                    return await self._finish_failed_invocation(
                        workflow_ir=workflow_ir,
                        session=session,
                        invocation=invocation,
                    )
                await self._record_event(
                    session,
                    invocation,
                    terminal_event_name,
                    node_execution_ids=(
                        changed_execution_ids
                        if invocation.state == "interrupted"
                        else ()
                    ),
                    force_recovery_checkpoint=True,
                    detail={
                        "state": invocation.state,
                        "error": error.to_record(),
                    },
                )
                self._record_agent_failed_user_event(
                    workflow_ir,
                    invocation,
                )
                invocation.execution_mailbox.close()
                return invocation

            if self._is_completed(workflow_ir=workflow_ir, invocation=invocation):
                invocation.mark_completed(
                    result=self._build_invocation_result(
                        workflow_ir=workflow_ir,
                        invocation=invocation,
                    )
                )
                await self._record_event(
                    session,
                    invocation,
                    "invocation.completed",
                )
                invocation.execution_mailbox.close()
                return invocation

            invocation.mark_failed(
                RuntimeErrorInfo(
                    code="WORKFLOW_DEAD_END",
                    message="Workflow has no ready, running, waiting, or completed exit node.",
                )
            )
            return await self._finish_failed_invocation(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            )

    async def _record_edge_event(
        self,
        session: Session,
        invocation: Invocation,
        execution: NodeExecution,
        evaluation: EdgeEvaluation,
    ) -> None:
        """Freeze one edge decision before Scheduler evaluates another edge."""

        await self._record_event(
            session,
            invocation,
            "edge.evaluated",
            node_execution_ids=(execution.id,),
            detail={
                "edge_id": evaluation.edge_id,
                "node_id": execution.node_id,
                "source_execution_id": str(execution.id),
                "target_node_id": evaluation.target_node_id,
                "state": evaluation.state,
                "selected": evaluation.selected,
                "reason": evaluation.reason,
            },
            elapsed_ns=evaluation.elapsed_ns,
            timing=(
                {"execution_ns": evaluation.elapsed_ns}
                if evaluation.elapsed_ns is not None
                else None
            ),
            occurred_at_ms=evaluation.updated_at_ms,
        )

    async def _record_new_skipped_nodes(
        self,
        session: Session,
        invocation: Invocation,
        previous: set[str],
    ) -> None:
        for instance_key in sorted(
            invocation.scheduler.skipped_node_instances - previous
        ):
            await self._record_event(
                session,
                invocation,
                "node.skipped",
                detail={
                    "node_id": instance_key.split("@", 1)[0],
                    "node_instance_key": instance_key,
                    "state": "skipped",
                },
            )

    async def _cancel_invocation(
        self,
        *,
        session: Session,
        invocation: Invocation,
    ) -> None:
        error = RuntimeErrorInfo(
            code="INVOCATION_CANCELLED",
            message="Invocation was cancelled by its caller.",
        )
        changed_execution_ids = await self._cancel_unfinished_nodes(
            session=session,
            invocation=invocation,
            error=error,
        )
        invocation.mark_cancelled()
        await self._record_event(
            session,
            invocation,
            "invocation.cancelled",
            node_execution_ids=changed_execution_ids,
        )

    async def acancel(
        self,
        *,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        """Cancel an Invocation that is not currently owned by a caller task."""

        if invocation.state not in {"created", "running", "waiting"}:
            raise ValueError(
                f"Invocation cannot be cancelled from state {invocation.state}."
            )
        await self._cancel_invocation(session=session, invocation=invocation)
        return invocation

    async def aresume(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        wait_key: str,
        output: Any = _MISSING,
    ) -> Invocation:
        try:
            return await self._aresume_impl(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
                wait_key=wait_key,
                output=output,
            )
        except asyncio.CancelledError:
            await self._cancel_invocation(session=session, invocation=invocation)
            raise

    async def _aresume_impl(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        wait_key: str,
        output: Any = _MISSING,
    ) -> Invocation:
        """Resume one persisted wait and continue the same Invocation.

        The external signal completes the existing waiting NodeExecution and
        exposes its transition to Scheduler. No second NodeExecution record is
        created.
        """

        waiting = invocation.scheduler.waiting_executions.get(wait_key)
        if waiting is None:
            raise KeyError(f"Unknown wait key: {wait_key}")
        node_execution = invocation.get_node_execution(waiting.node_execution_id)
        if node_execution is None:
            raise KeyError(f"Unknown waiting NodeExecution: {waiting.node_execution_id}")

        invocation.scheduler.remove_waiting_execution(wait_key)
        final_output = node_execution.output if output is _MISSING else output
        node_execution.output = final_output
        await self._record_event(
            session,
            invocation,
            "wait.resumed",
            node_execution_ids=(node_execution.id,),
            detail={
                "node_id": node_execution.node_id,
                "node_execution_id": str(node_execution.id),
                "wait_key": wait_key,
                "resumed": True,
            },
        )
        await self._complete_resumed_execution(
            workflow_ir=workflow_ir,
            session=session,
            invocation=invocation,
            node_execution=node_execution,
            output=final_output,
        )
        await self._record_event(
            session,
            invocation,
            f"node.{node_execution.state}",
            node_execution_ids=(node_execution.id,),
            detail={
                "node_id": node_execution.node_id,
                "node_execution_id": str(node_execution.id),
                "resumed": True,
            },
        )

        if not invocation.scheduler.waiting_executions:
            invocation.mark_running()
            await self._record_event(
                session,
                invocation,
                "invocation.running",
                detail={
                    "state": "running",
                    "resumed_wait_key": wait_key,
                },
            )
        return await self._drive(
            workflow_ir=workflow_ir,
            session=session,
            invocation=invocation,
        )

    async def _submit_ready_requests(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        ready_requests: list[NodeExecutionRequest],
    ) -> tuple[tuple[UUID, ...], list[NodeExecutionJob]]:
        jobs: list[NodeExecutionJob] = []
        changed_execution_ids: list[UUID] = []

        for request in ready_requests:
            node_ir = workflow_ir.nodes.get(request.node_id)
            if node_ir is None:
                invocation.mark_failed(
                    RuntimeErrorInfo(
                        code="UNKNOWN_NODE",
                        message=f"Ready request references unknown node: {request.node_id}",
                    )
                )
                return tuple(changed_execution_ids), jobs

            if invocation.execution_mode == "recovery":
                recovery = (
                    node_ir.policy.recovery
                    if node_ir.policy is not None
                    else None
                )
                rejected = (
                    recovery is None
                    or recovery.mode == "never"
                    or request.recovery_attempt > recovery.max_attempts
                )
                if rejected:
                    error = RuntimeErrorInfo(
                        code="NODE_RECOVERY_REJECTED",
                        message=(
                            f"Node {request.node_id} does not permit another "
                            "crash-recovery replay."
                        ),
                        detail={"node_id": request.node_id},
                    )
                    if workflow_ir.policy.failure.mode == "fail_fast":
                        await self._interrupt_recovery(
                            session,
                            invocation,
                            code=error.code,
                            message=error.message,
                        )
                        return tuple(changed_execution_ids), jobs
                    invocation.defer_terminal(error, state="interrupted")
                    self.scheduler.skip_ready_request(
                        workflow_ir=workflow_ir,
                        invocation=invocation,
                        request=request,
                    )
                    await self._record_event(
                        session,
                        invocation,
                        "recovery.node_skipped",
                        detail={
                            "recovery_blocked_node_id": request.node_id,
                        },
                    )
                    continue

            resource_error = self._check_node_execution_resource(
                invocation=invocation,
                node_ir=node_ir,
            )
            if resource_error is not None:
                invocation.mark_failed(resource_error)
                return tuple(changed_execution_ids), jobs

            node_execution = invocation.create_node_execution(
                node_ir.id,
                idempotency_key=request.idempotency_key,
                recovery_of_execution_id=request.recovery_of_execution_id,
                recovery_attempt=request.recovery_attempt,
                incoming_activations=request.activations,
                execution_scope=request.execution_scope,
            )
            changed_execution_ids.append(node_execution.id)
            if node_execution.idempotency_key is None:
                node_execution.idempotency_key = str(node_execution.id)
            invocation.scheduler.scheduled_node_instances.add(
                node_instance_key(node_ir.id, request.execution_scope)
            )
            node_execution.base_invocation_context_revision = (
                invocation.context.revision
            )
            node_execution.base_session_context_revision = (
                session.context.revision
            )
            invocation.mark_node_running(node_execution.id)
            await self._record_event(
                session,
                invocation,
                "node.running",
                node_execution_ids=(node_execution.id,),
                detail={
                    "node_id": node_ir.id,
                    "node_execution_id": str(node_execution.id),
                    "state": "running",
                },
            )

            mapping_phase = "input_mapping"
            mapping_failure_code = "INPUT_MAPPING_FAILED"
            mapping_started_ns = perf_counter_ns()
            try:
                map_policy = (
                    node_ir.policy.map
                    if node_ir.policy is not None
                    else None
                )
                node_input, node_incoming = await self._build_node_input(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    node_ir=node_ir,
                    request=request,
                )
                recovery = (
                    node_ir.policy.recovery
                    if node_ir.policy is not None
                    else None
                )
                if recovery is not None and recovery.mode == "idempotent":
                    accepted = {
                        parameter.name
                        for parameter in node_ir.input_contract.parameters
                    }
                    if "idempotency_key" not in accepted:
                        raise ValueError(
                            "Idempotent RecoveryPolicy requires an "
                            "idempotency_key Operator parameter."
                        )
                    if map_policy is None:
                        if not isinstance(node_input, dict):
                            raise TypeError(
                                "Idempotent recovery requires mapping Node input."
                            )
                        node_input = dict(node_input)
                        node_input["idempotency_key"] = (
                            node_execution.idempotency_key
                        )
            except Exception as exc:
                mapping_elapsed_ns = max(0, perf_counter_ns() - mapping_started_ns)
                await self._record_event(
                    session,
                    invocation,
                    "input_mapping.completed",
                    node_execution_ids=(node_execution.id,),
                    detail={
                        "node_id": node_ir.id,
                        "state": "failed",
                        "error_type": type(exc).__name__,
                    },
                    elapsed_ns=mapping_elapsed_ns,
                    timing={"execution_ns": mapping_elapsed_ns},
                )
                # Mapping is a workflow data-shaping phase, not an OperatorExecution.
                # Finalize the node here so retry and operator fallback cannot run.
                invocation.mark_node_failed(
                    node_execution.id,
                    RuntimeErrorInfo(
                        code=mapping_failure_code,
                        message=str(exc),
                        detail={
                            "node_id": node_ir.id,
                            "error_type": type(exc).__name__,
                        },
                    ),
                )
                await self._record_event(
                    session,
                    invocation,
                    "node.failed",
                    node_execution_ids=(node_execution.id,),
                    detail={
                        "node_id": node_ir.id,
                        "phase": mapping_phase,
                        "state": "failed",
                    },
                )
                continue
            mapping_elapsed_ns = max(0, perf_counter_ns() - mapping_started_ns)
            node_execution.input = node_input
            invocation.updated_at_ms = utc_timestamp_ms()
            await self._record_event(
                session,
                invocation,
                "input_mapping.completed",
                node_execution_ids=(node_execution.id,),
                detail={
                    "node_id": node_ir.id,
                    "node_execution_id": str(node_execution.id),
                    "state": "completed",
                },
                elapsed_ns=mapping_elapsed_ns,
                timing={"execution_ns": mapping_elapsed_ns},
                output=node_input,
            )

            resource_error = self._check_operator_attempt_resource(
                invocation=invocation,
                node_ir=node_ir,
            )
            if resource_error is not None:
                invocation.mark_node_failed(node_execution.id, resource_error)
                continue

            jobs.append(
                NodeExecutionJob(
                    node_ir=node_ir,
                    node_execution=node_execution,
                    input=node_input,
                    incoming=node_incoming,
                    max_operator_attempts=self._remaining_operator_attempts(
                        invocation=invocation,
                        node_ir=node_ir,
                    ),
                    concurrency_key=f"{workflow_ir.workflow_id}:{node_ir.id}",
                    recovery=invocation.execution_mode == "recovery",
                    event_mode=invocation.event_mode,
                    hook_context=capture_hook_context(
                        invocation_input=invocation.input,
                        invocation_context=invocation.context,
                        session_context=session.context,
                        outputs=invocation.outputs.scoped(node_ir.scope_node_ids),
                    ),
                )
            )

        return tuple(changed_execution_ids), jobs

    async def _apply_progress(
        self,
        *,
        session: Session,
        invocation: Invocation,
        progress: NodeExecutionProgress,
    ) -> None:
        """Apply one completed internal Node step on the Runtime control path."""

        node_execution = invocation.get_node_execution(progress.node_execution_id)
        if node_execution is None:
            invocation.mark_failed(
                RuntimeErrorInfo(
                    code="UNKNOWN_NODE_EXECUTION_PROGRESS",
                    message=(
                        "NodeExecutor returned progress for an unknown "
                        "NodeExecution."
                    ),
                    detail={
                        "node_execution_id": str(progress.node_execution_id),
                    },
                )
            )
            return

        if progress.kind == "user_event":
            if progress.user_event_specs:
                self._record_user_event_specs(
                    invocation,
                    progress.user_event_specs,
                )
            return

        if progress.kind == "operator_call":
            operator_execution = progress.operator_execution
            if operator_execution is None:  # pragma: no cover - validated message.
                return
            if any(
                existing.id == operator_execution.id
                for existing in node_execution.operator_executions
            ):
                return
            node_execution.operator_executions.append(operator_execution)
            await self._record_operator_call(
                session,
                invocation,
                node_execution,
                operator_execution,
                logical_elapsed_ns=progress.logical_elapsed_ns,
            )
            return

        phase = progress.phase
        if phase is None:  # pragma: no cover - validated message.
            return
        if phase.name == "aggregation.completed" and phase.status == "completed":
            node_execution.output = deepcopy(phase.output)
        await self._record_event(
            session,
            invocation,
            phase.name,
            node_execution_ids=(node_execution.id,),
            detail={
                "node_id": node_execution.node_id,
                "state": phase.status,
            },
            elapsed_ns=phase.elapsed_ns,
            timing=phase.timing,
            input=phase.input,
            output=phase.output,
            occurred_at_ms=phase.occurred_at_ms,
        )

    async def _apply_result(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        result: NodeExecutionResult,
    ) -> UUID | None:
        node_execution = invocation.get_node_execution(result.node_execution_id)
        if node_execution is None:
            invocation.mark_failed(
                RuntimeErrorInfo(
                    code="UNKNOWN_NODE_EXECUTION_RESULT",
                    message="NodeExecutor returned a result for an unknown NodeExecution.",
                    detail={"node_execution_id": str(result.node_execution_id)},
                )
            )
            return None

        before_operator = tuple(
            phase for phase in result.phases
            if phase.name == "item_selection.completed"
        )
        after_operator = tuple(
            phase for phase in result.phases
            if phase.name != "item_selection.completed"
        )
        for phase in before_operator:
            await self._record_event(
                session,
                invocation,
                phase.name,
                node_execution_ids=(node_execution.id,),
                detail={
                    "node_id": node_execution.node_id,
                    "state": phase.status,
                },
                elapsed_ns=phase.elapsed_ns,
                timing=phase.timing,
                input=phase.input,
                output=phase.output,
                occurred_at_ms=phase.occurred_at_ms,
            )
        for operator_execution in result.operator_executions:
            if any(
                existing.id == operator_execution.id
                for existing in node_execution.operator_executions
            ):
                continue
            node_execution.operator_executions.append(operator_execution)
            await self._record_operator_call(
                session,
                invocation,
                node_execution,
                operator_execution,
                logical_elapsed_ns=result.operator_elapsed_ns,
            )
        node_execution.resource_usage = result.resource_usage
        if after_operator and result.state == "completed":
            node_execution.output = result.output
        for phase in after_operator:
            await self._record_event(
                session,
                invocation,
                phase.name,
                node_execution_ids=(node_execution.id,),
                detail={
                    "node_id": node_execution.node_id,
                    "state": phase.status,
                },
                elapsed_ns=phase.elapsed_ns,
                timing=phase.timing,
                input=phase.input,
                output=phase.output,
                occurred_at_ms=phase.occurred_at_ms,
            )

        node_ir = workflow_ir.nodes[node_execution.node_id]

        runtime_error = self._check_runtime_resource_after_result(
            workflow_ir=workflow_ir,
            invocation=invocation,
            node_execution=node_execution,
        )
        if runtime_error is not None:
            invocation.mark_node_failed(node_execution.id, runtime_error)
            await self._record_event(
                session,
                invocation,
                "node.failed",
                node_execution_ids=(node_execution.id,),
                detail={
                    "node_id": node_execution.node_id,
                    "node_execution_id": str(node_execution.id),
                    "state": "failed",
                    "error": runtime_error.to_record(),
                },
            )
            return node_execution.id

        if result.state == "completed":
            node_execution.output = result.output
            has_output_binding = callable(node_ir.output_binding)
            binding_started_ns = perf_counter_ns()
            try:
                await self._run_output_binding(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    node_execution=node_execution,
                    output=result.output,
                )
            except Exception as exc:
                binding_elapsed_ns = max(
                    0,
                    perf_counter_ns() - binding_started_ns,
                )
                if has_output_binding:
                    await self._record_event(
                        session,
                        invocation,
                        "output_binding.completed",
                        node_execution_ids=(node_execution.id,),
                        detail={
                            "node_id": node_execution.node_id,
                            "state": "failed",
                            "error_type": type(exc).__name__,
                        },
                        elapsed_ns=binding_elapsed_ns,
                        timing={"execution_ns": binding_elapsed_ns},
                        input=result.output,
                    )
                # Binding runs after successful operator execution. Its failure
                # rolls back its isolated Context copies and finalizes the node
                # without retrying or selecting another operator.
                invocation.mark_node_failed(
                    node_execution.id,
                    RuntimeErrorInfo(
                        code="OUTPUT_BINDING_FAILED",
                        message=str(exc),
                        detail={
                            "node_id": node_execution.node_id,
                            "error_type": type(exc).__name__,
                        },
                    ),
                )
                session.mark_context_updated()
            else:
                binding_elapsed_ns = max(
                    0,
                    perf_counter_ns() - binding_started_ns,
                )
                if has_output_binding:
                    await self._record_event(
                        session,
                        invocation,
                        "output_binding.completed",
                        node_execution_ids=(node_execution.id,),
                        detail={
                            "node_id": node_execution.node_id,
                            "state": "completed",
                        },
                        elapsed_ns=binding_elapsed_ns,
                        timing={"execution_ns": binding_elapsed_ns},
                        input=result.output,
                    )
                invocation.mark_node_completed(node_execution.id, result.output)
                session.mark_context_updated()
                self._record_completed_user_events(
                    invocation=invocation,
                    node_ir=node_ir,
                    node_execution=node_execution,
                    output=result.output,
                )
        elif result.state == "waiting":
            try:
                invocation.mark_node_waiting(
                    node_execution.id,
                    wait_key=result.wait_key or str(node_execution.id),
                    wait_type=result.wait_type,
                    payload=result.wait_payload,
                    pending_output=result.output,
                )
            except ValueError as exc:
                invocation.mark_node_failed(
                    node_execution.id,
                    RuntimeErrorInfo(
                        code="WAIT_KEY_CONFLICT",
                        message=str(exc),
                        detail={"node_id": node_execution.node_id},
                    ),
                )
        else:
            invocation.mark_node_failed(
                node_execution.id,
                result.error or RuntimeErrorInfo(
                    code="NODE_EXECUTION_FAILED",
                    message="Node execution failed.",
                ),
            )
        await self._record_event(
            session,
            invocation,
            f"node.{node_execution.state}",
            node_execution_ids=(node_execution.id,),
            detail={
                "node_id": node_execution.node_id,
                "node_execution_id": str(node_execution.id),
                "state": node_execution.state,
                "error": (
                    node_execution.error.to_record()
                    if node_execution.error is not None
                    else None
                ),
            },
            elapsed_ns=(
                max(
                    0,
                    perf_counter_ns() - node_execution.started_at_monotonic_ns,
                )
                if node_execution.started_at_monotonic_ns
                else None
            ),
            timing={
                key: value
                for key, value in {
                    "concurrency_slot_ns": (
                        node_execution.resource_usage.concurrency_wait_ns
                    ),
                    "thread_pool_queue_ns": (
                        node_execution.resource_usage.thread_pool_queue_ns
                    ),
                    "retry_backoff_ns": (
                        node_execution.resource_usage.retry_backoff_ns
                    ),
                }.items()
                if value
            },
        )
        return node_execution.id

    def _record_completed_user_events(
        self,
        *,
        invocation: Invocation,
        node_ir: NodeIR,
        node_execution: NodeExecution,
        output: Any,
    ) -> None:
        mappings = normalize_user_event_mappings(node_ir.user_event_mapping)
        if not mappings:
            return
        operator_call_id = (
            node_execution.operator_executions[-1].id
            if node_execution.operator_executions
            else None
        )
        specs: list[UserEventSpec] = []
        for mapping in mappings:
            try:
                data = mapping.transform(deepcopy(output))
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
                    # Output mappings run on an isolated output copy and are
                    # recorded synchronously below, so RuntimeStore's JSON
                    # detachment is the only additional ownership copy needed.
                    data=data,
                    node_id=node_ir.id,
                    node_execution_id=node_execution.id,
                    workflow_path=node_ir.workflow_path,
                    operator_call_id=operator_call_id,
                )
            except Exception as exc:
                spec = UserEventSpec(
                    type="user_event_mapping_failed",
                    data={
                        "mapping_type": mapping.type,
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                        "source": "output",
                    },
                    node_id=node_ir.id,
                    node_execution_id=node_execution.id,
                    workflow_path=node_ir.workflow_path,
                    operator_call_id=operator_call_id,
                )
            specs.append(spec)
        self._record_user_event_specs(invocation, tuple(specs))

    def _record_user_event_spec(
        self,
        invocation: Invocation,
        spec: UserEventSpec,
    ) -> None:
        self._record_user_event_specs(invocation, (spec,))

    def _record_user_event_specs(
        self,
        invocation: Invocation,
        specs: tuple[UserEventSpec, ...],
    ) -> None:
        if not specs:
            return
        try:
            self.runtime_store._record_user_events(
                invocation_id=invocation.id,
                specs=specs,
            )
        except Exception as exc:
            # Batch serialization is all-or-nothing. Isolate the invalid spec
            # only on this exceptional path so valid siblings remain visible.
            if len(specs) > 1:
                for spec in specs:
                    self._record_user_event_specs(invocation, (spec,))
                return
            spec = specs[0]
            if spec.type != "user_event_mapping_failed":
                self.runtime_store._record_user_events(
                    invocation_id=invocation.id,
                    specs=(
                        UserEventSpec(
                            type="user_event_mapping_failed",
                            data={
                                "mapping_type": spec.type,
                                "error_type": type(exc).__name__,
                                "message": str(exc),
                                "source": "serialization",
                            },
                            node_id=spec.node_id,
                            node_execution_id=spec.node_execution_id,
                            workflow_path=spec.workflow_path,
                            operator_call_id=spec.operator_call_id,
                        ),
                    ),
                )

    def _record_agent_failed_user_event(
        self,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
    ) -> None:
        if invocation.state != "failed":
            return
        llm_execution = next(
            (
                execution
                for execution in reversed(invocation.node_executions)
                if workflow_ir.nodes[execution.node_id].metadata.get(
                    "_autoagent_user_event_stream"
                )
                == "message"
            ),
            None,
        )
        if llm_execution is None:
            return
        error = invocation.error or RuntimeErrorInfo(
            code="AGENT_FAILED",
            message="ReAct Workflow failed.",
        )
        self._record_user_event_spec(
            invocation,
            UserEventSpec(
                type="agent_failed",
                data=error.to_record(),
                node_id=llm_execution.node_id,
                node_execution_id=llm_execution.id,
                workflow_path=workflow_ir.nodes[
                    llm_execution.node_id
                ].workflow_path,
                operator_call_id=(
                    llm_execution.operator_executions[-1].id
                    if llm_execution.operator_executions
                    else None
                ),
            ),
        )

    def _record_abandoned_user_events(
        self,
        invocation: Invocation,
        messages: list[NodeExecutionProgress | NodeExecutionResult],
    ) -> None:
        for message in messages:
            if (
                isinstance(message, NodeExecutionProgress)
                and message.kind == "user_event"
                and message.user_event_specs
            ):
                self._record_user_event_specs(
                    invocation,
                    message.user_event_specs,
                )

    async def _record_operator_call(
        self,
        session: Session,
        invocation: Invocation,
        node_execution: NodeExecution,
        operator_execution: DirectOperatorExecution | ParallelOperatorExecution,
        *,
        logical_elapsed_ns: int,
    ) -> None:
        if isinstance(operator_execution, DirectOperatorExecution):
            usage = operator_execution.resource_usage
            detail = {
                "node_id": node_execution.node_id,
                "node_execution_id": str(node_execution.id),
                "operator_call_id": str(operator_execution.id),
                "operator_id": operator_execution.operator_id,
                "reason": operator_execution.reason,
                "state": operator_execution.state,
                "streaming": operator_execution.streaming,
                "stream_chunk_count": operator_execution.stream_chunk_count,
                "error": (
                    operator_execution.error.to_record()
                    if operator_execution.error is not None
                    else None
                ),
            }
            await self._record_event(
                session,
                invocation,
                "operator_call.completed",
                node_execution_ids=(node_execution.id,),
                detail=detail,
                elapsed_ns=usage.duration_ns,
                timing={
                    key: value
                    for key, value in {
                        "execution_ns": usage.execution_ns,
                        "thread_pool_queue_ns": usage.thread_pool_queue_ns,
                        "stream_consumption_ns": (
                            usage.stream_consumption_ns
                        ),
                        "stream_reduction_ns": usage.stream_reduction_ns,
                    }.items()
                    if value
                },
                input=operator_execution.input,
                output=operator_execution.output,
                occurred_at_ms=operator_execution.ended_at_ms,
            )
            return

        summary = operator_execution.summary
        await self._record_event(
            session,
            invocation,
            "operator_call.completed",
            node_execution_ids=(node_execution.id,),
            detail={
                "node_id": node_execution.node_id,
                "node_execution_id": str(node_execution.id),
                "operator_call_id": str(operator_execution.id),
                "kind": operator_execution.kind,
                "operator_ids": list(operator_execution.operator_ids),
                "state": operator_execution.state,
                "summary": summary.to_record(),
                "error": (
                    operator_execution.error.to_record()
                    if operator_execution.error is not None
                    else None
                ),
            },
            elapsed_ns=logical_elapsed_ns,
            timing={
                key: value
                for key, value in {
                    "execution_ns": summary.total_duration_ns,
                    "stream_consumption_ns": (
                        summary.stream_consumption_ns
                    ),
                    "stream_reduction_ns": summary.stream_reduction_ns,
                }.items()
                if value
            },
            occurred_at_ms=operator_execution.ended_at_ms,
        )

    async def _build_node_input(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        node_ir: NodeIR,
        request: NodeExecutionRequest,
    ) -> tuple[Any, tuple[IncomingOutput, ...]]:
        incoming = self._build_incoming_outputs(
            invocation=invocation,
            request=request,
        )
        scoped_incoming = self._scope_incoming_outputs(
            workflow_ir=workflow_ir,
            node_ir=node_ir,
            incoming=incoming,
        )
        map_policy = node_ir.policy.map if node_ir.policy is not None else None
        if map_policy is not None:
            if map_policy.item_selector is None:
                if len(incoming) != 1:
                    raise ValueError(
                        "MapPolicy without item_selector requires exactly one "
                        "incoming activation."
                    )
                return incoming[0].value, scoped_incoming
            if not incoming:
                return dict(invocation.input), scoped_incoming
            if len(incoming) == 1:
                return incoming[0].value, scoped_incoming
            return (
                {
                    item.source_node_id: item.value
                    for item in scoped_incoming
                },
                scoped_incoming,
            )
        if callable(node_ir.input_plan):
            return (
                await invoke_hook_async(
                    node_ir.input_plan,
                    InputMappingContext.create(
                        invocation_input=invocation.input,
                        invocation_context=invocation.context,
                        session_context=session.context,
                        outputs=invocation.outputs.scoped(node_ir.scope_node_ids),
                        node_id=node_ir.local_id or node_ir.id,
                        workflow_path=node_ir.workflow_path,
                        incoming=scoped_incoming,
                    ),
                ),
                scoped_incoming,
            )

        incoming_edge_ids = workflow_ir.graph.incoming_edges.get(node_ir.id, ())
        if not incoming_edge_ids:
            return dict(invocation.input), scoped_incoming

        if len(incoming) == 1:
            return incoming[0].value, scoped_incoming

        values: dict[str, Any] = {}
        for item in scoped_incoming:
            values[item.source_node_id] = item.value
        return values, scoped_incoming

    def _scope_incoming_outputs(
        self,
        *,
        workflow_ir: WorkflowIR,
        node_ir: NodeIR,
        incoming: tuple[IncomingOutput, ...],
    ) -> tuple[IncomingOutput, ...]:
        """Expose local ids for activations authored in the same child scope."""

        scoped: list[IncomingOutput] = []
        for item in incoming:
            edge = workflow_ir.edges.get(item.edge_id)
            same_scope = edge is not None and edge.workflow_path == node_ir.workflow_path
            edge_id = item.edge_id
            source_node_id = item.source_node_id
            if same_scope:
                edge_id = edge.local_id or edge.id
                source_node_id = edge.local_from_node or item.source_node_id
            scoped.append(
                IncomingOutput(
                    edge_id=edge_id,
                    source_node_id=source_node_id,
                    source_execution_id=item.source_execution_id,
                    value=item.value,
                )
            )
        return tuple(scoped)

    def _build_incoming_outputs(
        self,
        *,
        invocation: Invocation,
        request: NodeExecutionRequest,
    ) -> tuple[IncomingOutput, ...]:
        incoming: list[IncomingOutput] = []
        for activation in request.activations:
            source = invocation.get_node_execution(activation.source_execution_id)
            if source is None:
                raise KeyError(
                    "Ready request references unknown source execution: "
                    f"{activation.source_execution_id}"
                )
            incoming.append(
                IncomingOutput(
                    edge_id=activation.edge_id,
                    source_node_id=activation.source_node_id,
                    source_execution_id=activation.source_execution_id,
                    value=source.output,
                )
            )
        return tuple(incoming)

    async def _run_output_binding(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        node_execution: NodeExecution,
        output: Any,
    ) -> None:
        node_ir = workflow_ir.nodes[node_execution.node_id]
        if not callable(node_ir.output_binding):
            return
        # WorkflowExecutor is the single Context writer. The hook works on two
        # detached copies; both candidate commits are validated before either
        # authoritative Context reference is replaced.
        working_invocation_context = deepcopy(invocation.context)
        working_session_context = deepcopy(session.context)
        await invoke_hook_async(
            node_ir.output_binding,
            OutputBindingContext.create(
                invocation_input=invocation.input,
                invocation_context=working_invocation_context,
                session_context=working_session_context,
                outputs=invocation.outputs.scoped(node_ir.scope_node_ids),
                node_id=node_ir.local_id or node_ir.id,
                output=output,
                workflow_path=node_ir.workflow_path,
            ),
        )
        candidate_invocation_context = deepcopy(invocation.context)
        candidate_session_context = deepcopy(session.context)
        candidate_invocation_context.commit_isolated(
            working_invocation_context,
            base_revision=node_execution.base_invocation_context_revision,
            node_id=node_execution.node_id,
        )
        candidate_session_context.commit_isolated(
            working_session_context,
            base_revision=node_execution.base_session_context_revision,
            node_id=node_execution.node_id,
        )
        invocation.context = candidate_invocation_context
        session.context = candidate_session_context

    async def _complete_resumed_execution(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        node_execution: NodeExecution,
        output: Any,
    ) -> None:
        try:
            await self._run_output_binding(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
                node_execution=node_execution,
                output=output,
            )
        except Exception as exc:
            # Resumed completion follows the same non-retryable binding contract.
            invocation.mark_node_failed(
                node_execution.id,
                RuntimeErrorInfo(
                    code="OUTPUT_BINDING_FAILED",
                    message=str(exc),
                    detail={
                        "node_id": node_execution.node_id,
                        "error_type": type(exc).__name__,
                    },
                ),
            )
            session.mark_context_updated()
        else:
            invocation.mark_node_completed(node_execution.id, output)
            session.mark_context_updated()
            self._record_completed_user_events(
                invocation=invocation,
                node_ir=workflow_ir.nodes[node_execution.node_id],
                node_execution=node_execution,
                output=output,
            )

    def _check_node_execution_resource(
        self,
        *,
        invocation: Invocation,
        node_ir: NodeIR,
    ) -> RuntimeErrorInfo | None:
        resource = node_ir.policy.resource if node_ir.policy is not None else None
        if resource is None or resource.max_node_executions_per_invocation is None:
            return None
        actual = invocation.count_node_executions(node_ir.id)
        limit = resource.max_node_executions_per_invocation
        if actual >= limit:
            return RuntimeErrorInfo(
                code="RESOURCE_LIMIT_EXCEEDED",
                message="Node execution limit exceeded.",
                detail={
                    "scope": "invocation",
                    "resource": "node_executions",
                    "node_id": node_ir.id,
                    "limit": limit,
                    "actual": actual + 1,
                },
            )
        return None

    async def _abandon_active_work(
        self,
        session: Session,
        invocation: Invocation,
    ) -> tuple[UUID, ...]:
        error = RuntimeErrorInfo(
            code="INVOCATION_FAILED_FAST",
            message="Node execution was cancelled after fail-fast invocation failure.",
            detail={
                "invocation_error": (
                    invocation.error.to_record()
                    if invocation.error is not None
                    else None
                )
            },
        )
        return await self._cancel_unfinished_nodes(
            session=session,
            invocation=invocation,
            error=error,
        )

    async def _finish_failed_invocation(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        abandoned = await self._abandon_active_work(session, invocation)
        await self._record_event(
            session,
            invocation,
            "invocation.failed",
            node_execution_ids=abandoned,
            force_recovery_checkpoint=True,
            detail={
                "state": "failed",
                "error": (
                    invocation.error.to_record()
                    if invocation.error is not None
                    else None
                ),
            },
        )
        self._record_agent_failed_user_event(workflow_ir, invocation)
        invocation.execution_mailbox.close()
        return invocation

    async def _cancel_unfinished_nodes(
        self,
        *,
        session: Session,
        invocation: Invocation,
        error: RuntimeErrorInfo,
    ) -> tuple[UUID, ...]:
        changed_executions = tuple(
            execution
            for execution in invocation.node_executions
            if execution.state in {"created", "ready", "running", "waiting"}
        )
        invocation.cancel_active_node_executions(error)
        abandoned_messages = await self.node_executor.abandon(
            invocation.execution_mailbox
        )
        self._record_abandoned_user_events(invocation, abandoned_messages)
        await self._record_terminal_node_events(
            session=session,
            invocation=invocation,
            executions=changed_executions,
        )
        return tuple(execution.id for execution in changed_executions)

    async def _interrupt_unfinished_nodes(
        self,
        *,
        session: Session,
        invocation: Invocation,
        error: RuntimeErrorInfo,
        abandon_workers: bool = True,
    ) -> tuple[UUID, ...]:
        changed_executions = tuple(
            execution
            for execution in invocation.node_executions
            if execution.state in {"created", "ready", "running", "waiting"}
        )
        invocation.interrupt_active_node_executions(error)
        if abandon_workers:
            abandoned_messages = await self.node_executor.abandon(
                invocation.execution_mailbox
            )
            self._record_abandoned_user_events(
                invocation,
                abandoned_messages,
            )
        await self._record_terminal_node_events(
            session=session,
            invocation=invocation,
            executions=changed_executions,
        )
        return tuple(execution.id for execution in changed_executions)

    async def _record_terminal_node_events(
        self,
        *,
        session: Session,
        invocation: Invocation,
        executions: tuple[NodeExecution, ...],
    ) -> None:
        for execution in executions:
            await self._record_event(
                session,
                invocation,
                f"node.{execution.state}",
                node_execution_ids=(execution.id,),
                detail={
                    "node_id": execution.node_id,
                    "node_execution_id": str(execution.id),
                    "state": execution.state,
                    "error": (
                        execution.error.to_record()
                        if execution.error is not None
                        else None
                    ),
                },
                elapsed_ns=(
                    max(
                        0,
                        perf_counter_ns()
                        - execution.started_at_monotonic_ns,
                    )
                    if execution.started_at_monotonic_ns
                    else None
                ),
            )

    def _check_operator_attempt_resource(
        self,
        *,
        invocation: Invocation,
        node_ir: NodeIR,
    ) -> RuntimeErrorInfo | None:
        resource = node_ir.policy.resource if node_ir.policy is not None else None
        if resource is None or resource.max_operator_attempts_per_invocation is None:
            return None
        actual = invocation.count_operator_attempts(node_ir.id)
        limit = resource.max_operator_attempts_per_invocation
        if actual >= limit:
            return RuntimeErrorInfo(
                code="RESOURCE_LIMIT_EXCEEDED",
                message="Operator attempt limit exceeded.",
                detail={
                    "scope": "invocation",
                    "resource": "operator_executions",
                    "node_id": node_ir.id,
                    "limit": limit,
                    "actual": actual + 1,
                },
            )
        return None

    def _remaining_operator_attempts(
        self,
        *,
        invocation: Invocation,
        node_ir: NodeIR,
    ) -> int | None:
        """Return the remaining fallback-call budget for this node submission."""

        resource = node_ir.policy.resource if node_ir.policy is not None else None
        if resource is None or resource.max_operator_attempts_per_invocation is None:
            return None
        return max(
            0,
            resource.max_operator_attempts_per_invocation
            - invocation.count_operator_attempts(node_ir.id),
        )

    def _check_runtime_resource_after_result(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        node_execution: NodeExecution,
    ) -> RuntimeErrorInfo | None:
        node_ir = workflow_ir.nodes[node_execution.node_id]
        resource = node_ir.policy.resource if node_ir.policy is not None else None
        if resource is None or resource.max_runtime_ms_per_invocation is None:
            return None
        actual = invocation.sum_node_runtime_ms(node_ir.id)
        limit = resource.max_runtime_ms_per_invocation
        if actual > limit:
            return RuntimeErrorInfo(
                code="RESOURCE_LIMIT_EXCEEDED",
                message="Runtime limit exceeded.",
                detail={
                    "scope": "invocation",
                    "resource": "runtime_ms",
                    "node_id": node_ir.id,
                    "limit": limit,
                    "actual": actual,
                },
            )
        return None

    def _is_completed(self, *, workflow_ir: WorkflowIR, invocation: Invocation) -> bool:
        return any(
            execution.state == "completed" and execution.node_id in workflow_ir.exit_node_ids
            for execution in invocation.node_executions
        )

    def _build_invocation_result(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
    ) -> dict[str, Any]:
        outputs: dict[str, Any] = {}
        for node_id in workflow_ir.exit_node_ids:
            execution = invocation.latest_node_execution(node_id)
            if execution is not None and execution.state == "completed":
                outputs[node_id] = deepcopy(execution.output)
        if len(outputs) == 1:
            return {"output": next(iter(outputs.values()))}
        return {"outputs": outputs}
