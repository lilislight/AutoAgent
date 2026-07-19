from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID, uuid4

from autoagent.core.runtime.context import InvocationContext
from autoagent.core.runtime.execution import (
    NodeExecution,
    RuntimeErrorInfo,
)
from autoagent.core.runtime.output import OutputContext
from autoagent.core.runtime.mailbox import InvocationExecutionMailbox
from autoagent.core.runtime.scheduler import SchedulerContext
from autoagent.core.runtime.scheduler import EdgeActivation
from autoagent.core.runtime.status import InvocationStateValue
from autoagent.core.runtime.time import TimestampMs, coerce_timestamp_ms, utc_timestamp_ms


class Invocation:
    """One workflow invocation and all runtime-owned data needed to continue it.

    Invocation is the execution root below Session. WorkflowExecutor owns its
    lifecycle and should use public methods here instead of mutating fields
    directly when a change should be visible to tracing or persistence.

    workflow_id/workflow_version:
        Copied from WorkflowIR at invocation creation. They let persisted
        records explain which workflow definition produced this history.

    workflow_definition_hash:
        Canonical compiled graph identity. Durable recovery compares it with the
        currently registered Workflow before replaying or resuming any work.

    workflow_operator_manifest_hash:
        Exact Operator compatibility environment captured at compilation. It is
        checked separately because Operator upgrades do not change graph identity.

    entry_node_id:
        The selected entry node for this invocation. __init__ enqueues one
        NodeExecutionRequest for it unless the invocation is being restored.

    input:
        Caller input for this invocation. InputMappingContext reads it; runtime
        should treat it as immutable after invocation creation.

    context:
        User-controlled per-invocation data. OutputBindingContext may mutate it.
        Scheduler state and node outputs do not live here.

    scheduler:
        Scheduler-owned cursor: ready requests, waiting executions, transitions.
        It is persisted with the invocation so wait/resume and crash recovery can
        continue from the last known scheduling point.

    node_executions:
        Ordered history of logical node executions. Loops append another record
        for the same node_id instead of overwriting older records.
    """

    def __init__(
        self,
        workflow_id: str,
        workflow_version: str | int | None,
        entry_node_id: str,
        input: dict[str, Any] | None = None,
        *,
        workflow_definition_hash: str | None = None,
        workflow_operator_manifest_hash: str | None = None,
        id: UUID | None = None,
        state: InvocationStateValue = "created",
        context: InvocationContext | None = None,
        result: dict[str, Any] | None = None,
        scheduler: SchedulerContext | None = None,
        node_executions: list[NodeExecution] | None = None,
        error: RuntimeErrorInfo | None = None,
        created_at_ms: TimestampMs | None = None,
        updated_at_ms: TimestampMs | None = None,
        initialize_entry: bool = True,
        execution_mailbox: InvocationExecutionMailbox | None = None,
    ) -> None:
        self.id = id or uuid4()
        self.workflow_id = workflow_id
        self.workflow_version = workflow_version
        self.workflow_definition_hash = workflow_definition_hash
        self.workflow_operator_manifest_hash = workflow_operator_manifest_hash
        self.entry_node_id = entry_node_id
        self.state: InvocationStateValue = state
        self.input: dict[str, Any] = dict(input or {})
        self.context = context or InvocationContext()
        self.result = result
        self.scheduler = scheduler or SchedulerContext()
        self.node_executions: list[NodeExecution] = list(node_executions or [])
        # Worker futures are process-local and must never be serialized. Keeping
        # them on the Invocation prevents one session from consuming another
        # invocation's results when an App executes sessions concurrently.
        self.execution_mailbox = execution_mailbox or InvocationExecutionMailbox()
        self.error = error
        self.created_at_ms = created_at_ms or utc_timestamp_ms()
        self.updated_at_ms = updated_at_ms or self.created_at_ms

        if initialize_entry and not self.node_executions and not self.scheduler.ready_queue:
            # New invocations start by asking WorkflowExecutor to create one
            # NodeExecution for the selected entry node. SchedulerContext stores
            # the request only; actual NodeExecution history starts when the
            # executor drains this ready queue.
            self.scheduler.enqueue_ready(entry_node_id)

    @property
    def outputs(self) -> OutputContext:
        return OutputContext.from_executions(self.node_executions)

    def mark_running(self) -> None:
        self.state = "running"
        self.updated_at_ms = utc_timestamp_ms()

    def mark_waiting(self) -> None:
        self.state = "waiting"
        self.updated_at_ms = utc_timestamp_ms()

    def mark_completed(self, result: dict[str, Any] | None = None) -> None:
        self.state = "completed"
        self.result = result
        self.error = None
        self.updated_at_ms = utc_timestamp_ms()

    def mark_failed(self, error: RuntimeErrorInfo) -> None:
        self.state = "failed"
        self.error = error
        self.updated_at_ms = utc_timestamp_ms()

    def mark_cancelled(self) -> None:
        self.state = "cancelled"
        self.updated_at_ms = utc_timestamp_ms()

    def mark_interrupted(self, error: RuntimeErrorInfo | None = None) -> None:
        """Finalize an Invocation that cannot be replayed after process loss.

        Durable recovery calls this only after current Workflow and Operator
        compatibility checks reject automatic whole-node replay. Interrupted is
        terminal for this Invocation, so Session admission may accept a new one.
        """

        self.state = "interrupted"
        self.error = error or RuntimeErrorInfo(
            code="INVOCATION_INTERRUPTED",
            message="Invocation could not be recovered after process loss.",
        )
        self.updated_at_ms = utc_timestamp_ms()

    def create_node_execution(
        self,
        node_id: str,
        *,
        input: Any | None = None,
        idempotency_key: str | None = None,
        recovery_of_execution_id: UUID | None = None,
        recovery_attempt: int = 0,
        incoming_activations: tuple[EdgeActivation, ...] = (),
    ) -> NodeExecution:
        """Append a logical NodeExecution created from a ready request.

        WorkflowExecutor calls this after draining SchedulerContext.ready_queue.
        NodeExecutor then moves the returned object through running/completed,
        failed, or waiting. The append-only list is what makes loops and tracing
        readable.
        """

        execution = NodeExecution(
            node_id=node_id,
            sequence=self._next_node_sequence(),
            input=input,
            idempotency_key=idempotency_key,
            recovery_of_execution_id=recovery_of_execution_id,
            recovery_attempt=recovery_attempt,
            incoming_activations=incoming_activations,
        )
        execution.mark_ready()
        self.node_executions.append(execution)
        self.updated_at_ms = utc_timestamp_ms()
        return execution

    def get_node_execution(self, execution_id: UUID) -> NodeExecution | None:
        for execution in self.node_executions:
            if execution.id == execution_id:
                return execution
        return None

    def latest_node_execution(self, node_id: str) -> NodeExecution | None:
        for execution in reversed(self.node_executions):
            if execution.node_id == node_id:
                return execution
        return None

    def count_node_executions(self, node_id: str) -> int:
        """Count logical executions for one node_id in this Invocation.

        WorkflowExecutor should use this before creating another NodeExecution
        when enforcing ResourcePolicy.max_node_executions_per_invocation.
        """

        return sum(1 for execution in self.node_executions if execution.node_id == node_id)

    def count_operator_calls(self, node_id: str) -> int:
        """Count concrete OperatorCalls for one node_id in this Invocation.

        NodeExecutor should use this before creating another OperatorCall when
        enforcing ResourcePolicy.max_operator_calls_per_invocation.
        """

        return sum(
            len(execution.operator_calls)
            for execution in self.node_executions
            if execution.node_id == node_id
        )

    def sum_node_runtime_ms(self, node_id: str) -> int:
        """Sum accumulated runtime for one node_id in this Invocation.

        The primary source is NodeExecution.resource_usage.duration_ms. If an
        execution does not have explicit usage but has timestamps, the method
        derives a best-effort duration from started_at/ended_at for tests,
        tracing, and early executor implementations.
        """

        total = 0
        for execution in self.node_executions:
            if execution.node_id != node_id:
                continue
            if execution.resource_usage.duration_ms:
                total += execution.resource_usage.duration_ms
            elif (
                execution.started_at_ms is not None
                and execution.ended_at_ms is not None
            ):
                total += max(0, execution.ended_at_ms - execution.started_at_ms)
        return total

    def mark_node_running(
        self,
        execution_id: UUID,
        *,
        input: Any | None = None,
    ) -> NodeExecution:
        execution = self._require_node_execution(execution_id)
        execution.mark_running(input=input)
        self.mark_running()
        return execution

    def mark_node_completed(self, execution_id: UUID, output: Any) -> NodeExecution:
        """Finalize a NodeExecution and expose a scheduler transition."""

        execution = self._require_node_execution(execution_id)
        execution.mark_completed(output)
        self.scheduler.enqueue_transition(
            node_execution_id=execution.id,
            node_id=execution.node_id,
            state=execution.state,
        )
        self.updated_at_ms = utc_timestamp_ms()
        return execution

    def mark_node_failed(
        self,
        execution_id: UUID,
        error: RuntimeErrorInfo,
    ) -> NodeExecution:
        """Finalize a NodeExecution failure and expose it to scheduler."""

        execution = self._require_node_execution(execution_id)
        execution.mark_failed(error)
        self.scheduler.enqueue_transition(
            node_execution_id=execution.id,
            node_id=execution.node_id,
            state=execution.state,
        )
        self.updated_at_ms = utc_timestamp_ms()
        return execution

    def mark_node_waiting(
        self,
        execution_id: UUID,
        *,
        wait_key: str,
        wait_type: str | None = None,
        payload: dict[str, Any] | None = None,
        reason: str | None = None,
        pending_output: Any | None = None,
    ) -> NodeExecution:
        """Pause a NodeExecution on an externally resumable wait key.

        NodeExecutor/system commands call this for human approval, webhook,
        long timer, or generic external signal waits. The wait entry is keyed by
        wait_key because the same static node can wait multiple times in loops.
        """

        execution = self._require_node_execution(execution_id)
        execution.output = pending_output
        execution.mark_waiting(reason=reason)
        self.scheduler.add_waiting_execution(
            wait_key=wait_key,
            node_execution_id=execution.id,
            node_id=execution.node_id,
            wait_type=wait_type,
            payload=payload,
        )
        self.scheduler.enqueue_transition(
            node_execution_id=execution.id,
            node_id=execution.node_id,
            state=execution.state,
        )
        # The node can wait while independent branches are still ready or
        # running. WorkflowExecutor marks the whole Invocation waiting only at
        # the stable barrier where no further local progress is possible.
        self.updated_at_ms = utc_timestamp_ms()
        return execution

    def resume_waiting_node(
        self,
        *,
        wait_key: str,
        output: Any,
    ) -> NodeExecution:
        """Complete a waiting NodeExecution from an external resume signal."""

        waiting = self.scheduler.remove_waiting_execution(wait_key)
        execution = self._require_node_execution(waiting.node_execution_id)
        execution.mark_completed(output)
        self.scheduler.enqueue_transition(
            node_execution_id=execution.id,
            node_id=execution.node_id,
            state=execution.state,
        )
        if not self.scheduler.waiting_executions:
            self.mark_running()
        return execution

    def recover_interrupted_executions(self) -> list[NodeExecution]:
        """Mark executions that were running during process loss as interrupted."""

        interrupted: list[NodeExecution] = []
        error = RuntimeErrorInfo(
            code="WORKER_LOST",
            message="Node execution was running when runtime recovery started.",
        )
        for execution in self.node_executions:
            if execution.state == "running":
                execution.mark_interrupted(error)
                interrupted.append(execution)
        if interrupted:
            self.state = "interrupted"
            self.updated_at_ms = utc_timestamp_ms()
        return interrupted

    def cancel_active_node_executions(self, reason: RuntimeErrorInfo) -> None:
        """Finalize logical work whose worker results are abandoned by fail-fast."""

        for execution in self.node_executions:
            if execution.state in {"created", "ready", "running", "waiting"}:
                execution.mark_cancelled(reason)
        self.scheduler.ready_queue.clear()
        self.scheduler.waiting_executions.clear()
        self.scheduler.transition_queue.clear()
        self.updated_at_ms = utc_timestamp_ms()

    def interrupt_active_node_executions(self, reason: RuntimeErrorInfo) -> None:
        """Make all unfinished work terminal after recovery is rejected."""

        for execution in self.node_executions:
            if execution.state in {"created", "ready", "running", "waiting"}:
                execution.mark_interrupted(reason)
        self.scheduler.ready_queue.clear()
        self.scheduler.waiting_executions.clear()
        self.scheduler.transition_queue.clear()
        self.mark_interrupted(reason)

    def to_record(self, session_id: UUID) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "session_id": str(session_id),
            "workflow_id": self.workflow_id,
            "workflow_version": self.workflow_version,
            "workflow_definition_hash": self.workflow_definition_hash,
            "workflow_operator_manifest_hash": self.workflow_operator_manifest_hash,
            "entry_node_id": self.entry_node_id,
            "state": self.state,
            "input": dict(self.input),
            "context": self.context.to_record(),
            "result": self.result,
            "scheduler": self.scheduler.to_record(),
            "error": self.error.to_record() if self.error else None,
            "created_at_ms": self.created_at_ms,
            "updated_at_ms": self.updated_at_ms,
        }

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, Any],
        *,
        node_executions: list[NodeExecution] | None = None,
    ) -> Invocation:
        return cls(
            id=UUID(str(record["id"])),
            workflow_id=str(record["workflow_id"]),
            workflow_version=record.get("workflow_version"),
            workflow_definition_hash=record.get("workflow_definition_hash"),
            workflow_operator_manifest_hash=record.get(
                "workflow_operator_manifest_hash"
            ),
            entry_node_id=str(record["entry_node_id"]),
            state=record["state"],
            input=dict(record.get("input", {})),
            context=InvocationContext.from_record(
                record.get("context", {"data": record.get("data", {})})
            ),
            result=record.get("result"),
            scheduler=SchedulerContext.from_record(record.get("scheduler")),
            node_executions=list(node_executions or []),
            error=RuntimeErrorInfo.from_record(record.get("error")),
            created_at_ms=coerce_timestamp_ms(
                record.get("created_at_ms", record.get("created_at"))
            ),
            updated_at_ms=coerce_timestamp_ms(
                record.get("updated_at_ms", record.get("updated_at"))
            ),
            initialize_entry=False,
        )

    def _next_node_sequence(self) -> int:
        return len(self.node_executions) + 1

    def _require_node_execution(self, execution_id: UUID) -> NodeExecution:
        execution = self.get_node_execution(execution_id)
        if execution is None:
            raise KeyError(f"Unknown node execution: {execution_id}")
        return execution
