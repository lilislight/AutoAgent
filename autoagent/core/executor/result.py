from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from autoagent.core.operators import OperatorManifest
from autoagent.core.runtime import OperatorCall, ResourceUsage, RuntimeErrorInfo
from autoagent.core.runtime.status import (
    NodeExecutionStateValue,
    OperatorCallKind,
    OperatorCallStateValue,
)


@dataclass
class OperatorCallResult:
    """Worker-produced result for one concrete Operator call.

    Capability fallback may produce several of these inside one logical
    NodeExecutionResult. Each call is already checkpointed independently;
    WorkflowExecutor attaches the same identities to the live NodeExecution
    after worker code returns.
    """

    id: UUID
    call_no: int
    operator_id: str
    operator_manifest: OperatorManifest
    kind: OperatorCallKind
    state: OperatorCallStateValue
    input: Any | None = None
    output: Any | None = None
    error: RuntimeErrorInfo | None = None
    resource_usage: ResourceUsage = field(default_factory=ResourceUsage)
    item_index: int | None = None
    replica_index: int | None = None
    created_at_ms: int | None = None
    started_at_ms: int | None = None
    ended_at_ms: int | None = None

    def to_operator_call(self) -> OperatorCall:
        """Rebuild the same call persisted by the execution worker.

        OperatorCall is checkpointed before and after user code runs. The main
        workflow loop later attaches that exact record to NodeExecution, so its
        ID and timestamps must not be regenerated here.
        """

        updated_at_ms = (
            self.ended_at_ms or self.started_at_ms or self.created_at_ms
        )
        kwargs: dict[str, Any] = {}
        if self.created_at_ms is not None:
            kwargs["created_at_ms"] = self.created_at_ms
        if updated_at_ms is not None:
            kwargs["updated_at_ms"] = updated_at_ms
        return OperatorCall(
            id=self.id,
            call_no=self.call_no,
            operator_id=self.operator_id,
            operator_manifest=self.operator_manifest,
            kind=self.kind,
            item_index=self.item_index,
            replica_index=self.replica_index,
            state=self.state,
            input=self.input,
            output=self.output,
            error=self.error,
            resource_usage=self.resource_usage,
            started_at_ms=self.started_at_ms,
            ended_at_ms=self.ended_at_ms,
            **kwargs,
        )


@dataclass
class NodeExecutionResult:
    """Result returned by an execution lane for one logical NodeExecution.

    Execution lanes run user/operator code outside the WorkflowExecutor loop.
    They must not mutate Invocation, SchedulerContext, or NodeExecution state.
    Instead they return this result. WorkflowExecutor applies the result on the
    main control path and persists the changed runtime state.
    """

    node_execution_id: UUID
    state: NodeExecutionStateValue
    output: Any | None = None
    error: RuntimeErrorInfo | None = None
    resource_usage: ResourceUsage = field(default_factory=ResourceUsage)
    wait_key: str | None = None
    wait_type: str | None = None
    wait_payload: dict[str, Any] | None = None
    operator_calls: tuple[OperatorCallResult, ...] = ()
