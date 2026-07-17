from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from autoagent.operators import OperatorManifest
from autoagent.runtime import ResourceUsage, RuntimeErrorInfo
from autoagent.runtime.status import (
    NodeExecutionStateValue,
    OperatorCallKind,
    OperatorCallStateValue,
)


@dataclass
class OperatorCallResult:
    """Worker-produced result for one concrete Operator call.

    Capability fallback may produce several of these inside one logical
    NodeExecutionResult. WorkflowExecutor converts them into persistent
    OperatorCall records in order after worker code returns.
    """

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
