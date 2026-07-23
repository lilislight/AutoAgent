from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from autoagent.core.runtime.execution import (
    OperatorExecution,
    ResourceUsage,
    RuntimeErrorInfo,
)
from autoagent.core.runtime.status import NodeExecutionStateValue


@dataclass
class NodeExecutionResult:
    """Detached worker result applied by WorkflowExecutor on its control path."""

    node_execution_id: UUID
    state: NodeExecutionStateValue
    output: Any | None = None
    error: RuntimeErrorInfo | None = None
    resource_usage: ResourceUsage = field(default_factory=ResourceUsage)
    wait_key: str | None = None
    wait_type: str | None = None
    wait_payload: dict[str, Any] | None = None
    operator_executions: tuple[OperatorExecution, ...] = ()
