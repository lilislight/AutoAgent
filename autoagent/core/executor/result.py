from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID

from autoagent.core.runtime.execution import (
    OperatorCall,
    OperatorCallSummary,
    ParallelExecutionSummary,
    ResourceUsage,
    RuntimeErrorInfo,
)
from autoagent.core.runtime.status import NodeExecutionStateValue
from autoagent.core.runtime.time import utc_timestamp_ms
from autoagent.core.runtime.user_event import UserEventSpec


@dataclass(frozen=True)
class NodePhaseResult:
    name: str
    status: str
    elapsed_ns: int
    occurred_at_ms: int = field(default_factory=utc_timestamp_ms)
    input: Any | None = None
    output: Any | None = None
    timing: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class NodeExecutionProgress:
    """One completed internal Node step published before the Node terminates.

    WorkflowExecutor remains the only Runtime writer and event-sequence
    allocator. NodeExecutor uses this detached message to report the actual
    completion order of concurrent Node work without mutating Runtime state.
    """

    node_execution_id: UUID
    kind: Literal["phase", "operator_call", "user_event"]
    phase: NodePhaseResult | None = None
    operator_call: OperatorCall | None = None
    user_event_specs: tuple[UserEventSpec, ...] = ()

    def __post_init__(self) -> None:
        if self.kind == "phase":
            if (
                self.phase is None
                or self.operator_call is not None
                or self.user_event_specs
            ):
                raise ValueError("Phase progress must contain only a phase result.")
            return
        if self.kind == "operator_call":
            if (
                self.operator_call is None
                or self.phase is not None
                or self.user_event_specs
            ):
                raise ValueError(
                    "Operator-call progress must contain only one actual Call."
                )
            return
        if (
            not self.user_event_specs
            or self.phase is not None
            or self.operator_call is not None
        ):
            raise ValueError(
                "User-event progress must contain only UserEventSpecs."
            )


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
    operator_summary: OperatorCallSummary = field(
        default_factory=OperatorCallSummary
    )
    parallel_summary: ParallelExecutionSummary | None = None
    last_operator_call_id: UUID | None = None
    phases: tuple[NodePhaseResult, ...] = ()
