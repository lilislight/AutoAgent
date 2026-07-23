from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias
from uuid import UUID, uuid4

from autoagent.core.runtime.scheduler import EdgeActivation, ExecutionScope, LoopIteration
from autoagent.core.runtime.status import (
    DirectOperatorExecutionReason,
    EdgeEvaluationStateValue,
    NodeExecutionStateValue,
    OperatorExecutionStateValue,
    ParallelOperatorExecutionKind,
)
from autoagent.core.runtime.time import TimestampMs, coerce_timestamp_ms, utc_timestamp_ms


@dataclass
class RuntimeErrorInfo:
    """Stable framework error data stored on logical execution records."""

    code: str
    message: str
    detail: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "detail": dict(self.detail),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any] | None) -> RuntimeErrorInfo | None:
        if record is None:
            return None
        return cls(
            code=str(record["code"]),
            message=str(record["message"]),
            detail=dict(record.get("detail", {})),
        )


@dataclass
class ResourceUsage:
    """Framework-observed operator duration."""

    duration_ms: int = 0

    def add(self, *, duration_ms: int = 0) -> None:
        self.duration_ms += duration_ms

    def to_record(self) -> dict[str, Any]:
        return {"duration_ms": self.duration_ms}

    @classmethod
    def from_record(cls, record: Mapping[str, Any] | None) -> ResourceUsage:
        return cls(duration_ms=int((record or {}).get("duration_ms", 0)))


@dataclass
class DirectOperatorExecution:
    """One retained direct attempt used by normal/retry/fallback/recovery."""

    operator_id: str
    sequence: int
    reason: DirectOperatorExecutionReason = "normal"
    id: UUID = field(default_factory=uuid4)
    state: OperatorExecutionStateValue = "running"
    input: Any | None = None
    output: Any | None = None
    error: RuntimeErrorInfo | None = None
    resource_usage: ResourceUsage = field(default_factory=ResourceUsage)
    started_at_ms: TimestampMs = field(default_factory=utc_timestamp_ms)
    ended_at_ms: TimestampMs | None = None

    @property
    def attempt_count(self) -> int:
        return 1

    def mark_completed(self, output: Any) -> None:
        self.state = "completed"
        self.output = output
        self.error = None
        self.ended_at_ms = utc_timestamp_ms()

    def mark_failed(self, error: RuntimeErrorInfo) -> None:
        self.state = "failed"
        self.error = error
        self.ended_at_ms = utc_timestamp_ms()

    def mark_interrupted(self, error: RuntimeErrorInfo) -> None:
        self.state = "interrupted"
        self.error = error
        self.ended_at_ms = utc_timestamp_ms()

    def to_record(self) -> dict[str, Any]:
        return {
            "type": "direct",
            "id": str(self.id),
            "operator_id": self.operator_id,
            "sequence": self.sequence,
            "reason": self.reason,
            "state": self.state,
            "input": self.input,
            "output": self.output,
            "error": self.error.to_record() if self.error else None,
            "resource_usage": self.resource_usage.to_record(),
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> DirectOperatorExecution:
        return cls(
            id=UUID(str(record["id"])),
            operator_id=str(record["operator_id"]),
            sequence=int(record["sequence"]),
            reason=record.get("reason", "normal"),
            state=record["state"],
            input=record.get("input"),
            output=record.get("output"),
            error=RuntimeErrorInfo.from_record(record.get("error")),
            resource_usage=ResourceUsage.from_record(record.get("resource_usage")),
            started_at_ms=coerce_timestamp_ms(record.get("started_at_ms"))
            or utc_timestamp_ms(),
            ended_at_ms=coerce_timestamp_ms(record.get("ended_at_ms")),
        )


@dataclass
class ParallelExecutionSummary:
    """Bounded information retained after map/replication temporary units expire."""

    # call_count is the number of logical map items or replicas. attempt_count
    # includes retries and fallback Operators actually invoked for those units.
    call_count: int = 0
    attempt_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    cancelled_count: int = 0
    retry_count: int = 0
    fallback_count: int = 0
    total_duration_ms: int = 0
    min_duration_ms: int | None = None
    max_duration_ms: int | None = None
    peak_parallelism: int = 0
    failure_samples: tuple[dict[str, Any], ...] = ()

    def to_record(self) -> dict[str, Any]:
        return {
            "call_count": self.call_count,
            "attempt_count": self.attempt_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "cancelled_count": self.cancelled_count,
            "retry_count": self.retry_count,
            "fallback_count": self.fallback_count,
            "total_duration_ms": self.total_duration_ms,
            "min_duration_ms": self.min_duration_ms,
            "max_duration_ms": self.max_duration_ms,
            "peak_parallelism": self.peak_parallelism,
            "failure_samples": [dict(value) for value in self.failure_samples],
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any] | None) -> ParallelExecutionSummary:
        value = record or {}
        return cls(
            call_count=int(value.get("call_count", 0)),
            attempt_count=int(
                value.get("attempt_count", value.get("call_count", 0))
            ),
            success_count=int(value.get("success_count", 0)),
            failure_count=int(value.get("failure_count", 0)),
            cancelled_count=int(value.get("cancelled_count", 0)),
            retry_count=int(value.get("retry_count", 0)),
            fallback_count=int(value.get("fallback_count", 0)),
            total_duration_ms=int(value.get("total_duration_ms", 0)),
            min_duration_ms=value.get("min_duration_ms"),
            max_duration_ms=value.get("max_duration_ms"),
            peak_parallelism=int(value.get("peak_parallelism", 0)),
            failure_samples=tuple(
                dict(item) for item in value.get("failure_samples", [])
            ),
        )


@dataclass
class ParallelOperatorExecution:
    """One logical map/replication execution without per-unit inputs or outputs."""

    kind: ParallelOperatorExecutionKind
    summary: ParallelExecutionSummary
    operator_ids: tuple[str, ...] = ()
    id: UUID = field(default_factory=uuid4)
    state: OperatorExecutionStateValue = "running"
    error: RuntimeErrorInfo | None = None
    started_at_ms: TimestampMs = field(default_factory=utc_timestamp_ms)
    ended_at_ms: TimestampMs | None = None

    @property
    def attempt_count(self) -> int:
        return self.summary.attempt_count

    def to_record(self) -> dict[str, Any]:
        return {
            "type": "parallel",
            "id": str(self.id),
            "kind": self.kind,
            "state": self.state,
            "summary": self.summary.to_record(),
            "operator_ids": list(self.operator_ids),
            "error": self.error.to_record() if self.error else None,
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> ParallelOperatorExecution:
        return cls(
            id=UUID(str(record["id"])),
            kind=record["kind"],
            state=record["state"],
            summary=ParallelExecutionSummary.from_record(record.get("summary")),
            operator_ids=tuple(str(value) for value in record.get("operator_ids", [])),
            error=RuntimeErrorInfo.from_record(record.get("error")),
            started_at_ms=coerce_timestamp_ms(record.get("started_at_ms"))
            or utc_timestamp_ms(),
            ended_at_ms=coerce_timestamp_ms(record.get("ended_at_ms")),
        )


OperatorExecution: TypeAlias = DirectOperatorExecution | ParallelOperatorExecution


def operator_execution_from_record(record: Mapping[str, Any]) -> OperatorExecution:
    if record.get("type") == "parallel":
        return ParallelOperatorExecution.from_record(record)
    return DirectOperatorExecution.from_record(record)


@dataclass
class EdgeEvaluation:
    """One durable outgoing-edge decision after a NodeExecution commits."""

    edge_id: str
    target_node_id: str
    state: EdgeEvaluationStateValue
    selected: bool
    id: UUID = field(default_factory=uuid4)
    reason: str | None = None
    created_at_ms: TimestampMs = field(default_factory=utc_timestamp_ms)
    updated_at_ms: TimestampMs = field(default_factory=utc_timestamp_ms)

    def to_record(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "edge_id": self.edge_id,
            "target_node_id": self.target_node_id,
            "state": self.state,
            "selected": self.selected,
            "reason": self.reason,
            "created_at_ms": self.created_at_ms,
            "updated_at_ms": self.updated_at_ms,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> EdgeEvaluation:
        return cls(
            id=UUID(str(record["id"])),
            edge_id=str(record["edge_id"]),
            target_node_id=str(record["target_node_id"]),
            state=record["state"],
            selected=bool(record["selected"]),
            reason=record.get("reason"),
            created_at_ms=coerce_timestamp_ms(record.get("created_at_ms"))
            or utc_timestamp_ms(),
            updated_at_ms=coerce_timestamp_ms(record.get("updated_at_ms"))
            or utc_timestamp_ms(),
        )


@dataclass
class NodeExecution:
    """One logical workflow-node execution and its bounded durable history."""

    node_id: str
    sequence: int
    id: UUID = field(default_factory=uuid4)
    state: NodeExecutionStateValue = "created"
    input: Any | None = None
    output: Any | None = None
    error: RuntimeErrorInfo | None = None
    idempotency_key: str | None = None
    recovery_of_execution_id: UUID | None = None
    recovery_attempt: int = 0
    incoming_activations: tuple[EdgeActivation, ...] = ()
    execution_scope: ExecutionScope = ()
    operator_executions: list[OperatorExecution] = field(default_factory=list)
    edge_evaluations: list[EdgeEvaluation] = field(default_factory=list)
    resource_usage: ResourceUsage = field(default_factory=ResourceUsage)
    started_at_ms: TimestampMs | None = None
    ended_at_ms: TimestampMs | None = None
    created_at_ms: TimestampMs = field(default_factory=utc_timestamp_ms)
    updated_at_ms: TimestampMs = field(default_factory=utc_timestamp_ms)

    def mark_ready(self) -> None:
        self.state = "ready"
        self.updated_at_ms = utc_timestamp_ms()

    def mark_running(self, input: Any | None = None) -> None:
        self.state = "running"
        self.input = input
        self.started_at_ms = utc_timestamp_ms()
        self.updated_at_ms = self.started_at_ms

    def mark_waiting(self, reason: str | None = None) -> None:
        self.state = "waiting"
        if reason:
            self.error = RuntimeErrorInfo(code="NODE_WAITING", message=reason)
        self.updated_at_ms = utc_timestamp_ms()

    def mark_completed(self, output: Any) -> None:
        self.state = "completed"
        self.output = output
        self.error = None
        self.ended_at_ms = utc_timestamp_ms()
        self.updated_at_ms = self.ended_at_ms

    def mark_failed(self, error: RuntimeErrorInfo) -> None:
        self.state = "failed"
        self.error = error
        self.ended_at_ms = utc_timestamp_ms()
        self.updated_at_ms = self.ended_at_ms

    def mark_cancelled(self, error: RuntimeErrorInfo | None = None) -> None:
        self.state = "cancelled"
        self.error = error
        self.ended_at_ms = utc_timestamp_ms()
        self.updated_at_ms = self.ended_at_ms

    def mark_interrupted(self, error: RuntimeErrorInfo | None = None) -> None:
        self.state = "interrupted"
        self.error = error or RuntimeErrorInfo(
            code="NODE_INTERRUPTED",
            message="Node execution was interrupted before completion.",
        )
        self.ended_at_ms = utc_timestamp_ms()
        self.updated_at_ms = self.ended_at_ms
        for execution in self.operator_executions:
            if execution.state == "running":
                execution.state = "interrupted"
                execution.error = self.error
                execution.ended_at_ms = self.ended_at_ms

    def add_edge_evaluation(
        self,
        *,
        edge_id: str,
        target_node_id: str,
        state: EdgeEvaluationStateValue,
        selected: bool,
        reason: str | None = None,
    ) -> EdgeEvaluation:
        evaluation = EdgeEvaluation(
            edge_id=edge_id,
            target_node_id=target_node_id,
            state=state,
            selected=selected,
            reason=reason,
        )
        self.edge_evaluations.append(evaluation)
        self.updated_at_ms = utc_timestamp_ms()
        return evaluation

    def to_record(self, invocation_id: UUID) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "invocation_id": str(invocation_id),
            "node_id": self.node_id,
            "sequence": self.sequence,
            "state": self.state,
            "input": self.input,
            "output": self.output,
            "error": self.error.to_record() if self.error else None,
            "idempotency_key": self.idempotency_key,
            "recovery_of_execution_id": (
                str(self.recovery_of_execution_id)
                if self.recovery_of_execution_id is not None
                else None
            ),
            "recovery_attempt": self.recovery_attempt,
            "incoming_activations": [
                activation.to_record() for activation in self.incoming_activations
            ],
            "execution_scope": [frame.to_record() for frame in self.execution_scope],
            "operator_executions": [
                execution.to_record() for execution in self.operator_executions
            ],
            "edge_evaluations": [
                evaluation.to_record() for evaluation in self.edge_evaluations
            ],
            "resource_usage": self.resource_usage.to_record(),
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
            "created_at_ms": self.created_at_ms,
            "updated_at_ms": self.updated_at_ms,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> NodeExecution:
        return cls(
            id=UUID(str(record["id"])),
            node_id=str(record["node_id"]),
            sequence=int(record["sequence"]),
            state=record["state"],
            input=record.get("input"),
            output=record.get("output"),
            error=RuntimeErrorInfo.from_record(record.get("error")),
            idempotency_key=record.get("idempotency_key"),
            recovery_of_execution_id=(
                UUID(str(record["recovery_of_execution_id"]))
                if record.get("recovery_of_execution_id") is not None
                else None
            ),
            recovery_attempt=int(record.get("recovery_attempt", 0)),
            incoming_activations=tuple(
                EdgeActivation.from_record(item)
                for item in record.get("incoming_activations", [])
            ),
            execution_scope=tuple(
                LoopIteration.from_record(item)
                for item in record.get("execution_scope", [])
            ),
            operator_executions=[
                operator_execution_from_record(item)
                for item in record.get("operator_executions", [])
            ],
            edge_evaluations=[
                EdgeEvaluation.from_record(item)
                for item in record.get("edge_evaluations", [])
            ],
            resource_usage=ResourceUsage.from_record(record.get("resource_usage")),
            started_at_ms=coerce_timestamp_ms(record.get("started_at_ms")),
            ended_at_ms=coerce_timestamp_ms(record.get("ended_at_ms")),
            created_at_ms=coerce_timestamp_ms(record.get("created_at_ms"))
            or utc_timestamp_ms(),
            updated_at_ms=coerce_timestamp_ms(record.get("updated_at_ms"))
            or utc_timestamp_ms(),
        )
