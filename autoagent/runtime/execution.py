from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from autoagent.runtime.status import (
    EdgeEvaluationStateValue,
    NodeExecutionStateValue,
    OperatorCallKind,
    OperatorCallStateValue,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RuntimeErrorInfo:
    """Structured runtime error persisted on execution records.

    code should be stable enough for framework logic and tests. message is for
    humans. detail is optional machine-readable data from executors/operators.
    """

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
    """Runtime duration observed for an execution record.

    First version tracks only duration because it can be measured by framework
    infrastructure without operator-specific adapters.

    NodeExecutor should update duration_ms on OperatorCall and NodeExecution.
    Invocation-level resource checks aggregate these values by node_id.
    """

    duration_ms: int = 0

    def add(
        self,
        *,
        duration_ms: int = 0,
    ) -> None:
        self.duration_ms += duration_ms

    def to_record(self) -> dict[str, Any]:
        return {
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any] | None) -> ResourceUsage:
        record = record or {}
        return cls(
            duration_ms=int(record.get("duration_ms", 0)),
        )


@dataclass
class OperatorCall:
    """One concrete operator call inside one logical NodeExecution.

    NodeExecution is the scheduler-visible logical execution of a workflow node.
    OperatorCall is the executor-visible call record. A single
    NodeExecution may contain many calls because retry, fallback, map, and
    replication all call an operator multiple times before producing one final
    NodeExecution.output.

    kind explains why this call exists:
      - normal: first ordinary call for the logical node execution.
      - retry: another call after a failed invocation of the same path.
      - fallback: another call using a different selected operator.
      - map_item: call for one item produced by EdgePolicy.map.item_selector.
      - replica: call for one sample requested by NodePolicy.replication.
      - recover: recovery call after an interrupted execution.

    item_index and replica_index are only set for map_item/replica. Downstream
    workflow nodes should not read these fields during normal input mapping;
    they are for NodeExecutor aggregation and tracing.
    """

    operator_id: str
    call_no: int
    id: UUID = field(default_factory=uuid4)
    kind: OperatorCallKind = "normal"
    item_index: int | None = None
    replica_index: int | None = None
    state: OperatorCallStateValue = "created"
    input: Any | None = None
    output: Any | None = None
    error: RuntimeErrorInfo | None = None
    resource_usage: ResourceUsage = field(default_factory=ResourceUsage)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def mark_running(self, input: Any | None = None) -> None:
        self.state = "running"
        self.input = input
        self.started_at = utc_now()
        self.updated_at = self.started_at

    def mark_completed(self, output: Any) -> None:
        self.state = "completed"
        self.output = output
        self.error = None
        self.ended_at = utc_now()
        self.updated_at = self.ended_at

    def mark_failed(self, error: RuntimeErrorInfo) -> None:
        self.state = "failed"
        self.error = error
        self.ended_at = utc_now()
        self.updated_at = self.ended_at

    def mark_interrupted(self, error: RuntimeErrorInfo) -> None:
        self.state = "interrupted"
        self.error = error
        self.ended_at = utc_now()
        self.updated_at = self.ended_at

    def to_record(self, node_execution_id: UUID) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "node_execution_id": str(node_execution_id),
            "operator_id": self.operator_id,
            "call_no": self.call_no,
            "kind": self.kind,
            "item_index": self.item_index,
            "replica_index": self.replica_index,
            "state": self.state,
            "input": self.input,
            "output": self.output,
            "error": self.error.to_record() if self.error else None,
            "resource_usage": self.resource_usage.to_record(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> OperatorCall:
        return cls(
            id=UUID(str(record["id"])),
            operator_id=str(record["operator_id"]),
            call_no=int(record["call_no"]),
            kind=record.get("kind", "normal"),
            item_index=record.get("item_index"),
            replica_index=record.get("replica_index"),
            state=record["state"],
            input=record.get("input"),
            output=record.get("output"),
            error=RuntimeErrorInfo.from_record(record.get("error")),
            resource_usage=ResourceUsage.from_record(record.get("resource_usage")),
            started_at=_parse_datetime(record.get("started_at")),
            ended_at=_parse_datetime(record.get("ended_at")),
            created_at=_parse_datetime(record.get("created_at")) or utc_now(),
            updated_at=_parse_datetime(record.get("updated_at")) or utc_now(),
        )


@dataclass
class EdgeEvaluation:
    """Evaluation of one outgoing edge after a NodeExecution reaches final output.

    A source node can have many outgoing edges. Scheduler appends one
    EdgeEvaluation for each inspected edge after the source NodeExecution has a
    final logical output. For map/replication nodes this means conditions see
    NodeExecution.output after aggregation, not individual OperatorCall
    outputs.
    """

    edge_id: str
    target_node_id: str
    state: EdgeEvaluationStateValue
    selected: bool
    id: UUID = field(default_factory=uuid4)
    reason: str | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def to_record(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "edge_id": self.edge_id,
            "target_node_id": self.target_node_id,
            "state": self.state,
            "selected": self.selected,
            "reason": self.reason,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
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
            created_at=_parse_datetime(record.get("created_at")) or utc_now(),
            updated_at=_parse_datetime(record.get("updated_at")) or utc_now(),
        )


@dataclass
class NodeExecution:
    """One logical execution of a workflow node inside an invocation.

    Scheduler and downstream OutputContext only observe this object at the
    logical-node level. Retry, fallback, map item calls, and replication samples
    are internal OperatorCall records. NodeExecutor must not enqueue graph
    transitions until this object has one final output, a final error, or a
    stable waiting state.

    input:
        Logical input built by node input_mapping or runtime defaults before the
        operator execution starts. For map, the source edge item_selector may
        produce multiple operator call inputs internally; this field still
        represents the logical node input.

    output:
        Logical output visible to downstream input_mapping/condition through
        OutputContext. For normal/retry/fallback it is the successful
        OperatorCall output. For map/replication it is
        output_aggregator(operator_outputs).

    operator_calls:
        Ordered concrete operator calls made while completing this logical node.
        This is where map item outputs, replication samples, retry calls, and
        fallback calls are stored for tracing and recovery.

    edge_evaluations:
        Scheduler writes one entry for each outgoing edge evaluated after this
        NodeExecution has final output/state.
    """

    node_id: str
    sequence: int
    id: UUID = field(default_factory=uuid4)
    state: NodeExecutionStateValue = "created"
    input: Any | None = None
    output: Any | None = None
    error: RuntimeErrorInfo | None = None
    idempotency_key: str | None = None
    operator_calls: list[OperatorCall] = field(default_factory=list)
    edge_evaluations: list[EdgeEvaluation] = field(default_factory=list)
    resource_usage: ResourceUsage = field(default_factory=ResourceUsage)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def mark_ready(self) -> None:
        self.state = "ready"
        self.updated_at = utc_now()

    def mark_running(self, input: Any | None = None) -> None:
        self.state = "running"
        self.input = input
        self.started_at = utc_now()
        self.updated_at = self.started_at

    def mark_waiting(self, reason: str | None = None) -> None:
        self.state = "waiting"
        if reason:
            self.error = RuntimeErrorInfo(code="NODE_WAITING", message=reason)
        self.updated_at = utc_now()

    def mark_completed(self, output: Any) -> None:
        self.state = "completed"
        self.output = output
        self.error = None
        self.ended_at = utc_now()
        self.updated_at = self.ended_at

    def mark_failed(self, error: RuntimeErrorInfo) -> None:
        self.state = "failed"
        self.error = error
        self.ended_at = utc_now()
        self.updated_at = self.ended_at

    def mark_interrupted(self, error: RuntimeErrorInfo | None = None) -> None:
        self.state = "interrupted"
        self.error = error or RuntimeErrorInfo(
            code="NODE_INTERRUPTED",
            message="Node execution was interrupted before completion.",
        )
        self.ended_at = utc_now()
        self.updated_at = self.ended_at
        for call in self.operator_calls:
            if call.state == "running":
                call.mark_interrupted(self.error)

    def add_operator_call(
        self,
        operator_id: str,
        *,
        kind: OperatorCallKind = "normal",
        item_index: int | None = None,
        replica_index: int | None = None,
    ) -> OperatorCall:
        call = OperatorCall(
            operator_id=operator_id,
            call_no=len(self.operator_calls) + 1,
            kind=kind,
            item_index=item_index,
            replica_index=replica_index,
        )
        self.operator_calls.append(call)
        self.updated_at = utc_now()
        return call

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
        self.updated_at = utc_now()
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
            "edge_evaluations": [
                evaluation.to_record() for evaluation in self.edge_evaluations
            ],
            "resource_usage": self.resource_usage.to_record(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, Any],
        *,
        operator_calls: list[OperatorCall] | None = None,
    ) -> NodeExecution:
        return cls(
            id=UUID(str(record["id"])),
            node_id=str(record["node_id"]),
            sequence=int(record["sequence"]),
            state=record["state"],
            input=record.get("input"),
            output=record.get("output"),
            error=RuntimeErrorInfo.from_record(record.get("error")),
            idempotency_key=record.get("idempotency_key"),
            operator_calls=list(operator_calls or []),
            edge_evaluations=[
                EdgeEvaluation.from_record(item)
                for item in record.get("edge_evaluations", [])
            ],
            resource_usage=ResourceUsage.from_record(record.get("resource_usage")),
            started_at=_parse_datetime(record.get("started_at")),
            ended_at=_parse_datetime(record.get("ended_at")),
            created_at=_parse_datetime(record.get("created_at")) or utc_now(),
            updated_at=_parse_datetime(record.get("updated_at")) or utc_now(),
        )


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
