"""Safe user-facing projections independent from Runtime Event capture."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from .events import (
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    ChildAwaitReady,
    ChildAwaitSuspended,
    InvocationCancelled,
    InvocationCompleted,
    InvocationFailed,
    InvocationOpened,
    InvocationRecoveryRequested,
    InvocationStarted,
    InvocationWaiting,
    NodeOccurrenceCompleted,
    NodeOccurrenceFailed,
    NodeOccurrenceStarted,
    NodeOccurrenceWaiting,
    OperatorCallCompleted,
    OperatorCallFailed,
    OperatorCallStarted,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeLog,
    StateTransition,
    SchedulerInitialized,
    SessionOpened,
    WaitResumed,
)
from .values import DurableValue, freeze, thaw


TRACE_EVENT_SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class TraceEvent:
    """One allowlisted semantic observation with no recovery State changes."""

    id: str
    session_id: str
    trace_sequence: int
    kind: str
    occurred_at_ns: int
    invocation_id: str | None = None
    causation_id: str | None = None
    state_version: int | None = None
    subject_ids: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )
    status: str | None = None
    error: RuntimeErrorInfo | None = None
    metrics: DurableValue = None
    attributes: Mapping[str, DurableValue] = field(
        default_factory=lambda: MappingProxyType({})
    )
    schema_version: int = TRACE_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name, value in (
            ("id", self.id),
            ("session_id", self.session_id),
            ("kind", self.kind),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Trace Event {name} cannot be empty.")
        for name, value in (
            ("invocation_id", self.invocation_id),
            ("causation_id", self.causation_id),
            ("status", self.status),
        ):
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(f"Trace Event {name} must be non-empty or None.")
        for name, value, minimum in (
            ("trace_sequence", self.trace_sequence, 1),
            ("occurred_at_ns", self.occurred_at_ns, 0),
        ):
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < minimum
            ):
                raise ValueError(f"Trace Event {name} is invalid.")
        if self.state_version is not None and (
            not isinstance(self.state_version, int)
            or isinstance(self.state_version, bool)
            or self.state_version < 0
        ):
            raise ValueError("Trace Event state_version must be non-negative or None.")
        if self.schema_version != TRACE_EVENT_SCHEMA_VERSION:
            raise ValueError(f"Unsupported Trace Event schema {self.schema_version}.")
        if not isinstance(self.subject_ids, Mapping) or not all(
            isinstance(key, str)
            and key.strip()
            and isinstance(value, str)
            and value.strip()
            for key, value in self.subject_ids.items()
        ):
            raise TypeError("Trace Event subject_ids must map strings to strings.")
        if not isinstance(self.attributes, Mapping):
            raise TypeError("Trace Event attributes must be a mapping.")
        subjects = dict(self.subject_ids)
        attributes = freeze(dict(self.attributes))
        assert isinstance(attributes, Mapping)
        object.__setattr__(self, "subject_ids", MappingProxyType(subjects))
        object.__setattr__(self, "attributes", attributes)
        object.__setattr__(self, "metrics", freeze(self.metrics))

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "id": self.id,
            "session_id": self.session_id,
            "trace_sequence": self.trace_sequence,
            "kind": self.kind,
            "occurred_at_ns": self.occurred_at_ns,
            "invocation_id": self.invocation_id,
            "causation_id": self.causation_id,
            "state_version": self.state_version,
            "subject_ids": dict(self.subject_ids),
            "status": self.status,
            "error": _error_record(self.error),
            "metrics": thaw(self.metrics),
            "attributes": thaw(self.attributes),
        }

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "TraceEvent":
        """Decode only the exact current Trace Event schema."""

        if not isinstance(record, dict):
            raise TypeError("Trace Event record must be a mapping.")
        expected_fields = {
            "schema_version",
            "id",
            "session_id",
            "trace_sequence",
            "kind",
            "occurred_at_ns",
            "invocation_id",
            "causation_id",
            "state_version",
            "subject_ids",
            "status",
            "error",
            "metrics",
            "attributes",
        }
        if set(record) != expected_fields:
            raise TypeError("Trace Event record contains missing or unknown fields.")
        schema_version = _integer(record, "schema_version", minimum=1)
        if schema_version != TRACE_EVENT_SCHEMA_VERSION:
            raise ValueError(f"Unsupported Trace Event schema {schema_version}.")
        subject_ids = record.get("subject_ids")
        attributes = record.get("attributes")
        if not isinstance(subject_ids, dict) or not isinstance(attributes, dict):
            raise TypeError("Trace Event subjects and attributes must be mappings.")
        error_record = record.get("error")
        if error_record is not None and not isinstance(error_record, dict):
            raise TypeError("Trace Event error must be a mapping or None.")
        event = cls(
            id=_string(record, "id"),
            session_id=_string(record, "session_id"),
            trace_sequence=_integer(record, "trace_sequence", minimum=1),
            kind=_string(record, "kind"),
            occurred_at_ns=_integer(record, "occurred_at_ns", minimum=0),
            invocation_id=_optional_string(record, "invocation_id"),
            causation_id=_optional_string(record, "causation_id"),
            state_version=_optional_integer(record, "state_version"),
            subject_ids=subject_ids,  # type: ignore[arg-type]
            status=_optional_string(record, "status"),
            error=(
                _error_from_record(error_record)
                if isinstance(error_record, dict)
                else None
            ),
            metrics=record.get("metrics"),  # type: ignore[arg-type]
            attributes=attributes,  # type: ignore[arg-type]
            schema_version=schema_version,
        )
        if event.to_record() != record:
            raise TypeError("Trace Event record is not canonical.")
        return event


def project_trace_event(
    session_id: str,
    trace_sequence: int,
    log: RuntimeLog | StateTransition,
) -> TraceEvent:
    """Project one immediate Log/Transition at a caller-owned Trace sequence."""

    if isinstance(log, StateTransition):
        if log.session_id != session_id:
            raise ValueError("Trace projection Session does not match its Transition.")
        log = RuntimeLog(
            id=log.id,
            payload=log.payload,
            invocation_id=log.invocation_id,
            occurred_at_ns=log.occurred_at_ns,
            causation_id=log.causation_id,
        )
    subjects, status, error, metrics, attributes = _safe_projection(log.payload)
    return TraceEvent(
        id=log.id,
        session_id=session_id,
        trace_sequence=trace_sequence,
        kind=log.event_name,
        occurred_at_ns=log.occurred_at_ns,
        invocation_id=log.invocation_id,
        causation_id=log.causation_id,
        state_version=log.state_version,
        subject_ids=subjects,
        status=status,
        error=error,
        metrics=metrics,
        attributes=attributes,
    )


def project_trace_events(
    event: RuntimeEvent, *, start_sequence: int
) -> tuple[TraceEvent, ...]:
    """Project a sealed Event when a Host supplies its own Trace sequence."""

    return tuple(
        project_trace_event(event.session_id, start_sequence + index, log)
        for index, log in enumerate(event.logs)
    )


def _safe_projection(payload) -> tuple[
    dict[str, str],
    str | None,
    RuntimeErrorInfo | None,
    DurableValue,
    dict[str, DurableValue],
]:
    subjects: dict[str, str] = {}
    attributes: dict[str, DurableValue] = {}
    status: str | None = None
    error: RuntimeErrorInfo | None = None
    metrics: DurableValue = None

    if isinstance(payload, SessionOpened):
        status = "opened"
    elif isinstance(payload, InvocationOpened):
        subjects["workflow_id"] = payload.workflow_id
        subjects["workflow_revision_id"] = payload.workflow_revision_id
        subjects["entry_node_id"] = payload.entry_node_id
        status = "created"
    elif isinstance(payload, InvocationStarted):
        status = "running"
    elif isinstance(payload, InvocationWaiting):
        status = "waiting"
    elif isinstance(payload, InvocationCompleted):
        status = "completed"
    elif isinstance(payload, InvocationFailed):
        status, error = "failed", payload.error
    elif isinstance(payload, InvocationCancelled):
        status = "cancelled"
    elif isinstance(payload, SchedulerInitialized):
        status = "initialized"
    elif isinstance(payload, NodeOccurrenceStarted):
        subjects["occurrence_id"] = payload.occurrence_id
        status = "running"
    elif isinstance(payload, OperatorCallStarted):
        subjects.update(
            {
                "call_id": payload.call_id,
                "occurrence_id": payload.occurrence_id,
                "operator_id": payload.operator_id,
            }
        )
        attributes.update(
            {
                "unit_index": payload.unit_index,
            }
        )
        status = "running"
    elif isinstance(payload, OperatorCallCompleted):
        subjects["call_id"] = payload.call_id
        status = "completed"
    elif isinstance(payload, OperatorCallFailed):
        subjects["call_id"] = payload.call_id
        status, error = "failed", payload.error
    elif isinstance(payload, NodeOccurrenceWaiting):
        subjects.update(
            {"occurrence_id": payload.occurrence_id, "wait_id": payload.wait_id}
        )
        status = "waiting"
    elif isinstance(payload, WaitResumed):
        subjects["wait_id"] = payload.wait_id
        status = "resumed"
    elif isinstance(payload, InvocationRecoveryRequested):
        status = "recovering"
    elif isinstance(payload, ChildInvocationPlanned):
        subjects.update(
            {
                "creation_id": payload.creation_id,
                "parent_occurrence_id": payload.parent_occurrence_id,
                "workflow_id": payload.workflow_id,
                "workflow_revision_id": payload.workflow_revision_id,
            }
        )
        attributes.update({"mode": payload.mode, "unit_count": len(payload.units)})
        status = "planned"
    elif isinstance(payload, ChildInvocationPhaseChanged):
        subjects["creation_id"] = payload.creation_id
        attributes["unit_index"] = payload.unit_index
        status = payload.phase
    elif isinstance(payload, (ChildAwaitSuspended, ChildAwaitReady)):
        subjects.update(
            {
                "creation_id": payload.creation_id,
                "parent_occurrence_id": payload.parent_occurrence_id,
            }
        )
        status = "waiting" if isinstance(payload, ChildAwaitSuspended) else "ready"
    elif isinstance(payload, NodeOccurrenceCompleted):
        subjects["occurrence_id"] = payload.occurrence_id
        status = "completed"
        metrics = payload.metrics
    elif isinstance(payload, NodeOccurrenceFailed):
        subjects["occurrence_id"] = payload.occurrence_id
        status, error = "failed", payload.error
    return subjects, status, error, metrics, attributes


def _error_record(error: RuntimeErrorInfo | None) -> dict[str, object] | None:
    if error is None:
        return None
    return {
        "type": error.type,
        "message": error.message,
        "code": error.code,
        "phase": error.phase,
        "cause": error.cause,
    }


def _error_from_record(record: dict[str, object]) -> RuntimeErrorInfo:
    expected_fields = {"type", "message", "code", "phase", "cause"}
    if set(record) != expected_fields:
        raise TypeError("Trace Event error contains missing or unknown fields.")
    return RuntimeErrorInfo(
        type=_string(record, "type"),
        message=_string(record, "message"),
        code=_optional_string(record, "code"),
        phase=_optional_string(record, "phase"),
        cause=_optional_string(record, "cause"),
    )


def _string(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"Trace Event {key} must be a non-empty string.")
    return value


def _optional_string(record: dict[str, object], key: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"Trace Event {key} must be a non-empty string or None.")
    return value


def _integer(record: dict[str, object], key: str, *, minimum: int) -> int:
    value = record.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise TypeError(f"Trace Event {key} must be an integer >= {minimum}.")
    return value


def _optional_integer(record: dict[str, object], key: str) -> int | None:
    value = record.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise TypeError(f"Trace Event {key} must be a non-negative integer or None.")
    return value


__all__ = [
    "TRACE_EVENT_SCHEMA_VERSION",
    "TraceEvent",
    "project_trace_event",
    "project_trace_events",
]
