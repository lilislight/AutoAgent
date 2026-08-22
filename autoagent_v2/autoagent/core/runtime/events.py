"""Semantic Full-mode Runtime Events.

An Event records one atomic domain transition. It carries the transition data,
not a generic patch list and not a copy of the resulting Runtime State.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import ClassVar, Literal, TypeAlias
from uuid import uuid4

from .scheduling import SchedulerDelta, delta_from_record, delta_to_record
from .operations import StateOperation, StateOperationBatch
from .values import DurableValue, freeze, thaw
from ..context import ContextOperation, ContextPatch


RUNTIME_EVENT_SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class RuntimeErrorInfo:
    type: str
    message: str
    code: str | None = None
    phase: str | None = None
    retryable: bool | None = None
    cause: str | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.type, str)
            or not self.type.strip()
            or not isinstance(self.message, str)
            or not self.message.strip()
        ):
            raise ValueError("Runtime error type and message cannot be empty.")
        for name, value in (
            ("code", self.code),
            ("phase", self.phase),
            ("cause", self.cause),
        ):
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(f"Runtime error {name} must be a non-empty string.")
        if self.retryable is not None and type(self.retryable) is not bool:
            raise TypeError("Runtime error retryable must be bool or None.")


@dataclass(frozen=True, slots=True)
class SessionOpened:
    kind: ClassVar[str] = "session.opened"
    workflow_id: str
    context: DurableValue

    def __post_init__(self) -> None:
        object.__setattr__(self, "context", freeze(self.context))


@dataclass(frozen=True, slots=True)
class InvocationOpened:
    kind: ClassVar[str] = "invocation.opened"
    workflow_revision_id: str
    entry_node_id: str
    input: DurableValue

    def __post_init__(self) -> None:
        object.__setattr__(self, "input", freeze(self.input))


@dataclass(frozen=True, slots=True)
class InvocationStarted:
    kind: ClassVar[str] = "invocation.started"


@dataclass(frozen=True, slots=True)
class InvocationWaiting:
    kind: ClassVar[str] = "invocation.waiting"


@dataclass(frozen=True, slots=True)
class InvocationCompleted:
    kind: ClassVar[str] = "invocation.completed"
    output: DurableValue

    def __post_init__(self) -> None:
        object.__setattr__(self, "output", freeze(self.output))


@dataclass(frozen=True, slots=True)
class InvocationFailed:
    kind: ClassVar[str] = "invocation.failed"
    error: RuntimeErrorInfo


@dataclass(frozen=True, slots=True)
class InvocationCancelled:
    kind: ClassVar[str] = "invocation.cancelled"
    reason: str | None


@dataclass(frozen=True, slots=True)
class SchedulerInitialized:
    kind: ClassVar[str] = "scheduler.initialized"
    delta: SchedulerDelta


@dataclass(frozen=True, slots=True)
class NodeOccurrenceStarted:
    kind: ClassVar[str] = "node_occurrence.started"
    occurrence_id: str


@dataclass(frozen=True, slots=True)
class OperatorCallStarted:
    kind: ClassVar[str] = "operator_call.started"
    call_id: str
    occurrence_id: str
    operator_id: str
    unit_index: int
    input: DurableValue
    attempt: int = 1
    reason: Literal["normal", "retry", "fallback"] = "normal"

    def __post_init__(self) -> None:
        if self.unit_index < 0:
            raise ValueError("Operator Call unit_index cannot be negative.")
        if self.attempt < 1:
            raise ValueError("Operator Call attempt must be positive.")
        if self.reason not in {"normal", "retry", "fallback"}:
            raise ValueError(f"Unknown Operator Call reason {self.reason!r}.")
        object.__setattr__(self, "input", freeze(self.input))


@dataclass(frozen=True, slots=True)
class OperatorCallCompleted:
    kind: ClassVar[str] = "operator_call.completed"
    call_id: str
    output: DurableValue

    def __post_init__(self) -> None:
        object.__setattr__(self, "output", freeze(self.output))


@dataclass(frozen=True, slots=True)
class OperatorCallFailed:
    kind: ClassVar[str] = "operator_call.failed"
    call_id: str
    error: RuntimeErrorInfo


@dataclass(frozen=True, slots=True)
class NodeOccurrenceWaiting:
    kind: ClassVar[str] = "node_occurrence.waiting"
    occurrence_id: str
    wait_id: str
    request: DurableValue

    def __post_init__(self) -> None:
        object.__setattr__(self, "request", freeze(self.request))


@dataclass(frozen=True, slots=True)
class WaitResumed:
    kind: ClassVar[str] = "wait.resumed"
    wait_id: str
    response: DurableValue

    def __post_init__(self) -> None:
        object.__setattr__(self, "response", freeze(self.response))


@dataclass(frozen=True, slots=True)
class InvocationRecoveryRequested:
    kind: ClassVar[str] = "invocation.recovery_requested"


@dataclass(frozen=True, slots=True)
class ChildInvocationLinked:
    kind: ClassVar[str] = "child_invocation.linked"
    parent_occurrence_id: str
    child_session_id: str
    child_invocation_id: str
    workflow_id: str
    workflow_revision_id: str

    def __post_init__(self) -> None:
        if not all(
            (
                self.parent_occurrence_id,
                self.child_session_id,
                self.child_invocation_id,
                self.workflow_id,
                self.workflow_revision_id,
            )
        ):
            raise ValueError("Child Invocation link fields cannot be empty.")


@dataclass(frozen=True, slots=True)
class NodeOccurrenceCompleted:
    kind: ClassVar[str] = "node_occurrence.completed"
    occurrence_id: str
    output: DurableValue
    delta: SchedulerDelta
    patch: ContextPatch = ContextPatch()
    metrics: DurableValue = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "output", freeze(self.output))
        object.__setattr__(self, "patch", _freeze_patch(self.patch))
        object.__setattr__(self, "metrics", freeze(self.metrics))


@dataclass(frozen=True, slots=True)
class NodeOccurrenceFailed:
    kind: ClassVar[str] = "node_occurrence.failed"
    occurrence_id: str
    error: RuntimeErrorInfo
    delta: SchedulerDelta


RuntimeEventPayload: TypeAlias = (
    SessionOpened
    | InvocationOpened
    | InvocationStarted
    | InvocationWaiting
    | InvocationCompleted
    | InvocationFailed
    | InvocationCancelled
    | SchedulerInitialized
    | NodeOccurrenceStarted
    | OperatorCallStarted
    | OperatorCallCompleted
    | OperatorCallFailed
    | NodeOccurrenceWaiting
    | WaitResumed
    | InvocationRecoveryRequested
    | ChildInvocationLinked
    | NodeOccurrenceCompleted
    | NodeOccurrenceFailed
)


@dataclass(frozen=True, slots=True)
class RuntimeLog:
    """One semantic observation inside a Runtime Event envelope."""

    payload: RuntimeEventPayload
    invocation_id: str | None
    occurred_at_ns: int
    causation_id: str | None = None
    state_version: int | None = None
    id: str = field(default_factory=lambda: str(uuid4()))

    @property
    def event_name(self) -> str:
        return self.payload.kind

    def to_record(self, *, include_payload: bool = True) -> dict[str, object]:
        record: dict[str, object] = {
            "id": self.id,
            "invocation_id": self.invocation_id,
            "causation_id": self.causation_id,
            "occurred_at_ns": self.occurred_at_ns,
            "event_name": self.event_name,
            "state_version": self.state_version,
        }
        if include_payload:
            record["payload"] = _payload_to_record(self.payload)
        return record

    @classmethod
    def from_record(
        cls,
        record: dict[str, object],
        *,
        default_payload: RuntimeEventPayload | None = None,
    ) -> "RuntimeLog":
        event_name = _required_string(record, "event_name")
        payload = (
            _payload_from_record(event_name, _required_mapping(record, "payload"))
            if "payload" in record
            else default_payload
        )
        if payload is None or payload.kind != event_name:
            raise ValueError("Runtime Log payload does not match event_name.")
        return cls(
            id=_required_string(record, "id"),
            invocation_id=(
                _required_string(record, "invocation_id")
                if record.get("invocation_id") is not None
                else None
            ),
            causation_id=(
                _required_string(record, "causation_id")
                if record.get("causation_id") is not None
                else None
            ),
            occurred_at_ns=_required_integer(record, "occurred_at_ns"),
            state_version=(
                _required_integer(record, "state_version")
                if record.get("state_version") is not None
                else None
            ),
            payload=payload,
        )


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    session_id: str
    sequence: int
    payload: RuntimeEventPayload
    invocation_id: str | None = None
    causation_id: str | None = None
    from_state_version: int | None = None
    to_state_version: int | None = None
    operation_batches: tuple[StateOperationBatch, ...] = ()
    logs: tuple[RuntimeLog, ...] = ()
    occurred_at_ns: int = field(default_factory=time.time_ns)
    id: str = field(default_factory=lambda: str(uuid4()))
    schema_version: int = RUNTIME_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValueError("Runtime Event session_id cannot be empty.")
        for name, value in (
            ("invocation_id", self.invocation_id),
            ("causation_id", self.causation_id),
        ):
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(f"Runtime Event {name} must be non-empty or None.")
        if (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 1
        ):
            raise ValueError("Runtime Event sequence must be positive.")
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("Runtime Event id cannot be empty.")
        if (
            not isinstance(self.occurred_at_ns, int)
            or isinstance(self.occurred_at_ns, bool)
            or self.occurred_at_ns < 0
        ):
            raise ValueError("Runtime Event time cannot be negative.")
        if (self.from_state_version is None) != (self.to_state_version is None):
            raise ValueError(
                "Runtime Event state-version bounds must both be present or absent."
            )
        if self.from_state_version is not None:
            if (
                not isinstance(self.from_state_version, int)
                or isinstance(self.from_state_version, bool)
                or self.from_state_version < 0
                or not isinstance(self.to_state_version, int)
                or isinstance(self.to_state_version, bool)
                or self.to_state_version < self.from_state_version
            ):
                raise ValueError("Runtime Event state-version interval is invalid.")
            if (
                self.to_state_version > self.from_state_version
                and not self.operation_batches
            ):
                raise ValueError(
                    "A sealed Runtime Event must contain State Operation Batches."
                )
            if (
                self.to_state_version == self.from_state_version
                and self.operation_batches
            ):
                raise ValueError(
                    "A zero-width Runtime Event cannot contain State Operation Batches."
                )
            expected = self.from_state_version
            for batch in self.operation_batches:
                if batch.from_state_version != expected:
                    raise ValueError(
                        "Runtime Event State Operation Batches are not contiguous."
                    )
                expected = batch.to_state_version
            if expected != self.to_state_version:
                raise ValueError(
                    "Runtime Event state-version interval does not match its batches."
                )
        if not isinstance(self.schema_version, int) or isinstance(
            self.schema_version, bool
        ):
            raise TypeError("Runtime Event schema_version must be an integer.")
        if not isinstance(
            self.payload,
            (
                SessionOpened,
                InvocationOpened,
                InvocationStarted,
                InvocationWaiting,
                InvocationCompleted,
                InvocationFailed,
                InvocationCancelled,
                SchedulerInitialized,
                NodeOccurrenceStarted,
                OperatorCallStarted,
                OperatorCallCompleted,
                OperatorCallFailed,
                NodeOccurrenceWaiting,
                WaitResumed,
                InvocationRecoveryRequested,
                ChildInvocationLinked,
                NodeOccurrenceCompleted,
                NodeOccurrenceFailed,
            ),
        ):
            raise TypeError("Unsupported Runtime Event payload type.")
        if isinstance(self.payload, SessionOpened) != (self.invocation_id is None):
            raise ValueError(
                "Session Event must omit invocation_id; Invocation Event must provide it."
            )
        if not self.logs:
            object.__setattr__(
                self,
                "logs",
                (
                    RuntimeLog(
                        id=self.id,
                        payload=self.payload,
                        invocation_id=self.invocation_id,
                        occurred_at_ns=self.occurred_at_ns,
                        causation_id=self.causation_id,
                        state_version=self.to_state_version,
                    ),
                ),
            )
        if self.logs[-1].payload != self.payload:
            raise ValueError("Runtime Event payload must equal its final Runtime Log payload.")

    @property
    def event_name(self) -> str:
        return self.payload.kind

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "id": self.id,
            "session_id": self.session_id,
            "invocation_id": self.invocation_id,
            "sequence": self.sequence,
            "causation_id": self.causation_id,
            "occurred_at_ns": self.occurred_at_ns,
            "event_name": self.event_name,
            "payload": _payload_to_record(self.payload),
            "from_state_version": self.from_state_version,
            "to_state_version": self.to_state_version,
            "operation_batches": [
                batch.to_record() for batch in self.operation_batches
            ],
            "logs": [
                log.to_record(include_payload=index != len(self.logs) - 1)
                for index, log in enumerate(self.logs)
            ],
        }

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "RuntimeEvent":
        if not isinstance(record, dict):
            raise TypeError("Runtime Event record must be a mapping.")
        event_name = _required_string(record, "event_name")
        payload_record = record.get("payload")
        if not isinstance(payload_record, dict):
            raise TypeError("Runtime Event payload record must be a mapping.")
        return cls(
            schema_version=_required_integer(record, "schema_version"),
            id=_required_string(record, "id"),
            session_id=_required_string(record, "session_id"),
            invocation_id=(
                _required_string(record, "invocation_id")
                if record.get("invocation_id") is not None
                else None
            ),
            sequence=_required_integer(record, "sequence"),
            causation_id=(
                _required_string(record, "causation_id")
                if record.get("causation_id") is not None
                else None
            ),
            occurred_at_ns=_required_integer(record, "occurred_at_ns"),
            payload=_payload_from_record(event_name, payload_record),
            from_state_version=(
                _required_integer(record, "from_state_version")
                if record.get("from_state_version") is not None
                else None
            ),
            to_state_version=(
                _required_integer(record, "to_state_version")
                if record.get("to_state_version") is not None
                else None
            ),
            operation_batches=tuple(
                StateOperationBatch.from_record(item)
                for item in _required_list(record, "operation_batches")
            ),
            logs=_logs_from_record(
                _required_list(record, "logs"),
                _payload_from_record(event_name, payload_record),
            ),
        )

    @property
    def operations(self) -> tuple[StateOperation, ...]:
        return tuple(
            operation
            for batch in self.operation_batches
            for operation in batch.operations
        )


def _payload_to_record(payload: RuntimeEventPayload) -> dict[str, object]:
    if isinstance(payload, SessionOpened):
        return {
            "workflow_id": payload.workflow_id,
            "context": thaw(payload.context),
        }
    if isinstance(payload, InvocationOpened):
        return {
            "workflow_revision_id": payload.workflow_revision_id,
            "entry_node_id": payload.entry_node_id,
            "input": thaw(payload.input),
        }
    if isinstance(payload, InvocationStarted):
        return {}
    if isinstance(payload, InvocationWaiting):
        return {}
    if isinstance(payload, InvocationCompleted):
        return {
            "output": thaw(payload.output),
        }
    if isinstance(payload, InvocationFailed):
        return {"error": _error_to_record(payload.error)}
    if isinstance(payload, InvocationCancelled):
        return {
            "reason": payload.reason,
        }
    if isinstance(payload, SchedulerInitialized):
        return {"delta": delta_to_record(payload.delta)}
    if isinstance(payload, NodeOccurrenceStarted):
        return {"occurrence_id": payload.occurrence_id}
    if isinstance(payload, OperatorCallStarted):
        return {
            "call_id": payload.call_id,
            "occurrence_id": payload.occurrence_id,
            "operator_id": payload.operator_id,
            "unit_index": payload.unit_index,
            "input": thaw(payload.input),
            "attempt": payload.attempt,
            "reason": payload.reason,
        }
    if isinstance(payload, OperatorCallCompleted):
        return {"call_id": payload.call_id, "output": thaw(payload.output)}
    if isinstance(payload, OperatorCallFailed):
        return {
            "call_id": payload.call_id,
            "error": _error_to_record(payload.error),
        }
    if isinstance(payload, NodeOccurrenceWaiting):
        return {
            "occurrence_id": payload.occurrence_id,
            "wait_id": payload.wait_id,
            "request": thaw(payload.request),
        }
    if isinstance(payload, WaitResumed):
        return {"wait_id": payload.wait_id, "response": thaw(payload.response)}
    if isinstance(payload, InvocationRecoveryRequested):
        return {}
    if isinstance(payload, ChildInvocationLinked):
        return {
            "parent_occurrence_id": payload.parent_occurrence_id,
            "child_session_id": payload.child_session_id,
            "child_invocation_id": payload.child_invocation_id,
            "workflow_id": payload.workflow_id,
            "workflow_revision_id": payload.workflow_revision_id,
        }
    if isinstance(payload, NodeOccurrenceCompleted):
        return {
            "occurrence_id": payload.occurrence_id,
            "output": thaw(payload.output),
            "delta": delta_to_record(payload.delta),
            "patch": _patch_to_record(payload.patch),
            "metrics": thaw(payload.metrics),
        }
    if isinstance(payload, NodeOccurrenceFailed):
        return {
            "occurrence_id": payload.occurrence_id,
            "error": _error_to_record(payload.error),
            "delta": delta_to_record(payload.delta),
        }
    raise TypeError(f"Unsupported Runtime Event payload: {type(payload).__name__}.")


def _payload_from_record(
    event_name: str, record: dict[str, object]
) -> RuntimeEventPayload:
    if event_name == SessionOpened.kind:
        return SessionOpened(
            _required_string(record, "workflow_id"),
            _required_value(record, "context"),  # type: ignore[arg-type]
        )
    if event_name == InvocationOpened.kind:
        return InvocationOpened(
            _required_string(record, "workflow_revision_id"),
            _required_string(record, "entry_node_id"),
            _required_value(record, "input"),  # type: ignore[arg-type]
        )
    if event_name == InvocationStarted.kind:
        return InvocationStarted()
    if event_name == InvocationWaiting.kind:
        return InvocationWaiting()
    if event_name == InvocationCompleted.kind:
        return InvocationCompleted(_required_value(record, "output"))  # type: ignore[arg-type]
    if event_name == InvocationFailed.kind:
        error = record.get("error")
        if not isinstance(error, dict):
            raise TypeError("Invocation failed payload requires an error mapping.")
        return InvocationFailed(_error_from_record(error))
    if event_name == InvocationCancelled.kind:
        return InvocationCancelled(
            _required_string(record, "reason")
            if record.get("reason") is not None
            else None,
        )
    if event_name == SchedulerInitialized.kind:
        return SchedulerInitialized(delta_from_record(_required_mapping(record, "delta")))
    if event_name == NodeOccurrenceStarted.kind:
        return NodeOccurrenceStarted(_required_string(record, "occurrence_id"))
    if event_name == OperatorCallStarted.kind:
        reason = record.get("reason", "normal")
        if reason not in {"normal", "retry", "fallback"}:
            raise ValueError(f"Unknown Operator Call reason {reason!r}.")
        return OperatorCallStarted(
            _required_string(record, "call_id"),
            _required_string(record, "occurrence_id"),
            _required_string(record, "operator_id"),
            _required_integer(record, "unit_index"),
            _required_value(record, "input"),
            _optional_integer(record, "attempt", default=1),
            reason,  # type: ignore[arg-type]
        )
    if event_name == OperatorCallCompleted.kind:
        return OperatorCallCompleted(
            _required_string(record, "call_id"),
            _required_value(record, "output"),
        )
    if event_name == OperatorCallFailed.kind:
        return OperatorCallFailed(
            _required_string(record, "call_id"),
            _error_from_record(_required_mapping(record, "error")),
        )
    if event_name == NodeOccurrenceWaiting.kind:
        return NodeOccurrenceWaiting(
            _required_string(record, "occurrence_id"),
            _required_string(record, "wait_id"),
            _required_value(record, "request"),
        )
    if event_name == WaitResumed.kind:
        return WaitResumed(
            _required_string(record, "wait_id"),
            _required_value(record, "response"),
        )
    if event_name == InvocationRecoveryRequested.kind:
        return InvocationRecoveryRequested()
    if event_name == ChildInvocationLinked.kind:
        return ChildInvocationLinked(
            _required_string(record, "parent_occurrence_id"),
            _required_string(record, "child_session_id"),
            _required_string(record, "child_invocation_id"),
            _required_string(record, "workflow_id"),
            _required_string(record, "workflow_revision_id"),
        )
    if event_name == NodeOccurrenceCompleted.kind:
        return NodeOccurrenceCompleted(
            _required_string(record, "occurrence_id"),
            _required_value(record, "output"),  # type: ignore[arg-type]
            delta_from_record(_required_mapping(record, "delta")),
            _patch_from_record(_required_value(record, "patch")),
            _required_value(record, "metrics"),
        )
    if event_name == NodeOccurrenceFailed.kind:
        error = record.get("error")
        if not isinstance(error, dict):
            raise TypeError("Node failure payload requires an error mapping.")
        return NodeOccurrenceFailed(
            _required_string(record, "occurrence_id"),
            _error_from_record(error),
            delta_from_record(_required_mapping(record, "delta")),
        )
    raise ValueError(f"Unknown Runtime Event type {event_name!r}.")


def _error_to_record(error: RuntimeErrorInfo) -> dict[str, object]:
    return {
        key: value
        for key, value in {
            "type": error.type,
            "message": error.message,
            "code": error.code,
            "phase": error.phase,
            "retryable": error.retryable,
            "cause": error.cause,
        }.items()
        if value is not None
    }


def _error_from_record(value: dict[str, object]) -> RuntimeErrorInfo:
    error_type = value.get("type")
    message = value.get("message")
    if not isinstance(error_type, str) or not isinstance(message, str):
        raise TypeError("Runtime error type and message must be strings.")
    retryable = value.get("retryable")
    if retryable is not None and type(retryable) is not bool:
        raise TypeError("Runtime error retryable must be bool or None.")
    optional_strings: dict[str, str | None] = {}
    for name in ("code", "phase", "cause"):
        item = value.get(name)
        if item is not None and not isinstance(item, str):
            raise TypeError(f"Runtime error {name} must be a string or None.")
        optional_strings[name] = item
    return RuntimeErrorInfo(
        error_type,
        message,
        optional_strings["code"],
        optional_strings["phase"],
        retryable,
        optional_strings["cause"],
    )


def _required_string(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str):
        raise TypeError(f"Runtime Event {key} must be a string.")
    return value


def _required_integer(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Runtime Event {key} must be an integer.")
    return value


def _optional_integer(
    record: dict[str, object], key: str, *, default: int
) -> int:
    if key not in record:
        return default
    return _required_integer(record, key)


def _required_mapping(
    record: dict[str, object], key: str
) -> dict[str, object]:
    value = record.get(key)
    if not isinstance(value, dict):
        raise TypeError(f"Runtime Event {key} must be a mapping.")
    return value


def _required_list(record: dict[str, object], key: str) -> list[dict[str, object]]:
    value = record.get(key, [])
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise TypeError(f"Runtime Event {key} must be a list of mappings.")
    return value


def _logs_from_record(
    records: list[dict[str, object]], final_payload: RuntimeEventPayload
) -> tuple[RuntimeLog, ...]:
    if not records:
        return ()
    return tuple(
        RuntimeLog.from_record(
            record,
            default_payload=(final_payload if index == len(records) - 1 else None),
        )
        for index, record in enumerate(records)
    )


def _required_value(record: dict[str, object], key: str) -> object:
    if key not in record:
        raise KeyError(f"Runtime Event payload requires {key}.")
    return record[key]


def _freeze_patch(value: ContextPatch) -> ContextPatch:
    if not isinstance(value, ContextPatch):
        raise TypeError("Node completion patch must be ContextPatch.")
    return ContextPatch(
        invocation=tuple(
            ContextOperation(item.operation, item.path, freeze(item.value))
            for item in value.invocation
        ),
        session=tuple(
            ContextOperation(item.operation, item.path, freeze(item.value))
            for item in value.session
        ),
    )


def _patch_to_record(value: ContextPatch) -> dict[str, object]:
    def encode(item: ContextOperation) -> dict[str, object]:
        return {
            "operation": item.operation,
            "path": list(item.path),
            "value": thaw(item.value),
        }

    return {
        "invocation": [encode(item) for item in value.invocation],
        "session": [encode(item) for item in value.session],
    }


def _patch_from_record(value: object) -> ContextPatch:
    if not isinstance(value, dict):
        raise TypeError("Context Patch record must be a mapping.")

    def decode(item: object) -> ContextOperation:
        if not isinstance(item, dict):
            raise TypeError("Context operation record must be a mapping.")
        operation = item.get("operation")
        if not isinstance(operation, str):
            raise TypeError("Context operation must be a string.")
        if operation not in {"set", "delete"}:
            raise ValueError(f"Unknown Context operation {operation!r}.")
        path = item.get("path")
        if not isinstance(path, list) or not all(
            isinstance(part, str) for part in path
        ):
            raise TypeError("Context operation path must be a list of strings.")
        return ContextOperation(
            operation,  # type: ignore[arg-type]
            tuple(path),
            item.get("value"),
        )

    invocation = value.get("invocation", [])
    session = value.get("session", [])
    if not isinstance(invocation, list) or not isinstance(session, list):
        raise TypeError("Context Patch sections must be lists.")
    return ContextPatch(
        invocation=tuple(decode(item) for item in invocation),
        session=tuple(decode(item) for item in session),
    )
