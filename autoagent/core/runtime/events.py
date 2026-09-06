"""Durable Full-mode Runtime Event envelopes and semantic Runtime logs.

Each internal transition contributes one semantic log and one atomic State
Operation batch.  A Runtime Event may envelope one or more adjacent batches;
it never embeds a duplicate copy of the resulting Runtime State.
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


RUNTIME_EVENT_SCHEMA_VERSION = 3


@dataclass(frozen=True, slots=True)
class RuntimeErrorInfo:
    type: str
    message: str
    code: str | None = None
    phase: str | None = None
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


@dataclass(frozen=True, slots=True)
class SessionOpened:
    kind: ClassVar[str] = "session.opened"
    context: DurableValue

    def __post_init__(self) -> None:
        object.__setattr__(self, "context", freeze(self.context))


@dataclass(frozen=True, slots=True)
class InvocationOpened:
    kind: ClassVar[str] = "invocation.opened"
    workflow_id: str
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

    def __post_init__(self) -> None:
        if self.unit_index < 0:
            raise ValueError("Operator Call unit_index cannot be negative.")
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
class ChildUnitSpec:
    """Stable identity and durable input for one planned Child unit."""

    unit_index: int
    child_session_id: str
    child_invocation_id: str
    input: DurableValue

    def __post_init__(self) -> None:
        if (
            not isinstance(self.unit_index, int)
            or isinstance(self.unit_index, bool)
            or self.unit_index < 0
        ):
            raise ValueError("Child unit_index must be a non-negative integer.")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (self.child_session_id, self.child_invocation_id)
        ):
            raise ValueError("Child unit identities cannot be empty.")
        object.__setattr__(self, "input", freeze(self.input))


@dataclass(frozen=True, slots=True)
class ChildInvocationPlanned:
    kind: ClassVar[str] = "child_invocation.planned"
    creation_id: str
    parent_occurrence_id: str
    mode: Literal["await", "spawn"]
    workflow_id: str
    workflow_revision_id: str
    units: tuple[ChildUnitSpec, ...]

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value.strip()
            for value in (
                self.creation_id,
                self.parent_occurrence_id,
                self.workflow_id,
                self.workflow_revision_id,
            )
        ):
            raise ValueError("Child Invocation plan fields cannot be empty.")
        if self.mode not in {"await", "spawn"}:
            raise ValueError(f"Unknown Child Invocation mode {self.mode!r}.")
        if not isinstance(self.units, tuple) or not self.units:
            raise ValueError("Child Invocation plan units cannot be empty.")
        if not all(isinstance(unit, ChildUnitSpec) for unit in self.units):
            raise TypeError("Child Invocation plan units must be ChildUnitSpec values.")
        indexes = tuple(unit.unit_index for unit in self.units)
        if indexes != tuple(range(len(self.units))):
            raise ValueError("Child Invocation plan unit indexes must be ordered from zero.")
        session_ids = {unit.child_session_id for unit in self.units}
        invocation_ids = {unit.child_invocation_id for unit in self.units}
        if len(session_ids) != len(self.units) or len(invocation_ids) != len(self.units):
            raise ValueError("Child Invocation plan identities must be unique.")


@dataclass(frozen=True, slots=True)
class ChildInvocationPhaseChanged:
    kind: ClassVar[str] = "child_invocation.phase_changed"
    creation_id: str
    unit_index: int
    phase: Literal["opened", "accepted", "terminal"]

    def __post_init__(self) -> None:
        if not isinstance(self.creation_id, str) or not self.creation_id.strip():
            raise ValueError("Child Invocation creation_id cannot be empty.")
        if (
            not isinstance(self.unit_index, int)
            or isinstance(self.unit_index, bool)
            or self.unit_index < 0
        ):
            raise ValueError("Child unit_index must be a non-negative integer.")
        if self.phase not in {"opened", "accepted", "terminal"}:
            raise ValueError(f"Unknown Child Invocation phase {self.phase!r}.")


@dataclass(frozen=True, slots=True)
class ChildAwaitSuspended:
    kind: ClassVar[str] = "child_await.suspended"
    creation_id: str
    parent_occurrence_id: str

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value.strip()
            for value in (self.creation_id, self.parent_occurrence_id)
        ):
            raise ValueError("Child Await identities cannot be empty.")


@dataclass(frozen=True, slots=True)
class ChildAwaitReady:
    kind: ClassVar[str] = "child_await.ready"
    creation_id: str
    parent_occurrence_id: str

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value.strip()
            for value in (self.creation_id, self.parent_occurrence_id)
        ):
            raise ValueError("Child Await identities cannot be empty.")


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
    | ChildInvocationPlanned
    | ChildInvocationPhaseChanged
    | ChildAwaitSuspended
    | ChildAwaitReady
    | NodeOccurrenceCompleted
    | NodeOccurrenceFailed
)

_PAYLOAD_TYPES = (
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
    ChildInvocationPlanned,
    ChildInvocationPhaseChanged,
    ChildAwaitSuspended,
    ChildAwaitReady,
    NodeOccurrenceCompleted,
    NodeOccurrenceFailed,
)


@dataclass(frozen=True, slots=True)
class StateTransition:
    """Internal semantic request from which one State Operation Batch is planned."""

    session_id: str
    payload: RuntimeEventPayload
    invocation_id: str | None = None
    causation_id: str | None = None
    occurred_at_ns: int = field(default_factory=time.time_ns)
    id: str = field(default_factory=lambda: str(uuid4()))

    def __post_init__(self) -> None:
        if not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValueError("State Transition session_id cannot be empty.")
        for name, value in (
            ("invocation_id", self.invocation_id),
            ("causation_id", self.causation_id),
        ):
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(
                    f"State Transition {name} must be non-empty or None."
                )
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("State Transition id cannot be empty.")
        if (
            not isinstance(self.occurred_at_ns, int)
            or isinstance(self.occurred_at_ns, bool)
            or self.occurred_at_ns < 0
        ):
            raise ValueError("State Transition time cannot be negative.")
        if not isinstance(self.payload, _PAYLOAD_TYPES):
            raise TypeError("Unsupported State Transition payload type.")
        if isinstance(self.payload, SessionOpened) != (self.invocation_id is None):
            raise ValueError(
                "Session Transition must omit invocation_id; Invocation Transition "
                "must provide it."
            )

    def to_runtime_event(self, sequence: int) -> "RuntimeEvent":
        """Create the unsealed compatibility envelope consumed by StateReducer."""

        return RuntimeEvent(
            session_id=self.session_id,
            invocation_id=self.invocation_id,
            sequence=sequence,
            causation_id=self.causation_id,
            occurred_at_ns=self.occurred_at_ns,
            id=self.id,
            payload=self.payload,
        )


@dataclass(frozen=True, slots=True)
class RuntimeLog:
    """One semantic transition record inside a Runtime Event envelope."""

    payload: RuntimeEventPayload
    invocation_id: str | None
    occurred_at_ns: int
    causation_id: str | None = None
    state_version: int | None = None
    id: str = field(default_factory=lambda: str(uuid4()))

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("Runtime Log id cannot be empty.")
        for name, value in (
            ("invocation_id", self.invocation_id),
            ("causation_id", self.causation_id),
        ):
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(f"Runtime Log {name} must be non-empty or None.")
        if (
            not isinstance(self.occurred_at_ns, int)
            or isinstance(self.occurred_at_ns, bool)
            or self.occurred_at_ns < 0
        ):
            raise ValueError("Runtime Log time cannot be negative.")
        if self.state_version is not None and (
            not isinstance(self.state_version, int)
            or isinstance(self.state_version, bool)
            or self.state_version < 0
        ):
            raise ValueError("Runtime Log state_version must be non-negative or None.")
        if not isinstance(self.payload, _PAYLOAD_TYPES):
            raise TypeError("Unsupported Runtime Log payload type.")
        if isinstance(self.payload, SessionOpened) != (self.invocation_id is None):
            raise ValueError(
                "Session Log must omit invocation_id; Invocation Log must provide it."
            )

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
    previous_event_id: str | None = None
    previous_event_digest: str | None = None
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
            ("previous_event_id", self.previous_event_id),
            ("previous_event_digest", self.previous_event_digest),
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
        if self.schema_version != RUNTIME_EVENT_SCHEMA_VERSION:
            raise ValueError(f"Unsupported Runtime Event schema {self.schema_version}.")
        if not isinstance(self.payload, _PAYLOAD_TYPES):
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
        if not isinstance(self.logs, tuple) or not all(
            isinstance(log, RuntimeLog) for log in self.logs
        ):
            raise TypeError("Runtime Event logs must be RuntimeLog values.")
        if len({log.id for log in self.logs}) != len(self.logs):
            raise ValueError("Runtime Event Log ids must be unique.")
        if self.logs[-1].payload != self.payload:
            raise ValueError("Runtime Event payload must equal its final Runtime Log payload.")
        if self.logs[-1].invocation_id != self.invocation_id:
            raise ValueError(
                "Runtime Event invocation_id must equal its final Runtime Log."
            )
        if self.logs[-1].occurred_at_ns != self.occurred_at_ns:
            raise ValueError(
                "Runtime Event time must equal its final Runtime Log time."
            )
        if any(
            later.occurred_at_ns < earlier.occurred_at_ns
            for earlier, later in zip(self.logs, self.logs[1:])
        ):
            raise ValueError("Runtime Event Log times cannot move backwards.")
        if self.from_state_version is None:
            if any(log.state_version is not None for log in self.logs):
                raise ValueError("Unsealed Runtime Event Logs cannot have state versions.")
        else:
            assert self.to_state_version is not None
            versions = tuple(log.state_version for log in self.logs)
            if any(version is None for version in versions):
                raise ValueError("Sealed Runtime Event Logs require state versions.")
            sealed_versions = tuple(version for version in versions if version is not None)
            if (
                any(
                    version < self.from_state_version
                    or version > self.to_state_version
                    for version in sealed_versions
                )
                or any(
                    later < earlier
                    for earlier, later in zip(sealed_versions, sealed_versions[1:])
                )
                or sealed_versions[-1] != self.to_state_version
            ):
                raise ValueError(
                    "Runtime Event Log state versions do not match its interval."
                )

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
            "previous_event_id": self.previous_event_id,
            "previous_event_digest": self.previous_event_digest,
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
        expected_fields = {
            "schema_version",
            "id",
            "session_id",
            "invocation_id",
            "sequence",
            "causation_id",
            "previous_event_id",
            "previous_event_digest",
            "occurred_at_ns",
            "event_name",
            "payload",
            "from_state_version",
            "to_state_version",
            "operation_batches",
            "logs",
        }
        if set(record) != expected_fields:
            raise TypeError("Runtime Event record contains missing or unknown fields.")
        schema_version = _required_integer(record, "schema_version")
        if schema_version != RUNTIME_EVENT_SCHEMA_VERSION:
            raise ValueError(f"Unsupported Runtime Event schema {schema_version}.")
        event_name = _required_string(record, "event_name")
        payload_record = record.get("payload")
        if not isinstance(payload_record, dict):
            raise TypeError("Runtime Event payload record must be a mapping.")
        event = cls(
            schema_version=schema_version,
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
            previous_event_id=(
                _required_string(record, "previous_event_id")
                if record.get("previous_event_id") is not None
                else None
            ),
            previous_event_digest=(
                _required_string(record, "previous_event_digest")
                if record.get("previous_event_digest") is not None
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
        if event.to_record() != record:
            raise TypeError("Runtime Event record is not canonical.")
        return event

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
            "context": thaw(payload.context),
        }
    if isinstance(payload, InvocationOpened):
        return {
            "workflow_id": payload.workflow_id,
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
    if isinstance(payload, ChildInvocationPlanned):
        return {
            "creation_id": payload.creation_id,
            "parent_occurrence_id": payload.parent_occurrence_id,
            "mode": payload.mode,
            "workflow_id": payload.workflow_id,
            "workflow_revision_id": payload.workflow_revision_id,
            "units": [
                {
                    "unit_index": unit.unit_index,
                    "child_session_id": unit.child_session_id,
                    "child_invocation_id": unit.child_invocation_id,
                    "input": thaw(unit.input),
                }
                for unit in payload.units
            ],
        }
    if isinstance(payload, ChildInvocationPhaseChanged):
        return {
            "creation_id": payload.creation_id,
            "unit_index": payload.unit_index,
            "phase": payload.phase,
        }
    if isinstance(payload, (ChildAwaitSuspended, ChildAwaitReady)):
        return {
            "creation_id": payload.creation_id,
            "parent_occurrence_id": payload.parent_occurrence_id,
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
            _required_value(record, "context"),  # type: ignore[arg-type]
        )
    if event_name == InvocationOpened.kind:
        return InvocationOpened(
            _required_string(record, "workflow_id"),
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
        return OperatorCallStarted(
            _required_string(record, "call_id"),
            _required_string(record, "occurrence_id"),
            _required_string(record, "operator_id"),
            _required_integer(record, "unit_index"),
            _required_value(record, "input"),
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
    if event_name == ChildInvocationPlanned.kind:
        mode = _required_string(record, "mode")
        if mode not in {"await", "spawn"}:
            raise ValueError(f"Unknown Child Invocation mode {mode!r}.")
        return ChildInvocationPlanned(
            _required_string(record, "creation_id"),
            _required_string(record, "parent_occurrence_id"),
            mode,  # type: ignore[arg-type]
            _required_string(record, "workflow_id"),
            _required_string(record, "workflow_revision_id"),
            tuple(
                ChildUnitSpec(
                    _required_integer(item, "unit_index"),
                    _required_string(item, "child_session_id"),
                    _required_string(item, "child_invocation_id"),
                    _required_value(item, "input"),
                )
                for item in _required_list(record, "units")
            ),
        )
    if event_name == ChildInvocationPhaseChanged.kind:
        phase = _required_string(record, "phase")
        if phase not in {"opened", "accepted", "terminal"}:
            raise ValueError(f"Unknown Child Invocation phase {phase!r}.")
        return ChildInvocationPhaseChanged(
            _required_string(record, "creation_id"),
            _required_integer(record, "unit_index"),
            phase,  # type: ignore[arg-type]
        )
    if event_name == ChildAwaitSuspended.kind:
        return ChildAwaitSuspended(
            _required_string(record, "creation_id"),
            _required_string(record, "parent_occurrence_id"),
        )
    if event_name == ChildAwaitReady.kind:
        return ChildAwaitReady(
            _required_string(record, "creation_id"),
            _required_string(record, "parent_occurrence_id"),
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
            "cause": error.cause,
        }.items()
        if value is not None
    }


def _error_from_record(value: dict[str, object]) -> RuntimeErrorInfo:
    error_type = value.get("type")
    message = value.get("message")
    if not isinstance(error_type, str) or not isinstance(message, str):
        raise TypeError("Runtime error type and message must be strings.")
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
