"""One semantic execution boundary and its atomic, replayable StateDelta."""

from __future__ import annotations

from .clocks import unix_time_us
from dataclasses import dataclass, field, fields
from typing import ClassVar, Literal, TypeAlias
from uuid import uuid4

from .operations import StateDelta
from .values import DurableValue, freeze, thaw
from ..context import ContextOperation, ContextPatch


RUNTIME_EVENT_SCHEMA_VERSION = 5


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
class InvocationStarted:
    kind: ClassVar[str] = "invocation.started"
    workflow_id: str
    workflow_revision_id: str
    entry_node_id: str
    input: DurableValue

    def __post_init__(self) -> None:
        object.__setattr__(self, "input", freeze(self.input))


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
class NodeStarted:
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
    queue_duration_ns: int = 0

    def __post_init__(self) -> None:
        if self.unit_index < 0:
            raise ValueError("Operator Call unit_index cannot be negative.")
        object.__setattr__(self, "input", freeze(self.input))


@dataclass(frozen=True, slots=True)
class OperatorCallCompleted:
    kind: ClassVar[str] = "operator_call.completed"
    call_id: str
    output: DurableValue
    execution_duration_ns: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "output", freeze(self.output))


@dataclass(frozen=True, slots=True)
class OperatorCallFailed:
    kind: ClassVar[str] = "operator_call.failed"
    call_id: str
    error: RuntimeErrorInfo
    execution_duration_ns: int = 0


@dataclass(frozen=True, slots=True)
class WaitRequested:
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
class RecoveryApplied:
    kind: ClassVar[str] = "recovery.applied"
    recovered_occurrence_ids: tuple[str, ...] = ()
    lost_call_ids: tuple[str, ...] = ()


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
class NodeCompleted:
    kind: ClassVar[str] = "node_occurrence.completed"
    occurrence_id: str
    output: DurableValue
    metrics: DurableValue = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "output", freeze(self.output))
        object.__setattr__(self, "metrics", freeze(self.metrics))


@dataclass(frozen=True, slots=True)
class NodeFailed:
    kind: ClassVar[str] = "node_occurrence.failed"
    occurrence_id: str
    error: RuntimeErrorInfo


@dataclass(frozen=True, slots=True)
class InputMapped:
    kind: ClassVar[str] = "input.mapped"
    occurrence_id: str
    mapped_input: DurableValue
    duration_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "mapped_input", freeze(self.mapped_input))


@dataclass(frozen=True, slots=True)
class CapabilityResolved:
    kind: ClassVar[str] = "capability.resolved"
    occurrence_id: str
    capability_id: str
    operator_id: str
    duration_ns: int


@dataclass(frozen=True, slots=True)
class Aggregated:
    kind: ClassVar[str] = "node.aggregated"
    occurrence_id: str
    output: DurableValue
    duration_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "output", freeze(self.output))


@dataclass(frozen=True, slots=True)
class OutputBound:
    kind: ClassVar[str] = "output.bound"
    occurrence_id: str
    patch: ContextPatch
    duration_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "patch", _freeze_patch(self.patch))


@dataclass(frozen=True, slots=True)
class EdgeConditionResult:
    edge_id: str
    selected: bool
    duration_ns: int

    def __post_init__(self) -> None:
        if not isinstance(self.edge_id, str) or not self.edge_id.strip():
            raise ValueError("Condition edge_id must be a non-empty string.")
        if type(self.selected) is not bool:
            raise TypeError("Condition selected must be a bool.")
        _duration(self.duration_ns)


@dataclass(frozen=True, slots=True)
class RoutingResolved:
    kind: ClassVar[str] = "routing.resolved"
    occurrence_id: str
    source_status: Literal["complete", "error"]
    conditions: tuple[EdgeConditionResult, ...]
    duration_ns: int


@dataclass(frozen=True, slots=True)
class NodeFaulted:
    kind: ClassVar[str] = "node.faulted"
    occurrence_id: str
    phase: str
    error: RuntimeErrorInfo
    duration_ns: int | None = None


RuntimeEventPayload: TypeAlias = (
    InputMapped
    | CapabilityResolved
    | Aggregated
    | OutputBound
    | RoutingResolved
    | NodeFaulted
    | SessionOpened
    | InvocationStarted
    | InvocationCompleted
    | InvocationFailed
    | InvocationCancelled
    | NodeStarted
    | OperatorCallStarted
    | OperatorCallCompleted
    | OperatorCallFailed
    | WaitRequested
    | WaitResumed
    | RecoveryApplied
    | ChildInvocationPlanned
    | ChildInvocationPhaseChanged
    | ChildAwaitSuspended
    | ChildAwaitReady
    | NodeCompleted
    | NodeFailed
)

_PAYLOAD_TYPES = (
    InputMapped, CapabilityResolved, Aggregated, OutputBound, RoutingResolved, NodeFaulted,
    SessionOpened,
    InvocationStarted,
    InvocationCompleted,
    InvocationFailed,
    InvocationCancelled,
    NodeStarted,
    OperatorCallStarted,
    OperatorCallCompleted,
    OperatorCallFailed,
    WaitRequested,
    WaitResumed,
    RecoveryApplied,
    ChildInvocationPlanned,
    ChildInvocationPhaseChanged,
    ChildAwaitSuspended,
    ChildAwaitReady,
    NodeCompleted,
    NodeFailed,
)


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    """One committed semantic boundary with at most one atomic StateDelta."""

    session_id: str
    sequence: int
    payload: RuntimeEventPayload
    invocation_id: str | None = None
    delta: StateDelta | None = None
    occurred_at_us: int = field(default_factory=unix_time_us)
    id: str = field(default_factory=lambda: str(uuid4()))
    schema_version: int = RUNTIME_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in ("session_id", "id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Runtime Event {name} must be a non-empty string.")
        for name in ("invocation_id",):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"Runtime Event {name} must be a non-empty string or None.")
        for name, minimum in (("sequence", 1), ("occurred_at_us", 0)):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
                raise ValueError(f"Runtime Event {name} is invalid.")
        if self.schema_version != RUNTIME_EVENT_SCHEMA_VERSION:
            raise ValueError("Unsupported Runtime Event schema.")
        validate_payload(self.payload)
        if self.delta is not None and not isinstance(self.delta, StateDelta):
            raise TypeError("Runtime Event delta must be StateDelta or None.")

    @property
    def event_name(self) -> str:
        return self.payload.kind

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version, "id": self.id,
            "session_id": self.session_id, "invocation_id": self.invocation_id,
            "sequence": self.sequence, "event_name": self.event_name,
            "payload": _payload_to_record(self.payload),
            "delta": self.delta.to_record() if self.delta is not None else None,
            "occurred_at_us": self.occurred_at_us,
        }

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "RuntimeEvent":
        if not isinstance(record, dict):
            raise TypeError("Runtime Event record must be a mapping.")
        event = cls(
            session_id=_required_string(record, "session_id"),
            sequence=_required_integer(record, "sequence"),
            payload=_payload_from_record(_required_string(record, "event_name"), _required_mapping(record, "payload")),
            invocation_id=record.get("invocation_id"),
            delta=StateDelta.from_record(record["delta"]) if record.get("delta") is not None else None,
            occurred_at_us=_required_integer(record, "occurred_at_us"),
            id=_required_string(record, "id"),
            schema_version=_required_integer(record, "schema_version"),
        )
        if event.to_record() != record:
            raise TypeError("Runtime Event record contains missing, unknown or non-canonical fields.")
        return event


def _payload_to_record(payload: RuntimeEventPayload) -> dict[str, object]:
    if isinstance(payload, (InputMapped, CapabilityResolved, Aggregated, OutputBound, RoutingResolved, NodeFaulted, OperatorCallStarted, OperatorCallCompleted, OperatorCallFailed, InvocationStarted, NodeCompleted, NodeFailed, RecoveryApplied)):
        return {item.name: _encode_payload_value(getattr(payload, item.name)) for item in fields(payload)}
    if isinstance(payload, SessionOpened):
        return {
            "context": thaw(payload.context),
        }
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
    if isinstance(payload, NodeStarted):
        return {"occurrence_id": payload.occurrence_id}
    if isinstance(payload, WaitRequested):
        return {
            "occurrence_id": payload.occurrence_id,
            "wait_id": payload.wait_id,
            "request": thaw(payload.request),
        }
    if isinstance(payload, WaitResumed):
        return {"wait_id": payload.wait_id, "response": thaw(payload.response)}
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
    raise TypeError(f"Unsupported Runtime Event payload: {type(payload).__name__}.")


def _payload_from_record(
    event_name: str, record: dict[str, object]
) -> RuntimeEventPayload:
    for cls in (InputMapped, CapabilityResolved, Aggregated, OutputBound, RoutingResolved, NodeFaulted, OperatorCallStarted, OperatorCallCompleted, OperatorCallFailed, InvocationStarted, NodeCompleted, NodeFailed, RecoveryApplied):
        if event_name == cls.kind:
            values = dict(record)
            if "error" in values:
                values["error"] = _error_from_record(values["error"])
            if "patch" in values:
                values["patch"] = _patch_from_record(values["patch"])
            for key in ("recovered_occurrence_ids", "lost_call_ids"):
                if key in values:
                    values[key] = tuple(values[key])
            if "conditions" in values:
                values["conditions"] = tuple(EdgeConditionResult(**item) for item in values["conditions"])
            return cls(**values)
    if event_name == SessionOpened.kind:
        return SessionOpened(
            _required_value(record, "context"),  # type: ignore[arg-type]
        )
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
    if event_name == NodeStarted.kind:
        return NodeStarted(_required_string(record, "occurrence_id"))
    if event_name == WaitRequested.kind:
        return WaitRequested(
            _required_string(record, "occurrence_id"),
            _required_string(record, "wait_id"),
            _required_value(record, "request"),
        )
    if event_name == WaitResumed.kind:
        return WaitResumed(
            _required_string(record, "wait_id"),
            _required_value(record, "response"),
        )
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
            freeze(item.get("value")),
        )

    invocation = value.get("invocation", [])
    session = value.get("session", [])
    if not isinstance(invocation, list) or not isinstance(session, list):
        raise TypeError("Context Patch sections must be lists.")
    return ContextPatch(
        invocation=tuple(decode(item) for item in invocation),
        session=tuple(decode(item) for item in session),
    )


def _encode_payload_value(value):
    if isinstance(value, RuntimeErrorInfo):
        return _error_to_record(value)
    if isinstance(value, ContextPatch):
        return _patch_to_record(value)
    if isinstance(value, EdgeConditionResult):
        return {item.name: getattr(value, item.name) for item in fields(value)}
    if isinstance(value, tuple):
        return [_encode_payload_value(item) for item in value]
    return thaw(value)


def _duration(value: object) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("Duration must be a non-negative integer.")


def validate_payload(payload: RuntimeEventPayload) -> None:
    """Validate small event-local data without traversing the Runtime State."""
    if not isinstance(payload, _PAYLOAD_TYPES):
        raise TypeError("Unsupported Runtime Event payload.")
    for item in fields(payload):
        value = getattr(payload, item.name)
        if item.name.endswith("_id"):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{item.name} must be a non-empty string.")
        if item.name.endswith("duration_ns") and value is not None:
            _duration(value)
        if item.name == "error" and not isinstance(value, RuntimeErrorInfo):
            raise TypeError("Event error must be RuntimeErrorInfo.")
    if isinstance(payload, OperatorCallStarted):
        _duration(payload.unit_index)
    if isinstance(payload, RoutingResolved):
        if payload.source_status not in {"complete", "error"}:
            raise ValueError("Invalid routing source status.")
        if not isinstance(payload.conditions, tuple) or not all(isinstance(item, EdgeConditionResult) for item in payload.conditions):
            raise TypeError("Routing conditions must be a tuple of EdgeConditionResult.")
        if len({item.edge_id for item in payload.conditions}) != len(payload.conditions):
            raise ValueError("Routing conditions must have unique edge identities.")
    if isinstance(payload, NodeFaulted) and payload.phase not in {
        "input_mapping", "capability_resolution", "operator", "aggregation",
        "output_binding", "condition", "validation",
    }:
        raise ValueError("Invalid Node fault phase.")
    if isinstance(payload, RecoveryApplied):
        for values in (payload.recovered_occurrence_ids, payload.lost_call_ids):
            if not isinstance(values, tuple) or not all(isinstance(value, str) and value for value in values):
                raise TypeError("Recovery identities must be a tuple of strings.")
            if len(set(values)) != len(values):
                raise ValueError("Recovery identities cannot repeat.")
