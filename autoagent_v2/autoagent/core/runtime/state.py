"""Canonical immutable Runtime State for one Session."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal

from .events import RuntimeErrorInfo
from .scheduling import (
    Activation,
    EdgeResolution,
    ExecutionScope,
    LoopIteration,
    LoopBoundaryResolution,
)
from .values import DurableValue, freeze, thaw


RUNTIME_STATE_SCHEMA_VERSION = 2
InvocationStatus = Literal[
    "created", "running", "waiting", "completed", "failed", "cancelled"
]
NodeOccurrenceStatus = Literal[
    "ready", "running", "waiting", "completed", "failed", "skipped", "cancelled"
]
OperatorCallStatus = Literal["running", "completed", "failed", "lost", "cancelled"]


@dataclass(frozen=True, slots=True)
class WaitState:
    id: str
    occurrence_id: str
    status: Literal["waiting", "resumed", "cancelled"]
    request: DurableValue
    response: DurableValue = None
    created_at_ns: int = 0
    resumed_at_ns: int | None = None


@dataclass(frozen=True, slots=True)
class OperatorCallState:
    id: str
    occurrence_id: str
    operator_id: str
    unit_index: int
    status: OperatorCallStatus
    input: DurableValue
    output: DurableValue = None
    error: RuntimeErrorInfo | None = None
    started_at_ns: int = 0
    completed_at_ns: int | None = None
    attempt: int = 1
    reason: Literal["normal", "retry", "fallback"] = "normal"


@dataclass(frozen=True, slots=True)
class ChildInvocationState:
    parent_occurrence_id: str
    session_id: str
    invocation_id: str
    workflow_id: str
    workflow_revision_id: str


@dataclass(frozen=True, slots=True)
class NodeOccurrenceState:
    id: str
    node_id: str
    scope: ExecutionScope
    status: NodeOccurrenceStatus
    output: DurableValue = None
    error: RuntimeErrorInfo | None = None
    started_at_ns: int | None = None
    completed_at_ns: int | None = None
    started_state_version: int | None = None
    activations: tuple[Activation, ...] = ()
    metrics: DurableValue = None
    recovery_attempts: int = 0


@dataclass(frozen=True, slots=True)
class SchedulerState:
    initialized: bool = False
    ready: tuple[str, ...] = ()
    occurrences: Mapping[str, NodeOccurrenceState] = field(
        default_factory=lambda: MappingProxyType({})
    )
    resolutions: Mapping[str, EdgeResolution] = field(
        default_factory=lambda: MappingProxyType({})
    )
    boundary_resolutions: Mapping[str, LoopBoundaryResolution] = field(
        default_factory=lambda: MappingProxyType({})
    )
    operator_calls: Mapping[str, OperatorCallState] = field(
        default_factory=lambda: MappingProxyType({})
    )
    waits: Mapping[str, WaitState] = field(
        default_factory=lambda: MappingProxyType({})
    )


@dataclass(frozen=True, slots=True)
class SessionState:
    id: str
    workflow_id: str
    context: DurableValue
    created_at_ns: int
    updated_at_ns: int
    latest_invocation_id: str | None = None
    context_path_revisions: Mapping[tuple[str, ...], int] = field(
        default_factory=lambda: MappingProxyType({})
    )


@dataclass(frozen=True, slots=True)
class InvocationState:
    id: str
    workflow_revision_id: str
    entry_node_id: str
    status: InvocationStatus
    input: DurableValue
    context: DurableValue
    output: DurableValue = None
    error: RuntimeErrorInfo | None = None
    cancel_reason: str | None = None
    created_at_ns: int = 0
    started_at_ns: int | None = None
    completed_at_ns: int | None = None
    scheduler: SchedulerState = field(default_factory=SchedulerState)
    children: Mapping[str, ChildInvocationState] = field(
        default_factory=lambda: MappingProxyType({})
    )
    context_path_revisions: Mapping[tuple[str, ...], int] = field(
        default_factory=lambda: MappingProxyType({})
    )

    @property
    def terminal(self) -> bool:
        return self.status in {"completed", "failed", "cancelled"}


@dataclass(frozen=True, slots=True)
class RuntimeState:
    """The state reconstructed from one Session's Runtime Event prefix."""

    session: SessionState | None = None
    invocation: InvocationState | None = None
    state_version: int = 0
    sequence: int = 0
    last_event_id: str | None = None
    last_event_digest: str | None = None
    last_event_semantic_digest: str | None = None
    schema_version: int = RUNTIME_STATE_SCHEMA_VERSION

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "state_version": self.state_version,
            "sequence": self.sequence,
            "last_event_id": self.last_event_id,
            "last_event_digest": self.last_event_digest,
            "last_event_semantic_digest": self.last_event_semantic_digest,
            "session": _session_record(self.session),
            "invocation": _invocation_record(self.invocation),
        }

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "RuntimeState":
        """Restore the typed immutable State from its canonical durable record."""

        if not isinstance(record, dict):
            raise TypeError("Runtime State record must be a mapping.")
        schema_version = _integer(record, "schema_version")
        if schema_version != RUNTIME_STATE_SCHEMA_VERSION:
            raise ValueError(f"Unsupported Runtime State schema {schema_version}.")
        state = cls(
            session=_session_from_record(record.get("session")),
            invocation=_invocation_from_record(record.get("invocation")),
            state_version=_integer(record, "state_version", default=0),
            sequence=_integer(record, "sequence"),
            last_event_id=_optional_string(record, "last_event_id"),
            last_event_digest=_optional_string(record, "last_event_digest"),
            last_event_semantic_digest=_optional_string(
                record, "last_event_semantic_digest"
            ),
            schema_version=schema_version,
        )
        if state.to_record() != record:
            raise TypeError(
                "Runtime State record contains missing, unknown, or non-canonical fields."
            )
        return state


def _session_record(value: SessionState | None) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value.context, Mapping):
        raise TypeError("Session Context must be a mapping.")
    return {
        "id": value.id,
        "workflow_id": value.workflow_id,
        "context": thaw(value.context),
        "created_at_ns": value.created_at_ns,
        "updated_at_ns": value.updated_at_ns,
        "latest_invocation_id": value.latest_invocation_id,
        "context_path_revisions": {
            _context_path_key(path): revision
            for path, revision in value.context_path_revisions.items()
        },
    }


def _invocation_record(value: InvocationState | None) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value.context, Mapping):
        raise TypeError("Invocation Context must be a mapping.")
    return {
        "id": value.id,
        "workflow_revision_id": value.workflow_revision_id,
        "entry_node_id": value.entry_node_id,
        "status": value.status,
        "input": thaw(value.input),
        "context": thaw(value.context),
        "output": thaw(value.output),
        "error": _error_record(value.error),
        "cancel_reason": value.cancel_reason,
        "created_at_ns": value.created_at_ns,
        "started_at_ns": value.started_at_ns,
        "completed_at_ns": value.completed_at_ns,
        "context_path_revisions": {
            _context_path_key(path): revision
            for path, revision in value.context_path_revisions.items()
        },
        "children": {
            key: {
                "parent_occurrence_id": item.parent_occurrence_id,
                "session_id": item.session_id,
                "invocation_id": item.invocation_id,
                "workflow_id": item.workflow_id,
                "workflow_revision_id": item.workflow_revision_id,
            }
            for key, item in value.children.items()
        },
        "scheduler": {
            "initialized": value.scheduler.initialized,
            "ready": list(value.scheduler.ready),
            "occurrences": {
                key: _occurrence_record(item)
                for key, item in value.scheduler.occurrences.items()
            },
            "resolutions": {
                key: _resolution_record(item)
                for key, item in value.scheduler.resolutions.items()
            },
            "boundary_resolutions": {
                key: {
                    "loop_region_id": item.loop_region_id,
                    "loop_scope": [
                        {
                            "loop_region_id": frame.loop_region_id,
                            "iteration": frame.iteration,
                        }
                        for frame in item.loop_scope
                    ],
                    "edge_id": item.edge_id,
                    "source_scope": [
                        {
                            "loop_region_id": frame.loop_region_id,
                            "iteration": frame.iteration,
                        }
                        for frame in item.source_scope
                    ],
                    "target_node_id": item.target_node_id,
                    "selected": item.selected,
                    "activation": (
                        {
                            "edge_id": item.activation.edge_id,
                            "source_occurrence_id": item.activation.source_occurrence_id,
                            "target_node_id": item.activation.target_node_id,
                        }
                        if item.activation is not None
                        else None
                    ),
                }
                for key, item in value.scheduler.boundary_resolutions.items()
            },
            "operator_calls": {
                key: {
                    "id": item.id,
                    "occurrence_id": item.occurrence_id,
                    "operator_id": item.operator_id,
                    "unit_index": item.unit_index,
                    "status": item.status,
                    "input": thaw(item.input),
                    "output": thaw(item.output),
                    "error": _error_record(item.error),
                    "started_at_ns": item.started_at_ns,
                    "completed_at_ns": item.completed_at_ns,
                    "attempt": item.attempt,
                    "reason": item.reason,
                }
                for key, item in value.scheduler.operator_calls.items()
            },
            "waits": {
                key: {
                    "id": item.id,
                    "occurrence_id": item.occurrence_id,
                    "status": item.status,
                    "request": thaw(item.request),
                    "response": thaw(item.response),
                    "created_at_ns": item.created_at_ns,
                    "resumed_at_ns": item.resumed_at_ns,
                }
                for key, item in value.scheduler.waits.items()
            },
        },
    }


def _occurrence_record(value: NodeOccurrenceState) -> dict[str, object]:
    return {
        "id": value.id,
        "node_id": value.node_id,
        "scope": [
            {"loop_region_id": item.loop_region_id, "iteration": item.iteration}
            for item in value.scope
        ],
        "status": value.status,
        "output": thaw(value.output),
        "error": _error_record(value.error),
        "started_at_ns": value.started_at_ns,
        "completed_at_ns": value.completed_at_ns,
        "started_state_version": value.started_state_version,
        "activations": [
            {
                "edge_id": item.edge_id,
                "source_occurrence_id": item.source_occurrence_id,
                "target_node_id": item.target_node_id,
            }
            for item in value.activations
        ],
        "metrics": thaw(value.metrics),
        "recovery_attempts": value.recovery_attempts,
    }


def _error_record(value: RuntimeErrorInfo | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        key: item
        for key, item in {
            "type": value.type,
            "message": value.message,
            "code": value.code,
            "phase": value.phase,
            "retryable": value.retryable,
            "cause": value.cause,
        }.items()
        if item is not None
    }


def _resolution_record(value: EdgeResolution) -> dict[str, object]:
    return {
        "edge_id": value.edge_id,
        "target_node_id": value.target_node_id,
        "target_scope": [
            {"loop_region_id": item.loop_region_id, "iteration": item.iteration}
            for item in value.target_scope
        ],
        "selected": value.selected,
        "activation": (
            {
                "edge_id": value.activation.edge_id,
                "source_occurrence_id": value.activation.source_occurrence_id,
                "target_node_id": value.activation.target_node_id,
            }
            if value.activation is not None
            else None
        ),
    }


def _session_from_record(value: object) -> SessionState | None:
    if value is None:
        return None
    record = _mapping(value, "Session State")
    context = record.get("context")
    if not isinstance(context, dict):
        raise TypeError("Session Context must be a mapping.")
    return SessionState(
        id=_string(record, "id"),
        workflow_id=_string(record, "workflow_id"),
        context=freeze(context),
        created_at_ns=_integer(record, "created_at_ns"),
        updated_at_ns=_integer(record, "updated_at_ns"),
        latest_invocation_id=_optional_string(record, "latest_invocation_id"),
        context_path_revisions=MappingProxyType(
            _context_revisions(record.get("context_path_revisions"))
        ),
    )


def _invocation_from_record(value: object) -> InvocationState | None:
    if value is None:
        return None
    record = _mapping(value, "Invocation State")
    context = record.get("context")
    if not isinstance(context, dict):
        raise TypeError("Invocation Context must be a mapping.")
    children_record = _mapping(record.get("children"), "Child Invocations")
    scheduler = _scheduler_from_record(record.get("scheduler"))
    status = _string(record, "status")
    if status not in {"created", "running", "waiting", "completed", "failed", "cancelled"}:
        raise ValueError(f"Unsupported Invocation status {status!r}.")
    return InvocationState(
        id=_string(record, "id"),
        workflow_revision_id=_string(record, "workflow_revision_id"),
        entry_node_id=_string(record, "entry_node_id"),
        status=status,  # type: ignore[arg-type]
        input=freeze(record.get("input")),
        context=freeze(context),
        output=freeze(record.get("output")),
        error=_error_from_record(record.get("error")),
        cancel_reason=_optional_string(record, "cancel_reason"),
        created_at_ns=_integer(record, "created_at_ns"),
        started_at_ns=_optional_integer(record, "started_at_ns"),
        completed_at_ns=_optional_integer(record, "completed_at_ns"),
        scheduler=scheduler,
        children=MappingProxyType(
            {
                key: _child_from_record(item)
                for key, item in children_record.items()
            }
        ),
        context_path_revisions=MappingProxyType(
            _context_revisions(record.get("context_path_revisions"))
        ),
    )


def _scheduler_from_record(value: object) -> SchedulerState:
    record = _mapping(value, "Scheduler State")
    occurrences = _mapping(record.get("occurrences"), "Node Occurrences")
    resolutions = _mapping(record.get("resolutions"), "Edge Resolutions")
    boundaries = _mapping(
        record.get("boundary_resolutions"), "Loop Boundary Resolutions"
    )
    calls = _mapping(record.get("operator_calls"), "Operator Calls")
    waits = _mapping(record.get("waits"), "Waits")
    ready = record.get("ready")
    if not isinstance(ready, list) or not all(isinstance(item, str) for item in ready):
        raise TypeError("Scheduler ready must be a list of strings.")
    initialized = record.get("initialized")
    if type(initialized) is not bool:
        raise TypeError("Scheduler initialized must be bool.")
    return SchedulerState(
        initialized=initialized,
        ready=tuple(ready),
        occurrences=MappingProxyType(
            {key: _occurrence_from_record(item) for key, item in occurrences.items()}
        ),
        resolutions=MappingProxyType(
            {key: _resolution_from_record(item) for key, item in resolutions.items()}
        ),
        boundary_resolutions=MappingProxyType(
            {key: _boundary_from_record(item) for key, item in boundaries.items()}
        ),
        operator_calls=MappingProxyType(
            {key: _call_from_record(item) for key, item in calls.items()}
        ),
        waits=MappingProxyType(
            {key: _wait_from_record(item) for key, item in waits.items()}
        ),
    )


def _occurrence_from_record(value: object) -> NodeOccurrenceState:
    record = _mapping(value, "Node Occurrence")
    status = _string(record, "status")
    if status not in {"ready", "running", "waiting", "completed", "failed", "skipped", "cancelled"}:
        raise ValueError(f"Unsupported Node Occurrence status {status!r}.")
    activations = record.get("activations")
    if not isinstance(activations, list):
        raise TypeError("Node Occurrence activations must be a list.")
    return NodeOccurrenceState(
        id=_string(record, "id"),
        node_id=_string(record, "node_id"),
        scope=_scope_from_record(record.get("scope")),
        status=status,  # type: ignore[arg-type]
        output=freeze(record.get("output")),
        error=_error_from_record(record.get("error")),
        started_at_ns=_optional_integer(record, "started_at_ns"),
        completed_at_ns=_optional_integer(record, "completed_at_ns"),
        started_state_version=_optional_integer(record, "started_state_version"),
        activations=tuple(_activation_from_record(item) for item in activations),
        metrics=freeze(record.get("metrics")),
        recovery_attempts=_integer(record, "recovery_attempts"),
    )


def _resolution_from_record(value: object) -> EdgeResolution:
    record = _mapping(value, "Edge Resolution")
    selected = _boolean(record, "selected")
    return EdgeResolution(
        edge_id=_string(record, "edge_id"),
        target_node_id=_string(record, "target_node_id"),
        target_scope=_scope_from_record(record.get("target_scope")),
        selected=selected,
        activation=(
            _activation_from_record(record["activation"])
            if record.get("activation") is not None
            else None
        ),
    )


def _boundary_from_record(value: object) -> LoopBoundaryResolution:
    record = _mapping(value, "Loop Boundary Resolution")
    return LoopBoundaryResolution(
        loop_region_id=_string(record, "loop_region_id"),
        loop_scope=_scope_from_record(record.get("loop_scope")),
        edge_id=_string(record, "edge_id"),
        source_scope=_scope_from_record(record.get("source_scope")),
        target_node_id=_string(record, "target_node_id"),
        selected=_boolean(record, "selected"),
        activation=(
            _activation_from_record(record["activation"])
            if record.get("activation") is not None
            else None
        ),
    )


def _call_from_record(value: object) -> OperatorCallState:
    record = _mapping(value, "Operator Call")
    status = _string(record, "status")
    if status not in {"running", "completed", "failed", "lost", "cancelled"}:
        raise ValueError(f"Unsupported Operator Call status {status!r}.")
    reason = _string(record, "reason")
    if reason not in {"normal", "retry", "fallback"}:
        raise ValueError(f"Unsupported Operator Call reason {reason!r}.")
    return OperatorCallState(
        id=_string(record, "id"),
        occurrence_id=_string(record, "occurrence_id"),
        operator_id=_string(record, "operator_id"),
        unit_index=_integer(record, "unit_index"),
        status=status,  # type: ignore[arg-type]
        input=freeze(record.get("input")),
        output=freeze(record.get("output")),
        error=_error_from_record(record.get("error")),
        started_at_ns=_integer(record, "started_at_ns"),
        completed_at_ns=_optional_integer(record, "completed_at_ns"),
        attempt=_integer(record, "attempt"),
        reason=reason,  # type: ignore[arg-type]
    )


def _wait_from_record(value: object) -> WaitState:
    record = _mapping(value, "Wait State")
    status = _string(record, "status")
    if status not in {"waiting", "resumed", "cancelled"}:
        raise ValueError(f"Unsupported Wait status {status!r}.")
    return WaitState(
        id=_string(record, "id"),
        occurrence_id=_string(record, "occurrence_id"),
        status=status,  # type: ignore[arg-type]
        request=freeze(record.get("request")),
        response=freeze(record.get("response")),
        created_at_ns=_integer(record, "created_at_ns"),
        resumed_at_ns=_optional_integer(record, "resumed_at_ns"),
    )


def _child_from_record(value: object) -> ChildInvocationState:
    record = _mapping(value, "Child Invocation")
    return ChildInvocationState(
        parent_occurrence_id=_string(record, "parent_occurrence_id"),
        session_id=_string(record, "session_id"),
        invocation_id=_string(record, "invocation_id"),
        workflow_id=_string(record, "workflow_id"),
        workflow_revision_id=_string(record, "workflow_revision_id"),
    )


def _scope_from_record(value: object) -> ExecutionScope:
    if not isinstance(value, list):
        raise TypeError("Execution Scope must be a list.")
    return tuple(
        LoopIteration(
            _string(_mapping(item, "Loop Iteration"), "loop_region_id"),
            _integer(_mapping(item, "Loop Iteration"), "iteration"),
        )
        for item in value
    )


def _activation_from_record(value: object) -> Activation:
    record = _mapping(value, "Activation")
    return Activation(
        edge_id=_string(record, "edge_id"),
        source_occurrence_id=_string(record, "source_occurrence_id"),
        target_node_id=_string(record, "target_node_id"),
    )


def _error_from_record(value: object) -> RuntimeErrorInfo | None:
    if value is None:
        return None
    record = _mapping(value, "Runtime Error")
    retryable = record.get("retryable")
    if retryable is not None and type(retryable) is not bool:
        raise TypeError("Runtime Error retryable must be bool or None.")
    return RuntimeErrorInfo(
        type=_string(record, "type"),
        message=_string(record, "message"),
        code=_optional_string(record, "code"),
        phase=_optional_string(record, "phase"),
        retryable=retryable,
        cause=_optional_string(record, "cause"),
    )


def _context_revisions(value: object) -> dict[tuple[str, ...], int]:
    record = _mapping(value, "Context path revisions")
    result: dict[tuple[str, ...], int] = {}
    for key, revision in record.items():
        if not isinstance(key, str) or not key:
            raise TypeError("Context revision key must be a non-empty string.")
        if not isinstance(revision, int) or isinstance(revision, bool):
            raise TypeError("Context revision value must be an integer.")
        result[_context_path_from_key(key)] = revision
    return result


def _context_path_key(path: tuple[str, ...]) -> str:
    if not path:
        raise ValueError("Context revision path cannot be empty.")
    return "/" + "/".join(
        token.replace("~", "~0").replace("/", "~1") for token in path
    )


def _context_path_from_key(value: str) -> tuple[str, ...]:
    if not value.startswith("/") or value == "/":
        raise ValueError("Context revision key must be a non-root JSON Pointer.")
    return tuple(
        token.replace("~1", "/").replace("~0", "~")
        for token in value[1:].split("/")
    )


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be a mapping.")
    if not all(isinstance(key, str) for key in value):
        raise TypeError(f"{label} keys must be strings.")
    return value


def _string(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise TypeError(f"{key} must be a non-empty string.")
    return value


def _optional_string(record: dict[str, object], key: str) -> str | None:
    value = record.get(key)
    if value is not None and (not isinstance(value, str) or not value):
        raise TypeError(f"{key} must be a non-empty string or None.")
    return value


def _integer(
    record: dict[str, object], key: str, *, default: int | None = None
) -> int:
    value = record.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{key} must be an integer.")
    return value


def _optional_integer(record: dict[str, object], key: str) -> int | None:
    value = record.get(key)
    if value is not None and (
        not isinstance(value, int) or isinstance(value, bool)
    ):
        raise TypeError(f"{key} must be an integer or None.")
    return value


def _boolean(record: dict[str, object], key: str) -> bool:
    value = record.get(key)
    if type(value) is not bool:
        raise TypeError(f"{key} must be bool.")
    return value
