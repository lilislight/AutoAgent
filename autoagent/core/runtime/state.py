"""Canonical immutable Runtime State for one Session."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Literal

from .events import RuntimeErrorInfo
from .scheduling import (
    Activation,
    EdgeResolution,
    ExecutionScope,
    LoopIteration,
    LoopBoundaryResolution,
    occurrence_key,
)
from .values import DurableValue, freeze, thaw
from ._chunked import ChunkedUnits, runtime_mapping, child_units
from ..context import ContextPatch
from .events import EdgeConditionResult, _patch_to_record, _patch_from_record


RUNTIME_STATE_SCHEMA_VERSION = 8
InvocationStatus = Literal[
    "created", "running", "waiting", "settling", "completed", "failed", "cancelled"
]
NodeOccurrenceStatus = Literal[
    "ready", "running", "waiting", "completed", "failed", "skipped", "cancelled"
]
OperatorCallStatus = Literal["running", "completed", "failed", "lost", "cancelled"]
ChildInvocationMode = Literal["await", "spawn"]
ChildUnitPhase = Literal["planned", "opened", "accepted", "terminal", "abandoned"]


@dataclass(frozen=True, slots=True)
class WaitState:
    id: str
    occurrence_id: str
    status: Literal["waiting", "resumed", "cancelled"]
    request: DurableValue
    response: DurableValue = None
    created_at_us: int = 0
    resumed_at_us: int | None = None


@dataclass(frozen=True, slots=True)
class OperatorCallState:
    """Call lifecycle; input/output are released when the owning Node settles."""
    id: str
    occurrence_id: str
    operator_id: str
    unit_index: int
    status: OperatorCallStatus
    input: DurableValue
    output: DurableValue = None
    error: RuntimeErrorInfo | None = None
    started_at_us: int = 0
    completed_at_us: int | None = None
    queue_duration_ns: int = 0
    execution_duration_ns: int | None = None


@dataclass(frozen=True, slots=True)
class ChildUnitState:
    unit_index: int
    session_id: str
    invocation_id: str
    input: DurableValue
    phase: ChildUnitPhase = "planned"
    input_released: bool = False


@dataclass(frozen=True, slots=True)
class ChildInvocationPlan:
    creation_id: str
    parent_occurrence_id: str
    mode: ChildInvocationMode
    workflow_id: str
    workflow_revision_id: str
    units: tuple[ChildUnitState, ...] | ChunkedUnits = ()


@dataclass(frozen=True, slots=True)
class NodeExecutionState:
    """Materialized successful stages needed to continue without user recomputation."""

    phase: str = "none"
    completed_stages: tuple[str, ...] = ()
    mapped_input: DurableValue = None
    resolved_capability_id: str | None = None
    resolved_operator_id: str | None = None
    aggregate_output: DurableValue = None
    pending_context_patch: ContextPatch = ContextPatch()
    routing: tuple[EdgeConditionResult, ...] = ()
    routing_source_status: str | None = None
    fault: RuntimeErrorInfo | None = None


@dataclass(frozen=True, slots=True)
class NodeOccurrenceState:
    """Scheduling identity; output exists only while execution/result consumers need it."""
    id: str
    node_id: str
    scope: ExecutionScope
    status: NodeOccurrenceStatus
    output: DurableValue = None
    error: RuntimeErrorInfo | None = None
    started_at_us: int | None = None
    completed_at_us: int | None = None
    started_sequence: int | None = None
    ready_at_us: int | None = None
    execution: NodeExecutionState = field(default_factory=NodeExecutionState)
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
    context: DurableValue
    created_at_us: int
    updated_at_us: int
    latest_invocation_id: str | None = None
    context_path_revisions: Mapping[tuple[str, ...], int] = field(
        default_factory=lambda: MappingProxyType({})
    )


@dataclass(frozen=True, slots=True)
class InvocationState:
    id: str
    workflow_id: str
    workflow_revision_id: str
    entry_node_id: str
    status: InvocationStatus
    input: DurableValue
    context: DurableValue
    output: DurableValue = None
    error: RuntimeErrorInfo | None = None
    cancel_reason: str | None = None
    created_at_us: int = 0
    started_at_us: int | None = None
    completed_at_us: int | None = None
    scheduler: SchedulerState = field(default_factory=SchedulerState)
    child_plans: Mapping[str, ChildInvocationPlan] = field(
        default_factory=lambda: MappingProxyType({})
    )
    context_path_revisions: Mapping[tuple[str, ...], int] = field(
        default_factory=lambda: MappingProxyType({})
    )

    pending_outcome: Literal["completed", "failed", "cancelled"] | None = None

    @property
    def stopping(self) -> bool:
        return self.status in {"failed", "cancelled"} or (
            self.status == "settling" and self.pending_outcome in {"failed", "cancelled"}
        )

    @property
    def terminal(self) -> bool:
        return self.status in {"completed", "failed", "cancelled"}


_EMPTY_SCHEDULER = SchedulerState()
_EMPTY_CONTEXT = freeze({})


@dataclass(frozen=True, slots=True)
class ChildResult:
    """Terminal Child identity and result, without an execution workspace."""
    id: str
    workflow_id: str
    workflow_revision_id: str
    entry_node_id: str
    status: Literal["completed", "failed", "cancelled"]
    output: DurableValue
    error: RuntimeErrorInfo | None
    cancel_reason: str | None
    created_at_us: int
    started_at_us: int | None
    completed_at_us: int
    parent_session_id: str
    parent_invocation_id: str
    creation_id: str
    unit_index: int
    input_digest: str
    child_plans: Mapping[str, ChildInvocationPlan]
    kind: Literal["child_result"] = field(default="child_result", init=False)

    # Common read-only view used by graph traversal and result consumers.
    @property
    def terminal(self) -> bool:
        return True
    @property
    def stopping(self) -> bool:
        return self.status in {"failed", "cancelled"}
    @property
    def pending_outcome(self) -> None:
        return None
    @property
    def scheduler(self) -> SchedulerState:
        return _EMPTY_SCHEDULER
    @property
    def context(self) -> DurableValue:
        return _EMPTY_CONTEXT
    @property
    def context_path_revisions(self) -> Mapping[tuple[str, ...], int]:
        return _EMPTY_CONTEXT
    @property
    def input(self) -> None:
        return None


def child_input_digest(value: DurableValue) -> str:
    """A bounded-size admission proof for input-free terminal results."""
    import hashlib
    import json
    digest = hashlib.sha256()
    for chunk in json.JSONEncoder(sort_keys=True, separators=(",", ":"), allow_nan=False).iterencode(thaw(value)):
        digest.update(chunk.encode())
    return digest.hexdigest()


def release_child_inputs(plans: Mapping[str, ChildInvocationPlan]) -> Mapping[str, ChildInvocationPlan]:
    """Drop plan payloads after the owning Node no longer needs its input list."""
    changed = {}
    for key, plan in plans.items():
        if any(not unit.input_released for unit in plan.units):
            changed[key] = replace(plan, units=child_units(tuple(
                replace(unit, input=None, input_released=True) if not unit.input_released else unit
                for unit in plan.units)))
    return runtime_mapping({**plans, **changed}) if changed else plans


@dataclass(frozen=True, slots=True)
class RuntimeState:
    """The state reconstructed from one Session's Runtime Event prefix."""

    session: SessionState | None = None
    invocation: InvocationState | ChildResult | None = None
    sequence: int = 0
    last_event_id: str | None = None
    schema_version: int = RUNTIME_STATE_SCHEMA_VERSION

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "last_event_id": self.last_event_id,
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
            sequence=_integer(record, "sequence"),
            last_event_id=_optional_string(record, "last_event_id"),
            schema_version=schema_version,
        )
        validate_runtime_state(state)
        if state.to_record() != record:
            raise TypeError(
                "Runtime State record contains missing, unknown, or non-canonical fields."
            )
        return state


def validate_runtime_state(state: RuntimeState) -> None:
    """Validate one complete Runtime State and every durable internal reference."""

    if not isinstance(state, RuntimeState):
        raise TypeError("Runtime State must be a RuntimeState instance.")
    if state.schema_version != RUNTIME_STATE_SCHEMA_VERSION:
        raise ValueError(f"Unsupported Runtime State schema {state.schema_version}.")
    _non_negative(state.sequence, "Runtime State sequence")
    _optional_non_empty_string(state.last_event_id, "Runtime State last_event_id")
    if (state.sequence == 0) != (state.last_event_id is None):
        raise ValueError("Runtime State sequence and last Event identity disagree.")

    session = state.session
    invocation = state.invocation
    if session is None:
        if invocation is not None:
            raise ValueError("Runtime State Invocation requires a Session.")
        return

    _non_empty_string_value(session.id, "Session id")
    if not isinstance(session.context, Mapping):
        raise TypeError("Session Context must be a mapping.")
    _non_negative(session.created_at_us, "Session created_at_us")
    _non_negative(session.updated_at_us, "Session updated_at_us")
    _optional_non_empty_string(
        session.latest_invocation_id, "Session latest_invocation_id"
    )
    _validate_context_revision_map(
        session.context_path_revisions,
        state.sequence,
        "Session context_path_revisions",
    )

    if invocation is None:
        if session.latest_invocation_id is not None:
            raise ValueError(
                "Session latest_invocation_id requires a current Invocation."
            )
        return
    if session.latest_invocation_id != invocation.id:
        raise ValueError("Session and Invocation identities are inconsistent.")

    for value, label in (
        (invocation.id, "Invocation id"),
        (invocation.workflow_id, "Invocation workflow_id"),
        (invocation.workflow_revision_id, "Invocation workflow_revision_id"),
        (invocation.entry_node_id, "Invocation entry_node_id"),
    ):
        _non_empty_string_value(value, label)
    if invocation.status not in {
        "created",
        "running",
        "waiting",
        "settling",
        "completed",
        "failed",
        "cancelled",
    }:
        raise ValueError(f"Unsupported Invocation status {invocation.status!r}.")
    if not isinstance(invocation.context, Mapping):
        raise TypeError("Invocation Context must be a mapping.")
    _non_negative(invocation.created_at_us, "Invocation created_at_us")
    _optional_non_negative(invocation.started_at_us, "Invocation started_at_us")
    _optional_non_negative(invocation.completed_at_us, "Invocation completed_at_us")
    _timestamp_within_session(
        invocation.created_at_us,
        session,
        "Invocation created_at_us",
    )
    _optional_timestamp_within_session(
        invocation.started_at_us,
        session,
        "Invocation started_at_us",
    )
    _optional_timestamp_within_session(
        invocation.completed_at_us,
        session,
        "Invocation completed_at_us",
    )
    _validate_invocation_lifecycle(invocation)
    _optional_non_empty_string(invocation.cancel_reason, "Invocation cancel_reason")
    _validate_context_revision_map(
        invocation.context_path_revisions,
        state.sequence,
        "Invocation context_path_revisions",
    )
    if isinstance(invocation, ChildResult):
        if invocation.status not in {"completed", "failed", "cancelled"}:
            raise ValueError("ChildResult must be terminal.")
        for name in ("parent_session_id", "parent_invocation_id", "creation_id"):
            _non_empty_string_value(getattr(invocation, name), name)
        _non_negative(invocation.unit_index, "ChildResult unit_index")
        if len(invocation.input_digest) != 64 or any(c not in "0123456789abcdef" for c in invocation.input_digest):
            raise ValueError("Invalid ChildResult input digest.")
        if session.context or session.context_path_revisions:
            raise ValueError("Compacted Child cannot retain Session Context.")
        _validate_child_plans(invocation.child_plans, None)
        if any(not u.input_released for p in invocation.child_plans.values() for u in p.units):
            raise ValueError("Compacted Child cannot retain plan input.")
        return
    _validate_scheduler(
        invocation.scheduler,
        invocation.child_plans,
        state.sequence,
        session,
    )


def _validate_scheduler(
    scheduler: SchedulerState,
    child_plans: Mapping[str, ChildInvocationPlan],
    sequence: int,
    session: SessionState,
) -> None:
    if not isinstance(scheduler, SchedulerState):
        raise TypeError("Invocation scheduler must be SchedulerState.")
    if type(scheduler.initialized) is not bool:
        raise TypeError("Scheduler initialized must be bool.")
    if not isinstance(scheduler.occurrences, Mapping):
        raise TypeError("Scheduler occurrences must be a mapping.")

    occurrences = scheduler.occurrences
    for key, occurrence in occurrences.items():
        _non_empty_string_value(key, "Node Occurrence key")
        if not isinstance(occurrence, NodeOccurrenceState):
            raise TypeError("Scheduler occurrences must contain NodeOccurrenceState.")
        if key != occurrence.id:
            raise ValueError("Node Occurrence key must equal its id.")
        _non_empty_string_value(occurrence.id, "Node Occurrence id")
        _non_empty_string_value(occurrence.node_id, "Node Occurrence node_id")
        _validate_scope(occurrence.scope, "Node Occurrence scope")
        if occurrence.id != occurrence_key(occurrence.node_id, occurrence.scope):
            raise ValueError(
                "Node Occurrence id must match its node_id and Execution Scope."
            )
        if occurrence.status not in {
            "ready",
            "running",
            "waiting",
            "completed",
            "failed",
            "skipped",
            "cancelled",
        }:
            raise ValueError(
                f"Unsupported Node Occurrence status {occurrence.status!r}."
            )
        _optional_non_negative(occurrence.ready_at_us, "Node Occurrence ready_at_us")
        _validate_execution(occurrence.execution)
        _optional_non_negative(
            occurrence.started_at_us, "Node Occurrence started_at_us"
        )
        _optional_non_negative(
            occurrence.completed_at_us, "Node Occurrence completed_at_us"
        )
        _optional_timestamp_within_session(
            occurrence.started_at_us,
            session,
            "Node Occurrence started_at_us",
        )
        _optional_timestamp_within_session(
            occurrence.completed_at_us,
            session,
            "Node Occurrence completed_at_us",
        )
        _optional_non_negative(
            occurrence.started_sequence,
            "Node Occurrence started_sequence",
        )
        if (
            occurrence.started_sequence is not None
            and occurrence.started_sequence > sequence
        ):
            raise ValueError(
                "Node Occurrence started_sequence exceeds Runtime State version."
            )
        _non_negative(
            occurrence.recovery_attempts, "Node Occurrence recovery_attempts"
        )
        _validate_occurrence_lifecycle(occurrence)
        if not isinstance(occurrence.activations, tuple):
            raise TypeError("Node Occurrence activations must be a tuple.")
        for activation in occurrence.activations:
            _validate_activation(
                activation,
                occurrences,
                expected_target_node_id=occurrence.node_id,
            )

    if not isinstance(scheduler.ready, tuple):
        raise TypeError("Scheduler ready must be a tuple.")
    if len(scheduler.ready) != len(set(scheduler.ready)):
        raise ValueError("Scheduler ready cannot contain duplicate Occurrence ids.")
    for occurrence_id in scheduler.ready:
        _non_empty_string_value(occurrence_id, "Scheduler ready Occurrence id")
        occurrence = occurrences.get(occurrence_id)
        if occurrence is None:
            raise ValueError("Scheduler ready references an unknown Node Occurrence.")
        if occurrence.status != "ready":
            raise ValueError("Scheduler ready references a non-ready Node Occurrence.")
    ready_occurrences = {
        occurrence.id
        for occurrence in occurrences.values()
        if occurrence.status == "ready"
    }
    if set(scheduler.ready) != ready_occurrences:
        raise ValueError("Every ready Node Occurrence must appear in Scheduler ready.")

    _validate_resolution_map(scheduler.resolutions, occurrences)
    _validate_boundary_resolution_map(
        scheduler.boundary_resolutions, occurrences
    )
    _validate_operator_calls(scheduler.operator_calls, occurrences, session)
    _validate_waits(scheduler.waits, occurrences, session)
    _validate_child_plans(child_plans, occurrences)


def _validate_resolution_map(
    resolutions: Mapping[str, EdgeResolution],
    occurrences: Mapping[str, NodeOccurrenceState],
) -> None:
    if not isinstance(resolutions, Mapping):
        raise TypeError("Scheduler resolutions must be a mapping.")
    for key, resolution in resolutions.items():
        _non_empty_string_value(key, "Edge Resolution key")
        if not isinstance(resolution, EdgeResolution):
            raise TypeError("Scheduler resolutions must contain EdgeResolution.")
        if key != resolution.id:
            raise ValueError("Edge Resolution key must equal its id.")
        _non_empty_string_value(resolution.edge_id, "Edge Resolution edge_id")
        _non_empty_string_value(
            resolution.target_node_id, "Edge Resolution target_node_id"
        )
        _validate_scope(resolution.target_scope, "Edge Resolution target_scope")
        if type(resolution.selected) is not bool:
            raise TypeError("Edge Resolution selected must be bool.")
        if resolution.selected != (resolution.activation is not None):
            raise ValueError("Selected Edge Resolution requires one Activation.")
        if resolution.activation is not None:
            _validate_activation(
                resolution.activation,
                occurrences,
                expected_edge_id=resolution.edge_id,
                expected_target_node_id=resolution.target_node_id,
            )


def _validate_boundary_resolution_map(
    resolutions: Mapping[str, LoopBoundaryResolution],
    occurrences: Mapping[str, NodeOccurrenceState],
) -> None:
    if not isinstance(resolutions, Mapping):
        raise TypeError("Scheduler boundary_resolutions must be a mapping.")
    for key, resolution in resolutions.items():
        _non_empty_string_value(key, "Loop Boundary Resolution key")
        if not isinstance(resolution, LoopBoundaryResolution):
            raise TypeError(
                "Scheduler boundary_resolutions must contain LoopBoundaryResolution."
            )
        if key != resolution.id:
            raise ValueError("Loop Boundary Resolution key must equal its id.")
        _non_empty_string_value(
            resolution.loop_region_id, "Loop Boundary Resolution loop_region_id"
        )
        _non_empty_string_value(
            resolution.edge_id, "Loop Boundary Resolution edge_id"
        )
        _non_empty_string_value(
            resolution.target_node_id,
            "Loop Boundary Resolution target_node_id",
        )
        _validate_scope(resolution.loop_scope, "Loop Boundary Resolution loop_scope")
        _validate_scope(
            resolution.source_scope, "Loop Boundary Resolution source_scope"
        )
        if type(resolution.selected) is not bool:
            raise TypeError("Loop Boundary Resolution selected must be bool.")
        if resolution.selected != (resolution.activation is not None):
            raise ValueError("Selected Loop Boundary Resolution requires one Activation.")
        if resolution.activation is not None:
            _validate_activation(
                resolution.activation,
                occurrences,
                expected_edge_id=resolution.edge_id,
                expected_target_node_id=resolution.target_node_id,
            )


def _validate_operator_calls(
    calls: Mapping[str, OperatorCallState],
    occurrences: Mapping[str, NodeOccurrenceState],
    session: SessionState,
) -> None:
    if not isinstance(calls, Mapping):
        raise TypeError("Scheduler operator_calls must be a mapping.")
    for key, call in calls.items():
        _non_empty_string_value(key, "Operator Call key")
        if not isinstance(call, OperatorCallState):
            raise TypeError("Scheduler operator_calls must contain OperatorCallState.")
        if key != call.id:
            raise ValueError("Operator Call key must equal its id.")
        for value, label in (
            (call.id, "Operator Call id"),
            (call.occurrence_id, "Operator Call occurrence_id"),
            (call.operator_id, "Operator Call operator_id"),
        ):
            _non_empty_string_value(value, label)
        occurrence = occurrences.get(call.occurrence_id)
        if occurrence is None:
            raise ValueError("Operator Call references an unknown Node Occurrence.")
        _non_negative(call.unit_index, "Operator Call unit_index")
        _non_negative(call.queue_duration_ns, "Operator Call queue_duration_ns")
        _optional_non_negative(call.execution_duration_ns, "Operator Call execution_duration_ns")
        _non_negative(call.started_at_us, "Operator Call started_at_us")
        _optional_non_negative(call.completed_at_us, "Operator Call completed_at_us")
        _timestamp_within_session(
            call.started_at_us,
            session,
            "Operator Call started_at_us",
        )
        _optional_timestamp_within_session(
            call.completed_at_us,
            session,
            "Operator Call completed_at_us",
        )
        if call.status not in {
            "running",
            "completed",
            "failed",
            "lost",
            "cancelled",
        }:
            raise ValueError(f"Unsupported Operator Call status {call.status!r}.")
        if (call.status == "running") != (call.completed_at_us is None):
            raise ValueError(
                "Only a running Operator Call may omit completed_at_us."
            )
        if call.status == "failed":
            if call.error is None:
                raise ValueError("A failed Operator Call requires error details.")
        elif call.error is not None:
            raise ValueError("Only a failed Operator Call may carry error details.")
        if occurrence.status == "skipped":
            raise ValueError("Operator Call cannot belong to a skipped Node Occurrence.")
        if call.status == "running":
            if occurrence.status != "running":
                raise ValueError(
                    "A running Operator Call requires a running Node Occurrence."
                )
            assert occurrence.started_at_us is not None
        if call.status == "cancelled" and occurrence.status != "cancelled":
            raise ValueError(
                "A cancelled Operator Call requires a cancelled Node Occurrence."
            )


def _validate_waits(
    waits: Mapping[str, WaitState],
    occurrences: Mapping[str, NodeOccurrenceState],
    session: SessionState,
) -> None:
    if not isinstance(waits, Mapping):
        raise TypeError("Scheduler waits must be a mapping.")
    active_occurrence_ids: set[str] = set()
    for key, wait in waits.items():
        _non_empty_string_value(key, "Wait key")
        if not isinstance(wait, WaitState):
            raise TypeError("Scheduler waits must contain WaitState.")
        if key != wait.id:
            raise ValueError("Wait key must equal its id.")
        _non_empty_string_value(wait.id, "Wait id")
        _non_empty_string_value(wait.occurrence_id, "Wait occurrence_id")
        occurrence = occurrences.get(wait.occurrence_id)
        if occurrence is None:
            raise ValueError("Wait references an unknown Node Occurrence.")
        _non_negative(wait.created_at_us, "Wait created_at_us")
        _optional_non_negative(wait.resumed_at_us, "Wait resumed_at_us")
        _timestamp_within_session(wait.created_at_us, session, "Wait created_at_us")
        _optional_timestamp_within_session(
            wait.resumed_at_us,
            session,
            "Wait resumed_at_us",
        )
        if wait.status not in {"waiting", "resumed", "cancelled"}:
            raise ValueError(f"Unsupported Wait status {wait.status!r}.")
        if wait.status == "resumed":
            if wait.resumed_at_us is None:
                raise ValueError("A resumed Wait requires resumed_at_us.")
        elif wait.resumed_at_us is not None:
            raise ValueError("Only a resumed Wait may carry resumed_at_us.")
        if wait.status in {"waiting", "cancelled"} and wait.response is not None:
            raise ValueError("A non-resumed Wait cannot carry a response.")
        if wait.status == "waiting":
            if occurrence.status != "waiting":
                raise ValueError("A waiting Wait requires a waiting Node Occurrence.")
            if wait.occurrence_id in active_occurrence_ids:
                raise ValueError(
                    "A Node Occurrence cannot own multiple waiting Waits."
                )
            active_occurrence_ids.add(wait.occurrence_id)
        elif wait.status == "cancelled":
            if occurrence.status != "cancelled":
                raise ValueError(
                    "A cancelled Wait requires a cancelled Node Occurrence."
                )
        elif occurrence.status == "skipped":
            raise ValueError("A resumed Wait cannot belong to a skipped Occurrence.")


def _validate_child_plans(
    plans: Mapping[str, ChildInvocationPlan],
    occurrences: Mapping[str, NodeOccurrenceState] | None,
) -> None:
    if not isinstance(plans, Mapping):
        raise TypeError("Invocation child_plans must be a mapping.")
    session_ids: set[str] = set()
    invocation_ids: set[str] = set()
    for key, plan in plans.items():
        _non_empty_string_value(key, "Child Invocation Plan key")
        if not isinstance(plan, ChildInvocationPlan):
            raise TypeError("child_plans must contain ChildInvocationPlan.")
        if key != plan.creation_id:
            raise ValueError("Child Invocation Plan key must equal creation_id.")
        for value, label in (
            (plan.creation_id, "Child Invocation Plan creation_id"),
            (plan.parent_occurrence_id, "Child Invocation Plan parent_occurrence_id"),
            (plan.workflow_id, "Child Invocation Plan workflow_id"),
            (plan.workflow_revision_id, "Child Invocation Plan workflow_revision_id"),
        ):
            _non_empty_string_value(value, label)
        if occurrences is not None and plan.parent_occurrence_id not in occurrences:
            raise ValueError(
                "Child Invocation Plan references an unknown parent Node Occurrence."
            )
        if plan.mode not in {"await", "spawn"}:
            raise ValueError(f"Unsupported Child Invocation mode {plan.mode!r}.")
        if not isinstance(plan.units, (tuple, ChunkedUnits)) or not plan.units:
            raise ValueError("Child Invocation Plan units must be a non-empty tuple.")
        for expected_index, unit in enumerate(plan.units):
            if not isinstance(unit, ChildUnitState):
                raise TypeError("Child Invocation Plan units must be ChildUnitState.")
            if type(unit.input_released) is not bool or (unit.input_released and unit.input is not None):
                raise ValueError("Released Child input must be absent.")
            if unit.input_released and occurrences is not None:
                occurrence = occurrences[plan.parent_occurrence_id]
                if occurrence.status not in {"completed", "cancelled", "failed"}:
                    raise ValueError("Child input is still needed by its Parent Node.")
            _non_negative(unit.unit_index, "Child Invocation unit_index")
            if unit.unit_index != expected_index:
                raise ValueError(
                    "Child Invocation Plan unit indexes must be ordered from zero."
                )
            _non_empty_string_value(unit.session_id, "Child Invocation session_id")
            _non_empty_string_value(
                unit.invocation_id, "Child Invocation invocation_id"
            )
            if unit.session_id in session_ids or unit.invocation_id in invocation_ids:
                raise ValueError("Child Invocation identities must be unique.")
            session_ids.add(unit.session_id)
            invocation_ids.add(unit.invocation_id)
            if unit.phase not in {"planned", "opened", "accepted", "terminal", "abandoned"}:
                raise ValueError(f"Unsupported Child Invocation phase {unit.phase!r}.")


def _validate_activation(
    activation: Activation,
    occurrences: Mapping[str, NodeOccurrenceState],
    *,
    expected_edge_id: str | None = None,
    expected_target_node_id: str | None = None,
) -> None:
    if not isinstance(activation, Activation):
        raise TypeError("Activation must be an Activation instance.")
    for value, label in (
        (activation.edge_id, "Activation edge_id"),
        (activation.source_occurrence_id, "Activation source_occurrence_id"),
        (activation.target_node_id, "Activation target_node_id"),
    ):
        _non_empty_string_value(value, label)
    source = occurrences.get(activation.source_occurrence_id)
    if source is None:
        raise ValueError("Activation references an unknown source Node Occurrence.")
    if source.status not in {"completed", "failed"}:
        raise ValueError(
            "Activation source Node Occurrence must be completed or failed."
        )
    if expected_edge_id is not None and activation.edge_id != expected_edge_id:
        raise ValueError("Activation edge_id does not match its Resolution.")
    if (
        expected_target_node_id is not None
        and activation.target_node_id != expected_target_node_id
    ):
        raise ValueError("Activation target_node_id does not match its target.")


def _validate_invocation_lifecycle(invocation: InvocationState) -> None:
    if not invocation.stopping and any(
        u.phase == "abandoned" for p in invocation.child_plans.values() for u in p.units
    ):
        raise ValueError("Only a failed or cancelled Parent may abandon a Child plan.")
    if invocation.status == "settling":
        if invocation.pending_outcome not in {"completed", "failed", "cancelled"}:
            raise ValueError("Settling requires a pending outcome.")
    elif invocation.pending_outcome is not None:
        raise ValueError("Only settling may carry a pending outcome.")
    outcome = invocation.pending_outcome if invocation.status == "settling" else invocation.status
    terminal = invocation.status in {"completed", "failed", "cancelled"}
    if terminal != (invocation.completed_at_us is not None):
        raise ValueError(
            "Invocation completed_at_us must exist exactly for terminal status."
        )
    if terminal and any(
        u.phase not in {'terminal', 'abandoned'} for plan in invocation.child_plans.values() for u in plan.units
    ):
        raise ValueError("Terminal Invocation requires settled Child ownership.")
    if invocation.status == "settling" and (
        invocation.scheduler.ready or any(o.status in {"ready", "running", "waiting"}
        for o in invocation.scheduler.occurrences.values())
    ):
        raise ValueError("Settling Invocation cannot retain unfinished body work.")
    if invocation.status == "created":
        if invocation.started_at_us is not None:
            raise ValueError("A created Invocation cannot have started_at_us.")
    elif invocation.status in {"running", "waiting", "settling", "completed"}:
        if invocation.started_at_us is None:
            raise ValueError(
                f"A {invocation.status} Invocation requires started_at_us."
            )
    if outcome == "failed":
        if invocation.error is None:
            raise ValueError("A failed Invocation requires error details.")
    elif invocation.error is not None:
        raise ValueError("Only a failed Invocation may carry error details.")
    if outcome != "cancelled" and invocation.cancel_reason is not None:
        raise ValueError("Only a cancelled Invocation may carry cancel_reason.")


def _validate_occurrence_lifecycle(occurrence: NodeOccurrenceState) -> None:
    if (occurrence.started_at_us is None) != (
        occurrence.started_sequence is None
    ):
        raise ValueError(
            "Node Occurrence start time and State version must appear together."
        )
    if occurrence.status in {"running", "waiting", "completed", "failed"}:
        if occurrence.started_at_us is None:
            raise ValueError(
                f"A {occurrence.status} Node Occurrence requires start information."
            )
    terminal = occurrence.status in {
        "completed",
        "failed",
        "skipped",
        "cancelled",
    }
    if terminal != (occurrence.completed_at_us is not None):
        raise ValueError(
            "Node Occurrence completed_at_us must exist exactly for terminal status."
        )
    if occurrence.status == "failed":
        if occurrence.error is None:
            raise ValueError("A failed Node Occurrence requires error details.")
    elif occurrence.error is not None:
        raise ValueError("Only a failed Node Occurrence may carry error details.")


def _timestamp_within_session(value: int, session: SessionState, label: str) -> None:
    # Wall time may regress; only Event sequence establishes execution order.
    _non_negative(value, label)


def _optional_timestamp_within_session(
    value: int | None,
    session: SessionState,
    label: str,
) -> None:
    if value is not None:
        _timestamp_within_session(value, session, label)


def _validate_scope(scope: ExecutionScope, label: str) -> None:
    if not isinstance(scope, tuple):
        raise TypeError(f"{label} must be a tuple.")
    for frame in scope:
        if not isinstance(frame, LoopIteration):
            raise TypeError(f"{label} must contain LoopIteration values.")
        _non_empty_string_value(frame.loop_region_id, f"{label} loop_region_id")
        _non_negative(frame.iteration, f"{label} iteration")


def _validate_context_revision_map(
    revisions: Mapping[tuple[str, ...], int],
    sequence: int,
    label: str,
) -> None:
    if not isinstance(revisions, Mapping):
        raise TypeError(f"{label} must be a mapping.")
    for path, revision in revisions.items():
        if not isinstance(path, tuple) or not path or any(
            not isinstance(token, str) or not token.strip() for token in path
        ):
            raise ValueError(f"{label} paths must be non-empty tuples of strings.")
        _non_negative(revision, f"{label} value")
        if revision > sequence:
            raise ValueError(f"{label} value exceeds Runtime State version.")


def _non_negative(value: object, label: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{label} must be an integer.")
    if value < 0:
        raise ValueError(f"{label} must be non-negative.")


def _optional_non_negative(value: object | None, label: str) -> None:
    if value is not None:
        _non_negative(value, label)


def _non_empty_string_value(value: object, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{label} must be a non-empty string.")


def _optional_non_empty_string(value: object | None, label: str) -> None:
    if value is not None:
        _non_empty_string_value(value, label)


def _session_record(value: SessionState | None) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value.context, Mapping):
        raise TypeError("Session Context must be a mapping.")
    return {
        "id": value.id,
        "context": thaw(value.context),
        "created_at_us": value.created_at_us,
        "updated_at_us": value.updated_at_us,
        "latest_invocation_id": value.latest_invocation_id,
        "context_path_revisions": {
            _context_path_key(path): revision
            for path, revision in value.context_path_revisions.items()
        },
    }


def _child_plans_record(plans):
    return {
        creation_id: {
            "creation_id": plan.creation_id,
            "parent_occurrence_id": plan.parent_occurrence_id,
            "mode": plan.mode,
            "workflow_id": plan.workflow_id,
            "workflow_revision_id": plan.workflow_revision_id,
            "units": [
                {
                    "unit_index": unit.unit_index,
                    "session_id": unit.session_id,
                    "invocation_id": unit.invocation_id,
                    "input": thaw(unit.input),
                    "phase": unit.phase,
                    "input_released": unit.input_released,
                }
                for unit in plan.units
            ],
        }
        for creation_id, plan in plans.items()
    }

def _child_result_record(value):
    return {
        "kind": "child_result", "id": value.id,
        "workflow_id": value.workflow_id, "workflow_revision_id": value.workflow_revision_id,
        "entry_node_id": value.entry_node_id, "status": value.status,
        "output": thaw(value.output), "error": _error_record(value.error),
        "cancel_reason": value.cancel_reason,
        "created_at_us": value.created_at_us, "started_at_us": value.started_at_us,
        "completed_at_us": value.completed_at_us,
        "parent_session_id": value.parent_session_id, "parent_invocation_id": value.parent_invocation_id,
        "creation_id": value.creation_id, "unit_index": value.unit_index,
        "input_digest": value.input_digest, "child_plans": _child_plans_record(value.child_plans),
    }


def _invocation_record(value: InvocationState | ChildResult | None) -> dict[str, object] | None:
    if isinstance(value, ChildResult):
        return _child_result_record(value)
    if value is None:
        return None
    if not isinstance(value.context, Mapping):
        raise TypeError("Invocation Context must be a mapping.")
    return {
        "id": value.id,
        "workflow_id": value.workflow_id,
        "workflow_revision_id": value.workflow_revision_id,
        "entry_node_id": value.entry_node_id,
        "status": value.status,
        "input": thaw(value.input),
        "context": thaw(value.context),
        "output": thaw(value.output),
        "error": _error_record(value.error),
        "cancel_reason": value.cancel_reason,
        "pending_outcome": value.pending_outcome,
        "created_at_us": value.created_at_us,
        "started_at_us": value.started_at_us,
        "completed_at_us": value.completed_at_us,
        "context_path_revisions": {
            _context_path_key(path): revision
            for path, revision in value.context_path_revisions.items()
        },
        "child_plans": _child_plans_record(value.child_plans),
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
                    "queue_duration_ns": item.queue_duration_ns,
                    "execution_duration_ns": item.execution_duration_ns,
                    "started_at_us": item.started_at_us,
                    "completed_at_us": item.completed_at_us,
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
                    "created_at_us": item.created_at_us,
                    "resumed_at_us": item.resumed_at_us,
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
        "started_at_us": value.started_at_us,
        "completed_at_us": value.completed_at_us,
        "started_sequence": value.started_sequence,
        "ready_at_us": value.ready_at_us,
        "execution": _execution_record(value.execution),
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
        context=freeze(context),
        created_at_us=_integer(record, "created_at_us"),
        updated_at_us=_integer(record, "updated_at_us"),
        latest_invocation_id=_optional_string(record, "latest_invocation_id"),
        context_path_revisions=MappingProxyType(
            _context_revisions(record.get("context_path_revisions"))
        ),
    )


def _invocation_from_record(value: object) -> InvocationState | ChildResult | None:
    if value is None:
        return None
    record = _mapping(value, "Invocation State")
    if record.get("kind") == "child_result":
        return ChildResult(
            id=_string(record, "id"), workflow_id=_string(record, "workflow_id"),
            workflow_revision_id=_string(record, "workflow_revision_id"),
            entry_node_id=_string(record, "entry_node_id"), status=_string(record, "status"),
            output=freeze(record.get("output")), error=_error_from_record(record.get("error")),
            cancel_reason=_optional_string(record, "cancel_reason"),
            created_at_us=_integer(record, "created_at_us"), started_at_us=_optional_integer(record, "started_at_us"),
            completed_at_us=_integer(record, "completed_at_us"),
            parent_session_id=_string(record, "parent_session_id"), parent_invocation_id=_string(record, "parent_invocation_id"),
            creation_id=_string(record, "creation_id"), unit_index=_integer(record, "unit_index"),
            input_digest=_string(record, "input_digest"),
            child_plans=runtime_mapping(_child_plans_from_record(_mapping(record.get("child_plans"), "Child plans"))),
        )
    context = record.get("context")
    if not isinstance(context, dict):
        raise TypeError("Invocation Context must be a mapping.")
    child_plans_record = _mapping(record.get("child_plans"), "Child Invocation Plans")
    scheduler = _scheduler_from_record(record.get("scheduler"))
    status = _string(record, "status")
    if status not in {"created", "running", "waiting", "settling", "completed", "failed", "cancelled"}:
        raise ValueError(f"Unsupported Invocation status {status!r}.")
    return InvocationState(
        id=_string(record, "id"),
        workflow_id=_string(record, "workflow_id"),
        workflow_revision_id=_string(record, "workflow_revision_id"),
        entry_node_id=_string(record, "entry_node_id"),
        status=status,  # type: ignore[arg-type]
        input=freeze(record.get("input")),
        context=freeze(context),
        output=freeze(record.get("output")),
        error=_error_from_record(record.get("error")),
        cancel_reason=_optional_string(record, "cancel_reason"),
        pending_outcome=_optional_string(record, "pending_outcome"),
        created_at_us=_integer(record, "created_at_us"),
        started_at_us=_optional_integer(record, "started_at_us"),
        completed_at_us=_optional_integer(record, "completed_at_us"),
        scheduler=scheduler,
        child_plans=runtime_mapping(
            _child_plans_from_record(child_plans_record)
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
        occurrences=runtime_mapping(
            {key: _occurrence_from_record(item) for key, item in occurrences.items()}
        ),
        resolutions=runtime_mapping(
            {key: _resolution_from_record(item) for key, item in resolutions.items()}
        ),
        boundary_resolutions=runtime_mapping(
            {key: _boundary_from_record(item) for key, item in boundaries.items()}
        ),
        operator_calls=runtime_mapping(
            {key: _call_from_record(item) for key, item in calls.items()}
        ),
        waits=runtime_mapping(
            {key: _wait_from_record(item) for key, item in waits.items()}
        ),
    )


def _occurrence_from_record(value: object) -> NodeOccurrenceState:
    record = _mapping(value, "Node Occurrence")
    status = _string(record, "status")
    if status not in {
        "ready",
        "running",
        "waiting",
        "completed",
        "failed",
        "skipped",
        "cancelled",
    }:
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
        started_at_us=_optional_integer(record, "started_at_us"),
        completed_at_us=_optional_integer(record, "completed_at_us"),
        started_sequence=_optional_integer(record, "started_sequence"),
        ready_at_us=_optional_integer(record, "ready_at_us"),
        execution=_execution_from_record(record["execution"]),
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
    return OperatorCallState(
        id=_string(record, "id"),
        occurrence_id=_string(record, "occurrence_id"),
        operator_id=_string(record, "operator_id"),
        unit_index=_integer(record, "unit_index"),
        status=status,  # type: ignore[arg-type]
        input=freeze(record.get("input")),
        output=freeze(record.get("output")),
        error=_error_from_record(record.get("error")),
        queue_duration_ns=_integer(record, "queue_duration_ns"),
        execution_duration_ns=_optional_integer(record, "execution_duration_ns"),
        started_at_us=_integer(record, "started_at_us"),
        completed_at_us=_optional_integer(record, "completed_at_us"),
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
        created_at_us=_integer(record, "created_at_us"),
        resumed_at_us=_optional_integer(record, "resumed_at_us"),
    )


def _child_plans_from_record(
    record: dict[str, object],
) -> dict[str, ChildInvocationPlan]:
    plans: dict[str, ChildInvocationPlan] = {}
    session_ids: set[str] = set()
    invocation_ids: set[str] = set()
    for creation_id, value in record.items():
        plan = _child_plan_from_record(value)
        if creation_id != plan.creation_id:
            raise ValueError("Child Invocation Plan key must equal creation_id.")
        for unit in plan.units:
            if unit.session_id in session_ids or unit.invocation_id in invocation_ids:
                raise ValueError("Child Invocation identities must be unique.")
            session_ids.add(unit.session_id)
            invocation_ids.add(unit.invocation_id)
        plans[creation_id] = plan
    return plans


def _child_plan_from_record(value: object) -> ChildInvocationPlan:
    record = _mapping(value, "Child Invocation Plan")
    mode = _string(record, "mode")
    if mode not in {"await", "spawn"}:
        raise ValueError(f"Unsupported Child Invocation mode {mode!r}.")
    units_record = record.get("units")
    if not isinstance(units_record, list) or not units_record:
        raise TypeError("Child Invocation Plan units must be a non-empty list.")
    units: list[ChildUnitState] = []
    for item in units_record:
        unit = _child_unit_from_record(item)
        if unit.unit_index != len(units):
            raise ValueError(
                "Child Invocation Plan unit indexes must be ordered from zero."
            )
        units.append(unit)
    return ChildInvocationPlan(
        creation_id=_string(record, "creation_id"),
        parent_occurrence_id=_string(record, "parent_occurrence_id"),
        mode=mode,  # type: ignore[arg-type]
        workflow_id=_string(record, "workflow_id"),
        workflow_revision_id=_string(record, "workflow_revision_id"),
        units=child_units(tuple(units)),
    )


def _child_unit_from_record(value: object) -> ChildUnitState:
    record = _mapping(value, "Child Invocation Unit")
    phase = _string(record, "phase")
    if phase not in {"planned", "opened", "accepted", "terminal", "abandoned"}:
        raise ValueError(f"Unsupported Child Invocation phase {phase!r}.")
    return ChildUnitState(
        unit_index=_integer(record, "unit_index"),
        session_id=_string(record, "session_id"),
        invocation_id=_string(record, "invocation_id"),
        input=freeze(record.get("input")),
        phase=phase,  # type: ignore[arg-type]
        input_released=record.get("input_released", False),
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
    return RuntimeErrorInfo(
        type=_string(record, "type"),
        message=_string(record, "message"),
        code=_optional_string(record, "code"),
        phase=_optional_string(record, "phase"),
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
        if revision < 0:
            raise ValueError("Context revision value must be non-negative.")
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
    if value < 0:
        raise ValueError(f"{key} must be non-negative.")
    return value


def _optional_integer(record: dict[str, object], key: str) -> int | None:
    value = record.get(key)
    if value is not None and (
        not isinstance(value, int) or isinstance(value, bool)
    ):
        raise TypeError(f"{key} must be an integer or None.")
    if value is not None and value < 0:
        raise ValueError(f"{key} must be non-negative or None.")
    return value


def _boolean(record: dict[str, object], key: str) -> bool:
    value = record.get(key)
    if type(value) is not bool:
        raise TypeError(f"{key} must be bool.")
    return value


def _execution_record(value: NodeExecutionState) -> dict[str, object]:
    return {
        "phase": value.phase, "completed_stages": list(value.completed_stages), "mapped_input": thaw(value.mapped_input),
        "resolved_capability_id": value.resolved_capability_id,
        "resolved_operator_id": value.resolved_operator_id,
        "aggregate_output": thaw(value.aggregate_output),
        "pending_context_patch": _patch_to_record(value.pending_context_patch),
        "routing": [{"edge_id": r.edge_id, "selected": r.selected, "duration_ns": r.duration_ns} for r in value.routing],
        "routing_source_status": value.routing_source_status,
        "fault": _error_record(value.fault),
    }


def _execution_from_record(record: object) -> NodeExecutionState:
    if not isinstance(record, dict):
        raise TypeError("Node execution must be a mapping.")
    return NodeExecutionState(
        phase=_string(record, "phase"), completed_stages=tuple(record["completed_stages"]), mapped_input=freeze(record.get("mapped_input")),
        resolved_capability_id=_optional_string(record, "resolved_capability_id"),
        resolved_operator_id=_optional_string(record, "resolved_operator_id"),
        aggregate_output=freeze(record.get("aggregate_output")),
        pending_context_patch=_patch_from_record(record.get("pending_context_patch")),
        routing=tuple(EdgeConditionResult(**item) for item in record["routing"]),
        routing_source_status=_optional_string(record, "routing_source_status"),
        fault=_error_from_record(record.get("fault")),
    )


def _validate_execution(execution: NodeExecutionState) -> None:
    if not isinstance(execution, NodeExecutionState):
        raise TypeError("Occurrence execution must be NodeExecutionState.")
    stages = {"input_mapped", "capability_resolved", "aggregated", "output_bound", "routing_resolved"}
    if execution.phase not in stages | {"none", "started", "executing", "faulted"}:
        raise ValueError("Unknown execution phase.")
    if not isinstance(execution.completed_stages, tuple) or not all(stage in stages for stage in execution.completed_stages):
        raise ValueError("Unknown completed execution stage.")
    if len(set(execution.completed_stages)) != len(execution.completed_stages):
        raise ValueError("Completed execution stages cannot repeat.")
    for value in (execution.resolved_capability_id, execution.resolved_operator_id):
        _optional_non_empty_string(value, "Resolved execution identity")
    if "capability_resolved" in execution.completed_stages and (
        execution.resolved_capability_id is None or execution.resolved_operator_id is None
    ):
        raise ValueError("Resolved capability requires durable identities.")
    if not isinstance(execution.pending_context_patch, ContextPatch):
        raise TypeError("Execution pending patch must be ContextPatch.")
    if not isinstance(execution.routing, tuple) or not all(isinstance(item, EdgeConditionResult) for item in execution.routing):
        raise TypeError("Execution routing must contain condition results.")
    if "routing_resolved" in execution.completed_stages and execution.routing_source_status not in {"complete", "error"}:
        raise ValueError("Resolved routing must retain its source status.")
    if execution.fault is not None and not isinstance(execution.fault, RuntimeErrorInfo):
        raise TypeError("Execution fault must be RuntimeErrorInfo.")
    if execution.phase == "faulted" and execution.fault is None:
        raise ValueError("Faulted execution requires its durable error.")
