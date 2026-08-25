"""Small public values returned by the Core application facade."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, TypeAlias

from ..runtime import (
    RuntimeCheckpointBundle,
    RuntimeErrorInfo,
    RuntimeState,
    TraceEvent,
    UserEvent,
)


InvocationStatus = Literal[
    "created", "running", "waiting", "completed", "failed", "cancelled"
]


@dataclass(frozen=True, slots=True)
class InvocationRef:
    """Exact identity required by every control operation."""

    session_id: str
    invocation_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValueError("InvocationRef session_id cannot be empty.")
        if not isinstance(self.invocation_id, str) or not self.invocation_id.strip():
            raise ValueError("InvocationRef invocation_id cannot be empty.")


@dataclass(frozen=True, slots=True)
class InvocationWait:
    id: str
    request: object


@dataclass(frozen=True, slots=True)
class InvocationSubmission:
    """A reliably admitted background Invocation and its recovery boundary."""

    ref: InvocationRef
    checkpoint: RuntimeCheckpointBundle
    trace_events: tuple[TraceEvent, ...] = ()
    user_events: tuple[UserEvent, ...] = ()

    @property
    def status(self) -> Literal["running"]:
        return "running"

    @property
    def session_id(self) -> str:
        return self.ref.session_id

    @property
    def invocation_id(self) -> str:
        return self.ref.invocation_id


@dataclass(frozen=True, slots=True)
class InvocationResult:
    """One stable execution boundary with observations and a fresh checkpoint."""

    ref: InvocationRef
    status: InvocationStatus
    checkpoint: RuntimeCheckpointBundle
    output: object = None
    error: RuntimeErrorInfo | None = None
    waits: tuple[InvocationWait, ...] = ()
    trace_events: tuple[TraceEvent, ...] = ()
    user_events: tuple[UserEvent, ...] = ()

    @property
    def session_id(self) -> str:
        return self.ref.session_id

    @property
    def invocation_id(self) -> str:
        return self.ref.invocation_id


@dataclass(frozen=True, slots=True)
class InvocationUpdate:
    """One backpressured observation and an optional safe recovery boundary."""

    event: TraceEvent | UserEvent
    checkpoint: RuntimeCheckpointBundle | None = None


@dataclass(frozen=True, slots=True)
class AppCheckpoint:
    """Clean-shutdown checkpoints for every independent in-memory root."""

    roots: tuple[RuntimeCheckpointBundle, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.roots, tuple) or not all(
            isinstance(item, RuntimeCheckpointBundle) for item in self.roots
        ):
            raise TypeError("AppCheckpoint roots must be RuntimeCheckpointBundle values.")
        root_ids = tuple(item.root_session_id for item in self.roots)
        if len(root_ids) != len(set(root_ids)):
            raise ValueError("AppCheckpoint Root Session ids must be unique.")
        state_ids: set[str] = set()
        for bundle in self.roots:
            overlap = state_ids.intersection(bundle.states)
            if overlap:
                raise ValueError("AppCheckpoint Runtime graphs cannot overlap.")
            state_ids.update(bundle.states)
        _validate_graph_claims(
            tuple((bundle.root_session_id, bundle.states) for bundle in self.roots)
        )

    def to_record(self) -> dict[str, object]:
        return {"roots": [item.to_record() for item in self.roots]}

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "AppCheckpoint":
        if not isinstance(record, dict) or set(record) != {"roots"}:
            raise TypeError("AppCheckpoint record must contain only roots.")
        roots = record.get("roots")
        if not isinstance(roots, list) or not all(isinstance(item, dict) for item in roots):
            raise TypeError("AppCheckpoint roots must be a list of mappings.")
        checkpoint = cls(
            tuple(RuntimeCheckpointBundle.from_record(item) for item in roots)
        )
        if checkpoint.to_record() != record:
            raise TypeError("AppCheckpoint record is not canonical.")
        return checkpoint


@dataclass(frozen=True, slots=True)
class CheckpointLoadResult:
    """Exact current Invocations installed by one atomic load."""

    roots: tuple[InvocationRef, ...]
    invocations: tuple[InvocationRef, ...]


StreamItem: TypeAlias = InvocationUpdate | InvocationResult


def _validate_graph_claims(
    graphs: tuple[tuple[str, Mapping[str, RuntimeState]], ...],
) -> None:
    """Ensure every present or planned Child Session has exactly one Root owner."""

    state_owners: dict[str, str] = {}
    states: dict[str, RuntimeState] = {}
    claims: dict[
        str,
        tuple[str, str, str, int, str, str, str],
    ] = {}
    terminal_claims: set[str] = set()

    for root_session_id, graph_states in graphs:
        for session_id, state in graph_states.items():
            owner = state_owners.get(session_id)
            if owner is not None and owner != root_session_id:
                raise ValueError(
                    "Runtime State cannot belong to multiple AppCheckpoint Roots."
                )
            state_owners[session_id] = root_session_id
            states.setdefault(session_id, state)

        for parent_session_id, state in graph_states.items():
            invocation = state.invocation
            if invocation is None:
                continue
            for creation_id, plan in invocation.child_plans.items():
                for unit in plan.units:
                    descriptor = (
                        root_session_id,
                        parent_session_id,
                        creation_id,
                        unit.unit_index,
                        unit.invocation_id,
                        plan.workflow_id,
                        plan.workflow_revision_id,
                    )
                    previous = claims.get(unit.session_id)
                    if previous is not None and previous != descriptor:
                        raise ValueError(
                            "Child Session is claimed by multiple Runtime parents."
                        )
                    claims[unit.session_id] = descriptor
                    if unit.phase == "terminal":
                        terminal_claims.add(unit.session_id)

    for child_session_id, descriptor in claims.items():
        (
            expected_root,
            _parent_session_id,
            _creation_id,
            _unit_index,
            expected_invocation_id,
            expected_workflow_id,
            expected_revision_id,
        ) = descriptor
        child_state = states.get(child_session_id)
        if child_state is None:
            continue
        if state_owners[child_session_id] != expected_root:
            raise ValueError(
                "A Child Session cannot also be an independent Checkpoint Root."
            )
        child = child_state.invocation
        if child is None or (
            child.id != expected_invocation_id
            or child.workflow_id != expected_workflow_id
            or child.workflow_revision_id != expected_revision_id
        ):
            raise ValueError("Child Runtime State does not match its owning plan.")
        if child_session_id in terminal_claims and not child.terminal:
            raise ValueError("A terminal Child plan requires terminal Runtime State.")


__all__ = [
    "AppCheckpoint",
    "CheckpointLoadResult",
    "InvocationRef",
    "InvocationResult",
    "InvocationStatus",
    "InvocationSubmission",
    "InvocationUpdate",
    "InvocationWait",
    "StreamItem",
]
