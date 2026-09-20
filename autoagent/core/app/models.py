"""Small public values returned by the Core application facade."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

from ..runtime import (
    RuntimeErrorInfo,
    UserEvent,
)
from ..workflow.models import InvocationRef
from ..runtime.graph_checkpoint import RuntimeGraphCheckpoint


InvocationStatus = Literal[
    "created", "running", "waiting", "joining_children", "completed", "failed", "cancelled"
]


@dataclass(frozen=True, slots=True)
class InvocationWait:
    id: str
    request: object


@dataclass(frozen=True, slots=True)
class InvocationSubmission:
    """A reliably admitted background Invocation."""

    ref: InvocationRef

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
    """One stable execution boundary."""

    ref: InvocationRef
    status: InvocationStatus
    output: object = None
    error: RuntimeErrorInfo | None = None
    waits: tuple[InvocationWait, ...] = ()

    @property
    def session_id(self) -> str:
        return self.ref.session_id

    @property
    def invocation_id(self) -> str:
        return self.ref.invocation_id


@dataclass(frozen=True, slots=True)
class InvocationUpdate:
    """One backpressured User Event from the exact streamed Invocation."""

    event: UserEvent


@dataclass(frozen=True, slots=True)
class AppCheckpoint:
    """Complete Root graphs captured at clean process shutdown."""
    graphs: tuple[RuntimeGraphCheckpoint, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.graphs, tuple) or not all(isinstance(g, RuntimeGraphCheckpoint) for g in self.graphs):
            raise TypeError("AppCheckpoint graphs must be RuntimeGraphCheckpoint values.")
        ids = [s.session_id for g in self.graphs for s in g.sessions]
        claims = [u.session_id for g in self.graphs for s in g.sessions if s.state.invocation is not None
                  for p in s.state.invocation.child_plans.values() for u in p.units]
        if len(ids) != len(set(ids)) or len(claims) != len(set(claims)):
            raise ValueError("AppCheckpoint contains overlapping graphs.")
        roots = {g.root_session_id for g in self.graphs}
        if roots.intersection(claims):
            raise ValueError("A bundled Root cannot be owned by another Graph.")

    def to_record(self) -> dict[str, object]:
        return {"schema_version": 1, "graphs": [g.to_record() for g in self.graphs]}

    @classmethod
    def from_record(cls, record: dict[str, object]) -> AppCheckpoint:
        if not isinstance(record, dict) or set(record) != {"schema_version", "graphs"}:
            raise TypeError("Invalid AppCheckpoint schema.")
        if type(record['schema_version']) is not int or record['schema_version'] != 1:
            raise ValueError("Unsupported AppCheckpoint version.")
        if not isinstance(record['graphs'], list):
            raise TypeError("AppCheckpoint graphs must be a list.")
        return cls(tuple(RuntimeGraphCheckpoint.from_record(g) for g in record['graphs']))


@dataclass(frozen=True, slots=True)
class CheckpointLoadResult:
    """Exact current Invocations installed by one atomic load."""

    invocations: tuple[InvocationRef, ...]


StreamItem: TypeAlias = InvocationUpdate | InvocationResult


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
