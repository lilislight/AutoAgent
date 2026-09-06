"""Small public values returned by the Core application facade."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

from ..runtime import (
    SessionCheckpoint,
    RuntimeErrorInfo,
    UserEvent,
)
from ..workflow.models import InvocationRef


InvocationStatus = Literal[
    "created", "running", "waiting", "completed", "failed", "cancelled"
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
    """Clean-shutdown checkpoints for every in-memory Runtime Session."""

    sessions: tuple[SessionCheckpoint, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.sessions, tuple) or not all(
            isinstance(item, SessionCheckpoint) for item in self.sessions
        ):
            raise TypeError("AppCheckpoint sessions must be SessionCheckpoint values.")
        session_ids = tuple(item.session_id for item in self.sessions)
        if len(session_ids) != len(set(session_ids)):
            raise ValueError("AppCheckpoint Session ids must be unique.")

    def to_record(self) -> dict[str, object]:
        return {"sessions": [item.to_record() for item in self.sessions]}

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "AppCheckpoint":
        if not isinstance(record, dict) or set(record) != {"sessions"}:
            raise TypeError("AppCheckpoint record must contain only sessions.")
        sessions = record.get("sessions")
        if not isinstance(sessions, list) or not all(
            isinstance(item, dict) for item in sessions
        ):
            raise TypeError("AppCheckpoint sessions must be a list of mappings.")
        return cls(
            tuple(SessionCheckpoint.from_record(item) for item in sessions)
        )


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
