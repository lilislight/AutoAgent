"""Immutable Event contracts emitted by V2 Core."""

from __future__ import annotations

import copy
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, TypeAlias
from uuid import UUID, uuid4

from .serialization import json_value


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class EventMode(StrEnum):
    MINIMAL = "minimal"
    STANDARD = "standard"
    FULL = "full"


@dataclass(frozen=True, slots=True)
class StateOperation:
    op: Literal["add", "replace", "remove"]
    path: tuple[str | int, ...]
    value: Any = None

    def to_record(self) -> dict[str, Any]:
        return {"op": self.op, "path": list(self.path), "value": json_value(self.value)}

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "StateOperation":
        return cls(value["op"], tuple(value["path"]), value.get("value"))


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    workflow_id: str
    workflow_revision_id: str
    session_id: str
    invocation_id: UUID
    sequence: int
    event_name: str
    subject_type: str
    subject_id: str
    status: str | None = None
    workflow_path: tuple[str, ...] = ()
    duration_ns: int | None = None
    payload: Any = None
    operations: tuple[StateOperation, ...] = ()
    occurred_at_ms: int = field(default_factory=now_ms)
    id: UUID = field(default_factory=uuid4)

    @classmethod
    def detached(cls, **values: Any) -> "RuntimeEvent":
        return cls(**copy.deepcopy(values))

    def to_record(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "workflow_id": self.workflow_id,
            "workflow_revision_id": self.workflow_revision_id,
            "session_id": self.session_id,
            "invocation_id": str(self.invocation_id),
            "sequence": self.sequence,
            "event_name": self.event_name,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "status": self.status,
            "workflow_path": list(self.workflow_path),
            "duration_ns": self.duration_ns,
            "payload": json_value(self.payload),
            "operations": [item.to_record() for item in self.operations],
            "occurred_at_ms": self.occurred_at_ms,
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "RuntimeEvent":
        return cls(
            id=UUID(str(value["id"])),
            workflow_id=str(value["workflow_id"]),
            workflow_revision_id=str(value["workflow_revision_id"]),
            session_id=str(value["session_id"]),
            invocation_id=UUID(str(value["invocation_id"])),
            sequence=int(value["sequence"]),
            event_name=str(value["event_name"]),
            subject_type=str(value["subject_type"]),
            subject_id=str(value["subject_id"]),
            status=value.get("status"),
            workflow_path=tuple(str(item) for item in value.get("workflow_path", [])),
            duration_ns=(
                int(value["duration_ns"])
                if value.get("duration_ns") is not None
                else None
            ),
            payload=copy.deepcopy(value.get("payload")),
            operations=tuple(
                StateOperation.from_record(item) for item in value.get("operations", [])
            ),
            occurred_at_ms=int(value["occurred_at_ms"]),
        )


@dataclass(frozen=True, slots=True)
class UserEvent:
    workflow_id: str
    workflow_revision_id: str
    session_id: str
    invocation_id: UUID
    sequence: int
    type: str
    data: Any
    node_id: str
    workflow_path: tuple[str, ...] = ()
    occurred_at_ms: int = field(default_factory=now_ms)
    id: UUID = field(default_factory=uuid4)

    @classmethod
    def detached(cls, **values: Any) -> "UserEvent":
        return cls(**copy.deepcopy(values))

    def to_record(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "workflow_id": self.workflow_id,
            "workflow_revision_id": self.workflow_revision_id,
            "session_id": self.session_id,
            "invocation_id": str(self.invocation_id),
            "sequence": self.sequence,
            "type": self.type,
            "data": json_value(self.data),
            "node_id": self.node_id,
            "workflow_path": list(self.workflow_path),
            "occurred_at_ms": self.occurred_at_ms,
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "UserEvent":
        return cls(
            id=UUID(str(value["id"])),
            workflow_id=str(value["workflow_id"]),
            workflow_revision_id=str(value["workflow_revision_id"]),
            session_id=str(value["session_id"]),
            invocation_id=UUID(str(value["invocation_id"])),
            sequence=int(value["sequence"]),
            type=str(value["type"]),
            data=copy.deepcopy(value.get("data")),
            node_id=str(value["node_id"]),
            workflow_path=tuple(str(item) for item in value.get("workflow_path", [])),
            occurred_at_ms=int(value["occurred_at_ms"]),
        )


Event: TypeAlias = RuntimeEvent | UserEvent
