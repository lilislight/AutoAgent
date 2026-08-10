"""Immutable Event contracts emitted by V2 Core."""

from __future__ import annotations

import copy
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, TypeAlias
from uuid import UUID, uuid4

from .serialization import RuntimeValueCodec, decode_json_record, encode_json_record
from .state import StateOperation, StateOperationBatch


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class EventMode(StrEnum):
    MINIMAL = "minimal"
    STANDARD = "standard"
    FULL = "full"


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
    started_at_ms: int | None = None
    completed_at_ms: int | None = None
    payload: Any = None
    operation_batches: tuple[StateOperationBatch, ...] = ()
    occurred_at_ms: int = field(default_factory=now_ms)
    id: UUID = field(default_factory=uuid4)
    _persistent_payload: Any = field(default=None, repr=False, compare=False)

    @property
    def operations(self) -> tuple[StateOperation, ...]:
        """Flatten batches for consumers that do not need atomic boundaries."""

        return tuple(
            operation
            for batch in self.operation_batches
            for operation in batch.operations
        )

    @classmethod
    def detached(cls, **values: Any) -> "RuntimeEvent":
        detached = copy.deepcopy(
            {
                key: item
                for key, item in values.items()
                if key not in {"payload", "operation_batches"}
            }
        )
        captured = RuntimeValueCodec.capture(values.get("payload"))
        detached["payload"] = captured.transfer_to_runtime()
        detached["_persistent_payload"] = captured.persistent_value()
        detached["operation_batches"] = tuple(
            StateOperationBatch(
                state_version=batch.state_version,
                operations=tuple(
                    StateOperation.capture(operation)
                    for operation in batch.operations
                ),
            )
            for batch in values.get("operation_batches", ())
        )
        return cls(**detached)

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
            "started_at_ms": self.started_at_ms,
            "completed_at_ms": self.completed_at_ms,
            "payload": (
                self._persistent_payload
                if self._persistent_payload is not None
                else RuntimeValueCodec.encode(self.payload)
            ),
            "operation_batches": [
                item.to_record() for item in self.operation_batches
            ],
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
            started_at_ms=(
                int(value["started_at_ms"])
                if value.get("started_at_ms") is not None
                else None
            ),
            completed_at_ms=(
                int(value["completed_at_ms"])
                if value.get("completed_at_ms") is not None
                else None
            ),
            payload=RuntimeValueCodec.decode(value.get("payload")),
            operation_batches=tuple(
                StateOperationBatch.from_record(item)
                for item in value.get("operation_batches", [])
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
    _persistent_data: Any = field(default=None, repr=False, compare=False)

    @classmethod
    def detached(cls, **values: Any) -> "UserEvent":
        detached = copy.deepcopy({key: item for key, item in values.items() if key != "data"})
        captured = RuntimeValueCodec.capture(values.get("data"))
        detached["data"] = captured.transfer_to_runtime()
        detached["_persistent_data"] = captured.persistent_value()
        return cls(**detached)

    def to_record(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "workflow_id": self.workflow_id,
            "workflow_revision_id": self.workflow_revision_id,
            "session_id": self.session_id,
            "invocation_id": str(self.invocation_id),
            "sequence": self.sequence,
            "type": self.type,
            "data": (
                self._persistent_data
                if self._persistent_data is not None
                else RuntimeValueCodec.encode(self.data)
            ),
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
            data=RuntimeValueCodec.decode(value.get("data")),
            node_id=str(value["node_id"]),
            workflow_path=tuple(str(item) for item in value.get("workflow_path", [])),
            occurred_at_ms=int(value["occurred_at_ms"]),
        )


Event: TypeAlias = RuntimeEvent | UserEvent


@dataclass(frozen=True, slots=True)
class SerializedEvent:
    """Immutable Core-to-Sink ownership envelope."""

    id: UUID
    workflow_id: str
    workflow_revision_id: str
    session_id: str
    invocation_id: UUID
    channel: Literal["runtime", "user"]
    sequence: int
    occurred_at_ms: int
    event_type: str
    subject_type: str | None
    subject_id: str | None
    payload: bytes
    size_bytes: int

    @classmethod
    def from_event(cls, event: Event) -> "SerializedEvent":
        payload = encode_json_record(event.to_record())
        if isinstance(event, RuntimeEvent):
            channel: Literal["runtime", "user"] = "runtime"
            event_type = event.event_name
            subject_type = event.subject_type
            subject_id = event.subject_id
        else:
            channel = "user"
            event_type = event.type
            subject_type = "node"
            subject_id = event.node_id
        return cls(
            id=event.id,
            workflow_id=event.workflow_id,
            workflow_revision_id=event.workflow_revision_id,
            session_id=event.session_id,
            invocation_id=event.invocation_id,
            channel=channel,
            sequence=event.sequence,
            occurred_at_ms=event.occurred_at_ms,
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_id,
            payload=payload,
            size_bytes=len(payload),
        )

    def decode(self) -> Event:
        record = decode_json_record(self.payload)
        if self.channel == "runtime":
            return RuntimeEvent.from_record(record)
        return UserEvent.from_record(record)
