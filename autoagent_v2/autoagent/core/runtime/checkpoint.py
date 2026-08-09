"""Latest executable state passed independently from append-only Events."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from ..scheduler import EdgeResolution, NodeExecutionRequest
from .serialization import (
    decode_json_record,
    decode_runtime_value,
    encode_json_record,
    encode_runtime_value,
)


@dataclass(frozen=True, slots=True)
class SchedulerCheckpoint:
    ready: tuple[NodeExecutionRequest, ...]
    resolutions: tuple[tuple[str, EdgeResolution], ...]
    scheduled: tuple[str, ...]
    skipped: tuple[str, ...]

    def to_record(self) -> dict[str, Any]:
        return {
            "ready": [item.to_record() for item in self.ready],
            "resolutions": [
                {"key": key, "value": value.to_record()}
                for key, value in self.resolutions
            ],
            "scheduled": list(self.scheduled),
            "skipped": list(self.skipped),
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "SchedulerCheckpoint":
        return cls(
            ready=tuple(NodeExecutionRequest.from_record(item) for item in value["ready"]),
            resolutions=tuple(
                (str(item["key"]), EdgeResolution.from_record(item["value"]))
                for item in value["resolutions"]
            ),
            scheduled=tuple(str(item) for item in value["scheduled"]),
            skipped=tuple(str(item) for item in value["skipped"]),
        )


@dataclass(frozen=True, slots=True)
class NodeCheckpoint:
    execution_id: UUID
    node_id: str
    scope: tuple[tuple[str, int], ...]
    state: str

    def to_record(self) -> dict[str, Any]:
        return {
            "execution_id": str(self.execution_id),
            "node_id": self.node_id,
            "scope": [[region_id, iteration] for region_id, iteration in self.scope],
            "state": self.state,
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "NodeCheckpoint":
        return cls(
            execution_id=UUID(str(value["execution_id"])),
            node_id=str(value["node_id"]),
            scope=tuple((str(item[0]), int(item[1])) for item in value["scope"]),
            state=str(value["state"]),
        )


@dataclass(frozen=True, slots=True)
class WaitCheckpoint:
    id: UUID
    node_execution_id: UUID
    request: NodeExecutionRequest
    payload: Any = None

    def to_record(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "node_execution_id": str(self.node_execution_id),
            "request": self.request.to_record(),
            "payload": encode_runtime_value(self.payload),
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "WaitCheckpoint":
        return cls(
            id=UUID(str(value["id"])),
            node_execution_id=UUID(str(value["node_execution_id"])),
            request=NodeExecutionRequest.from_record(value["request"]),
            payload=decode_runtime_value(value.get("payload")),
        )


@dataclass(frozen=True, slots=True)
class RecoveryCheckpoint:
    schema_version: int
    workflow_id: str
    workflow_revision_id: str
    session_id: str
    invocation_id: UUID
    invocation_state: str
    runtime_event_sequence: int
    user_event_sequence: int
    invocation_input: Any
    session_context: dict[str, Any]
    invocation_context: dict[str, Any]
    scheduler_state: SchedulerCheckpoint
    node_states: tuple[NodeCheckpoint, ...]
    required_outputs: dict[UUID, Any]
    latest_output_ids: dict[str, UUID]
    node_execution_counts: dict[str, int]
    operator_attempt_counts: dict[str, int]
    operator_runtime_ns: dict[str, int]
    waits: tuple[WaitCheckpoint, ...]
    created_at_ms: int

    @classmethod
    def detached(cls, **values: Any) -> "RecoveryCheckpoint":
        return cls(**copy.deepcopy(values))

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "workflow_id": self.workflow_id,
            "workflow_revision_id": self.workflow_revision_id,
            "session_id": self.session_id,
            "invocation_id": str(self.invocation_id),
            "invocation_state": self.invocation_state,
            "runtime_event_sequence": self.runtime_event_sequence,
            "user_event_sequence": self.user_event_sequence,
            "invocation_input": encode_runtime_value(self.invocation_input),
            "session_context": encode_runtime_value(self.session_context),
            "invocation_context": encode_runtime_value(self.invocation_context),
            "scheduler_state": self.scheduler_state.to_record(),
            "node_states": [item.to_record() for item in self.node_states],
            "required_outputs": {
                str(key): encode_runtime_value(value)
                for key, value in self.required_outputs.items()
            },
            "latest_output_ids": {
                key: str(value) for key, value in self.latest_output_ids.items()
            },
            "node_execution_counts": dict(self.node_execution_counts),
            "operator_attempt_counts": dict(self.operator_attempt_counts),
            "operator_runtime_ns": dict(self.operator_runtime_ns),
            "waits": [wait.to_record() for wait in self.waits],
            "created_at_ms": self.created_at_ms,
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "RecoveryCheckpoint":
        return cls(
            schema_version=int(value["schema_version"]),
            workflow_id=str(value["workflow_id"]),
            workflow_revision_id=str(value["workflow_revision_id"]),
            session_id=str(value["session_id"]),
            invocation_id=UUID(str(value["invocation_id"])),
            invocation_state=str(value["invocation_state"]),
            runtime_event_sequence=int(value["runtime_event_sequence"]),
            user_event_sequence=int(value["user_event_sequence"]),
            invocation_input=decode_runtime_value(value.get("invocation_input")),
            session_context=decode_runtime_value(value.get("session_context", {})),
            invocation_context=decode_runtime_value(value.get("invocation_context", {})),
            scheduler_state=SchedulerCheckpoint.from_record(value["scheduler_state"]),
            node_states=tuple(NodeCheckpoint.from_record(item) for item in value["node_states"]),
            required_outputs={
                UUID(str(key)): decode_runtime_value(item)
                for key, item in value.get("required_outputs", {}).items()
            },
            latest_output_ids={
                str(key): UUID(str(item))
                for key, item in value.get("latest_output_ids", {}).items()
            },
            node_execution_counts={
                str(key): int(item)
                for key, item in value.get("node_execution_counts", {}).items()
            },
            operator_attempt_counts={
                str(key): int(item)
                for key, item in value.get("operator_attempt_counts", {}).items()
            },
            operator_runtime_ns={
                str(key): int(item)
                for key, item in value.get("operator_runtime_ns", {}).items()
            },
            waits=tuple(
                WaitCheckpoint.from_record(wait) for wait in value.get("waits", [])
            ),
            created_at_ms=int(value["created_at_ms"]),
        )


@dataclass(frozen=True, slots=True)
class SerializedCheckpoint:
    """Immutable latest-wins Core-to-Sink checkpoint envelope."""

    workflow_id: str
    workflow_revision_id: str
    session_id: str
    invocation_id: UUID
    invocation_state: str
    runtime_event_sequence: int
    user_event_sequence: int
    created_at_ms: int
    payload: bytes
    size_bytes: int

    @classmethod
    def from_checkpoint(
        cls, checkpoint: RecoveryCheckpoint
    ) -> "SerializedCheckpoint":
        payload = encode_json_record(checkpoint.to_record())
        return cls(
            workflow_id=checkpoint.workflow_id,
            workflow_revision_id=checkpoint.workflow_revision_id,
            session_id=checkpoint.session_id,
            invocation_id=checkpoint.invocation_id,
            invocation_state=checkpoint.invocation_state,
            runtime_event_sequence=checkpoint.runtime_event_sequence,
            user_event_sequence=checkpoint.user_event_sequence,
            created_at_ms=checkpoint.created_at_ms,
            payload=payload,
            size_bytes=len(payload),
        )

    def decode(self) -> RecoveryCheckpoint:
        return RecoveryCheckpoint.from_record(decode_json_record(self.payload))
