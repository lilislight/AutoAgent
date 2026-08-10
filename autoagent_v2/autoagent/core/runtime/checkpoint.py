"""Latest executable state passed independently from append-only Events."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from ..scheduler import EdgeResolution, NodeExecutionRequest
from .serialization import (
    RuntimeValueCodec,
    decode_json_record,
    encode_json_record,
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
    input: Any = None
    error: str | None = None
    idempotency_key: str | None = None
    started_state_version: int = 0
    restart_session_context: dict[str, Any] | None = None
    restart_invocation_context: dict[str, Any] | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "execution_id": str(self.execution_id),
            "node_id": self.node_id,
            "scope": [[region_id, iteration] for region_id, iteration in self.scope],
            "state": self.state,
            "input": RuntimeValueCodec.encode(self.input),
            "error": self.error,
            "idempotency_key": self.idempotency_key,
            "started_state_version": self.started_state_version,
            "restart_session_context": RuntimeValueCodec.encode(
                self.restart_session_context
            ),
            "restart_invocation_context": RuntimeValueCodec.encode(
                self.restart_invocation_context
            ),
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "NodeCheckpoint":
        return cls(
            execution_id=UUID(str(value["execution_id"])),
            node_id=str(value["node_id"]),
            scope=tuple((str(item[0]), int(item[1])) for item in value["scope"]),
            state=str(value["state"]),
            input=RuntimeValueCodec.decode(value.get("input")),
            error=value.get("error"),
            idempotency_key=value.get("idempotency_key"),
            started_state_version=int(value.get("started_state_version", 0)),
            restart_session_context=RuntimeValueCodec.decode(
                value.get("restart_session_context")
            ),
            restart_invocation_context=RuntimeValueCodec.decode(
                value.get("restart_invocation_context")
            ),
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
            "payload": RuntimeValueCodec.encode(self.payload),
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "WaitCheckpoint":
        return cls(
            id=UUID(str(value["id"])),
            node_execution_id=UUID(str(value["node_execution_id"])),
            request=NodeExecutionRequest.from_record(value["request"]),
            payload=RuntimeValueCodec.decode(value.get("payload")),
        )


@dataclass(frozen=True, slots=True)
class RecoveryCheckpoint:
    schema_version: int
    workflow_id: str
    workflow_revision_id: str
    session_id: str
    invocation_id: UUID
    invocation_state: str
    state_version: int
    runtime_event_sequence: int
    user_event_sequence: int
    invocation_input: Any
    invocation_output: dict[str, Any] | None
    invocation_error: dict[str, str] | None
    session_context: dict[str, Any]
    invocation_context: dict[str, Any]
    session_path_revisions: dict[tuple[str, ...], int]
    invocation_path_revisions: dict[tuple[str, ...], int]
    scheduler_state: SchedulerCheckpoint
    node_states: tuple[NodeCheckpoint, ...]
    required_outputs: dict[UUID, Any]
    latest_output_ids: dict[str, UUID]
    node_execution_counts: dict[str, int]
    operator_attempt_counts: dict[str, int]
    operator_runtime_ns: dict[str, int]
    waits: tuple[WaitCheckpoint, ...]
    pending_advances: tuple[tuple[UUID, NodeExecutionRequest], ...]
    session_created_at_ms: int
    invocation_created_at_ms: int
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
            "state_version": self.state_version,
            "runtime_event_sequence": self.runtime_event_sequence,
            "user_event_sequence": self.user_event_sequence,
            "invocation_input": RuntimeValueCodec.encode(self.invocation_input),
            "invocation_output": RuntimeValueCodec.encode(self.invocation_output),
            "invocation_error": RuntimeValueCodec.encode(self.invocation_error),
            "session_context": RuntimeValueCodec.encode(self.session_context),
            "invocation_context": RuntimeValueCodec.encode(self.invocation_context),
            "session_path_revisions": [
                {"path": list(path), "version": version}
                for path, version in sorted(self.session_path_revisions.items())
            ],
            "invocation_path_revisions": [
                {"path": list(path), "version": version}
                for path, version in sorted(self.invocation_path_revisions.items())
            ],
            "scheduler_state": self.scheduler_state.to_record(),
            "node_states": [item.to_record() for item in self.node_states],
            "required_outputs": {
                str(key): RuntimeValueCodec.encode(value)
                for key, value in self.required_outputs.items()
            },
            "latest_output_ids": {
                key: str(value) for key, value in self.latest_output_ids.items()
            },
            "node_execution_counts": dict(self.node_execution_counts),
            "operator_attempt_counts": dict(self.operator_attempt_counts),
            "operator_runtime_ns": dict(self.operator_runtime_ns),
            "waits": [wait.to_record() for wait in self.waits],
            "pending_advances": [
                {"execution_id": str(execution_id), "request": request.to_record()}
                for execution_id, request in self.pending_advances
            ],
            "session_created_at_ms": self.session_created_at_ms,
            "invocation_created_at_ms": self.invocation_created_at_ms,
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
            state_version=int(value.get("state_version", 0)),
            runtime_event_sequence=int(value["runtime_event_sequence"]),
            user_event_sequence=int(value["user_event_sequence"]),
            invocation_input=RuntimeValueCodec.decode(value.get("invocation_input")),
            invocation_output=RuntimeValueCodec.decode(value.get("invocation_output")),
            invocation_error=RuntimeValueCodec.decode(value.get("invocation_error")),
            session_context=RuntimeValueCodec.decode(value.get("session_context", {})),
            invocation_context=RuntimeValueCodec.decode(value.get("invocation_context", {})),
            session_path_revisions={
                tuple(str(token) for token in item["path"]): int(item["version"])
                for item in value.get("session_path_revisions", [])
            },
            invocation_path_revisions={
                tuple(str(token) for token in item["path"]): int(item["version"])
                for item in value.get("invocation_path_revisions", [])
            },
            scheduler_state=SchedulerCheckpoint.from_record(value["scheduler_state"]),
            node_states=tuple(NodeCheckpoint.from_record(item) for item in value["node_states"]),
            required_outputs={
                UUID(str(key)): RuntimeValueCodec.decode(item)
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
            pending_advances=tuple(
                (
                    UUID(str(item["execution_id"])),
                    NodeExecutionRequest.from_record(item["request"]),
                )
                for item in value.get("pending_advances", [])
            ),
            session_created_at_ms=int(
                value.get("session_created_at_ms", value["created_at_ms"])
            ),
            invocation_created_at_ms=int(
                value.get("invocation_created_at_ms", value["created_at_ms"])
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
