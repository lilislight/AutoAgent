"""Mutable execution records owned by one active Invocation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID, uuid4

from ..scheduler import ExecutionScope


NodeState = Literal[
    "pending", "ready", "running", "waiting", "completed", "failed", "skipped", "cancelled"
]


@dataclass(slots=True)
class NodeExecution:
    node_id: str
    scope: ExecutionScope
    state: NodeState = "ready"
    id: UUID = field(default_factory=uuid4)
    input: Any = None
    output: Any = None
    error: str | None = None
    started_at_ms: int | None = None
    started_perf_ns: int | None = None
    completed_at_ms: int | None = None
    duration_ns: int | None = None
    operator_attempts: int = 0
    logical_occurrence: int = 1
    idempotency_key: str | None = None
    session_context_revision: int = 0
    invocation_context_revision: int = 0


@dataclass(frozen=True, slots=True)
class OperatorCallRecord:
    id: UUID
    node_execution_id: UUID
    node_id: str
    operator_id: str
    unit_kind: str
    unit_index: int
    attempt: int
    status: Literal["completed", "failed", "timed_out", "cancelled"]
    duration_ns: int
    queue_wait_ns: int = 0
    handler_ns: int = 0
    stream_ns: int = 0
    stream_delivery_ns: int = 0
    input: Any = None
    output: Any = None
    error: str | None = None
    idempotency_key: str | None = None
