from __future__ import annotations

from typing import Any, Literal, TypeAlias
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.runtime.time import TimestampMs


RuntimeEventMode: TypeAlias = Literal["minimal", "standard", "full"]
RuntimeEventType: TypeAlias = Literal[
    "state_change",
    "phase",
    "operator_call",
    "routing",
    "wait",
    "recovery",
]
RuntimeEventSubjectType: TypeAlias = Literal[
    "invocation",
    "node",
    "edge",
    "operator_call",
    "wait",
    "recovery",
]


class StateOperation(BaseModel):
    """One deterministic increment applied to the Runtime State tree."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    op: Literal["add", "replace", "remove"]
    path: tuple[str | int, ...]
    value: Any | None = None


class RuntimeEvent(BaseModel):
    """One immutable, invocation-local Runtime fact.

    ``event_type`` selects the payload family while ``event_name`` identifies
    the exact semantic occurrence. Full-mode Events additionally carry the
    StateOperations needed to rebuild the Runtime State after this Event.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID = Field(default_factory=uuid4)
    invocation_id: UUID
    sequence: int = Field(ge=1)
    schema_version: int = Field(default=1, ge=1)
    event_type: RuntimeEventType
    event_name: str = Field(min_length=1)
    subject_type: RuntimeEventSubjectType
    subject_id: str = Field(min_length=1)
    occurred_at_ms: TimestampMs
    elapsed_ns: int | None = Field(default=None, ge=0)
    status: str | None = None
    timing: dict[str, int] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)
    input: Any | None = None
    output: Any | None = None
    operations: tuple[StateOperation, ...] | None = None
