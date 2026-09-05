from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from autoagent.core.runtime.time import TimestampMs, utc_timestamp_ms
from autoagent.core.workflow.user_event import validate_user_event_type


@dataclass(frozen=True)
class UserEventSpec:
    """Detached process-local request for one Runtime-assigned UserEvent."""

    type: str
    data: Any
    node_id: str
    node_execution_id: UUID
    workflow_path: tuple[str, ...] = ()
    operator_call_id: UUID | None = None
    occurred_at_ms: TimestampMs = 0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "type",
            validate_user_event_type(self.type),
        )
        if self.occurred_at_ms == 0:
            object.__setattr__(self, "occurred_at_ms", utc_timestamp_ms())


class UserEvent(BaseModel):
    """One immutable Invocation-local event for Agent or application UIs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID = Field(default_factory=uuid4)
    invocation_id: UUID
    sequence: int = Field(ge=1)
    schema_version: int = Field(default=2, ge=1)
    type: str
    data: Any
    node_id: str
    workflow_path: tuple[str, ...] = ()
    node_execution_id: UUID
    operator_call_id: UUID | None = None
    occurred_at_ms: TimestampMs

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        return validate_user_event_type(value)
