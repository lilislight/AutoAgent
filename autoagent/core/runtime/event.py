from __future__ import annotations

from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.runtime.time import TimestampMs


RuntimeEventRole = Literal["boundary"]


class RuntimeEvent(BaseModel):
    """One immutable, invocation-local state transition.

    V1 persists only reducer-relevant boundaries. Trace/UI representations are
    derived views and do not occupy this sequence or the durable journal.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID = Field(default_factory=uuid4)
    invocation_id: UUID
    sequence: int = Field(ge=1)
    schema_version: int = Field(default=1, ge=1)
    type: str = Field(min_length=1)
    occurred_at_ms: TimestampMs
    payload: dict[str, Any] = Field(default_factory=dict)
    role: RuntimeEventRole = "boundary"

    @property
    def boundary(self) -> str:
        return self.type
