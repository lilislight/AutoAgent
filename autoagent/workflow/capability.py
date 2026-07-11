from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CapabilityRef(BaseModel):
    """Reference to an abstract capability requirement."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(
        description="Capability name, such as web_search.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic auxiliary data.",
    )


class OperatorRef(BaseModel):
    """Reference to a specific operator implementation."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(
        description="Specific operator id to execute.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic auxiliary data.",
    )


class SystemCommand(BaseModel):
    """Runtime system command request."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(
        description="System command id, such as wait_human_input.",
    )
    command: Any | None = Field(
        default=None,
        description="Optional system command object.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic auxiliary data.",
    )
