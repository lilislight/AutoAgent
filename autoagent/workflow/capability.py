from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


WAIT_SYSTEM_COMMAND_ID = "wait"


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
    """Reference to a framework-owned runtime command.

    V1 supports only ``SystemCommand(id="wait")``. Unlike an Operator, a
    SystemCommand is interpreted by the framework and never resolved through an
    OperatorRegistry.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(
        description="Framework-owned command id. V1 accepts only 'wait'.",
    )
    command: Any | None = Field(
        default=None,
        description="Reserved for future command objects; unsupported in V1.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic auxiliary data.",
    )
