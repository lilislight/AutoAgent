from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from autoagent.core.operators.contract import OperatorContract


class Capability(BaseModel):
    """Application-registered contract implemented by one or more Operators.

    A Capability is not executable. Workflow nodes use CapabilityRef to request
    this contract, and NodeExecutor selects an available registered Operator at
    execution time. Keeping the contract separate lets implementations change
    without changing or recompiling the Workflow structure.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    id: str = Field(
        description="Stable application-local capability id, such as web_search.",
    )
    description: str | None = Field(
        default=None,
        description="Optional human-readable capability description.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic data for tooling and integrations.",
    )
    _contract: OperatorContract | None = PrivateAttr(default=None)

    @property
    def contract(self) -> OperatorContract | None:
        """Contract established by the first registered Operator implementation."""

        return self._contract

    def _bind_contract(self, contract: OperatorContract) -> None:
        """Bind the implementation-derived contract once; registry owns this call."""

        if self._contract is not None:
            raise ValueError(f"Capability contract is already established: {self.id}")
        self._contract = contract

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        resolved = value.strip()
        if not resolved:
            raise ValueError("Capability id cannot be empty.")
        return resolved
