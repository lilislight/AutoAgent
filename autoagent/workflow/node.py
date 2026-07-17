from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from autoagent.workflow.capability import CapabilityRef, OperatorRef, SystemCommand
from autoagent.workflow.mapping import InputMapping, OutputBinding
from autoagent.workflow.policy import NodePolicy


class Node(BaseModel):
    """Static schedulable execution unit inside a Workflow source definition."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str | None = Field(
        default=None,
        description=(
            "Optional unique node id inside one Workflow. Compiler assigns one "
            "before emitting Workflow IR when omitted."
        )
    )
    capability: Callable[..., Any] | str | CapabilityRef | OperatorRef | SystemCommand = Field(
        description=(
            "Capability executed by this node. str is shorthand for CapabilityRef."
        )
    )
    name: str | None = Field(
        default=None,
        description="Optional human-readable display name.",
    )
    description: str | None = Field(
        default=None,
        description=(
            "Optional human-readable explanation of what this node does."
        ),
    )
    input_mapping: InputMapping | None = Field(
        default=None,
        description=(
            "Optional function that builds named Operator arguments from runtime "
            "data. It must return a Mapping keyed by parameter name; runtime "
            "copies the result to a dict before Operator execution."
        ),
    )
    output_binding: OutputBinding | None = Field(
        default=None,
        description=(
            "Optional post-completion hook. Runtime passes a restricted context "
            "that may mutate invocation data or session data only."
        ),
    )
    entry: bool | None = Field(
        default=None,
        description=(
            "True marks this node as a Workflow entry. None allows compiler "
            "entry inference."
        ),
    )
    policy: NodePolicy | None = Field(
        default=None,
        description=(
            "Optional node-level execution policy such as retry, timeout, "
            "resource, replication, or capability selection behavior."
        ),
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Non-semantic auxiliary data for tooling, visualization, debugging, "
            "or integrations."
        ),
    )
