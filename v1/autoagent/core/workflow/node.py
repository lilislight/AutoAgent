from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from autoagent.core.operators.operator import Operator
from autoagent.core.workflow.capability import CapabilityRef, OperatorRef, SystemCommand
from autoagent.core.workflow.mapping import InputMapping, OutputBinding
from autoagent.core.workflow.policy import NodePolicy
from autoagent.core.workflow.user_event import (
    UserEventMapping,
    UserEventMappings,
)

if TYPE_CHECKING:
    from autoagent.core.workflow.workflow import Workflow


class Node(BaseModel):
    """Static schedulable execution unit inside a Workflow source definition."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        validate_assignment=True,
    )

    id: str = Field(
        description="Required unique node id inside one Workflow."
    )
    capability: (
        Callable[..., Any]
        | Operator
        | str
        | CapabilityRef
        | OperatorRef
        | SystemCommand
        | Workflow
    ) = Field(
        description=(
            "Executable binding for this node. A direct Callable is compiled "
            "into a direct Operator; str is shorthand for CapabilityRef; a "
            "Workflow is recursively expanded at compile time."
        )
    )
    child_entry_node_id: str | None = Field(
        default=None,
        description=(
            "Source node id selected as the entry when capability is a child "
            "Workflow with multiple entries."
        ),
    )
    child_exit_node_id: str | None = Field(
        default=None,
        description=(
            "Source node id selected as the exit when capability is a child "
            "Workflow with multiple exits."
        ),
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
    stream_user_event_mapping: UserEventMappings = Field(
        default=None,
        description=(
            "Optional type-and-transform mapping applied to each explicit "
            "StreamingResult chunk. The framework owns lifecycle metadata."
        ),
    )
    user_event_mapping: UserEventMappings = Field(
        default=None,
        description=(
            "Optional type-and-transform mapping applied after Output Binding "
            "and successful Node completion."
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

    # Compiler-only provenance used after recursive expansion. Authors cannot
    # provide these private values on the source model.
    _local_id: str | None = PrivateAttr(default=None)
    _scope_node_ids: dict[str, str] = PrivateAttr(default_factory=dict)
    _workflow_path: tuple[str, ...] = PrivateAttr(default_factory=tuple)

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        resolved = value.strip()
        if not resolved:
            raise ValueError("Node id cannot be empty.")
        return resolved

    @field_validator("child_entry_node_id", "child_exit_node_id")
    @classmethod
    def validate_child_boundary_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        resolved = value.strip()
        if not resolved:
            raise ValueError("Child Workflow boundary node id cannot be empty.")
        return resolved
