from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from autoagent.workflow.node import Node


class Edge(BaseModel):
    """Static control-flow or dependency relationship between two Workflow nodes."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str | None = Field(
        default=None,
        description=(
            "Optional unique edge id inside one Workflow. Compiler assigns one "
            "before emitting Workflow IR when omitted."
        )
    )
    from_node: str | Node = Field(
        description=(
            "Source node reference as a node id string or Node object. Compiler "
            "validates it against the Workflow's nodes."
        )
    )
    to_node: str | Node = Field(
        description=(
            "Target node reference as a node id string or Node object. Compiler "
            "validates it against the Workflow's nodes."
        )
    )
    condition: Callable[..., bool] | str | None = Field(
        default=None,
        description=(
            "Optional bool condition for selecting this edge. It may be a "
            "callable or a string expression for compiler validation."
        ),
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Non-semantic auxiliary data for tooling, visualization, debugging, "
            "optimizer notes, or integrations."
        ),
    )
