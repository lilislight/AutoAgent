from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from autoagent.workflow.policy import (
    EdgePolicy,
    NodePolicy,
    WorkflowPolicy,
)


class GraphIR(BaseModel):
    """Compiled graph indexes for scheduler lookup."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    outgoing_edges: dict[str, tuple[str, ...]] = Field(
        default_factory=dict,
        description="Node id to outgoing edge ids.",
    )
    incoming_edges: dict[str, tuple[str, ...]] = Field(
        default_factory=dict,
        description="Node id to incoming edge ids.",
    )
    predecessors: dict[str, tuple[str, ...]] = Field(
        default_factory=dict,
        description="Node id to predecessor node ids.",
    )
    successors: dict[str, tuple[str, ...]] = Field(
        default_factory=dict,
        description="Node id to successor node ids.",
    )


class NodeIR(BaseModel):
    """Compiled runtime-ready node definition."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(description="Compiled unique node id.")
    capability: Any = Field(
        description="Compiled capability binding. Exact type is defined later."
    )
    input_schema: Any | None = Field(
        default=None,
        description="Compiled input schema when available.",
    )
    output_schema: Any | None = Field(
        default=None,
        description="Compiled output schema when available.",
    )
    input_plan: Any | None = Field(
        default=None,
        description="Compiled input plan. Exact type is defined later.",
    )
    output_binding: Any | None = Field(
        default=None,
        description="Compiled output binding. Exact type is defined later.",
    )
    policy: NodePolicy | None = Field(
        default=None,
        description="Compiled node-level policy.",
    )
    entry: bool = Field(
        default=False,
        description="Whether this node is a compiled entry node.",
    )
    exit: bool = Field(
        default=False,
        description="Whether this node is a compiled exit node.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic auxiliary data.",
    )


class EdgeIR(BaseModel):
    """Compiled runtime-ready edge definition."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(description="Compiled unique edge id.")
    from_node: str = Field(description="Compiled source node id.")
    to_node: str = Field(description="Compiled target node id.")
    condition: Any | None = Field(
        default=None,
        description="Compiled edge condition. Exact type is defined later.",
    )
    policy: EdgePolicy | None = Field(
        default=None,
        description="Compiled edge-level policy such as map/fan-out.",
    )
    order: int = Field(
        default=0,
        description="Stable outgoing edge order for routing policy.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic auxiliary data.",
    )


class WorkflowIR(BaseModel):
    """Compiled Workflow representation consumed by runtime components."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    ir_version: str = Field(description="Workflow IR schema version.")
    compiler_version: str = Field(description="Compiler version that emitted IR.")
    workflow_id: str = Field(description="Compiled Workflow id.")
    workflow_version: str | int | None = Field(
        default=None,
        description="Compiled Workflow version.",
    )
    nodes: dict[str, NodeIR] = Field(
        default_factory=dict,
        description="Compiled nodes keyed by node id.",
    )
    edges: dict[str, EdgeIR] = Field(
        default_factory=dict,
        description="Compiled edges keyed by edge id.",
    )
    graph: GraphIR = Field(
        default_factory=GraphIR,
        description="Compiled graph indexes.",
    )
    entry_node_ids: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Compiled entry node ids.",
    )
    exit_node_ids: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Compiled exit node ids.",
    )
    policy: WorkflowPolicy | None = Field(
        default=None,
        description="Compiled Workflow-level policy.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic auxiliary data.",
    )
