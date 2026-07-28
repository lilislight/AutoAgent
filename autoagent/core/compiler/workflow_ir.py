from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer
from autoagent.core.workflow.user_event import UserEventMappings

from autoagent.core.operators.contract import SchemaContract
from autoagent.core.workflow.policy import (
    EdgePolicy,
    NodePolicy,
    WorkflowPolicy,
)


class LoopRegionIR(BaseModel):
    """Compiled natural loop with ordinary graph semantics inside its body.

    A loop region is derived by the compiler; workflow authors do not create it.
    A natural loop is identified by one header and one or more back edges whose
    target is that header. Regions may be nested. Runtime executes the acyclic
    body between back edges with the same fan-out and complete fan-in rules as
    the rest of the graph.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(description="Stable compiler-generated loop region id.")
    node_ids: tuple[str, ...] = Field(
        description="Workflow nodes contained in this natural-loop body."
    )
    header_node_id: str = Field(
        description="Only node that external entry and back edges may target."
    )
    internal_edge_ids: tuple[str, ...] = Field(
        description="Repeatable edges whose source and target are inside the region."
    )
    external_entry_edge_ids: tuple[str, ...] = Field(
        description="Edges entering the header from outside this loop."
    )
    back_edge_ids: tuple[str, ...] = Field(
        description="Cycle-closing edges from the loop body to its header."
    )
    exit_edge_ids: tuple[str, ...] = Field(
        description="Edges leaving this loop for its parent scope."
    )
    parent_loop_region_id: str | None = Field(
        default=None,
        description="Immediately containing natural loop, if this loop is nested.",
    )
    child_loop_region_ids: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Immediately nested natural loops in declaration order.",
    )
    depth: int = Field(
        default=0,
        description="Zero-based nesting depth used to build execution scopes.",
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
    loop_regions: dict[str, LoopRegionIR] = Field(
        default_factory=dict,
        description="Compiler-derived loop regions keyed by loop region id.",
    )
    node_loop_stacks: dict[str, tuple[str, ...]] = Field(
        default_factory=dict,
        description="Node id to containing loop ids ordered outermost to innermost.",
    )


class NodeIR(BaseModel):
    """Compiled runtime-ready node definition."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        validate_assignment=True,
    )

    id: str = Field(description="Compiled unique node id.")
    local_id: str | None = Field(
        default=None,
        description="Source node id visible in its authoring Workflow scope.",
    )
    scope_node_ids: dict[str, str] = Field(
        default_factory=dict,
        description="Source node ids to expanded ids used by hook output lookups.",
    )
    workflow_path: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Containing child Workflow node path; empty at root.",
    )
    name: str | None = Field(
        default=None,
        description="Optional display name retained for tracing and graph views.",
    )
    description: str | None = Field(
        default=None,
        description="Optional display description retained for observation tools.",
    )
    capability: Any = Field(
        description="Compiled capability binding. Exact type is defined later."
    )
    input_contract: SchemaContract = Field(
        description=(
            "Compiled named-argument input contract for each OperatorExecution."
        ),
    )
    operator_output_contract: SchemaContract = Field(
        description=(
            "Compiled output contract for each individual OperatorExecution before "
            "map or replication aggregation."
        ),
    )
    output_contract: SchemaContract = Field(
        description=(
            "Final NodeExecution output contract after optional map or "
            "replication aggregation."
        ),
    )
    input_plan: Any | None = Field(
        default=None,
        description="Compiled input plan. Exact type is defined later.",
    )
    output_binding: Any | None = Field(
        default=None,
        description="Compiled output binding. Exact type is defined later.",
    )
    stream_user_event_mapping: UserEventMappings = None
    user_event_mapping: UserEventMappings = None
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

    @field_serializer(
        "input_contract",
        "operator_output_contract",
        "output_contract",
    )
    def serialize_contract(self, contract: SchemaContract) -> dict[str, Any]:
        """Exclude live validators and call signatures from serialized IR views."""

        return contract.describe()


class EdgeIR(BaseModel):
    """Compiled runtime-ready edge definition."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(description="Compiled unique edge id.")
    local_id: str | None = Field(
        default=None,
        description="Source edge id visible in its authoring Workflow scope.",
    )
    local_from_node: str | None = Field(
        default=None,
        description="Source endpoint id visible to the condition hook.",
    )
    local_to_node: str | None = Field(
        default=None,
        description="Target endpoint id visible to the condition hook.",
    )
    scope_node_ids: dict[str, str] = Field(
        default_factory=dict,
        description="Source node ids to expanded ids for condition output lookups.",
    )
    workflow_path: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Containing child Workflow node path; empty at root.",
    )
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
        description="Stable outgoing edge order for evaluation and tracing.",
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
    definition_hash: str = Field(
        description=(
            "SHA-256 hash of canonical execution semantics. Display metadata and "
            "Operator implementation code are excluded."
        ),
    )
    name: str | None = Field(
        default=None,
        description="Optional Workflow display name retained for observation.",
    )
    description: str | None = Field(
        default=None,
        description="Optional Workflow display description retained for observation.",
    )
    policy: WorkflowPolicy = Field(
        default_factory=WorkflowPolicy,
        description="Compiled Workflow failure behavior.",
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
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-semantic auxiliary data.",
    )
