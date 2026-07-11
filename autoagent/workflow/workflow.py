from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from autoagent.workflow.edge import Edge
from autoagent.workflow.mapping import InputMapping, OutputBinding
from autoagent.workflow.node import Node
from autoagent.workflow.policy import WorkflowPolicy


class Workflow(BaseModel):
    """Static Workflow source definition authored by the developer."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # TODO: YAML/JSON/UI builders should translate serialized authoring forms
    # into this source model. In particular, they need rules for mapping
    # expressions and string conditions before those formats can be supported.

    id: str | None = Field(
        default=None,
        description=(
            "Optional stable Workflow id. Compiler assigns one before emitting "
            "Workflow IR when omitted."
        )
    )
    version: str | int | None = Field(
        default=None,
        description=(
            "Optional Workflow version. Compiler or packaging may assign one "
            "when omitted."
        )
    )
    nodes: list[Node] = Field(
        default_factory=list,
        description=(
            "Ordered source node definitions. Compiler uses them to build "
            "Workflow IR nodes and graph indexes."
        )
    )
    edges: list[Edge] = Field(
        default_factory=list,
        description=(
            "Ordered source edge definitions. Compiler validates endpoints and "
            "builds Workflow IR graph indexes."
        )
    )
    name: str | None = Field(
        default=None,
        description="Optional human-readable Workflow name.",
    )
    description: str | None = Field(
        default=None,
        description=(
            "Optional human-readable explanation of the Workflow purpose."
        ),
    )
    policy: WorkflowPolicy | None = Field(
        default=None,
        description=(
            "Optional Workflow-level policy, such as unhandled branch failure "
            "behavior."
        ),
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Non-semantic auxiliary data for tooling, visualization, debugging, "
            "optimizer notes, or integrations."
        ),
    )

    # Supported authoring forms:
    # - add_node(function)
    # - add_node("capability_string")
    # - add_node(Node(...))
    # - add_node(function, node_id="node_id")
    def add_node(
        self,
        node_or_capability: Node | Callable[..., Any] | str,
        *,
        node_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        input_schema: Any | None = None,
        input_mapping: InputMapping | None = None,
        output_binding: OutputBinding | None = None,
        entry: bool | None = None,
        policy: Any | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Node:
        """Create or add a Node to this Workflow and return it."""

        if isinstance(node_or_capability, Node):
            if any(
                value is not None
                for value in (
                    node_id,
                    name,
                    description,
                    input_schema,
                    input_mapping,
                    output_binding,
                    entry,
                    policy,
                    metadata,
                )
            ):
                raise ValueError("Cannot override fields when adding a Node object.")

            self.nodes.append(node_or_capability)
            return node_or_capability

        resolved_node_id = node_id
        node_capability = node_or_capability

        if isinstance(node_capability, str) and resolved_node_id is not None:
            raise ValueError(
                "Cannot provide node_id when adding a string capability."
            )

        if resolved_node_id is None and isinstance(node_capability, Callable):
            resolved_node_id = getattr(node_capability, "__name__", None)

        node = Node(
            id=resolved_node_id,
            capability=node_capability,
            name=name,
            description=description,
            input_schema=input_schema,
            input_mapping=input_mapping,
            output_binding=output_binding,
            entry=entry,
            policy=policy,
            metadata=metadata or {},
        )
        self.nodes.append(node)
        return node

    # Supported authoring forms:
    # - add_edge(Edge(...))
    # - add_edge(from_node, to_node)
    # - add_edge(from_node, to_node, edge_id="edge_id")
    # - add_edge(from_node, to_node, condition=condition)
    def add_edge(
        self,
        edge_or_from_node: Edge | Node | str,
        to_node: Node | str | None = None,
        *,
        edge_id: str | None = None,
        condition: Callable[..., bool] | str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Edge:
        """Create or add an Edge to this Workflow and return it."""

        if isinstance(edge_or_from_node, Edge):
            if to_node is not None or any(
                value is not None
                for value in (
                    edge_id,
                    condition,
                    metadata,
                )
            ):
                raise ValueError("Cannot override fields when adding an Edge object.")

            self.edges.append(edge_or_from_node)
            return edge_or_from_node

        if to_node is None:
            raise ValueError("add_edge requires a target node.")

        edge = Edge(
            id=edge_id,
            from_node=edge_or_from_node,
            to_node=to_node,
            condition=condition,
            metadata=metadata or {},
        )
        self.edges.append(edge)
        return edge
