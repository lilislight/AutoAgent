from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from autoagent.core.operators.operator import Operator
from autoagent.core.workflow.edge import Edge
from autoagent.core.workflow.capability import CapabilityRef, OperatorRef, SystemCommand
from autoagent.core.workflow.mapping import InputMapping, OutputBinding
from autoagent.core.workflow.node import Node
from autoagent.core.workflow.policy import EdgePolicy, WorkflowPolicy
from autoagent.core.workflow.user_event import UserEventMappings

if TYPE_CHECKING:
    from autoagent.core.compiler import WorkflowCompiler
    from autoagent.core.workflow.diagram import WorkflowDiagram


class Workflow(BaseModel):
    """Static Workflow source definition authored by the developer."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # TODO: YAML/JSON/UI builders should translate serialized authoring forms
    # into this source model. In particular, they need rules for mapping
    # expressions and string conditions before those formats can be supported.

    id: str = Field(
        description=(
            "Stable Workflow id used to locate sessions and persisted versions "
            "across application restarts."
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
    policy: WorkflowPolicy = Field(
        default_factory=WorkflowPolicy,
        description="Workflow-wide failure behavior.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Non-semantic auxiliary data for tooling, visualization, debugging, "
            "optimizer notes, or integrations."
        ),
    )

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        """Reject identities that cannot safely key registries or durable data."""

        resolved = value.strip()
        if not resolved:
            raise ValueError("Workflow id cannot be empty.")
        return resolved

    # Supported authoring forms:
    # - add_node(function, node_id="node_id")
    # - add_node("capability_string", node_id="node_id")
    # - add_node(child_workflow, node_id="child")
    # - add_node(Node(...))
    def add_node(
        self,
        node_or_capability: (
            Node
            | Callable[..., Any]
            | Operator
            | str
            | CapabilityRef
            | OperatorRef
            | SystemCommand
            | Workflow
        ),
        *,
        node_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        input_mapping: InputMapping | None = None,
        output_binding: OutputBinding | None = None,
        stream_user_event_mapping: UserEventMappings = None,
        user_event_mapping: UserEventMappings = None,
        entry: bool | None = None,
        policy: Any | None = None,
        metadata: dict[str, Any] | None = None,
        child_entry_node_id: str | None = None,
        child_exit_node_id: str | None = None,
    ) -> Node:
        """Create or add a Node to this Workflow and return it.

        A Workflow capability is an expandable child, not a runtime Operator.
        `node_id` becomes its namespace. Multiple child boundaries require
        child_entry_node_id and child_exit_node_id during compilation.
        """

        if isinstance(node_or_capability, Node):
            if any(
                value is not None
                for value in (
                    node_id,
                    name,
                    description,
                    input_mapping,
                    output_binding,
                    stream_user_event_mapping,
                    user_event_mapping,
                    entry,
                    policy,
                    metadata,
                    child_entry_node_id,
                    child_exit_node_id,
                )
            ):
                raise ValueError("Cannot override fields when adding a Node object.")

            self.nodes.append(node_or_capability)
            return node_or_capability

        if node_id is None:
            raise ValueError("node_id is required when adding a node capability.")

        node_capability = node_or_capability

        node = Node(
            id=node_id,
            capability=node_capability,
            name=name,
            description=description,
            input_mapping=input_mapping,
            output_binding=output_binding,
            stream_user_event_mapping=stream_user_event_mapping,
            user_event_mapping=user_event_mapping,
            entry=entry,
            policy=policy,
            metadata=metadata or {},
            child_entry_node_id=child_entry_node_id,
            child_exit_node_id=child_exit_node_id,
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
        condition: Callable[..., bool | Awaitable[bool]] | str | None = None,
        policy: EdgePolicy | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Edge:
        """Create or add an Edge to this Workflow and return it."""

        if isinstance(edge_or_from_node, Edge):
            if to_node is not None or any(
                value is not None
                for value in (
                    edge_id,
                    condition,
                    policy,
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
            policy=policy,
            metadata=metadata or {},
        )
        self.edges.append(edge)
        return edge

    def diagram(
        self,
        *,
        compiler: WorkflowCompiler | None = None,
    ) -> WorkflowDiagram:
        """Build a compiler-assisted static preview without executing Workflow."""

        from autoagent.core.workflow.diagram import WorkflowDiagram

        return WorkflowDiagram.from_workflow(self, compiler=compiler)

    def to_mermaid(self, *, compiler: WorkflowCompiler | None = None) -> str:
        """Return a Mermaid flowchart with invalid edges highlighted."""

        return self.diagram(compiler=compiler).to_mermaid()

    def preview(
        self,
        path: str | Path | None = None,
        *,
        compiler: WorkflowCompiler | None = None,
    ) -> Path:
        """Write a Mermaid graph preview and return its path."""

        from autoagent.core.workflow.diagram import default_preview_path

        target = path if path is not None else default_preview_path(self)
        return self.diagram(compiler=compiler).save(target)


# Resolve Node.capability's recursive Workflow annotation only after both
# Pydantic models exist. Authors still bind the Workflow object directly.
Node.model_rebuild(_types_namespace={"Workflow": Workflow})
