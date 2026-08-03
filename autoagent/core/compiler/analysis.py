from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.compiler.id_generation import generate_edge_id
from autoagent.core.compiler.workflow_ir import EdgeIR, GraphIR, NodeIR
from autoagent.core.operators import Operator, callable_operator_name
from autoagent.core.workflow.capability import (
    CapabilityRef,
    OperatorRef,
    SystemCommand,
)
from autoagent.core.workflow.node import Node
from autoagent.core.workflow.workflow import Workflow


class WorkflowAnalysisBinding(BaseModel):
    """Serializable description of one expanded Node binding."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["operator", "capability", "system_command"]
    id: str


class WorkflowAnalysisMapPolicy(BaseModel):
    """Display-safe MapPolicy facts without retaining live hooks."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_parallelism: int | None = None
    has_item_selector: bool = False
    has_output_aggregator: bool = False


class WorkflowAnalysisEdgePolicy(BaseModel):
    """Display-safe EdgePolicy facts."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    map: WorkflowAnalysisMapPolicy | None = None


class WorkflowAnalysisNode(BaseModel):
    """One Node in the Compiler's expanded candidate execution graph."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    local_id: str
    workflow_path: tuple[str, ...] = ()
    source_index: int = Field(ge=0)
    name: str | None = None
    description: str | None = None
    binding: WorkflowAnalysisBinding
    resolved: bool
    entry: bool | None = None
    exit: bool | None = None


class WorkflowAnalysisEdge(BaseModel):
    """One resolved or unresolved Edge in the expanded source graph."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    local_id: str
    workflow_path: tuple[str, ...] = ()
    source_index: int = Field(ge=0)
    from_node_id: str
    to_node_id: str
    source_resolved: bool
    target_resolved: bool
    conditional: bool = False
    policy: WorkflowAnalysisEdgePolicy | None = None


class WorkflowAnalysisLoop(BaseModel):
    """Compiler-derived natural-loop structure safe for static tooling."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    node_ids: tuple[str, ...]
    header_node_id: str
    internal_edge_ids: tuple[str, ...]
    external_entry_edge_ids: tuple[str, ...]
    back_edge_ids: tuple[str, ...]
    exit_edge_ids: tuple[str, ...]
    parent_loop_id: str | None = None
    child_loop_ids: tuple[str, ...] = ()
    depth: int = Field(default=0, ge=0)


class WorkflowAnalysis(BaseModel):
    """Static Compiler analysis available even when WorkflowIR is unavailable.

    ``None`` topology fields mean the candidate graph was incomplete, not that
    the corresponding collection was proven empty. Scheduler and Executor must
    consume WorkflowIR rather than this authoring/tooling projection.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: str
    workflow_version: str | int | None
    name: str | None = None
    description: str | None = None
    complete: bool
    nodes: tuple[WorkflowAnalysisNode, ...]
    edges: tuple[WorkflowAnalysisEdge, ...]
    entry_node_ids: tuple[str, ...] | None
    exit_node_ids: tuple[str, ...] | None
    loop_regions: tuple[WorkflowAnalysisLoop, ...] | None


def build_workflow_analysis(
    *,
    workflow: Workflow,
    workflow_id: str,
    workflow_version: str | int | None,
    nodes: dict[str, NodeIR],
    edges: dict[str, EdgeIR],
    graph: GraphIR,
    entry_node_ids: tuple[str, ...],
    exit_node_ids: tuple[str, ...],
    complete: bool,
) -> WorkflowAnalysis:
    """Project Compiler-owned intermediate state into an immutable analysis."""

    known_node_ids = {node.id for node in workflow.nodes}
    unique_node_ids = len(known_node_ids) == len(workflow.nodes)
    analysis_edges = _analysis_edges(workflow, known_node_ids=known_node_ids)
    endpoints_resolved = all(
        edge.source_resolved and edge.target_resolved for edge in analysis_edges
    )
    compiled_graph_complete = (
        unique_node_ids
        and endpoints_resolved
        and len(nodes) == len(workflow.nodes)
        and len(edges) == len(workflow.edges)
    )
    entry_ids = set(entry_node_ids)
    exit_ids = set(exit_node_ids)

    analysis_nodes = tuple(
        WorkflowAnalysisNode(
            id=node.id,
            local_id=node._local_id or node.id,
            workflow_path=node._workflow_path,
            source_index=index,
            name=node.name,
            description=node.description,
            binding=_binding(node.capability),
            resolved=node.id in nodes,
            entry=(node.id in entry_ids) if compiled_graph_complete else None,
            exit=(node.id in exit_ids) if compiled_graph_complete else None,
        )
        for index, node in enumerate(workflow.nodes)
    )
    loops = (
        tuple(
            WorkflowAnalysisLoop(
                id=region.id,
                node_ids=region.node_ids,
                header_node_id=region.header_node_id,
                internal_edge_ids=region.internal_edge_ids,
                external_entry_edge_ids=region.external_entry_edge_ids,
                back_edge_ids=region.back_edge_ids,
                exit_edge_ids=region.exit_edge_ids,
                parent_loop_id=region.parent_loop_region_id,
                child_loop_ids=region.child_loop_region_ids,
                depth=region.depth,
            )
            for region in graph.loop_regions.values()
        )
        if compiled_graph_complete
        else None
    )
    return WorkflowAnalysis(
        workflow_id=workflow_id,
        workflow_version=workflow_version,
        name=workflow.name,
        description=workflow.description,
        complete=complete,
        nodes=analysis_nodes,
        edges=analysis_edges,
        entry_node_ids=entry_node_ids if compiled_graph_complete else None,
        exit_node_ids=exit_node_ids if compiled_graph_complete else None,
        loop_regions=loops,
    )


def _analysis_edges(
    workflow: Workflow,
    *,
    known_node_ids: set[str],
) -> tuple[WorkflowAnalysisEdge, ...]:
    manual_ids = {edge.id for edge in workflow.edges if edge.id is not None}
    used_ids = set(manual_ids)
    values: list[WorkflowAnalysisEdge] = []
    for index, edge in enumerate(workflow.edges):
        from_node_id = _node_ref_id(edge.from_node)
        to_node_id = _node_ref_id(edge.to_node)
        if edge.id is None:
            edge_id = generate_edge_id(
                f"edge_{from_node_id}_{to_node_id}",
                used_ids,
            )
        else:
            edge_id = edge.id
        used_ids.add(edge_id)
        map_policy = edge.policy.map if edge.policy is not None else None
        policy = (
            WorkflowAnalysisEdgePolicy(
                map=WorkflowAnalysisMapPolicy(
                    max_parallelism=map_policy.max_parallelism,
                    has_item_selector=map_policy.item_selector is not None,
                    has_output_aggregator=map_policy.output_aggregator is not None,
                )
            )
            if map_policy is not None
            else None
        )
        values.append(
            WorkflowAnalysisEdge(
                id=edge_id,
                local_id=edge._local_id or edge_id,
                workflow_path=edge._workflow_path,
                source_index=index,
                from_node_id=from_node_id,
                to_node_id=to_node_id,
                source_resolved=from_node_id in known_node_ids,
                target_resolved=to_node_id in known_node_ids,
                conditional=edge.condition is not None,
                policy=policy,
            )
        )
    return tuple(values)


def _node_ref_id(value: str | Node) -> str:
    return value.id if isinstance(value, Node) else value


def _binding(value: Any) -> WorkflowAnalysisBinding:
    if isinstance(value, Operator):
        return WorkflowAnalysisBinding(kind="operator", id=value.definition_name)
    if callable(value):
        return WorkflowAnalysisBinding(
            kind="operator",
            id=callable_operator_name(value),
        )
    if isinstance(value, str):
        return WorkflowAnalysisBinding(kind="capability", id=value)
    if isinstance(value, CapabilityRef):
        return WorkflowAnalysisBinding(kind="capability", id=value.id)
    if isinstance(value, OperatorRef):
        return WorkflowAnalysisBinding(kind="operator", id=value.id)
    if isinstance(value, SystemCommand):
        return WorkflowAnalysisBinding(kind="system_command", id=value.id)
    return WorkflowAnalysisBinding(
        kind="operator",
        id=type(value).__name__,
    )
