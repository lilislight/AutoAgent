from __future__ import annotations

from dataclasses import dataclass

from autoagent.compiler.diagnostic import Diagnostic
from autoagent.compiler.id_generation import generate_edge_id
from autoagent.workflow import Edge, Node, Workflow


@dataclass
class _ExpandedWorkflow:
    """Recursive source expansion before ordinary Workflow compilation."""

    nodes: list[Node]
    edges: list[Edge]
    input_endpoints: dict[str, str]
    output_endpoints: dict[str, str]
    entry_by_source_id: dict[str, str]
    exit_by_source_id: dict[str, str]


def expand_child_workflows(
    workflow: Workflow,
    diagnostics: list[Diagnostic],
) -> Workflow:
    """Flatten directly bound child Workflows into ordinary source nodes/edges.

    Expansion happens before node compilation, graph indexes, loop analysis, and
    Policy validation. Scheduler and Executor therefore never need special child
    Workflow behavior.
    """

    expanded = _expand(
        workflow,
        path=(),
        stack=(),
        diagnostics=diagnostics,
    )
    return workflow.model_copy(
        update={
            "nodes": expanded.nodes,
            "edges": expanded.edges,
        }
    )


def _expand(
    workflow: Workflow,
    *,
    path: tuple[str, ...],
    stack: tuple[int, ...],
    diagnostics: list[Diagnostic],
) -> _ExpandedWorkflow:
    if id(workflow) in stack:
        diagnostics.append(
            Diagnostic(
                code="SUBWORKFLOW_RECURSION",
                severity="error",
                message="A child Workflow cannot contain itself recursively.",
                subject=_path_id(path) or workflow.id,
            )
        )
        return _empty_expansion()

    next_stack = (*stack, id(workflow))
    nodes: list[Node] = []
    edges: list[Edge] = []
    own_nodes: list[Node] = []
    own_edges: list[Edge] = []
    input_endpoints: dict[str, str] = {}
    output_endpoints: dict[str, str] = {}
    object_input_endpoints: dict[int, str] = {}
    object_output_endpoints: dict[int, str] = {}
    source_nodes_by_id = {node.id: node for node in workflow.nodes}
    source_node_object_ids = {id(node) for node in workflow.nodes}
    used_local_edge_ids = {
        edge.id for edge in workflow.edges if edge.id is not None
    }

    for source_node in workflow.nodes:
        if isinstance(source_node.capability, Workflow):
            selected = _expand_child_node(
                source_node,
                path=path,
                stack=next_stack,
                diagnostics=diagnostics,
            )
            nodes.extend(selected.nodes)
            edges.extend(selected.edges)
            if selected.entry_node_id is not None:
                input_endpoints.setdefault(source_node.id, selected.entry_node_id)
                object_input_endpoints[id(source_node)] = selected.entry_node_id
            if selected.exit_node_id is not None:
                output_endpoints.setdefault(source_node.id, selected.exit_node_id)
                object_output_endpoints[id(source_node)] = selected.exit_node_id
            continue

        if (
            source_node.child_entry_node_id is not None
            or source_node.child_exit_node_id is not None
        ):
            diagnostics.append(
                Diagnostic(
                    code="SUBWORKFLOW_SELECTOR_INVALID",
                    severity="error",
                    message=(
                        "child_entry_node_id and child_exit_node_id require a "
                        "Workflow capability."
                    ),
                    subject=_qualify(path, source_node.id),
                )
            )

        expanded_id = _qualify(path, source_node.id)
        cloned = source_node.model_copy(update={"id": expanded_id})
        cloned._local_id = source_node.id
        cloned._workflow_path = path
        nodes.append(cloned)
        own_nodes.append(cloned)
        input_endpoints.setdefault(source_node.id, expanded_id)
        output_endpoints.setdefault(source_node.id, expanded_id)
        object_input_endpoints[id(source_node)] = expanded_id
        object_output_endpoints[id(source_node)] = expanded_id

    for source_edge in workflow.edges:
        local_from = _node_ref_id(source_edge.from_node)
        local_to = _node_ref_id(source_edge.to_node)
        local_edge_id = source_edge.id or generate_edge_id(
            f"edge_{local_from}_{local_to}",
            used_local_edge_ids,
        )
        used_local_edge_ids.add(local_edge_id)
        target_source_node = _source_node(
            source_edge.to_node,
            by_id=source_nodes_by_id,
            object_ids=source_node_object_ids,
        )
        if (
            target_source_node is not None
            and isinstance(target_source_node.capability, Workflow)
            and source_edge.policy is not None
            and source_edge.policy.map is not None
        ):
            diagnostics.append(
                Diagnostic(
                    code="SUBWORKFLOW_MAP_UNSUPPORTED",
                    severity="error",
                    message=(
                        "MapPolicy cannot target a child Workflow placeholder in "
                        "V1 because that would map only its expanded entry node, "
                        "not one complete child execution per item."
                    ),
                    subject=_qualify(path, local_edge_id),
                )
            )
        expanded_from = _resolve_endpoint(
            source_edge.from_node,
            by_id=output_endpoints,
            by_object=object_output_endpoints,
        )
        expanded_to = _resolve_endpoint(
            source_edge.to_node,
            by_id=input_endpoints,
            by_object=object_input_endpoints,
        )
        expanded_edge_id = _qualify(path, local_edge_id)
        cloned = source_edge.model_copy(
            update={
                "id": expanded_edge_id,
                "from_node": expanded_from,
                "to_node": expanded_to,
            }
        )
        cloned._local_id = local_edge_id
        cloned._local_from_node = local_from
        cloned._local_to_node = local_to
        cloned._workflow_path = path
        edges.append(cloned)
        own_edges.append(cloned)

    # Hooks authored in this Workflow use its local top-level ids. A child
    # placeholder resolves to the selected child exit because that is the
    # logical output exposed by the expanded child.
    scope_node_ids = {
        local_id: expanded_id
        for local_id, expanded_id in output_endpoints.items()
        if local_id != expanded_id
    }
    for node in own_nodes:
        node._scope_node_ids = scope_node_ids
    for edge in own_edges:
        edge._scope_node_ids = scope_node_ids

    known_ids = {node.id for node in nodes}
    incoming = {node_id: 0 for node_id in known_ids}
    outgoing = {node_id: 0 for node_id in known_ids}
    for edge in edges:
        if isinstance(edge.from_node, str) and edge.from_node in outgoing:
            outgoing[edge.from_node] += 1
        if isinstance(edge.to_node, str) and edge.to_node in incoming:
            incoming[edge.to_node] += 1

    if path:
        for node in nodes:
            if node.entry and incoming.get(node.id, 0) > 0:
                diagnostics.append(
                    Diagnostic(
                        code="WF_ENTRY_HAS_INCOMING_EDGE",
                        severity="error",
                        message="An explicit entry node cannot have incoming edges.",
                        subject=node.id,
                    )
                )

    entry_ids = {node_id for node_id, count in incoming.items() if count == 0}
    exit_ids = {node_id for node_id, count in outgoing.items() if count == 0}
    return _ExpandedWorkflow(
        nodes=nodes,
        edges=edges,
        input_endpoints=input_endpoints,
        output_endpoints=output_endpoints,
        entry_by_source_id={
            source_id: expanded_id
            for source_id, expanded_id in input_endpoints.items()
            if expanded_id in entry_ids
        },
        exit_by_source_id={
            source_id: expanded_id
            for source_id, expanded_id in output_endpoints.items()
            if expanded_id in exit_ids
        },
    )


@dataclass
class _SelectedChild:
    nodes: list[Node]
    edges: list[Edge]
    entry_node_id: str | None
    exit_node_id: str | None


def _expand_child_node(
    source_node: Node,
    *,
    path: tuple[str, ...],
    stack: tuple[int, ...],
    diagnostics: list[Diagnostic],
) -> _SelectedChild:
    child = source_node.capability
    assert isinstance(child, Workflow)
    child_path = (*path, source_node.id)
    subject = _path_id(child_path)

    unsupported_fields = []
    if source_node.input_mapping is not None:
        unsupported_fields.append("input_mapping")
    if source_node.output_binding is not None:
        unsupported_fields.append("output_binding")
    if source_node.policy is not None:
        unsupported_fields.append("policy")
    if unsupported_fields:
        diagnostics.append(
            Diagnostic(
                code="SUBWORKFLOW_NODE_BEHAVIOR_UNSUPPORTED",
                severity="error",
                message=(
                    "A child Workflow placeholder cannot define input_mapping, "
                    "output_binding, or NodePolicy in V1; define behavior on the "
                    "child boundary nodes."
                ),
                subject=subject,
                metadata={"fields": unsupported_fields},
            )
        )

    expanded = _expand(
        child,
        path=child_path,
        stack=stack,
        diagnostics=diagnostics,
    )
    entry_node_id = _select_boundary(
        kind="entry",
        selector=source_node.child_entry_node_id,
        candidates=expanded.entry_by_source_id,
        subject=subject,
        diagnostics=diagnostics,
    )
    if entry_node_id is None:
        return _SelectedChild([], [], None, None)

    reachable = _reachable_node_ids(
        entry_node_id,
        nodes=expanded.nodes,
        edges=expanded.edges,
    )
    selected_nodes = [node for node in expanded.nodes if node.id in reachable]
    selected_edges = [
        edge
        for edge in expanded.edges
        if isinstance(edge.from_node, str)
        and edge.from_node in reachable
    ]
    reachable_exits = {
        source_id: expanded_id
        for source_id, expanded_id in expanded.exit_by_source_id.items()
        if expanded_id in reachable
    }
    exit_node_id = _select_boundary(
        kind="exit",
        selector=source_node.child_exit_node_id,
        candidates=reachable_exits,
        subject=subject,
        diagnostics=diagnostics,
    )
    if exit_node_id is None:
        return _SelectedChild([], [], None, None)

    # Child entry markers describe the child in isolation. Once embedded, only
    # the placeholder's own entry assertion may become a root Workflow entry.
    selected_ids = {node.id for node in selected_nodes}
    for node in selected_nodes:
        node.entry = None
        node._scope_node_ids = {
            local_id: expanded_id
            for local_id, expanded_id in node._scope_node_ids.items()
            if expanded_id in selected_ids
        }
    selected_entry = next(node for node in selected_nodes if node.id == entry_node_id)
    selected_entry.entry = source_node.entry
    for edge in selected_edges:
        edge._scope_node_ids = {
            local_id: expanded_id
            for local_id, expanded_id in edge._scope_node_ids.items()
            if expanded_id in selected_ids
        }

    return _SelectedChild(
        nodes=selected_nodes,
        edges=selected_edges,
        entry_node_id=entry_node_id,
        exit_node_id=exit_node_id,
    )


def _select_boundary(
    *,
    kind: str,
    selector: str | None,
    candidates: dict[str, str],
    subject: str,
    diagnostics: list[Diagnostic],
) -> str | None:
    if selector is not None:
        selected = candidates.get(selector)
        if selected is not None:
            return selected
        diagnostics.append(
            Diagnostic(
                code=f"SUBWORKFLOW_{kind.upper()}_INVALID",
                severity="error",
                message=(
                    f"Selected child Workflow {kind} is not a valid {kind} "
                    "for this embedding."
                ),
                subject=subject,
                metadata={
                    "selected_node_id": selector,
                    "candidate_node_ids": sorted(candidates),
                },
            )
        )
        return None

    if len(candidates) == 1:
        return next(iter(candidates.values()))
    code = "REQUIRED" if candidates else "MISSING"
    diagnostics.append(
        Diagnostic(
            code=f"SUBWORKFLOW_{kind.upper()}_{code}",
            severity="error",
            message=(
                f"Child Workflow has {len(candidates)} {kind} candidates; "
                f"set child_{kind}_node_id explicitly."
            ),
            subject=subject,
            metadata={"candidate_node_ids": sorted(candidates)},
        )
    )
    return None


def _reachable_node_ids(
    entry_node_id: str,
    *,
    nodes: list[Node],
    edges: list[Edge],
) -> set[str]:
    successors: dict[str, list[str]] = {node.id: [] for node in nodes}
    for edge in edges:
        if (
            isinstance(edge.from_node, str)
            and isinstance(edge.to_node, str)
            and edge.from_node in successors
        ):
            successors[edge.from_node].append(edge.to_node)
    reachable: set[str] = set()
    pending = [entry_node_id]
    while pending:
        node_id = pending.pop()
        if node_id in reachable:
            continue
        reachable.add(node_id)
        pending.extend(successors.get(node_id, ()))
    return reachable


def _resolve_endpoint(
    ref: str | Node,
    *,
    by_id: dict[str, str],
    by_object: dict[int, str],
) -> str | Node:
    if isinstance(ref, Node):
        return by_object.get(id(ref), ref)
    return by_id.get(ref, ref)


def _node_ref_id(ref: str | Node) -> str:
    return ref.id if isinstance(ref, Node) else ref


def _source_node(
    ref: str | Node,
    *,
    by_id: dict[str, Node],
    object_ids: set[int],
) -> Node | None:
    if isinstance(ref, Node):
        return ref if id(ref) in object_ids else None
    return by_id.get(ref)


def _qualify(path: tuple[str, ...], object_id: str) -> str:
    return "/".join((*path, object_id)) if path else object_id


def _path_id(path: tuple[str, ...]) -> str:
    return "/".join(path)


def _empty_expansion() -> _ExpandedWorkflow:
    return _ExpandedWorkflow([], [], {}, {}, {}, {})
