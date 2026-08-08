"""Normative V2 graph and Loop analysis from ``workflow.md``."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace

from ..errors import WorkflowCompileError
from ..workflow import EdgeIR, LoopRegionIR


def _compile_error(code: str, message: str) -> WorkflowCompileError:
    return WorkflowCompileError(message, code=code)


def _dominators(
    node_ids: tuple[str, ...],
    predecessors: dict[str, set[str]],
    entries: tuple[str, ...],
) -> dict[str, set[str]]:
    all_nodes = set(node_ids)
    values = {
        node_id: ({node_id} if node_id in entries else set(all_nodes))
        for node_id in node_ids
    }
    changed = True
    while changed:
        changed = False
        for node_id in node_ids:
            if node_id in entries:
                continue
            incoming = predecessors[node_id]
            common = (
                set.intersection(*(values[source] for source in incoming))
                if incoming
                else set()
            )
            updated = {node_id, *common}
            if updated != values[node_id]:
                values[node_id] = updated
                changed = True
    return values


def _strongly_connected_components(
    node_ids: tuple[str, ...], outgoing: dict[str, tuple[str, ...]]
) -> tuple[tuple[str, ...], ...]:
    """Return SCCs with iterative Kosaraju passes.

    Workflow depth is user-controlled, so recursive DFS would make a long DAG
    fail compilation at Python's recursion limit even though it has no cycle.
    """

    order = {node_id: index for index, node_id in enumerate(node_ids)}
    visited: set[str] = set()
    finished: list[str] = []
    for root in node_ids:
        if root in visited:
            continue
        visited.add(root)
        stack: list[tuple[str, int]] = [(root, 0)]
        while stack:
            node_id, next_child = stack[-1]
            targets = outgoing[node_id]
            if next_child < len(targets):
                target = targets[next_child]
                stack[-1] = (node_id, next_child + 1)
                if target not in visited:
                    visited.add(target)
                    stack.append((target, 0))
                continue
            finished.append(node_id)
            stack.pop()

    reverse: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for source, targets in outgoing.items():
        for target in targets:
            reverse[target].append(source)

    assigned: set[str] = set()
    components: list[tuple[str, ...]] = []
    for root in reversed(finished):
        if root in assigned:
            continue
        assigned.add(root)
        members: list[str] = []
        pending = [root]
        while pending:
            node_id = pending.pop()
            members.append(node_id)
            for predecessor in reverse[node_id]:
                if predecessor not in assigned:
                    assigned.add(predecessor)
                    pending.append(predecessor)
        components.append(tuple(sorted(members, key=order.__getitem__)))
    return tuple(components)


def _natural_region(
    header: str,
    latch: str,
    predecessors: dict[str, set[str]],
) -> set[str]:
    if header == latch:
        # A self-Back owns only the Header. Walking the Header's predecessors
        # would incorrectly absorb its external Entry into the Loop body.
        return {header}
    members = {header, latch}
    pending = [latch]
    while pending:
        current = pending.pop()
        for predecessor in predecessors[current]:
            if predecessor in members:
                continue
            members.add(predecessor)
            if predecessor != header:
                pending.append(predecessor)
    return members


def analyze_loops(
    node_ids: tuple[str, ...],
    edges: tuple[EdgeIR, ...],
    entries: tuple[str, ...],
) -> tuple[LoopRegionIR, ...]:
    """Compile reducible cycles into one-Back-Edge Loop regions."""

    predecessors: dict[str, set[str]] = {node_id: set() for node_id in node_ids}
    outgoing_nodes: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    outgoing_edges: dict[str, list[EdgeIR]] = defaultdict(list)
    for edge in edges:
        predecessors[edge.target].add(edge.source)
        outgoing_nodes[edge.source].append(edge.target)
        outgoing_edges[edge.source].append(edge)
    dominators = _dominators(node_ids, predecessors, entries)

    raw: list[LoopRegionIR] = []
    for edge in edges:
        if edge.target not in dominators[edge.source]:
            continue
        header = edge.target
        members = _natural_region(header, edge.source, predecessors)
        illegal_entries = tuple(
            candidate.id
            for candidate in edges
            if candidate.target in members
            and candidate.source not in members
            and candidate.target != header
        )
        if illegal_entries:
            raise _compile_error(
                "LOOP_NON_HEADER_ENTRY",
                f"Loop headed by {header!r} has external Edges entering Body "
                f"Nodes: {', '.join(illegal_entries)}.",
            )
        entry_edges = tuple(
            candidate.id
            for candidate in edges
            if candidate.target == header and candidate.source not in members
        )
        exit_edges = tuple(
            candidate.id
            for candidate in edges
            if candidate.source in members and candidate.target not in members
        )
        if not exit_edges:
            raise _compile_error(
                "LOOP_WITHOUT_EXIT",
                f"Loop headed by {header!r} and Back Edge {edge.id!r} has no "
                "structural Exit Edge.",
            )
        raw.append(
            LoopRegionIR(
                id=f"loop:{header}:{edge.id}",
                header_node_id=header,
                node_ids=tuple(node_id for node_id in node_ids if node_id in members),
                entry_edge_ids=entry_edges,
                back_edge_ids=(edge.id,),
                exit_edge_ids=exit_edges,
            )
        )

    # Equal regions mean the topology supplied several independent Back Edges
    # for one logical body. V2 requires authors to merge before one Back Edge.
    for index, left in enumerate(raw):
        left_nodes = set(left.node_ids)
        for right in raw[index + 1 :]:
            right_nodes = set(right.node_ids)
            if left_nodes == right_nodes:
                raise _compile_error(
                    "LOOP_MULTIPLE_BACK_EDGES",
                    f"Back Edges {left.back_edge_ids[0]!r} and "
                    f"{right.back_edge_ids[0]!r} define the same Loop region; "
                    "merge their branches before one Back Edge.",
                )

    completed: list[LoopRegionIR] = []
    for region in raw:
        region_nodes = set(region.node_ids)
        parents = [
            candidate
            for candidate in raw
            if region_nodes < set(candidate.node_ids)
        ]
        parent = min(parents, key=lambda item: len(item.node_ids), default=None)
        completed.append(
            replace(
                region,
                parent_loop_region_id=parent.id if parent is not None else None,
            )
        )

    # A strict same-Header containment is a nested Loop only when execution
    # reaches the parent-only body through the child body. A direct Header
    # entry into the parent-only body makes the topology indistinguishable
    # from sibling Loops connected by a Body cross-Edge, which is forbidden.
    for child in completed:
        child_nodes = set(child.node_ids)
        for parent in completed:
            parent_nodes = set(parent.node_ids)
            if (
                child.id == parent.id
                or child.header_node_id != parent.header_node_id
                or not child_nodes < parent_nodes
            ):
                continue
            parent_only = parent_nodes - child_nodes
            bypasses = [
                edge.id
                for edge in edges
                if edge.source == child.header_node_id
                and edge.target in parent_only
            ]
            if bypasses:
                raise _compile_error(
                    "LOOP_REGION_OVERLAP",
                    f"Same-Header Loop regions {child.id!r} and {parent.id!r} "
                    "have direct Header entries that bypass the nested body: "
                    + ", ".join(bypasses),
                )

    # Regions must be laminar. Same-header siblings may share only their Header.
    for index, left in enumerate(completed):
        left_nodes = set(left.node_ids)
        for right in completed[index + 1 :]:
            right_nodes = set(right.node_ids)
            intersection = left_nodes & right_nodes
            if not intersection:
                continue
            if left_nodes < right_nodes or right_nodes < left_nodes:
                continue
            if (
                left.header_node_id == right.header_node_id
                and intersection == {left.header_node_id}
            ):
                left_body = left_nodes - intersection
                right_body = right_nodes - intersection
                crossing = [
                    edge.id
                    for edge in edges
                    if (edge.source in left_body and edge.target in right_body)
                    or (edge.source in right_body and edge.target in left_body)
                ]
                if not crossing:
                    continue
            raise _compile_error(
                "LOOP_REGION_OVERLAP",
                f"Loop regions {left.id!r} and {right.id!r} overlap without "
                "containment or a valid shared-Header sibling relationship.",
            )

    regions = tuple(
        sorted(completed, key=lambda item: (len(item.node_ids), item.id))
    )
    _validate_cyclic_components(
        node_ids,
        edges,
        tuple(tuple(values) for values in outgoing_nodes.values()),
        regions,
    )
    _validate_static_control(edges, regions)
    return regions


def _validate_cyclic_components(
    node_ids: tuple[str, ...],
    edges: tuple[EdgeIR, ...],
    outgoing_values: tuple[tuple[str, ...], ...],
    regions: tuple[LoopRegionIR, ...],
) -> None:
    outgoing = {
        node_id: outgoing_values[index] for index, node_id in enumerate(node_ids)
    }
    for component in _strongly_connected_components(node_ids, outgoing):
        members = set(component)
        cyclic = len(component) > 1 or any(
            edge.source == edge.target and edge.source in members for edge in edges
        )
        if not cyclic:
            continue
        external_entries = [
            edge
            for edge in edges
            if edge.source not in members and edge.target in members
        ]
        entry_targets = {edge.target for edge in external_entries}
        if len(entry_targets) > 1:
            raise _compile_error(
                "LOOP_NON_HEADER_ENTRY",
                "Cyclic region has external Edges entering non-header Nodes: "
                + ", ".join(edge.id for edge in external_entries),
            )
        if not any(
            edge.source in members and edge.target not in members for edge in edges
        ):
            raise _compile_error(
                "CYCLIC_REGION_WITHOUT_EXIT",
                "Cyclic region has no Edge leaving the SCC: "
                + ", ".join(component),
            )
        covered_nodes = set().union(
            *(
                set(region.node_ids)
                for region in regions
                if set(region.node_ids) <= members
            )
        ) if regions else set()
        uncovered_edges = [
            edge.id
            for edge in edges
            if edge.source in members
            and edge.target in members
            and not any(
                edge.source in region.node_ids and edge.target in region.node_ids
                for region in regions
            )
        ]
        if not members <= covered_nodes or uncovered_edges:
            details = (
                f"; uncovered Edges: {', '.join(uncovered_edges)}"
                if uncovered_edges
                else ""
            )
            raise _compile_error(
                "LOOP_IRREDUCIBLE",
                "Cyclic region cannot be represented as reducible Loop scopes: "
                + ", ".join(component)
                + details,
            )


def _validate_static_control(
    edges: tuple[EdgeIR, ...], regions: tuple[LoopRegionIR, ...]
) -> None:
    by_source: dict[str, list[EdgeIR]] = defaultdict(list)
    for edge in edges:
        if edge.condition is None:
            by_source[edge.source].append(edge)

    for source, unconditional in by_source.items():
        containing = [region for region in regions if source in region.node_ids]
        for region in containing:
            members = set(region.node_ids)
            inside = [edge.id for edge in unconditional if edge.target in members]
            outside = [edge.id for edge in unconditional if edge.target not in members]
            if inside and outside:
                raise _compile_error(
                    "LOOP_STATIC_CONTROL_CONFLICT",
                    f"Node {source!r} unconditionally selects Edges that both "
                    f"continue and exit Loop {region.id!r}: "
                    + ", ".join((*inside, *outside)),
                )

        exit_sets = {
            tuple(
                sorted(
                    region.id
                    for region in containing
                    if edge.target not in region.node_ids
                )
            )
            for edge in unconditional
            if any(edge.target not in region.node_ids for region in containing)
        }
        if len(exit_sets) > 1:
            raise _compile_error(
                "LOOP_STATIC_CONTROL_CONFLICT",
                f"Node {source!r} has unconditional Exit Edges that close "
                "different Loop scope sets.",
            )
