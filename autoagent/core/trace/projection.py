from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID

from autoagent.core.trace.models import (
    ProjectedEdge,
    ProjectedNode,
    ProjectedNodeExecution,
    RuntimeProjection,
)
from autoagent.core.runtime import RuntimeEvent


def project_runtime_events(
    invocation_id: UUID,
    events: Iterable[RuntimeEvent],
    *,
    through_sequence: int | None = None,
    base_projection: RuntimeProjection | None = None,
) -> RuntimeProjection:
    """Reduce immutable events into graph state at one deterministic cursor."""

    if base_projection is not None and base_projection.invocation_id != invocation_id:
        raise ValueError("Projection checkpoint belongs to another Invocation.")
    invocation_state = (
        base_projection.invocation_state if base_projection is not None else "created"
    )
    applied_sequence = (
        base_projection.through_sequence if base_projection is not None else 0
    )
    executions = (
        dict(base_projection.node_executions) if base_projection is not None else {}
    )
    edge_values = dict(base_projection.edges) if base_projection is not None else {}
    operator_states = (
        dict(base_projection.operator_states) if base_projection is not None else {}
    )

    ordered = sorted(events, key=lambda item: item.sequence)
    for event in ordered:
        if event.sequence <= applied_sequence:
            continue
        if through_sequence is not None and event.sequence > through_sequence:
            break
        applied_sequence = event.sequence
        if event.type == "invocation.state_changed":
            invocation_state = str(event.payload.get("to", invocation_state))
            continue
        if event.type == "node.execution_created" and event.entity_id and event.node_id:
            executions[event.entity_id] = ProjectedNodeExecution(
                execution_id=event.entity_id,
                node_id=event.node_id,
                sequence=int(event.payload.get("sequence", 0)),
                state=str(event.payload.get("state", "created")),
            )
            continue
        if event.type == "node.state_changed" and event.entity_id and event.node_id:
            previous = executions.get(event.entity_id)
            executions[event.entity_id] = ProjectedNodeExecution(
                execution_id=event.entity_id,
                node_id=event.node_id,
                sequence=previous.sequence if previous is not None else 0,
                state=str(event.payload.get("to", "created")),
                input=(
                    event.payload.get("input")
                    if event.payload.get("to") == "running"
                    else (previous.input if previous is not None else None)
                ),
                output=(
                    event.payload.get("output")
                    if event.payload.get("to") == "completed"
                    else (previous.output if previous is not None else None)
                ),
                error=event.payload.get("error"),
            )
            continue
        if event.type in {"operator.call_started", "operator.call_finished"}:
            if event.entity_id:
                operator_states[event.entity_id] = (
                    "running"
                    if event.type == "operator.call_started"
                    else str(event.payload.get("state", "completed"))
                )
            continue
        if event.type == "edge.evaluated" and event.edge_id:
            previous_edge = edge_values.get(event.edge_id)
            edge_values[event.edge_id] = ProjectedEdge(
                edge_id=event.edge_id,
                state=str(event.payload.get("state", "evaluated")),
                selected=bool(event.payload.get("selected", False)),
                evaluation_count=(
                    previous_edge.evaluation_count + 1
                    if previous_edge is not None
                    else 1
                ),
                selected_count=(
                    (previous_edge.selected_count if previous_edge is not None else 0)
                    + (1 if bool(event.payload.get("selected", False)) else 0)
                ),
                skipped_count=(
                    (previous_edge.skipped_count if previous_edge is not None else 0)
                    + (1 if event.payload.get("state") == "skipped" else 0)
                ),
                failed_count=(
                    (previous_edge.failed_count if previous_edge is not None else 0)
                    + (1 if event.payload.get("state") == "failed" else 0)
                ),
                source_execution_id=event.payload.get("node_execution_id"),
                target_node_id=event.payload.get("target_node_id"),
            )

    nodes: dict[str, ProjectedNode] = {}
    by_node: dict[str, list[ProjectedNodeExecution]] = {}
    for execution in executions.values():
        by_node.setdefault(execution.node_id, []).append(execution)
    for node_id, node_executions in by_node.items():
        latest = max(
            node_executions,
            key=lambda value: (value.sequence, value.execution_id),
        )
        nodes[node_id] = ProjectedNode(
            node_id=node_id,
            state=latest.state,
            latest_execution_id=latest.execution_id,
            execution_count=len(node_executions),
        )

    return RuntimeProjection(
        invocation_id=invocation_id,
        through_sequence=applied_sequence,
        invocation_state=invocation_state,
        node_executions=executions,
        nodes=nodes,
        edges=edge_values,
        operator_states=operator_states,
    )
