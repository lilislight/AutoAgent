from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from autoagent.runtime import RuntimeEvent


class TraceModel(BaseModel):
    """Read-only API base; trace DTOs never expose runtime mutators."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class WorkflowSummary(TraceModel):
    workflow_id: str
    workflow_version: str | int | None
    definition_hash: str
    operator_manifest_hash: str
    name: str | None = None
    description: str | None = None


class WorkflowNodeView(TraceModel):
    id: str
    local_id: str | None = None
    workflow_path: tuple[str, ...] = ()
    name: str | None = None
    description: str | None = None
    capability: dict[str, Any]
    entry: bool
    exit: bool
    policy: dict[str, Any] | None = None
    input_plan: dict[str, Any] | None = None
    output_binding: dict[str, Any] | None = None
    input_contract: dict[str, Any]
    operator_output_contract: dict[str, Any]
    output_contract: dict[str, Any]


class WorkflowEdgeView(TraceModel):
    id: str
    local_id: str | None = None
    workflow_path: tuple[str, ...] = ()
    from_node: str
    to_node: str
    order: int
    condition: Any | None = None
    policy: dict[str, Any] | None = None


class WorkflowGroupView(TraceModel):
    """Sub-workflow visual group derived from expanded IR workflow_path values."""

    id: str
    parent_group_id: str | None = None
    label: str
    workflow_path: tuple[str, ...]
    node_ids: tuple[str, ...]
    direct_node_ids: tuple[str, ...]
    entry_node_ids: tuple[str, ...]
    exit_node_ids: tuple[str, ...]


class WorkflowGraphView(TraceModel):
    workflow_id: str
    workflow_version: str | int | None
    definition_hash: str
    operator_manifest_hash: str
    name: str | None = None
    description: str | None = None
    nodes: tuple[WorkflowNodeView, ...]
    edges: tuple[WorkflowEdgeView, ...]
    groups: tuple[WorkflowGroupView, ...] = ()
    operator_manifests: tuple[dict[str, Any], ...] = ()
    entry_node_ids: tuple[str, ...]
    exit_node_ids: tuple[str, ...]
    loop_regions: tuple[dict[str, Any], ...] = ()


class SessionSummary(TraceModel):
    id: UUID
    namespace: str
    workflow_id: str
    session_key: str | None
    current_invocation_id: UUID | None
    invocation_count: int
    created_at_ms: int
    updated_at_ms: int


class InvocationSummary(TraceModel):
    id: UUID
    workflow_id: str
    workflow_version: str | int | None
    definition_hash: str | None
    operator_manifest_hash: str | None
    entry_node_id: str
    state: str
    created_at_ms: int
    updated_at_ms: int


class OperatorCallView(TraceModel):
    id: UUID
    operator_id: str
    call_no: int
    kind: str
    item_index: int | None
    replica_index: int | None
    state: str
    input: Any | None = None
    output: Any | None = None
    error: dict[str, Any] | None = None
    resource_usage: dict[str, Any]
    started_at_ms: int | None
    ended_at_ms: int | None
    created_at_ms: int
    updated_at_ms: int


class EdgeEvaluationView(TraceModel):
    id: UUID
    edge_id: str
    source_execution_id: UUID
    source_node_id: str
    target_node_id: str
    state: str
    selected: bool
    reason: str | None
    created_at_ms: int


class NodeExecutionView(TraceModel):
    id: UUID
    node_id: str
    sequence: int
    state: str
    input: Any | None = None
    output: Any | None = None
    error: dict[str, Any] | None = None
    incoming_activations: tuple[dict[str, Any], ...]
    edge_evaluations: tuple[EdgeEvaluationView, ...]
    operator_calls: tuple[OperatorCallView, ...]
    resource_usage: dict[str, Any]
    started_at_ms: int | None
    ended_at_ms: int | None
    created_at_ms: int
    updated_at_ms: int


class InvocationDetail(InvocationSummary):
    input: dict[str, Any]
    context: dict[str, Any]
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    node_executions: tuple[NodeExecutionView, ...]


TimelineSpanKind = Literal["node_execution", "operator_call"]


class TimelineSpan(TraceModel):
    id: str
    kind: TimelineSpanKind
    parent_id: str | None = None
    node_id: str
    label: str
    state: str
    sequence: int
    started_at_ms: int
    ended_at_ms: int | None
    duration_ms: int | None = Field(default=None, ge=0)


class TimelineView(TraceModel):
    invocation_id: UUID
    started_at_ms: int
    ended_at_ms: int | None
    spans: tuple[TimelineSpan, ...]


class ProjectedNodeExecution(TraceModel):
    execution_id: str
    node_id: str
    sequence: int
    state: str
    input: Any | None = None
    output: Any | None = None
    error: dict[str, Any] | None = None


class ProjectedNode(TraceModel):
    node_id: str
    state: str
    latest_execution_id: str
    execution_count: int


class ProjectedEdge(TraceModel):
    edge_id: str
    state: str
    selected: bool
    evaluation_count: int
    selected_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    source_execution_id: str | None = None
    target_node_id: str | None = None


class RuntimeProjection(TraceModel):
    invocation_id: UUID
    through_sequence: int
    invocation_state: str
    node_executions: dict[str, ProjectedNodeExecution]
    nodes: dict[str, ProjectedNode]
    edges: dict[str, ProjectedEdge]
    operator_states: dict[str, str]


class RuntimeEventPage(TraceModel):
    """One forward event page and the cursor for requesting the next page."""

    events: tuple[RuntimeEvent, ...]
    next_after_sequence: int
    previous_before_sequence: int | None
    has_more: bool


class TraceBootstrap(TraceModel):
    graph: WorkflowGraphView
    session: SessionSummary
    invocation: InvocationDetail
    timeline: TimelineView
    checkpoint: RuntimeProjection
    events: tuple[RuntimeEvent, ...]
    projection: RuntimeProjection
