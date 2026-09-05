export type RuntimeState =
  | "created"
  | "ready"
  | "running"
  | "waiting"
  | "completed"
  | "failed"
  | "cancelled"
  | "interrupted"
  | "skipped"
  | string;

export type EdgeRuntimeState =
  | "selected"
  | "skipped"
  | "failed"
  | "pending"
  | string;

export interface ServerHealth {
  status: string;
  execution_enabled: boolean;
  authentication_required: boolean;
  authenticated: boolean;
}

export interface RuntimeStatus {
  service: {
    status: string;
    started_at_ms: number;
  };
  execution: {
    enabled: boolean;
    accepting_invocations: boolean;
    refusal_reason: string | null;
  };
  store: {
    kind: "memory" | "durable" | string;
  };
  persistence: {
    enabled: boolean;
    backend_kind: string | null;
    worker_state: string;
    health: "memory_only" | "healthy" | "retrying" | "unavailable" | string;
    pending_count: number;
    pending_bytes: number;
    low_watermark_bytes: number;
    high_watermark_bytes: number;
    hard_watermark_bytes: number;
    pressure: "normal" | "high" | "hard" | string;
    last_error: string | null;
    changed_at_ms: number | null;
    last_success_at_ms: number | null;
  };
}

export interface InvocationSubmitResponse {
  workflow_id: string;
  workflow_revision_id: string;
  session_id: string;
  session_key: string;
  invocation_id: string;
  state: RuntimeState;
}

export interface InvocationResumeResponse {
  workflow_id: string;
  workflow_revision_id: string;
  session_id: string;
  session_key: string;
  invocation_id: string;
  state: RuntimeState;
}

export interface InvocationCancelResponse {
  invocation_id: string;
  state: RuntimeState;
}

export interface WorkflowSummary {
  workflow_id: string;
  workflow_version: string | number | null;
  revision_id: string;
  definition_hash: string;
  name: string | null;
  description: string | null;
  registered?: boolean;
  created_at_ms?: number;
  updated_at_ms?: number;
}

export interface WorkflowNodeView {
  id: string;
  local_id?: string | null;
  workflow_path?: string[];
  name: string | null;
  description: string | null;
  capability: Record<string, unknown>;
  entry: boolean;
  exit: boolean;
  policy: Record<string, unknown> | null;
  input_mapping: Record<string, unknown> | null;
  output_binding: Record<string, unknown> | null;
  input_contract: Record<string, unknown>;
  operator_output_contract: Record<string, unknown>;
  output_contract: Record<string, unknown>;
}

export interface WorkflowEdgeView {
  id: string;
  local_id?: string | null;
  workflow_path?: string[];
  from_node: string;
  to_node: string;
  order: number;
  condition: unknown;
}

export interface WorkflowGroupView {
  id: string;
  parent_group_id: string | null;
  label: string;
  workflow_path: string[];
  node_ids: string[];
  direct_node_ids: string[];
  edge_ids?: string[];
  direct_edge_ids?: string[];
  entry_node_ids: string[];
  exit_node_ids: string[];
}

export interface WorkflowGraphView extends WorkflowSummary {
  nodes: WorkflowNodeView[];
  edges: WorkflowEdgeView[];
  groups: WorkflowGroupView[];
  entry_node_ids: string[];
  exit_node_ids: string[];
  loop_regions: Record<string, unknown>[];
}

export interface SessionSummary {
  id: string;
  workflow_id: string;
  workflow_revision_id: string;
  session_key: string | null;
  current_invocation_id: string | null;
  current_invocation_state?: RuntimeState | null;
  invocation_count: number;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface InvocationSummary {
  id: string;
  session_id?: string;
  workflow_id: string;
  workflow_revision_id: string;
  workflow_version: string | number | null;
  definition_hash: string | null;
  entry_node_id: string;
  state: RuntimeState;
  execution_mode?: string;
  event_mode?: "minimal" | "standard" | "full";
  live_sequence?: number;
  durable_sequence?: number;
  persistence_status?: string;
  user_event_persistence_status?: string;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface InvocationRecord extends InvocationSummary {
  input: Record<string, unknown>;
  result: Record<string, unknown> | null;
  error: Record<string, unknown> | null;
}

export interface OperatorCallView {
  id: string;
  operator_id: string;
  call_no: number;
  kind: string;
  reason?: string | null;
  unit_index: number | null;
  unit_attempt_no: number;
  state: RuntimeState;
  input: unknown;
  output: unknown;
  error: Record<string, unknown> | null;
  resource_usage: Record<string, unknown>;
  started_at_ms: number | null;
  ended_at_ms: number | null;
  created_at_ms: number;
  updated_at_ms: number;
  streaming?: boolean;
  stream_chunk_count?: number;
}

export interface EdgeEvaluationView {
  id: string;
  edge_id: string;
  source_execution_id: string;
  source_node_id: string;
  target_node_id: string;
  state: EdgeRuntimeState;
  selected: boolean;
  reason: string | null;
  created_at_ms: number;
}

export interface NodeExecutionView {
  id: string;
  node_id: string;
  sequence: number;
  state: RuntimeState;
  input: unknown;
  output: unknown;
  error: Record<string, unknown> | null;
  execution_scope: Record<string, unknown>[];
  incoming_activations: Record<string, unknown>[];
  edge_evaluations: EdgeEvaluationView[];
  operator_calls: OperatorCallView[];
  resource_usage: Record<string, unknown>;
  started_at_ms: number | null;
  ended_at_ms: number | null;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface InvocationDetail extends InvocationSummary {
  input: Record<string, unknown>;
  context: Record<string, unknown>;
  result: Record<string, unknown> | null;
  error: Record<string, unknown> | null;
  node_executions: NodeExecutionView[];
}

export interface TimelineSpan {
  id: string;
  kind: "node_execution" | "operator_call";
  parent_id: string | null;
  node_id: string;
  label: string;
  state: RuntimeState;
  sequence: number;
  started_at_ms: number;
  ended_at_ms: number | null;
  duration_ms: number | null;
  omitted_child_count?: number;
}

export interface TimelineView {
  invocation_id: string;
  started_at_ms: number;
  ended_at_ms: number | null;
  spans: TimelineSpan[];
}

export interface RuntimeEvent {
  id: string;
  invocation_id: string;
  sequence: number;
  schema_version: number;
  event_type: string;
  event_name: string;
  subject_type: string;
  subject_id: string;
  elapsed_ns: number | null;
  status: string | null;
  timing: Record<string, number>;
  has_input: boolean;
  has_output: boolean;
  has_operations?: boolean;
  input?: unknown;
  output?: unknown;
  operations?: Array<Record<string, unknown>> | null;
  occurred_at_ms: number;
  payload: Record<string, unknown>;
}

export interface UserEvent {
  id: string;
  invocation_id: string;
  sequence: number;
  schema_version: number;
  type: string;
  data: unknown;
  node_id: string;
  workflow_path: string[];
  node_execution_id: string;
  operator_call_id: string | null;
  occurred_at_ms: number;
}

export interface UserEventPage {
  items: UserEvent[];
  last_sequence: number | null;
  has_later: boolean;
  live_sequence: number;
}

export interface ProjectedNodeExecution {
  execution_id: string;
  node_id: string;
  sequence: number;
  first_event_sequence?: number;
  state: RuntimeState;
  input: unknown;
  output: unknown;
  error: Record<string, unknown> | null;
  started_at_ms?: number | null;
  ended_at_ms?: number | null;
  elapsed_ns?: number | null;
  timing?: Record<string, number>;
  operator_call_count?: number;
  failed_operator_call_count?: number;
  retry_count?: number;
  fallback_count?: number;
  timeout_count?: number;
  streaming_call_count?: number;
  stream_chunk_count?: number;
  parallel_summary?: Record<string, unknown> | null;
  operator_calls?: ProjectedOperatorCall[];
}

export interface ProjectedOperatorCall {
  id: string;
  event_sequence: number;
  node_execution_id: string;
  operator_id: string;
  kind: string;
  call_no: number;
  unit_index: number | null;
  unit_attempt_no: number;
  reason: string | null;
  state: RuntimeState;
  error: Record<string, unknown> | null;
  started_at_ms: number | null;
  occurred_at_ms: number;
  elapsed_ns: number | null;
  timing: Record<string, number>;
  streaming: boolean;
  stream_chunk_count: number;
}

export interface ProjectedNode {
  node_id: string;
  state: RuntimeState;
  latest_occurrence_state?: RuntimeState;
  latest_occurrence_sequence?: number;
  latest_skipped_instance_key?: string | null;
  latest_execution_id: string | null;
  execution_count: number;
  skipped_count?: number;
  latest_error?: unknown;
  latest_elapsed_ns?: number | null;
  latest_timing?: Record<string, number>;
  operator_call_count?: number;
  failed_operator_call_count?: number;
  retry_count?: number;
  fallback_count?: number;
  timeout_count?: number;
  parallel_call_count?: number;
  streaming_call_count?: number;
  stream_chunk_count?: number;
  latest_operator_kind?: string | null;
}

export interface ProjectedEdge {
  edge_id: string;
  state: EdgeRuntimeState;
  latest_state?: EdgeRuntimeState;
  latest_evaluation_sequence?: number;
  selected: boolean;
  latest_selected?: boolean;
  evaluation_count: number;
  selected_count: number;
  skipped_count: number;
  failed_count: number;
  source_execution_id: string | null;
  target_node_id: string | null;
}

export interface RuntimeProjection {
  schema_version?: number;
  invocation_id: string;
  through_sequence: number;
  invocation_state: RuntimeState;
  node_executions: Record<string, ProjectedNodeExecution>;
  nodes: Record<string, ProjectedNode>;
  edges: Record<string, ProjectedEdge>;
  active_waits?: Record<string, {
    wait_key: string;
    created_at_ms: number;
    node_execution_id?: string;
    node_id?: string;
    wait_type?: string | null;
    payload?: Record<string, unknown>;
  }>;
  latest_phase?: Record<string, unknown> | null;
}

export interface RuntimeEventPage {
  events: RuntimeEvent[];
  next_after_sequence: number;
  previous_before_sequence: number | null;
  has_more: boolean;
  has_later?: boolean;
  live_sequence: number;
  invocation_state: RuntimeState;
}

export interface TraceBootstrap {
  graph: WorkflowGraphView;
  session: SessionSummary;
  invocation: InvocationDetail;
  timeline: TimelineView;
  checkpoint: RuntimeProjection;
  events: RuntimeEvent[];
  projection: RuntimeProjection;
  capabilities: {
    has_events: boolean;
    has_graph_trace: boolean;
    has_internal_phases: boolean;
    has_historical_runtime_state: boolean;
    fork_available: boolean;
    design_available: boolean;
  };
  has_more_events: boolean;
}

export type TraceSelection =
  | { type: "invocation"; id: string }
  | { type: "node"; id: string }
  | { type: "edge"; id: string }
  | { type: "group"; id: string }
  | { type: "event"; id: string; sequence: number }
  | { type: "node_execution"; id: string }
  | { type: "operator_call"; id: string }
  | null;
