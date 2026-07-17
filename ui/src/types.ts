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

export interface ObservationHealth {
  status: string;
  authentication_required: boolean;
  authenticated: boolean;
}

export interface WorkflowSummary {
  workflow_id: string;
  workflow_version: string | number | null;
  definition_hash: string;
  operator_manifest_hash: string;
  name: string | null;
  description: string | null;
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
  policy: Record<string, unknown> | null;
}

export interface WorkflowGraphView extends WorkflowSummary {
  nodes: WorkflowNodeView[];
  edges: WorkflowEdgeView[];
  entry_node_ids: string[];
  exit_node_ids: string[];
  loop_regions: Record<string, unknown>[];
}

export interface SessionSummary {
  id: string;
  namespace: string;
  workflow_id: string;
  session_key: string | null;
  current_invocation_id: string | null;
  invocation_count: number;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface InvocationSummary {
  id: string;
  workflow_id: string;
  workflow_version: string | number | null;
  definition_hash: string | null;
  operator_manifest_hash: string | null;
  entry_node_id: string;
  state: RuntimeState;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface OperatorCallView {
  id: string;
  operator_id: string;
  call_no: number;
  kind: string;
  item_index: number | null;
  replica_index: number | null;
  state: RuntimeState;
  input: unknown;
  output: unknown;
  error: Record<string, unknown> | null;
  resource_usage: Record<string, unknown>;
  started_at_ms: number | null;
  ended_at_ms: number | null;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface EdgeEvaluationView {
  id: string;
  edge_id: string;
  target_node_id: string;
  state: string;
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
}

export interface TimelineView {
  invocation_id: string;
  started_at_ms: number;
  ended_at_ms: number | null;
  spans: TimelineSpan[];
}

export interface RuntimeEvent {
  id: string;
  namespace: string;
  workflow_id: string;
  session_id: string;
  invocation_id: string;
  sequence: number;
  type: string;
  entity_type: string;
  entity_id: string | null;
  node_id: string | null;
  edge_id: string | null;
  occurred_at_ms: number;
  channel: "runtime" | "output";
  visibility: "internal" | "user";
  payload: Record<string, unknown>;
}

export interface ProjectedNodeExecution {
  execution_id: string;
  node_id: string;
  sequence: number;
  state: RuntimeState;
  input: unknown;
  output: unknown;
  error: Record<string, unknown> | null;
}

export interface ProjectedNode {
  node_id: string;
  state: RuntimeState;
  latest_execution_id: string;
  execution_count: number;
}

export interface ProjectedEdge {
  edge_id: string;
  state: string;
  selected: boolean;
  evaluation_count: number;
  source_execution_id: string | null;
  target_node_id: string | null;
}

export interface RuntimeProjection {
  invocation_id: string;
  through_sequence: number;
  invocation_state: RuntimeState;
  node_executions: Record<string, ProjectedNodeExecution>;
  nodes: Record<string, ProjectedNode>;
  edges: Record<string, ProjectedEdge>;
  operator_states: Record<string, RuntimeState>;
}

export interface RuntimeEventPage {
  events: RuntimeEvent[];
  next_after_sequence: number;
  previous_before_sequence: number | null;
  has_more: boolean;
}

export interface ObservationBootstrap {
  graph: WorkflowGraphView;
  session: SessionSummary;
  invocation: InvocationDetail;
  timeline: TimelineView;
  checkpoint: RuntimeProjection;
  events: RuntimeEvent[];
  projection: RuntimeProjection;
}

export type TraceSelection =
  | { type: "node"; id: string }
  | { type: "edge"; id: string }
  | { type: "node_execution"; id: string }
  | { type: "operator_call"; id: string }
  | null;
