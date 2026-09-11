export interface Page<T> {
  items: T[];
  next_cursor: string | null;
  has_more: boolean;
}

export interface WorkflowSummary {
  workflow_id: string;
  workflow_version: string;
  workflow_revision_id: string;
  definition_hash: string;
  created_at_ns: string;
}

export interface WorkflowNode {
  id: string;
  executable?: { kind?: string; name?: string; id?: string } | string;
  execution_mode?: string;
  map?: unknown;
  stream?: unknown;
}

export interface WorkflowEdge {
  id?: string;
  source: string;
  target: string;
  on?: string;
  condition?: unknown;
}

export interface WorkflowSnapshot {
  schema_version: number;
  workflow_id: string;
  workflow_version: string;
  workflow_revision_id: string;
  definition_hash: string;
  definition: {
    nodes: WorkflowNode[];
    edges: WorkflowEdge[];
    entry_node_ids?: string[];
    exit_node_ids?: string[];
    loops?: unknown[];
    [key: string]: unknown;
  };
}

export interface SessionSummary {
  session_id: string;
  root_session_id: string;
  current_invocation_id: string | null;
  invocation_count: number;
  workflow_id: string | null;
  workflow_revision_id: string | null;
  status: string | null;
  parent_session_id: string | null;
  parent_invocation_id: string | null;
  creation_id: string | null;
  unit_index: number | null;
  created_at_ns: string;
  updated_at_ns: string;
}

export interface InvocationSummary {
  invocation_id: string;
  session_id: string;
  root_session_id: string;
  workflow_id: string;
  workflow_revision_id: string;
  entry_node_id: string;
  status: string;
  parent_session_id: string | null;
  parent_invocation_id: string | null;
  creation_id: string | null;
  unit_index: number | null;
  first_event_sequence: number;
  last_event_sequence: number;
  created_at_ns: string;
  updated_at_ns: string;
  ended_at_ns: string | null;
}

export interface ChildSessionSummary {
  session_id: string;
  root_session_id: string;
  parent_session_id: string;
  parent_invocation_id: string;
  creation_id: string;
  unit_index: number;
  parent_occurrence_id: string;
  mode: "await" | "spawn";
  planned_workflow_id: string;
  planned_workflow_revision_id: string;
  planned_invocation_id: string;
  planned_event_sequence: number;
  phase: "planned" | "opened" | "accepted" | "terminal";
  current_invocation_id: string | null;
  invocation_count: number;
  workflow_id: string | null;
  workflow_revision_id: string | null;
  status: string;
  created_at_ns: string | null;
  updated_at_ns: string | null;
}

export interface TraceEvent {
  schema_version: number;
  id: string;
  session_id: string;
  trace_sequence: number;
  kind: string;
  occurred_at_ns: string;
  invocation_id: string | null;
  state_version: number | null;
  subject_ids: Record<string, string>;
  status: string | null;
  error: Record<string, unknown> | null;
  metrics: unknown;
  attributes: Record<string, unknown>;
}

export interface TracePage {
  items: TraceEvent[];
  next_cursor: string | null;
  resume_cursor: string | null;
  resume_sequence: number;
  has_more: boolean;
  has_earlier: boolean;
}

export interface UserEvent {
  id: string;
  session_id: string;
  invocation_id: string;
  sequence: number;
  kind: string;
  payload: unknown;
  occurrence_id: string | null;
  occurred_at_ns: string;
}

export interface UserEventPage {
  items: UserEvent[];
  next_cursor: string | null;
  resume_cursor: string | null;
  resume_sequence: number;
  has_more: boolean;
  has_earlier: boolean;
}

export type RuntimeStateRecord = Record<string, unknown>;

export interface InvocationStateResponse {
  invocation_id: string;
  session_id: string;
  through_sequence: number;
  state: RuntimeStateRecord;
}
