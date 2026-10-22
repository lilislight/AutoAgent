export interface WorkflowSummary {
  workflow_id: string;
  workflow_version: string;
  revision_id: string;
  runnable: boolean;
  definition_hash: string | null;
  registered_at_us: number | null;
}

export interface WorkflowPage {
  items: WorkflowSummary[];
  next_cursor: string | null;
  frontier: number | null;
}

export interface WorkflowCatalogEntry {
  workflow_id: string;
  workflow_version: string;
  revision_id: string;
  current_revision_id: string | null;
  matched_revision_id: string | null;
}

export interface WorkflowCatalogPage {
  items: WorkflowCatalogEntry[];
  next_cursor: string | null;
}

export interface GraphNode {
  id: string;
  executable: { kind: string; id: string };
  mapped: boolean;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  on: "complete" | "error";
  conditional: boolean;
}

export interface GraphDefinition {
  nodes: GraphNode[];
  edges: GraphEdge[];
  entry_node_ids: string[];
  exit_node_ids: string[];
}

export interface WorkflowGraphData {
  workflow_id: string;
  workflow_version: string;
  workflow_revision_id: string;
  definition_hash: string;
  schema_version: number;
  runnable: boolean;
  registered_at_us?: number | null;
  definition: GraphDefinition;
}

export interface WorkflowRunSpec {
  workflow_id: string; revision_id: string; registered: boolean; current_revision_id: string | null;
  entries: { node_id: string; input_mode: 'raw_json' | 'contract'; input_schema: unknown }[];
  can_run: boolean; code: string | null; message: string | null;
  session: { exists: boolean; available: boolean; code: string | null; message: string | null } | null;
}
export interface RunWorkflowInput {
  workflow_id: string; revision_id: string; entry_node_id: string; input: unknown;
  session_mode: 'new' | 'existing'; session_id?: string; session_context?: Record<string, unknown>;
}
export interface AcceptedExecution {
  accepted: true; workflow_id: string; revision_id: string; invocation_id: string; session_id: string;
}

export interface PendingResponse { invocation_id:string; wait_id:string; occurrence_id:string; created_at_us:number; node_id?:string|null; store_order?:number }
export type ExecutionActionKind='pause'|'recover'|'cancel'|'respond'|'signal';
export interface ExecutionControls {
  invocation_id:string; root_invocation_id:string; root_session_id:string; status:string; control_state:string;
  recovery_reason:'paused'|'interrupted'|'failed'|'cancelled'|null; expected_version:number; recovery_count:number; endpoints:string[]; pending_responses:PendingResponse[]; next_cursor:string|null; frontier:number;
  actions:Record<'pause'|'recover'|'cancel'|'respond'|'signal'|'run_again',{available:boolean;reason:string|null}>;
  last_recovery_id?:string|null;
  response_target?:{invocation_id:string;wait_id:string;status:string}|null;
}
export interface ExecutionOperation {
  operation_id:string; invocation_id:string; action:ExecutionActionKind; status:'running'|'succeeded'|'failed'|'unobserved';
  result:Record<string,unknown>|null; error:{code:string;message:string}|null;
}
export interface ExecutionActionSpec {root_invocation_id:string; action:ExecutionActionKind; input_schema:unknown; endpoints:string[]; next_sequence:number}
export interface ExecutionActionInput {
  invocation_id:string; request_id:string; action:ExecutionActionKind; payload?:unknown; reason?:string;
  target_invocation_id?:string; wait_id?:string; endpoint?:string; source_id?:string; sequence?:number; expected_version?:number;
}

export type InspectorTab = 'Summary' | 'Input / Output' | 'Execution' | 'Definition' | 'Actions' | 'Changes' | 'Details';
export interface ExecutionRecordSelection { kind: 'node' | 'call' | 'wait' | 'edge'; id: string; section?: InspectorTab; waitId?: string }
export type ExecutionView = 'graph' | 'timeline' | 'split';
export type Selection = { kind: "node" | "edge" | "invocation" | "workflow"; id: string; section?: InspectorTab; record?: ExecutionRecordSelection } | null;

export interface LocationState {
  view: "workflows" | "executions";
  execution: string | null;
  workflow: string | null;
  revision: string | null;
  session: string | null;
  selection: Selection;
  executionView?: ExecutionView;
  compareBase?: string;
}

export interface TraceRow {
  invocation_id: string;
  session_id?: string;
  workflow_id?: string;
  workflow_version?: string;
  workflow_revision_id?: string;
  root_invocation_id?: string;
  parent_invocation_id?: string | null;
  parent_occurrence_id?: string | null;
  unit_index?: number | null;
  entry_node_id?: string;
  status?: string | null;
  control_state?: string | null;
  occurrence_id?: string;
  call_id?: string;
  wait_id?: string;
  operator_id?: string;
  operator_definition_id?: string | null;
  node_id?: string;
  started_at_us?: number | null;
  ready_at_us?: number | null;
  completed_at_us?: number | null;
  queue_duration_ns?: number;
  execution_duration_ns?: number | null;
  call_count?: number;
  peak_parallelism?: number;
  error_type?: string | null;
  error_code?: string | null;
  condition_result?: boolean;
  duration_ns?: number;
  sequence?: number;
  source_occurrence_id?: string;
  values: string[];
  value_keys?: Record<string, string>;
  [key: string]: unknown;
}

export interface TracePage {
  items: TraceRow[];
  next_cursor: string | null;
  frontier: number;
}

export interface SessionSummary {
  session_id: string;
  invocation_count: number;
  workflow_count: number;
  matching_count: number;
  updated_at_us: number;
}

export interface SessionPage {
  items: SessionSummary[];
  next_cursor: string | null;
  frontier: number;
  total_count: number;
}

export interface ExecutionFilters {
  query: string;
  workflowIds: string[];
  revisionIds: string[];
  statuses: string[];
  startedFrom: string;
  startedTo: string;
}

export interface ExecutionTrace {
  fork?: ForkOrigin | null;
  source_id?: string;
  invocation_version?: number;
  observed_at_us: number;
  last_event: TimelineMarker | null;
  invocation: TraceRow & { workflow_id: string; workflow_revision_id: string; root_invocation_id: string };
  nodes: { node_id: string; status: string; occurrences: number; occurrence_id: string; call_count?: number }[];
  edges: { edge_id: string; condition_result: boolean; evaluations: number; evaluation?: TraceRow }[];
  active_routes: { edge_id: string; source_occurrence_id: string; target_node_id: string; target_occurrence_id: string; status: string }[];
  child_count: number;
  frontier: number;
  view: "durable";
}
export interface ForkOrigin {
  source_session_id:string; source_invocation_id:string; source_sequence:number; source_event_id:string;
  source_root_invocation_id:string; root_invocation_id:string;
  inherited_occurrences:Record<string,string>;
}
export interface ForkPlan {
  available:boolean; reason:string|null; code:string|null; invocation_id:string; sequence:number;
  members?:{source_invocation_id:string;source_session_id:string;source_sequence:number}[];
  replay_calls?:{operator_id:string;call_id:string;occurrence_id:string}[];
}
export interface AcceptedFork extends AcceptedExecution {
  root_invocation_id:string; selected_invocation_id:string; selected_session_id:string;
  source_invocation_id:string; source_sequence:number;
}
export interface RecordedValue { available: boolean; value: unknown }
export interface ExecutionFamily {
  current: TraceRow; root: TraceRow | null; ancestors: TraceRow[];
  ancestry_complete: boolean; frontier: number; view: 'durable';
}

export interface TimelineOverview {
  invocation_id: string; start_at_us: number | null; end_at_us: number | null;
  observed_at_us: number; status: string | null; control_state: string | null;
  counts: { node: number; call: number; wait: number }; untimed_count: number;
  node_start_bins: number[];
  pause_intervals?: { start_at_us: number; end_at_us: number | null }[];
  idle?: TimelineIdle | null;
  frontier: number; view: 'durable';
}
export interface TimelineIdle { state: 'waiting' | 'paused'; since_at_us: number; at_us: number }
export interface TimelineRow extends TraceRow {
  id: string; kind: 'node' | 'call' | 'wait'; occurrence_id: string;
  start_at_us: number; end_at_us: number | null; ordinal?: number; wait_kind?: string;
  timeline_visible?: boolean;
  activations?: { edge_id: string; source_occurrence_id: string }[];
  wait_count?: number;
  wait?: TimelineRow | null;
  preview_calls?: TimelineRow[];
}
export interface TimelinePage { items: TimelineRow[]; next_cursor: string | null; frontier: number }
export interface TimelineMarker { event_id: string; event_name: string; sequence: number; occurred_at_us: number; control_state?: string; invocation_status?: string }
export interface TimelineMarkerPage { items: TimelineMarker[]; next_cursor: string | null; frontier: number }

export interface TimelineEventGroup {
  count: number; names: Record<string, number>; occurred_at_us: number;
  start_us: number; end_us: number; first_sequence: number; last_sequence: number;
  marker: TimelineMarker | null;
}
export interface TimelineEventGroups { items: TimelineEventGroup[]; total_count: number; frontier: number }

export interface ReplayOccurrence { occurrence_id: string; node_id: string; status: string; created_sequence: number }
export interface ReplayEvent extends TimelineMarker {
  changes: { entity_type: 'node' | 'edge' | 'invocation'; entity_id: string; node_id: string | null; change: { status?: string; control_state?: string; node_id?: string; edge_id?: string; selected?: boolean } }[];
  evaluations: { edge_id: string; condition_result: boolean }[];
  routes: Omit<ExecutionTrace['active_routes'][number], 'status'>[];
  child_ids: string[];
  call_starts: { node_id: string; count: number }[];
  initial_edges?: ExecutionTrace['edges'];
}
export interface ReplayWindow {
  history_complete: boolean;
  baseline: { trace: ExecutionTrace; occurrences: ReplayOccurrence[]; child_ids: string[] } | null;
  events: ReplayEvent[];
  boundary: TimelineMarker | null; previous: TimelineMarker | null; next: TimelineMarker | null;
  first: TimelineMarker | null; last: TimelineMarker | null; frontier: number;
  time_from_us: number; time_until_us: number | null;
}


export interface TimelinePosition { cursor: string | null; page_index: number; frontier: number }
export interface TimelineFocus { serial: number; occurrenceId: string; nodeId?: string; callId?: string; kind: ExecutionRecordSelection['kind']; interval: { start_at_us: number; end_at_us: number | null; status?: string | null } }

export interface TraceDatasetManifest {
  cursor: string; base_cursor: string | null; delta: boolean;
  schema_version: number; source_id: string; snapshot_id: string; version: string;
  invocation_id: string; frontier: number; through_sequence: number;
  trace: ExecutionTrace; overview: TimelineOverview; initial_trace: ExecutionTrace | null;
  history_complete: boolean | null; replay: boolean; base_version: string | null;
  sections: Record<string, { count: number; pages: number }>;
}
export interface TraceEdgeUpdate { edge_id: string; condition_result?: boolean; evaluations_delta: number }
export interface TraceHead { invocation_id: string; root_invocation_id: string; invocation_version: number; family_version: number; status: string | null; active: boolean; store_id: string; cursor: string; frontier: number; server_now_us: number }
export interface TraceDatasetPage { items: (TimelineRow | TimelineMarker | ReplayEvent | TraceEdgeUpdate)[]; snapshot_id: string; section: string; page: number; next_page: number | null }
export interface TraceDataset { manifest: TraceDatasetManifest; node: TimelineRow[]; call: TimelineRow[]; wait: TimelineRow[]; event: TimelineMarker[]; edge?: TraceEdgeUpdate[] }
export interface LocalReplayResult { frame: ExecutionTrace; marker: TimelineMarker; previous: TimelineMarker | null; next: TimelineMarker | null; cursorTime: number }

export type RevisionChangeStatus = 'added' | 'removed' | 'modified';
export interface RevisionChange { kind: 'workflow' | 'node' | 'edge'; id: string; status: RevisionChangeStatus; fields: string[]; ordinal: number }
export interface RevisionComparisonPage {
  items: RevisionChange[]; next_cursor: string | null; total_count: number; frontier: number;
  counts: Record<RevisionChangeStatus | 'workflow' | 'node' | 'edge', number>;
}
export interface ComparisonObject { fields_only?: boolean; base: { present: boolean; value: unknown }; target: { present: boolean; value: unknown } }
export interface SessionInformation {
  session_id: string; created_at_us: number | null; updated_at_us: number | null;
  latest_invocation_id: string | null; parent_invocation_id: string | null;
  invocation_count: number; workflow_count: number; revision_count: number; durable_sequence: number; frontier: number;
}
export interface SessionHistoryRow extends TraceRow { ordinal: number; opening_sequence: number | null; recovery_count: number }
export interface SessionWorkflow {
  workflow_id: string; workflow_version: string; workflow_revision_id: string; invocation_count: number;
  first_sequence: number; first_started_at_us: number | null; last_started_at_us: number | null; runnable: boolean;
}
export interface MetadataPage<T> { items: T[]; next_cursor: string | null; total_count: number; frontier: number }
export interface CheckpointMember {
  session_id: string; invocation_id: string | null; workflow_revision_id: string | null; sequence: number;
}
export interface SessionCheckpoint {
  checkpoint_id: string; root_session_id: string; captured_at_us: number; digest: string;
  pinned: boolean; created_reason: string; members: CheckpointMember[]; sequence: number;
}
export interface CheckpointDetails extends Omit<SessionCheckpoint, 'sequence'> {
  delete_blockers: string[]; deletable: boolean;
}
