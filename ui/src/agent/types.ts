export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export interface QueuedMessage {
  id: string; session_id: string; position: number; message: string;
  images?: Content[] | null;
  state: 'queued' | 'starting' | 'started' | 'sending' | 'sent' | 'cancelled' | 'blocked';
  invocation_id: string | null; error: string | null;
}
export interface MessageQueue { items: QueuedMessage[]; invocation_id: string | null }
export type AccessAction = 'allow' | 'ask' | 'deny';
export interface SessionSettings {
  model: { provider: string; model: string | null; reasoning_effort?: string | null };
  approval_mode: 'always_ask' | 'policy' | 'auto_approve';
}
export interface AgentWorkspaceInfo { id: string; name: string; backend: string; path: string | null; is_project: boolean; available: boolean | null }
export interface SettingsRecord { session_id: string; version: number; settings: SessionSettings; workspace: { workspace_id: string; working_directory: string } | null; title: string | null; metadata_version: number }
export interface WorkspaceAgent {
  revision_id: string; user_signal_endpoint: string;
  include_ask_user: boolean; subagents: boolean;
  context_policy: { context_window: number | null; compact_at_ratio: number; compact_at_tokens: number | null; target_ratio: number; target_tokens: number | null };
}
export interface AgentCatalog { agent: WorkspaceAgent | null; providers: string[]; provider_options?: { id: string; name: string; models: string[]; model_details?: import('./settings/types.js').ConfiguredModel[] }[]; defaults: SessionSettings; project_workspace_id: string }
export interface AgentSession { session_id: string; title: string; invocation_id: string; updated_at_us: number; invocation_count: number; workspace_id?: string | null; status?: string; control_state?: string; last_sequence?: number }
export interface Page<T> { items: T[]; next_cursor: string | null }
export interface AgentInvocation { invocation_id: string; session_id: string; workflow_id: string; workflow_revision_id: string; root_invocation_id?: string; parent_invocation_id?: string | null; status: string; started_at_us?: number | null; completed_at_us?: number | null; [key: string]: unknown }
export interface SessionSubagent { invocation_id: string; session_id: string; task: string | null; status: string }
export interface PendingWait { invocation_id: string; wait_id: string; node_id?: string; occurrence_id: string }
export interface Controls {
  root_invocation_id: string; root_session_id: string; status: string; control_state: string; expected_version: number;
  recovery_reason: string | null; pending_responses: PendingWait[]; next_cursor: string | null;
  actions: Record<'pause' | 'recover' | 'cancel' | 'signal' | 'respond' | 'run_again', { available: boolean; reason: string | null }>;
}
export interface AgentExecution {
  current: AgentInvocation; root: AgentInvocation; ancestors: AgentInvocation[]; children: AgentInvocation[]; children_truncated: boolean;
  controls: Controls; snapshot: { snapshot_id: string; settings_version: number; settings: SessionSettings } | null;
}
export interface AgentResources { user_signal_endpoint: string; tools: { name: string; description: string | null; origin: string; version: string }[]; skills: { name: string; description: string; origin: string }[] }
export interface Usage { input_tokens: number | null; output_tokens: number | null; total_tokens: number | null; cost_usd: string | null; reasoning_tokens?: number | null; cache_read_tokens?: number | null; call_count: number; pending_calls: number; unsettled_calls: number; missing_usage_calls: number; missing_cost_calls: number }
export interface Content { type: string; text?: string; source?: Record<string, Json>; [key: string]: unknown }
export interface AgentMessage { message_id: string; source: 'user' | 'agent'; sender_id: string | null; content: Content[]; reply_requested: boolean; reply_to: string | null }
export type ResponseBlock = { type: 'text' | 'refusal'; text: string } | { type: 'reasoning'; text: string | null; summary: string[] } | { type: 'tool'; tool_id: string } | { type: 'media'; content: Content };
export interface ToolSnapshot {
  tool_id: string; response_id: string; call_id: string; name: string; arguments: string;
  state: 'queued' | 'running' | 'waiting' | 'completed' | 'failed' | 'denied' | 'cancelled';
  result: { content: Content[]; structured_content: Json; is_error: boolean } | null; progress: Json;
}
export interface ResponseSnapshot { response_id: string; model: string; finish_reason: string; blocks: ResponseBlock[]; usage: Record<string, number | null> | null }
export interface Interaction { interaction_id: string; type: 'question' | 'approval'; state: 'pending' | 'resolved'; tool_ids: string[]; request: Record<string, Json>; resolution: { outcome: string; response: Json; error: Json } | null }
export interface Compaction { compaction_id: string; state: 'running' | 'completed'; before_tokens: number | null; after_tokens: number | null; view_generation: number }
export interface Payloads {
  'agent.messages': { messages: AgentMessage[] };
  'agent.response.delta': { response_id: string; stream_id: string; item_key: string; content_index: number | null; kind: string; delta: string };
  'agent.response': ResponseSnapshot;
  'agent.tools': { tools: ToolSnapshot[] };
  'agent.interaction': Interaction;
  'agent.compaction': Compaction;
  'agent.error': { error: { type: string; message: string }; stage: string | null; handled: boolean };
  'agent.completed': { final_response_id: string; rounds: number };
}
export type AgentEvent = { [K in keyof Payloads]: { id: string; session_id: string; invocation_id: string; occurrence_id: string | null; occurred_at_us: number; sequence: number | null; persist: boolean; kind: K; payload: Payloads[K] & { v: 1; node_id: string } } }[keyof Payloads];
export interface InteractionReview { reviewer: 'user' | 'system' | 'policy' | 'agent'; requires_user_input: boolean }
export interface ActionSpec { root_invocation_id: string; request: Record<string, Json> | null; review?: InteractionReview; response_example: Json; input_schema: unknown; endpoints: string[]; next_sequence: number }
export interface ActionInput { invocation_id: string; action: 'pause' | 'recover' | 'cancel' | 'respond' | 'signal'; request_id: string; expected_version?: number; target_invocation_id?: string; wait_id?: string; payload?: unknown; endpoint?: string; source_id?: string; sequence?: number }
export interface Operation { operation_id: string; status: 'running' | 'succeeded' | 'failed' | 'unobserved'; result: Record<string, Json> | null; error: { code: string; message: string } | null }
