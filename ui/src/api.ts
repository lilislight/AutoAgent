import { projectEvents } from "./projection";
import type {
  InvocationDetail,
  InvocationRecord,
  InvocationCancelResponse,
  InvocationSummary,
  InvocationResumeResponse,
  InvocationSubmitResponse,
  NodeExecutionView,
  RuntimeEvent,
  RuntimeEventPage,
  UserEvent,
  UserEventPage,
  RuntimeProjection,
  RuntimeStatus,
  ServerHealth,
  SessionSummary,
  TimelineView,
  TraceBootstrap,
  WorkflowGraphView,
  WorkflowSummary,
} from "./types";

const API = "/api/v1";

async function requestJson<T>(path: string, timeoutMs = 15_000): Promise<T> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(path, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) {
      const body = await response.json().catch(() => null) as { detail?: string } | null;
      throw new Error(body?.detail ?? `${response.status} ${response.statusText}`);
    }
    return (await response.json()) as T;
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(
        `AutoAgent Server did not respond within ${Math.round(timeoutMs / 1_000)}s at ${path}.`,
      );
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 30_000);
  try {
    const response = await fetch(path, {
      method: "POST",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => null) as { detail?: string } | null;
      throw new Error(payload?.detail ?? `${response.status} ${response.statusText}`);
    }
    return (await response.json()) as T;
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`AutoAgent Server action timed out at ${path}.`);
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

interface Page<T> {
  items: T[];
  next_cursor: string | null;
  has_more: boolean;
}

interface ServerEventPage {
  items: ServerRuntimeEvent[];
  first_sequence: number | null;
  last_sequence: number | null;
  has_earlier: boolean;
  has_later: boolean;
  live_sequence: number;
  invocation_state: InvocationSummary["state"];
}

interface ServerRuntimeEvent {
  id: string;
  invocation_id: string;
  sequence: number;
  schema_version: number;
  event_type: string;
  event_name: string;
  subject_type: string;
  subject_id: string;
  occurred_at_ms: number;
  elapsed_ns: number | null;
  status: string | null;
  timing: Record<string, number>;
  payload: Record<string, unknown>;
  has_input: boolean;
  has_output: boolean;
  has_operations?: boolean;
  input?: unknown;
  output?: unknown;
  operations?: Array<Record<string, unknown>> | null;
}

interface ServerTraceBootstrap {
  workflow: WorkflowGraphView;
  session: SessionSummary;
  invocation: InvocationSummary & {
    input: Record<string, unknown>;
    result: Record<string, unknown> | null;
    error: Record<string, unknown> | null;
  };
  capabilities: TraceBootstrap["capabilities"];
  checkpoint: {
    schema_version: number;
    through_sequence: number;
    projection: RuntimeProjection;
  };
  event_page: ServerEventPage;
}

export async function listWorkflows(): Promise<WorkflowSummary[]> {
  return listAllWorkflowPages(`${API}/workflows`);
}

export async function listRegisteredWorkflows(): Promise<WorkflowSummary[]> {
  return listAllWorkflowPages(`${API}/registered-workflows`);
}

async function listAllWorkflowPages(path: string): Promise<WorkflowSummary[]> {
  const values: WorkflowSummary[] = [];
  let cursor: string | null = null;
  do {
    const query = new URLSearchParams({ limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await requestJson<Page<WorkflowSummary>>(
      `${path}?${query.toString()}`,
    );
    values.push(...page.items);
    cursor = page.has_more ? page.next_cursor : null;
  } while (cursor);
  return values;
}

export function getWorkflowGraph(workflow: WorkflowSummary): Promise<WorkflowGraphView> {
  return requestJson(`${API}/workflow-revisions/${workflow.revision_id}`);
}

export function getHealth(): Promise<ServerHealth> {
  return requestJson(`${API}/health`, 10_000);
}

export function getRuntimeStatus(): Promise<RuntimeStatus> {
  return requestJson(`${API}/runtime/status`);
}

export function subscribeToRuntimeStatus(
  onStatus: (status: RuntimeStatus) => void,
  onConnectionChange: (connected: boolean) => void,
): () => void {
  const source = new EventSource(`${API}/runtime/stream`);
  source.addEventListener("runtime_status", ((message: MessageEvent<string>) => {
    onStatus(JSON.parse(message.data) as RuntimeStatus);
  }) as EventListener);
  source.onopen = () => onConnectionChange(true);
  source.onerror = () => onConnectionChange(false);
  return () => {
    source.close();
    onConnectionChange(false);
  };
}

export async function createAuthenticationSession(token: string): Promise<void> {
  await postJson(`${API}/auth/session`, { token });
}

export async function listSessions(
  workflowRevisionId: string,
): Promise<SessionSummary[]> {
  return (await requestJson<Page<SessionSummary>>(
    `${API}/workflow-revisions/${
      encodeURIComponent(workflowRevisionId)
    }/sessions?limit=200`,
  )).items;
}

export async function listInvocations(sessionId: string): Promise<InvocationSummary[]> {
  return (await requestJson<Page<InvocationSummary>>(
    `${API}/sessions/${sessionId}/invocations?limit=200`,
  )).items;
}

export function listAgentInvocationNeighbors(
  sessionId: string,
  anchorInvocationId: string,
  direction: "older" | "newer",
  limit = 20,
): Promise<{
  items: InvocationSummary[];
  has_more: boolean;
  direction: "older" | "newer";
  anchor_invocation_id: string;
}> {
  const query = new URLSearchParams({
    anchor_invocation_id: anchorInvocationId,
    direction,
    limit: String(limit),
  });
  return requestJson(
    `${API}/sessions/${sessionId}/agent-invocations?${query.toString()}`,
  );
}

export function submitInvocation(
  workflowRevisionId: string,
  body: {
    input?: Record<string, unknown> | null;
    session_id?: string | null;
    entry_node_id?: string | null;
    event_mode?: "minimal" | "standard" | "full";
  },
): Promise<InvocationSubmitResponse> {
  return postJson(
    `${API}/workflow-revisions/${
      encodeURIComponent(workflowRevisionId)
    }/invocations`,
    {
      input: body.input,
      session_key: body.session_id,
      entry_node_id: body.entry_node_id,
      event_mode: body.event_mode ?? "standard",
    },
  );
}

export function resumeInvocation(
  workflowRevisionId: string,
  body: { session_id: string; wait_key: string; output?: unknown },
): Promise<InvocationResumeResponse> {
  return postJson(
    `${API}/workflow-revisions/${
      encodeURIComponent(workflowRevisionId)
    }/resume`,
    {
      session_key: body.session_id,
      wait_key: body.wait_key,
      output: body.output,
    },
  );
}

export function cancelInvocation(
  invocationId: string,
): Promise<InvocationCancelResponse> {
  return postJson(`${API}/invocations/${invocationId}/cancel`, {});
}

export async function getTraceView(
  _sessionId: string,
  invocationId: string,
): Promise<TraceBootstrap> {
  const raw = await requestJson<ServerTraceBootstrap>(
    `${API}/invocations/${invocationId}/trace?tail_limit=200`,
  );
  const events = raw.event_page.items.map(normalizeEvent);
  const projection = raw.checkpoint.projection;
  const invocation = invocationDetail(raw.invocation, projection);
  return {
    graph: raw.workflow,
    session: raw.session,
    invocation,
    timeline: buildTimelineView(invocation, projection),
    checkpoint: raw.checkpoint.projection,
    events,
    projection,
    capabilities: raw.capabilities,
    has_more_events: raw.event_page.has_later,
  };
}

export function getInvocation(
  invocationId: string,
): Promise<InvocationRecord> {
  return requestJson(`${API}/invocations/${invocationId}`);
}

export async function getEarlierEvents(
  _sessionId: string,
  invocationId: string,
  beforeSequence: number,
  limit = 200,
): Promise<RuntimeEventPage> {
  const raw = await requestJson<ServerEventPage>(
    `${API}/invocations/${invocationId}/events?before_sequence=${beforeSequence}&limit=${limit}`,
  );
  return eventPage(raw);
}

export async function getLaterEvents(
  _sessionId: string,
  invocationId: string,
  afterSequence: number,
  limit = 200,
): Promise<RuntimeEventPage> {
  const raw = await requestJson<ServerEventPage>(
    `${API}/invocations/${invocationId}/events?after_sequence=${afterSequence}&limit=${limit}`,
  );
  return eventPage(raw);
}

export async function getEventDetail(
  invocationId: string,
  sequence: number,
): Promise<RuntimeEvent> {
  return normalizeEvent(
    await requestJson<ServerRuntimeEvent>(
      `${API}/invocations/${invocationId}/events/${sequence}`,
    ),
  );
}

export function getRuntimeState(
  invocationId: string,
  throughSequence: number,
): Promise<Record<string, unknown>> {
  return requestJson(
    `${API}/invocations/${invocationId}/state?through_sequence=${throughSequence}`,
  );
}

export function subscribeToInvocation(
  invocationId: string,
  afterSequence: number,
  onEvent: (event: RuntimeEvent) => void,
  onStatus: (status: InvocationSummary) => void,
  onConnectionChange: (connected: boolean) => void,
): () => void {
  const source = new EventSource(
    `${API}/invocations/${invocationId}/stream?after_sequence=${afterSequence}`,
  );
  source.addEventListener("runtime_event", ((message: MessageEvent<string>) => {
    onEvent(normalizeEvent(JSON.parse(message.data) as ServerRuntimeEvent));
  }) as EventListener);
  source.addEventListener("invocation_status", ((message: MessageEvent<string>) => {
    const status = JSON.parse(message.data) as InvocationSummary;
    onStatus(status);
  }) as EventListener);
  source.addEventListener("stream_end", (() => {
    source.close();
    onConnectionChange(false);
  }) as EventListener);
  source.onopen = () => onConnectionChange(true);
  source.onerror = () => onConnectionChange(false);
  return () => {
    source.close();
    onConnectionChange(false);
  };
}

export function getUserEvents(
  invocationId: string,
  afterSequence = 0,
  limit = 1_000,
): Promise<UserEventPage> {
  return requestJson(
    `${API}/invocations/${invocationId}/user-events?after_sequence=${afterSequence}&limit=${limit}`,
  );
}

export async function getAllUserEvents(
  invocationId: string,
): Promise<UserEvent[]> {
  const events: UserEvent[] = [];
  let cursor = 0;
  while (true) {
    const page = await getUserEvents(invocationId, cursor);
    events.push(...page.items);
    if (!page.has_later || page.items.length === 0) return events;
    cursor = page.items.at(-1)!.sequence;
  }
}

export function subscribeToUserEvents(
  invocationId: string,
  afterSequence: number,
  onEvent: (event: UserEvent) => void,
  onConnectionChange: (connected: boolean) => void,
): () => void {
  const source = new EventSource(
    `${API}/invocations/${invocationId}/user-events/stream?after_sequence=${afterSequence}`,
  );
  source.addEventListener("user_event", ((message: MessageEvent<string>) => {
    onEvent(JSON.parse(message.data) as UserEvent);
  }) as EventListener);
  source.addEventListener("stream_end", (() => {
    source.close();
    onConnectionChange(false);
  }) as EventListener);
  source.onopen = () => onConnectionChange(true);
  source.onerror = () => onConnectionChange(false);
  return () => {
    source.close();
    onConnectionChange(false);
  };
}

function normalizeEvent(event: ServerRuntimeEvent): RuntimeEvent {
  const payload = event.payload ?? {};
  const nodeId =
    event.subject_type === "node" || event.subject_type === "node_execution"
      ? String(payload.node_id ?? event.subject_id)
      : typeof payload.node_id === "string" ? payload.node_id : null;
  const edgeId =
    event.subject_type === "edge"
      ? String(payload.edge_id ?? event.subject_id)
      : typeof payload.edge_id === "string" ? payload.edge_id : null;
  return {
    ...event,
    type: event.event_name,
    entity_type: event.subject_type,
    entity_id: event.subject_id,
    node_id: nodeId,
    edge_id: edgeId,
    channel: "runtime",
    visibility: "internal",
  };
}

function eventPage(raw: ServerEventPage): RuntimeEventPage {
  const events = raw.items.map(normalizeEvent);
  return {
    events,
    next_after_sequence: raw.last_sequence ?? 0,
    previous_before_sequence: raw.first_sequence,
    has_more: raw.has_earlier,
    has_later: raw.has_later,
    live_sequence: raw.live_sequence,
    invocation_state: raw.invocation_state,
  };
}

function invocationDetail(
  value: ServerTraceBootstrap["invocation"],
  projection: RuntimeProjection,
): InvocationDetail {
  return {
    ...value,
    context: {},
    node_executions: Object.values(projection.node_executions).map(
      (execution): NodeExecutionView => ({
        id: execution.execution_id,
        node_id: execution.node_id,
        sequence: execution.sequence,
        state: execution.state,
        input: execution.input ?? null,
        output: execution.output ?? null,
        error: execution.error ?? null,
        execution_scope: [],
        incoming_activations: [],
        edge_evaluations: [],
        operator_calls: (execution.operator_calls ?? []).map((call, index) => ({
          id: call.id,
          operator_id: call.operator_id,
          call_no: index + 1,
          kind: call.kind,
          reason: call.reason,
          item_index: null,
          replica_index: null,
          state: call.state,
          input: null,
          output: null,
          error: call.error,
          resource_usage: {
            ...call.timing,
            elapsed_ns: call.elapsed_ns,
            summary: call.summary,
            reason: call.reason,
          },
          started_at_ms:
            call.elapsed_ns == null
              ? null
              : call.occurred_at_ms - call.elapsed_ns / 1_000_000,
          ended_at_ms: call.occurred_at_ms,
          created_at_ms: call.occurred_at_ms,
          updated_at_ms: call.occurred_at_ms,
          streaming: call.streaming,
          stream_chunk_count: call.stream_chunk_count,
        })),
        resource_usage: execution.timing ?? {},
        started_at_ms: execution.started_at_ms ?? null,
        ended_at_ms: execution.ended_at_ms ?? null,
        created_at_ms: execution.started_at_ms ?? value.created_at_ms,
        updated_at_ms: execution.ended_at_ms ?? value.updated_at_ms,
      }),
    ),
  };
}

export function buildTimelineView(
  invocation: InvocationDetail,
  projection: RuntimeProjection,
): TimelineView {
  const spans = Object.values(projection.node_executions).map((execution) => {
    return {
      id: execution.execution_id,
      kind: "node_execution" as const,
      parent_id: null,
      node_id: execution.node_id,
      label: execution.node_id,
      state: execution.state,
      sequence: execution.first_event_sequence ?? execution.sequence,
      started_at_ms: execution.started_at_ms ?? invocation.created_at_ms,
      ended_at_ms: execution.ended_at_ms ?? null,
      duration_ms:
        execution.elapsed_ns === null || execution.elapsed_ns === undefined
          ? null
          : execution.elapsed_ns / 1_000_000,
    };
  });
  return {
    invocation_id: invocation.id,
    started_at_ms: invocation.created_at_ms,
    ended_at_ms:
      ["completed", "failed", "cancelled", "interrupted"].includes(invocation.state)
        ? invocation.updated_at_ms
        : null,
    spans,
  };
}
