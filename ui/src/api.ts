import type {
  InvocationSummary,
  InvocationResumeResponse,
  InvocationSubmitResponse,
  TraceBootstrap,
  ServerHealth,
  RuntimeEvent,
  RuntimeEventPage,
  SessionSummary,
  WorkflowGraphView,
  WorkflowSummary,
} from "./types";

async function requestJson<T>(path: string): Promise<T> {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `${response.status} ${response.statusText}`);
  }
  return (await response.json()) as T;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `${response.status} ${response.statusText}`);
  }
  return (await response.json()) as T;
}

export function listWorkflows(): Promise<WorkflowSummary[]> {
  return requestJson("/api/workflows");
}

/** Workflows compiled into the App currently backing this server. */
export function listRegisteredWorkflows(): Promise<WorkflowSummary[]> {
  return requestJson("/api/registered-workflows");
}

export function getWorkflowGraph(workflow: WorkflowSummary): Promise<WorkflowGraphView> {
  const query = new URLSearchParams({
    operator_manifest_hash: workflow.operator_manifest_hash,
  });
  return requestJson(
    `/api/workflows/${workflow.workflow_id}/versions/${workflow.definition_hash}?${query}`,
  );
}

export function getHealth(): Promise<ServerHealth> {
  return requestJson("/api/health");
}

export async function createAuthenticationSession(token: string): Promise<void> {
  const response = await fetch("/api/auth/session", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ token }),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || "Authentication failed.");
  }
}

export function listSessions(workflowId: string): Promise<SessionSummary[]> {
  const query = new URLSearchParams({ workflow_id: workflowId });
  return requestJson(`/api/sessions?${query}`);
}

export function listInvocations(sessionId: string): Promise<InvocationSummary[]> {
  return requestJson(`/api/sessions/${sessionId}/invocations`);
}

export function submitInvocation(
  workflowId: string,
  body: {
    input?: Record<string, unknown> | null;
    session_id?: string | null;
    entry_node_id?: string | null;
  },
): Promise<InvocationSubmitResponse> {
  return postJson(`/api/workflows/${workflowId}/invocations`, body);
}

export function resumeInvocation(
  workflowId: string,
  body: {
    session_id: string;
    wait_key: string;
    output?: unknown;
  },
): Promise<InvocationResumeResponse> {
  return postJson(`/api/workflows/${workflowId}/resume`, body);
}

export function getTraceView(
  sessionId: string,
  invocationId: string,
): Promise<TraceBootstrap> {
  return requestJson(
    `/api/sessions/${sessionId}/invocations/${invocationId}/view`,
  );
}

export function getEarlierEvents(
  sessionId: string,
  invocationId: string,
  beforeSequence: number,
  limit = 1000,
): Promise<RuntimeEventPage> {
  const query = new URLSearchParams({
    before_sequence: String(beforeSequence),
    limit: String(limit),
  });
  return requestJson(
    `/api/sessions/${sessionId}/invocations/${invocationId}/events?${query}`,
  );
}

export function getLaterEvents(
  sessionId: string,
  invocationId: string,
  afterSequence: number,
  limit = 100,
): Promise<RuntimeEventPage> {
  const query = new URLSearchParams({
    after_sequence: String(afterSequence),
    limit: String(limit),
  });
  return requestJson(
    `/api/sessions/${sessionId}/invocations/${invocationId}/events?${query}`,
  );
}

export function subscribeToInvocation(
  sessionId: string,
  invocationId: string,
  afterSequence: number,
  onEvent: (event: RuntimeEvent) => void,
  onConnectionChange: (connected: boolean) => void,
): () => void {
  const query = new URLSearchParams({ after_sequence: String(afterSequence) });
  const source = new EventSource(
    `/api/sessions/${sessionId}/invocations/${invocationId}/stream?${query}`,
  );
  const receive = (message: MessageEvent<string>) => {
    onEvent(JSON.parse(message.data) as RuntimeEvent);
  };
  source.addEventListener("runtime", receive as EventListener);
  source.addEventListener("output", receive as EventListener);
  source.onopen = () => onConnectionChange(true);
  source.onerror = () => onConnectionChange(false);
  return () => {
    source.close();
    onConnectionChange(false);
  };
}
