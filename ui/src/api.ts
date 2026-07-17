import type {
  InvocationSummary,
  ObservationBootstrap,
  ObservationHealth,
  RuntimeEvent,
  SessionSummary,
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

export function listWorkflows(): Promise<WorkflowSummary[]> {
  return requestJson("/api/workflows");
}

export function getHealth(): Promise<ObservationHealth> {
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

export function getObservationView(
  sessionId: string,
  invocationId: string,
): Promise<ObservationBootstrap> {
  return requestJson(
    `/api/sessions/${sessionId}/invocations/${invocationId}/view`,
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
