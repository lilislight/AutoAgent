import type {
  ChildSessionSummary,
  InvocationSummary,
  InvocationStateResponse,
  Page,
  SessionSummary,
  TraceEvent,
  TracePage,
  WorkflowSnapshot,
  WorkflowSummary,
} from "./types";

const API = "/api/v1";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API}${path}`, { signal });
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as {
        detail?: string | { message?: string };
      };
      message =
        typeof body.detail === "string"
          ? body.detail
          : body.detail?.message ?? message;
    } catch {
      // Keep the HTTP status when the response is not JSON.
    }
    throw new ApiError(response.status, message);
  }
  return (await response.json()) as T;
}

function query(values: Record<string, string | number | null | undefined>): string {
  const parameters = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== null && value !== undefined) parameters.set(key, String(value));
  }
  const encoded = parameters.toString();
  return encoded ? `?${encoded}` : "";
}

async function childrenThroughKnown(
  invocationId: string,
  knownSessionIds: readonly string[],
  signal?: AbortSignal,
): Promise<Page<ChildSessionSummary>> {
  const remaining = new Set(knownSessionIds);
  const seenCursors = new Set<string>();
  const items: ChildSessionSummary[] = [];
  let cursor: string | null = null;
  let pagesAfterKnown = 0;
  do {
    const knownCompleteBeforePage = remaining.size === 0;
    const page: Page<ChildSessionSummary> = await get<Page<ChildSessionSummary>>(
      `/invocations/children${query({
        invocation_id: invocationId,
        limit: 100,
        cursor,
      })}`,
      signal,
    );
    items.push(...page.items);
    for (const item of page.items) remaining.delete(item.session_id);
    cursor = page.next_cursor;
    if (knownCompleteBeforePage) pagesAfterKnown += 1;
    if (cursor !== null) {
      if (seenCursors.has(cursor)) {
        throw new Error("Child pagination returned a repeated cursor.");
      }
      seenCursors.add(cursor);
    }
    // Once the complete loaded prefix has been refreshed, read one more page.
    // That page discovers children appended just beyond an exact page boundary
    // even when their planned Trace was missed during an SSE reconnect.
  } while (
    cursor !== null &&
    (remaining.size > 0 || pagesAfterKnown === 0)
  );
  return { items, next_cursor: cursor, has_more: cursor !== null };
}

export const api = {
  workflows: (cursor?: string | null, signal?: AbortSignal) =>
    get<Page<WorkflowSummary>>(`/workflows${query({ limit: 100, cursor })}`, signal),
  workflow: (revisionId: string, signal?: AbortSignal) =>
    get<WorkflowSnapshot>(`/workflows/detail${query({ revision_id: revisionId })}`, signal),
  sessions: (revisionId: string, cursor?: string | null, signal?: AbortSignal) =>
    get<Page<SessionSummary>>(
      `/workflows/sessions${query({ revision_id: revisionId, limit: 100, cursor })}`,
      signal,
    ),
  invocations: (
    sessionId: string,
    workflowRevisionId: string,
    cursor?: string | null,
    signal?: AbortSignal,
  ) =>
    get<Page<InvocationSummary>>(
      `/sessions/invocations${query({
        session_id: sessionId,
        workflow_revision_id: workflowRevisionId,
        limit: 100,
        cursor,
      })}`,
      signal,
    ),
  children: (invocationId: string, cursor?: string | null, signal?: AbortSignal) =>
    get<Page<ChildSessionSummary>>(
      `/invocations/children${query({ invocation_id: invocationId, limit: 100, cursor })}`,
      signal,
    ),
  childrenThroughKnown,
  invocation: (invocationId: string, signal?: AbortSignal) =>
    get<InvocationSummary>(`/invocations/detail${query({ invocation_id: invocationId })}`, signal),
  traceTail: (invocationId: string, tailLimit = 500, signal?: AbortSignal) =>
    get<TracePage>(
      `/invocations/trace${query({ invocation_id: invocationId, tail_limit: tailLimit })}`,
      signal,
    ),
  traceBefore: (
    invocationId: string,
    beforeSequence: number,
    limit = 500,
    signal?: AbortSignal,
  ) =>
    get<TracePage>(
      `/invocations/trace${query({
        invocation_id: invocationId,
        before_sequence: beforeSequence,
        limit,
      })}`,
      signal,
    ),
  tracePage: (
    invocationId: string,
    cursor: string | null,
    limit = 200,
    signal?: AbortSignal,
  ) =>
    get<TracePage>(
      `/invocations/trace${query({ invocation_id: invocationId, cursor, limit })}`,
      signal,
    ),
  state: (invocationId: string, signal?: AbortSignal) =>
    get<InvocationStateResponse>(
      `/invocations/state${query({ invocation_id: invocationId })}`,
      signal,
    ),
  traceStream: (invocationId: string, cursor: string | null) =>
    new EventSource(
      `${API}/invocations/stream${query({ invocation_id: invocationId, cursor })}`,
    ),
};
