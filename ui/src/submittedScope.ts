import type { InfiniteData } from "@tanstack/react-query";

import type {
  InvocationSummary,
  SessionSummary,
} from "./types.js";

interface CursorPage<T> {
  items: T[];
  next_cursor: string | null;
  has_more: boolean;
}

export interface SubmittedScope {
  workflowId: string;
  workflowRevisionId: string;
  sessionId: string;
  invocationId: string;
  sessionKey: string;
  entryNodeId: string;
  state: string;
  eventMode?: "minimal" | "standard" | "full";
}

export function upsertSubmittedSessionPage(
  current: InfiniteData<CursorPage<SessionSummary>> | undefined,
  value: SubmittedScope,
  now: number,
): InfiniteData<CursorPage<SessionSummary>> {
  const item: SessionSummary = {
    id: value.sessionId,
    workflow_id: value.workflowId,
    workflow_revision_id: value.workflowRevisionId,
    session_key: value.sessionKey,
    current_invocation_id: value.invocationId,
    current_invocation_state: value.state,
    invocation_count: 1,
    created_at_ms: now,
    updated_at_ms: now,
  };
  return upsertPage(
    current,
    item,
    (candidate) => candidate.id === value.sessionId,
    (candidate) => ({
      ...candidate,
      session_key: value.sessionKey,
      current_invocation_id: value.invocationId,
      current_invocation_state: value.state,
      invocation_count: Math.max(candidate.invocation_count, 1),
      updated_at_ms: now,
    }),
  );
}

export function upsertSubmittedInvocationPage(
  current: InfiniteData<CursorPage<InvocationSummary>> | undefined,
  value: SubmittedScope,
  now: number,
): InfiniteData<CursorPage<InvocationSummary>> {
  const item: InvocationSummary = {
    id: value.invocationId,
    session_id: value.sessionId,
    workflow_id: value.workflowId,
    workflow_revision_id: value.workflowRevisionId,
    workflow_version: null,
    definition_hash: null,
    entry_node_id: value.entryNodeId,
    state: value.state,
    event_mode: value.eventMode,
    live_sequence: 0,
    durable_sequence: 0,
    persistence_status: "pending",
    created_at_ms: now,
    updated_at_ms: now,
  };
  return upsertPage(
    current,
    item,
    (candidate) => candidate.id === value.invocationId,
    (candidate) => ({
      ...candidate,
      state: value.state,
      updated_at_ms: now,
    }),
  );
}

function upsertPage<T>(
  current: InfiniteData<CursorPage<T>> | undefined,
  item: T,
  matches: (candidate: T) => boolean,
  update: (candidate: T) => T,
): InfiniteData<CursorPage<T>> {
  if (!current || current.pages.length === 0) {
    return {
      pages: [{ items: [item], next_cursor: null, has_more: false }],
      pageParams: [null],
    };
  }
  if (current.pages.some((page) => page.items.some(matches))) {
    return {
      ...current,
      pages: current.pages.map((page) => ({
        ...page,
        items: page.items.map((candidate) =>
          matches(candidate) ? update(candidate) : candidate
        ),
      })),
    };
  }
  const [first, ...rest] = current.pages;
  return {
    ...current,
    pages: [{ ...first, items: [item, ...first.items] }, ...rest],
  };
}
