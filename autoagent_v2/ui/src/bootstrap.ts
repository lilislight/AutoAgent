import { api } from "./api.js";
import type {
  ChildSessionSummary,
  InvocationSummary,
  Page,
  TracePage,
  UserEventPage,
} from "./types";

export interface InvocationBootstrapClient {
  traceTail(
    invocationId: string,
    tailLimit: number,
    signal?: AbortSignal,
  ): Promise<TracePage>;
  userEventTail(
    invocationId: string,
    tailLimit: number,
    signal?: AbortSignal,
  ): Promise<UserEventPage>;
  invocation(
    invocationId: string,
    signal?: AbortSignal,
  ): Promise<InvocationSummary>;
  children(
    invocationId: string,
    cursor?: string | null,
    signal?: AbortSignal,
  ): Promise<Page<ChildSessionSummary>>;
}

export interface InvocationBootstrap {
  history: TracePage;
  userHistory: UserEventPage;
  summary: InvocationSummary;
  childPage: Page<ChildSessionSummary>;
}

export async function loadInvocationBootstrap(
  invocationId: string,
  signal: AbortSignal,
  isCurrent: () => boolean,
  client: InvocationBootstrapClient = api,
): Promise<InvocationBootstrap | null> {
  const [history, userHistory] = await Promise.all([
    client.traceTail(invocationId, 500, signal),
    client.userEventTail(invocationId, 500, signal),
  ]);
  if (!isCurrent()) return null;
  const [summary, childPage] = await Promise.all([
    client.invocation(invocationId, signal),
    client.children(invocationId, null, signal),
  ]);
  if (!isCurrent()) return null;
  return { childPage, history, summary, userHistory };
}
