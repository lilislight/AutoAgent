export type Selection = {
  workflow: string | null;
  session: string | null;
  invocation: string | null;
};

export type NavigationTransition = Readonly<{
  selection: Selection;
  clear: "workflow" | "session" | "invocation";
  invalidatedScopes: readonly string[];
}>;

const DOWNSTREAM_OF_WORKFLOW = [
  "workflow-bootstrap",
  "invocation-list",
  "invocation-bootstrap",
  "sse",
  "user-event-sse",
  "boundary-summary",
  "boundary-state",
  "historical-state",
  "boundary-children",
  "more-sessions",
  "more-invocations",
  "more-children",
  "earlier-trace",
  "earlier-user-events",
] as const;

const DOWNSTREAM_OF_SESSION = [
  "invocation-list",
  "invocation-bootstrap",
  "sse",
  "user-event-sse",
  "boundary-summary",
  "boundary-state",
  "historical-state",
  "boundary-children",
  "more-invocations",
  "more-children",
  "earlier-trace",
  "earlier-user-events",
] as const;

const DOWNSTREAM_OF_INVOCATION = [
  "invocation-bootstrap",
  "sse",
  "user-event-sse",
  "boundary-summary",
  "boundary-state",
  "historical-state",
  "boundary-children",
  "more-children",
  "earlier-trace",
  "earlier-user-events",
] as const;

export function switchWorkflow(
  current: Selection,
  workflow: string | null,
): NavigationTransition | null {
  if (current.workflow === workflow) return null;
  return {
    selection: { workflow, session: null, invocation: null },
    clear: "workflow",
    invalidatedScopes: DOWNSTREAM_OF_WORKFLOW,
  };
}

export function switchSession(
  current: Selection,
  session: string | null,
): NavigationTransition | null {
  if (current.session === session) return null;
  return {
    selection: { ...current, session, invocation: null },
    clear: "session",
    invalidatedScopes: DOWNSTREAM_OF_SESSION,
  };
}

export function switchInvocation(
  current: Selection,
  invocation: string | null,
): NavigationTransition | null {
  if (current.invocation === invocation) return null;
  return {
    selection: { ...current, invocation },
    clear: "invocation",
    invalidatedScopes: DOWNSTREAM_OF_INVOCATION,
  };
}
