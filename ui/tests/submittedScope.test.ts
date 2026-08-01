import {
  upsertSubmittedInvocationPage,
  upsertSubmittedSessionPage,
  type SubmittedScope,
} from "../src/submittedScope.js";

const scope: SubmittedScope = {
  workflowId: "workflow",
  workflowRevisionId: "workflow:revision",
  sessionId: "generated-session-id",
  sessionKey: "generated-session-key",
  invocationId: "invocation-1",
  entryNodeId: "entry",
  state: "running",
  eventMode: "standard",
};

{
  const sessions = upsertSubmittedSessionPage(undefined, scope, 10);
  equal(sessions.pages[0]?.items[0]?.id, scope.sessionId);
  equal(sessions.pages[0]?.items[0]?.session_key, scope.sessionKey);
  equal(sessions.pages[0]?.items[0]?.current_invocation_id, scope.invocationId);
}

{
  const invocations = upsertSubmittedInvocationPage(undefined, scope, 10);
  equal(invocations.pages[0]?.items[0]?.id, scope.invocationId);
  equal(invocations.pages[0]?.items[0]?.session_id, scope.sessionId);
}

{
  const sessions = upsertSubmittedSessionPage(
    {
      pages: [{
        items: [{
          id: scope.sessionId,
          workflow_id: scope.workflowId,
          workflow_revision_id: scope.workflowRevisionId,
          session_key: null,
          current_invocation_id: null,
          invocation_count: 0,
          created_at_ms: 1,
          updated_at_ms: 1,
        }],
        next_cursor: null,
        has_more: false,
      }],
      pageParams: [null],
    },
    scope,
    10,
  );
  equal(sessions.pages[0]?.items[0]?.session_key, scope.sessionKey);
  equal(sessions.pages[0]?.items[0]?.current_invocation_state, "running");
}

console.log("submitted scope tests passed");

function equal(actual: unknown, expected: unknown): void {
  if (actual !== expected) {
    throw new Error(`Expected ${String(expected)}, received ${String(actual)}`);
  }
}
