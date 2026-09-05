import { mergeInvocationStatus } from "../src/invocationStatus.js";
import type {
  InvocationDetail,
  InvocationSummary,
} from "../src/types.js";

function detail(
  state: string,
  updatedAtMs: number,
): InvocationDetail {
  return {
    id: "invocation-1",
    workflow_id: "workflow",
    workflow_revision_id: "workflow:revision",
    workflow_version: "1",
    definition_hash: "revision",
    entry_node_id: "entry",
    state,
    created_at_ms: 1,
    updated_at_ms: updatedAtMs,
    input: {},
    context: {},
    result: null,
    error: null,
    node_executions: [],
  };
}

function status(
  state: string,
  updatedAtMs: number,
): InvocationSummary {
  return {
    id: "invocation-1",
    workflow_id: "workflow",
    workflow_revision_id: "workflow:revision",
    workflow_version: "1",
    definition_hash: "revision",
    entry_node_id: "entry",
    state,
    created_at_ms: 1,
    updated_at_ms: updatedAtMs,
  };
}

{
  const merged = mergeInvocationStatus(
    detail("completed", 20),
    status("running", 10),
  );
  equal(merged.state, "completed");
}

{
  const merged = mergeInvocationStatus(
    detail("completed", 20),
    status("running", 20),
  );
  equal(merged.state, "completed");
}

{
  const merged = mergeInvocationStatus(
    detail("running", 10),
    status("completed", 20),
  );
  equal(merged.state, "completed");
}

console.log("invocation status tests passed");

function equal(actual: unknown, expected: unknown): void {
  if (actual !== expected) {
    throw new Error(`Expected ${String(expected)}, received ${String(actual)}`);
  }
}
