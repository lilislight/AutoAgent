import {
  buildTimelineView,
  visibleTimelineSpans,
} from "../src/timeline.js";
import type {
  InvocationDetail,
  ProjectedNodeExecution,
  RuntimeProjection,
} from "../src/types.js";

const calls = Array.from({ length: 50 }, (_, index) => ({
  id: `call-${index + 11}`,
  event_sequence: index + 12,
  node_execution_id: "execution-1",
  operator_id: "mapped",
  kind: "map",
  call_no: index + 11,
  unit_index: index + 10,
  unit_attempt_no: 1,
  reason: "normal",
  state: "completed",
  error: null,
  started_at_ms: index + 2,
  occurred_at_ms: index + 3,
  elapsed_ns: 1_000_000,
  timing: { execution_ns: 1_000_000 },
  streaming: false,
  stream_chunk_count: 0,
}));

const execution: ProjectedNodeExecution = {
  execution_id: "execution-1",
  node_id: "mapped",
  sequence: 1,
  first_event_sequence: 1,
  state: "completed",
  input: null,
  output: null,
  error: null,
  started_at_ms: 1,
  ended_at_ms: 60,
  elapsed_ns: 59_000_000,
  operator_call_count: 60,
  operator_calls: calls,
};
const projection: RuntimeProjection = {
  schema_version: 5,
  invocation_id: "invocation-1",
  through_sequence: 61,
  invocation_state: "completed",
  nodes: {},
  node_executions: { "execution-1": execution },
  edges: {},
};
const invocation: InvocationDetail = {
  id: "invocation-1",
  workflow_id: "workflow",
  workflow_revision_id: "revision",
  workflow_version: 1,
  definition_hash: "hash",
  entry_node_id: "mapped",
  state: "completed",
  created_at_ms: 1,
  updated_at_ms: 60,
  input: {},
  context: {},
  result: null,
  error: null,
  node_executions: [],
};

const timeline = buildTimelineView(invocation, projection);
equal(timeline.spans.length, 51);
equal(timeline.spans[0].kind, "node_execution");
equal(timeline.spans[0].omitted_child_count, 10);
equal(timeline.spans[1].kind, "operator_call");
equal(timeline.spans[1].parent_id, "execution-1");
equal(timeline.spans[1].started_at_ms, 2);
equal(timeline.spans[1].ended_at_ms, 3);
equal(visibleTimelineSpans(timeline.spans, new Set()).length, 51);
const collapsed = visibleTimelineSpans(
  timeline.spans,
  new Set(["execution-1"]),
);
equal(collapsed.length, 1);
equal(collapsed[0].id, "execution-1");

console.log("timeline tests passed");

function equal(actual: unknown, expected: unknown): void {
  if (actual !== expected) {
    throw new Error(`Expected ${String(expected)}, received ${String(actual)}`);
  }
}
