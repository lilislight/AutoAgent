import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { WorkflowGraph } from "../src/components/WorkflowGraph.js";
import type { TraceEvent, WorkflowSnapshot } from "../src/types.js";

const workflow: WorkflowSnapshot = {
  schema_version: 1,
  workflow_id: "graph",
  workflow_version: "1",
  workflow_revision_id: "graph:revision",
  definition_hash: "hash",
  definition: {
    nodes: [{ id: "work", executable: { kind: "operator", id: "search" } }],
    edges: [],
    entry_node_ids: ["work"],
    exit_node_ids: ["work"],
    loops: [{ id: "loop" }],
  },
};

const completed: TraceEvent = {
  schema_version: 1,
  id: "trace",
  session_id: "session",
  trace_sequence: 1,
  kind: "node_occurrence.completed",
  occurred_at_ns: "1",
  invocation_id: "invocation",
  causation_id: null,
  state_version: 1,
  subject_ids: { occurrence_id: "work@root", node_id: "work" },
  status: "completed",
  error: null,
  metrics: null,
  attributes: {},
};

test("renders author identity, structural roles, loops, and runtime state", () => {
  const markup = renderToStaticMarkup(
    <WorkflowGraph workflow={workflow} events={[completed]} />,
  );
  assert.match(markup, /node-completed/);
  assert.match(markup, /entry · exit/);
  assert.match(markup, />search</);
  assert.match(markup, / 1 loops</);
});
