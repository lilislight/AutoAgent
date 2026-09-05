import { runtimeEventBelongsToNodeExecution } from "../src/inspection.js";
import type { RuntimeEvent } from "../src/types.js";

function runtimeEvent(
  subjectId: string,
  payload: Record<string, unknown>,
): RuntimeEvent {
  return {
    id: "event-1",
    invocation_id: "invocation-1",
    sequence: 1,
    schema_version: 1,
    event_type: "phase",
    event_name: "operator_call.completed",
    subject_type: "operator_call",
    subject_id: subjectId,
    elapsed_ns: 1,
    status: "completed",
    timing: {},
    has_input: false,
    has_output: false,
    occurred_at_ms: 1,
    payload,
  };
}

equal(
  runtimeEventBelongsToNodeExecution(
    runtimeEvent("node-execution-1", {}),
    "node-execution-1",
  ),
  true,
  "NodeExecution Events belong through their subject",
);

equal(
  runtimeEventBelongsToNodeExecution(
    runtimeEvent("operator-call-1", { node_execution_id: "node-execution-1" }),
    "node-execution-1",
  ),
  true,
  "Operator Call Events belong through node_execution_id",
);

equal(
  runtimeEventBelongsToNodeExecution(
    runtimeEvent("edge-1", { source_execution_id: "node-execution-1" }),
    "node-execution-1",
  ),
  true,
  "outgoing Edge Events belong through source_execution_id",
);

equal(
  runtimeEventBelongsToNodeExecution(
    runtimeEvent("operator-call-2", { node_execution_id: "node-execution-2" }),
    "node-execution-1",
  ),
  false,
  "unrelated Events are excluded",
);

console.log("inspection tests passed");

function equal(actual: unknown, expected: unknown, message: string): void {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${String(expected)}, received ${String(actual)}`);
  }
}
