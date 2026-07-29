import {
  logicalUserEventKeys,
  projectAgentInvocation,
  selectAgentInvocationAnchor,
  shouldCountHydratedEventsAsUnread,
} from "../src/agentConversation.js";
import type { InvocationSummary, UserEvent } from "../src/types.js";

const invocation: InvocationSummary = {
  id: "invocation-1",
  workflow_id: "workflow",
  workflow_revision_id: "revision-1",
  workflow_version: "1",
  definition_hash: "hash",
  operator_manifest_hash: "operators",
  entry_node_id: "start",
  state: "completed",
  created_at_ms: 1_000,
  updated_at_ms: 2_000,
};

const events: UserEvent[] = [
  event(1, "reasoning_delta", { delta: "look " }, 1_100, "llm-1"),
  event(2, "reasoning_delta", { delta: "outside" }, 1_125, "llm-1"),
  event(3, "tool_call_requested", {
    reasoning_content: "look outside",
    calls: [
      {
        tool_call_id: "call-1",
        name: "weather",
        raw_arguments: "{\"city\":\"Paris\"}",
      },
      {
        tool_call_id: "call-2",
        name: "city",
        raw_arguments: "{\"city\":\"Paris\"}",
      },
    ],
  }, 1_160, "llm-1"),
  event(4, "tool_result", {
    results: [
      {
        tool_call_id: "call-1",
        tool_id: "weather",
        output: { temperature: 21 },
        error: null,
      },
    ],
  }, 1_190, "tool-1"),
  event(5, "tool_result", {
    results: [
      {
        tool_call_id: "call-2",
        tool_id: "city",
        output: { country: "France" },
        error: null,
      },
    ],
  }, 1_195, "tool-2"),
  event(6, "reasoning_delta", { delta: "compose answer" }, 1_200, "llm-2"),
  event(7, "message_completed", {
    message: { role: "assistant", content: "Superseded response." },
    finish_reason: "stop",
    model: "model",
  }, 1_210, "llm-2"),
  event(8, "agent_output", { output: "It is sunny." }, 1_220, "finish"),
];

const projection = projectAgentInvocation(invocation, events);
equal(
  projection.activity.map((item) => item.kind),
  ["thinking", "tool", "tool", "thinking"],
);
const firstThinking = projection.activity[0];
if (firstThinking.kind !== "thinking") throw new Error("Expected Thinking.");
equal(firstThinking.message.reasoning, "look outside");
equal(firstThinking.message.reasoningStartedAtMs, 1_100);
equal(firstThinking.message.reasoningEndedAtMs, 1_160);
const firstTool = projection.activity[1];
if (firstTool.kind !== "tool") throw new Error("Expected Tool.");
equal(firstTool.tool.toolCallId, "call-1");
equal(firstTool.tool.requestedAtMs, 1_160);
equal(firstTool.tool.completedAtMs, 1_190);
equal(firstTool.tool.result, { temperature: 21 });
equal(projection.agentOutput, "It is sunny.");

const beforeOutput = projectAgentInvocation(invocation, events.slice(0, -1));
equal(
  beforeOutput.activity.map((item) => item.kind),
  ["thinking", "tool", "tool", "thinking", "message"],
);

equal(logicalUserEventKeys(events[0]), []);
equal(
  logicalUserEventKeys(events[2]),
  ["tool_call:call-1", "tool_call:call-2"],
);
equal(logicalUserEventKeys(events[3]), ["tool_result:call-1"]);
equal(logicalUserEventKeys(events[7]), ["agent_output:invocation-1"]);
equal(shouldCountHydratedEventsAsUnread(1_000, 1_001), false);
equal(shouldCountHydratedEventsAsUnread(1_002, 1_001), true);
const olderInvocation = {
  ...invocation,
  id: "invocation-older",
  created_at_ms: 500,
};
equal(
  selectAgentInvocationAnchor(
    [invocation, olderInvocation],
    olderInvocation.id,
  )?.id,
  olderInvocation.id,
);
equal(
  selectAgentInvocationAnchor([olderInvocation, invocation], null)?.id,
  invocation.id,
);

console.log("ok - Agent conversation projection and unread units");

function event(
  sequence: number,
  type: string,
  data: unknown,
  occurredAtMs: number,
  operatorCallId: string,
): UserEvent {
  return {
    id: `event-${sequence}`,
    invocation_id: invocation.id,
    sequence,
    schema_version: 1,
    type,
    data,
    node_id: "node",
    node_execution_id: `node-${operatorCallId}`,
    operator_call_id: operatorCallId,
    occurred_at_ms: occurredAtMs,
  };
}

function equal(actual: unknown, expected: unknown): void {
  const left = JSON.stringify(actual);
  const right = JSON.stringify(expected);
  if (left !== right) {
    throw new Error(`Expected ${right}, received ${left}`);
  }
}
