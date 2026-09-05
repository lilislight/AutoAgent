import type { InvocationSummary, UserEvent } from "./types.js";

export interface AgentMessageView {
  key: string;
  operatorCallId: string | null;
  firstSequence: number;
  reasoningFirstSequence: number | null;
  messageFirstSequence: number | null;
  reasoning: string;
  reasoningStartedAtMs: number | null;
  reasoningEndedAtMs: number | null;
  content: string;
  completed: boolean;
  occurredAtMs: number;
}

export interface AgentToolView {
  key: string;
  toolCallId: string | null;
  name: string | null;
  rawArguments: string;
  result: unknown;
  error: unknown;
  requestedAtMs: number | null;
  completedAtMs: number | null;
  firstSequence: number;
}

export interface AgentGenericEventView {
  event: UserEvent;
  tone: "default" | "error";
}

export type AgentActivityItem =
  | {
      kind: "thinking";
      key: string;
      sequence: number;
      message: AgentMessageView;
    }
  | {
      kind: "message";
      key: string;
      sequence: number;
      message: AgentMessageView;
    }
  | {
      kind: "tool";
      key: string;
      sequence: number;
      tool: AgentToolView;
    }
  | {
      kind: "generic";
      key: string;
      sequence: number;
      value: AgentGenericEventView;
    };

export interface AgentStandardBlock {
  kind: "react" | "llm";
  key: string;
  sequence: number;
  activity: AgentActivityItem[];
  output: unknown;
  outputSequence: number | null;
}

export interface AgentCustomBlock {
  kind: "custom";
  key: string;
  sequence: number;
  value: AgentGenericEventView;
}

export type AgentActivityBlock = AgentStandardBlock | AgentCustomBlock;

export interface AgentInvocationView {
  invocation: InvocationSummary;
  blocks: AgentActivityBlock[];
}

type MutableMessage = AgentMessageView;
type MutableTool = AgentToolView;

const DELTA_TYPES = new Set([
  "message_delta",
  "reasoning_delta",
  "tool_call_delta",
]);
const KNOWN_TYPES = new Set([
  ...DELTA_TYPES,
  "message_completed",
  "tool_call_requested",
  "tool_result",
  "agent_output",
  "message_aborted",
  "agent_failed",
]);

export function logicalUnreadUserEventKeys(
  events: UserEvent[],
  readThroughSequence: number,
): string[] {
  return events.flatMap((event) =>
    event.sequence > readThroughSequence
      ? logicalUserEventKeys(event)
      : [],
  );
}

export function selectAgentInvocationAnchor(
  invocations: InvocationSummary[],
  invocationId: string | null,
): InvocationSummary | null {
  const ordered = [...invocations].sort(
    (left, right) =>
      left.created_at_ms - right.created_at_ms ||
      left.id.localeCompare(right.id),
  );
  return (
    ordered.find((invocation) => invocation.id === invocationId) ??
    ordered.at(-1) ??
    null
  );
}

export function projectAgentInvocation(
  invocation: InvocationSummary,
  sourceEvents: UserEvent[],
): AgentInvocationView {
  const events = [...sourceEvents].sort(
    (left, right) =>
      left.sequence - right.sequence ||
      left.occurred_at_ms - right.occurred_at_ms,
  );
  const reactPaths = new Set(
    events
      .filter(
        (event) =>
          event.type === "agent_output" ||
          event.type === "agent_failed",
      )
      .map((event) => workflowPathKey(event.workflow_path)),
  );
  const grouped = new Map<
    string,
    {
      kind: "react" | "llm";
      sequence: number;
      events: UserEvent[];
    }
  >();
  const blocks: AgentActivityBlock[] = [];

  for (const event of events) {
    if (!KNOWN_TYPES.has(event.type)) {
      blocks.push({
        kind: "custom",
        key: event.id,
        sequence: event.sequence,
        value: genericEvent(event),
      });
      continue;
    }
    const pathKey = workflowPathKey(event.workflow_path);
    const react = (event.workflow_path?.length ?? 0) > 0 ||
      reactPaths.has(pathKey);
    const key = react
      ? `react:${pathKey}`
      : `llm:${
          event.operator_call_id ??
          event.node_execution_id ??
          event.node_id
        }`;
    const existing = grouped.get(key);
    if (existing) {
      existing.events.push(event);
      continue;
    }
    grouped.set(key, {
      kind: react ? "react" : "llm",
      sequence: event.sequence,
      events: [event],
    });
  }

  for (const [key, group] of grouped) {
    const projection = projectStandardEvents(group.events);
    blocks.push({
      kind: group.kind,
      key,
      sequence: group.sequence,
      activity: projection.activity,
      output: projection.output,
      outputSequence: projection.outputSequence,
    });
  }
  blocks.sort((left, right) => left.sequence - right.sequence);
  return { invocation, blocks };
}

function projectStandardEvents(sourceEvents: UserEvent[]): {
  activity: AgentActivityItem[];
  output: unknown;
  outputSequence: number | null;
} {
  const events = [...sourceEvents].sort(
    (left, right) => left.sequence - right.sequence,
  );
  const messages = new Map<string, MutableMessage>();
  const tools = new Map<string, MutableTool>();
  const toolIndexKeys = new Map<string, string>();
  const genericEvents: AgentGenericEventView[] = [];
  let agentOutput: unknown = undefined;
  let agentOutputSequence: number | null = null;

  const messageFor = (event: UserEvent): MutableMessage => {
    const key =
      event.operator_call_id ??
      event.node_execution_id ??
      `message:${event.sequence}`;
    let value = messages.get(key);
    if (!value) {
      value = {
        key,
        operatorCallId: event.operator_call_id,
        firstSequence: event.sequence,
        reasoningFirstSequence: null,
        messageFirstSequence: null,
        reasoning: "",
        reasoningStartedAtMs: null,
        reasoningEndedAtMs: null,
        content: "",
        completed: false,
        occurredAtMs: event.occurred_at_ms,
      };
      messages.set(key, value);
    }
    return value;
  };

  for (const event of events) {
    const data = recordOf(event.data);
    if (event.type === "reasoning_delta") {
      const message = messageFor(event);
      message.reasoning += stringOf(data.delta);
      message.reasoningFirstSequence ??= event.sequence;
      message.reasoningStartedAtMs ??= event.occurred_at_ms;
      message.reasoningEndedAtMs = event.occurred_at_ms;
      continue;
    }
    if (event.type === "message_delta") {
      const message = messageFor(event);
      message.content += stringOf(data.delta);
      message.messageFirstSequence ??= event.sequence;
      message.occurredAtMs = event.occurred_at_ms;
      continue;
    }
    if (event.type === "message_completed") {
      const message = messageFor(event);
      message.messageFirstSequence ??= event.sequence;
      const completedMessage = recordOf(data.message);
      if (typeof completedMessage.content === "string") {
        message.content = completedMessage.content;
      }
      if (
        message.reasoning.length === 0 &&
        typeof completedMessage.reasoning_content === "string"
      ) {
        message.reasoning = completedMessage.reasoning_content;
        message.reasoningFirstSequence = event.sequence;
        message.reasoningStartedAtMs = event.occurred_at_ms;
        message.reasoningEndedAtMs = event.occurred_at_ms;
      }
      message.completed = true;
      message.occurredAtMs = event.occurred_at_ms;
      continue;
    }
    if (event.type === "tool_call_delta") {
      const index = numberOf(data.tool_call_index, 0);
      const indexKey = `${event.operator_call_id ?? event.node_execution_id}:${index}`;
      const explicitId = nullableString(data.tool_call_id);
      const key = explicitId ? `tool:${explicitId}` : `tool-index:${indexKey}`;
      const previousKey = toolIndexKeys.get(indexKey);
      const existing = previousKey ? tools.get(previousKey) : undefined;
      const tool = existing ?? createTool(key, explicitId, event);
      if (existing && previousKey !== key && explicitId) {
        tools.delete(previousKey!);
        tool.key = key;
        tool.toolCallId = explicitId;
      }
      tool.name = nullableString(data.tool_name) ?? tool.name;
      tool.rawArguments += stringOf(data.arguments_delta);
      tools.set(tool.key, tool);
      toolIndexKeys.set(indexKey, tool.key);
      continue;
    }
    if (event.type === "tool_call_requested") {
      const completeReasoning = nullableString(data.reasoning_content);
      if (completeReasoning !== null) {
        const message = messageFor(event);
        message.reasoning = completeReasoning;
        message.reasoningFirstSequence ??= event.sequence;
        message.reasoningStartedAtMs ??= event.occurred_at_ms;
        message.reasoningEndedAtMs = event.occurred_at_ms;
      }
      arrayOf(data.calls).forEach((rawCall, index) => {
        const call = recordOf(rawCall);
        const id = stringOf(call.tool_call_id);
        const key = `tool:${id || `${event.sequence}:${index}`}`;
        const indexKey = `${event.operator_call_id ?? event.node_execution_id}:${index}`;
        const streamedKey = toolIndexKeys.get(indexKey);
        const streamedTool = streamedKey
          ? tools.get(streamedKey)
          : undefined;
        const tool = streamedTool ?? createTool(key, id || null, event);
        if (streamedKey && streamedKey !== key) tools.delete(streamedKey);
        tool.key = key;
        tool.toolCallId = id || tool.toolCallId;
        tool.name = nullableString(call.name) ?? tool.name;
        tool.rawArguments = stringOf(call.raw_arguments) || tool.rawArguments;
        tool.requestedAtMs = event.occurred_at_ms;
        tools.set(key, tool);
        toolIndexKeys.set(indexKey, key);
      });
      continue;
    }
    if (event.type === "tool_result") {
      for (const rawResult of arrayOf(data.results)) {
        const result = recordOf(rawResult);
        const id = stringOf(result.tool_call_id);
        const key = `tool:${id}`;
        const tool = tools.get(key) ?? createTool(key, id || null, event);
        tool.result = result.output;
        tool.error = result.error;
        tool.completedAtMs = event.occurred_at_ms;
        tools.set(key, tool);
      }
      continue;
    }
    if (event.type === "agent_output") {
      agentOutput = data.output;
      agentOutputSequence = event.sequence;
      continue;
    }
    if (event.type === "message_aborted" || event.type === "agent_failed") {
      genericEvents.push(genericEvent(event));
      continue;
    }
  }

  const activity: AgentActivityItem[] = [];
  for (const message of messages.values()) {
    if (
      message.reasoning.length > 0 &&
      message.reasoningFirstSequence !== null
    ) {
      activity.push({
        kind: "thinking",
        key: `thinking:${message.key}`,
        sequence: message.reasoningFirstSequence,
        message,
      });
    }
    // Once Agent Output exists it is the authoritative visible answer.
    // Intermediate completed messages may be failed structured-output repair
    // attempts, so keeping them would duplicate or expose superseded output.
    if (
      message.content.length > 0 &&
      message.messageFirstSequence !== null &&
      agentOutputSequence === null
    ) {
      activity.push({
        kind: "message",
        key: `message:${message.key}`,
        sequence: message.messageFirstSequence,
        message,
      });
    }
  }
  for (const tool of tools.values()) {
    activity.push({
      kind: "tool",
      key: tool.key,
      sequence: tool.firstSequence,
      tool,
    });
  }
  for (const value of genericEvents) {
    activity.push({
      kind: "generic",
      key: value.event.id,
      sequence: value.event.sequence,
      value,
    });
  }
  // Array.sort is stable: multiple Tool Calls emitted by one UserEvent retain
  // the Provider-normalized call order within that shared sequence.
  activity.sort((left, right) => left.sequence - right.sequence);

  return {
    activity,
    output: agentOutput,
    outputSequence: agentOutputSequence,
  };
}

export function logicalUserEventKeys(event: UserEvent): string[] {
  if (DELTA_TYPES.has(event.type)) return [];
  const data = recordOf(event.data);
  if (event.type === "tool_call_requested") {
    return arrayOf(data.calls).map((rawCall, index) => {
      const call = recordOf(rawCall);
      return `tool_call:${stringOf(call.tool_call_id) || `${event.id}:${index}`}`;
    });
  }
  if (event.type === "tool_result") {
    return arrayOf(data.results).map((rawResult, index) => {
      const result = recordOf(rawResult);
      return `tool_result:${stringOf(result.tool_call_id) || `${event.id}:${index}`}`;
    });
  }
  if (event.type === "message_completed") {
    return [
      `message:${event.operator_call_id ?? event.node_execution_id ?? event.id}`,
    ];
  }
  if (event.type === "agent_output") {
    return [`agent_output:${event.invocation_id}`];
  }
  return [`event:${event.id}`];
}

function createTool(
  key: string,
  id: string | null,
  event: UserEvent,
): MutableTool {
  return {
    key,
    toolCallId: id,
    name: null,
    rawArguments: "",
    result: undefined,
    error: null,
    requestedAtMs: null,
    completedAtMs: null,
    firstSequence: event.sequence,
  };
}

function genericEvent(event: UserEvent): AgentGenericEventView {
  return {
    event,
    tone: (
      event.type.endsWith("_failed") ||
      event.type.includes("error") ||
      event.type === "message_aborted"
    ) ? "error" : "default",
  };
}

function workflowPathKey(path: string[] | undefined): string {
  return path && path.length > 0 ? path.join("\u0000") : "<root>";
}

function recordOf(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function arrayOf(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function stringOf(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function nullableString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function numberOf(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}
