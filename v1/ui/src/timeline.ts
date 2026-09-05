import type {
  InvocationDetail,
  RuntimeProjection,
  TimelineSpan,
  TimelineView,
} from "./types.js";

export function buildTimelineView(
  invocation: InvocationDetail,
  projection: RuntimeProjection,
): TimelineView {
  const spans = Object.values(projection.node_executions).flatMap((execution) => {
    const nodeSpan: TimelineSpan = {
      id: execution.execution_id,
      kind: "node_execution",
      parent_id: null,
      node_id: execution.node_id,
      label: execution.node_id,
      state: execution.state,
      sequence: execution.first_event_sequence ?? execution.sequence,
      started_at_ms: execution.started_at_ms ?? invocation.created_at_ms,
      ended_at_ms: execution.ended_at_ms ?? null,
      duration_ms:
        execution.elapsed_ns === null || execution.elapsed_ns === undefined
          ? null
          : execution.elapsed_ns / 1_000_000,
      omitted_child_count: Math.max(
        0,
        (execution.operator_call_count ?? 0) -
          (execution.operator_calls?.length ?? 0),
      ),
    };
    const callSpans: TimelineSpan[] = (execution.operator_calls ?? []).map(
      (call) => ({
        id: call.id,
        kind: "operator_call",
        parent_id: execution.execution_id,
        node_id: execution.node_id,
        label: `${call.operator_id} · call ${call.call_no}`,
        state: call.state,
        sequence: call.event_sequence,
        started_at_ms: call.started_at_ms ?? call.occurred_at_ms,
        ended_at_ms: call.occurred_at_ms,
        duration_ms:
          call.elapsed_ns == null ? null : call.elapsed_ns / 1_000_000,
      }),
    );
    return [nodeSpan, ...callSpans];
  });
  return {
    invocation_id: invocation.id,
    started_at_ms: invocation.created_at_ms,
    ended_at_ms:
      ["completed", "failed", "cancelled", "interrupted"].includes(invocation.state)
        ? invocation.updated_at_ms
        : null,
    spans,
  };
}

export function visibleTimelineSpans(
  spans: TimelineSpan[],
  collapsedNodeExecutionIds: ReadonlySet<string>,
): TimelineSpan[] {
  const nodes = spans
    .filter((span) => span.kind === "node_execution")
    .sort(compareTimelineSpans);
  const calls = spans.filter((span) => span.kind === "operator_call");
  return nodes.flatMap((node) => [
    node,
    ...(collapsedNodeExecutionIds.has(node.id)
      ? []
      : calls
          .filter((call) => call.parent_id === node.id)
          .sort(compareTimelineSpans)),
  ]);
}

function compareTimelineSpans(left: TimelineSpan, right: TimelineSpan): number {
  if (left.sequence !== right.sequence) return left.sequence - right.sequence;
  if (left.started_at_ms !== right.started_at_ms) {
    return left.started_at_ms - right.started_at_ms;
  }
  return left.id.localeCompare(right.id);
}
