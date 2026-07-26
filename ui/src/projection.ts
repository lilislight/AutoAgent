import type {
  ProjectedEdge,
  ProjectedNode,
  ProjectedNodeExecution,
  RuntimeEvent,
  RuntimeProjection,
  RuntimeState,
} from "./types";

export function projectEvents(
  invocationId: string,
  source: RuntimeEvent[],
  throughSequence?: number,
  base?: RuntimeProjection,
): RuntimeProjection {
  let invocationState: RuntimeState = base?.invocation_state ?? "created";
  let appliedSequence = base?.through_sequence ?? 0;
  const nodeExecutions = structuredClone(base?.node_executions ?? {});
  const nodes = structuredClone(base?.nodes ?? {});
  const edges = structuredClone(base?.edges ?? {});
  const operatorStates = { ...(base?.operator_states ?? {}) };
  const activeWaits = structuredClone(base?.active_waits ?? {});
  let latestPhase = base?.latest_phase ?? null;

  // API pages and mergeEvents keep this journal in sequence order. Avoid
  // re-sorting it on every replay cursor movement.
  for (const event of source) {
    if (event.sequence <= appliedSequence) continue;
    if (throughSequence !== undefined && event.sequence > throughSequence) break;
    appliedSequence = event.sequence;
    const name = event.event_name;
    const payload = event.payload;

    if (name.startsWith("invocation.")) {
      invocationState = event.status ?? name.split(".").at(-1) ?? invocationState;
      continue;
    }
    if (name.startsWith("node.")) {
      applyNodeEvent(nodes, nodeExecutions, event);
      continue;
    }
    if (name === "edge.evaluated") {
      const edgeId = String(payload.edge_id ?? event.subject_id);
      const previous = edges[edgeId];
      const selected = Boolean(payload.selected);
      const state = String(payload.state ?? event.status ?? "evaluated");
      edges[edgeId] = {
        edge_id: edgeId,
        state,
        selected,
        evaluation_count: (previous?.evaluation_count ?? 0) + 1,
        selected_count: (previous?.selected_count ?? 0) + Number(selected),
        skipped_count: (previous?.skipped_count ?? 0) + Number(state === "skipped"),
        failed_count: (previous?.failed_count ?? 0) + Number(state === "failed"),
        source_execution_id: nullableString(payload.source_execution_id),
        target_node_id: nullableString(payload.target_node_id),
      };
      continue;
    }
    if (name === "operator_call.completed") {
      const callId = String(payload.operator_call_id ?? event.subject_id);
      operatorStates[callId] = String(payload.state ?? event.status ?? "completed");
      const execution = nodeExecutions[String(payload.node_execution_id ?? "")];
      if (execution) {
        const error = asRecordOrNull(payload.error);
        const reason = payload.reason == null ? null : String(payload.reason);
        const summary = asRecordOrNull(payload.summary);
        const operatorIds = (
          (payload.operator_ids as unknown[] | undefined) ?? []
        ).map(String);
        const call = {
          id: callId,
          event_sequence: event.sequence,
          node_execution_id: execution.execution_id,
          operator_id: String(
            payload.operator_id ??
            (operatorIds.length > 0 ? operatorIds.join(", ") : undefined) ??
            payload.kind ??
            "operator"
          ),
          kind: String(payload.kind ?? "direct"),
          reason,
          state: operatorStates[callId],
          error,
          summary,
          occurred_at_ms: event.occurred_at_ms,
          elapsed_ns: event.elapsed_ns,
          timing: event.timing,
        };
        execution.operator_calls = [
          ...(execution.operator_calls ?? []).filter((value) => value.id !== callId),
          call,
        ];
        execution.operator_call_count = (execution.operator_call_count ?? 0) + 1;
        execution.failed_operator_call_count =
          (execution.failed_operator_call_count ?? 0) +
          Number(operatorStates[callId] !== "completed");
        execution.retry_count = (execution.retry_count ?? 0) + Number(reason === "retry");
        execution.fallback_count =
          (execution.fallback_count ?? 0) + Number(reason === "fallback");
        execution.timeout_count =
          (execution.timeout_count ?? 0) + Number(error?.code === "OPERATOR_TIMEOUT");
        const node = nodes[execution.node_id];
        if (node) Object.assign(node, nodeOperatorSummary(execution));
      }
      continue;
    }
    if (name === "wait.created") {
      const waits = (payload.waits as Array<Record<string, unknown>> | undefined) ?? [];
      if (waits.length > 0) {
        for (const wait of waits) {
          const waitKey = String(wait.wait_key);
          activeWaits[waitKey] = {
            ...wait,
            wait_key: waitKey,
            created_at_ms: event.occurred_at_ms,
          };
        }
      } else {
        for (const waitKey of (payload.wait_keys as unknown[] | undefined) ?? []) {
          activeWaits[String(waitKey)] = {
            wait_key: String(waitKey),
            created_at_ms: event.occurred_at_ms,
          };
        }
      }
      invocationState = "waiting";
      continue;
    }
    if (name === "wait.resumed" && payload.wait_key !== undefined) {
      delete activeWaits[String(payload.wait_key)];
      continue;
    }
    if (event.event_type === "phase") {
      latestPhase = {
        event_name: name,
        subject_id: event.subject_id,
        status: event.status,
        occurred_at_ms: event.occurred_at_ms,
        elapsed_ns: event.elapsed_ns,
      };
    }
  }
  return {
    schema_version: base?.schema_version ?? 2,
    invocation_id: invocationId,
    through_sequence: appliedSequence,
    invocation_state: invocationState,
    node_executions: nodeExecutions,
    nodes,
    edges,
    operator_states: operatorStates,
    active_waits: activeWaits,
    latest_phase: latestPhase,
  };
}

function applyNodeEvent(
  nodes: Record<string, ProjectedNode>,
  executions: Record<string, ProjectedNodeExecution>,
  event: RuntimeEvent,
) {
  const payload = event.payload;
  const nodeId = String(payload.node_id ?? event.subject_id);
  const state = String(event.status ?? event.event_name.split(".").at(-1) ?? "created");
  const executionId = nullableString(payload.node_execution_id);
  if (executionId) {
    const previous = executions[executionId];
    executions[executionId] = {
      execution_id: executionId,
      node_id: nodeId,
      sequence: previous?.sequence ?? countExecutions(executions, nodeId) + 1,
      first_event_sequence: previous?.first_event_sequence ?? event.sequence,
      state,
      input: previous?.input ?? null,
      output: previous?.output ?? null,
      error: (payload.error as Record<string, unknown> | null) ?? previous?.error ?? null,
      started_at_ms: state === "running" ? event.occurred_at_ms : previous?.started_at_ms,
      ended_at_ms: isTerminal(state) ? event.occurred_at_ms : null,
      elapsed_ns: event.elapsed_ns,
      timing: event.timing,
      operator_call_count: previous?.operator_call_count ?? 0,
      failed_operator_call_count: previous?.failed_operator_call_count ?? 0,
      retry_count: previous?.retry_count ?? 0,
      fallback_count: previous?.fallback_count ?? 0,
      timeout_count: previous?.timeout_count ?? 0,
      operator_calls: previous?.operator_calls ?? [],
    };
  }
  const matching = Object.values(executions).filter((value) => value.node_id === nodeId);
  const latest = matching.sort((a, b) =>
    a.sequence === b.sequence
      ? b.execution_id.localeCompare(a.execution_id)
      : b.sequence - a.sequence,
  )[0];
  nodes[nodeId] = {
    node_id: nodeId,
    state,
    latest_execution_id: latest?.execution_id ?? null,
    execution_count: Math.max(matching.length, event.event_name === "node.skipped" ? 1 : 0),
    latest_error: payload.error,
    latest_elapsed_ns: event.elapsed_ns,
    latest_timing: event.timing,
    ...nodeOperatorSummary(latest),
  };
}

function nodeOperatorSummary(
  execution: ProjectedNodeExecution | undefined,
): Pick<
  ProjectedNode,
  | "operator_call_count"
  | "failed_operator_call_count"
  | "retry_count"
  | "fallback_count"
  | "timeout_count"
  | "parallel_call_count"
  | "latest_operator_kind"
> {
  const calls = execution?.operator_calls ?? [];
  return {
    operator_call_count: execution?.operator_call_count ?? 0,
    failed_operator_call_count: execution?.failed_operator_call_count ?? 0,
    retry_count: execution?.retry_count ?? 0,
    fallback_count: execution?.fallback_count ?? 0,
    timeout_count: execution?.timeout_count ?? 0,
    parallel_call_count: calls
      .filter((call) => ["map", "replication"].includes(call.kind))
      .reduce((count, call) => count + Number(call.summary?.call_count ?? 0), 0),
    latest_operator_kind: calls.at(-1)?.kind ?? null,
  };
}

function countExecutions(
  executions: Record<string, ProjectedNodeExecution>,
  nodeId: string,
): number {
  return Object.values(executions).filter((value) => value.node_id === nodeId).length;
}

function nullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

function asRecordOrNull(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object"
    ? value as Record<string, unknown>
    : null;
}

function isTerminal(state: string): boolean {
  return ["completed", "failed", "cancelled", "interrupted", "skipped"].includes(state);
}
