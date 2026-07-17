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
): RuntimeProjection {
  let invocationState: RuntimeState = "created";
  let appliedSequence = 0;
  const nodeExecutions: Record<string, ProjectedNodeExecution> = {};
  const edges: Record<string, ProjectedEdge> = {};
  const operatorStates: Record<string, RuntimeState> = {};

  const events = [...source].sort((left, right) => left.sequence - right.sequence);
  for (const event of events) {
    if (throughSequence !== undefined && event.sequence > throughSequence) break;
    appliedSequence = event.sequence;
    if (event.type === "invocation.state_changed") {
      invocationState = String(event.payload.to ?? invocationState);
      continue;
    }
    if (
      event.type === "node.execution_created" &&
      event.entity_id &&
      event.node_id
    ) {
      nodeExecutions[event.entity_id] = {
        execution_id: event.entity_id,
        node_id: event.node_id,
        sequence: Number(event.payload.sequence ?? 0),
        state: String(event.payload.state ?? "created"),
        input: null,
        output: null,
        error: null,
      };
      continue;
    }
    if (event.type === "node.state_changed" && event.entity_id && event.node_id) {
      const previous = nodeExecutions[event.entity_id];
      const state = String(event.payload.to ?? "created");
      nodeExecutions[event.entity_id] = {
        execution_id: event.entity_id,
        node_id: event.node_id,
        sequence: previous?.sequence ?? 0,
        state,
        input: state === "running" ? event.payload.input : previous?.input,
        output: state === "completed" ? event.payload.output : previous?.output,
        error: (event.payload.error as Record<string, unknown> | null) ?? null,
      };
      continue;
    }
    if (event.type === "operator.call_started" && event.entity_id) {
      operatorStates[event.entity_id] = "running";
      continue;
    }
    if (event.type === "operator.call_finished" && event.entity_id) {
      operatorStates[event.entity_id] = String(event.payload.state ?? "completed");
      continue;
    }
    if (event.type === "edge.evaluated" && event.edge_id) {
      const previous = edges[event.edge_id];
      edges[event.edge_id] = {
        edge_id: event.edge_id,
        state: String(event.payload.state ?? "evaluated"),
        selected: Boolean(event.payload.selected),
        evaluation_count: (previous?.evaluation_count ?? 0) + 1,
        source_execution_id: asNullableString(event.payload.node_execution_id),
        target_node_id: asNullableString(event.payload.target_node_id),
      };
    }
  }

  const grouped = new Map<string, ProjectedNodeExecution[]>();
  for (const execution of Object.values(nodeExecutions)) {
    const values = grouped.get(execution.node_id) ?? [];
    values.push(execution);
    grouped.set(execution.node_id, values);
  }
  const nodes: Record<string, ProjectedNode> = {};
  for (const [nodeId, executions] of grouped) {
    const latest = [...executions].sort((left, right) => {
      if (left.sequence !== right.sequence) return right.sequence - left.sequence;
      return right.execution_id.localeCompare(left.execution_id);
    })[0];
    nodes[nodeId] = {
      node_id: nodeId,
      state: latest.state,
      latest_execution_id: latest.execution_id,
      execution_count: executions.length,
    };
  }

  return {
    invocation_id: invocationId,
    through_sequence: appliedSequence,
    invocation_state: invocationState,
    node_executions: nodeExecutions,
    nodes,
    edges,
    operator_states: operatorStates,
  };
}

function asNullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}
