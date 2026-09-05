import type {
  RuntimeStateRecord,
  TraceEvent,
  WorkflowSnapshot,
} from "./types";

export type RuntimeProjectionSource = "state" | "trace" | "none";

export interface NodeRuntimeProjection {
  nodeId: string;
  latestStatus: string;
  occurrenceCount: number;
  statusCounts: Readonly<Record<string, number>>;
}

export interface EdgeRuntimeProjection {
  edgeId: string;
  resolutionCount: number;
  selectedCount: number;
  skippedCount: number;
  latestSelected: boolean;
}

export interface RuntimeSpan {
  id: string;
  kind: "node" | "operator";
  occurrenceId: string;
  nodeId: string | null;
  operatorId: string | null;
  unitIndex: number | null;
  status: string;
  startedAtNs: string | null;
  completedAtNs: string | null;
  durationNs: string | null;
}

export interface RuntimeProjection {
  source: RuntimeProjectionSource;
  nodes: ReadonlyMap<string, NodeRuntimeProjection>;
  edges: ReadonlyMap<string, EdgeRuntimeProjection>;
  spans: readonly RuntimeSpan[];
}

/**
 * Project the durable Runtime State used by graph and span views. Trace is
 * deliberately consulted only while State has not loaded yet.
 */
export function projectRuntime(
  state: RuntimeStateRecord | null,
  traceFallback: readonly TraceEvent[] = [],
  workflow: WorkflowSnapshot | null = null,
): RuntimeProjection {
  return state === null
    ? projectTraceFallback(traceFallback)
    : projectRuntimeState(state, workflow);
}

export function projectRuntimeState(
  state: RuntimeStateRecord,
  workflow: WorkflowSnapshot | null = null,
): RuntimeProjection {
  const nodes = new Map<string, MutableNodeProjection>();
  const edges = new Map<string, MutableEdgeProjection>();
  const spans: RuntimeSpan[] = [];
  const occurrencesById = new Map<string, string>();
  const occurrenceDecisions: {
    occurrenceId: string;
    nodeId: string;
    status: string;
  }[] = [];
  const selectedDecisions = new Set<string>();
  const scheduler = record(record(state.invocation)?.scheduler);

  for (const [recordId, value] of entries(scheduler?.occurrences)) {
    const occurrence = record(value);
    if (!occurrence) continue;
    const occurrenceId = stringValue(occurrence.id) ?? recordId;
    const nodeId = stringValue(occurrence.node_id);
    if (!nodeId) continue;
    const status = stringValue(occurrence.status) ?? "unknown";
    occurrencesById.set(occurrenceId, nodeId);
    occurrenceDecisions.push({ occurrenceId, nodeId, status });
    incrementNode(nodes, nodeId, status, occurrenceId);

    spans.push({
      id: `node:${occurrenceId}`,
      kind: "node",
      occurrenceId,
      nodeId,
      operatorId: null,
      unitIndex: null,
      status,
      startedAtNs: integerString(occurrence.started_at_ns),
      completedAtNs: integerString(occurrence.completed_at_ns),
      durationNs: duration(
        integerString(occurrence.started_at_ns),
        integerString(occurrence.completed_at_ns),
      ),
    });

    // Selected resolutions are consumed when their target occurrence is
    // created. Activations are the durable representation that survives that
    // consumption, so include them in the historical edge projection.
    for (const activationValue of array(occurrence.activations)) {
      const activation = record(activationValue);
      const edgeId = stringValue(activation?.edge_id);
      if (activation && edgeId) {
        const sourceOccurrenceId = stringValue(activation.source_occurrence_id);
        if (sourceOccurrenceId) {
          selectedDecisions.add(edgeDecisionIdentity(edgeId, sourceOccurrenceId));
        }
        if (!workflow) {
          incrementEdge(edges, edgeId, true, activationIdentity(activation));
        }
      }
    }
  }

  for (const [resolutionId, value] of [
    ...entries(scheduler?.resolutions),
    ...entries(scheduler?.boundary_resolutions),
  ]) {
    const resolution = record(value);
    const edgeId = stringValue(resolution?.edge_id);
    const selected = booleanValue(resolution?.selected);
    if (!edgeId || selected === null) continue;
    const activation = record(resolution?.activation);
    const sourceOccurrenceId = stringValue(activation?.source_occurrence_id);
    if (selected && sourceOccurrenceId) {
      selectedDecisions.add(edgeDecisionIdentity(edgeId, sourceOccurrenceId));
    }
    if (!workflow) {
      incrementEdge(edges, edgeId, selected, `resolution:${resolutionId}`);
    }
  }

  if (workflow) {
    const outgoing = new Map<string, { id: string }[]>();
    for (const edge of workflow.definition.edges ?? []) {
      if (!edge.id) continue;
      const edgesForNode = outgoing.get(edge.source) ?? [];
      edgesForNode.push({ id: edge.id });
      outgoing.set(edge.source, edgesForNode);
    }
    for (const occurrence of occurrenceDecisions) {
      if (!routesOutgoing(occurrence.status)) continue;
      for (const edge of outgoing.get(occurrence.nodeId) ?? []) {
        const identity = edgeDecisionIdentity(edge.id, occurrence.occurrenceId);
        incrementEdge(edges, edge.id, selectedDecisions.has(identity), identity);
      }
    }
  }

  for (const [recordId, value] of entries(scheduler?.operator_calls)) {
    const call = record(value);
    if (!call) continue;
    const callId = stringValue(call.id) ?? recordId;
    const occurrenceId = stringValue(call.occurrence_id);
    if (!occurrenceId) continue;
    const startedAtNs = integerString(call.started_at_ns);
    const completedAtNs = integerString(call.completed_at_ns);
    spans.push({
      id: `operator:${callId}`,
      kind: "operator",
      occurrenceId,
      nodeId: occurrencesById.get(occurrenceId) ?? null,
      operatorId: stringValue(call.operator_id),
      unitIndex: integerValue(call.unit_index),
      status: stringValue(call.status) ?? "unknown",
      startedAtNs,
      completedAtNs,
      durationNs: duration(startedAtNs, completedAtNs),
    });
  }

  return {
    source: "state",
    nodes: finalizedNodes(nodes),
    edges: finalizedEdges(edges),
    spans: spans.sort(compareSpans),
  };
}

function projectTraceFallback(trace: readonly TraceEvent[]): RuntimeProjection {
  if (trace.length === 0) {
    return { source: "none", nodes: new Map(), edges: new Map(), spans: [] };
  }
  const nodes = new Map<string, MutableNodeProjection>();
  const seenOccurrences = new Set<string>();
  for (const event of trace) {
    const nodeId = event.subject_ids.node_id;
    if (!nodeId) continue;
    const occurrenceId =
      event.subject_ids.occurrence_id ?? `trace-node:${nodeId}`;
    const status = event.status ?? event.kind.split(".").at(-1) ?? "seen";
    const firstForOccurrence = !seenOccurrences.has(occurrenceId);
    seenOccurrences.add(occurrenceId);
    incrementNode(nodes, nodeId, status, occurrenceId, firstForOccurrence);
  }
  return {
    source: "trace",
    nodes: finalizedNodes(nodes),
    edges: new Map(),
    spans: [],
  };
}

interface MutableNodeProjection {
  nodeId: string;
  latestStatus: string;
  occurrenceCount: number;
  statusCounts: Record<string, number>;
  occurrenceStatuses: Map<string, string>;
}

function incrementNode(
  nodes: Map<string, MutableNodeProjection>,
  nodeId: string,
  status: string,
  occurrenceId: string,
  countOccurrence = true,
): void {
  let node = nodes.get(nodeId);
  if (!node) {
    node = {
      nodeId,
      latestStatus: status,
      occurrenceCount: 0,
      statusCounts: {},
      occurrenceStatuses: new Map(),
    };
    nodes.set(nodeId, node);
  }
  const previous = node.occurrenceStatuses.get(occurrenceId);
  if (previous !== undefined) {
    const remaining = (node.statusCounts[previous] ?? 1) - 1;
    if (remaining > 0) node.statusCounts[previous] = remaining;
    else delete node.statusCounts[previous];
  } else if (countOccurrence) {
    node.occurrenceCount += 1;
  }
  node.occurrenceStatuses.set(occurrenceId, status);
  node.statusCounts[status] = (node.statusCounts[status] ?? 0) + 1;
  node.latestStatus = status;
}

function finalizedNodes(
  nodes: Map<string, MutableNodeProjection>,
): ReadonlyMap<string, NodeRuntimeProjection> {
  return new Map(
    [...nodes].map(([nodeId, value]) => [
      nodeId,
      {
        nodeId,
        latestStatus: value.latestStatus,
        occurrenceCount: value.occurrenceCount,
        statusCounts: { ...value.statusCounts },
      },
    ]),
  );
}

interface MutableEdgeProjection {
  edgeId: string;
  resolutionCount: number;
  selectedCount: number;
  skippedCount: number;
  latestSelected: boolean;
  identities: Set<string>;
}

function incrementEdge(
  edges: Map<string, MutableEdgeProjection>,
  edgeId: string,
  selected: boolean,
  identity: string,
): void {
  let edge = edges.get(edgeId);
  if (!edge) {
    edge = {
      edgeId,
      resolutionCount: 0,
      selectedCount: 0,
      skippedCount: 0,
      latestSelected: selected,
      identities: new Set(),
    };
    edges.set(edgeId, edge);
  }
  if (edge.identities.has(identity)) return;
  edge.identities.add(identity);
  edge.resolutionCount += 1;
  if (selected) edge.selectedCount += 1;
  else edge.skippedCount += 1;
  edge.latestSelected = selected;
}

function finalizedEdges(
  edges: Map<string, MutableEdgeProjection>,
): ReadonlyMap<string, EdgeRuntimeProjection> {
  return new Map(
    [...edges].map(([edgeId, value]) => [
      edgeId,
      {
        edgeId,
        resolutionCount: value.resolutionCount,
        selectedCount: value.selectedCount,
        skippedCount: value.skippedCount,
        latestSelected: value.latestSelected,
      },
    ]),
  );
}

function activationIdentity(value: Record<string, unknown>): string {
  return [
    "activation",
    stringValue(value.edge_id) ?? "",
    stringValue(value.source_occurrence_id) ?? "",
    stringValue(value.target_node_id) ?? "",
  ].join(":");
}

function edgeDecisionIdentity(edgeId: string, sourceOccurrenceId: string): string {
  return JSON.stringify([edgeId, sourceOccurrenceId]);
}

function routesOutgoing(status: string): boolean {
  return status === "completed" || status === "failed" || status === "skipped";
}

function compareSpans(left: RuntimeSpan, right: RuntimeSpan): number {
  const leftStart = bigintValue(left.startedAtNs);
  const rightStart = bigintValue(right.startedAtNs);
  if (leftStart !== null && rightStart !== null && leftStart !== rightStart) {
    return leftStart < rightStart ? -1 : 1;
  }
  if (leftStart !== null) return -1;
  if (rightStart !== null) return 1;
  if (left.kind !== right.kind) return left.kind === "node" ? -1 : 1;
  return left.id.localeCompare(right.id);
}

function duration(startedAtNs: string | null, completedAtNs: string | null): string | null {
  const start = bigintValue(startedAtNs);
  const completed = bigintValue(completedAtNs);
  if (start === null || completed === null || completed < start) return null;
  return String(completed - start);
}

function entries(value: unknown): [string, unknown][] {
  const candidate = record(value);
  return candidate ? Object.entries(candidate) : [];
}

function array(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function booleanValue(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function integerValue(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) ? value : null;
}

function integerString(value: unknown): string | null {
  if (typeof value === "string" && /^-?\d+$/.test(value)) return value;
  if (typeof value === "number" && Number.isSafeInteger(value)) return String(value);
  return null;
}

function bigintValue(value: string | null): bigint | null {
  if (value === null) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}
