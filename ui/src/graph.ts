import type { WorkflowEdge, WorkflowNode } from "./types";

export interface PositionedNode extends WorkflowNode {
  x: number;
  y: number;
  rank: number;
}

export interface GraphLayout {
  nodes: PositionedNode[];
  edges: WorkflowEdge[];
  width: number;
  height: number;
}

const NODE_WIDTH = 176;
const NODE_HEIGHT = 72;
const COLUMN_GAP = 96;
const ROW_GAP = 34;

export function layoutGraph(nodes: WorkflowNode[], edges: WorkflowEdge[]): GraphLayout {
  const ids = new Set(nodes.map((node) => node.id));
  const incoming = new Map(nodes.map((node) => [node.id, 0]));
  const outgoing = new Map(nodes.map((node) => [node.id, [] as string[]]));
  for (const edge of edges) {
    if (!ids.has(edge.source) || !ids.has(edge.target) || edge.source === edge.target) continue;
    incoming.set(edge.target, (incoming.get(edge.target) ?? 0) + 1);
    outgoing.get(edge.source)?.push(edge.target);
  }

  const rank = new Map(nodes.map((node) => [node.id, 0]));
  const queue = nodes.filter((node) => incoming.get(node.id) === 0).map((node) => node.id);
  const visited = new Set<string>();
  while (queue.length) {
    const id = queue.shift()!;
    if (visited.has(id)) continue;
    visited.add(id);
    for (const target of outgoing.get(id) ?? []) {
      rank.set(target, Math.max(rank.get(target) ?? 0, (rank.get(id) ?? 0) + 1));
      incoming.set(target, (incoming.get(target) ?? 1) - 1);
      if (incoming.get(target) === 0) queue.push(target);
    }
  }
  // Loop members left by topological traversal get a stable position without
  // trying to reinterpret the compiler's loop semantics in the browser.
  for (const node of nodes) {
    if (!visited.has(node.id)) {
      const predecessors = edges.filter((edge) => edge.target === node.id);
      const inferred = Math.max(0, ...predecessors.map((edge) => (rank.get(edge.source) ?? 0) + 1));
      rank.set(node.id, inferred);
    }
  }

  const rows = new Map<number, number>();
  const positioned = nodes.map((node) => {
    const nodeRank = rank.get(node.id) ?? 0;
    const row = rows.get(nodeRank) ?? 0;
    rows.set(nodeRank, row + 1);
    return {
      ...node,
      rank: nodeRank,
      x: 28 + nodeRank * (NODE_WIDTH + COLUMN_GAP),
      y: 28 + row * (NODE_HEIGHT + ROW_GAP),
    };
  });
  const maxRank = Math.max(0, ...positioned.map((node) => node.rank));
  const maxRows = Math.max(1, ...rows.values());
  return {
    nodes: positioned,
    edges,
    width: 56 + (maxRank + 1) * NODE_WIDTH + maxRank * COLUMN_GAP,
    height: 56 + maxRows * NODE_HEIGHT + (maxRows - 1) * ROW_GAP,
  };
}

export const graphGeometry = { nodeWidth: NODE_WIDTH, nodeHeight: NODE_HEIGHT };
