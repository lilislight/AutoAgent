import type { ExecutionTrace } from './types.js';

export type ExecutionEdgeState = 'active' | 'waiting' | 'paused' | 'taken' | 'not-taken' | 'not-evaluated';
const ACTIVE = new Set(['ready', 'running', 'waiting']);
const TERMINAL = new Set(['completed', 'failed', 'cancelled', 'lost']);

export function executionGraphStates(trace: ExecutionTrace) {
  const nodes = new Map(trace.nodes.map(node => [node.node_id,
    trace.invocation.control_state === 'paused' && ACTIVE.has(node.status) ? 'paused' : node.status]));
  const edges = new Map<string, ExecutionEdgeState>(trace.edges.map(edge => [edge.edge_id, edge.condition_result ? 'taken' : 'not-taken']));
  const activeEdges = new Map<string, ExecutionEdgeState>();
  const priority: Record<ExecutionEdgeState, number> = { paused: 0, active: 1, waiting: 2, taken: 3, 'not-taken': 4, 'not-evaluated': 5 };
  for (const route of trace.active_routes) {
    const state = trace.invocation.control_state === 'paused' ? 'paused' : route.status === 'running' ? 'active' : route.status === 'waiting' ? 'waiting' : 'taken';
    if (priority[state] < priority[activeEdges.get(route.edge_id) ?? 'not-evaluated']) activeEdges.set(route.edge_id, state);
  }
  for (const [id, state] of activeEdges) edges.set(id, state);
  return { nodes, edges, unrecordedNode: TERMINAL.has(trace.invocation.status ?? '') ? 'not-run' : 'not-started' };
}

export function executionStateLabel(state: string): string {
  return state.split('-').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
}
