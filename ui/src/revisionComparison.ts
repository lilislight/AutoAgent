import type { ComparisonObject, RevisionChange, RevisionChangeStatus, WorkflowGraphData } from './types.js';

export interface ComparisonMarks { nodes: Record<string, RevisionChangeStatus>; edges: Record<string, RevisionChangeStatus>; aliases: Record<string, string> }
export function comparisonGraph(base: WorkflowGraphData, target: WorkflowGraphData, changes: RevisionChange[]) {
  const nodes = new Map(target.definition.nodes.map(node => [node.id, node]));
  for (const node of base.definition.nodes) if (!nodes.has(node.id)) nodes.set(node.id, node);
  const edges = new Map(target.definition.edges.map(edge => [edge.id, edge]));
  const marks: ComparisonMarks = { nodes: {}, edges: {}, aliases: {} };
  for (const change of changes) if (change.kind !== 'workflow') marks[change.kind === 'node' ? 'nodes' : 'edges'][change.id] = change.status;
  for (const edge of base.definition.edges) {
    const current = edges.get(edge.id);
    if (!current) edges.set(edge.id, edge);
    else if (current.source !== edge.source || current.target !== edge.target) {
      // An old connection gets a collision-free drawing ID and shares one Inspector.
      let id = `${edge.id}:base`;
      while (edges.has(id) || base.definition.edges.some(item => item.id === id)) id += ':base';
      edges.set(id, { ...edge, id }); marks.edges[id] = 'removed'; marks.aliases[id] = edge.id;
    }
  }
  return { marks, workflow: { ...target, definition_hash: `compare:${base.definition_hash}:${target.definition_hash}`,
    definition: { ...target.definition, nodes: [...nodes.values()], edges: [...edges.values()] } } };
}

/** Keep each Revision's real objects while routing both through the union layout. */
export function splitComparisonGraphs(base: WorkflowGraphData, target: WorkflowGraphData, drawing: ReturnType<typeof comparisonGraph>) {
  const oldIds = new Map(Object.entries(drawing.marks.aliases).map(([alias, original]) => [original, alias]));
  const baseGraph = { ...base, definition: { ...base.definition,
    edges: base.definition.edges.map(edge => oldIds.has(edge.id) ? { ...edge, id: oldIds.get(edge.id)! } : edge),
  } };
  const baseMarks: ComparisonMarks = { nodes: drawing.marks.nodes, edges: { ...drawing.marks.edges }, aliases: drawing.marks.aliases };
  for (const [original, alias] of oldIds) baseMarks.edges[alias] = drawing.marks.edges[original];
  return { base: { workflow: baseGraph, marks: baseMarks }, target: { workflow: target, marks: drawing.marks } };
}

export interface DefinitionDifference { path: string; base: { present: boolean; value: unknown }; target: { present: boolean; value: unknown } }
function equalJSON(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object' || Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every(key => Object.hasOwn(b, key) && equalJSON((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key]));
}
export function definitionDifferences(object: ComparisonObject): DefinitionDifference[] {
  const result: DefinitionDifference[] = [];
  function visit(left: ComparisonObject['base'], right: ComparisonObject['target'], path: string) {
    if (left.present && right.present && Object.is(left.value, right.value)) return;
    const a = left.value, b = right.value;
    if (left.present && right.present && a && b && typeof a === 'object' && typeof b === 'object' && !Array.isArray(a) && !Array.isArray(b)) {
      for (const key of [...new Set([...Object.keys(a), ...Object.keys(b)])].sort()) visit(
        { present: Object.hasOwn(a, key), value: (a as Record<string, unknown>)[key] },
        { present: Object.hasOwn(b, key), value: (b as Record<string, unknown>)[key] }, `${path}/${key.replaceAll('~','~0').replaceAll('/','~1')}`);
    } else if (!left.present || !right.present || !equalJSON(a,b)) result.push({ path: path || '/', base:left, target:right });
  }
  visit(object.base, object.target, '');
  return result;
}
