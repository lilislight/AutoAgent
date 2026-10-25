/** Exact authoring ownership and its visible (possibly folded) graph. */
import type { GraphDefinition, GraphEdge, WorkflowGroup } from './types.js';
import type { Point } from './layout.js';
import { GRAPH_NODE_WIDTH, GRAPH_NODE_HEIGHT } from './graphRouting.js';
export const groupNodeId = (id: string) => `@group:${id}`;
export const groupPortId = (side: 'in' | 'out', id: string) => JSON.stringify([side, id]);
export const GROUP_PADDING = { top: 54, left: 28, right: 28, bottom: 28 };
export interface GroupPort { id: string; nodeId: string; side: 'in' | 'out'; label: string; y: number }
export interface VisibleEdge extends GraphEdge { sourceHandle?: string; targetHandle?: string }
export interface GroupPresentation {
  groups: WorkflowGroup[]; owners: Map<string, string>; allMembers: Map<string, Set<string>>;
  nodes: GraphDefinition['nodes']; edges: VisibleEdge[];
  ports: Map<string, GroupPort[]>; sizes: Map<string, { width: number; height: number }>;
  representedBy: Map<string, string>;
}
export function workflowGroups(definition: GraphDefinition, folded: ReadonlySet<string> = new Set()): GroupPresentation {
  const groups = definition.groups ?? [], byId = new Map(groups.map(g => [g.id, g]));
  const owners = new Map<string, string>(), allMembers = new Map<string, Set<string>>();
  const prefixes = new Map<string, string>();
  function prefix(group: WorkflowGroup): string {
    if (!prefixes.has(group.id)) prefixes.set(group.id, (group.parent_group_id ? prefix(byId.get(group.parent_group_id)!) : '') + group.label + '.');
    return prefixes.get(group.id)!;
  }
  for (const group of groups) {
    for (const id of group.node_ids) owners.set(id, group.id);
    allMembers.set(group.id, new Set(group.node_ids));
  }
  // Membership comes from explicit metadata, never by splitting Node IDs.
  for (const group of groups) for (const id of group.node_ids) {
    let parent = group.parent_group_id;
    while (parent) { allMembers.get(parent)?.add(id); parent = byId.get(parent)?.parent_group_id ?? null; }
  }
  function foldedOwner(nodeId: string): string | undefined {
    let owner = owners.get(nodeId), result: string | undefined;
    while (owner) { if (folded.has(owner)) result = owner; owner = byId.get(owner)?.parent_group_id ?? undefined; }
    return result;
  }
  const representedBy = new Map(definition.nodes.map(n => [n.id, foldedOwner(n.id) ? groupNodeId(foldedOwner(n.id)!) : n.id]));
  const visibleGroups = groups.filter(g => {
    let parent = g.parent_group_id;
    while (parent) { if (folded.has(parent)) return false; parent = byId.get(parent)?.parent_group_id ?? null; }
    return true;
  });
  const ports = new Map<string, GroupPort[]>();
  function addPort(groupId: string, nodeId: string, side: 'in' | 'out') {
    const list = ports.get(groupId) ?? [], id = groupPortId(side, nodeId);
    if (!list.some(p => p.id === id)) {
      const group = byId.get(groupId)!, fullPrefix = prefix(group);
      list.push({ id, nodeId, side, label: nodeId.startsWith(fullPrefix) ? nodeId.slice(fullPrefix.length) : nodeId, y: 0 });
      ports.set(groupId, list);
    }
    return id;
  }
  const edges: VisibleEdge[] = [];
  for (const edge of definition.edges) {
    const source = representedBy.get(edge.source)!, target = representedBy.get(edge.target)!;
    if (source === target && source !== edge.source) continue;
    const sourceGroup = foldedOwner(edge.source), targetGroup = foldedOwner(edge.target);
    edges.push({ ...edge, source, target,
      sourceHandle: sourceGroup ? addPort(sourceGroup, edge.source, 'out') : undefined,
      targetHandle: targetGroup ? addPort(targetGroup, edge.target, 'in') : undefined });
  }
  for (const [ids, side] of [[definition.entry_node_ids, 'in'], [definition.exit_node_ids, 'out']] as const) for (const id of ids) {
    const owner = foldedOwner(id); if (owner) addPort(owner, id, side);
  }
  const order = new Map(definition.nodes.map((n, i) => [n.id, i]));
  const sizes = new Map<string, { width: number; height: number }>();
  for (const group of visibleGroups) if (folded.has(group.id)) {
    const list = ports.get(group.id) ?? [];
    let rows = 1;
    for (const side of ['in', 'out']) {
      const sidePorts = list.filter(p => p.side === side).sort((a,b) => order.get(a.nodeId)! - order.get(b.nodeId)!);
      rows = Math.max(rows, sidePorts.length);
      sidePorts.forEach((p,i) => p.y = 54 + i * 24);
    }
    sizes.set(groupNodeId(group.id), { width: GRAPH_NODE_WIDTH, height: Math.max(GRAPH_NODE_HEIGHT, 54 + rows * 24 + 16) });
  }
  return { groups: visibleGroups, owners, allMembers, nodes: definition.nodes.filter(n => representedBy.get(n.id) === n.id), edges, ports, sizes, representedBy };
}
export function localNodeLabel(definition: GraphDefinition, nodeId: string): string {
  const groups = definition.groups ?? [], owner = groups.find(g => g.node_ids.includes(nodeId));
  if (!owner) return nodeId;
  let group: WorkflowGroup | undefined = owner; const path: string[] = [];
  while (group) { path.unshift(group.label); group = groups.find(g => g.id === group!.parent_group_id); }
  const prefix = path.join('.') + '.';
  return nodeId.startsWith(prefix) ? nodeId.slice(prefix.length) : nodeId;
}
export interface GroupGeometry { positions: Record<string, Point>; sizes: Record<string, { width: number; height: number }>; parents: Record<string, string> }
export function constrainGroupMove(id: string, requested: Point, geometry: GroupGeometry): Point {
  const parent = geometry.parents[id];
  const size = geometry.sizes[id], parentSize = geometry.sizes[parent];
  if (!size) return requested;
  const origin = geometry.positions[parent];
  const point = parent && parentSize ? { x: Math.max(origin.x + GROUP_PADDING.left, Math.min(requested.x, origin.x + parentSize.width - GROUP_PADDING.right - size.width)),
    y: Math.max(origin.y + GROUP_PADDING.top, Math.min(requested.y, origin.y + parentSize.height - GROUP_PADDING.bottom - size.height)) } : requested;
  // Other containers are structural obstacles; dragging never reparents members.
  for (const sibling of Object.keys(geometry.positions)) {
    if (sibling === id || geometry.parents[sibling] !== parent || !sibling.startsWith('@group:') && !id.startsWith('@group:')) continue;
    const at = geometry.positions[sibling], rect = geometry.sizes[sibling];
    if (point.x < at.x + rect.width + 12 && point.x + size.width > at.x - 12 && point.y < at.y + rect.height + 12 && point.y + size.height > at.y - 12) return geometry.positions[id];
  }
  return point;
}
