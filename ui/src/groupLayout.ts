import type { GraphDefinition } from './types.js';
import type { Layout, LayoutEngine, Point } from './layout.js';
import { GRAPH_NODE_WIDTH, GRAPH_NODE_HEIGHT } from './graphRouting.js';
import { GROUP_PADDING, groupNodeId, workflowGroups } from './workflowGroups.js';
interface ElkBox {
  id: string; x?: number; y?: number; width?: number; height?: number;
  children?: ElkBox[]; edges?: { id: string; sections?: { startPoint: Point; endPoint: Point; bendPoints?: Point[] }[] }[];
}
const nodePort = (id: string, side: string) => JSON.stringify([id, side]);
export async function computeGroupLayout(definition: GraphDefinition, engine: LayoutEngine, folded: ReadonlySet<string>): Promise<Layout> {
  const visible = workflowGroups(definition, folded);
  const parents: Record<string, string> = {}, sizes: Record<string, { width: number; height: number }> = {};
  const containers = new Map<string | null, Record<string, unknown>>();
  const children = new Map<string | null, Record<string, unknown>[]>(), edges = new Map<string | null, unknown[]>();
  const options = { 'elk.algorithm': 'layered', 'elk.direction': 'RIGHT', 'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
    'elk.edgeRouting': 'ORTHOGONAL', 'elk.spacing.nodeNode': '50', 'elk.spacing.edgeNode': '22',
    'elk.layered.spacing.nodeNodeBetweenLayers': '90', 'elk.layered.mergeEdges': 'false' };
  const padding = GROUP_PADDING;
  containers.set(null, { id: '@root', layoutOptions: options });
  for (const group of visible.groups) {
    const id = groupNodeId(group.id), parent = group.parent_group_id ? groupNodeId(group.parent_group_id) : null;
    if (parent) parents[id] = parent;
    let box: Record<string, unknown>;
    if (folded.has(group.id)) {
      const size = visible.sizes.get(id)!; sizes[id] = size;
      box = { id, ...size, layoutOptions: { 'elk.portConstraints': 'FIXED_POS' }, ports: (visible.ports.get(group.id) ?? []).map(p => ({
        id: nodePort(id, p.id), x: p.side === 'in' ? 0 : size.width, y: p.y, width: 0, height: 0,
        layoutOptions: { 'elk.port.side': p.side === 'in' ? 'WEST' : 'EAST' } })) };
    } else {
      box = { id, layoutOptions: { ...options, 'elk.padding': `[top=${padding.top},left=${padding.left},bottom=${padding.bottom},right=${padding.right}]` } };
      containers.set(id, box);
    }
    const peers = children.get(parent) ?? []; peers.push(box); children.set(parent, peers);
  }
  for (const node of visible.nodes) {
    const parent = visible.owners.get(node.id), owner = parent ? groupNodeId(parent) : null;
    if (owner) parents[node.id] = owner;
    sizes[node.id] = { width: GRAPH_NODE_WIDTH, height: GRAPH_NODE_HEIGHT };
    const peers = children.get(owner) ?? [];
    peers.push({ id: node.id, ...sizes[node.id], layoutOptions: { 'elk.portConstraints': 'FIXED_POS' }, ports: ['in','out'].map(side => ({
      id: nodePort(node.id, side), x: side === 'in' ? 0 : GRAPH_NODE_WIDTH, y: GRAPH_NODE_HEIGHT / 2, width: 0, height: 0,
      layoutOptions: { 'elk.port.side': side === 'in' ? 'WEST' : 'EAST' } })) });
    children.set(owner, peers);
  }
  for (const edge of visible.edges) {
    const ancestors = new Set<string | null>([null]); let current: string | undefined = parents[edge.source];
    while (current) { ancestors.add(current); current = parents[current]; }
    let owner: string | null = parents[edge.target] ?? null;
    while (owner && !ancestors.has(owner)) owner = parents[owner] ?? null;
    const list = edges.get(owner) ?? [];
    list.push({ id: edge.id, sources: [nodePort(edge.source, edge.sourceHandle ?? 'out')], targets: [nodePort(edge.target, edge.targetHandle ?? 'in')] });
    edges.set(owner, list);
  }
  for (const [id, box] of containers) { box.children = children.get(id) ?? []; box.edges = edges.get(id) ?? []; if (!(children.get(id)?.length)) Object.assign(box,{width:GRAPH_NODE_WIDTH,height:90}); }
  const result = await engine.layout(containers.get(null)) as ElkBox;
  const positions: Layout['positions'] = {}, routes: Layout['routes'] = {};
  function flatten(box: ElkBox, offset: Point) {
    const at = { x: offset.x + (box.x ?? 0), y: offset.y + (box.y ?? 0) };
    if (box.id !== '@root') { positions[box.id] = at; sizes[box.id] = { width: box.width ?? GRAPH_NODE_WIDTH, height: box.height ?? GRAPH_NODE_HEIGHT }; }
    for (const edge of box.edges ?? []) routes[edge.id] = (edge.sections ?? []).flatMap((s,i) => {
      const points = [s.startPoint, ...(s.bendPoints ?? []), s.endPoint].map(p => ({ x: at.x + p.x, y: at.y + p.y }));
      return i ? points.slice(1) : points;
    });
    for (const child of box.children ?? []) flatten(child, at);
  }
  flatten(result,{x:0,y:0});
  if ([...visible.nodes.map(n => n.id), ...visible.groups.map(g => groupNodeId(g.id))].some(id => !positions[id])) throw new Error('Incomplete nested Workflow layout');
  return { positions, routes, sizes, parents };
}
