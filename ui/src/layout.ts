import type { GraphDefinition } from "./types.js";
import { routeEdgesAroundNodes } from "./graphRouting.js";
import { computeGroupLayout } from './groupLayout.js';
import { fallbackGroups } from './groupGeometry.js';
import { definitionPorts, edgePortId, type GraphPorts } from './graphPorts.js';

export const NODE_WIDTH = 232;
export const NODE_HEIGHT = 106;
export interface Point { x: number; y: number }
export interface Layout { ports?:GraphPorts; positions: Record<string, Point>; routes: Record<string, Point[]>; sizes?: Record<string, { width: number; height: number }>; parents?: Record<string, string> }
interface ElkNode {
  id: string;
  x?: number;
  y?: number;
  children?: ElkNode[];
  ports?: {id:string;y?:number}[];
  edges?: { id: string; sections?: { startPoint: Point; bendPoints?: Point[]; endPoint: Point }[] }[];
}
export interface LayoutEngine { layout: (value: unknown) => Promise<ElkNode> }

export function routeDefinition(definition: GraphDefinition, positions: Layout["positions"], ids = definition.edges.map(edge => edge.id), ports:GraphPorts=definitionPorts(definition)): Layout["routes"] {
  const routes = routeEdgesAroundNodes(definition.nodes, definition.edges.map(edge => ({ ...edge, sourceOffset:ports[edge.source]?.find(p=>p.id===edgePortId(edge.id,'out'))?.y,targetOffset:ports[edge.target]?.find(p=>p.id===edgePortId(edge.id,'in'))?.y })), positions, ids);
  return Object.fromEntries(Object.entries(routes).map(([id, route]) => [id, route.points]));
}

export function fallbackLayout(definition: GraphDefinition, folded: ReadonlySet<string> = new Set()): Layout {
  if (definition.groups?.length) return fallbackGroups(definition,folded);
  const positions = Object.fromEntries(definition.nodes.map((node, index) => [node.id, {
      x: (index % 3) * (NODE_WIDTH + 80),
      y: Math.floor(index / 3) * (NODE_HEIGHT + 100),
    }]));
  const ports=definitionPorts(definition);
  return { positions, ports, routes: routeDefinition(definition, positions,undefined,ports) };
}

export async function computeLayout(definition: GraphDefinition, engine: LayoutEngine, folded: ReadonlySet<string> = new Set()): Promise<Layout> {
  if (definition.groups?.length) return computeGroupLayout(definition, engine, folded);
  const ports=definitionPorts(definition);
  const result = await engine.layout({
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered", "elk.direction": "RIGHT",
      "elk.edgeRouting": "ORTHOGONAL", "elk.spacing.nodeNode": "60",
      "elk.layered.spacing.nodeNodeBetweenLayers": "100",
      "elk.layered.mergeEdges":"false", "elk.spacing.edgeEdge":"16",
      "elk.layered.spacing.edgeEdgeBetweenLayers":"18", "elk.spacing.portPort":"12",
    },
    children: definition.nodes.map(node => ({
      id: node.id, width: NODE_WIDTH, height: NODE_HEIGHT,
      layoutOptions: { "elk.portConstraints": "FIXED_SIDE" },
      ports: ports[node.id].map(p=>({id:p.id,width:0,height:0,
        layoutOptions:{'elk.port.side':p.side==='in'?'WEST':'EAST'}})),
    })),
    edges: definition.edges.map(edge => ({ id: edge.id, sources: [edgePortId(edge.id, 'out')], targets: [edgePortId(edge.id, 'in')] })),
  });
  const positions: Layout["positions"] = {};
  for (const node of result.children ?? []) {
    if (node.x !== undefined && node.y !== undefined) positions[node.id] = { x: node.x, y: node.y };
    const actual=new Map(node.ports?.map(p=>[p.id,p.y]));
    for(const p of ports[node.id]??[])p.y=actual.get(p.id)??p.y;
  }
  const routes: Layout["routes"] = {};
  for (const edge of result.edges ?? []) {
    const section = edge.sections?.[0];
    if (section) routes[edge.id] = [section.startPoint, ...(section.bendPoints ?? []), section.endPoint];
  }
  if (definition.nodes.some(node => !positions[node.id])) throw new Error("Incomplete graph layout");
  return { positions, routes, ports };
}
