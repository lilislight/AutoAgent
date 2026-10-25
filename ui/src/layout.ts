import type { GraphDefinition } from "./types.js";
import { routeEdgesAroundNodes } from "./graphRouting.js";
import { computeGroupLayout } from './groupLayout.js';
import { fallbackGroups } from './groupGeometry.js';

export const NODE_WIDTH = 232;
export const NODE_HEIGHT = 106;
export interface Point { x: number; y: number }
export interface Layout { positions: Record<string, Point>; routes: Record<string, Point[]>; sizes?: Record<string, { width: number; height: number }>; parents?: Record<string, string> }
interface ElkNode {
  id: string;
  x?: number;
  y?: number;
  children?: ElkNode[];
  edges?: { id: string; sections?: { startPoint: Point; bendPoints?: Point[]; endPoint: Point }[] }[];
}
export interface LayoutEngine { layout: (value: unknown) => Promise<ElkNode> }

export function routeDefinition(definition: GraphDefinition, positions: Layout["positions"], ids = definition.edges.map(edge => edge.id)): Layout["routes"] {
  const routes = routeEdgesAroundNodes(definition.nodes, definition.edges.map((edge, order) => ({ ...edge, order })), positions, ids);
  return Object.fromEntries(Object.entries(routes).map(([id, route]) => [id, route.points]));
}

export function fallbackLayout(definition: GraphDefinition, folded: ReadonlySet<string> = new Set()): Layout {
  if (definition.groups?.length) return fallbackGroups(definition,folded);
  const positions = Object.fromEntries(definition.nodes.map((node, index) => [node.id, {
      x: (index % 3) * (NODE_WIDTH + 80),
      y: Math.floor(index / 3) * (NODE_HEIGHT + 100),
    }]));
  return { positions, routes: routeDefinition(definition, positions) };
}

export async function computeLayout(definition: GraphDefinition, engine: LayoutEngine, folded: ReadonlySet<string> = new Set()): Promise<Layout> {
  if (definition.groups?.length) return computeGroupLayout(definition, engine, folded);
  const port = (id: string, side: string) => JSON.stringify([id, side]);
  const result = await engine.layout({
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered", "elk.direction": "RIGHT",
      "elk.edgeRouting": "ORTHOGONAL", "elk.spacing.nodeNode": "60",
      "elk.layered.spacing.nodeNodeBetweenLayers": "100",
    },
    children: definition.nodes.map(node => ({
      id: node.id, width: NODE_WIDTH, height: NODE_HEIGHT,
      layoutOptions: { "elk.portConstraints": "FIXED_SIDE" },
      ports: [
        { id: port(node.id, "in"), width: 0, height: 0, layoutOptions: { "elk.port.side": "WEST" } },
        { id: port(node.id, "out"), width: 0, height: 0, layoutOptions: { "elk.port.side": "EAST" } },
      ],
    })),
    edges: definition.edges.map(edge => ({ id: edge.id, sources: [port(edge.source, "out")], targets: [port(edge.target, "in")] })),
  });
  const positions: Layout["positions"] = {};
  for (const node of result.children ?? []) {
    if (node.x !== undefined && node.y !== undefined) positions[node.id] = { x: node.x, y: node.y };
  }
  const routes: Layout["routes"] = {};
  for (const edge of result.edges ?? []) {
    const section = edge.sections?.[0];
    if (section) routes[edge.id] = [section.startPoint, ...(section.bendPoints ?? []), section.endPoint];
  }
  if (definition.nodes.some(node => !positions[node.id])) throw new Error("Incomplete graph layout");
  const groups = new Map<string, string[]>();
  for (const edge of definition.edges) {
    const key = JSON.stringify([edge.source, edge.target]);
    groups.set(key, [...(groups.get(key) ?? []), edge.id]);
  }
  const parallel = [...groups.values()].filter(ids => ids.length > 1).flat();
  Object.assign(routes, routeDefinition(definition, positions, parallel));
  return { positions, routes };
}
