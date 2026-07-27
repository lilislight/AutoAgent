import type { WorkflowGraphView } from "./types";

export interface NodePosition {
  x: number;
  y: number;
}

export interface EdgeRoute {
  points: NodePosition[];
  label: NodePosition;
}

export interface WorkflowLayout {
  positions: Record<string, NodePosition>;
  edgeRoutes: Record<string, EdgeRoute>;
}

type RoutedLayoutEdge = {
  id: string;
  sections?: {
    startPoint: NodePosition;
    bendPoints?: NodePosition[];
    endPoint: NodePosition;
  }[];
};

export async function layoutWorkflow(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  // ELK is substantially larger than the tracing shell. Load it only when a
  // Workflow version has no saved layout or the user requests auto-layout.
  const { default: ELK } = await import("elkjs/lib/elk.bundled.js");
  const elk = new ELK();
  const result = await elk.layout({
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "RIGHT",
      "elk.edgeRouting": "ORTHOGONAL",
      "elk.layered.spacing.nodeNodeBetweenLayers": "120",
      "elk.layered.spacing.edgeNodeBetweenLayers": "38",
      "elk.spacing.nodeNode": "64",
      "elk.spacing.edgeNode": "32",
      "elk.padding": "[top=64,left=64,bottom=64,right=64]",
      "elk.layered.cycleBreaking.strategy": "DEPTH_FIRST",
      "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
      "elk.layered.nodePlacement.favorStraightEdges": "true",
    },
    children: graph.nodes.map((node) => ({
      id: node.id,
      width: 224,
      height: 104,
      layoutOptions: node.entry
        ? {
            "elk.layered.layering.layerConstraint": "FIRST_SEPARATE",
          }
        : node.exit
          ? {
              "elk.layered.layering.layerConstraint": "LAST_SEPARATE",
            }
          : undefined,
    })),
    edges: graph.edges.map((edge) => ({
      id: edge.id,
      sources: [edge.from_node],
      targets: [edge.to_node],
    })),
  });
  const positions = Object.fromEntries(
    (result.children ?? []).map((node) => [
      node.id,
      { x: node.x ?? 0, y: node.y ?? 0 },
    ]),
  );
  const routedEdges = (result.edges ?? []) as RoutedLayoutEdge[];
  const edgeRoutes: Record<string, EdgeRoute> = Object.fromEntries(
    routedEdges.flatMap((edge) => {
      const points = (edge.sections ?? []).flatMap((section, sectionIndex) => {
        const sectionPoints = [
          section.startPoint,
          ...(section.bendPoints ?? []),
          section.endPoint,
        ].map((point) => ({ x: point.x, y: point.y }));
        return sectionIndex === 0 ? sectionPoints : sectionPoints.slice(1);
      });
      if (points.length < 2) return [];
      return [[edge.id, { points, label: routeMidpoint(points) }]];
    }),
  );
  return { positions, edgeRoutes };
}

export function loadSavedLayout(
  definitionHash: string,
): WorkflowLayout | null {
  const raw = localStorage.getItem(layoutKey(definitionHash));
  if (!raw) return null;
  try {
    const value = JSON.parse(raw) as WorkflowLayout;
    if (!value.positions || !value.edgeRoutes) return null;
    return value;
  } catch {
    return null;
  }
}

export function saveLayout(
  definitionHash: string,
  layout: WorkflowLayout,
): void {
  localStorage.setItem(layoutKey(definitionHash), JSON.stringify(layout));
}

function layoutKey(definitionHash: string): string {
  return `autoagent:layout:v3:${definitionHash}`;
}

function routeMidpoint(points: NodePosition[]): NodePosition {
  const segments = points.slice(1).map((point, index) => {
    const previous = points[index];
    return {
      from: previous,
      to: point,
      length: Math.hypot(point.x - previous.x, point.y - previous.y),
    };
  });
  const total = segments.reduce((sum, segment) => sum + segment.length, 0);
  let remaining = total / 2;
  for (const segment of segments) {
    if (remaining <= segment.length) {
      const ratio = segment.length === 0 ? 0 : remaining / segment.length;
      return {
        x: segment.from.x + (segment.to.x - segment.from.x) * ratio,
        y: segment.from.y + (segment.to.y - segment.from.y) * ratio,
      };
    }
    remaining -= segment.length;
  }
  return points.at(-1) ?? { x: 0, y: 0 };
}
