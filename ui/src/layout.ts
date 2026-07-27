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

const NODE_WIDTH = 224;
const NODE_HEIGHT = 104;
const ENTRY_ANCHOR_ID = "__autoagent_entry_anchor__";
const EXIT_ANCHOR_ID = "__autoagent_exit_anchor__";
const VIRTUAL_EDGE_PREFIX = "__autoagent_layout_edge__";

export async function layoutWorkflow(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  // ELK is substantially larger than the tracing shell. Load it only when a
  // Workflow version has no saved layout or the user requests auto-layout.
  const { default: ELK } = await import("elkjs/lib/elk.bundled.js");
  const elk = new ELK();
  const visibleChildren = graph.nodes.map((node) => ({
    id: node.id,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
  }));
  const entryAnchor = {
    id: ENTRY_ANCHOR_ID,
    width: 1,
    height: 1,
    layoutOptions: {
      "elk.layered.layering.layerConstraint": "FIRST_SEPARATE",
    },
  };
  const exitAnchor = {
    id: EXIT_ANCHOR_ID,
    width: 1,
    height: 1,
    layoutOptions: {
      "elk.layered.layering.layerConstraint": "LAST_SEPARATE",
    },
  };
  const groupAnchors = graph.groups.flatMap((group, index) => [
    {
      id: groupAnchorId("entry", index),
      width: 1,
      height: 1,
    },
    {
      id: groupAnchorId("exit", index),
      width: 1,
      height: 1,
    },
  ]);
  const entryEdges = graph.entry_node_ids.map((nodeId, index) => ({
    id: `${VIRTUAL_EDGE_PREFIX}entry_${index}`,
    sources: [ENTRY_ANCHOR_ID],
    targets: [nodeId],
  }));
  const exitEdges = graph.exit_node_ids.map((nodeId, index) => ({
    id: `${VIRTUAL_EDGE_PREFIX}exit_${index}`,
    sources: [nodeId],
    targets: [EXIT_ANCHOR_ID],
  }));
  const groupAnchorEdges = graph.groups.flatMap((group, groupIndex) => [
    ...group.entry_node_ids.map((nodeId, nodeIndex) => ({
      id: `${VIRTUAL_EDGE_PREFIX}group_${groupIndex}_entry_${nodeIndex}`,
      sources: [groupAnchorId("entry", groupIndex)],
      targets: [nodeId],
    })),
    ...group.exit_node_ids.map((nodeId, nodeIndex) => ({
      id: `${VIRTUAL_EDGE_PREFIX}group_${groupIndex}_exit_${nodeIndex}`,
      sources: [nodeId],
      targets: [groupAnchorId("exit", groupIndex)],
    })),
  ]);
  const result = await elk.layout({
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "RIGHT",
      "elk.edgeRouting": "ORTHOGONAL",
      "elk.layered.spacing.nodeNodeBetweenLayers": "82",
      "elk.layered.spacing.edgeNodeBetweenLayers": "38",
      "elk.layered.spacing.edgeEdgeBetweenLayers": "22",
      "elk.spacing.nodeNode": "44",
      "elk.spacing.edgeNode": "36",
      "elk.padding": "[top=48,left=48,bottom=48,right=48]",
      "elk.layered.cycleBreaking.strategy": "DEPTH_FIRST",
      "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
      "elk.layered.nodePlacement.favorStraightEdges": "true",
    },
    children: [
      entryAnchor,
      ...visibleChildren,
      ...groupAnchors,
      exitAnchor,
    ],
    edges: [
      ...entryEdges,
      ...graph.edges.map((edge) => ({
        id: edge.id,
        sources: [edge.from_node],
        targets: [edge.to_node],
      })),
      ...groupAnchorEdges,
      ...exitEdges,
    ],
  });
  const positions = Object.fromEntries(
    (result.children ?? [])
      .filter(
        (node) =>
          node.id !== ENTRY_ANCHOR_ID &&
          node.id !== EXIT_ANCHOR_ID &&
          !node.id.startsWith(`${VIRTUAL_EDGE_PREFIX}group_anchor_`),
      )
      .map((node) => [
        node.id,
        { x: node.x ?? 0, y: node.y ?? 0 },
      ]),
  );
  const routedEdges = (result.edges ?? []) as RoutedLayoutEdge[];
  const edgeRoutes: Record<string, EdgeRoute> = Object.fromEntries(
    routedEdges
      .filter((edge) => !edge.id.startsWith(VIRTUAL_EDGE_PREFIX))
      .flatMap((edge) => {
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
  return `autoagent:layout:v6:${definitionHash}`;
}

function groupAnchorId(kind: "entry" | "exit", index: number): string {
  return `${VIRTUAL_EDGE_PREFIX}group_anchor_${kind}_${index}`;
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
