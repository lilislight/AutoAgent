import ELKModule from "elkjs/lib/elk.bundled.js";

import {
  GRAPH_NODE_HEIGHT,
  GRAPH_NODE_WIDTH,
  routeEdgesAroundNodes,
  routeMidpoint,
  type GraphPoint,
} from "./graphRouting.js";
import type { WorkflowLayout } from "./layoutTypes.js";
import type { WorkflowGraphView } from "./types.js";

type RoutedLayoutEdge = {
  id: string;
  sections?: {
    startPoint: GraphPoint;
    bendPoints?: GraphPoint[];
    endPoint: GraphPoint;
  }[];
};

type LayoutResult = {
  children?: {
    id: string;
    x?: number;
    y?: number;
  }[];
  edges?: RoutedLayoutEdge[];
};

type ElkConstructor = new () => {
  layout: (graph: unknown) => Promise<LayoutResult>;
};

const ELK = ELKModule as unknown as ElkConstructor;

const ENTRY_ANCHOR_ID = "__autoagent_entry_anchor__";
const EXIT_ANCHOR_ID = "__autoagent_exit_anchor__";
const VIRTUAL_EDGE_PREFIX = "__autoagent_layout_edge__";
const INPUT_PORT_SUFFIX = "__autoagent_input_port__";
const OUTPUT_PORT_SUFFIX = "__autoagent_output_port__";

export async function computeWorkflowLayout(
  graph: WorkflowGraphView,
): Promise<WorkflowLayout> {
  const elk = new ELK();
  const visibleChildren = graph.nodes.map((node) => ({
    id: node.id,
    width: GRAPH_NODE_WIDTH,
    height: GRAPH_NODE_HEIGHT,
    layoutOptions: {
      "elk.portConstraints": "FIXED_SIDE",
    },
    ports: [
      {
        id: inputPortId(node.id),
        width: 1,
        height: 1,
        layoutOptions: {
          "elk.port.side": "WEST",
        },
      },
      {
        id: outputPortId(node.id),
        width: 1,
        height: 1,
        layoutOptions: {
          "elk.port.side": "EAST",
        },
      },
    ],
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
    targets: [inputPortId(nodeId)],
  }));
  const exitEdges = graph.exit_node_ids.map((nodeId, index) => ({
    id: `${VIRTUAL_EDGE_PREFIX}exit_${index}`,
    sources: [outputPortId(nodeId)],
    targets: [EXIT_ANCHOR_ID],
  }));
  const groupAnchorEdges = graph.groups.flatMap((group, groupIndex) => [
    ...group.entry_node_ids.map((nodeId, nodeIndex) => ({
      id: `${VIRTUAL_EDGE_PREFIX}group_${groupIndex}_entry_${nodeIndex}`,
      sources: [groupAnchorId("entry", groupIndex)],
      targets: [inputPortId(nodeId)],
    })),
    ...group.exit_node_ids.map((nodeId, nodeIndex) => ({
      id: `${VIRTUAL_EDGE_PREFIX}group_${groupIndex}_exit_${nodeIndex}`,
      sources: [outputPortId(nodeId)],
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
      "elk.layered.mergeEdges": "false",
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
        sources: [outputPortId(edge.from_node)],
        targets: [inputPortId(edge.to_node)],
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
  const edgeRoutes = Object.fromEntries(
    routedEdges
      .filter((edge) => !edge.id.startsWith(VIRTUAL_EDGE_PREFIX))
      .flatMap((edge) => {
        const points = (edge.sections ?? []).flatMap(
          (section, sectionIndex) => {
            const sectionPoints = [
              section.startPoint,
              ...(section.bendPoints ?? []),
              section.endPoint,
            ].map((point) => ({ x: point.x, y: point.y }));
            return sectionIndex === 0
              ? sectionPoints
              : sectionPoints.slice(1);
          },
        );
        if (points.length < 2) return [];
        return [[edge.id, { points, label: routeMidpoint(points) }]];
      }),
  );
  const parallelEdgeIds = parallelEdges(graph);
  const separatedParallelRoutes = routeEdgesAroundNodes(
    graph.nodes,
    graph.edges,
    positions,
    parallelEdgeIds,
  );
  return {
    positions,
    edgeRoutes: {
      ...edgeRoutes,
      ...separatedParallelRoutes,
    },
  };
}

function groupAnchorId(kind: "entry" | "exit", index: number): string {
  return `${VIRTUAL_EDGE_PREFIX}group_anchor_${kind}_${index}`;
}

function inputPortId(nodeId: string): string {
  return `${nodeId}${INPUT_PORT_SUFFIX}`;
}

function outputPortId(nodeId: string): string {
  return `${nodeId}${OUTPUT_PORT_SUFFIX}`;
}

function parallelEdges(graph: WorkflowGraphView): Set<string> {
  const groups = new Map<string, string[]>();
  for (const edge of graph.edges) {
    const key = `${edge.from_node}\u0000${edge.to_node}`;
    groups.set(key, [...(groups.get(key) ?? []), edge.id]);
  }
  return new Set(
    [...groups.values()]
      .filter((edgeIds) => edgeIds.length > 1)
      .flat(),
  );
}
