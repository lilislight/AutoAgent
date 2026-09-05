import {
  GRAPH_NODE_HEIGHT,
  GRAPH_NODE_WIDTH,
  routeEdgesAroundNodes,
  routeMidpoint,
  type GraphPoint,
} from "./graphRouting.js";
import type { EdgeRoute, WorkflowLayout } from "./layoutTypes.js";
import type {
  WorkflowEdgeView,
  WorkflowGraphView,
  WorkflowGroupView,
} from "./types.js";

type ElkSection = {
  startPoint: GraphPoint;
  bendPoints?: GraphPoint[];
  endPoint: GraphPoint;
};

type ElkEdge = {
  id: string;
  sections?: ElkSection[];
};

type ElkNode = {
  id: string;
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  children?: ElkNode[];
  edges?: ElkEdge[];
};

type LayoutResult = ElkNode;

export type ElkLayoutEngine = {
  layout: (graph: unknown) => Promise<LayoutResult>;
};

const ENTRY_ANCHOR_ID = "__autoagent_entry_anchor__";
const EXIT_ANCHOR_ID = "__autoagent_exit_anchor__";
const VIRTUAL_PREFIX = "__autoagent_layout__";
const INPUT_PORT_SUFFIX = "__autoagent_input_port__";
const OUTPUT_PORT_SUFFIX = "__autoagent_output_port__";
const GROUP_PADDING = {
  top: 54,
  left: 30,
  bottom: 30,
  right: 30,
};

export async function computeWorkflowLayout(
  graph: WorkflowGraphView,
  engine: ElkLayoutEngine,
): Promise<WorkflowLayout> {
  const groupById = new Map(graph.groups.map((group) => [group.id, group]));
  const root = buildContainer(
    null,
    graph,
    groupById,
    graph.entry_node_ids,
    graph.exit_node_ids,
  );
  const result = await engine.layout(root);
  const positions: WorkflowLayout["positions"] = {};
  const edgeRoutes: WorkflowLayout["edgeRoutes"] = {};
  const groupBounds: WorkflowLayout["groupBounds"] = {};
  flattenLayout(result, { x: 0, y: 0 }, positions, edgeRoutes, groupBounds, groupById);

  // The lightweight router deliberately remains a flat-graph fallback. With
  // compound Workflows, ELK owns hierarchy-aware routing so an internal Edge
  // cannot be pulled outside its Workflow container.
  if (graph.groups.length === 0) {
    const separatedParallelRoutes = routeEdgesAroundNodes(
      graph.nodes,
      graph.edges,
      positions,
      parallelEdges(graph),
    );
    Object.assign(edgeRoutes, separatedParallelRoutes);
  }
  return { positions, edgeRoutes, groupBounds };
}

export async function computeFlatWorkflowLayout(
  graph: WorkflowGraphView,
  engine: ElkLayoutEngine,
): Promise<WorkflowLayout> {
  const layout = await computeWorkflowLayout(
    { ...graph, groups: [] },
    engine,
  );
  return {
    ...layout,
    groupBounds: calculateGroupBounds(
      graph.groups,
      graph.nodes,
      layout.positions,
      layout.edgeRoutes,
    ),
  };
}

export function computeEmergencyWorkflowLayout(
  graph: WorkflowGraphView,
): WorkflowLayout {
  const positions = Object.fromEntries(
    graph.nodes.map((node, index) => [
      node.id,
      {
        x: (index % 5) * (GRAPH_NODE_WIDTH + 96),
        y: Math.floor(index / 5) * (GRAPH_NODE_HEIGHT + 72),
      },
    ]),
  );
  const edgeRoutes = routeEdgesAroundNodes(
    graph.nodes,
    graph.edges,
    positions,
    graph.edges.map((edge) => edge.id),
  );
  return {
    positions,
    edgeRoutes,
    groupBounds: calculateGroupBounds(
      graph.groups,
      graph.nodes,
      positions,
      edgeRoutes,
    ),
  };
}

export function calculateGroupBounds(
  groups: WorkflowGroupView[],
  nodes: WorkflowGraphView["nodes"],
  positions: WorkflowLayout["positions"],
  edgeRoutes: WorkflowLayout["edgeRoutes"],
): WorkflowLayout["groupBounds"] {
  const result: WorkflowLayout["groupBounds"] = {};
  const nodeIds = new Set(nodes.map((node) => node.id));
  const ordered = [...groups].sort(
    (left, right) => right.workflow_path.length - left.workflow_path.length,
  );
  for (const group of ordered) {
    const points: GraphPoint[] = [];
    for (const nodeId of group.direct_node_ids) {
      if (!nodeIds.has(nodeId)) continue;
      const position = positions[nodeId];
      if (!position) continue;
      points.push(
        position,
        {
          x: position.x + GRAPH_NODE_WIDTH,
          y: position.y + GRAPH_NODE_HEIGHT,
        },
      );
    }
    for (const edgeId of group.direct_edge_ids ?? []) {
      points.push(...(edgeRoutes[edgeId]?.points ?? []));
    }
    for (const child of groups) {
      if (child.parent_group_id !== group.id) continue;
      const bounds = result[child.id];
      if (!bounds) continue;
      points.push(
        { x: bounds.left, y: bounds.top },
        { x: bounds.right, y: bounds.bottom },
      );
    }
    if (points.length === 0) continue;
    result[group.id] = {
      left: Math.min(...points.map((point) => point.x)) - GROUP_PADDING.left,
      top: Math.min(...points.map((point) => point.y)) - GROUP_PADDING.top,
      right: Math.max(...points.map((point) => point.x)) + GROUP_PADDING.right,
      bottom:
        Math.max(...points.map((point) => point.y)) + GROUP_PADDING.bottom,
    };
  }
  return result;
}

function buildContainer(
  groupId: string | null,
  graph: WorkflowGraphView,
  groupById: Map<string, WorkflowGroupView>,
  entryNodeIds: string[],
  exitNodeIds: string[],
): Record<string, unknown> {
  const group = groupId === null ? null : groupById.get(groupId);
  if (groupId !== null && group === undefined) {
    throw new Error(`Unknown Workflow group: ${groupId}`);
  }
  const directNodeIds = new Set(
    group === null
      ? (graph.groups.length === 0
          ? graph.nodes
          : graph.nodes.filter(
              (node) => (node.workflow_path?.length ?? 0) === 0,
            ))
          .map((node) => node.id)
      : group!.direct_node_ids,
  );
  const childGroups = graph.groups.filter(
    (candidate) => candidate.parent_group_id === groupId,
  );
  const ownedEdges = graph.edges.filter((edge) =>
    group === null
      ? graph.groups.length === 0 ||
        (edge.workflow_path?.length ?? 0) === 0
      : directEdgeIds(group!, graph.edges).has(edge.id),
  );
  const children = [
    ...(groupId === null
      ? [anchorNode(ENTRY_ANCHOR_ID, "FIRST_SEPARATE")]
      : []),
    ...graph.nodes
      .filter((node) => directNodeIds.has(node.id))
      .map(layoutNode),
    ...childGroups.map((child) =>
      buildContainer(
        child.id,
        graph,
        groupById,
        child.entry_node_ids,
        child.exit_node_ids,
      ),
    ),
    ...(groupId === null
      ? [anchorNode(EXIT_ANCHOR_ID, "LAST_SEPARATE")]
      : []),
  ];
  const edges = [
    ...(groupId === null
      ? entryNodeIds.map((nodeId, index) => ({
          id: `${VIRTUAL_PREFIX}entry_${index}`,
          sources: [ENTRY_ANCHOR_ID],
          targets: [inputPortId(nodeId)],
        }))
      : []),
    ...ownedEdges.map(layoutEdge),
    ...(groupId === null
      ? exitNodeIds.map((nodeId, index) => ({
          id: `${VIRTUAL_PREFIX}exit_${index}`,
          sources: [outputPortId(nodeId)],
          targets: [EXIT_ANCHOR_ID],
        }))
      : []),
  ];
  return {
    id: groupId ?? "root",
    layoutOptions: containerLayoutOptions(group !== null),
    children,
    edges,
  };
}

function containerLayoutOptions(group: boolean): Record<string, string> {
  return {
    "elk.algorithm": "layered",
    "elk.direction": "RIGHT",
    "elk.edgeRouting": "ORTHOGONAL",
    "elk.hierarchyHandling": "INCLUDE_CHILDREN",
    "elk.layered.spacing.nodeNodeBetweenLayers": "82",
    "elk.layered.spacing.edgeNodeBetweenLayers": "38",
    "elk.layered.spacing.edgeEdgeBetweenLayers": "22",
    "elk.spacing.nodeNode": "44",
    "elk.spacing.edgeNode": "36",
    "elk.padding": group
      ? `[top=${GROUP_PADDING.top},left=${GROUP_PADDING.left},bottom=${GROUP_PADDING.bottom},right=${GROUP_PADDING.right}]`
      : "[top=48,left=48,bottom=48,right=48]",
    "elk.layered.cycleBreaking.strategy": "DEPTH_FIRST",
    "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
    "elk.layered.nodePlacement.favorStraightEdges": "true",
    "elk.layered.mergeEdges": "false",
  };
}

function layoutNode(node: WorkflowGraphView["nodes"][number]): Record<string, unknown> {
  return {
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
        layoutOptions: { "elk.port.side": "WEST" },
      },
      {
        id: outputPortId(node.id),
        width: 1,
        height: 1,
        layoutOptions: { "elk.port.side": "EAST" },
      },
    ],
  };
}

function anchorNode(id: string, constraint: string): Record<string, unknown> {
  return {
    id,
    width: 1,
    height: 1,
    layoutOptions: {
      "elk.layered.layering.layerConstraint": constraint,
    },
  };
}

function layoutEdge(edge: WorkflowEdgeView): Record<string, unknown> {
  return {
    id: edge.id,
    sources: [outputPortId(edge.from_node)],
    targets: [inputPortId(edge.to_node)],
  };
}

function directEdgeIds(
  group: WorkflowGroupView,
  edges: WorkflowEdgeView[],
): Set<string> {
  if (group.direct_edge_ids !== undefined) {
    return new Set(group.direct_edge_ids);
  }
  const pathKey = group.workflow_path.join("\u0000");
  return new Set(
    edges
      .filter((edge) => (edge.workflow_path ?? []).join("\u0000") === pathKey)
      .map((edge) => edge.id),
  );
}

function flattenLayout(
  container: ElkNode,
  parentOffset: GraphPoint,
  positions: WorkflowLayout["positions"],
  edgeRoutes: WorkflowLayout["edgeRoutes"],
  groupBounds: WorkflowLayout["groupBounds"],
  groupById: Map<string, WorkflowGroupView>,
): void {
  const offset = {
    x: parentOffset.x + (container.x ?? 0),
    y: parentOffset.y + (container.y ?? 0),
  };
  if (groupById.has(container.id)) {
    groupBounds[container.id] = {
      left: offset.x,
      top: offset.y,
      right: offset.x + (container.width ?? 0),
      bottom: offset.y + (container.height ?? 0),
    };
  }
  for (const edge of container.edges ?? []) {
    if (edge.id.startsWith(VIRTUAL_PREFIX)) continue;
    const route = flattenEdge(edge, offset);
    if (route !== null) edgeRoutes[edge.id] = route;
  }
  for (const child of container.children ?? []) {
    if (
      child.id === ENTRY_ANCHOR_ID ||
      child.id === EXIT_ANCHOR_ID
    ) {
      continue;
    }
    if (groupById.has(child.id)) {
      flattenLayout(
        child,
        offset,
        positions,
        edgeRoutes,
        groupBounds,
        groupById,
      );
      continue;
    }
    positions[child.id] = {
      x: offset.x + (child.x ?? 0),
      y: offset.y + (child.y ?? 0),
    };
  }
}

function flattenEdge(edge: ElkEdge, offset: GraphPoint): EdgeRoute | null {
  const points = (edge.sections ?? []).flatMap((section, sectionIndex) => {
    const values = [
      section.startPoint,
      ...(section.bendPoints ?? []),
      section.endPoint,
    ].map((point) => ({
      x: offset.x + point.x,
      y: offset.y + point.y,
    }));
    return sectionIndex === 0 ? values : values.slice(1);
  });
  if (points.length < 2) return null;
  return { points, label: routeMidpoint(points) };
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
