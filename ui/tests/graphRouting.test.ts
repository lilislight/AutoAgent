import {
  affectedEdgeIdsAfterNodeMove,
  GRAPH_NODE_HEIGHT,
  GRAPH_NODE_WIDTH,
  routeEdgesAroundNodes,
  segmentIntersectsRect,
  sourcePort,
  targetPort,
  type GraphEdgeRoute,
  type GraphPoint,
  type GraphRect,
  type RoutableEdge,
  type RoutableNode,
} from "../src/graphRouting.js";
import { computeWorkflowLayoutOnMainThread as computeWorkflowLayout } from "../src/layoutFallback.js";
import {
  calculateGroupBounds,
  loadAutomaticLayout,
  loadSavedLayout,
  saveAutomaticLayout,
  saveLayout,
} from "../src/layout.js";
import { projectEvents } from "../src/projection.js";
import type { RuntimeEvent, WorkflowGraphView } from "../src/types.js";

type TestCase = {
  name: string;
  run: () => void | Promise<void>;
};

const tests: TestCase[] = [
  {
    name: "projection marks a fail-fast sibling Node cancelled",
    run: () => {
      const invocationId = "invocation";
      const events = [
        runtimeEvent(1, "node.running", "running"),
        runtimeEvent(2, "node.cancelled", "cancelled", {
          code: "INVOCATION_FAILED_FAST",
          message: "Sibling branch failed.",
        }),
        {
          ...runtimeEvent(3, "invocation.failed", "failed"),
          subject_type: "invocation",
          subject_id: invocationId,
          payload: { state: "failed" },
        },
      ];

      const projection = projectEvents(invocationId, events);

      equal(projection.nodes.worker.state, "cancelled");
      equal(
        (
          projection.nodes.worker.latest_error as
            | Record<string, unknown>
            | null
        )?.code,
        "INVOCATION_FAILED_FAST",
      );
      equal(projection.invocation_state, "failed");
    },
  },
  {
    name: "ports are constrained to right output and left input",
    run: () => {
      const position = { x: 40, y: 70 };
      equal(sourcePort(position), {
        x: 40 + GRAPH_NODE_WIDTH,
        y: 70 + GRAPH_NODE_HEIGHT / 2,
      });
      equal(targetPort(position), {
        x: 40,
        y: 70 + GRAPH_NODE_HEIGHT / 2,
      });
    },
  },
  {
    name: "a forward edge avoids a node between its endpoints",
    run: () => {
      const nodes = nodeList("source", "obstacle", "target");
      const edges = [edge("edge", "source", "target")];
      const positions = {
        source: { x: 0, y: 100 },
        obstacle: { x: 300, y: 80 },
        target: { x: 600, y: 100 },
      };
      const route = routeEdgesAroundNodes(
        nodes,
        edges,
        positions,
        ["edge"],
      ).edge;
      assert(route, "route should be created");
      equal(route.points[0], sourcePort(positions.source));
      equal(route.points.at(-1), targetPort(positions.target));
      const obstacle = expandedRect(positions.obstacle);
      assert(
        !routeIntersects(route, obstacle),
        "route must avoid the middle Node",
      );
    },
  },
  {
    name: "a self loop exits right and re-enters left without crossing its Node",
    run: () => {
      const nodes = nodeList("loop");
      const edges = [edge("loop-edge", "loop", "loop")];
      const positions = { loop: { x: 100, y: 100 } };
      const route = routeEdgesAroundNodes(
        nodes,
        edges,
        positions,
        ["loop-edge"],
      )["loop-edge"];
      assert(route.points.length >= 5, "self loop should have outer bends");
      equal(route.points[0], sourcePort(positions.loop));
      equal(route.points.at(-1), targetPort(positions.loop));
      const nodeRect: GraphRect = {
        left: positions.loop.x,
        top: positions.loop.y,
        right: positions.loop.x + GRAPH_NODE_WIDTH,
        bottom: positions.loop.y + GRAPH_NODE_HEIGHT,
      };
      assert(
        !route.points.slice(2, -2).some((point, index) =>
          segmentIntersectsRect(
            route.points[index + 1],
            point,
            nodeRect,
          ),
        ),
        "the outer part of a self loop must not cross the Node",
      );
    },
  },
  {
    name: "parallel edges receive distinct lanes",
    run: () => {
      const nodes = nodeList("source", "target");
      const edges = [
        edge("first", "source", "target", 0),
        edge("second", "source", "target", 1),
      ];
      const positions = {
        source: { x: 0, y: 0 },
        target: { x: 500, y: 0 },
      };
      const routes = routeEdgesAroundNodes(
        nodes,
        edges,
        positions,
        edges.map((value) => value.id),
      );
      assert(
        JSON.stringify(routes.first.points) !==
          JSON.stringify(routes.second.points),
        "parallel routes should not overlap exactly",
      );
    },
  },
  {
    name: "ELK routes every edge from the right side into the left side",
    run: async () => {
      const graph = workflowGraph();
      const layout = await computeWorkflowLayout(graph);
      const sourcePosition = layout.positions.source;
      const targetPosition = layout.positions.target;
      const points = layout.edgeRoutes.edge.points;
      assert(points.length >= 2, "ELK route should contain endpoints");
      assert(
        nearlyEqual(points[0].x, sourcePosition.x + GRAPH_NODE_WIDTH),
        "ELK output must leave the source on its right side",
      );
      assert(
        nearlyEqual(points.at(-1)!.x, targetPosition.x),
        "ELK input must enter the target on its left side",
      );
    },
  },
  {
    name: "ELK layout separates parallel edges into distinct lanes",
    run: async () => {
      const graph = workflowGraph();
      graph.edges.push({
        ...graph.edges[0],
        id: "second-edge",
        order: 1,
      });
      const layout = await computeWorkflowLayout(graph);
      assert(
        JSON.stringify(layout.edgeRoutes.edge.points) !==
          JSON.stringify(layout.edgeRoutes["second-edge"].points),
        "initial ELK layout must not overlap parallel edges",
      );
    },
  },
  {
    name: "compound layout contains sibling and nested Workflow members",
    run: async () => {
      const graph = groupedWorkflowGraph();
      const layout = await computeWorkflowLayout(graph);
      const first = layout.groupBounds.first;
      const inner = layout.groupBounds["first/inner"];
      const second = layout.groupBounds.second;
      assert(first && inner && second, "every Workflow group needs bounds");
      assert(rectContainsRect(first, inner), "parent must contain nested group");
      assert(
        first.right < second.left || second.right < first.left,
        "sibling Workflow groups must not overlap in automatic layout",
      );
      for (const group of graph.groups) {
        const bounds = layout.groupBounds[group.id];
        for (const nodeId of group.node_ids) {
          assert(
            rectContainsNode(bounds, layout.positions[nodeId]),
            `${group.id} must contain ${nodeId}`,
          );
        }
        for (const edgeId of group.edge_ids ?? []) {
          assert(
            layout.edgeRoutes[edgeId].points.every((point) =>
              rectContainsPoint(bounds, point)
            ),
            `${group.id} must contain internal Edge ${edgeId}`,
          );
        }
      }
      for (const externalNodeId of ["start", "end"]) {
        for (const [groupId, bounds] of Object.entries(layout.groupBounds)) {
          assert(
            !rectContainsNode(bounds, layout.positions[externalNodeId]),
            `automatic layout must keep ${externalNodeId} outside ${groupId}`,
          );
        }
      }
    },
  },
  {
    name: "drag bounds follow internal members and ignore external Nodes",
    run: async () => {
      const graph = groupedWorkflowGraph();
      const layout = await computeWorkflowLayout(graph);
      const moved = {
        ...layout.positions,
        "first/inner/a": { x: 900, y: 420 },
        start: { x: 900, y: 420 },
      };
      const bounds = calculateGroupBounds(
        graph.groups,
        graph.nodes,
        moved,
        layout.edgeRoutes,
      );
      assert(
        rectContainsNode(bounds["first/inner"], moved["first/inner/a"]),
        "dragged internal Node must remain inside its direct group",
      );
      assert(
        rectContainsRect(bounds.first, bounds["first/inner"]),
        "dragged internal Node must expand every ancestor group",
      );

      const externalMovedAgain = {
        ...moved,
        start: { x: -10_000, y: -10_000 },
      };
      equal(
        calculateGroupBounds(
          graph.groups,
          graph.nodes,
          externalMovedAgain,
          layout.edgeRoutes,
        ),
        bounds,
      );
    },
  },
  {
    name: "compound layout handles a ReAct-style internal cycle",
    run: async () => {
      const graph = reactLikeWorkflowGraph();
      const layout = await computeWorkflowLayout(graph);
      assert(
        Object.keys(layout.positions).length === graph.nodes.length,
        "cyclic child Workflow layout must retain every Node",
      );
      assert(
        Object.keys(layout.edgeRoutes).length === graph.edges.length,
        "cyclic child Workflow layout must retain every Edge",
      );
      const bounds = layout.groupBounds.agent;
      for (const nodeId of graph.groups[0].node_ids) {
        assert(
          rectContainsNode(bounds, layout.positions[nodeId]),
          `ReAct group must contain ${nodeId}`,
        );
      }
      for (const edgeId of graph.groups[0].edge_ids ?? []) {
        assert(
          layout.edgeRoutes[edgeId].points.every((point) =>
            rectContainsPoint(bounds, point)
          ),
          `ReAct group must contain ${edgeId}`,
        );
      }
    },
  },
  {
    name: "compound layout derives Edge ownership from older Group payloads",
    run: async () => {
      const graph = reactLikeWorkflowGraph();
      graph.groups = graph.groups.map((group) => ({
        ...group,
        edge_ids: undefined,
        direct_edge_ids: undefined,
      }));
      const layout = await computeWorkflowLayout(graph);
      assert(
        Object.keys(layout.edgeRoutes).length === graph.edges.length,
        "missing derived Group Edge fields must not blank the graph",
      );
    },
  },
  {
    name: "automatic layout is stored independently from dragged layout",
    run: () => {
      const values = new Map<string, string>();
      Object.defineProperty(globalThis, "localStorage", {
        configurable: true,
        value: {
          getItem: (key: string) => values.get(key) ?? null,
          setItem: (key: string, value: string) => {
            values.set(key, value);
          },
        },
      });
      const automatic = {
        positions: { node: { x: 10, y: 20 } },
        edgeRoutes: {},
        groupBounds: {},
      };
      const dragged = {
        positions: { node: { x: 200, y: 300 } },
        edgeRoutes: {},
        groupBounds: {},
      };
      assert(saveAutomaticLayout("revision", automatic), "automatic layout should save");
      assert(saveLayout("revision", dragged), "dragged layout should save");
      equal(loadAutomaticLayout("revision"), automatic);
      equal(loadSavedLayout("revision"), dragged);
      Reflect.deleteProperty(globalThis, "localStorage");
    },
  },
  {
    name: "layout storage failures never block graph rendering",
    run: () => {
      Object.defineProperty(globalThis, "localStorage", {
        configurable: true,
        value: {
          getItem: () => {
            throw new Error("blocked");
          },
          setItem: () => {
            throw new Error("quota");
          },
        },
      });
      assert(
        loadSavedLayout("blocked") === null,
        "blocked storage should behave as a cache miss",
      );
      assert(
        saveLayout("blocked", {
          positions: {},
          edgeRoutes: {},
          groupBounds: {},
        }) === false,
        "failed storage should be reported without throwing",
      );
      Reflect.deleteProperty(globalThis, "localStorage");
    },
  },
  {
    name: "moving a Node reroutes incident and newly obstructed edges",
    run: () => {
      const edges = [
        edge("incident", "moving", "target"),
        edge("obstructed", "other-source", "other-target"),
        edge("safe", "safe-source", "safe-target"),
      ];
      const routes: Record<string, GraphEdgeRoute> = {
        obstructed: route([
          { x: 0, y: 150 },
          { x: 800, y: 150 },
        ]),
        safe: route([
          { x: 0, y: 500 },
          { x: 800, y: 500 },
        ]),
      };
      const affected = affectedEdgeIdsAfterNodeMove(
        routes,
        edges,
        "moving",
        { moving: { x: 300, y: 100 } },
      );
      assert(affected.has("incident"), "incident edge should be rerouted");
      assert(affected.has("obstructed"), "crossing edge should be rerouted");
      assert(!affected.has("safe"), "unrelated safe edge should remain");
    },
  },
];

function runtimeEvent(
  sequence: number,
  eventName: string,
  state: string,
  error: Record<string, unknown> | null = null,
): RuntimeEvent {
  return {
    id: `event-${sequence}`,
    invocation_id: "invocation",
    sequence,
    schema_version: 1,
    event_type: "state_change",
    event_name: eventName,
    subject_type: "node",
    subject_id: "execution",
    occurred_at_ms: sequence,
    elapsed_ns: null,
    status: state,
    timing: {},
    has_input: false,
    has_output: false,
    has_operations: false,
    payload: {
      node_id: "worker",
      node_execution_id: "execution",
      state,
      error,
    },
    input: null,
    output: null,
    operations: null,
    type: eventName,
    entity_type: "node",
    entity_id: "execution",
    node_id: "worker",
    edge_id: null,
    channel: "runtime",
    visibility: "internal",
  };
}

for (const test of tests) {
  await test.run();
  console.log(`ok - ${test.name}`);
}

function nodeList(...ids: string[]): RoutableNode[] {
  return ids.map((id) => ({ id }));
}

function edge(
  id: string,
  from_node: string,
  to_node: string,
  order = 0,
): RoutableEdge {
  return { id, from_node, to_node, order };
}

function expandedRect(position: GraphPoint): GraphRect {
  return {
    left: position.x - 18,
    top: position.y - 18,
    right: position.x + GRAPH_NODE_WIDTH + 18,
    bottom: position.y + GRAPH_NODE_HEIGHT + 18,
  };
}

function route(points: GraphPoint[]): GraphEdgeRoute {
  return { points, label: points[0] };
}

function routeIntersects(
  routeValue: GraphEdgeRoute,
  rect: GraphRect,
): boolean {
  return routeValue.points.slice(1).some((point, index) =>
    segmentIntersectsRect(routeValue.points[index], point, rect),
  );
}

function assert(
  condition: unknown,
  message: string,
): asserts condition {
  if (!condition) throw new Error(message);
}

function equal(actual: unknown, expected: unknown): void {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    throw new Error(`Expected ${expectedJson}, received ${actualJson}`);
  }
}

function nearlyEqual(left: number, right: number): boolean {
  return Math.abs(left - right) < 1.1;
}

function workflowGraph(): WorkflowGraphView {
  return {
    workflow_id: "workflow",
    workflow_version: "1",
    revision_id: "revision",
    definition_hash: "hash",
    name: "test",
    description: null,
    nodes: [
      workflowNode("source", true, false),
      workflowNode("target", false, true),
    ],
    edges: [
      {
        id: "edge",
        from_node: "source",
        to_node: "target",
        order: 0,
        condition: null,
        policy: null,
      },
    ],
    groups: [],
    entry_node_ids: ["source"],
    exit_node_ids: ["target"],
    loop_regions: [],
  };
}

function groupedWorkflowGraph(): WorkflowGraphView {
  const graph = workflowGraph();
  const nodes = [
    workflowNode("start", true, false),
    {
      ...workflowNode("first/inner/a", false, false),
      workflow_path: ["first", "inner"],
    },
    {
      ...workflowNode("first/inner/b", false, false),
      workflow_path: ["first", "inner"],
    },
    {
      ...workflowNode("second/c", false, false),
      workflow_path: ["second"],
    },
    {
      ...workflowNode("second/d", false, false),
      workflow_path: ["second"],
    },
    workflowNode("end", false, true),
  ];
  return {
    ...graph,
    nodes,
    edges: [
      {
        ...workflowEdge("enter-first", "start", "first/inner/a", 0),
        workflow_path: [],
      },
      {
        ...workflowEdge("first-inside", "first/inner/a", "first/inner/b", 1),
        workflow_path: ["first", "inner"],
      },
      {
        ...workflowEdge("between", "first/inner/b", "second/c", 2),
        workflow_path: [],
      },
      {
        ...workflowEdge("second-inside", "second/c", "second/d", 3),
        workflow_path: ["second"],
      },
      {
        ...workflowEdge("leave-second", "second/d", "end", 4),
        workflow_path: [],
      },
    ],
    groups: [
      {
        id: "first",
        parent_group_id: null,
        label: "first",
        workflow_path: ["first"],
        node_ids: ["first/inner/a", "first/inner/b"],
        direct_node_ids: [],
        edge_ids: ["first-inside"],
        direct_edge_ids: [],
        entry_node_ids: ["first/inner/a"],
        exit_node_ids: ["first/inner/b"],
      },
      {
        id: "first/inner",
        parent_group_id: "first",
        label: "inner",
        workflow_path: ["first", "inner"],
        node_ids: ["first/inner/a", "first/inner/b"],
        direct_node_ids: ["first/inner/a", "first/inner/b"],
        edge_ids: ["first-inside"],
        direct_edge_ids: ["first-inside"],
        entry_node_ids: ["first/inner/a"],
        exit_node_ids: ["first/inner/b"],
      },
      {
        id: "second",
        parent_group_id: null,
        label: "second",
        workflow_path: ["second"],
        node_ids: ["second/c", "second/d"],
        direct_node_ids: ["second/c", "second/d"],
        edge_ids: ["second-inside"],
        direct_edge_ids: ["second-inside"],
        entry_node_ids: ["second/c"],
        exit_node_ids: ["second/d"],
      },
    ],
    entry_node_ids: ["start"],
    exit_node_ids: ["end"],
  };
}

function reactLikeWorkflowGraph(): WorkflowGraphView {
  const internalIds = [
    "agent/start",
    "agent/prepare",
    "agent/llm",
    "agent/classify",
    "agent/tool",
    "agent/collect",
    "agent/finish",
  ];
  const internalEdges = [
    workflowEdge("agent/start_prepare", "agent/start", "agent/prepare", 0),
    workflowEdge("agent/prepare_llm", "agent/prepare", "agent/llm", 1),
    workflowEdge("agent/llm_classify", "agent/llm", "agent/classify", 2),
    workflowEdge("agent/classify_tool", "agent/classify", "agent/tool", 3),
    workflowEdge("agent/tool_collect", "agent/tool", "agent/collect", 4),
    workflowEdge("agent/collect_prepare", "agent/collect", "agent/prepare", 5),
    workflowEdge("agent/classify_finish", "agent/classify", "agent/finish", 6),
  ].map((value) => ({ ...value, workflow_path: ["agent"] }));
  return {
    ...workflowGraph(),
    nodes: [
      ...internalIds.map((id) => ({
        ...workflowNode(id, id === "agent/start", id === "agent/finish"),
        workflow_path: ["agent"],
      })),
      workflowNode("translate", false, true),
    ],
    edges: [
      ...internalEdges,
      {
        ...workflowEdge("agent_translate", "agent/finish", "translate", 7),
        workflow_path: [],
      },
    ],
    groups: [
      {
        id: "agent",
        parent_group_id: null,
        label: "agent",
        workflow_path: ["agent"],
        node_ids: internalIds,
        direct_node_ids: internalIds,
        edge_ids: internalEdges.map((edge) => edge.id),
        direct_edge_ids: internalEdges.map((edge) => edge.id),
        entry_node_ids: ["agent/start"],
        exit_node_ids: ["agent/finish"],
      },
    ],
    entry_node_ids: ["agent/start"],
    exit_node_ids: ["translate"],
  };
}

function workflowNode(id: string, entry: boolean, exit: boolean) {
  return {
    id,
    name: id,
    description: null,
    capability: {},
    entry,
    exit,
    policy: null,
    input_plan: null,
    output_binding: null,
    input_contract: {},
    operator_output_contract: {},
    output_contract: {},
  };
}

function workflowEdge(
  id: string,
  fromNode: string,
  toNode: string,
  order: number,
) {
  return {
    id,
    from_node: fromNode,
    to_node: toNode,
    order,
    condition: null,
    policy: null,
  };
}

function rectContainsPoint(rect: GraphRect, point: GraphPoint): boolean {
  return (
    point.x >= rect.left &&
    point.x <= rect.right &&
    point.y >= rect.top &&
    point.y <= rect.bottom
  );
}

function rectContainsNode(rect: GraphRect, position: GraphPoint): boolean {
  return (
    rectContainsPoint(rect, position) &&
    rectContainsPoint(rect, {
      x: position.x + GRAPH_NODE_WIDTH,
      y: position.y + GRAPH_NODE_HEIGHT,
    })
  );
}

function rectContainsRect(parent: GraphRect, child: GraphRect): boolean {
  return (
    rectContainsPoint(parent, { x: child.left, y: child.top }) &&
    rectContainsPoint(parent, { x: child.right, y: child.bottom })
  );
}
