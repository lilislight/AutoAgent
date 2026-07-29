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
import { loadSavedLayout, saveLayout } from "../src/layout.js";
import type { WorkflowGraphView } from "../src/types.js";

type TestCase = {
  name: string;
  run: () => void | Promise<void>;
};

const tests: TestCase[] = [
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
  const node = (id: string, entry: boolean, exit: boolean) => ({
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
  });
  return {
    workflow_id: "workflow",
    workflow_version: "1",
    revision_id: "revision",
    definition_hash: "hash",
    operator_manifest_hash: "manifest",
    name: "test",
    description: null,
    nodes: [
      node("source", true, false),
      node("target", false, true),
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
    operator_manifests: [],
    entry_node_ids: ["source"],
    exit_node_ids: ["target"],
    loop_regions: [],
  };
}
