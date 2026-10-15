import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { computeLayout, fallbackLayout, routeDefinition, NODE_WIDTH, NODE_HEIGHT, type LayoutEngine } from "../src/layout.js";
import { segmentIntersectsRect } from "../src/graphRouting.js";
import type { GraphDefinition } from "../src/types.js";

const ELK = createRequire(import.meta.url)("elkjs/lib/elk.bundled.js") as new () => LayoutEngine;

function graph(): GraphDefinition {
  return {
    nodes: ["start", "branch", "finish"].map(id => ({ id, executable: { kind: "operator", id }, mapped: false })),
    edges: [
      { id: "forward", source: "start", target: "branch", on: "complete", conditional: false },
      { id: "parallel", source: "start", target: "branch", on: "error", conditional: false },
      { id: "finish", source: "branch", target: "finish", on: "complete", conditional: false },
      { id: "back", source: "finish", target: "start", on: "complete", conditional: false },
      { id: "self", source: "branch", target: "branch", on: "complete", conditional: false },
    ],
    entry_node_ids: ["start"], exit_node_ids: ["finish"],
  };
}

test("layout supports parallel edges, graph back edges and self loops with distinct routes", async () => {
  const definition = graph();
  const layout = await computeLayout(definition, new ELK());
  for (const edge of definition.edges) {
    const route = layout.routes[edge.id];
    assert.ok(route.length >= 2);
    assert.equal(route[0].x, layout.positions[edge.source].x + NODE_WIDTH);
    assert.equal(route.at(-1)!.x, layout.positions[edge.target].x);
  }
  assert.notDeepEqual(layout.routes.forward, layout.routes.parallel);
  assert.ok(layout.routes.self.length >= 4);
});

test("fallback keeps all exact node IDs including punctuation and non-ASCII names", () => {
  const definition = graph();
  definition.nodes[0].id = "嵌入.节点/开始";
  assert.deepEqual(Object.keys(fallbackLayout(definition).positions), definition.nodes.map(node => node.id));
});

test("rerouting a dragged graph avoids intervening nodes and retains distinct parallel lanes", () => {
  const definition = graph();
  const positions = { start: { x: 0, y: 0 }, branch: { x: 700, y: 0 }, finish: { x: 340, y: -20 } };
  const routes = routeDefinition(definition, positions);
  const obstacle = { left: 340, top: -20, right: 340 + NODE_WIDTH, bottom: -20 + NODE_HEIGHT };
  for (const id of ["forward", "parallel"]) {
    for (let i = 1; i < routes[id].length; i++) {
      assert.equal(segmentIntersectsRect(routes[id][i - 1], routes[id][i], obstacle), false);
    }
  }
  assert.notDeepEqual(routes.forward, routes.parallel);
});
