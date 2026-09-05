import assert from "node:assert/strict";
import test from "node:test";
import { layoutGraph } from "../src/graph.js";

test("places a DAG in stable source-to-target columns", () => {
  const layout = layoutGraph(
    [{ id: "a" }, { id: "b" }, { id: "c" }],
    [
      { source: "a", target: "b" },
      { source: "b", target: "c" },
    ],
  );
  const positions = Object.fromEntries(layout.nodes.map((node) => [node.id, node.x]));
  assert.ok(positions.a < positions.b);
  assert.ok(positions.b < positions.c);
});

test("keeps looped nodes finite and deterministic", () => {
  const input = [
    { source: "a", target: "b" },
    { source: "b", target: "a" },
  ];
  const first = layoutGraph([{ id: "a" }, { id: "b" }], input);
  const second = layoutGraph([{ id: "a" }, { id: "b" }], input);
  assert.deepEqual(first, second);
  assert.ok(first.nodes.every((node) => Number.isFinite(node.x) && Number.isFinite(node.y)));
});
