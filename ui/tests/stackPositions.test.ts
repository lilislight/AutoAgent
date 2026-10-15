import { test } from "node:test";
import assert from "node:assert/strict";
import { StackPositions } from "../src/stackPositions.js";

test("stack positions belong to each invocation and individual Node or Edge", () => {
  const memory = new StackPositions();
  memory.write("run1", "node", "refine", 24, 7);
  memory.write("run1", "edge", "refine", 24, 2);
  assert.equal(memory.read("run1", "node", "refine", 24), 7);
  assert.equal(memory.read("run1", "edge", "refine", 24), 2);
  assert.equal(memory.read("run1", "node", "publish", 24), 0);
  assert.equal(memory.read("run2", "node", "refine", 24), 0);
});

test("remembered execution ordinals survive newly appended executions", () => {
  const memory = new StackPositions();
  memory.write("run", "node", "refine", 24, 23);
  assert.equal(memory.read("run", "node", "refine", 24), 23);
  assert.equal(memory.read("run", "node", "refine", 27), 26);
});

test("reopening preserves a fractional wheel position without snapping", () => {
  const memory = new StackPositions();
  memory.write("run", "node", "refine", 24, 7.25);
  assert.equal(memory.read("run", "node", "refine", 24), 7.25);
  assert.equal(memory.read("run", "node", "refine", 27), 10.25);
});

test("position memory is bounded and evicts the least recently visited stack", () => {
  const memory = new StackPositions(2);
  memory.write("run", "node", "a", 24, 7);
  memory.write("run", "node", "b", 24, 4);
  assert.equal(memory.read("run", "node", "a", 24), 7);
  memory.write("run", "edge", "c", 24, 1);
  assert.equal(memory.read("run", "node", "b", 24), 0);
  assert.equal(memory.read("run", "node", "a", 24), 7);
});
