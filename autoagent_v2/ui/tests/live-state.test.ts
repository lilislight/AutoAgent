import assert from "node:assert/strict";
import test from "node:test";
import { shouldRefreshRuntimeState } from "../src/liveState.js";

test("full State refreshes only at stable, child, and explicit stream boundaries", () => {
  assert.equal(shouldRefreshRuntimeState("node_occurrence.started"), false);
  assert.equal(shouldRefreshRuntimeState("operator_call.completed"), false);
  assert.equal(shouldRefreshRuntimeState("node_occurrence.completed"), false);
  assert.equal(shouldRefreshRuntimeState("invocation.waiting"), true);
  assert.equal(shouldRefreshRuntimeState("invocation.completed"), true);
  assert.equal(shouldRefreshRuntimeState("child_invocation.planned"), true);
  assert.equal(shouldRefreshRuntimeState("child_invocation.phase_changed"), true);
});
