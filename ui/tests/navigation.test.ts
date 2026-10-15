import { test } from "node:test";
import assert from "node:assert/strict";
import { readLocation, writeLocation } from "../src/navigation.js";
import type { LocationState } from "../src/types.js";

test("definition deep links preserve exact Workflow, Revision and selected object IDs", () => {
  const state: LocationState = {
    view: "workflows", execution: null, session: null,
    workflow: "目录/嵌入", revision: "目录/嵌入:012345",
    selection: { kind: "node", id: "prepare.步骤 & input" },
  };
  assert.deepEqual(readLocation(writeLocation(state)), state);
  const edge: LocationState = { ...state, selection: { kind: "edge", id: "prepare->finish" } };
  assert.deepEqual(readLocation(writeLocation(edge)), edge);
  assert.equal(new URLSearchParams(writeLocation(edge)).has("node"), false);
});

test("empty navigation has no synthetic selection or Revision", () => {
  assert.deepEqual(readLocation(""), { view: "workflows", execution: null, session: null, workflow: null, revision: null, selection: null });
});

test("execution deep links preserve the selected invocation and object", () => {
  const state: LocationState = { view: "executions", session: null, execution: "child/a & b", workflow: null, revision: null, selection: { kind: "node", id: "wait/approval" } };
  assert.deepEqual(readLocation(writeLocation(state)), state);
});


test("session navigation does not invent an invocation or carry removed search state", () => {
  const state: LocationState = { view: "executions", execution: null, session: "session/a", workflow: null, revision: null, selection: null };
  assert.deepEqual(readLocation(writeLocation(state)), state);
  assert.deepEqual(readLocation("?view=executions&session=session%2Fa&q=rvw&matches=1&workflow=review&revision=review%3Aabc"), state);
});

test("whole definition selection preserves its Workflow and Revision context", () => {
  const state: LocationState = { ...readLocation("?workflow=review&revision=review%3Aabc"), selection: { kind: "workflow", id: "review:abc" } };
  assert.deepEqual(readLocation(writeLocation(state)), state);
});
