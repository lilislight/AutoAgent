import assert from "node:assert/strict";
import test from "node:test";
import {
  switchInvocation,
  switchSession,
  switchWorkflow,
  type Selection,
} from "../src/navigation.js";

const selected: Selection = {
  workflow: "workflow-a",
  session: "session-a",
  invocation: "invocation-a",
};

test("switching Workflow clears Session and Invocation ownership", () => {
  const transition = switchWorkflow(selected, "workflow-b");

  assert.equal(transition?.clear, "workflow");
  assert.deepEqual(transition?.selection, {
    workflow: "workflow-b",
    session: null,
    invocation: null,
  });
  assert.ok(transition?.invalidatedScopes.includes("sse"));
  assert.ok(transition?.invalidatedScopes.includes("more-sessions"));
});

test("switching Session preserves Workflow and clears Invocation", () => {
  const transition = switchSession(selected, "session-b");

  assert.equal(transition?.clear, "session");
  assert.deepEqual(transition?.selection, {
    workflow: "workflow-a",
    session: "session-b",
    invocation: null,
  });
  assert.ok(transition?.invalidatedScopes.includes("invocation-bootstrap"));
  assert.ok(transition?.invalidatedScopes.includes("more-invocations"));
});

test("switching Invocation invalidates its bootstrap, stream, and child page", () => {
  const transition = switchInvocation(selected, "invocation-b");

  assert.equal(transition?.clear, "invocation");
  assert.deepEqual(transition?.selection, {
    ...selected,
    invocation: "invocation-b",
  });
  assert.ok(transition?.invalidatedScopes.includes("invocation-bootstrap"));
  assert.ok(transition?.invalidatedScopes.includes("sse"));
  assert.ok(transition?.invalidatedScopes.includes("more-children"));
  assert.ok(transition?.invalidatedScopes.includes("earlier-trace"));
});

test("selecting the current identity is a no-op", () => {
  assert.equal(switchWorkflow(selected, selected.workflow), null);
  assert.equal(switchSession(selected, selected.session), null);
  assert.equal(switchInvocation(selected, selected.invocation), null);
});
