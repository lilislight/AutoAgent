import assert from "node:assert/strict";
import test from "node:test";
import { RequestGate } from "../src/requestGate.js";

test("rejects an old bootstrap response after a newer selection starts", () => {
  const gate = new RequestGate();
  const oldBootstrap = gate.start("invocation-bootstrap");
  const newBootstrap = gate.start("invocation-bootstrap");

  assert.equal(gate.isCurrent(oldBootstrap), false);
  assert.equal(gate.isCurrent(newBootstrap), true);
});

test("invalidates late SSE messages when navigation leaves an invocation", () => {
  const gate = new RequestGate();
  const stream = gate.start("sse");

  gate.invalidate("sse");

  assert.equal(gate.isCurrent(stream), false);
});

test("allows only one in-flight Load More request in a scope", () => {
  const gate = new RequestGate();
  const first = gate.tryStartExclusive("more-sessions");

  assert.notEqual(first, null);
  assert.equal(gate.tryStartExclusive("more-sessions"), null);
  assert.equal(gate.finish(first!), true);
  assert.notEqual(gate.tryStartExclusive("more-sessions"), null);
});

test("releases Load More after selection invalidates its stale response", () => {
  const gate = new RequestGate();
  const oldPage = gate.tryStartExclusive("more-invocations");

  gate.invalidate("more-invocations");

  assert.equal(gate.isCurrent(oldPage!), false);
  assert.notEqual(gate.tryStartExclusive("more-invocations"), null);
});
