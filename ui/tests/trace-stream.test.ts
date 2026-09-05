import assert from "node:assert/strict";
import test from "node:test";
import { handleTraceStreamError } from "../src/traceStream.js";

test("stream Store failure closes the source and stops live refresh", () => {
  let closes = 0;
  let stops = 0;
  const errors: string[] = [];

  handleTraceStreamError(
    JSON.stringify({
      invocation_id: "invocation-1",
      code: "store_unavailable",
      message: "Tracing data is unavailable or corrupt.",
    }),
    "invocation-1",
    {
      closeSource: () => { closes += 1; },
      stopRefresh: () => { stops += 1; },
      showError: (message) => errors.push(message),
    },
  );

  assert.equal(closes, 1);
  assert.equal(stops, 1);
  assert.deepEqual(errors, ["Tracing data is unavailable or corrupt."]);
});

test("malformed stream failure remains terminal and reports no payload detail", () => {
  let closes = 0;
  let stops = 0;
  const errors: string[] = [];

  handleTraceStreamError("secret database exception", "invocation-1", {
    closeSource: () => { closes += 1; },
    stopRefresh: () => { stops += 1; },
    showError: (message) => errors.push(message),
  });

  assert.equal(closes, 1);
  assert.equal(stops, 1);
  assert.deepEqual(errors, ["Tracing data is unavailable or corrupt."]);
});
