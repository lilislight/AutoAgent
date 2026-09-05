import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { RuntimeSpans } from "../src/components/RuntimeSpans.js";
import type { RuntimeSpan } from "../src/runtimeProjection.js";

test("renders NodeOccurrence and mapped OperatorCall spans without replacing Trace", () => {
  const spans: RuntimeSpan[] = [
    {
      id: "node:map@root",
      kind: "node",
      occurrenceId: "map@root",
      nodeId: "map",
      operatorId: null,
      unitIndex: null,
      status: "running",
      startedAtNs: "1000000",
      completedAtNs: null,
      durationNs: null,
    },
    {
      id: "operator:call-2",
      kind: "operator",
      occurrenceId: "map@root",
      nodeId: "map",
      operatorId: "search",
      unitIndex: 2,
      status: "completed",
      startedAtNs: "1100000",
      completedAtNs: "1600000",
      durationNs: "500000",
    },
  ];

  const markup = renderToStaticMarkup(<RuntimeSpans spans={spans} />);

  assert.match(markup, />map</);
  assert.match(markup, />search</);
  assert.match(markup, /unit 2/);
  assert.match(markup, /500μs/);
});
