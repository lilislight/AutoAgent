import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { UserEventTimeline } from "../src/components/UserEventTimeline.js";
import type { UserEvent } from "../src/types.js";

test("renders UserEvent type, independent sequence, and data", () => {
  const event: UserEvent = {
    id: "user-1",
    session_id: "session",
    invocation_id: "invocation",
    sequence: 7,
    kind: "assistant.token",
    payload: { text: "hello" },
    occurrence_id: "agent@root",
    occurred_at_ns: "1000",
  };

  const markup = renderToStaticMarkup(
    <UserEventTimeline events={[event]} hiddenCount={3} live />,
  );

  assert.match(markup, /assistant\.token/);
  assert.match(markup, /&quot;text&quot;:&quot;hello&quot;/);
  assert.match(markup, /#7/);
  assert.match(markup, /agent@root/);
  assert.match(markup, /Live UserEvent stream/);
  assert.match(markup, /3 earlier positions/);
});
