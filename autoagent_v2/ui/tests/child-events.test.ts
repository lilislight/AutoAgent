import assert from "node:assert/strict";
import test from "node:test";
import { applyChildTrace } from "../src/childEvents.js";

const parent = {
  invocation_id: "parent-invocation",
  session_id: "parent-session",
  root_session_id: "parent-session",
};

test("planned child trace adds every durable unit identity", () => {
  const event = {
    kind: "child_invocation.planned",
    status: "planned",
    subject_ids: {
      creation_id: "creation",
      parent_occurrence_id: "occurrence",
      workflow_id: "child",
      workflow_revision_id: "child:revision",
    },
    attributes: {
      mode: "spawn",
      planned_event_sequence: 7,
      units: [
        { invocation_id: "inv-0", session_id: "session-0", unit_index: 0 },
        { invocation_id: "inv-1", session_id: "session-1", unit_index: 1 },
      ],
    },
  };
  const children = applyChildTrace([], event, parent);
  assert.equal(children.length, 2);
  assert.equal(children[0].planned_workflow_id, "child");
  assert.equal(children[1].planned_invocation_id, "inv-1");
});

test("phase trace updates the matching child without losing siblings", () => {
  const planned = applyChildTrace([], {
    kind: "child_invocation.planned",
    status: "planned",
    subject_ids: {
      creation_id: "creation",
      parent_occurrence_id: "occurrence",
      workflow_id: "child",
      workflow_revision_id: "child:revision",
    },
    attributes: {
      mode: "await",
      planned_event_sequence: 7,
      units: [
        { invocation_id: "inv-0", session_id: "session-0", unit_index: 0 },
        { invocation_id: "inv-1", session_id: "session-1", unit_index: 1 },
      ],
    },
  }, parent);
  const updated = applyChildTrace(planned, {
    kind: "child_invocation.phase_changed",
    status: "terminal",
    subject_ids: { creation_id: "creation" },
    attributes: { unit_index: 1 },
  }, parent);
  assert.equal(updated[0].phase, "planned");
  assert.equal(updated[1].phase, "terminal");
});
