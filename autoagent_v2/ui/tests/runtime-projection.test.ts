import assert from "node:assert/strict";
import test from "node:test";
import {
  projectRuntime,
  projectRuntimeState,
} from "../src/runtimeProjection.js";
import type { RuntimeStateRecord, TraceEvent } from "../src/types.js";
import type { WorkflowSnapshot } from "../src/types.js";

test("State remains authoritative when the Trace tail lost an early Node", () => {
  const state = runtimeState({
    occurrences: {
      "early@root": occurrence("early@root", "early", "completed", 10, 20),
      "tail@root": occurrence("tail@root", "tail", "completed", 30, 40),
    },
  });
  const traceTail = [trace("tail-trace", "tail", "tail@root", "failed")];

  const projection = projectRuntime(state, traceTail);

  assert.equal(projection.source, "state");
  assert.equal(projection.nodes.get("early")?.latestStatus, "completed");
  assert.equal(projection.nodes.get("tail")?.latestStatus, "completed");
  assert.equal(projection.nodes.size, 2);
});

test("condition and error Edge decisions include normal and Loop boundary resolutions", () => {
  const state = runtimeState({
    occurrences: {
      "a@0": occurrence("a@0", "a", "completed", 1, 2),
      "b@0": occurrence("b@0", "b", "completed", 3, 4, [
        activation("condition-edge", "a@0", "b"),
      ]),
      "a@1": occurrence("a@1", "a", "completed", 5, 6),
      "b@1": occurrence("b@1", "b", "completed", 7, 8, [
        activation("condition-edge", "a@1", "b"),
      ]),
    },
    resolutions: {
      "condition-edge@2": resolution("condition-edge", "b", false),
      "error-edge@0": resolution("error-edge", "recover", true),
    },
    boundaryResolutions: {
      "error-edge@loop": resolution("error-edge", "recover", false),
    },
  });

  const projection = projectRuntimeState(state);
  assert.deepEqual(projection.edges.get("condition-edge"), {
    edgeId: "condition-edge",
    resolutionCount: 3,
    selectedCount: 2,
    skippedCount: 1,
    latestSelected: false,
  });
  assert.deepEqual(projection.edges.get("error-edge"), {
    edgeId: "error-edge",
    resolutionCount: 2,
    selectedCount: 1,
    skippedCount: 1,
    latestSelected: false,
  });
});

test("completed State reconstructs consumed condition/error Edge decisions from occurrences", () => {
  const workflow: WorkflowSnapshot = {
    schema_version: 1,
    workflow_id: "routing",
    workflow_version: "1",
    workflow_revision_id: "routing:1",
    definition_hash: "hash",
    definition: {
      nodes: [{ id: "source" }, { id: "next" }, { id: "recover" }],
      edges: [
        { id: "complete-edge", source: "source", target: "next", on: "complete" },
        { id: "error-edge", source: "source", target: "recover", on: "error" },
      ],
    },
  };
  const state = runtimeState({
    occurrences: {
      "source@0": occurrence("source@0", "source", "completed", 1, 2),
      "next@0": occurrence("next@0", "next", "completed", 3, 4, [
        activation("complete-edge", "source@0", "next"),
      ]),
      "source@1": occurrence("source@1", "source", "failed", 5, 6),
      "recover@1": occurrence("recover@1", "recover", "completed", 7, 8, [
        activation("error-edge", "source@1", "recover"),
      ]),
    },
  });

  const projection = projectRuntimeState(state, workflow);
  assert.deepEqual(projection.edges.get("complete-edge"), {
    edgeId: "complete-edge",
    resolutionCount: 2,
    selectedCount: 1,
    skippedCount: 1,
    latestSelected: false,
  });
  assert.deepEqual(projection.edges.get("error-edge"), {
    edgeId: "error-edge",
    resolutionCount: 2,
    selectedCount: 1,
    skippedCount: 1,
    latestSelected: true,
  });
});

test("Loop occurrences aggregate into one Node with the latest occurrence status", () => {
  const projection = projectRuntimeState(runtimeState({
    occurrences: {
      "loop@0": occurrence("loop@0", "loop", "completed", 100, 200),
      "loop@1": occurrence("loop@1", "loop", "running", 300, null),
    },
  }));

  assert.equal(projection.nodes.get("loop")?.occurrenceCount, 2);
  assert.equal(projection.nodes.get("loop")?.latestStatus, "running");
  assert.deepEqual(projection.nodes.get("loop")?.statusCounts, {
    completed: 1,
    running: 1,
  });
  assert.equal(projection.spans.filter((span) => span.kind === "node").length, 2);
});

test("map OperatorCall spans preserve unit identity, owning Node, and duration", () => {
  const projection = projectRuntimeState(runtimeState({
    occurrences: {
      "map@root": occurrence("map@root", "map", "running", "1000000", null),
    },
    operatorCalls: {
      "call-0": operatorCall("call-0", "map@root", 0, "completed", "1100000", "1600000"),
      "call-1": operatorCall("call-1", "map@root", 1, "running", "1200000", null),
    },
  }));

  const calls = projection.spans.filter((span) => span.kind === "operator");
  assert.deepEqual(calls.map((span) => span.unitIndex), [0, 1]);
  assert.ok(calls.every((span) => span.nodeId === "map"));
  assert.equal(calls[0]?.durationNs, "500000");
  assert.equal(calls[1]?.durationNs, null);
});

test("Trace fallback counts a Node occurrence once across multiple Trace events", () => {
  const started = trace("started", "work", "work@root", "running", 10);
  const completed = trace("completed", "work", "work@root", "completed", 11);

  const projection = projectRuntime(null, [started, completed]);

  assert.equal(projection.source, "trace");
  assert.equal(projection.nodes.get("work")?.occurrenceCount, 1);
  assert.equal(projection.nodes.get("work")?.latestStatus, "completed");
  assert.equal(projection.spans.length, 0);
});

function runtimeState({
  occurrences = {},
  resolutions = {},
  boundaryResolutions = {},
  operatorCalls = {},
}: {
  occurrences?: Record<string, unknown>;
  resolutions?: Record<string, unknown>;
  boundaryResolutions?: Record<string, unknown>;
  operatorCalls?: Record<string, unknown>;
}): RuntimeStateRecord {
  return {
    state_version: 1,
    invocation: {
      scheduler: {
        occurrences,
        resolutions,
        boundary_resolutions: boundaryResolutions,
        operator_calls: operatorCalls,
      },
    },
  };
}

function occurrence(
  id: string,
  nodeId: string,
  status: string,
  startedAtNs: string | number,
  completedAtNs: string | number | null,
  activations: unknown[] = [],
) {
  return {
    id,
    node_id: nodeId,
    status,
    started_at_ns: startedAtNs,
    completed_at_ns: completedAtNs,
    activations,
  };
}

function activation(edgeId: string, sourceOccurrenceId: string, targetNodeId: string) {
  return {
    edge_id: edgeId,
    source_occurrence_id: sourceOccurrenceId,
    target_node_id: targetNodeId,
  };
}

function resolution(edgeId: string, targetNodeId: string, selected: boolean) {
  return { edge_id: edgeId, target_node_id: targetNodeId, selected };
}

function operatorCall(
  id: string,
  occurrenceId: string,
  unitIndex: number,
  status: string,
  startedAtNs: string,
  completedAtNs: string | null,
) {
  return {
    id,
    occurrence_id: occurrenceId,
    operator_id: "mapped-operator",
    unit_index: unitIndex,
    status,
    started_at_ns: startedAtNs,
    completed_at_ns: completedAtNs,
  };
}

function trace(
  id: string,
  nodeId: string,
  occurrenceId: string,
  status: string,
  sequence = 500,
): TraceEvent {
  return {
    schema_version: 1,
    id,
    session_id: "session",
    trace_sequence: sequence,
    kind: `node_occurrence.${status}`,
    occurred_at_ns: String(sequence),
    invocation_id: "invocation",
    causation_id: null,
    state_version: sequence,
    subject_ids: { node_id: nodeId, occurrence_id: occurrenceId },
    status,
    error: null,
    metrics: null,
    attributes: {},
  };
}
