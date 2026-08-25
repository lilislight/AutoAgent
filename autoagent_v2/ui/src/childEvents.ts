import type {
  ChildSessionSummary,
  InvocationSummary,
  TraceEvent,
} from "./types";

type ChildTraceEvent = Pick<
  TraceEvent,
  "kind" | "status" | "subject_ids" | "attributes"
>;
type ParentIdentity = Pick<
  InvocationSummary,
  "invocation_id" | "session_id" | "root_session_id"
>;

const PHASES = new Set(["planned", "opened", "accepted", "terminal"]);

export function applyChildTrace(
  current: ChildSessionSummary[],
  event: ChildTraceEvent,
  parent: ParentIdentity,
): ChildSessionSummary[] {
  if (event.kind === "child_invocation.phase_changed") {
    const creationId = event.subject_ids.creation_id;
    const unitIndex = event.attributes.unit_index;
    const phase = event.status;
    if (
      !creationId ||
      typeof unitIndex !== "number" ||
      !Number.isSafeInteger(unitIndex) ||
      typeof phase !== "string" ||
      !PHASES.has(phase)
    ) return current;
    return current.map((item) =>
      item.creation_id === creationId && item.unit_index === unitIndex
        ? { ...item, phase: phase as ChildSessionSummary["phase"] }
        : item,
    );
  }
  if (event.kind !== "child_invocation.planned") return current;
  const creationId = event.subject_ids.creation_id;
  const parentOccurrenceId = event.subject_ids.parent_occurrence_id;
  const workflowId = event.subject_ids.workflow_id;
  const workflowRevisionId = event.subject_ids.workflow_revision_id;
  const mode = event.attributes.mode;
  const plannedEventSequence = event.attributes.planned_event_sequence;
  const units = event.attributes.units;
  if (
    !creationId ||
    !parentOccurrenceId ||
    !workflowId ||
    !workflowRevisionId ||
    (mode !== "await" && mode !== "spawn") ||
    typeof plannedEventSequence !== "number" ||
    !Number.isSafeInteger(plannedEventSequence) ||
    !Array.isArray(units)
  ) return current;
  const known = new Set(current.map((item) => item.session_id));
  const appended = [...current];
  for (const unit of units) {
    if (!isUnit(unit) || known.has(unit.session_id)) continue;
    known.add(unit.session_id);
    appended.push({
      session_id: unit.session_id,
      root_session_id: parent.root_session_id,
      parent_session_id: parent.session_id,
      parent_invocation_id: parent.invocation_id,
      creation_id: creationId,
      unit_index: unit.unit_index,
      parent_occurrence_id: parentOccurrenceId,
      mode,
      planned_workflow_id: workflowId,
      planned_workflow_revision_id: workflowRevisionId,
      planned_invocation_id: unit.invocation_id,
      planned_event_sequence: plannedEventSequence,
      phase: "planned",
      current_invocation_id: null,
      invocation_count: 0,
      workflow_id: null,
      workflow_revision_id: null,
      status: "planned",
      created_at_ns: null,
      updated_at_ns: null,
    });
  }
  return appended;
}

function isUnit(
  value: unknown,
): value is { invocation_id: string; session_id: string; unit_index: number } {
  if (typeof value !== "object" || value === null) return false;
  const unit = value as Record<string, unknown>;
  return (
    typeof unit.invocation_id === "string" &&
    unit.invocation_id.length > 0 &&
    typeof unit.session_id === "string" &&
    unit.session_id.length > 0 &&
    typeof unit.unit_index === "number" &&
    Number.isSafeInteger(unit.unit_index) &&
    unit.unit_index >= 0
  );
}
