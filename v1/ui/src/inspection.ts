import type { RuntimeEvent } from "./types.js";

/** Return whether an Event belongs to one concrete NodeExecution. */
export function runtimeEventBelongsToNodeExecution(
  event: RuntimeEvent,
  nodeExecutionId: string,
): boolean {
  if (event.subject_id === nodeExecutionId) return true;
  return [
    event.payload.node_execution_id,
    event.payload.source_execution_id,
  ].some(
    (value) => value !== null && value !== undefined && String(value) === nodeExecutionId,
  );
}
