const STABLE_INVOCATION_KINDS = new Set([
  "invocation.waiting",
  "invocation.completed",
  "invocation.failed",
  "invocation.cancelled",
]);

const CHILD_BOUNDARY_KINDS = new Set([
  "child_invocation.planned",
  "child_invocation.phase_changed",
]);

/** Full State reconstruction is reserved for durable runtime boundaries. */
export function shouldRefreshRuntimeState(kind: string): boolean {
  return STABLE_INVOCATION_KINDS.has(kind) || CHILD_BOUNDARY_KINDS.has(kind);
}
