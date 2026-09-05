import type {
  InvocationDetail,
  InvocationSummary,
} from "./types.js";

const TERMINAL_STATES = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

export function mergeInvocationStatus(
  detail: InvocationDetail,
  status: InvocationSummary | null | undefined,
): InvocationDetail {
  if (!status || status.id !== detail.id) return detail;
  if (status.updated_at_ms < detail.updated_at_ms) return detail;
  if (
    status.updated_at_ms === detail.updated_at_ms
    && TERMINAL_STATES.has(detail.state)
    && !TERMINAL_STATES.has(status.state)
  ) {
    return detail;
  }
  return { ...detail, ...status };
}
