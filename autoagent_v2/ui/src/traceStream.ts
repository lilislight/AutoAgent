const STORE_UNAVAILABLE_MESSAGE = "Tracing data is unavailable or corrupt.";

export interface TraceStreamErrorActions {
  closeSource(): void;
  stopRefresh(): void;
  showError(message: string): void;
}

export function handleTraceStreamError(
  raw: string,
  invocationId: string,
  actions: TraceStreamErrorActions,
): void {
  actions.closeSource();
  actions.stopRefresh();
  actions.showError(streamErrorMessage(raw, invocationId));
}

function streamErrorMessage(raw: string, invocationId: string): string {
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    if (
      value.invocation_id === invocationId &&
      value.code === "store_unavailable" &&
      value.message === STORE_UNAVAILABLE_MESSAGE
    ) {
      return STORE_UNAVAILABLE_MESSAGE;
    }
  } catch {
    // A malformed terminal frame is still terminal and must not reconnect.
  }
  return STORE_UNAVAILABLE_MESSAGE;
}
