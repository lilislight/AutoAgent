import type { ExecutionActionInput, ExecutionActionKind, ExecutionControls, ExecutionOperation } from './types.js';

// Only observation metadata belongs in browser storage, never action payloads.
export interface PendingOperation {
  operation_id: string;
  action?: ExecutionActionKind;
  target_invocation_id?: string;
  wait_id?: string;
  recovery_reason?: 'paused' | 'interrupted';
  expected_version?: number;
}

export function pendingOperation(input: ExecutionActionInput, controls?: ExecutionControls): PendingOperation {
  return { operation_id: input.request_id, action: input.action,
    ...(input.target_invocation_id ? { target_invocation_id: input.target_invocation_id } : {}),
    ...(input.wait_id ? { wait_id: input.wait_id } : {}),
    ...(input.action === 'recover' && (controls?.recovery_reason === 'paused' || controls?.recovery_reason === 'interrupted')
      ? { recovery_reason: controls.recovery_reason, expected_version: input.expected_version } : {}) };
}

export function readPendingOperation(value: string | null): PendingOperation | null {
  if (!value) return null;
  // An older open tab may still contain only an ID. Its result stays unknown.
  if (!value.startsWith('{')) return { operation_id: value };
  try {
    const parsed = JSON.parse(value) as PendingOperation;
    if (typeof parsed?.operation_id !== 'string' || !parsed.operation_id) return null;
    const actions = ['pause', 'recover', 'cancel', 'respond', 'signal'];
    return { operation_id: parsed.operation_id,
      ...(actions.includes(parsed.action ?? '') ? { action: parsed.action } : {}),
      ...(typeof parsed.target_invocation_id === 'string' ? { target_invocation_id: parsed.target_invocation_id } : {}),
      ...(typeof parsed.wait_id === 'string' ? { wait_id: parsed.wait_id } : {}),
      ...(parsed.recovery_reason === 'paused' || parsed.recovery_reason === 'interrupted' ? { recovery_reason: parsed.recovery_reason } : {}),
      ...(Number.isSafeInteger(parsed.expected_version) && parsed.expected_version! >= 0 ? { expected_version: parsed.expected_version } : {}) };
  } catch { return null; }
}

export function operationStateMatches(pending: PendingOperation, controls: ExecutionControls): boolean {
  switch (pending.action) {
    case 'pause': return controls.control_state === 'paused';
    case 'cancel': return controls.status === 'cancelled';
    case 'recover': return controls.last_recovery_id === pending.operation_id
      // Continuing a suspended graph does not reopen terminal work and has no
      // terminal-recovery request marker. Check that its suspension was lifted.
      || Boolean(pending.recovery_reason && pending.expected_version !== undefined
        && controls.expected_version > pending.expected_version && controls.control_state === 'active' && controls.recovery_reason === null);
    case 'respond': {
      const target = controls.response_target;
      return Boolean(pending.wait_id && target && target.wait_id === pending.wait_id
        && target.invocation_id === (pending.target_invocation_id ?? controls.invocation_id) && target.status === 'resumed');
    }
    default: return false;
  }
}

export type OperationObservation = { kind: 'receipt'; receipt: ExecutionOperation }
  | { kind: 'reconciled'; controls: ExecutionControls; message: string | null };

export async function observeOperation(pending: PendingOperation, io: {
  receipt: () => Promise<ExecutionOperation>;
  controls: () => Promise<ExecutionControls>;
  unavailable: (error: unknown) => boolean;
}): Promise<OperationObservation> {
  try { return { kind: 'receipt', receipt: await io.receipt() }; }
  catch (error) { if (!io.unavailable(error)) throw error; }
  const controls = await io.controls();
  return { kind: 'reconciled', controls, message: operationStateMatches(pending, controls) ? null
    : 'The execution state has been refreshed, but this operation’s result could not be confirmed. Review the current state before submitting another action.' };
}
