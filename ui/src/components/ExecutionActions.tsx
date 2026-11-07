import { BusyLabel, InlineLoading } from './LoadingState';
import { useEffect, useId, useLayoutEffect, useRef, useState, type RefObject } from 'react';
import { useInfiniteQuery, useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, getExecutionControls, getExecutionOperation, submitExecutionAction } from '../api';
import type { ExecutionActionInput, ExecutionActionKind, ExecutionControls, ExecutionTrace } from '../types';
import type { FieldError } from '../jsonSchema';
import { observeOperation, pendingOperation, readPendingOperation, type PendingOperation, type OperationObservation } from '../executionOperations';

const progressLabels: Record<ExecutionActionKind, string> = { pause: 'Pausing…', recover: 'Recovering…', cancel: 'Cancelling…', respond: 'Responding…', signal: 'Sending…' };
function savedOperation(iid: string, value?: PendingOperation | null) {
  try {
    const key = `autoagent.operation:${iid}`;
    if (value === undefined) return readPendingOperation(sessionStorage.getItem(key));
    if (value) sessionStorage.setItem(key, JSON.stringify(value)); else sessionStorage.removeItem(key);
  } catch { /* An in-memory receipt still prevents duplicate requests on this page. */ }
  return null;
}

export function useExecutionActions(trace: ExecutionTrace, active: boolean, replaying: boolean, onRunAgain: () => void, onSettled: () => Promise<void>, observing = false) {
  const iid = trace.invocation.invocation_id, client = useQueryClient();
  const settledCallback = useRef(onSettled), refreshedOperation = useRef<string | null>(null);
  settledCallback.current = onSettled;
  const [cancelOpen, setCancelOpen] = useState(false), cancelButton = useRef<HTMLButtonElement>(null);
  const [pendingOperationState, setPendingOperation] = useState<PendingOperation | null>(() => savedOperation(iid));
  const operationId = pendingOperationState?.operation_id ?? null;
  const [fieldErrors, setFieldErrors] = useState<FieldError[]>([]);
  const [submitting, setSubmitting] = useState<ExecutionActionKind | null>(null), submittingRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const recoveryRefresh = useRef<string | null>(null);
  const cachedOperation = client.getQueryData<OperationObservation>(['execution-operation', operationId]);
  const operationRunning = cachedOperation?.kind === 'receipt' && cachedOperation.receipt.status === 'running';
  const controls = useQuery({ queryKey: ['execution-controls', iid], queryFn: ({ signal }) => getExecutionControls(iid, signal), enabled: active,
    staleTime: 1000, gcTime: 30_000, retry: false, refetchOnWindowFocus: false,
    refetchInterval: () => active && operationRunning ? 700 : active && !observing ? 3000 : false });
  useEffect(() => { if (active) void controls.refetch({ cancelRefetch: false }); }, [trace.frontier, active]);
  useEffect(() => { if (!active || replaying) setCancelOpen(false); }, [active, replaying]);
  const operation = useQuery({ queryKey: ['execution-operation', operationId], queryFn: ({ signal }) => observeOperation(pendingOperationState!, {
    receipt: () => getExecutionOperation(operationId!, signal),
    controls: () => getExecutionControls(iid, signal, null, pendingOperationState!),
    unavailable: error => error instanceof ApiError && error.status === 404,
  }), enabled: active && Boolean(operationId), retry: false,
    refetchInterval: query => active && query.state.data?.kind === 'receipt' && query.state.data.receipt.status === 'running' ? 700 : false, refetchOnWindowFocus: false, gcTime: 30_000 });
  const receipt = operation.data?.kind === 'receipt' ? operation.data.receipt : null;
  useEffect(() => {
    if (receipt?.status !== 'running' || receipt.action !== 'recover' || !controls.data || controls.data.status === trace.invocation.status) return;
    const key = `${receipt.operation_id}:${controls.data.status}:${controls.data.recovery_count}`;
    if (recoveryRefresh.current === key) return;
    recoveryRefresh.current = key;
    void settledCallback.current();
  }, [receipt, controls.data, trace.invocation.status]);
  useEffect(() => {
    const observed = operation.data;
    if (!operationId || !observed || (observed.kind === 'receipt' && observed.receipt.status === 'running')) return;
    savedOperation(iid, null);
    setPendingOperation(null);
    setError(observed.kind === 'receipt' ? observed.receipt.error?.message ?? null : observed.message);
    if (observed.kind === 'reconciled') client.setQueryData(['execution-controls', iid], observed.controls);
    if (refreshedOperation.current !== operationId) {
      refreshedOperation.current = operationId;
      void settledCallback.current();
    }
    void client.invalidateQueries({ predicate: query => ['execution-controls', 'execution-action-spec', 'session-invocations-page', 'execution-family', 'family-children', 'inspector-items', 'execution-record'].includes(String(query.queryKey[0])), refetchType: 'active' });
  }, [operation.data, operationId, iid, client]);
  const unresolved = Boolean(operationId && (operation.isPending || operation.error));
  const busy = Boolean(submitting || unresolved || receipt?.status === 'running');
  const pending = useInfiniteQuery({ queryKey: ['pending-responses', iid, controls.data?.frontier], queryFn: ({ pageParam, signal }) => getExecutionControls(iid, signal, pageParam), initialPageParam: controls.data?.next_cursor ?? null, getNextPageParam: page => page.next_cursor, enabled: active && !replaying && Boolean(controls.data?.actions.respond.available && controls.data.next_cursor), staleTime: 1000, gcTime: 10_000, retry: false, refetchOnWindowFocus: false });
  useEffect(() => { if (active && !replaying && pending.hasNextPage && !pending.isFetchingNextPage && !pending.error) void pending.fetchNextPage(); }, [active, replaying, pending.hasNextPage, pending.isFetchingNextPage, pending.data, pending.error]);
  const pendingResponses = controls.data?.actions.respond.available ? [...new Map([...(controls.data.pending_responses ?? []), ...(pending.data?.pages.flatMap(page => page.pending_responses) ?? [])].map(wait => [`${wait.invocation_id}:${wait.wait_id}`, wait])).values()].filter(wait => wait.store_order == null || wait.store_order <= trace.frontier) : [];
  const send = async (action: ExecutionActionKind, options: Partial<ExecutionActionInput> = {}) => {
    if (submittingRef.current || busy || replaying || !active) return;
    submittingRef.current = true; setSubmitting(action); setError(null); setFieldErrors([]);
    const request_id = crypto.randomUUID();
    const input: ExecutionActionInput = { ...options, ...(action === 'recover' ? { expected_version: controls.data?.expected_version } : {}), invocation_id: iid, action, request_id };
    const pending = pendingOperation(input, controls.data);
    // Retain the observation key even if the page reloads before POST returns.
    savedOperation(iid, pending);
    try {
      const receipt = await submitExecutionAction(input, new AbortController().signal);
      client.setQueryData<OperationObservation>(['execution-operation', receipt.operation_id], () => ({ kind: 'receipt', receipt }));
      const accepted = { ...pending, operation_id: receipt.operation_id };
      savedOperation(iid, accepted); setPendingOperation(accepted); setCancelOpen(false);
    } catch (failure) {
      if (!(failure instanceof ApiError) || failure.status >= 500) {
        setPendingOperation(pending); setCancelOpen(false);
      } else { savedOperation(iid, null); setError(failure.message); setFieldErrors(failure.fields); }
      void controls.refetch();
    } finally { submittingRef.current = false; setSubmitting(null); }
  };
  const actionError = error ?? receipt?.error?.message ?? operation.error?.message ?? null;
  const clearError = () => { setError(null); setFieldErrors([]); };
  const child = Boolean(controls.data && iid !== controls.data.root_invocation_id);
  const continuing = Boolean(controls.data?.recovery_reason || controls.data?.actions.recover.available);
  const stateAction = continuing ? 'recover' : 'pause';
  const availability = continuing ? controls.data?.actions.recover : controls.data?.actions.pause;
  const disabled = replaying || !active || busy;
  const pendingAction = submitting ?? (receipt?.status === 'running' ? receipt.action : null);
  const reason = (available?: { available: boolean; reason: string | null }) => replaying ? 'Execution controls are unavailable during Replay.' : available?.reason ?? undefined;
  return {
    paused: controls.data?.control_state === 'paused', canRespond: Boolean(!replaying && controls.data?.actions.respond.available), pendingResponses, send, busy, error: actionError, fieldErrors, clearError,
    toolbar: <div className="execution-actions" aria-label="Execution controls">
      <button type="button" disabled={disabled || !controls.data?.actions.run_again.available} title={reason(controls.data?.actions.run_again) ?? 'Open a prefilled Run form; execution starts when you submit it.'} onClick={() => { clearError(); onRunAgain(); }}>Run again</button>
      <button type="button" disabled={disabled || !availability?.available} title={reason(availability) ?? (child ? 'Controls the Root and all its owned children.' : stateAction === 'pause' ? 'Pause at a resumable Node boundary.' : 'Recover this Root and its children in the same Invocations. Successful work is preserved.')} onClick={() => void send(stateAction)}><BusyLabel busy={pendingAction === stateAction} label={progressLabels[stateAction]}>{`${continuing ? 'Recover' : 'Pause'}${child ? ' root' : ''}`}</BusyLabel></button>
      <button ref={cancelButton} type="button" className="execution-action-cancel" aria-haspopup="dialog" aria-expanded={cancelOpen} disabled={disabled || !controls.data?.actions.cancel.available} title={reason(controls.data?.actions.cancel) ?? 'Cancel this Root and its owned child executions.'} onClick={() => { clearError(); setCancelOpen(true); }}><BusyLabel busy={pendingAction === 'cancel'} label={progressLabels.cancel}>{child ? 'Cancel root' : 'Cancel'}</BusyLabel></button>
      {pendingAction === 'respond' ? <span className="execution-operation-state" role="status">Responding…</span> : null}
      {unresolved && !operation.error ? <InlineLoading label="Checking operation" /> : null}
      {controls.error || actionError ? <div className="execution-control-error" role="alert"><span title={controls.error?.message ?? actionError ?? ''}>{controls.error?.message ?? actionError}</span><button type="button" onClick={() => { if (operation.error) void operation.refetch(); else if (controls.error) void controls.refetch(); else clearError(); }}>{operation.error ? 'Retry status' : controls.error ? 'Retry controls' : 'Dismiss'}</button></div> : null}
    </div>,
    confirmation: cancelOpen && active && !replaying && controls.data ? <CancelConfirmation anchor={cancelButton} rootId={controls.data.root_invocation_id} busy={busy} disabled={!controls.data.actions.cancel.available} error={actionError} onClose={() => setCancelOpen(false)} onConfirm={reason => send('cancel', { reason: reason || undefined })} /> : null,
  };
}

function CancelConfirmation({ anchor, rootId, busy, disabled, error, onClose, onConfirm }: {
  anchor: RefObject<HTMLButtonElement | null>; rootId: string; busy: boolean; disabled: boolean;
  error: string | null; onClose: () => void; onConfirm: (reason: string) => Promise<void>;
}) {
  const dialog = useRef<HTMLDialogElement>(null), reasonId = useId();
  const [reason, setReason] = useState('');
  useLayoutEffect(() => {
    const popup = dialog.current, button = anchor.current; if (!popup || !button) return;
    const previous = document.activeElement;
    popup.showModal();
    const position = () => { const bounds = button.getBoundingClientRect(); popup.style.left = `${Math.max(12, Math.min(bounds.right - popup.offsetWidth, window.innerWidth - popup.offsetWidth - 12))}px`; popup.style.top = `${Math.max(12, Math.min(bounds.bottom + 8, window.innerHeight - popup.offsetHeight - 12))}px`; };
    position(); const observer = new ResizeObserver(position); observer.observe(popup); observer.observe(button);
    window.addEventListener('resize', position);
    return () => { observer.disconnect(); window.removeEventListener('resize', position); popup.close(); if (previous instanceof HTMLElement && previous.isConnected) previous.focus({ preventScroll: true }); };
  }, [anchor]);
  return <dialog ref={dialog} className="cancel-execution-popover" aria-label="Cancel Root execution" onCancel={event => { event.preventDefault(); if (!busy) onClose(); }} onClick={event => { if (event.target !== event.currentTarget || busy) return; const rect = event.currentTarget.getBoundingClientRect(); if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) onClose(); }}>
    <form onSubmit={event => { event.preventDefault(); if (!busy && !disabled) void onConfirm(reason.trim()); }}>
      <h3>Cancel Root execution?</h3><p>This cancels the Root and all its owned child executions.</p><code className="cancel-root-id">{rootId}</code>
      <label className="run-field" htmlFor={reasonId}>Reason <small>Optional</small><textarea id={reasonId} value={reason} onChange={event => setReason(event.target.value)} maxLength={4096} rows={2} disabled={busy} /></label>
      {error ? <p className="run-error" role="alert">{error}</p> : null}
      <footer><button type="button" onClick={onClose} disabled={busy}>Go back</button><button type="submit" className="confirm-cancel" disabled={busy || disabled}><BusyLabel busy={busy} label="Cancelling">Cancel execution</BusyLabel></button></footer>
    </form>
  </dialog>;
}
