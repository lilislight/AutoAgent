import { useEffect, useRef, useState } from 'react';
import { newId } from '../ids.js';
import { LoadingIndicator } from '../components/LoadingState.js';
import { Tooltip } from '../components/Tooltip.js';
import { AgentIcon } from './AgentIcon.js';

export type ForkHandler = (invocation: string, destination: string, signal: AbortSignal) => Promise<void>;

export function ForkButton({ invocation, onFork }: { invocation: string; onFork: ForkHandler }) {
  const destination = useRef<string | null>(null), request = useRef<AbortController | null>(null);
  const [busy, setBusy] = useState(false), [error, setError] = useState('');
  useEffect(() => () => request.current?.abort(), []);
  const fork = async () => {
    if (request.current) return;
    destination.current ??= newId();
    const controller = new AbortController(); request.current = controller;
    setBusy(true); setError('');
    try { await onFork(invocation, destination.current, controller.signal); }
    catch (reason) { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { if (!controller.signal.aborted) { setBusy(false); request.current = null; } }
  };
  return <><Tooltip content={busy ? 'Forking conversation' : 'Fork conversation'}><button type="button" className="agent-answer-copy" aria-label={busy ? 'Forking conversation' : 'Fork conversation'} aria-busy={busy} disabled={busy} onClick={() => void fork()}>
    {busy ? <LoadingIndicator small decorative label="Forking conversation" /> : <AgentIcon name="fork" />}
  </button></Tooltip>{error ? <span className="agent-copy-error" role="alert">{error}</span> : null}</>;
}
