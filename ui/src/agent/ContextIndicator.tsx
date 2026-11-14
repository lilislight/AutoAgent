import { useCallback, useRef, useState } from 'react';
import { Tooltip } from '../components/Tooltip.js';
import { AgentPopover } from './AgentPopover.js';
import { compactTokens, contextPercentage, type ContextUsage } from './contextUsage.js';

const unknown: ContextUsage = { usedTokens: null, windowTokens: null, source: 'unavailable', compacting: false };

export function ContextFacts({ value, loading = false }: { value: ContextUsage; loading?: boolean }) {
  const percent = contextPercentage(value);
  return <div className="agent-context-facts">
    <p className="agent-context-remaining">{loading ? 'Loading…' : percent === null ? 'Usage unavailable'
      : percent > 100 ? 'Context window exceeded' : `${Math.max(0, 100 - Math.round(percent))}% remaining`}</p>
    <p className="agent-context-count"><span>{loading || value.usedTokens === null ? '—' : `~${compactTokens(value.usedTokens)}`}</span><span aria-hidden="true"> / </span><span>{compactTokens(value.windowTokens)}</span> tokens used</p>
    {value.compacting ? <p className="agent-context-note">Compacting…</p> : null}
  </div>;
}

/** A static occupancy ring, not an execution spinner. Touch opens the same details. */
export function ContextIndicator({ value = unknown, loading = false }: { value?: ContextUsage; loading?: boolean }) {
  const [open, setOpen] = useState(false), trigger = useRef<HTMLButtonElement>(null);
  const close = useCallback(() => setOpen(false), []);
  const percent = loading ? null : contextPercentage(value);
  const used = percent === null ? 0 : Math.max(0, Math.min(100, percent));
  const label = percent === null ? 'Context window: usage unavailable' : `Context window: ${Math.round(percent)}% used`;
  return <><Tooltip placement="top" disabled={open} content={<div className="agent-context-hint"><strong>Context window</strong><ContextFacts value={value} loading={loading} /></div>}>
    <button ref={trigger} type="button" className="agent-context-indicator" data-level={percent !== null && percent >= 100 ? 'full' : percent !== null && percent >= 80 ? 'high' : 'normal'}
      aria-label={loading ? 'Context window: loading usage' : label} aria-busy={loading || undefined} aria-expanded={open} aria-haspopup="dialog" onClick={() => setOpen(value => !value)}>
      <svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><circle className="agent-context-track" cx="12" cy="12" r="9" /><circle className="agent-context-fill" cx="12" cy="12" r="9" pathLength="100" opacity={used > 0 ? 1 : 0} strokeDasharray={`${used} 100`} transform="rotate(-90 12 12)" /></svg>
    </button>
  </Tooltip>{open ? <AgentPopover anchor={trigger} label="Context window" className="agent-context-popover" onClose={close}><ContextFacts value={value} loading={loading} /></AgentPopover> : null}</>;
}
