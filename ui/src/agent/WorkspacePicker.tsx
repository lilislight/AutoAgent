import { useCallback, useEffect, useRef, useState } from 'react';
import { SkeletonText } from '../components/LoadingState.js';
import { Tooltip } from '../components/Tooltip.js';
import { AgentIcon } from './AgentIcon.js';
import { AgentPopover } from './AgentPopover.js';
import type { AgentWorkspaceInfo } from './types.js';

export function WorkspacePicker({ value, items, locked, disabled, loading, error, onRetry, onChange, mobile = false, active = true }: {
  mobile?: boolean; active?: boolean; value: string | null; items?: AgentWorkspaceInfo[]; locked: boolean; disabled: boolean;
  loading: boolean; error?: Error | null; onRetry: () => void; onChange: (workspace: string | null) => void;
}) {
  const [open, setOpen] = useState(false), anchor = useRef<HTMLButtonElement>(null);
  const restoreFocus = useRef(false);
  const close = useCallback(() => setOpen(false), []);
  useEffect(() => { if (!active) setOpen(false); }, [active]);
  const workspace = items?.find(item => item.id === value);
  const name = value ? workspace?.name ?? 'Workspace' : 'No workspace';
  const description = value ? workspace?.path ?? value : 'No workspace is bound to this conversation';
  const choose = (id: string | null) => { restoreFocus.current = true; onChange(id); setOpen(false); };
  useEffect(() => {
    if (!open && restoreFocus.current) { restoreFocus.current = false; anchor.current?.focus({ preventScroll: true }); }
  }, [open, value]);
  return <div className="agent-workspace-context">
    {locked ? <Tooltip content={description} disabled={mobile}><span className="agent-bound-workspace"><AgentIcon name="folder" /><span>{name}</span></span></Tooltip>
      : <Tooltip content={description} disabled={mobile}><button ref={anchor} type="button" className="agent-workspace-picker" aria-label={`Choose workspace: ${name}`} aria-haspopup="dialog" aria-expanded={open} disabled={disabled} onClick={() => setOpen(value => !value)}><AgentIcon name="folder" /><span>{name}</span><AgentIcon name="chevron" /></button></Tooltip>}
    {active && open && !locked ? <AgentPopover sheet={mobile} anchor={anchor} label="Choose workspace" onClose={close} className="agent-workspace-options">
      <button type="button" className="agent-workspace-option" aria-pressed={value === null} onClick={() => choose(null)}><AgentIcon name="message" /><span><strong>No workspace</strong></span>{value === null ? <AgentIcon name="check" /> : null}</button>
      {loading ? <div className="agent-workspace-option" role="status" aria-label="Loading workspaces"><SkeletonText width="80%" /></div> : null}
      {items?.map(item => <button key={item.id} type="button" className="agent-workspace-option" aria-pressed={value === item.id} disabled={item.available === false} onClick={() => choose(item.id)}><AgentIcon name="folder" /><span><strong>{item.name}</strong><small>{item.path ?? item.id}{item.available === false ? ' · Unavailable' : ''}</small></span>{value === item.id ? <AgentIcon name="check" /> : null}</button>)}
      {error ? <p className="agent-error-text">Could not load workspaces. <button type="button" onClick={onRetry}>Retry</button></p> : null}
    </AgentPopover> : null}
  </div>;
}
