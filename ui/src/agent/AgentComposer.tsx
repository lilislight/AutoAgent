import { useLayoutEffect, type RefObject } from 'react';
import { composerHeight } from './composerSizing.js';
import { Tooltip } from '../components/Tooltip.js';
import { LoadingIndicator } from '../components/LoadingState.js';
import { AgentIcon } from './AgentIcon.js';
import type { ComposerAction } from './composerActions.js';
import type { SessionSettings } from './types.js';

const approvalLabels = { policy: 'Default', always_ask: 'Always ask', auto_approve: 'Auto approve' };

interface Props {
  controlsDisabled?: boolean; primary: ComposerAction; onControl: (action: 'pause' | 'recover') => void;
  session: string; draft: string; busy: boolean; running: boolean; disabled: boolean; canSend: boolean;
  model?: string | null; approval?: SessionSettings['approval_mode']; caption: string;
  inputRef: RefObject<HTMLTextAreaElement | null>; sidebar: boolean; onSidebar: () => void;
  onDraft: (value: string) => void; onSend: () => void; onPanel: (panel: 'Tools' | 'Settings') => void;
  onSendNow?: () => void; canSendNow?: boolean;
}

export function AgentComposer({ controlsDisabled = false, primary, onControl, session, draft, busy, running, disabled, canSend, model, approval, caption, inputRef, sidebar, onSidebar, onDraft, onSend, onSendNow, canSendNow = false, onPanel }: Props) {
  useLayoutEffect(() => {
    const element = inputRef.current;
    if (!element) return;
    const resize = () => {
      if (!element.clientWidth) return;
      const style = getComputedStyle(element);
      const padding = parseFloat(style.paddingTop) + parseFloat(style.paddingBottom);
      element.style.height = 'auto';
      const value = composerHeight(element.scrollHeight, parseFloat(style.lineHeight), padding, innerHeight);
      element.style.height = `${value.height}px`;
      element.style.overflowY = value.overflowing ? 'auto' : 'hidden';
    };
    resize();
    let width = element.clientWidth;
    const observer = new ResizeObserver(() => { if (width !== element.clientWidth) { width = element.clientWidth; resize(); } });
    observer.observe(element); window.addEventListener('resize', resize);
    return () => { observer.disconnect(); window.removeEventListener('resize', resize); };
  }, [draft, inputRef]);
  return <>
    <form className="agent-composer" onSubmit={event => { event.preventDefault(); if (canSend && draft.trim() && !busy) onSend(); }}>
      <label className="sr-only" htmlFor={`agent-message-${session}`}>Message Agent</label>
      <textarea ref={inputRef} id={`agent-message-${session}`} rows={2} value={draft} onChange={event => onDraft(event.target.value)} placeholder={running ? 'Queue the next task, or send an update now…' : 'Ask, explore, or make something…'} onKeyDown={event => {
        if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); if (canSend && draft.trim() && !busy) onSend(); }
      }} disabled={disabled} />
      <div className="agent-composer-toolbar"><Tooltip content={sidebar ? 'Hide conversations' : 'Show conversations'}><button type="button" className="agent-icon-button" aria-label="Toggle conversations" aria-controls="agent-conversations" aria-expanded={sidebar} onClick={onSidebar}><AgentIcon name="panel" /></button></Tooltip>
        <Tooltip content="Available tools and skills"><button type="button" className="agent-icon-button" aria-label="Available tools and skills" disabled={controlsDisabled} onClick={() => onPanel('Tools')}><AgentIcon name="tools" /></button></Tooltip>
        <Tooltip content={model ?? 'Provider default'}><button type="button" className="agent-model-picker" disabled={controlsDisabled} onClick={() => onPanel('Settings')}><span>{model ?? 'Provider default'}</span><AgentIcon name="chevron" /></button></Tooltip>
        <button type="button" className="agent-approval-picker" disabled={controlsDisabled} onClick={() => onPanel('Settings')}><AgentIcon name="shield" />{controlsDisabled ? 'Approval mode' : approvalLabels[approval ?? 'policy']}</button>
        {draft.trim() && onSendNow ? <Tooltip content="Send as a message to the current task without starting a new turn"><button type="button" className="agent-send-now" disabled={busy || !canSendNow} onClick={onSendNow}><AgentIcon name="message" />Send now</button></Tooltip> : null}
        <Tooltip content={primary.kind === 'pause' ? 'Pause after the current step completes' : primary.label}><button type={primary.kind === 'send' ? 'submit' : 'button'} className="agent-send" aria-label={primary.label} aria-busy={primary.disabled && primary.working || undefined} disabled={primary.disabled} onClick={primary.kind === 'pause' || primary.kind === 'recover' ? () => onControl(primary.kind as 'pause' | 'recover') : undefined}>
          {primary.working ? <LoadingIndicator label={primary.label} small decorative /> : <AgentIcon name={primary.kind === 'recover' ? 'play' : primary.kind === 'pause' || primary.kind === 'none' && running ? 'activity' : 'send'} />}
        </button></Tooltip>
      </div>
    </form>
    <div className="agent-composer-caption" role="status" aria-live="polite"><span>{caption}</span></div>
  </>;
}
