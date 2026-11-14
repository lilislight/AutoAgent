import { useLayoutEffect, type RefObject } from 'react';
import { composerHeight } from './composerSizing.js';
import { Tooltip } from '../components/Tooltip.js';
import { LoadingIndicator } from '../components/LoadingState.js';
import { ComposerSettings } from './ComposerSettings.js';
import { AgentIcon } from './AgentIcon.js';
import type { ComposerAction } from './composerActions.js';
import type { AgentCatalog, SessionSettings } from './types.js';
import type { ContextUsage } from './contextUsage.js';

interface Props {
  controlsDisabled?: boolean; primary: ComposerAction; onControl: (action: 'pause' | 'recover' | 'cancel') => void;
  session: string; draft: string; busy: boolean; running: boolean; disabled: boolean; canSend: boolean;
  caption: string;
  inputRef: RefObject<HTMLTextAreaElement | null>;
  onDraft: (value: string) => void; onSend: () => void;
  settings?: SessionSettings; providerOptions?: NonNullable<AgentCatalog['provider_options']>; onSettingsChange?: (settings: SessionSettings) => Promise<void>; canCancel?: boolean;
  onSendNow?: () => void; canSendNow?: boolean;
  contextUsage?: ContextUsage; contextLoading?: boolean;
}

export function AgentComposer({ controlsDisabled = false, primary, onControl, session, draft, busy, running, disabled, canSend, caption, inputRef, onDraft, onSend, onSendNow, canSendNow = false, settings, providerOptions = [], onSettingsChange, canCancel = false, contextUsage, contextLoading }: Props) {
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
    <form className="agent-composer" onSubmit={event => { event.preventDefault(); if (primary.kind === 'send' && !primary.disabled && canSend && draft.trim() && !busy) onSend(); }}>
      <label className="sr-only" htmlFor={`agent-message-${session}`}>Message Agent</label>
      <textarea ref={inputRef} id={`agent-message-${session}`} rows={2} value={draft} onChange={event => onDraft(event.target.value)} placeholder={running ? 'Queue a message…' : 'Message Agent…'} onKeyDown={event => {
        if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); if (primary.kind === 'send' && !primary.disabled && canSend && draft.trim() && !busy) onSend(); }
      }} disabled={disabled} />
      <div className="agent-composer-toolbar">
        <ComposerSettings settings={settings} providers={providerOptions} disabled={controlsDisabled || busy} onChange={onSettingsChange} contextUsage={contextUsage} contextLoading={contextLoading} />
        {draft.trim() && onSendNow ? <Tooltip content="Send to current task"><button type="button" className="agent-send-now" disabled={busy || !canSendNow} onClick={onSendNow}><AgentIcon name="message" />Send now</button></Tooltip> : null}
        <div className="agent-main-control" tabIndex={canCancel && primary.disabled ? 0 : undefined} aria-label={canCancel && primary.disabled ? "Task controls" : undefined}>{canCancel ? <button type="button" className="agent-cancel-action" aria-label="Cancel task" onClick={() => onControl('cancel')}><AgentIcon name="stop" /><span>Cancel task</span></button> : null}
        <Tooltip content={primary.kind === 'pause' ? 'Pause after the current step completes' : primary.label}><button type={primary.kind === 'send' ? 'submit' : 'button'} className="agent-send" aria-label={primary.label} aria-busy={primary.disabled && primary.working || undefined} disabled={primary.disabled} onClick={primary.kind === 'pause' || primary.kind === 'recover' ? () => onControl(primary.kind as 'pause' | 'recover' | 'cancel') : undefined}>
          {primary.working ? <LoadingIndicator label={primary.label} small decorative /> : <AgentIcon name={primary.kind === 'recover' ? 'play' : primary.kind === 'pause' ? 'pause' : 'send'} />}
        </button></Tooltip></div>
      </div>
    </form>
    {caption ? <div className="agent-composer-caption" role="status" aria-live="polite"><span>{caption}</span></div> : null}
  </>;
}
