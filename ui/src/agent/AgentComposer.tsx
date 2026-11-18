import { useEffect, useLayoutEffect, useRef, useState, type RefObject } from 'react';
import { composerHeight } from './composerSizing.js';
import { Tooltip } from '../components/Tooltip.js';
import { LoadingIndicator } from '../components/LoadingState.js';
import { ComposerSettings } from './ComposerSettings.js';
import { AgentIcon } from './AgentIcon.js';
import { AgentSheet } from './AgentSheet.js';
import { AddImages, ImageThumbnails, maxImages } from './ComposerImages.js';
import type { ComposerAction } from './composerActions.js';
import type { AgentCatalog, SessionSettings } from './types.js';
import type { ContextUsage } from './contextUsage.js';

interface Props {
  mobile?: boolean; active?: boolean; controlsDisabled?: boolean; primary: ComposerAction; onControl: (action: 'pause' | 'recover' | 'cancel') => void;
  session: string; draft: string; busy: boolean; running: boolean; disabled: boolean; canSend: boolean;
  caption: string;
  inputRef: RefObject<HTMLTextAreaElement | null>;
  onDraft: (value: string) => void; onSend: () => void;
  images: { id: string; file: File }[]; preparingImages?: boolean; onPickImages: (files: File[]) => void; onRemoveImage: (id: string) => void;
  settings?: SessionSettings; providerOptions?: NonNullable<AgentCatalog['provider_options']>; onSettingsChange?: (settings: SessionSettings) => Promise<void>; canCancel?: boolean;
  onSendNow?: () => void; canSendNow?: boolean;
  contextUsage?: ContextUsage; contextLoading?: boolean;
}

export function AgentComposer({ mobile = false, active = true, controlsDisabled = false, primary, onControl, session, draft, busy, running, disabled, canSend, caption, inputRef, onDraft, onSend, images, preparingImages = false, onPickImages, onRemoveImage, onSendNow, canSendNow = false, settings, providerOptions = [], onSettingsChange, canCancel = false, contextUsage, contextLoading }: Props) {
  const [actions, setActions] = useState(false);
  const actionsButton = useRef<HTMLButtonElement>(null);
  const hasContent = !!draft.trim() || images.length > 0;
  const sendNow = hasContent && !!onSendNow;
  const hasActions = canCancel || sendNow;
  useEffect(() => { if (!active || !mobile || !hasActions) setActions(false); }, [active, mobile, hasActions]);

  useLayoutEffect(() => {
    const element = inputRef.current;
    if (!element) return;
    const resize = () => {
      if (!element.clientWidth) return;
      const style = getComputedStyle(element);
      const padding = parseFloat(style.paddingTop) + parseFloat(style.paddingBottom);
      element.style.height = 'auto';
      const value = composerHeight(element.scrollHeight, parseFloat(style.lineHeight), padding, mobile ? window.visualViewport?.height ?? innerHeight : innerHeight);
      element.style.height = `${value.height}px`;
      element.style.overflowY = value.overflowing ? 'auto' : 'hidden';
    };
    resize();
    let width = element.clientWidth;
    const observer = new ResizeObserver(() => { if (width !== element.clientWidth) { width = element.clientWidth; resize(); } });
    observer.observe(element); window.addEventListener('resize', resize);
    if (mobile) window.visualViewport?.addEventListener('resize', resize);
    return () => { observer.disconnect(); window.removeEventListener('resize', resize); if (mobile) window.visualViewport?.removeEventListener('resize', resize); };
  }, [draft, inputRef, mobile]);
  return <>
    <form className="agent-composer" onSubmit={event => { event.preventDefault(); if (primary.kind === 'send' && !primary.disabled && canSend && hasContent && !busy) onSend(); }}>
      {images.length > 0 && <ImageThumbnails images={images} disabled={disabled || preparingImages} active={active} onRemove={onRemoveImage} />}
      <label className="sr-only" htmlFor={`agent-message-${session}`}>Message Agent</label>
      <textarea ref={inputRef} id={`agent-message-${session}`} rows={2} value={draft} onChange={event => onDraft(event.target.value)} placeholder={running ? 'Queue a message…' : 'Message Agent…'} onKeyDown={event => {
        if (!mobile && event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); if (primary.kind === 'send' && !primary.disabled && canSend && hasContent && !busy) onSend(); }
      }} disabled={disabled} />
      <div className="agent-composer-toolbar">
        <AddImages mobile={mobile} preparing={preparingImages} disabled={disabled || !canSend || images.length >= maxImages} onPick={onPickImages} />
        <ComposerSettings mobile={mobile} active={active} settings={settings} providers={providerOptions} disabled={controlsDisabled || busy} onChange={onSettingsChange} contextUsage={contextUsage} contextLoading={contextLoading} />
        {!mobile && sendNow ? <Tooltip content="Send to current task"><button type="button" className="agent-send-now" disabled={busy || !canSendNow} onClick={onSendNow}><AgentIcon name="message" />Send now</button></Tooltip> : null}
        {mobile && hasActions && <button ref={actionsButton} type="button" className="agent-run-actions" aria-label="Run actions"
          aria-haspopup="dialog" aria-expanded={actions} onClick={() => setActions(true)}><AgentIcon name="more" /></button>}
        <div className="agent-main-control" tabIndex={!mobile && canCancel && primary.disabled ? 0 : undefined} aria-label={!mobile && canCancel && primary.disabled ? "Task controls" : undefined}>{!mobile && canCancel ? <button type="button" className="agent-cancel-action" aria-label="Cancel task" onClick={() => onControl('cancel')}><AgentIcon name="stop" /><span>Cancel task</span></button> : null}
        <Tooltip disabled={mobile} content={primary.kind === 'pause' ? 'Pause after the current step completes' : primary.label}><button type={primary.kind === 'send' ? 'submit' : 'button'} className="agent-send" aria-label={primary.label} aria-busy={primary.disabled && primary.working || undefined} disabled={primary.disabled} onClick={primary.kind === 'pause' || primary.kind === 'recover' ? () => onControl(primary.kind as 'pause' | 'recover' | 'cancel') : undefined}>
          {primary.working ? <LoadingIndicator label={primary.label} small decorative /> : <AgentIcon name={primary.kind === 'recover' ? 'play' : primary.kind === 'pause' ? 'pause' : 'send'} />}
        </button></Tooltip></div>
      </div>
    </form>
    {mobile && active && actions && hasActions && <AgentSheet anchor={actionsButton} label="Run actions" onClose={() => setActions(false)}>
      {sendNow && <button type="button" className="agent-header-menu-item" disabled={busy || !canSendNow}
        onClick={() => { setActions(false); onSendNow?.(); }}><AgentIcon name="message" />Send to current task</button>}
      {canCancel && <button type="button" className="agent-header-menu-item agent-run-cancel"
        onClick={() => { setActions(false); onControl('cancel'); }}><AgentIcon name="stop" />Cancel task</button>}
    </AgentSheet>}
    {caption ? <div className="agent-composer-caption" role="status" aria-live="polite"><span>{caption}</span></div> : null}
  </>;
}
