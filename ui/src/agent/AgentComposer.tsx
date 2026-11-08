import type { Ref } from 'react';
import { LoadingIndicator } from '../components/LoadingState';
import { AgentIcon } from './AgentIcon';
import type { DetailsTab } from './Details';
import type { SessionSettings } from './types';

const approvalLabels = { policy: 'Ask when needed', always_ask: 'Always ask', never_ask: 'Never ask', bypass: 'Bypass approval' };

interface Props {
  session: string; draft: string; busy: boolean; running: boolean; disabled: boolean; canSend: boolean;
  model?: string | null; approval?: SessionSettings['approval_mode']; caption: string;
  inputRef: Ref<HTMLTextAreaElement>;
  onDraft: (value: string) => void; onSend: () => void; onPanel: (panel: DetailsTab) => void;
}

export function AgentComposer({ session, draft, busy, running, disabled, canSend, model, approval, caption, inputRef, onDraft, onSend, onPanel }: Props) {
  return <>
    <form className="agent-composer" onSubmit={event => { event.preventDefault(); onSend(); }}>
      <label className="sr-only" htmlFor={`agent-message-${session}`}>Message Agent</label>
      <textarea ref={inputRef} id={`agent-message-${session}`} rows={2} value={draft} onChange={event => onDraft(event.target.value)} placeholder={running ? 'Send an update to this Agent…' : 'Ask, explore, or make something…'} onKeyDown={event => {
        if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); onSend(); }
      }} disabled={disabled} />
      <div className="agent-composer-toolbar">
        <button type="button" className="agent-icon-button" title="Available tools and skills" aria-label="Available tools and skills" onClick={() => onPanel('Tools')}><AgentIcon name="tools" /></button>
        <button type="button" className="agent-model-picker" title={model ?? 'Provider default'} onClick={() => onPanel('Settings')}>{model ?? 'Provider default'} <span>⌄</span></button>
        <button type="button" className="agent-approval-picker" onClick={() => onPanel('Settings')}><AgentIcon name="shield" />{approvalLabels[approval ?? 'policy']}</button>
        <button type="button" className="agent-icon-button" title="Access permissions" aria-label="Access permissions" onClick={() => onPanel('Settings')}><AgentIcon name="access" /></button>
        <button type="submit" className="agent-send" aria-label={running ? 'Send update' : 'Send message'} disabled={busy || !draft.trim() || !canSend}>{busy ? <LoadingIndicator label="Sending message" small /> : <AgentIcon name="send" />}</button>
      </div>
    </form>
    <div className="agent-composer-caption"><span>{caption}</span></div>
  </>;
}
