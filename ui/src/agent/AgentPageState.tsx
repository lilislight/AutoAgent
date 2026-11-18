import { useId, useRef } from 'react';
import { BusyLabel, ConversationSkeleton } from '../components/LoadingState.js';
import { AgentComposer } from './AgentComposer.js';
import { AgentIcon } from './AgentIcon.js';

interface Props {
  title: string; onBack?: () => void;
  loading?: boolean; error?: Error | null; retrying?: boolean; onRetry?: () => void;
}
const noop = () => {};

/** Keep navigation and the two-line composer in their final positions during reads. */
export function AgentPageState({ title, onBack, loading, error, retrying, onRetry }: Props) {
  const inputRef = useRef<HTMLTextAreaElement>(null), id = useId();
  return <main className="agent-chat" aria-label="Agent conversation">
    <header className="agent-conversation-header">{onBack && <button type="button" id="agent-mobile-back" className="agent-icon-button" aria-label="Conversations" onClick={onBack}><AgentIcon name="back" /></button>}<h1><span className="agent-conversation-title">{title}</span></h1></header>
    <div className="agent-feed-shell"><div className="agent-chat-scroll">
    {loading ? <div className="agent-feed"><ConversationSkeleton /></div> : <div className="agent-empty">
      <h2>{error ? 'Could not load chat' : 'Chat is disabled'}</h2>
      <p role={error ? 'alert' : undefined}>{error?.message ?? 'Enable the SDK Agent for this project to start conversations.'}</p>
      {error && onRetry ? <button disabled={retrying} onClick={onRetry}><BusyLabel busy={retrying} label="Retrying">Retry</BusyLabel></button> : null}
    </div>}
    <div className="agent-composer-dock"><div className="agent-composer-area"><AgentComposer mobile={!!onBack} session={id} draft="" busy={false} running={false} disabled canSend={false} controlsDisabled
      primary={{ kind: 'send', label: 'Send message', disabled: true, working: false }} onControl={noop} inputRef={inputRef}
      onDraft={noop} onSend={noop} images={[]} onPickImages={noop} onRemoveImage={noop}
      contextLoading={loading} caption={loading ? 'Loading chat…' : 'Chat unavailable'} /></div></div>
    </div></div>
  </main>;
}
