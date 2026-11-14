import { useId, useRef } from 'react';
import { BusyLabel, ConversationSkeleton } from '../components/LoadingState.js';
import { AgentComposer } from './AgentComposer.js';

interface Props {
  title: string;
  loading?: boolean; error?: Error | null; retrying?: boolean; onRetry?: () => void;
}
const noop = () => {};

/** Keep navigation and the two-line composer in their final positions during reads. */
export function AgentPageState({ loading, error, retrying, onRetry }: Props) {
  const inputRef = useRef<HTMLTextAreaElement>(null), id = useId();
  return <main className="agent-chat" aria-label="Agent conversation">
    <div className="agent-feed-shell"><div className="agent-chat-scroll">
    {loading ? <div className="agent-feed"><ConversationSkeleton /></div> : <div className="agent-empty">
      <h2>{error ? 'Could not load chat' : 'Chat is disabled'}</h2>
      <p role={error ? 'alert' : undefined}>{error?.message ?? 'Enable the SDK Agent for this project to start conversations.'}</p>
      {error && onRetry ? <button disabled={retrying} onClick={onRetry}><BusyLabel busy={retrying} label="Retrying">Retry</BusyLabel></button> : null}
    </div>}
    <div className="agent-composer-dock"><div className="agent-composer-area"><AgentComposer session={id} draft="" busy={false} running={false} disabled canSend={false} controlsDisabled
      primary={{ kind: 'send', label: 'Send message', disabled: true, working: false }} onControl={noop} inputRef={inputRef}
      onDraft={noop} onSend={noop} contextLoading={loading} caption={loading ? 'Loading chat…' : 'Chat unavailable'} /></div></div>
    </div></div>
  </main>;
}
