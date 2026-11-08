import { BusyLabel, ConversationSkeleton } from '../components/LoadingState';
import { AgentIcon } from './AgentIcon';

interface Props {
  title: string; sidebar: boolean; onSidebar: () => void;
  loading?: boolean; error?: Error | null; retrying?: boolean; onRetry?: () => void;
}

/** Keep the page frame and known navigation in place before the session is ready. */
export function AgentPageState({ title, sidebar, onSidebar, loading, error, retrying, onRetry }: Props) {
  return <main className="agent-chat" aria-label="Agent conversation">
    <header className="agent-chat-header">
      <button className="agent-icon-button" onClick={onSidebar} aria-label="Toggle conversations" aria-controls="agent-conversations" aria-expanded={sidebar}><AgentIcon name="panel" /></button>
      <div className="agent-heading"><h1 title={title}>{title}</h1></div>
    </header>
    {loading ? <div className="agent-feed-shell"><div className="agent-feed"><ConversationSkeleton /></div></div> : <div className="agent-empty">
      <h2>{error ? 'Could not load chat' : 'Chat is disabled'}</h2>
      <p role={error ? 'alert' : undefined}>{error?.message ?? 'Enable the SDK Agent for this project to start conversations.'}</p>
      {error && onRetry ? <button disabled={retrying} onClick={onRetry}><BusyLabel busy={retrying} label="Retrying">Retry</BusyLabel></button> : null}
    </div>}
  </main>;
}
