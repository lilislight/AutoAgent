import { LoadingIndicator } from '../components/LoadingState.js';

export function SessionActivityBadge({ running, unread }: { running: boolean; unread: boolean }) {
  return <span className="agent-session-activity" role={running || unread ? 'img' : undefined} aria-label={running ? 'Task running' : unread ? 'New task result' : undefined} aria-hidden={!running && !unread || undefined}>
    {running ? <LoadingIndicator label="Task running" small decorative /> : unread ? <span className="agent-session-unread" /> : null}
  </span>;
}
