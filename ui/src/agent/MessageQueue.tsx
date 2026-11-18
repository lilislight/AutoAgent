import { useState } from 'react';
import { LoadingIndicator } from '../components/LoadingState.js';
import { Tooltip } from '../components/Tooltip.js';
import { AgentIcon } from './AgentIcon.js';
import { MessageContent } from './MessageContent.js';
import type { QueuedMessage } from './types.js';

export function MessageQueue({ items, pending, canSendNow, onAction }: {
  items: QueuedMessage[]; pending: string | null; canSendNow: boolean;
  onAction: (item: QueuedMessage, action: 'cancel' | 'send_now' | 'continue') => void;
}) {
  const [expanded, setExpanded] = useState<string | null>(null);
  if (!items.length) return null;
  return <section className="agent-message-queue" aria-label="Queued messages">
    <div className="agent-queue-heading"><span>Queued</span><span>{items.length}</span></div>
    <ol>{items.map((item, index) => {
      const busy = pending === item.id, editable = item.state === 'queued' || item.state === 'blocked';
      const count = item.images?.length ?? 0;
      return <li className={`agent-queue-item ${expanded === item.id ? 'is-expanded' : ''}`} key={item.id} aria-busy={busy}>
        <div className="agent-queue-row"><span className="agent-queue-position">{index + 1}</span>
          <button className="agent-queue-preview" aria-expanded={expanded === item.id} onClick={() => setExpanded(value => value === item.id ? null : item.id)}>{item.message || `${count} ${count === 1 ? 'image' : 'images'}`}</button>
          {busy || !editable ? <LoadingIndicator label={busy ? 'Updating queued message' : item.state === 'sending' ? 'Sending message' : 'Starting task'} small /> : <>
            {item.state === 'blocked' ? <button className="agent-queue-continue" disabled={pending !== null} onClick={() => onAction(item, 'continue')}>Continue queue</button> : null}
            <Tooltip content="Send to the current task now"><button className="agent-icon-button" aria-label={`Send queued message ${index + 1} now`} disabled={pending !== null || !canSendNow} onClick={() => onAction(item, 'send_now')}><AgentIcon name="message" /></button></Tooltip>
            <Tooltip content="Remove from queue"><button className="agent-icon-button" aria-label={`Cancel queued message ${index + 1}`} disabled={pending !== null} onClick={() => onAction(item, 'cancel')}><AgentIcon name="close" /></button></Tooltip>
          </>}
        </div>{expanded === item.id && !!item.images?.length && <div className="agent-queue-images"><MessageContent content={item.images} /></div>}
        {item.error ? <p className="agent-queue-error" role="status">{item.error}</p> : null}
      </li>;
    })}</ol>
  </section>;
}
