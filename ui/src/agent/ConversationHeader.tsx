import { useCallback, useEffect, useRef, useState } from 'react';
import { useInfiniteQuery, useMutation, useQuery } from '@tanstack/react-query';
import { BusyLabel, FactsSkeleton, SkeletonText } from '../components/LoadingState';
import { Tooltip } from '../components/Tooltip';
import { AgentIcon } from './AgentIcon';
import { AgentPopover } from './AgentPopover';
import { agentClient } from './client';
import { UsageFacts } from './UsageButton';
import { ContextFacts } from './ContextIndicator';
import type { ContextUsage } from './contextUsage';
import type { ConversationLandmark } from './conversationLandmarks';

const usageLabels = ['Model calls', 'Input tokens', 'Cached input tokens', 'Output tokens', 'Total tokens', 'Cost (USD)'];

function SessionUsage({ session, running }: { session: string; running: boolean }) {
  const usage = useQuery({ queryKey: ['agent-usage', session, null],
    queryFn: ({ signal }) => agentClient.usage(session, null, signal), retry: false, refetchInterval: running ? 4000 : false });
  return <>
    {usage.isPending && <FactsSkeleton label="Loading session usage" labels={usageLabels} className="agent-usage-facts" />}
    {usage.error && <p className="agent-usage-error" role="alert">Could not load session usage.<button type="button" onClick={() => void usage.refetch()}>Retry</button></p>}
    {usage.data && <UsageFacts usage={usage.data.session} showCachedInput />}
  </>;
}

const activeStatus = new Set(['created', 'running', 'waiting', 'settling']);

function Subagents({ session, running, onSelect }: { session: string; running: boolean; onSelect: (session: string, invocation: string) => void }) {
  const agents = useInfiniteQuery({ queryKey: ['agent-subagents', session],
    queryFn: ({ pageParam, signal }) => agentClient.subagents(session, pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor, retry: false,
    refetchInterval: query => running || query.state.data?.pages.some(page => page.items.some(item => activeStatus.has(item.status))) ? 4000 : false });
  const rows = agents.data?.pages.flatMap(page => page.items) ?? [];
  return <>
    {agents.isPending && <div aria-label="Loading subagents" role="status">
      {[0, 1].map(index => <div className="agent-subagent-row" key={index} aria-hidden="true"><AgentIcon name="agent" /><span className="agent-subagent-task"><SkeletonText width="82%" /></span><SkeletonText width="60px" /></div>)}
    </div>}
    {agents.error && <p className="agent-usage-error" role="alert">Could not load subagents.<button type="button" onClick={() => void agents.refetch()}>Retry</button></p>}
    {agents.isSuccess && !rows.length && <p className="agent-usage-note">No subagents used in this conversation.</p>}
    {rows.length > 0 && <ul className="agent-subagent-list">{rows.map(row => <li key={row.invocation_id}>
      <button type="button" className="agent-subagent-row" onClick={() => onSelect(row.session_id, row.invocation_id)}>
        <AgentIcon name="agent" /><span className="agent-subagent-task">{row.task || 'Subagent'}</span><small>{row.status}</small>
      </button>
    </li>)}</ul>}
    {agents.hasNextPage && <button type="button" className="agent-header-menu-item" disabled={agents.isFetchingNextPage} onClick={() => void agents.fetchNextPage()}>
      <BusyLabel busy={agents.isFetchingNextPage} label="Loading subagents">Load more</BusyLabel>
    </button>}
  </>;
}

function ConversationActions({ title, rename, onDone, onOpenTurns, mobile }: {
  title: string; rename?: (value: string) => Promise<void>; onDone: () => void; onOpenTurns?: () => void; mobile: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(title);
  const [feedback, setFeedback] = useState('');
  const [manualLink, setManualLink] = useState('');
  const editor = useCallback((input: HTMLInputElement | null) => { input?.focus(); input?.select(); }, []);
  const mutation = useMutation({ mutationFn: (name: string) => rename!(name), onSuccess: onDone });
  if (editing) return <form className="agent-rename-form" onSubmit={event => {
    event.preventDefault();
    if (draft.trim() && !mutation.isPending) mutation.mutate(draft.trim());
  }}>
    <label>Conversation name<input ref={editor} value={draft} maxLength={120} required disabled={mutation.isPending}
      onChange={event => setDraft(event.target.value)} /></label>
    {mutation.error && <p className="agent-usage-error" role="alert">{mutation.error.message}</p>}
    <button type="submit" disabled={!draft.trim() || mutation.isPending}><BusyLabel busy={mutation.isPending} label="Saving">Save</BusyLabel></button>
  </form>;
  return <>
    <button className="agent-header-menu-item" type="button" disabled={!rename} onClick={() => { setDraft(title); setEditing(true); }}><AgentIcon name="edit" />Rename</button>
    <button className="agent-header-menu-item" type="button" onClick={async () => {
      try { await navigator.clipboard.writeText(window.location.href); setFeedback('Link copied'); }
      catch {
        if (mobile) setManualLink(window.location.href);
        else setFeedback('Copy the conversation URL from the address bar.');
      }
    }}><AgentIcon name="link" />Copy link</button>
    {manualLink && <label className="agent-manual-link">Conversation link<input value={manualLink} readOnly onFocus={event => event.currentTarget.select()} /></label>}
    {onOpenTurns && <button className="agent-header-menu-item" type="button" onClick={onOpenTurns}><AgentIcon name="task" />Jump to turn</button>}
    {feedback && <p className="agent-usage-note" role="status">{feedback}</p>}
  </>;
}

interface Props {
  session: string; title: string; existing: boolean; active: boolean; running: boolean; hasWorkspace: boolean;
  mobile: boolean; onBack?: () => void; hasParent: boolean;
  context: ContextUsage; contextLoading: boolean; landmarks: ConversationLandmark[];
  onJump: (item: ConversationLandmark) => void;
  onRename?: (value: string) => Promise<void>;
  onOpenSubagent: (session: string, invocation: string) => void;
}

export function ConversationHeader({ session, title, existing, active, running, hasWorkspace, onRename, onOpenSubagent, mobile, onBack, hasParent, context, contextLoading, landmarks, onJump }: Props) {
  const [panel, setPanel] = useState<'actions' | 'usage' | 'subagents' | 'turns' | null>(null);
  const titleButton = useRef<HTMLButtonElement>(null), usageButton = useRef<HTMLButtonElement>(null), agentsButton = useRef<HTMLButtonElement>(null);
  const close = useCallback(() => setPanel(null), []);
  useEffect(() => { if (!active) close(); }, [active, close]);
  return <header className="agent-conversation-header">
    {mobile && onBack && <button id="agent-mobile-back" type="button" className="agent-icon-button"
      aria-label={hasParent ? 'Back to parent conversation' : 'Conversations'} onClick={onBack}><AgentIcon name="back" /></button>}
    <h1>{existing ?
      <button ref={titleButton} type="button" className="agent-conversation-title" title={title}
        aria-haspopup="dialog" aria-expanded={(panel === 'actions' || panel === 'turns') && active} onClick={() => setPanel(current => current === 'actions' ? null : 'actions')}>
        {!mobile && hasWorkspace && <AgentIcon name="folder" />}<span>{title}</span><AgentIcon name="chevron" />
      </button> : <span className="agent-conversation-title">{title}</span>}
    </h1>
    {existing && <div className="agent-conversation-actions">
      <Tooltip content="View session usage" disabled={mobile || panel === 'usage'}><button ref={usageButton} type="button" className="agent-icon-button"
        aria-label="View session usage" aria-haspopup="dialog" aria-expanded={panel === 'usage' && active}
        onClick={() => setPanel(current => current === 'usage' ? null : 'usage')}><AgentIcon name="usage" /></button></Tooltip>
      <Tooltip content="View subagents" disabled={mobile || panel === 'subagents'}><button ref={agentsButton} type="button" className="agent-icon-button"
        aria-label="View subagents" aria-haspopup="dialog" aria-expanded={panel === 'subagents' && active}
        onClick={() => setPanel(current => current === 'subagents' ? null : 'subagents')}><AgentIcon name="agent" /></button></Tooltip>
    </div>}
    {active && (panel === 'actions' || panel === 'turns') && <AgentPopover sheet={mobile} anchor={titleButton} label={panel === 'turns' ? 'Jump to turn' : 'Conversation'} className="agent-conversation-popover" onClose={close}>
      {panel === 'turns' ? <ol className="agent-mobile-turns">{landmarks.map(item => <li key={item.id}>
        <button type="button" onClick={() => { close(); requestAnimationFrame(() => onJump(item)); }}>
          <strong>{item.question}</strong><span>{item.answer}</span>
        </button>
      </li>)}</ol> : <ConversationActions mobile={mobile} title={title} rename={onRename}
        onOpenTurns={mobile && landmarks.length ? () => setPanel('turns') : undefined}
        onDone={() => { close(); if (!mobile) titleButton.current?.focus({ preventScroll: true }); }} />}
    </AgentPopover>}
    {active && panel === 'usage' && <AgentPopover sheet={mobile} anchor={usageButton} label="Session usage" className="agent-conversation-popover" onClose={close}>
      <SessionUsage session={session} running={running} />
      {mobile && <section className="agent-sheet-context" aria-label="Context window"><h3>Context window</h3><ContextFacts value={context} loading={contextLoading} /></section>}
    </AgentPopover>}
    {active && panel === 'subagents' && <AgentPopover sheet={mobile} anchor={agentsButton} label="Subagents" className="agent-conversation-popover" onClose={close}>
      <Subagents session={session} running={running} onSelect={(sid, iid) => { close(); onOpenSubagent(sid, iid); }} />
    </AgentPopover>}
  </header>;
}
