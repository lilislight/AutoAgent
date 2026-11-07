import { BusyLabel, LoadingIndicator } from '../components/LoadingState';
import { newId } from './ids';
import { useCallback, useEffect, useRef, useState } from 'react';
import { useInfiniteQuery, useQuery, useQueryClient } from '@tanstack/react-query';
import { agentClient, performAction } from './client';
import { Conversation } from './Conversation';
import { Details, type DetailsTab } from './Details';
import { Interactions } from './Interactions';
import { useConversation } from './useConversation';
import { effectiveSettings } from './settingsState';
import { executionStatus } from './executionState';
import type { AgentCatalog, AgentInvocation, WorkspaceAgent } from './types';

const terminal = new Set(['completed', 'failed', 'cancelled']);
export default function AgentSession({ session, catalog, agent, active, invocation, onAccepted, onAgent, onTrace, onRefreshDirectory }: { session: string; catalog: AgentCatalog; agent?: WorkspaceAgent; active: boolean; invocation: string | null; onAccepted: (iid: string) => void; onAgent: (agent: AgentInvocation) => void; onTrace: (iid: string, sid: string) => void; onRefreshDirectory: () => void }) {
  const client = useQueryClient();
  const turns = useInfiniteQuery({ queryKey: ['agent-invocations', session], queryFn: ({ pageParam, signal }) => agentClient.invocations(session, pageParam, signal), initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor, enabled: active && !!invocation, retry: false, refetchInterval: active && invocation ? 4000 : false });
  const invocations = turns.data?.pages.flatMap(page => page.items) ?? [];
  const iid = invocation ?? invocations[0]?.invocation_id;
  const latestId = invocations[0]?.invocation_id;
  const requestedKnown = invocations.some(item => item.invocation_id === invocation);
  useEffect(() => { if (active && invocation && requestedKnown && latestId && latestId !== invocation) onAccepted(latestId); }, [active, invocation, requestedKnown, latestId, onAccepted]);
  const execution = useQuery({ queryKey: ['agent-execution', iid], queryFn: ({ signal }) => agentClient.execution(iid!, signal), enabled: active && !!iid, refetchInterval: query => active && iid && (!query.state.data || !terminal.has(query.state.data.current.status)) ? 2000 : false, retry: false });
  const settings = useQuery({ queryKey: ['agent-settings', session], queryFn: ({ signal }) => agentClient.settings(session, signal), enabled: active });
  const revision = execution.data?.current.workflow_revision_id ?? agent?.revision_id;
  const resources = useQuery({ queryKey: ['agent-resources', revision], queryFn: ({ signal }) => agentClient.resources(revision!, signal), enabled: active && !!revision, refetchInterval: active ? 15000 : false, retry: false });
  const historyInvocations = iid && !invocations.some(item => item.invocation_id === iid) ? [...invocations, { invocation_id: iid } as AgentInvocation] : invocations;
  const conversation = useConversation(session, historyInvocations, active);
  const [historyPresented, setHistoryPresented] = useState(() => !invocation);
  const loadingHistory = !historyPresented && (turns.isLoading || conversation.loading);
  useEffect(() => { if (active && !loadingHistory) setHistoryPresented(true); }, [active, loadingHistory]);
  const observedInvocation = Object.values(conversation.state.rows).at(-1)?.invocation;
  useEffect(() => { if (active && !iid && observedInvocation) onAccepted(observedInvocation); }, [active, iid, observedInvocation, onAccepted]);
  const [tab, setTab] = useState<DetailsTab>('Agents'), [details, setDetails] = useState(() => window.innerWidth >= 1000);
  const [draft, setDraft] = useState(''), [busy, setBusy] = useState(false), [error, setError] = useState('');
  const [queued, setQueued] = useState<{ id: string; text: string; invocation: string }[]>([]);
  const operation = useRef<AbortController | null>(null);
  useEffect(() => () => operation.current?.abort(), []);
  const refresh = useCallback(() => {
    void client.invalidateQueries({ queryKey: ['agent-execution'] });
    void client.invalidateQueries({ queryKey: ['agent-invocations', session] });
    void client.invalidateQueries({ queryKey: ['agent-settings', session] });
    void client.invalidateQueries({ queryKey: ['agent-usage', session] });
    onRefreshDirectory();
  }, [client, session, onRefreshDirectory]);
  useEffect(() => {
    const ids = new Set(Object.values(conversation.state.rows).flatMap(row => row.type === 'message' ? [row.message.message_id] : []));
    setQueued(current => current.some(item => ids.has(item.id)) ? current.filter(item => !ids.has(item.id)) : current);
  }, [conversation.state.rows, queued]);
  useEffect(() => { if (active) document.title = 'AutoAgent'; }, [active]);
  const current = execution.data?.current, controls = execution.data?.controls;
  const running = current?.status === 'running' || current?.status === 'waiting';
  const child = !!current?.parent_invocation_id;
  const canStart = !iid || !!current && terminal.has(current.status) && !child;
  const snapshot = execution.data?.snapshot;
  const saved = effectiveSettings(settings.data, snapshot, child);
  const changed = !!saved && !!snapshot && JSON.stringify(saved.settings) !== JSON.stringify(snapshot.settings);
  const control = async (action: 'pause' | 'recover' | 'cancel') => {
    if (!controls || busy) return;
    operation.current?.abort(); operation.current = new AbortController(); setBusy(true); setError('');
    try {
      await performAction({ invocation_id: controls.root_invocation_id, action, ...(action === 'recover' ? { expected_version: controls.expected_version } : {}) }, operation.current.signal);
      refresh(); conversation.refresh();
    } catch (reason) { if (!operation.current?.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  };
  const send = async () => {
    const message = draft.trim();
    if (!message || busy) return;
    operation.current?.abort(); operation.current = new AbortController(); setBusy(true); setError('');
    try {
      if (running && iid) {
        const endpoint = resources.data?.user_signal_endpoint;
        if (!endpoint) throw new Error('This Agent’s message endpoint is unavailable.');
        const source_id = `ui_${newId().replaceAll('-', '')}`;
        await agentClient.spec(iid, 'signal', { endpoint, source_id }, operation.current.signal);
        const result = await performAction({ invocation_id: iid, action: 'signal', endpoint, source_id, sequence: 1, payload: { message, reply_requested: false, reply_to: null, wait_timeout_seconds: null } }, operation.current.signal);
        if (typeof result.result?.message_id === 'string') setQueued(items => [...items, { id: result.result!.message_id as string, text: message, invocation: iid }]);
      } else if (canStart && agent) {
        const result = await agentClient.run(session, message, operation.current.signal);
        onAccepted(result.invocation_id);
      } else throw new Error('Resume the current conversation before sending another message.');
      setDraft(''); refresh();
    } catch (reason) { if (!operation.current?.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  };
  const [allPending, setAllPending] = useState(false);
  const moreRequests = useInfiniteQuery({ queryKey: ['agent-more-requests', controls?.root_invocation_id, controls?.expected_version, controls?.next_cursor],
    queryFn: ({ pageParam, signal }) => agentClient.controls(controls!.root_invocation_id, pageParam!, signal),
    initialPageParam: controls?.next_cursor ?? null, getNextPageParam: page => page.next_cursor,
    enabled: active && allPending && !!controls?.next_cursor, retry: false });
  const waiting = [...new Map([...(controls?.pending_responses ?? []), ...(controls?.next_cursor ? moreRequests.data?.pages.flatMap(page => page.pending_responses) ?? [] : [])].map(wait => [wait.wait_id, wait])).values()];
  const status = executionStatus(current?.status ?? (iid ? 'loading' : 'ready'), controls);
  return <div className="agent-session-workspace"><main className="agent-chat" aria-label="Agent conversation"><header className="agent-chat-header"><div><div className="agent-eyebrow">AGENT WORKSPACE</div><h1>AutoAgent</h1>{['running', 'waiting', 'paused', 'interrupted', 'failed', 'cancelled'].includes(status) ? <div className="agent-header-status" role="status" aria-live="polite"><span className={`agent-status-dot is-${status}`} />{{ running: 'Working', waiting: 'Waiting for input', paused: 'Paused', interrupted: 'Interrupted', failed: 'Needs attention', cancelled: 'Stopped' }[status]}{child ? <span className="agent-pill">Subagent</span> : null}</div> : null}</div>
      <div className="agent-header-actions">{controls ? <>{(['pause', 'recover', 'cancel'] as const).filter(action => controls.actions[action]?.available).map(action => <button key={action} disabled={busy} onClick={() => void control(action)} title={child ? 'Controls the main Agent and its execution family' : undefined}>{action === 'recover' ? 'Resume / recover' : action === 'pause' ? 'Pause' : 'Stop'}{child ? ' family' : ''}</button>)}</> : null}{iid ? <button onClick={() => onTrace(iid, session)} title="Open execution trace">Trace ↗</button> : null}<button aria-expanded={details} aria-label="Toggle workspace details" onClick={() => setDetails(value => !value)}>☷</button></div></header>
      {changed ? <div className="agent-setting-notice">New settings saved. The active execution keeps its current settings until the next start or explicit resume.</div> : null}
      {turns.hasNextPage ? <button className="agent-load-older" disabled={turns.isFetchingNextPage} onClick={() => void turns.fetchNextPage()}><BusyLabel busy={turns.isFetchingNextPage} label="Loading older turns">Load older turns</BusyLabel></button> : null}
      {execution.error || turns.error ? <div className="agent-inline-error" role="alert">{execution.error?.message ?? turns.error?.message}<button onClick={refresh}>Refresh</button></div> : null}
      <Conversation state={conversation.state} loading={loadingHistory} error={conversation.error} onRetry={conversation.refresh} />
      {controls && waiting.length ? <Interactions rootId={controls.root_invocation_id} waits={waiting} active={active} onResolved={refresh} /> : null}
      {controls?.next_cursor && (!allPending || moreRequests.hasNextPage || moreRequests.isPending) ? <button className="agent-load-older" disabled={allPending && moreRequests.isFetching} onClick={() => { if (!allPending) setAllPending(true); else void moreRequests.fetchNextPage(); }}><BusyLabel busy={allPending && moreRequests.isFetching} label="Loading requests">Load more pending requests</BusyLabel></button> : null}
      {allPending && moreRequests.error ? <p className="agent-inline-error" role="alert">{moreRequests.error.message}<button onClick={() => void moreRequests.refetch()}>Retry</button></p> : null}
      <div className="agent-composer-area">{queued.map(item => <div className="agent-queued" key={item.id}><span>✓ {item.invocation !== iid || current && terminal.has(current.status) ? 'Accepted · execution ended before it was read' : 'Accepted · waiting to be read'}</span><p>{item.text}</p></div>)}{error ? <p className="agent-error-text" role="alert">{error}</p> : null}
        <form className="agent-composer" onSubmit={event => { event.preventDefault(); void send(); }}><label className="sr-only" htmlFor={`agent-message-${session}`}>Message Agent</label><textarea id={`agent-message-${session}`} rows={3} value={draft} onChange={event => setDraft(event.target.value)} placeholder={running ? 'Send an update to this Agent…' : child ? 'This subagent has finished.' : 'Ask, explore, or delegate a task…'} onKeyDown={event => { if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); void send(); } }} disabled={!!current && !running && !canStart} /><div className="agent-composer-toolbar"><button type="button" className="agent-model-picker" onClick={() => { setDetails(true); setTab('Settings'); }}>{saved?.settings.model.model ?? 'Provider default'} <span>⌄</span></button><button type="button" className="agent-approval-picker" onClick={() => { setDetails(true); setTab('Settings'); }}>{saved?.settings.approval_mode ?? 'policy'}</button><button type="submit" className="agent-send" aria-label={running ? 'Send update' : 'Send message'} disabled={busy || !draft.trim() || !(running || canStart && agent)}>{busy ? <LoadingIndicator label="Sending message" small /> : '↑'}</button></div></form>
        <div className="agent-composer-caption"><span>{status === 'paused' || status === 'interrupted' ? 'Updates will be read after you resume.' : running ? 'Updates enter context at the next preparation step.' : child ? 'Subagents are managed by their parent.' : 'Enter to send · Shift + Enter for a new line'}</span></div>
      </div>
    </main>{details ? <Details tab={tab} setTab={setTab} onClose={() => setDetails(false)} execution={execution.data} agent={agent} settings={saved} resources={resources.data} catalog={catalog} session={session} active={active} state={conversation.state} onAgent={onAgent} onSaved={refresh} executionError={execution.error} contextLoading={loadingHistory} executionLoading={execution.isLoading} resourcesLoading={resources.isLoading} settingsLoading={settings.isLoading} settingsError={settings.error} resourcesError={resources.error} onRetry={() => { void execution.refetch(); void settings.refetch(); void resources.refetch(); }} onTrace={() => iid && onTrace(iid, session)} /> : null}</div>;
}
