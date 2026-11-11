import { BusyLabel } from '../components/LoadingState';
import { newId } from './ids';
import { config } from '../http';
import { AgentComposer } from './AgentComposer';
import { readDraft, saveDraft } from './drafts';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useInfiniteQuery, useQuery, useQueryClient } from '@tanstack/react-query';
import { agentClient } from './client';
import { Conversation } from './Conversation';
import { Details, type DetailsTab } from './Details';
import { Interactions } from './Interactions';
import { MessageQueue } from './MessageQueue';
import { useConversation } from './useConversation';
import { effectiveSettings } from './settingsState';
import { executionStatus } from './executionState';
import { composerAction } from './composerActions';
import { retryAgentRead, agentReadRetryDelay } from './readFeedback';
import type { AgentCatalog, AgentInvocation, AgentWorkspaceInfo, MessageQueue as QueueState, QueuedMessage, SettingsRecord, SessionSettings, WorkspaceAgent } from './types';
import { WorkspacePicker } from './WorkspacePicker';

const terminal = new Set(['completed', 'failed', 'cancelled']);
interface Props {
  session: string; existing: boolean; title: string; catalog: AgentCatalog; agent: WorkspaceAgent;
  workspace: string | null;
  workspaces?: AgentWorkspaceInfo[]; workspacesLoading: boolean; workspaceError?: Error | null;
  onWorkspaceChange: (workspace: string | null) => void; onRetryWorkspaces: () => void;
  active: boolean; invocation: string | null; sidebar: boolean; onSidebar: () => void;
  panel: DetailsTab | null; onPanel: (panel: DetailsTab | null) => void;
  onAccepted: (iid: string | null) => void; onAgent: (agent: AgentInvocation) => void;
  onTrace: (iid: string, sid: string) => void; onRefreshDirectory: () => void;
}

export default function AgentSession({ session, existing, title, catalog, agent, workspace, workspaces, workspacesLoading, workspaceError, onWorkspaceChange, onRetryWorkspaces, active, invocation, sidebar, onSidebar, panel, onPanel, onAccepted, onAgent, onTrace, onRefreshDirectory }: Props) {
  const tab = panel ?? 'Agents';
  const details = panel !== null;
  const client = useQueryClient();
  const turns = useInfiniteQuery({ queryKey: ['agent-invocations', session], queryFn: ({ pageParam, signal }) => agentClient.invocations(session, pageParam, signal), initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor, enabled: active && existing, retry: retryAgentRead, retryDelay: agentReadRetryDelay, refetchInterval: active && existing ? 4000 : false });
  const invocations = useMemo(() => turns.data?.pages.flatMap(page => page.items) ?? [], [turns.data?.pages]);
  const iid = invocation ?? invocations[0]?.invocation_id;
  const latestId = invocations[0]?.invocation_id;
  const requestedKnown = invocations.some(item => item.invocation_id === invocation);
  useEffect(() => { if (active && invocation && requestedKnown && latestId && latestId !== invocation) onAccepted(latestId); }, [active, invocation, requestedKnown, latestId, onAccepted]);
  const execution = useQuery({ queryKey: ['agent-execution', iid], queryFn: ({ signal }) => agentClient.execution(iid!, signal), enabled: active && !!iid, refetchInterval: query => active && iid && (!query.state.data || !terminal.has(query.state.data.current.status)) ? 2000 : false, retry: retryAgentRead, retryDelay: agentReadRetryDelay });
  const settings = useQuery({ queryKey: ['agent-settings', session, workspace], queryFn: ({ signal }) => agentClient.settings(session, signal, workspace), enabled: active, staleTime: existing ? 0 : Infinity });
  const saveDraftSettings = (value: SessionSettings) => client.setQueryData<SettingsRecord>(['agent-settings', session, workspace], previous => previous ? { ...previous, settings: value } : previous);
  // This catalog describes live capabilities; execution keeps its historical Revision.
  const revision = agent.revision_id;
  const resources = useQuery({ queryKey: ['agent-resources', revision, session, workspace], queryFn: ({ signal }) => agentClient.resources(revision!, session, signal, workspace), enabled: active && !!revision, refetchInterval: active ? 15000 : false, retry: false });
  const historyInvocations = useMemo(() => iid && !invocations.some(item => item.invocation_id === iid) ? [...invocations, { invocation_id: iid } as AgentInvocation] : invocations, [iid, invocations]);
  const conversation = useConversation(session, historyInvocations, active);
  const [historyPresented, setHistoryPresented] = useState(() => !existing);
  const loadingHistory = !historyPresented && (turns.isLoading || conversation.loading);
  useEffect(() => { if (active && !loadingHistory) setHistoryPresented(true); }, [active, loadingHistory]);
  const observedInvocation = Object.values(conversation.state.rows).at(-1)?.invocation;
  useEffect(() => { if (active && !iid && observedInvocation) onAccepted(observedInvocation); }, [active, iid, observedInvocation, onAccepted]);
  const draftKey = existing ? 'session:' + session : 'new:' + (workspace ?? 'unbound');
  const [draft, setDraft] = useState(() => readDraft(config.apiPrefix, draftKey)), [busy, setBusy] = useState(false), [error, setError] = useState('');
  useEffect(() => saveDraft(config.apiPrefix, draftKey, draft), [draftKey, draft]);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const loadUsage = useCallback((invocation: string, signal: AbortSignal) => agentClient.usage(session, invocation, signal), [session]);
  const queue = useQuery({ queryKey: ['agent-queue', session], queryFn: ({ signal }) => agentClient.queue(session, signal), enabled: active, refetchInterval: active && existing ? 1500 : false, retry: retryAgentRead, retryDelay: agentReadRetryDelay });
  const [queuePending, setQueuePending] = useState<string | null>(null);
  const submission = useRef<{ text: string; id: string } | null>(null);
  const operation = useRef<AbortController | null>(null), queueRequest = useRef<AbortController | null>(null), controlRequest = useRef<AbortController | null>(null);
  const [controlSubmitting, setControlSubmitting] = useState<'pause' | 'recover' | null>(null);
  const [pendingControl, setPendingControl] = useState<{ id: string; action: 'pause' | 'recover' } | null>(null);
  const controlOperation = useQuery({ queryKey: ['agent-composer-control', pendingControl?.id], queryFn: ({ signal }) => agentClient.operation(pendingControl!.id, signal), enabled: active && !!pendingControl, refetchInterval: query => active && pendingControl && query.state.data?.status !== 'succeeded' && query.state.data?.status !== 'failed' ? 500 : false, retry: false });
  useEffect(() => () => { operation.current?.abort(); queueRequest.current?.abort(); controlRequest.current?.abort(); }, []);
  const refresh = useCallback(() => {
    void client.invalidateQueries({ queryKey: ['agent-execution'] });
    void client.invalidateQueries({ queryKey: ['agent-invocations', session] });
    void client.invalidateQueries({ queryKey: ['agent-settings', session] });
    void client.invalidateQueries({ queryKey: ['agent-usage', session] });
    void client.invalidateQueries({ queryKey: ['agent-queue', session] });
    onRefreshDirectory();
  }, [client, session, onRefreshDirectory]);
  const recentUser = Object.values(conversation.state.rows).find(row => row.type === 'message' && row.message.source === 'user');
  const heading = title === 'Conversation' && recentUser?.type === 'message' ? recentUser.message.content.find(part => part.type === 'text')?.text?.slice(0, 100) || title : title;
  useEffect(() => { if (active) document.title = existing ? heading + ' · AutoAgent' : 'AutoAgent'; }, [active, existing, heading]);
  const current = execution.data?.current, controls = execution.data?.controls;
  const running = current?.status === 'running' || current?.status === 'waiting';
  const child = !!current?.parent_invocation_id;
  const canStart = !iid && (!existing || turns.isSuccess) || !!current && terminal.has(current.status) && !child;
  const snapshot = execution.data?.snapshot;
  const saved = effectiveSettings(settings.data, snapshot, child);
  const changed = !!saved && !!snapshot && (running || current?.status === 'paused') && (
    saved.settings.model.provider !== snapshot.settings.model.provider || saved.settings.model.model !== snapshot.settings.model.model);
  useEffect(() => {
    if (!pendingControl || !controlOperation.data || controlOperation.data.status === 'running') return;
    if (controlOperation.data.status !== 'succeeded') setError(controlOperation.data.error?.message ?? 'Could not complete the execution control.');
    setPendingControl(null); refresh(); conversation.refresh();
  }, [pendingControl, controlOperation.data, refresh, conversation.refresh]);
  const control = async (action: 'pause' | 'recover') => {
    if (!controls || controlSubmitting || pendingControl) return;
    const controller = new AbortController(); controlRequest.current = controller;
    setControlSubmitting(action); setError('');
    try {
      const fresh = await agentClient.controls(controls.root_invocation_id, null, controller.signal);
      if (!fresh.actions[action].available) throw new Error(fresh.actions[action].reason ?? 'This control is unavailable.');
      const receipt = await agentClient.action({ invocation_id: fresh.root_invocation_id, action, request_id: newId().replaceAll('-', ''), ...(action === 'recover' ? { expected_version: fresh.expected_version } : {}) }, controller.signal);
      if (receipt.status === 'running') setPendingControl({ id: receipt.operation_id, action });
      else if (receipt.status !== 'succeeded') throw new Error(receipt.error?.message ?? 'Could not complete the execution control.');
      refresh(); conversation.refresh();
    } catch (reason) { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setControlSubmitting(null); }
  };
  const updateQueue = (item: QueuedMessage) => {
    client.setQueryData<QueueState>(['agent-queue', session], previous => ({
      invocation_id: item.state === 'started' ? item.invocation_id : previous?.invocation_id ?? iid ?? null,
      items: [...(previous?.items ?? []).filter(value => value.id !== item.id), ...(['queued', 'blocked', 'starting', 'sending'].includes(item.state) ? [item] : [])].sort((a, b) => a.position - b.position),
    }));
  };
  const send = async (immediate = false) => {
    const message = draft.trim();
    if (!message || busy) return;
    operation.current?.abort(); operation.current = new AbortController(); setBusy(true); setError('');
    try {
      if (!submission.current || submission.current.text !== message) submission.current = { text: message, id: newId() };
      const item = await agentClient.enqueue(session, message, submission.current.id, workspace, operation.current.signal, immediate && iid ? iid : undefined, existing ? undefined : settings.data?.settings);
      updateQueue(item); submission.current = null;
      saveDraft(config.apiPrefix, draftKey, '');
      if (!existing || item.state === 'started') onAccepted(item.state === 'started' ? item.invocation_id : iid ?? null);
      setDraft(current => current.trim() === message ? '' : current); refresh();
    } catch (reason) { if (!operation.current?.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  };
  const queueAction = async (item: QueuedMessage, action: 'cancel' | 'send_now' | 'continue') => {
    if (queuePending) return;
    const controller = new AbortController(); queueRequest.current = controller;
    setQueuePending(item.id); setError('');
    try {
      updateQueue(await agentClient.queueAction(session, item.id, action, controller.signal, iid));
      refresh();
    } catch (reason) { if (!controller.signal.aborted) { setError(reason instanceof Error ? reason.message : String(reason)); void queue.refetch(); } }
    finally { setQueuePending(null); }
  };
  const [allPending, setAllPending] = useState(false);
  const moreRequests = useInfiniteQuery({ queryKey: ['agent-more-requests', controls?.root_invocation_id, controls?.expected_version, controls?.next_cursor],
    queryFn: ({ pageParam, signal }) => agentClient.controls(controls!.root_invocation_id, pageParam!, signal),
    initialPageParam: controls?.next_cursor ?? null, getNextPageParam: page => page.next_cursor,
    enabled: active && allPending && !!controls?.next_cursor, retry: false });
  const waiting = [...new Map([...(controls?.pending_responses ?? []), ...(controls?.next_cursor ? moreRequests.data?.pages.flatMap(page => page.pending_responses) ?? [] : [])].map(wait => [wait.wait_id, wait])).values()];
  const status = executionStatus(current?.status ?? (iid ? 'loading' : 'ready'), controls);
  const paused = status === 'paused';
  const canSignal = running && status !== 'interrupted' && !!resources.data?.user_signal_endpoint && !!controls?.actions.signal.available;
  const canSend = !child && !!agent && (canStart || !!current || !!queue.data);
  const pending = controlSubmitting ?? pendingControl?.action ?? (controls?.control_state === 'pausing' ? 'pause' : null);
  const primary = composerAction({ draft, canSend, running, lifecycle: status, canPause: !!controls?.actions.pause.available, canResume: !!controls?.actions.recover.available, sending: busy, pending });
  const caption = pending === 'pause' ? 'Pausing after the current step · New messages join the queue.' : pending === 'recover' ? 'Resuming task…' : paused ? 'Paused · Resume to continue the task and queue.' : status === 'interrupted' || status === 'failed' ? 'Task interrupted · Resume it or continue the queued messages.' : status === 'waiting' ? 'Waiting for input · Enter queues the next task.' : running ? 'Enter to queue the next task · Send now updates the current task.' : child ? 'Subagents are managed by their parent.' : 'Enter to send · Shift + Enter for a new line';
  const suggest = useCallback((text: string) => { setDraft(text); inputRef.current?.focus(); }, []);
  return <div className="agent-session-workspace"><main className="agent-chat" aria-label="Agent conversation">
    <Conversation session={session} loadUsage={loadUsage} invocations={historyInvocations} execution={execution.data} active={active} state={conversation.state} loading={loadingHistory} error={conversation.error} onRetry={conversation.refresh} onSuggest={suggest} running={status === 'running' || status === 'pausing'} before={<>
      {changed ? <div className="agent-setting-notice">The selected model applies to new submissions. This turn keeps its current model.</div> : null}
      {turns.hasNextPage ? <button className="agent-load-older" disabled={turns.isFetchingNextPage} onClick={() => void turns.fetchNextPage()}><BusyLabel busy={turns.isFetchingNextPage} label="Loading older turns">Load older turns</BusyLabel></button> : null}
      {execution.error || turns.error ? <div className="agent-inline-error" role="alert">{execution.error?.message ?? turns.error?.message}<button onClick={refresh}>Refresh</button></div> : null}
    </>} after={<>
      {controls ? <Interactions rootId={controls.root_invocation_id} waits={waiting} active={active} onResolved={refresh} /> : null}
      {controls?.next_cursor && (!allPending || moreRequests.hasNextPage || moreRequests.isPending) ? <button className="agent-load-older" disabled={allPending && moreRequests.isFetching} onClick={() => { if (!allPending) setAllPending(true); else void moreRequests.fetchNextPage(); }}><BusyLabel busy={allPending && moreRequests.isFetching} label="Loading requests">Load more pending requests</BusyLabel></button> : null}
      {allPending && moreRequests.error ? <p className="agent-inline-error" role="alert">{moreRequests.error.message}<button onClick={() => void moreRequests.refetch()}>Retry</button></p> : null}
    </>} footer={<div className="agent-composer-area"><MessageQueue items={queue.data?.items ?? []} pending={queuePending} canSendNow={canSignal && !child} onAction={(item, action) => void queueAction(item, action)} />
      {queue.error ? <p className="agent-error-text" role="alert">Could not refresh queued messages. <button onClick={() => void queue.refetch()}>Retry</button></p> : null}
      {error ? <p className="agent-error-text" role="alert">{error}</p> : null}{controlOperation.error && pendingControl ? <p className="agent-error-text" role="alert">Could not refresh execution status.<button type="button" onClick={() => void controlOperation.refetch()}>Retry</button></p> : null}
        <WorkspacePicker value={workspace} items={workspaces} loading={workspacesLoading} error={workspaceError} onRetry={onRetryWorkspaces} locked={existing} disabled={busy || !settings.data} onChange={onWorkspaceChange} />
        <AgentComposer primary={primary} onControl={action => void control(action)} sidebar={sidebar} onSidebar={onSidebar} session={session} draft={draft} onDraft={setDraft} inputRef={inputRef} busy={busy} running={running || paused} disabled={busy || child} canSend={canSend} canSendNow={canSignal && !child} onSendNow={running ? () => void send(true) : undefined} model={saved?.settings.model.model} approval={saved?.settings.approval_mode} onSend={() => void send()} onPanel={onPanel} caption={caption} />
      </div>} />
    </main>{details ? <Details tab={tab} setTab={onPanel} onClose={() => onPanel(null)} execution={execution.data} agent={agent} settings={saved} resources={resources.data} catalog={catalog} session={session} active={active} state={conversation.state} onAgent={onAgent} onSaved={refresh} onDraftSettings={existing ? undefined : saveDraftSettings} executionError={execution.error} contextLoading={loadingHistory} executionLoading={execution.isLoading} resourcesLoading={resources.isLoading} settingsLoading={settings.isLoading} settingsError={settings.error} resourcesError={resources.error} onRetry={() => { void execution.refetch(); void settings.refetch(); void resources.refetch(); }} onTrace={() => iid && onTrace(iid, session)} /> : null}</div>;
}
