import { BusyLabel } from '../components/LoadingState';
import { newId } from '../ids';
import { ApiError, config } from '../http';
import { AgentComposer } from './AgentComposer';
import { maxImages, validateImages } from './ComposerImages';
import { isHeifPhoto, prepareMobilePhoto } from './mobilePhoto';
import { readDraft, saveDraft } from './drafts';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { keepPreviousData, useInfiniteQuery, useQuery, useQueryClient } from '@tanstack/react-query';
import { agentClient } from './client';
import { Conversation } from './Conversation';
import { ConversationHeader } from './ConversationHeader';
import { Interactions } from './Interactions';
import { MessageQueue } from './MessageQueue';
import { useConversation } from './useConversation';
import { conversationSummary } from './conversationState';
import { effectiveSettings } from './settingsState';
import { executionStatus } from './executionState';
import { composerAction } from './composerActions';
import { retryAgentRead, agentReadRetryDelay } from './readFeedback';
import type { AgentCatalog, AgentInvocation, AgentWorkspaceInfo, MessageQueue as QueueState, QueuedMessage, SettingsRecord, SessionSettings, WorkspaceAgent } from './types';
import { WorkspacePicker } from './WorkspacePicker';
import { contextUsage } from './contextUsage';

const terminal = new Set(['completed', 'failed', 'cancelled']);
interface Props {
  session: string; existing: boolean; title: string; catalog: AgentCatalog; agent: WorkspaceAgent;
  workspace: string | null;
  workspaces?: AgentWorkspaceInfo[]; workspacesLoading: boolean; workspaceError?: Error | null;
  onWorkspaceChange: (workspace: string | null) => void; onRetryWorkspaces: () => void;
  active: boolean; mobile: boolean; onShowDirectory?: () => void;
  onReturnToParent: (sid: string, iid: string) => void; invocation: string | null;
  onAccepted: (iid: string | null) => void;
  onOpenSubagent: (sid: string, iid: string) => void;
  onRefreshDirectory: () => void;
  onForked: (record: SettingsRecord) => void;
}

export default function AgentSession({ session, existing, title, catalog, agent, workspace, workspaces, workspacesLoading, workspaceError, onWorkspaceChange, onRetryWorkspaces, active, mobile, onShowDirectory, onReturnToParent, invocation, onAccepted, onOpenSubagent, onRefreshDirectory, onForked }: Props) {
  const client = useQueryClient();
  const [historyAnchor, setHistoryAnchor] = useState<string | null>(null);
  const turns = useInfiniteQuery({
    queryKey: ['agent-invocations', session], queryFn: ({ pageParam, signal }) => agentClient.invocations(session, pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    enabled: active && existing, staleTime: Infinity, refetchOnWindowFocus: false,
    retry: retryAgentRead, retryDelay: agentReadRetryDelay,
    refetchInterval: query => active && existing && query.state.data?.pages[0]?.items.some(item => !terminal.has(item.status)) ? 4000 : false,
  });
  const archived = useInfiniteQuery({
    queryKey: ['agent-invocation-window', session, historyAnchor],
    queryFn: ({ pageParam, signal }) => agentClient.invocations(session, pageParam, signal, historyAnchor),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    enabled: active && existing && !!historyAnchor, placeholderData: keepPreviousData,
    staleTime: Infinity, retry: retryAgentRead, retryDelay: agentReadRetryDelay,
  });
  const turnIndex = useQuery({
    queryKey: ['agent-turn-index', session], queryFn: ({ signal }) => agentClient.turnIndex(session, signal),
    enabled: active && existing, staleTime: Infinity, refetchOnWindowFocus: false,
    retry: retryAgentRead, retryDelay: agentReadRetryDelay,
  });
  const invocations = useMemo(() => turns.data?.pages.flatMap(page => page.items) ?? [], [turns.data?.pages]);
  const owned = invocations.filter(item => !item.inherited);
  const iid = invocation ?? owned[0]?.invocation_id;
  const latestId = owned[0]?.invocation_id;
  const requestedKnown = owned.some(item => item.invocation_id === invocation);
  useEffect(() => { if (active && invocation && requestedKnown && latestId && latestId !== invocation) onAccepted(latestId); }, [active, invocation, requestedKnown, latestId, onAccepted]);
  const execution = useQuery({ queryKey: ['agent-execution', iid], queryFn: ({ signal }) => agentClient.execution(iid!, signal), enabled: active && !!iid, refetchInterval: query => active && iid && (!query.state.data || !terminal.has(query.state.data.current.status)) ? 2000 : false, retry: retryAgentRead, retryDelay: agentReadRetryDelay });
  const settings = useQuery({ queryKey: ['agent-settings', session, workspace], queryFn: ({ signal }) => agentClient.settings(session, signal, workspace), enabled: active, staleTime: existing ? 0 : Infinity });
  const saveDraftSettings = (value: SessionSettings) => client.setQueryData<SettingsRecord>(['agent-settings', session, workspace], previous => previous ? { ...previous, settings: value } : previous);
  // This catalog describes live capabilities; execution keeps its historical Revision.
  const revision = agent.revision_id;
  const resources = useQuery({ queryKey: ['agent-resources', revision, session, workspace], queryFn: ({ signal }) => agentClient.resources(revision!, session, signal, workspace), enabled: active && !!revision, refetchInterval: active ? 15000 : false, retry: false });
  const selectedExecution = execution.data?.current ?? invocations.find(item => item.invocation_id === iid);
  const liveHistory = useMemo(() => iid && (!existing || selectedExecution && !terminal.has(selectedExecution.status))
    ? [selectedExecution ?? { invocation_id: iid } as AgentInvocation] : [], [existing, iid, selectedExecution]);
  const conversation = useConversation(session, liveHistory, active);
  const showingArchive = !!historyAnchor && !!archived.data;
  const visible = showingArchive ? archived : turns;
  const visibleInvocations = useMemo(() => visible.data?.pages.flatMap(page => page.items) ?? [], [visible.data?.pages]);
  const savedTurns = useMemo(() => conversationSummary(visible.data?.pages), [visible.data?.pages]);
  const summary = useMemo(() => {
    if (showingArchive) return savedTurns;
    const live = conversation.state;
    return { ...live, rows: { ...savedTurns.rows, ...live.rows }, responses: { ...savedTurns.responses, ...live.responses },
      completed: { ...savedTurns.completed, ...live.completed } };
  }, [savedTurns, showingArchive, conversation.state]);
  const switchingWindow = !!historyAnchor && archived.isFetching && (archived.isPending || archived.isPlaceholderData);
  const loadingHistory = turns.isLoading;
  const observedInvocation = Object.values(conversation.state.rows).at(-1)?.invocation;
  useEffect(() => { if (active && !iid && observedInvocation) onAccepted(observedInvocation); }, [active, iid, observedInvocation, onAccepted]);
  const draftKey = existing ? 'session:' + session : 'new:' + (workspace ?? 'unbound');
  const [draft, setDraft] = useState(() => readDraft(config.apiPrefix, draftKey)), [busy, setBusy] = useState(false), [error, setError] = useState('');
  const [images, setImages] = useState<{ id: string; file: File }[]>([]);
  const [preparingImages, setPreparingImages] = useState(false);
  const pickImages = async (files: File[]) => {
    if (preparingImages || busy) return;
    if (images.length + files.length > maxImages) { setError(`Attach up to ${maxImages} images per message.`); return; }
    setError('');
    try {
      if (mobile && files.some(isHeifPhoto)) {
        setPreparingImages(true);
        const prepared = [];
        // Decode large photos one at a time to keep peak memory bounded on phones.
        for (const file of files) prepared.push(await prepareMobilePhoto(file));
        files = prepared;
      }
      const problem = validateImages([...images.map(image => image.file), ...files]);
      if (problem) setError(problem);
      else setImages([...images, ...files.map(file => ({ id: newId(), file }))]);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setPreparingImages(false);
    }
  };
  useEffect(() => saveDraft(config.apiPrefix, draftKey, draft), [draftKey, draft]);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  useEffect(() => { if (settings.data?.fork_source_session_id && !iid) inputRef.current?.focus({ preventScroll: true }); }, [settings.data?.fork_source_session_id, iid]);
  const fork = useCallback(async (invocation: string, destination: string, signal: AbortSignal) => {
    const record = await agentClient.fork(session, invocation, destination, signal);
    client.setQueryData(['agent-binding', record.session_id], record);
    client.setQueryData(['agent-settings', record.session_id, record.workspace?.workspace_id ?? null], record);
    onRefreshDirectory();
    onForked(record);
  }, [client, session, onForked, onRefreshDirectory]);
  const loadUsage = useCallback((invocation: string, signal: AbortSignal) => agentClient.usage(session, invocation, signal), [session]);
  const queue = useQuery({ queryKey: ['agent-queue', session], queryFn: ({ signal }) => agentClient.queue(session, signal), enabled: active, refetchInterval: active && existing ? 1500 : false, retry: retryAgentRead, retryDelay: agentReadRetryDelay });
  const [queuePending, setQueuePending] = useState<string | null>(null);
  const submission = useRef<{ key: string; id: string } | null>(null);
  const operation = useRef<AbortController | null>(null), queueRequest = useRef<AbortController | null>(null), controlRequest = useRef<AbortController | null>(null);
  const [controlSubmitting, setControlSubmitting] = useState<'pause' | 'recover' | 'cancel' | null>(null);
  const [pendingControl, setPendingControl] = useState<{ id: string; action: 'pause' | 'recover' | 'cancel' } | null>(null);
  const controlOperation = useQuery({ queryKey: ['agent-composer-control', pendingControl?.id], queryFn: ({ signal }) => agentClient.operation(pendingControl!.id, signal), enabled: active && !!pendingControl, refetchInterval: query => active && pendingControl && query.state.data?.status !== 'succeeded' && query.state.data?.status !== 'failed' ? 500 : false, retry: false });
  useEffect(() => () => { operation.current?.abort(); queueRequest.current?.abort(); controlRequest.current?.abort(); }, []);
  const refresh = useCallback(() => {
    void client.invalidateQueries({ queryKey: ['agent-execution'] });
    void client.invalidateQueries({ queryKey: ['agent-invocations', session] });
    void client.invalidateQueries({ queryKey: ['agent-settings', session] });
    void client.invalidateQueries({ queryKey: ['agent-usage', session] });
    void client.invalidateQueries({ queryKey: ['agent-subagents', session] });
    void client.invalidateQueries({ queryKey: ['agent-queue', session] });
    onRefreshDirectory();
  }, [client, session, onRefreshDirectory]);
  const recentUser = Object.values(conversation.state.rows).find(row => row.type === 'message' && row.message.source === 'user');
  const heading = settings.data?.title || (title === 'Conversation' && recentUser?.type === 'message' ? recentUser.message.content.find(part => part.type === 'text')?.text?.slice(0, 100) || title : title);
  useEffect(() => { if (active) document.title = existing ? heading + ' · AutoAgent' : 'AutoAgent'; }, [active, existing, heading]);
  const current = execution.data?.current, controls = execution.data?.controls;
  const currentId = current?.invocation_id, currentStatus = current?.status;
  const running = currentStatus === 'running' || currentStatus === 'waiting';
  useEffect(() => {
    if (!active || !currentId || running) return;
    void client.invalidateQueries({ queryKey: ['agent-usage', session, null] });
    void client.invalidateQueries({ queryKey: ['agent-subagents', session] });
  }, [active, client, currentId, currentStatus, running, session]);
  const child = !!current?.parent_invocation_id;
  const parent = execution.data?.ancestors.find(item => item.invocation_id === current?.parent_invocation_id);
  const canStart = !iid && (!existing || turns.isSuccess) || !!current && terminal.has(current.status) && !child;
  const snapshot = execution.data?.snapshot;
  const saved = effectiveSettings(settings.data, snapshot, child);
  const selectedModel = catalog.provider_options?.find(item => item.id === saved?.settings.model.provider)
    ?.model_details?.find(item => item.id === saved?.settings.model.model);
  const context = contextUsage(showingArchive ? conversationSummary(turns.data?.pages) : summary, agent.context_policy.context_window, selectedModel?.model_info.max_input_tokens);
  if (conversation.error && context.source === 'empty') { context.usedTokens = null; context.source = 'unavailable'; }
  const changeSettings = async (next: SessionSettings) => {
    if (!saved) return;
    if (!existing) { saveDraftSettings(next); return; }
    try { const record = await agentClient.configure(session, next, saved.version, new AbortController().signal); client.setQueryData(['agent-settings', session, workspace], record); refresh(); }
    catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); throw reason; }
  };
  const rename = async (name: string) => {
    if (!settings.data) return;
    try {
      const record = await agentClient.rename(session, name, settings.data.metadata_version, new AbortController().signal);
      client.setQueryData(['agent-settings', session, workspace], record);
      client.setQueryData(['agent-binding', session], record);
      onRefreshDirectory();
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 409) await settings.refetch();
      throw reason;
    }
  };

  const changed = !!saved && !!snapshot && (running || current?.status === 'paused') && (
    saved.settings.model.provider !== snapshot.settings.model.provider || saved.settings.model.model !== snapshot.settings.model.model || saved.settings.model.reasoning_effort !== snapshot.settings.model.reasoning_effort);
  useEffect(() => {
    if (!pendingControl || !controlOperation.data || controlOperation.data.status === 'running') return;
    if (controlOperation.data.status !== 'succeeded') setError(controlOperation.data.error?.message ?? 'Could not complete the execution control.');
    setPendingControl(null); refresh(); conversation.refresh();
  }, [pendingControl, controlOperation.data, refresh, conversation.refresh]);
  const controlSerial = useRef(0);
  const control = async (action: 'pause' | 'recover' | 'cancel') => {
    if (!controls || (controlSubmitting || pendingControl) && action !== 'cancel') return;
    const serial = ++controlSerial.current;
    const controller = new AbortController(); controlRequest.current = controller;
    setControlSubmitting(action); setPendingControl(null); setError('');
    try {
      const fresh = await agentClient.controls(controls.root_invocation_id, null, controller.signal);
      if (!fresh.actions[action].available) throw new Error(fresh.actions[action].reason ?? 'This control is unavailable.');
      const receipt = await agentClient.action({ invocation_id: fresh.root_invocation_id, action, request_id: newId().replaceAll('-', ''), ...(action === 'recover' ? { expected_version: fresh.expected_version } : {}) }, controller.signal);
      if (serial !== controlSerial.current) return;
      if (receipt.status === 'running') setPendingControl({ id: receipt.operation_id, action });
      else if (receipt.status !== 'succeeded') throw new Error(receipt.error?.message ?? 'Could not complete the execution control.');
      refresh(); conversation.refresh();
    } catch (reason) { if (!controller.signal.aborted && serial === controlSerial.current) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { if (serial === controlSerial.current) setControlSubmitting(null); }
  };
  const updateQueue = (item: QueuedMessage) => {
    client.setQueryData<QueueState>(['agent-queue', session], previous => ({
      invocation_id: item.state === 'started' ? item.invocation_id : previous?.invocation_id ?? iid ?? null,
      items: [...(previous?.items ?? []).filter(value => value.id !== item.id), ...(['queued', 'blocked', 'starting', 'sending'].includes(item.state) ? [item] : [])].sort((a, b) => a.position - b.position),
    }));
  };
  const send = async (immediate = false) => {
    const message = draft.trim();
    if ((!message && !images.length) || busy || preparingImages) return;
    operation.current?.abort(); operation.current = new AbortController(); setBusy(true); setError('');
    try {
      const target = immediate ? iid : undefined;
      const key = JSON.stringify([message, target, images.map(image => image.id)]);
      if (submission.current?.key !== key) submission.current = { key, id: newId() };
      const item = await agentClient.enqueue(session, message, submission.current.id, workspace, operation.current.signal, target, existing ? undefined : settings.data?.settings, images.map(image => image.file));
      updateQueue(item); submission.current = null;
      setImages([]);
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
  const canSignal = running && !!resources.data?.user_signal_endpoint && !!controls?.actions.signal.available;
  const canSend = !preparingImages && !child && !!agent && (canStart || !!current || !!queue.data);
  const pending = controlSubmitting ?? pendingControl?.action ?? (controls?.control_state === 'pausing' ? 'pause' : null);
  const primary = composerAction({ draft, hasImages: images.length > 0, canSend, running, lifecycle: status, canPause: !!controls?.actions.pause.available, canResume: paused && !!controls?.actions.recover.available, sending: busy || !!iid && !current && execution.isFetching, pending });
  const caption = pending === 'pause' ? 'Pausing…' : pending === 'recover' ? 'Resuming…' : pending === 'cancel' ? 'Cancelling…' : paused ? 'Paused' : status === 'waiting' ? 'Waiting' : '';
  return <div className="agent-session-workspace"><main className="agent-chat" aria-label="Agent conversation">
    <Conversation onFork={fork} mobile={mobile} session={session} loadUsage={loadUsage} invocations={visibleInvocations} execution={execution.data} active={active} state={summary} loading={loadingHistory}
      error={turns.error ?? conversation.error} onRetry={conversation.refresh} running={status === 'running' || status === 'pausing'}
      turnIndex={turnIndex.data?.items} archived={showingArchive} onLoadTurn={setHistoryAnchor} onLatest={() => setHistoryAnchor(null)}
      onLoadOlder={visible.hasNextPage ? () => void visible.fetchNextPage() : undefined} loadingOlder={visible.isFetchingNextPage}
      switchingWindow={switchingWindow} onCancelJump={() => {
        if (switchingWindow) setHistoryAnchor(archived.data?.pages[0]?.items[0]?.invocation_id ?? null);
      }}
      header={(landmarks, jumpTo) => <ConversationHeader session={session} title={heading} existing={existing} active={active} running={running}
        hasWorkspace={!!workspace} onRename={settings.data ? rename : undefined} onOpenSubagent={onOpenSubagent}
        mobile={mobile} context={context} contextLoading={loadingHistory} landmarks={landmarks} onJump={jumpTo}
        onBack={parent ? () => onReturnToParent(parent.session_id, parent.invocation_id) : onShowDirectory} hasParent={!!parent} />} before={<>
      {changed ? <div className="agent-setting-notice">Model changed for the next task.</div> : null}
      {switchingWindow ? <div className="agent-load-older" role="status"><BusyLabel busy label="Loading turn">Loading turn</BusyLabel></div> : null}
      {historyAnchor && archived.error ? <div className="agent-inline-error" role="alert">Could not load this part of the conversation.<button onClick={() => void archived.refetch()}>Retry</button></div> : null}
      {turnIndex.error ? <div className="agent-inline-error" role="alert">Could not load all turns.<button onClick={() => void turnIndex.refetch()}>Retry</button></div> : null}
      {execution.error ? <div className="agent-inline-error" role="alert">{execution.error.message}<button onClick={refresh}>Refresh</button></div> : null}
    </>} after={<>
      {controls ? <Interactions rootId={controls.root_invocation_id} waits={waiting} active={active} onResolved={refresh} /> : null}
      {controls?.next_cursor && (!allPending || moreRequests.hasNextPage || moreRequests.isPending) ? <button className="agent-load-older" disabled={allPending && moreRequests.isFetching} onClick={() => { if (!allPending) setAllPending(true); else void moreRequests.fetchNextPage(); }}><BusyLabel busy={allPending && moreRequests.isFetching} label="Loading requests">Load more pending requests</BusyLabel></button> : null}
      {allPending && moreRequests.error ? <p className="agent-inline-error" role="alert">{moreRequests.error.message}<button onClick={() => void moreRequests.refetch()}>Retry</button></p> : null}
    </>} footer={<div className="agent-composer-area"><MessageQueue items={queue.data?.items ?? []} pending={queuePending} canSendNow={canSignal && !child} onAction={(item, action) => void queueAction(item, action)} />
      {queue.error ? <p className="agent-error-text" role="alert">Could not refresh queued messages. <button onClick={() => void queue.refetch()}>Retry</button></p> : null}
      {error ? <p className="agent-error-text" role="alert">{error}</p> : null}{controlOperation.error && pendingControl ? <p className="agent-error-text" role="alert">Could not refresh execution status.<button type="button" onClick={() => void controlOperation.refetch()}>Retry</button></p> : null}
        <WorkspacePicker active={active} mobile={mobile} value={workspace} items={workspaces} loading={workspacesLoading} error={workspaceError} onRetry={onRetryWorkspaces} locked={existing} disabled={busy || !settings.data} onChange={onWorkspaceChange} />
        <AgentComposer active={active} mobile={mobile} contextUsage={context} contextLoading={loadingHistory} primary={primary} onControl={action => void control(action)} session={session} draft={draft} onDraft={setDraft} inputRef={inputRef} busy={busy} running={running || paused} disabled={busy || child} canSend={canSend} canSendNow={canSignal && !child && !preparingImages} onSendNow={running ? () => void send(true) : undefined} onSend={() => void send()} settings={saved?.settings} providerOptions={catalog.provider_options ?? []} onSettingsChange={changeSettings} canCancel={!child && !!controls?.actions.cancel.available && pending !== 'cancel' && !busy} caption={caption}
          images={images} preparingImages={preparingImages} onPickImages={files => void pickImages(files)}
          onRemoveImage={id => { setImages(current => current.filter(image => image.id !== id)); setError(''); }} />
      </div>} />
    </main></div>;
}
