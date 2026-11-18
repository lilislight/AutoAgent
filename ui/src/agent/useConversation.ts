import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { useQueries, useQueryClient } from '@tanstack/react-query';
import { agentClient } from './client';
import { applyEvents, emptyConversation, resetTransient, type ConversationState } from './conversationState';
import type { AgentEvent, AgentInvocation } from './types';
import { eventRefresh } from './eventRefresh';
import { retryAgentRead, agentReadRetryDelay } from './readFeedback';

type Action = { type: 'events'; events: AgentEvent[] } | { type: 'reset' };
function reducer(state: ConversationState, action: Action) { return action.type === 'reset' ? resetTransient(state) : applyEvents(state, action.events); }

export function useConversation(session: string, invocations: AgentInvocation[], active: boolean) {
  const client = useQueryClient();
  const [state, dispatch] = useReducer(reducer, undefined, emptyConversation);
  const [connection, setConnection] = useState<'connecting' | 'live' | 'reconnecting' | 'offline'>('connecting');
  const [epoch, setEpoch] = useState(0);
  const loaded = useRef(new Map<string, number>());
  const latest = invocations.find(item => !['completed', 'failed', 'cancelled'].includes(item.status))?.invocation_id ?? invocations[0]?.invocation_id;
  const queries = useQueries({ queries: invocations.map(invocation => ({
    queryKey: ['agent-history', session, invocation.invocation_id],
    queryFn: ({ signal }: { signal: AbortSignal }) => agentClient.history(invocation.invocation_id, signal),
    enabled: active, staleTime: 0, retry: retryAgentRead, retryDelay: agentReadRetryDelay, refetchOnWindowFocus: false,
    refetchInterval: active && connection !== 'live' && invocation.invocation_id === latest ? 2000 : false,
  })) });
  const historyStamp = queries.map(item => item.dataUpdatedAt).join(':');
  const ids = invocations.map(item => item.invocation_id).join('\n');
  useEffect(() => {
    const events: AgentEvent[] = [];
    let applied = false;
    for (const iid of ids.split('\n').filter(Boolean)) {
      const key = ['agent-history', session, iid];
      const entry = client.getQueryState<AgentEvent[]>(key);
      if (entry?.data && loaded.current.get(iid) !== entry.dataUpdatedAt) {
        loaded.current.set(iid, entry.dataUpdatedAt); events.push(...entry.data);
        applied = true;
      }
    }
    // Empty snapshots also complete hydration and must release the loader.
    if (applied) dispatch({ type: 'events', events });
  }, [historyStamp, ids, session, client]);

  useEffect(() => {
    if (!active) return;
    const source = agentClient.watch(session);
    let frame = 0, retry = 0, pending: AgentEvent[] = [];
    const flush = () => {
      frame = 0;
      if (!pending.length) return;
      const events = pending; pending = [];
      dispatch({ type: 'events', events });
      const reads = eventRefresh(events);
      if (reads.execution) void client.invalidateQueries({ queryKey: ['agent-execution'] }, { cancelRefetch: false });
      if (reads.invocations) void client.invalidateQueries({ queryKey: ['agent-invocations', session] }, { cancelRefetch: false });
      if (reads.directory) void client.invalidateQueries({ queryKey: ['agent-sessions'] }, { cancelRefetch: false });
    };
    const ready = () => {
      cancelAnimationFrame(frame); pending = []; frame = 0;
      dispatch({ type: 'reset' }); setConnection('live');
      // Subscribe first, then reconcile saved snapshots. Event IDs deduplicate
      // events crossing the live/history boundary without replaying deltas.
      void client.invalidateQueries({ queryKey: ['agent-history', session] });
      void client.invalidateQueries({ queryKey: ['agent-invocations', session] });
      void client.invalidateQueries({ queryKey: ['agent-queue', session] });
    };
    const receive = (message: MessageEvent<string>) => {
      try {
        const event = JSON.parse(message.data) as AgentEvent;
        if (event.session_id !== session || event.payload?.v !== 1) return;
        // Hidden pages may stop animation frames entirely. Durable history is
        // reconciled on return instead of retaining an unbounded delta backlog.
        if (document.hidden) return;
        pending.push(event);
        if (!frame) frame = requestAnimationFrame(flush);
      } catch { setConnection('offline'); }
    };
    source.addEventListener('ready', ready);
    source.addEventListener('user_event', receive);
    source.addEventListener('reset', () => {
      source.close(); cancelAnimationFrame(frame); frame = 0; pending = [];
      setConnection('reconnecting'); dispatch({ type: 'reset' });
      retry = window.setTimeout(() => setEpoch(value => value + 1), 1500);
    });
    source.onerror = () => setConnection('reconnecting');
    const visible = () => {
      cancelAnimationFrame(frame); frame = 0; pending = [];
      if (!document.hidden) {
        if (source.readyState === EventSource.CLOSED) setEpoch(value => value + 1);
        dispatch({ type: 'reset' });
        void client.invalidateQueries({ queryKey: ['agent-history', session] });
        void client.invalidateQueries({ queryKey: ['agent-invocations', session] });
        void client.invalidateQueries({ queryKey: ['agent-execution'] });
        void client.invalidateQueries({ queryKey: ['agent-queue', session] });
      }
    };
    const restored = (event: PageTransitionEvent) => { if (event.persisted) setEpoch(value => value + 1); };
    const online = () => { if (source.readyState !== EventSource.OPEN) setEpoch(value => value + 1); };
    document.addEventListener('visibilitychange', visible);
    window.addEventListener('pageshow', restored);
    window.addEventListener('online', online);
    return () => {
      source.close(); clearTimeout(retry); cancelAnimationFrame(frame);
      document.removeEventListener('visibilitychange', visible);
      window.removeEventListener('pageshow', restored);
      window.removeEventListener('online', online);
    };
  }, [session, active, client, epoch]);
  const refresh = useCallback(() => { if (connection !== 'live') setEpoch(value => value + 1); void client.invalidateQueries({ queryKey: ['agent-history', session] }); }, [client, session, connection]);
  const loading = queries.some((item, index) => item.isLoading || item.data !== undefined && loaded.current.get(invocations[index].invocation_id) !== item.dataUpdatedAt);
  return { state, refresh, loading, error: queries.find(item => item.error)?.error };
}
