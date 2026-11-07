import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { useQueries, useQueryClient } from '@tanstack/react-query';
import { agentClient } from './client';
import { applyEvents, emptyConversation, resetTransient, type ConversationState } from './conversationState';
import type { AgentEvent, AgentInvocation } from './types';

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
    enabled: active, staleTime: 0, retry: 1, refetchOnWindowFocus: false,
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
      if (events.some(event => event.persist || event.kind === 'agent.interaction')) {
        void client.invalidateQueries({ queryKey: ['agent-execution'] });
        void client.invalidateQueries({ queryKey: ['agent-invocations', session] });
        void client.invalidateQueries({ queryKey: ['agent-sessions'] });
      }
    };
    const ready = () => {
      cancelAnimationFrame(frame); pending = []; frame = 0;
      dispatch({ type: 'reset' }); setConnection('live');
      // Subscribe first, then reconcile saved snapshots. Event IDs deduplicate
      // events crossing the live/history boundary without replaying deltas.
      void client.invalidateQueries({ queryKey: ['agent-history', session] });
      void client.invalidateQueries({ queryKey: ['agent-invocations', session] });
    };
    const receive = (message: MessageEvent<string>) => {
      try {
        const event = JSON.parse(message.data) as AgentEvent;
        if (event.session_id !== session || event.payload?.v !== 1) return;
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
    return () => { source.close(); clearTimeout(retry); cancelAnimationFrame(frame); };
  }, [session, active, client, epoch]);
  const refresh = useCallback(() => { setEpoch(value => value + 1); void client.invalidateQueries({ queryKey: ['agent-history', session] }); }, [client, session]);
  const loading = queries.some((item, index) => item.isLoading || item.data !== undefined && loaded.current.get(invocations[index].invocation_id) !== item.dataUpdatedAt);
  return { state, refresh, loading, error: queries.find(item => item.error)?.error };
}
