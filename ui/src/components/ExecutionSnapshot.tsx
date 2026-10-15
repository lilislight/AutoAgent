import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from 'react';
import { useIsFetching, useQueryClient } from '@tanstack/react-query';
import type { ExecutionTrace } from '../types';
import { belongsToSnapshot, metadataScope, snapshotTime } from '../snapshot';
import { formatTime } from './TraceDetails';

const SnapshotContext = createContext({ epoch: 0, meta: {} as Record<string, unknown>, refreshing: false, error: null as string | null, refresh: async () => {} });
export function useExecutionSnapshot() { return useContext(SnapshotContext); }
/** Keep the last visible metadata during a refresh, only for the same record/page. */
export function useSnapshotMetadata<T>(scope: string, data: T | undefined) {
  const previous = useRef<{ scope: string; data: T } | undefined>(undefined);
  if (data !== undefined) previous.current = { scope, data };
  return data ?? (previous.current?.scope === scope ? previous.current.data : undefined);
}
export function ExecutionSnapshotProvider({ trace, onRefresh, children, preparing = false }: { trace: ExecutionTrace; onRefresh: () => Promise<void>; children: ReactNode; preparing?: boolean }) {
  const client = useQueryClient();
  const iid = trace.invocation.invocation_id;
  const epoch = useRef(trace.observed_at_us).current;
  const published = useRef(trace.frontier);
  useEffect(() => {
    if (published.current === trace.frontier) return;
    published.current = trace.frontier;
    void client.invalidateQueries({ predicate: query => belongsToSnapshot(query.queryKey, query.meta, iid, epoch), refetchType: 'active' });
  }, [trace.frontier, iid, epoch, client]);
  const [cycle, setCycle] = useState<{ phase: 'idle' | 'trace' | 'metadata'; started: number; error: string | null }>({ phase: 'idle', started: 0, error: null });
  const busy = useRef(false);
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  const fetching = useIsFetching({ predicate: query => belongsToSnapshot(query.queryKey, query.meta, iid, epoch) });
  const refresh = async () => {
    if (busy.current) return;
    busy.current = true;
    const started = performance.now();
    setCycle({ phase: 'trace', started, error: null });
    try {
      await onRefresh();
      await client.invalidateQueries({ predicate: query => belongsToSnapshot(query.queryKey, query.meta, iid, epoch), refetchType: 'active' });
      if (mounted.current) setCycle({ phase: 'metadata', started, error: null });
    } catch (error) {
      busy.current = false;
      if (mounted.current) setCycle({ phase: 'idle', started, error: error instanceof Error ? error.message : 'Could not refresh this execution.' });
    }
  };
  useEffect(() => {
    if (cycle.phase !== 'metadata' || fetching || preparing) return;
    // Allow dependent, visible pages to mount before declaring this refresh complete.
    const timer = setTimeout(() => {
      const failed = client.getQueryCache().getAll().find(query => query.isActive() && belongsToSnapshot(query.queryKey, query.meta, iid, epoch) && query.state.status === 'error');
      busy.current = false;
      setCycle({ phase: 'idle', started: cycle.started, error: failed ? 'Some execution details could not refresh. Try again.' : null });
    }, Math.max(50, 300 - (performance.now() - cycle.started)));
    return () => clearTimeout(timer);
  }, [cycle.phase, cycle.started, fetching, preparing, iid, epoch, client]);
  return <SnapshotContext.Provider value={{ epoch, meta: metadataScope(iid, epoch), refreshing: cycle.phase !== 'idle', error: cycle.error, refresh }}>{children}</SnapshotContext.Provider>;
}

export function ExecutionSnapshotStatus({ trace, active, compact=false }: { trace: ExecutionTrace; active: boolean; compact?: boolean }) {
  const { refresh, refreshing, error } = useExecutionSnapshot();
  const event = trace.last_event;
  const title = `${event ? `Last saved event: #${event.sequence} ${event.event_name}\n${formatTime(event.occurred_at_us, true)}` : 'No Runtime Events have been saved for this execution.'}\nChecked: ${formatTime(trace.observed_at_us, true)}\nGraph colours reflect saved data. Refresh to check for updates.${error ? `\n${error}` : ''}`;
  return <div className={`execution-snapshot-status ${error ? 'has-error' : ''}`}>
    <span className="snapshot-label" title={title} role={error ? 'alert' : 'status'}>{refreshing ? 'Refreshing…' : error ? 'Refresh incomplete' : compact ? '' : 'Snapshot'}{!refreshing && !error ? <time dateTime={event ? new Date(event.occurred_at_us / 1000).toISOString() : undefined}>{snapshotTime(event?.occurred_at_us)}</time> : null}</span>
    <button className={`snapshot-refresh ${refreshing ? 'is-refreshing' : ''}`} aria-label="Refresh execution snapshot" title={error ? 'Retry execution refresh' : 'Refresh Graph, Timeline and Inspector'} disabled={!active || refreshing} onClick={() => void refresh()}><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="M15 6a6 6 0 1 0 1 6M15 2v4h-4" /></svg></button>
  </div>;
}
