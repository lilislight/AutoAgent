import { useEffect, useMemo, useRef, useState } from 'react';
import { keepPreviousData, useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { getTimelineEventGroups, getTimelineMarkers } from '../api';
import { clusterEventGroups, eventGroupCategory, EVENT_MIN_GAP, type EventCluster } from '../eventRail';
import { eventCategory, timelineTimestamp, type TimeWindow } from '../timeline';
import { useExecutionSnapshot } from './ExecutionSnapshot';

export function TimelineEvents({ iid, window, full, previewing, queryWindow, active, trackWidth, onReplay, replaying, through, rangeRevision }: {
  rangeRevision: number;
  iid: string; window: TimeWindow; full: TimeWindow; previewing: boolean; queryWindow: TimeWindow; frontier: number; active: boolean; trackWidth: number; through: number;
  onReplay: (position: { sequence?: number; at_us?: number }) => void; replaying: boolean;
}) {
  const snapshot = useExecutionSnapshot();
  // A bounded, whole-execution aggregate supplies immediate geometry while the
  // selection moves. Exact selected-range counts replace it after settling.
  const whole = useQuery({ queryKey: ['timeline-event-groups', iid, full.start, full.end + 1, 512, through, snapshot.epoch], meta:snapshot.meta,
    queryFn: ({ signal }) => getTimelineEventGroups(iid, full.start, full.end + 1, 512, through, signal),
    enabled:active, staleTime:Infinity, gcTime:5000, refetchOnWindowFocus:false });
  const buckets = Math.max(1, Math.min(512, Math.ceil(trackWidth / EVENT_MIN_GAP) * 2));
  const query = useQuery({ queryKey: ['timeline-event-groups', iid, queryWindow.start, queryWindow.end, buckets, through, snapshot.epoch], meta:snapshot.meta,
    queryFn: ({ signal }) => getTimelineEventGroups(iid, queryWindow.start, queryWindow.end, buckets, through, signal),
    enabled:active && !previewing && trackWidth > 0, placeholderData:keepPreviousData, staleTime:Infinity, gcTime:5000, refetchOnWindowFocus:false });
  const current = !previewing && queryWindow.start === window.start && queryWindow.end === window.end + 1 && !query.isPlaceholderData;
  const groups = useMemo(() => clusterEventGroups((current ? query.data?.items : whole.data?.items ?? query.data?.items ?? []) ?? [], window, trackWidth), [query.data, whole.data, current, window.start, window.end, trackWidth]);
  const [cluster, setCluster] = useState<EventCluster | null>(null);
  const details = useInfiniteQuery({ queryKey:['timeline-marker-details', iid, cluster?.start_us, cluster?.end_us, snapshot.epoch], meta:snapshot.meta,
    queryFn:({ signal, pageParam }) => getTimelineMarkers(iid, cluster!.start_us, cluster!.end_us, pageParam, signal, through),
    initialPageParam:null as string | null, getNextPageParam:page => page.next_cursor ?? undefined,
    enabled:active && Boolean(cluster && cluster.count > 1), staleTime:Infinity, gcTime:5000, refetchOnWindowFocus:false });
  const markers = cluster?.marker ? [cluster.marker] : details.data?.pages.flatMap(page => page.items) ?? [];
  useEffect(() => setCluster(null), [iid, rangeRevision]);
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!cluster) return;
    const close = (event: PointerEvent) => { if (!container.current?.contains(event.target as Node)) setCluster(null); };
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') setCluster(null); };
    document.addEventListener('pointerdown',close); document.addEventListener('keydown',escape);
    return () => { document.removeEventListener('pointerdown',close); document.removeEventListener('keydown',escape); };
  }, [cluster]);
  return <div className="timeline-events" ref={container} aria-label="Runtime Events" aria-busy={query.isFetching || !current}>
    <div className="timeline-row"><div className="timeline-event-summary"><span title="All Events in the selected time window. Nearby Events are merged by their rendered distance.">{previewing ? 'Adjusting range…' : query.isFetching || !current ? 'Loading Events…' : `${query.data?.total_count ?? 0} Events`}</span></div>
      <div className="timeline-track timeline-event-track">{groups.map(group => <button key={`${group.first_sequence}:${group.last_sequence}`} data-event-count={group.count} className={`timeline-event-marker event-category-${eventGroupCategory(group)} ${group.count > 1 ? 'is-cluster' : ''}`} style={{ left:`${group.left}%` }}
        title={group.count > 1 ? `${group.count} Events · #${group.first_sequence}–#${group.last_sequence} · Click to inspect` : `#${group.marker!.sequence} ${group.marker!.event_name}`}
        aria-label={group.count > 1 ? `Inspect ${group.count} Events` : `Inspect Event ${group.marker!.sequence}`} onClick={() => { if (replaying && group.marker) onReplay({ sequence:group.marker.sequence }); else setCluster(group); }}><span className="timeline-event-dot" aria-hidden="true">{group.count > 1 ? group.count > 99 ? '99+' : group.count : ''}</span></button>)}</div>
    </div>
    {query.error ? <div className="timeline-message sidebar-error">{query.error.message}<button onClick={() => void query.refetch()}>Retry</button></div> : null}
    {cluster ? <div className="timeline-event-popover" role="dialog" aria-label="Nearby Events" onScroll={event => { const el=event.currentTarget; if (el.scrollHeight-el.scrollTop-el.clientHeight < 100 && details.hasNextPage && !details.isFetchingNextPage) void details.fetchNextPage(); }}>
      <header><strong>{cluster.count} Runtime Events</strong><button aria-label="Close Events" onClick={() => setCluster(null)}>×</button></header>
      {markers.map(marker => <div className={`timeline-event-record event-category-${eventCategory(marker.event_name)}`} key={marker.event_id}><code>#{marker.sequence}</code><span><i aria-hidden="true" />{marker.event_name}</span><button onClick={() => { onReplay({ sequence:marker.sequence }); setCluster(null); }} aria-label={`Replay Event ${marker.sequence}`}>Replay here</button><small>{timelineTimestamp(marker.occurred_at_us)}</small></div>)}
      {!cluster.marker && details.isPending ? <p className="timeline-message">Loading Event headers…</p> : null}
      {details.error ? <div className="timeline-message">{details.error.message}<button onClick={() => void details.refetch()}>Retry</button></div> : null}
      {details.hasNextPage ? <button className="load-more" disabled={details.isFetchingNextPage} onClick={() => void details.fetchNextPage()}>{details.isFetchingNextPage ? 'Loading…' : 'More Events'}</button> : null}
    </div> : null}
  </div>;
}
