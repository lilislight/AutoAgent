import { memo, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { keepPreviousData, useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { getTimelineEventGroups, getTimelineMarkers } from '../api';
import { clusterEventGroups, eventGroupCategory, EVENT_MIN_GAP, type EventCluster } from '../eventRail';
import { eventCategory, timelineTimestamp, type TimeWindow } from '../timeline';
import { eventZoomWindow } from '../timelineNavigation';
import { useExecutionSnapshot } from './ExecutionSnapshot';
import type { OpenFork } from './ExecutionFork';

export const TimelineEvents = memo(function TimelineEvents({ iid, window, full, queryWindow, active, trackWidth, onReplay, onFork, onZoom, through, rangeRevision }: {
  rangeRevision: number;
  iid: string; window: TimeWindow; full: TimeWindow; queryWindow: TimeWindow; frontier: number; active: boolean; trackWidth: number; through: number;
  onZoom: (window: TimeWindow) => void;
  onReplay: (position: { sequence?: number; at_us?: number }) => void; onFork:OpenFork;
}) {
  const snapshot = useExecutionSnapshot();
  // Re-bin cached Event headers in the Worker during brushing as well as after
  // settling. Whole-execution bins cannot resolve a newly enlarged interval.
  const buckets = Math.max(1, Math.min(512, Math.ceil(trackWidth / EVENT_MIN_GAP) * 2));
  const query = useQuery({ queryKey: ['timeline-event-groups', iid, queryWindow.start, queryWindow.end, buckets, through, snapshot.epoch], meta:snapshot.meta,
    queryFn: ({ signal }) => getTimelineEventGroups(iid, queryWindow.start, queryWindow.end, buckets, through, signal),
    enabled:active && trackWidth > 0, placeholderData:keepPreviousData, staleTime:Infinity, gcTime:5000, refetchOnWindowFocus:false });
  const current = queryWindow.start === window.start && queryWindow.end === window.end + 1 && !query.isPlaceholderData;
  const groups = useMemo(() => clusterEventGroups(query.data?.items ?? [], window, trackWidth), [query.data, window.start, window.end, trackWidth]);
  const count = current ? query.data?.total_count : groups.reduce((sum,group)=>sum+group.count,0);
  const [cluster, setCluster] = useState<EventCluster | null>(null);
  const anchor = useRef<HTMLButtonElement | null>(null), popup = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState<{left:number;top:number;maxHeight:number;above:boolean}|null>(null);
  const details = useInfiniteQuery({ queryKey:['timeline-marker-details', iid, cluster?.start_us, cluster?.end_us, snapshot.epoch], meta:snapshot.meta,
    queryFn:({ signal, pageParam }) => getTimelineMarkers(iid, cluster!.start_us, cluster!.end_us, pageParam, signal, through),
    initialPageParam:null as string | null, getNextPageParam:page => page.next_cursor ?? undefined,
    enabled:active && Boolean(cluster && cluster.count > 1), staleTime:Infinity, gcTime:5000, refetchOnWindowFocus:false });
  const markers = cluster?.marker ? [cluster.marker] : details.data?.pages.flatMap(page => page.items) ?? [];
  const zoomTarget = cluster ? eventZoomWindow(full,cluster.start_us,cluster.end_us) : null;
  useEffect(() => setCluster(null), [iid, rangeRevision]);
  useEffect(() => { if (!active) setCluster(null); }, [active]);
  const container = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    if (!cluster || !anchor.current || !popup.current) return;
    const button = anchor.current, panel = popup.current, viewport = globalThis.window;
    const update = () => {
      if (!button.isConnected || !button.getClientRects().length) { setCluster(null); return; }
      const rect = button.getBoundingClientRect(), margin = 8, gap = 6;
      const width = panel.offsetWidth;
      const below = Math.max(0, viewport.innerHeight - rect.bottom - gap - margin);
      const above = Math.max(0, rect.top - gap - margin);
      const opensAbove = below < Math.min(panel.scrollHeight, 140) && above > below;
      const maxHeight = Math.min(260, opensAbove ? above : below);
      const height = Math.min(panel.scrollHeight + 2, maxHeight);
      const next = {left: Math.max(margin, Math.min(rect.left + rect.width / 2 - width / 2, viewport.innerWidth - width - margin)),
        top: Math.max(margin, Math.min(opensAbove ? rect.top - gap - height : rect.bottom + gap, viewport.innerHeight - height - margin)),
        maxHeight, above: opensAbove};
      setPosition(current => current && current.left===next.left && current.top===next.top && current.maxHeight===next.maxHeight && current.above===next.above ? current : next);
    };
    update();
    const observer = new ResizeObserver(update);
    observer.observe(button); observer.observe(panel);
    if (container.current) observer.observe(container.current);
    viewport.addEventListener('resize', update); viewport.addEventListener('scroll', update, true);
    return () => { observer.disconnect(); viewport.removeEventListener('resize', update); viewport.removeEventListener('scroll', update, true); };
  }, [cluster]);
  useEffect(() => {
    if (!cluster) return;
    const close = (event: PointerEvent) => { if (!container.current?.contains(event.target as Node) && !popup.current?.contains(event.target as Node)) setCluster(null); };
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') setCluster(null); };
    document.addEventListener('pointerdown',close); document.addEventListener('keydown',escape);
    return () => { document.removeEventListener('pointerdown',close); document.removeEventListener('keydown',escape); };
  }, [cluster]);
  return <div className="timeline-events" ref={container} aria-label="Runtime Events" aria-busy={query.isFetching || !current}>
    <div className="timeline-row"><div className="timeline-event-summary"><span title="All Events in the selected time window. Nearby Events are merged by their rendered distance.">{count == null ? 'Loading Events…' : `${count} Events`}</span></div>
      <div className="timeline-track timeline-event-track">{groups.map(group => <button key={`${group.first_sequence}:${group.last_sequence}`} data-event-count={group.count} className={`timeline-event-marker event-category-${eventGroupCategory(group)} ${group.count > 1 ? 'is-cluster' : ''}`} style={{ left:`${group.left}%` }}
        title={group.count > 1 ? `${group.count} Events · #${group.first_sequence}–#${group.last_sequence} · Click to inspect` : `#${group.marker!.sequence} ${group.marker!.event_name}`}
        aria-label={group.count > 1 ? `Inspect ${group.count} Events` : `Inspect Event ${group.marker!.sequence}`} aria-haspopup="dialog" aria-expanded={cluster?.first_sequence===group.first_sequence && cluster.last_sequence===group.last_sequence} onClick={event => { anchor.current=event.currentTarget;setCluster(group); }}><span className="timeline-event-dot" aria-hidden="true">{group.count > 1 ? group.count > 99 ? '99+' : group.count : ''}</span></button>)}</div>
    </div>
    {query.error ? <div className="timeline-message sidebar-error">{query.error.message}<button onClick={() => void query.refetch()}>Retry</button></div> : null}
    {cluster && active ? createPortal(<div key={`${cluster.first_sequence}:${cluster.last_sequence}`} ref={popup} className={`timeline-event-popover ${position?.above ? 'opens-above' : ''}`} style={{left:position?.left,top:position?.top,maxHeight:position?.maxHeight,visibility:position ? undefined : 'hidden'}} role="dialog" aria-label="Runtime Events" onScroll={event => { const el=event.currentTarget; if (el.scrollHeight-el.scrollTop-el.clientHeight < 100 && details.hasNextPage && !details.isFetchingNextPage) void details.fetchNextPage(); }}>
      <header><strong>{cluster.count} Runtime {cluster.count===1?'Event':'Events'}</strong><button aria-label="Close Events" onClick={() => setCluster(null)}>×</button></header>
      {cluster.count > 1 ? <div className="timeline-cluster-zoom">{zoomTarget ? <button onClick={()=>{onZoom(zoomTarget);setCluster(null);}}>Zoom into interval</button> : <span className="muted">Same timestamp · ordered by Event sequence</span>}</div> : null}
      {markers.map(marker => <div className={`timeline-event-record event-category-${eventCategory(marker.event_name)}`} key={marker.event_id}><code>#{marker.sequence}</code><span className="timeline-event-kind" title={marker.event_name}><i aria-hidden="true" /><span className="timeline-event-name">{marker.event_name}</span></span><small>{timelineTimestamp(marker.occurred_at_us)}</small><div className="timeline-event-actions"><button onClick={() => { onReplay({ sequence:marker.sequence }); setCluster(null); }} aria-label={`Replay Event ${marker.sequence}`}>Replay here</button><button onClick={event=>{onFork(marker.sequence,event.currentTarget,anchor.current);setCluster(null);}} aria-label={`Fork after Event ${marker.sequence}`}>Fork here</button></div></div>)}
      {!cluster.marker && details.isPending ? <p className="timeline-message">Loading Event headers…</p> : null}
      {details.error ? <div className="timeline-message">{details.error.message}<button onClick={() => void details.refetch()}>Retry</button></div> : null}
      {!cluster.marker && details.hasNextPage ? <button className="load-more" disabled={details.isFetchingNextPage} onClick={() => void details.fetchNextPage()}>{details.isFetchingNextPage ? 'Loading…' : 'More Events'}</button> : null}
    </div>, document.body) : null}
  </div>;
});
