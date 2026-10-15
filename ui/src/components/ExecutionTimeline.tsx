import { useEffect, useLayoutEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { getExecutionTimeline, getTimelineItems, getTimelinePosition } from '../api';
import type { ExecutionTrace, TimelineFocus, TimelineOverview, TimelineRow, WorkflowGraphData } from '../types';
import { elapsedLabel, focusWindow, fullWindow, moveWindow, spanGeometry, timelineScale, timelineTimestamp, zoomWindow, type TimeWindow } from '../timeline';
import { duration } from './TraceDetails';
import { callPresentation, executionTiming, isSystemCall } from '../inspectorModel';
import { useExecutionSnapshot, useSnapshotMetadata } from './ExecutionSnapshot';
import { TimelineEvents } from './TimelineEvents';
import { TimelineRange } from './TimelineRange';
import { TimelineNodeFilter } from './TimelineNodeFilter';
import { TimelineReveal } from './TimelineReveal';
import type { ExecutionReplay } from './ExecutionReplay';

interface Props {
  nowUs: number; followingLive: boolean;
  ready: boolean; loadingStatus: string; loadError?: string; onRetry: () => void;
  trace: ExecutionTrace; workflow: WorkflowGraphData; active: boolean;
  selectedEvent: { id: string; time: number; sequence: number } | null; focus: TimelineFocus | null; selected: string | null; selectedNode: string | null; onPick: (row: TimelineRow) => void;
  replay: ExecutionReplay; onReplay: (position: { sequence?: number; at_us?: number }) => void;
}
function usePage(iid: string, kind: string, window: TimeWindow, occurrence: string | null, active: boolean, frontier: number, nodeId: string | null = null, seekId: string | null = null) {
  const snapshot = useExecutionSnapshot();
  const scope = JSON.stringify([iid, kind, window.start, window.end, occurrence, nodeId, seekId]);
  const [seek, setSeek] = useState<{ scope: string; page?: number }>({ scope });
  const desiredPage = seek.scope === scope ? seek.page : undefined;
  const location = useQuery({ queryKey: ['timeline-position', scope, desiredPage, snapshot.epoch], meta: snapshot.meta,
    queryFn: ({ signal }) => getTimelinePosition(iid, kind, seekId!, window.start, window.end, occurrence, nodeId, desiredPage, signal),
    enabled: active && Boolean(seekId), retry: false, staleTime: Infinity, gcTime: 5000, refetchOnWindowFocus: false });
  const located = useSnapshotMetadata(JSON.stringify([scope, desiredPage]), location.data);
  const anchor = seekId ? located?.cursor ?? null : null;
  const baseIndex = seekId ? located?.page_index ?? 0 : 0;
  const pageScope = JSON.stringify([scope, anchor, baseIndex]);
  const [position, setPosition] = useState({ scope: pageScope, cursors: [anchor] as (string | null)[], index: 0 });
  const current = position.scope === pageScope ? position : { scope: pageScope, cursors: [anchor], index: 0 };
  const query = useQuery({ queryKey: ['timeline-items', iid, kind, window.start, window.end, occurrence, current.cursors[current.index], nodeId, snapshot.epoch], meta: snapshot.meta,
    queryFn: ({ signal }) => getTimelineItems(iid, kind, window.start, window.end, occurrence, current.cursors[current.index], signal, nodeId),
    enabled: active && (!seekId || Boolean(located) || Boolean(location.error)), staleTime: Infinity, gcTime: 5000, refetchOnWindowFocus: false });
  const data = useSnapshotMetadata(JSON.stringify([scope, current.cursors[current.index]]), query.data);
  // Range previews reproject the visible page immediately. Keep it while the
  // settled range loads, but never carry a page across filters or seek targets.
  const retentionScope = JSON.stringify([iid, kind, occurrence, nodeId, seekId, baseIndex + current.index]);
  const retained = useSnapshotMetadata(retentionScope, data);
  const visibleQuery = { ...query, data: seekId && !located && !location.error ? undefined : retained, isPending: !retained && (query.isPending || Boolean(seekId && location.isPending)), isFetching: query.isFetching || Boolean(seekId && location.isFetching) };
  return { query: visibleQuery, location, index: baseIndex + current.index,
    previous: () => { if (current.index) setPosition({ ...current, index: current.index - 1 }); else if (baseIndex) setSeek({ scope, page: baseIndex - 1 }); },
    next: () => { if (data?.next_cursor) setPosition({ scope: pageScope, cursors: [...current.cursors.slice(0, current.index + 1), data.next_cursor], index: current.index + 1 }); } };
}
function PageControls({ page, label }: { page: ReturnType<typeof usePage>; label: string }) {
  if (!page.index && !page.query.data?.next_cursor) return null;
  return <nav className="timeline-pagination" aria-label={`${label} pagination`}><button disabled={!page.index || page.query.isFetching} onClick={page.previous}>Previous</button><span>Page {page.index + 1}</span><button disabled={!page.query.data?.next_cursor || page.query.isFetching} onClick={page.next}>Next</button></nav>;
}
function QueryMessage({ page, label }: { page: ReturnType<typeof usePage>; label: string }) {
  return page.query.isPending ? <p className="timeline-message" role="status">Loading {label}…</p> : page.query.error ? <div className="timeline-message sidebar-error"><p>{page.query.error.message}</p><button onClick={() => void page.query.refetch()}>Retry</button></div> : !page.query.data?.items.length ? <p className="timeline-message muted">No {label} in this window.</p> : null;
}
function EmbeddedIntervals({ calls, wait, window, observed, paused }: { calls?: TimelineRow[]; wait?: TimelineRow | null; window: TimeWindow; observed: number; paused: boolean }) {
  const segment = (row: TimelineRow) => {
    const geo = spanGeometry(row, window, observed);
    const presented = row.kind === 'call' ? callPresentation(row, row.wait) : row;
    const status = paused && ['ready', 'running', 'waiting'].includes(presented.status ?? '') ? 'paused' : presented.status;
    return geo.visible ? <span key={`${row.kind}:${row.id}`} className={`timeline-span timeline-embedded timeline-embedded-${row.kind} timeline-state-${status} ${geo.open ? 'is-open' : ''}`} data-preview-kind={row.kind} style={{ left: `${geo.left}%`, width: `${geo.width}%` }} /> : null;
  };
  return <span className="timeline-embedded-intervals" aria-hidden="true">{calls?.map(call => <span key={call.id}>{segment(call)}{call.wait ? segment(call.wait) : null}</span>)}{wait ? segment(wait) : null}</span>;
}
function SpanRow({ reveal, row, label, window, observed, origin, trackWidth, selected, selectedNode, paused, onPick, onZoom, expanded, onExpand }: {
  reveal?: number; row: TimelineRow; label: string; window: TimeWindow; observed: number; selected: string | null;
  origin: number; trackWidth: number; onZoom: (row: TimelineRow) => void;
  selectedNode?: string | null; paused: boolean; onPick: (row: TimelineRow) => void; expanded?: boolean; onExpand?: () => void;
}) {
  const geo = spanGeometry(row, window, observed);
  const isSelected = selected === `${row.kind}:${row.id}`;
  const element = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!isSelected || reveal == null) return;
    const timer = setTimeout(() => {
      const row = element.current;
      const area = row?.closest<HTMLElement>('.timeline-scroll');
      if (!row || !area || row.closest('[inert]')) return;
      const top = row.getBoundingClientRect().top - area.getBoundingClientRect().top + area.scrollTop;
      if (top < area.scrollTop + 40 || top + row.offsetHeight > area.scrollTop + area.clientHeight) area.scrollTop = Math.max(0, top - 70);
    }, 240);
    return () => clearTimeout(timer);
  }, [isSelected, reveal]);
  const graphSelected = !selected && row.kind === 'node' && selectedNode === row.node_id;
  const presented = row.kind === 'call' ? callPresentation(row, row.wait) : row;
  const status = paused && ['ready', 'running', 'waiting'].includes(presented.status ?? '') ? 'paused' : presented.status ?? '';
  const execution = executionTiming(row);
  const waitGeo = row.wait ? spanGeometry(row.wait, window, observed) : null;
  const elapsed = row.status === 'skipped' ? 'Skipped' : geo.elapsed === null ? 'End not recorded' : `${geo.open ? '~ ' : ''}${duration(geo.elapsed)}${geo.open ? ' · open' : ''}`;
  const startLabel = row.kind === 'wait' ? 'Waiting since' : row.kind === 'node' && row.started_at_us == null ? row.ready_at_us != null ? 'Ready since' : 'Recorded' : 'Started';
  const relativeStart = elapsedLabel(row.start_at_us - origin);
  const description = `${label}\n${status}\n${startLabel}: ${timelineTimestamp(row.start_at_us)} (+${relativeStart})\nEnd: ${row.end_at_us == null ? 'Not ended at snapshot' : timelineTimestamp(row.end_at_us)}\nElapsed: ${elapsed}${row.kind === 'call' && execution !== null ? `\nQueue: ${duration((row.queue_duration_ns ?? 0) / 1000)}\nExecution: ${duration(execution)}` : ''}${waitGeo ? `\nWaiting: ${duration(waitGeo.elapsed)} (within this Call)` : ''}${row.preview_calls?.length ? '\nUp to 6 visible Calls are previewed in time order. Expand for details.' : ''}\n${row.id}`;
  const tiny = geo.point || geo.width * trackWidth / 100 < 7;
  const barClasses = `timeline-state-${status} ${geo.clippedStart ? 'clipped-start' : ''} ${geo.clippedEnd ? 'clipped-end' : ''} ${geo.open ? 'is-open' : ''}`;
  return <div ref={element} className={`timeline-row timeline-${row.kind} ${expanded ? 'is-expanded' : ''} ${isSelected ? 'is-selected' : graphSelected ? 'graph-selected' : ''}`} data-timeline-id={row.id}>
    <div className="timeline-row-label">{onExpand ? <button className="timeline-disclosure" aria-expanded={expanded} aria-label={`${expanded ? 'Hide' : 'Show'} details for ${label}`} onClick={onExpand}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m6 4 4 4-4 4" /></svg></button> : <span className="timeline-disclosure-spacer" />}<button className="timeline-item-label" onClick={() => onPick(row)} title={description}><span>{label}</span><small><span className={`timeline-kind timeline-kind-${row.kind}`}>{row.kind === 'call' ? isSystemCall(row) ? 'Command' : 'Call' : row.kind === 'node' ? 'Node' : 'Wait'}</span>{row.kind === 'node' ? `${status}${row.call_count ? ` · ${row.call_count} ${row.call_count === 1 ? 'call' : 'calls'}` : ''}` : row.kind === 'wait' ? `${row.wait_kind} · ${status}` : `Unit ${row.unit_index} · ${status}`}</small></button><button className="timeline-row-zoom" aria-label={`Focus interval for ${label}`} title="Focus interval" onClick={() => onZoom(row)}><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><circle cx="8" cy="8" r="5" /><path d="m12 12 5 5M5 8h6M8 5v6" /></svg></button></div>
    <div className="timeline-duration" title={description}><strong>{elapsed}</strong>{row.kind === 'call' && execution !== null ? <small>Execution {duration(execution)}</small> : waitGeo && !expanded ? <small>Waiting {duration(waitGeo.elapsed)}</small> : row.kind === 'wait' ? <small>Within command Call</small> : null}</div>
    <div className="timeline-track">
      {geo.visible ? <><span className={`timeline-span timeline-block ${barClasses}`} style={{ left: `${geo.left}%`, width: `${geo.width}%` }} />{!expanded ? <EmbeddedIntervals calls={row.preview_calls} wait={row.wait} window={window} observed={observed} paused={paused} /> : null}{tiny ? <span className={`timeline-span timeline-small-mark ${barClasses} ${geo.point ? 'is-instant' : ''}`} style={{ left: `${geo.left}%` }} /> : null}<button className="timeline-block-hit" style={{ left: `${geo.left}%`, width: `${geo.width}%` }} onClick={() => onPick(row)} onDoubleClick={() => onZoom(row)} title={`${description}${tiny ? '\nShort interval: double-click to focus' : ''}`} aria-label={`Inspect ${label}, elapsed ${elapsed}`} /></> : null}
    </div>
  </div>;
}
function CallRow({ row, ...props }: Omit<Parameters<typeof SpanRow>[0], 'label' | 'expanded' | 'onExpand'>) {
  const [expanded, setExpanded] = useState(false);
  useEffect(() => { if (props.reveal != null && row.wait && props.selected === `wait:${row.wait.id}`) setExpanded(true); }, [props.selected, props.reveal, row.wait?.id]);
  const wait = row.wait ? { ...row.wait, node_id: row.node_id, ui_ordinal: row.ui_ordinal } : null;
  return <>
    <SpanRow row={row} label={row.operator_id?.replace(/^system_command:/, '') ?? 'Call'} {...props} expanded={expanded} onExpand={wait ? () => setExpanded(value => !value) : undefined} />
    {wait ? <TimelineReveal open={expanded}><div className="timeline-call-details"><SpanRow row={wait} label="Waiting interval" {...props} /></div></TimelineReveal> : null}
  </>;
}
function ExpandedOccurrence({ node, window, queryWindow, overview, active, selected, onPick, onZoom, trackWidth, focus }: {
  focus: TimelineFocus | null; node: TimelineRow; window: TimeWindow; queryWindow: TimeWindow; overview: TimelineOverview; active: boolean;
  selected: string | null; onPick: (row: TimelineRow) => void;
  trackWidth: number; onZoom: (row: TimelineRow) => void;
}) {
  const calls = usePage(node.invocation_id, 'call', queryWindow, node.occurrence_id, active && (node.call_count ?? 0) > 0, overview.frontier, null, focus?.occurrenceId === node.id ? focus.callId ?? null : null);
  const common = { reveal: focus?.serial, window, observed: overview.observed_at_us, origin: overview.start_at_us!, trackWidth, selected, paused: overview.control_state === 'paused', onPick, onZoom };
  return <div className="timeline-children">
    {(node.call_count ?? 0) > 0 ? <QueryMessage page={calls} label="calls" /> : null}
    {calls.query.data?.items.map(row => <CallRow key={row.id} row={{ ...row, node_id: node.node_id, ui_ordinal: node.ordinal }} {...common} />)}
    <PageControls page={calls} label="Calls" />
  </div>;
}
function TimelineBody({ overview, ...props }: Props & { overview: TimelineOverview }) {
  const snapshot = useExecutionSnapshot();
  const full = fullWindow(overview)!;
  const [navigation, setNavigation] = useState(() => ({ window: full, whole: true, following: false, history: [] as TimeWindow[] }));
  const committedWindow = navigation.following ? moveWindow(full, full.end - (navigation.window.end - navigation.window.start), navigation.window.end - navigation.window.start) : navigation.whole ? full : moveWindow(full, navigation.window.start, navigation.window.end - navigation.window.start);
  const [rangeRevision,setRangeRevision]=useState(0);
  const [preview, setPreview] = useState<TimeWindow | null>(null);
  const window = preview ?? committedWindow;
  useEffect(() => {
    if (snapshot.refreshing) setNavigation(current => current.whole ? { ...current, window: full, whole: false } : current);
  }, [snapshot.refreshing]);
  const [queryWindow, setQueryWindow] = useState(committedWindow);
  const [expanded, setExpanded] = useState(() => new Set<string>(props.focus && ['call', 'wait'].includes(props.focus.kind) ? [props.focus.occurrenceId] : []));
  const [nodeFilter, setNodeFilter] = useState<string | null>(null);
  const [focusEnabled, setFocusEnabled] = useState(Boolean(props.focus));
  const [appliedFocus, setAppliedFocus] = useState<number | null>(props.focus?.serial ?? null);
  const [showEvents, setShowEvents] = useState(false);
  const [hoverTime, setHoverTime] = useState<number | null>(null);
  const scroll = useRef<HTMLDivElement>(null);
  const axisTrack = useRef<HTMLDivElement>(null);
  const [trackWidth, setTrackWidth] = useState(0);
  const [scrollbarWidth, setScrollbarWidth] = useState(0);
  useLayoutEffect(() => {
    const track = axisTrack.current;
    if (!track) return;
    const measure = () => { setTrackWidth(track.clientWidth); if (scroll.current) setScrollbarWidth(scroll.current.offsetWidth - scroll.current.clientWidth); };
    measure(); const observer = new ResizeObserver(measure); observer.observe(track);
    return () => observer.disconnect();
  }, []);
  const rangeRequestAt = useRef(0);
  useEffect(() => {
    // Reproject cached rows on every frame; fetch newly exposed rows at most
    // five times a second during a drag, then settle on the final exact range.
    const delay = preview ? Math.max(0, 200 - (performance.now() - rangeRequestAt.current)) : 140;
    const timer = setTimeout(() => { rangeRequestAt.current = performance.now(); setQueryWindow(window); }, delay);
    return () => clearTimeout(timer);
  }, [window.start, window.end, Boolean(preview)]);
  // The visible range includes its end; keep that bound stable as a running snapshot grows.
  const requestWindow = useMemo(() => ({ start: queryWindow.start, end: queryWindow.end + 1 }), [queryWindow.start, queryWindow.end]);
  const focused = focusEnabled && appliedFocus === props.focus?.serial ? props.focus : null;
  const canReveal = Boolean(focused && spanGeometry(focused.interval, window, overview.observed_at_us).visible && (!nodeFilter || !focused.nodeId || focused.nodeId === nodeFilter));
  const page = usePage(props.trace.invocation.invocation_id, 'node', requestWindow, null, props.active, overview.frontier, nodeFilter, canReveal ? focused!.occurrenceId : null);
  const counts = useMemo(() => new Map(props.trace.nodes.map(n => [n.node_id, n.occurrences])), [props.trace.nodes]);
  const width = window.end - window.start;
  const total = full.end - full.start;
  const change = (next: TimeWindow, preserveFocus = false) => { setRangeRevision(value=>value+1); if (!preserveFocus) setFocusEnabled(false); setNavigation(current => {
    const bounded = moveWindow(full, next.start, next.end - next.start);
    const previous = committedWindow;
    if (previous.start === bounded.start && previous.end === bounded.end) return current;
    return { window: bounded, whole: bounded.start === full.start && bounded.end === full.end, following: false, history: [...current.history, previous].slice(-20) };
  }); };
  const reset = () => { change(full); setNavigation(current => ({...current,whole:true,following:false})); };
  const follow = () => { setRangeRevision(value=>value+1); setNavigation(current => ({window:committedWindow,whole:false,following:!current.following,history:[...current.history,committedWindow].slice(-20)})); };
  const back = () => { setRangeRevision(value=>value+1); setFocusEnabled(false); setNavigation(current => {
    const previous = current.history.at(-1);
    return previous ? { window: previous, whole: previous.start === full.start && previous.end === full.end, following: false, history: current.history.slice(0, -1) } : current;
  }); };
  useEffect(() => {
    const target = props.focus;
    if (!props.active || !target || appliedFocus === target.serial) return;
    // Locate/expand within the chosen range. Only explicit range controls zoom.
    setAppliedFocus(target.serial);
    setFocusEnabled(true);
    if (target.kind === 'call' || target.kind === 'wait') setExpanded(current => new Set(current).add(target.occurrenceId));
  }, [props.active, props.focus, appliedFocus]);
  const zoom = (factor: number) => change(zoomWindow(full, window, factor));
  const interaction = useRef({ full, window, change });
  interaction.current = { full, window, change };
  useEffect(() => {
    const element = scroll.current;
    if (!element) return;
    const wheel = (event: WheelEvent) => {
      if (!(event.target as Element).closest('.timeline-track') || !props.active) return;
      const bounds = axisTrack.current?.getBoundingClientRect();
      if (!bounds) return;
      const { full: all, window: current, change: commit } = interaction.current;
      if (event.ctrlKey || event.metaKey) {
        event.preventDefault();
        commit(zoomWindow(all, current, Math.exp(Math.max(-1, Math.min(1, event.deltaY * .005))), (event.clientX - bounds.left) / bounds.width));
      } else if (event.shiftKey || Math.abs(event.deltaX) > Math.abs(event.deltaY)) {
        event.preventDefault();
        commit(moveWindow(all, current.start + (event.deltaX || event.deltaY) / bounds.width * (current.end - current.start), current.end - current.start));
      }
    };
    element.addEventListener('wheel', wheel, { passive: false });
    return () => element.removeEventListener('wheel', wheel);
  }, [props.active]);
  const focus = (row: TimelineRow) => change(focusWindow(full, row, overview.observed_at_us));
  const scale = timelineScale(window);
  const zoomed = width < total;
  const updating = page.query.isFetching || window.start !== queryWindow.start || window.end !== queryWindow.end;
  const visibleRows = page.query.data?.items.filter(row => spanGeometry(row, { start: window.start, end: window.end + 1 }, overview.observed_at_us).visible);
  const hoverVisible = hoverTime != null && hoverTime >= window.start && hoverTime <= window.end;
  const eventVisible = !props.replay.enabled && props.selectedEvent && props.selectedEvent.time >= window.start && props.selectedEvent.time <= window.end;
  const replayVisible = props.replay.enabled && props.replay.cursorTime !== null && props.replay.cursorTime >= window.start && props.replay.cursorTime <= window.end;
  const trackStyle = { '--timeline-selected-event': replayVisible ? `${(props.replay.cursorTime! - window.start) / width * 100}%` : eventVisible ? `${(props.selectedEvent!.time - window.start) / width * 100}%` : undefined, '--timeline-hover': hoverVisible ? `${(hoverTime - window.start) / width * 100}%` : undefined } as CSSProperties;
  const pointerTime = (x: number) => {
    const bounds = axisTrack.current?.getBoundingClientRect();
    return bounds ? Math.round(window.start + Math.max(0, Math.min(1, (x - bounds.left) / bounds.width)) * width) : window.start;
  };
  return <div className={`timeline-body ${preview ? 'is-range-dragging' : ''}`} style={{ '--timeline-scrollbar-width': `${scrollbarWidth}px` } as CSSProperties}>
    <header className="timeline-toolbar"><div><strong>Timeline</strong><span className="muted">{overview.counts.node} node runs · {elapsedLabel(total)}</span></div><div className="timeline-range-controls"><button aria-label="Previous time range" disabled={!navigation.history.length} onClick={back}>← Back</button><button className="timeline-reset-range" disabled={!zoomed} onClick={reset}>Full range</button>{!navigation.whole && !navigation.following && full.end > window.end + 1 ? <button className="timeline-new-activity" onClick={follow}>New activity</button> : null}<button className="timeline-follow" aria-pressed={navigation.following} disabled={!props.followingLive} onClick={follow} title="Keep this time span at the latest execution time">Follow latest</button></div></header>
    <TimelineRange full={full} window={committedWindow} bins={overview.node_start_bins} onChange={change} onPreview={setPreview} onReset={reset} replayTime={props.replay.enabled ? props.replay.cursorTime : null} />
    <div className="timeline-list-toolbar">
      <div className="timeline-row-options"><TimelineNodeFilter nodes={props.workflow.definition.nodes.map(node => node.id)} value={nodeFilter} onChange={value => { setNodeFilter(value); setFocusEnabled(false); }} /><button aria-pressed={showEvents} onClick={() => setShowEvents(value => !value)}>Events</button><button aria-pressed={props.replay.enabled} disabled={!props.trace.last_event} onClick={() => { if (props.replay.enabled) props.replay.stop(); else { setShowEvents(true); props.onReplay({ at_us: window.start }); } }}>{props.replay.enabled ? "End replay" : "Replay"}</button><button className="timeline-collapse-all" disabled={!expanded.size} onClick={() => setExpanded(new Set())}>Collapse all</button></div>
      <div className={`timeline-toolbar-track ${props.replay.enabled ? 'timeline-replay-controls' : ''}`}>
        {props.replay.enabled ? <div className="timeline-replay-track"><input type="range" aria-label="Replay time" min={0} max={width} step={1} value={Math.max(0, Math.min(width, (props.replay.cursorTime ?? window.start) - window.start))} onChange={event => props.onReplay({ at_us: window.start + Number(event.target.value) })} /></div> : null}
        <span className={`timeline-update-status ${props.replay.enabled ? 'sr-only' : ''}`} role="status">{updating ? 'Updating…' : `${elapsedLabel(width)} visible`}</span>
      </div>
    </div>
    {focusEnabled && props.focus && (page.location.error || !canReveal) ? <div className="timeline-location-notice" role="status"><span>Selected record is outside the current range or filter.</span><button onClick={() => { change(focusWindow(full, props.focus!.interval, overview.observed_at_us), true); }}>Focus selected interval</button>{nodeFilter ? <button onClick={() => setNodeFilter(null)}>Show all nodes</button> : null}<button aria-label="Dismiss location notice" onClick={() => setFocusEnabled(false)}>×</button></div> : null}
    <div className={`timeline-pinned ${eventVisible || replayVisible ? 'has-selected-event' : ''} ${replayVisible ? 'has-replay-position' : ''}`} style={trackStyle}>
      {showEvents ? <TimelineEvents rangeRevision={rangeRevision} replaying={props.replay.enabled} onReplay={props.onReplay} iid={props.trace.invocation.invocation_id} window={window} full={full} previewing={Boolean(preview)} queryWindow={requestWindow} frontier={overview.frontier} active={props.active} trackWidth={trackWidth} through={props.trace.last_event?.sequence ?? 0} /> : null}
      <div className="timeline-axis timeline-row"><div className="timeline-row-label">Execution</div><div className="timeline-duration">Elapsed</div><div className="timeline-track" ref={axisTrack}>{eventVisible ? <span className="timeline-selected-boundary" style={{ left: `clamp(22px, ${(props.selectedEvent!.time - window.start) / width * 100}%, calc(100% - 22px))` }} title={`Selected Edge evaluation · Event #${props.selectedEvent!.sequence}`} role="img" aria-label={`Selected Edge evaluation, Event ${props.selectedEvent!.sequence}`}>#{props.selectedEvent!.sequence}</span> : null}<span className="timeline-axis-unit">Time since start ({scale.unit})</span>{[0,.25,.5,.75,1].map(ratio => <span key={ratio} className={`timeline-tick ${ratio === 0 ? 'first' : ratio === 1 ? 'last' : 'middle'}`} style={{ left: `${ratio * 100}%` }}><span>{Number(((window.start - full.start + width * ratio) / scale.divisor).toFixed(3))}</span><i /></span>)}</div></div>
    </div>
    <div ref={scroll} className={`timeline-scroll ${hoverVisible ? 'has-time-hover' : ''} ${eventVisible || replayVisible ? 'has-selected-event' : ''} ${replayVisible ? 'has-replay-position' : ''}`} style={trackStyle} aria-busy={updating} tabIndex={0} aria-label="Timeline intervals. Control or Command and scroll to zoom. Escape returns to the full range."
      onKeyDown={event => { if (event.target !== event.currentTarget) return; if (['+', '=', '-', 'ArrowLeft', 'ArrowRight', 'Escape'].includes(event.key)) event.preventDefault(); if (event.key === '+' || event.key === '=') zoom(.5); if (event.key === '-') zoom(2); if (event.key === 'Escape') reset(); if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') change(moveWindow(full, window.start + width / 4 * (event.key === 'ArrowLeft' ? -1 : 1), width)); }}
      onPointerMove={event => setHoverTime((event.target as Element).closest('.timeline-track') ? pointerTime(event.clientX) : null)} onPointerLeave={() => setHoverTime(null)}>
      <QueryMessage page={page} label="Node runs" />
      {visibleRows?.map(row => <div key={row.id}><SpanRow reveal={canReveal ? focused?.serial : undefined} row={{ ...row, ui_ordinal: row.ordinal }} label={`${row.node_id}${(counts.get(row.node_id!) ?? 0) > 1 ? ` (${row.ordinal} of ${counts.get(row.node_id!)})` : ''}`} window={window} origin={full.start} trackWidth={trackWidth} observed={overview.observed_at_us} paused={overview.control_state === 'paused'} selected={props.selected} selectedNode={props.selectedNode} onPick={props.onPick} onZoom={focus} expanded={expanded.has(row.id)} onExpand={(row.call_count ?? 0) > 0 ? () => setExpanded(value => { const next = new Set(value); if (next.has(row.id)) next.delete(row.id); else next.add(row.id); return next; }) : undefined} /><TimelineReveal open={expanded.has(row.id)}><ExpandedOccurrence node={row} window={window} queryWindow={requestWindow} overview={overview} active={props.active && expanded.has(row.id)} focus={canReveal ? focused : null} selected={props.selected} onPick={props.onPick} onZoom={focus} trackWidth={trackWidth} /></TimelineReveal></div>)}
      {overview.untimed_count ? <p className="timeline-message muted">{overview.untimed_count} records have no timestamp and cannot be positioned.</p> : null}
    </div>
    <footer className="timeline-footer"><div className="timeline-color-key"><span><i className="key-node" />Node</span><span><i className="key-call" />Call</span><span><i className="key-wait" />Wait</span></div><span>Drag the overview to focus · Double-click to reset</span><PageControls page={page} label="Occurrences" /></footer>
  </div>;
}
export default function ExecutionTimeline(props: Props) {
  const snapshot = useExecutionSnapshot();
  const query = useQuery({ queryKey: ['execution-timeline', props.trace.invocation.invocation_id, snapshot.epoch], meta: snapshot.meta, placeholderData: keepPreviousData, queryFn: ({ signal }) => getExecutionTimeline(props.trace.invocation.invocation_id, signal, props.nowUs), enabled: props.active && props.ready, refetchInterval: props.followingLive ? 500 : false, staleTime: Infinity, gcTime: 5000, refetchOnWindowFocus: false });
  const overview = useSnapshotMetadata(props.trace.invocation.invocation_id, query.data);
  const hasTimes = overview && fullWindow(overview);
  return <section className="execution-timeline" aria-label="Execution Timeline">
    {!props.ready ? <div className="timeline-message" role="status">{props.loadingStatus}{props.loadError ? <><p>{props.loadError}</p><button onClick={props.onRetry}>Retry</button></> : null}</div> : null}
    {props.ready && query.error ? <div className="timeline-message sidebar-error"><p>{query.error.message}</p><button onClick={() => void query.refetch()}>Retry</button></div> : null}
    {!props.ready ? null : !overview && query.isPending ? <p className="timeline-message" role="status">Loading Timeline…</p> : hasTimes ? <TimelineBody {...props} overview={overview!} /> : !query.error ? <p className="timeline-message muted">No recorded execution times.</p> : null}
  </section>;
}
