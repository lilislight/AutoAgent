import { CardsSkeleton, InlineLoading } from './LoadingState';
import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { getExecutionItems } from "../api";
import type { TraceRow } from "../types";
import { duration, formatTime, StatusBadge } from "./TraceDetails";
import { advanceMotion, motionSettled, placeStack, wheelMotion, STACK_SCENE_TOP, type StackAnchor, type StackMotion } from "../stackMotion";
import { useExecutionSnapshot, useSnapshotMetadata } from './ExecutionSnapshot';
import { stackPositions } from "../stackPositions";
import { stackIndex, stackJump, stackRecords, stepStack } from '../stackSelection';

export type StackTarget = { kind: "node" | "edge"; id: string; total: number; anchor?: StackAnchor };
export const executionRecordKey = (row: TraceRow, kind: StackTarget["kind"]) => kind === "node"
  ? row.occurrence_id! : `${String(row.event_id)}:${String(row.result_index)}`;

export function OccurrenceStack({ invocationId, target, onPick, onClose }: {
  invocationId: string; target: StackTarget; onPick: (row: TraceRow) => void; onClose: () => void;
}) {
  const headingId = useId();
  const snapshot = useExecutionSnapshot();
  const overlay = useRef<HTMLDivElement>(null);
  const front = useRef<HTMLButtonElement>(null);
  const [phase, setPhase] = useState("opening");
  const [position, setPosition] = useState(() => stackPositions.read(invocationId, target.kind, target.id, target.total));
  const [requested, setRequested] = useState<number | null>(null);
  const [status, setStatus] = useState('all');
  const [editing, setEditing] = useState(false), [draft, setDraft] = useState('');
  const cancelEdit = useRef(false);
  const filterPositions = useRef(new Map<string, number>());
  const rememberedPosition = useRef(position);
  const [moving, setMoving] = useState(false);
  const motion = useRef<StackMotion>({ position, target: position });
  const motionFrame = useRef(0);
  const [placement, setPlacement] = useState<ReturnType<typeof placeStack> | null>(null);
  const closing = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const query = useInfiniteQuery({
    queryKey: ["occurrence-stack", invocationId, target.kind, target.id, snapshot.epoch], meta: snapshot.meta,
    queryFn: ({ pageParam, signal }) => getExecutionItems(invocationId, target.kind,
      { [target.kind === "node" ? "node_id" : "edge_id"]: target.id }, pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    staleTime: Infinity, gcTime: 0, refetchOnWindowFocus: false,
  });
  const data = useSnapshotMetadata(JSON.stringify([invocationId, target.kind, target.id]), query.data);
  const allRows = useMemo(() => data?.pages.flatMap(page => page.items) ?? [], [data]);
  const records = useMemo(() => stackRecords(allRows, target.total, target.kind), [allRows, target.total, target.kind]);
  const statuses = useMemo(() => [...new Set(records.map(record => record.status))].sort(), [records]);
  const filtered = useMemo(() => status === 'all' ? records : records.filter(record => record.status === status), [records, status]);
  const rows = useMemo(() => filtered.map(record => record.row), [filtered]);
  const total = status === 'all' ? target.total : rows.length;
  const displayPosition = Math.min(position, Math.max(0, rows.length - 1));
  const frontIndex = stackIndex(displayPosition, rows.length);
  const index = stackIndex(requested ?? position, total);
  if (!closing.current) rememberedPosition.current = status === 'all' ? motion.current.target : filtered[index]?.sourceIndex ?? rememberedPosition.current;
  // Keep exactly one preceding outline, including between settled positions.
  const start = Math.max(0, frontIndex - 1);
  const missing = status === 'all' && position > rows.length - 1;
  const restoring = missing && !query.error && (query.isPending || query.hasNextPage);
  const visible = missing ? [] : rows.slice(start, start + 5);
  useLayoutEffect(() => {
    const element = overlay.current;
    if (!element) return;
    const measure = () => {
      const rect = element.getBoundingClientRect();
      setPlacement(placeStack(target.anchor, rect, target.total));
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [target.anchor, target.total]);
  useEffect(() => {
    // Only lightweight records for this Node/Edge are read. Complete the pages
    // so filters cover every status; payload values remain lazy in the Inspector.
    if (query.hasNextPage && !query.isFetching && !query.error) void query.fetchNextPage();
  }, [data, query.hasNextPage, query.isFetching, query.error, query.fetchNextPage]);
  const drive = () => {
    if (motionFrame.current) return;
    setMoving(true);
    let previous = performance.now();
    const tick = (now: number) => {
      motionFrame.current = 0;
      if (closing.current) return;
      if (overlay.current?.closest("[inert]")) {
        const position = Math.round(motion.current.position);
        motion.current = { position, target: position };
        setPosition(position); setRequested(null); setMoving(false); return;
      }
      motion.current = advanceMotion(motion.current, Math.min(.05, (now - previous) / 1000), total - 1);
      previous = now;
      setPosition(motion.current.position);
      if (motionSettled(motion.current)) { setMoving(false); setRequested(null); }
      else motionFrame.current = requestAnimationFrame(tick);
    };
    motionFrame.current = requestAnimationFrame(tick);
  };
  const close = (row?: TraceRow) => {
    if (closing.current) return;
    closing.current = true;
    cancelAnimationFrame(motionFrame.current); motionFrame.current = 0; setMoving(false);
    if (row) rememberedPosition.current = records.find(record => executionRecordKey(record.row, target.kind) === executionRecordKey(row, target.kind))?.sourceIndex ?? rememberedPosition.current;
    setPhase("closing");
    timer.current = setTimeout(() => row ? onPick(row) : onClose(),
      window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 260);
  };
  const navigate = (targetIndex: number, animate = true) => {
    if (closing.current) return;
    targetIndex = stackIndex(targetIndex, total);
    setRequested(targetIndex);
    motion.current = { position: motion.current.position, target: targetIndex };
    if (!animate || window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      cancelAnimationFrame(motionFrame.current); motionFrame.current = 0;
      motion.current.position = targetIndex; setPosition(targetIndex); setRequested(null); setMoving(false);
    } else drive();
  };
  const nudge = (direction: number) => navigate(stepStack(motion.current.target, direction, total));
  const scroll = (pixels: number) => {
    if (closing.current || !pixels) return;
    cancelAnimationFrame(motionFrame.current); motionFrame.current = 0; setMoving(false); setRequested(null);
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) { nudge(-Math.sign(pixels)); return; }
    motion.current = wheelMotion(motion.current, pixels, total - 1);
    setPosition(motion.current.position);
  };
  const selectStatus = (next: string) => {
    if (closing.current || next === status) return;
    filterPositions.current.set(status, motion.current.target);
    const choices = next === 'all' ? records : records.filter(record => record.status === next);
    const nextTotal = next === 'all' ? target.total : choices.length;
    const nextPosition = stackIndex(filterPositions.current.get(next) ?? 0, nextTotal);
    cancelAnimationFrame(motionFrame.current); motionFrame.current = 0;
    motion.current = { position: nextPosition, target: nextPosition };
    setMoving(false); setRequested(null); setEditing(false); setPosition(nextPosition); setStatus(next);
  };
  const commitJump = () => {
    setEditing(false);
    if (cancelEdit.current) { cancelEdit.current = false; return; }
    const next = stackJump(draft, total);
    if (next !== null) navigate(next, false);
  };
  const actions = useRef({ close, nudge, scroll });
  useLayoutEffect(() => { actions.current = { close, nudge, scroll }; });
  useEffect(() => {
    const previous = document.activeElement;
    const element = overlay.current;
    let secondFrame = 0;
    const frame = requestAnimationFrame(() => { secondFrame = requestAnimationFrame(() => { if (!closing.current) setPhase("open"); }); });
    const keydown = (event: KeyboardEvent) => {
      if (!element || element.closest("[inert]")) return;
      if (event.key === "Escape") {
        if (event.target instanceof HTMLInputElement) return;
        event.preventDefault(); event.stopImmediatePropagation(); actions.current.close();
      } else if (element.contains(document.activeElement)) {
        if ((event.key === "ArrowUp" || event.key === "ArrowDown") && !(event.target instanceof HTMLInputElement)) {
          event.preventDefault(); event.stopImmediatePropagation(); actions.current.nudge(event.key === "ArrowUp" ? 1 : -1);
        } else if (event.key === "Tab") {
          const buttons = Array.from(element.querySelectorAll<HTMLElement>("button:not(:disabled),input:not(:disabled)"));
          const first = buttons[0], last = buttons.at(-1);
          if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
          else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
        }
      }
    };
    const wheel = (event: WheelEvent) => {
      if (event.ctrlKey || element?.closest("[inert]")) return;
      const filters = (event.target as Element).closest('.stack-status-filters');
      if (filters) { event.preventDefault(); filters.scrollLeft += event.deltaY; return; }
      if (event.target instanceof HTMLInputElement) return;
      event.preventDefault(); event.stopPropagation();
      actions.current.scroll(event.deltaY * (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? element?.clientHeight ?? 200 : 1));
    };
    document.addEventListener("keydown", keydown, true);
    element?.addEventListener("wheel", wheel, { passive: false });
    return () => {
      cancelAnimationFrame(frame); cancelAnimationFrame(secondFrame); clearTimeout(timer.current); cancelAnimationFrame(motionFrame.current);
      stackPositions.write(invocationId, target.kind, target.id, target.total, rememberedPosition.current);
      document.removeEventListener("keydown", keydown, true);
      element?.removeEventListener("wheel", wheel);
      if (previous instanceof HTMLElement && previous.isConnected && !previous.closest("[inert]") &&
          (element?.contains(document.activeElement) || document.activeElement === document.body)) previous.focus({ preventScroll: true });
    };
  }, []);
  useEffect(() => {
    const focused = document.activeElement;
    if (phase === "open" && (focused === document.body || !overlay.current?.contains(focused) || focused?.closest('.occurrence-card'))) (front.current ?? overlay.current?.querySelector<HTMLButtonElement>("button"))?.focus({ preventScroll: true });
  }, [frontIndex, phase, rows.length]);
  const placementStyle = placement ? { left: placement.left, top: placement.top, width: placement.width,
    "--stack-origin-x": `${placement.originX}px`, "--stack-origin-y": `${placement.originY}px`,
    "--stack-source-x": placement.sourceScaleX, "--stack-source-y": placement.sourceScaleY,
    "--stack-center-y": `${STACK_SCENE_TOP + placement.frontY + placement.cardHeight / 2}px`,
    "--stack-scene-height": `${placement.sceneHeight}px`, "--stack-card-height": `${placement.cardHeight}px`,
    "--stack-front-y": `${placement.frontY}px`,
  } as CSSProperties : { visibility: "hidden" } as CSSProperties;
  return <div ref={overlay} className={`occurrence-stack-overlay is-${phase}${moving ? " is-moving" : ""}`} role="dialog" aria-modal="false" aria-labelledby={headingId} data-occurrence-stack
    onClick={event => { if (event.target === event.currentTarget) close(); }}>
    <div className="occurrence-stack-content" style={placementStyle}>
      <div className="occurrence-stack-heading"><div><span>{target.kind === "node" ? "NODE OCCURRENCES" : "EDGE EVALUATIONS"}</span><strong id={headingId} title={target.id}>{target.id} <small>×{target.total}</small></strong></div><button className="stack-close" aria-label="Close execution stack" onClick={() => close()}>×</button></div>
      <div className="stack-status-filters" role="group" aria-label="Filter execution status" aria-busy={query.isFetching}>
          <button type="button" aria-pressed={status === 'all'} onClick={() => selectStatus('all')}>All</button>
          {statuses.map(value => <button type="button" key={value} className="stack-status-filter" aria-pressed={status === value} onClick={() => selectStatus(value)}>{value}</button>)}
          {query.isFetchingNextPage ? <InlineLoading label="Loading execution statuses" /> : null}
      </div>
      <div className="occurrence-stack-scene" aria-busy={query.isFetching} onClick={event => { if (event.target === event.currentTarget) close(); }}>
        {query.isPending || restoring ? <div className="stack-message"><CardsSkeleton label="Loading executions" /></div> : null}
        {query.error && !rows.length ? <div className="stack-message"><span>{query.error.message}</span><button className="text-button" onClick={() => void query.refetch()}>Retry</button></div> : null}
        {!query.isPending && !query.error && !rows.length ? <div className="stack-message">{status === 'all' ? 'No recorded executions.' : query.hasNextPage ? 'Checking remaining executions…' : 'No executions match this status.'}</div> : null}
        {visible.map((row, offset) => {
          const rowIndex = start + offset;
          const rank = rowIndex - displayPosition;
          const departing = rowIndex < frontIndex;
          const ordinal = filtered[rowIndex].ordinal;
          const elapsed = target.kind === "node" ? row.started_at_us != null && row.completed_at_us != null ? row.completed_at_us - row.started_at_us : null : (row.duration_ns ?? 0) / 1000;
          const style = { "--stack-y": `${(placement?.frontY ?? 96) - rank * (placement?.spacing ?? 30)}px`, "--stack-z": `${-rank * 24}px`,
            "--stack-scale": 1 - Math.max(0, rank) * 0.035, "--stack-tilt": `${-4 - rank * 2}deg`,
            "--stack-opacity": departing ? .28 * Math.min(1, Math.max(0, 2 + rank)) : 1, pointerEvents: departing ? "none" : undefined,
            zIndex: 20 - offset } as CSSProperties;
          return <button key={executionRecordKey(row, target.kind)} ref={rowIndex === frontIndex ? front : undefined} type="button" style={style}
            className={`occurrence-card ${rowIndex === frontIndex ? "is-front" : ""}${departing ? " is-departing" : ""}`} aria-label={`Inspect ${target.kind === "node" ? "occurrence" : "evaluation"} ${ordinal}`} data-record-key={executionRecordKey(row, target.kind)}
            onClick={() => close({ ...row, ui_ordinal: ordinal })} disabled={phase === "closing" || departing} aria-hidden={departing}>
            <div className="occurrence-card-top"><span>{target.kind === "node" ? "Occurrence" : "Evaluation"} #{ordinal}</span>{target.kind === "node" ? <StatusBadge row={row} /> : <span className={`status-badge status-${row.condition_result ? "completed" : "skipped"}`}>{row.condition_result ? "Selected" : "Skipped"}</span>}</div>
            <strong title={target.id}>{target.id}</strong>
            <code title={row.occurrence_id ?? row.source_occurrence_id}>{row.occurrence_id ?? row.source_occurrence_id}</code>
            <div className="occurrence-card-meta"><span>{formatTime(target.kind === "node" ? row.started_at_us ?? row.ready_at_us : row.occurred_at_us as number)}</span>{target.kind==='node'&&row.call_count!=null?<span>{row.call_count} {row.call_count===1?'call':'calls'}</span>:null}<span>{duration(elapsed)}</span></div>
            {row.error_type ? <div className="occurrence-card-error">{row.error_type}{row.error_code ? ` · ${row.error_code}` : ""}</div> : null}
          </button>;
        })}
      </div>
      {query.isFetchNextPageError ? <div className="occurrence-stack-error" role="alert"><span>{query.error?.message}</span><button className="text-button" onClick={() => void query.fetchNextPage()}>Retry</button></div> : null}
      <div className="occurrence-stack-controls">
        <button aria-label="Older execution" disabled={!total || index >= total - 1 || phase === "closing"} onClick={() => nudge(1)}><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m5 12 5-5 5 5" /></svg></button>
        <label className="stack-position"><input type="text" inputMode="numeric" aria-label={`Execution number${status !== 'all' ? ' in filtered results' : ''}, 1 to ${total}`} aria-invalid={editing && stackJump(draft,total) === null || undefined}
          value={editing ? draft : total ? String(total - index) : ''} style={{ width:`${Math.max(2,String(total).length)}ch` }} disabled={!total || phase === 'closing'}
          onFocus={event => { setDraft(total ? String(total - index) : ''); setEditing(true); event.currentTarget.select(); }} onChange={event => setDraft(event.target.value)} onBlur={commitJump}
          onKeyDown={event => { if (event.key === 'Enter' || event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); cancelEdit.current = event.key === 'Escape'; event.currentTarget.blur(); } }}/><span>of {total}{status !== 'all' && query.hasNextPage ? '+' : ''}</span><span className="sr-only" role="status">{total ? `${total-index} of ${total}` : 'No executions'}</span></label>
        <button aria-label="Newer execution" disabled={!total || !index || phase === "closing"} onClick={() => nudge(-1)}><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m5 8 5 5 5-5" /></svg></button>
      </div>
    </div>
  </div>;
}
