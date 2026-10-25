import { useEffect, useId, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getExecutionFamily, getExecutionItems } from '../api';
import type { ExecutionTrace, TracePage, TraceRow } from '../types';
import { traceQuery } from '../traceClient';
import type { HandoffCue } from './useFamilyHandoff';
import { duration, StatusBadge } from './TraceDetails';
import { useSnapshotMetadata, useExecutionSnapshot } from './ExecutionSnapshot';
import { WorkflowIcon } from './WorkflowDirectory';
import { useLayoutMotion } from './useLayoutMotion';
import { useFamilyPresence } from './useFamilyPresence';

export function ExecutionFamilyBar({ trace, visibleTrace, replaySequence, active, onOpen, cue }: {
  trace: ExecutionTrace; visibleTrace: ExecutionTrace; replaySequence?: number;
  active: boolean; onOpen: (row: TraceRow) => void; cue:HandoffCue|null;
}) {
  const current = trace.invocation;
  const {meta} = useExecutionSnapshot();
  const available = Boolean(current.parent_invocation_id || trace.child_count);
  const presence = useFamilyPresence();
  const { expanded: openPanel, reveal, dismiss } = presence;
  useEffect(()=>{if(!active){presence.pointer(false);presence.focus(false);}},[active]);
  const panelId = useId();
  const familyQuery = useQuery({ queryKey: ['execution-family', current.invocation_id], meta,
    queryFn: ({ signal }) => getExecutionFamily(current.invocation_id, signal), enabled: active && available,
    placeholderData: (previous, query) => query?.queryKey[1] === current.invocation_id ? previous : undefined,
    staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  const family = { ...familyQuery, data: useSnapshotMetadata(current.invocation_id, familyQuery.data) };
  const ancestors = family.data?.ancestors ?? [];
  const parent = ancestors.at(-1);
  const [scopeChoice, setScopeChoice] = useState<{ iid: string; children: boolean } | null>(null);
  useEffect(()=>{if(cue)setScopeChoice({iid:current.invocation_id,children:true});},[cue?.key,current.invocation_id]);
  const showChildren = Boolean(cue) || !parent || scopeChoice?.iid === current.invocation_id && scopeChoice.children;
  const owner = showChildren ? current : parent;
  const localReplay=replaySequence!==undefined && owner?.invocation_id===current.invocation_id;
  const scope = `${owner?.invocation_id}:${localReplay?'replay':'live'}`;
  const [position, setPosition] = useState({ scope, cursors: [null] as (string | null)[], index: 0 });
  const savedPage = position.scope === scope ? position : { scope, cursors: [null], index: 0 };
  // A backward Replay seek may predate this page: return to the first bounded page.
  const page = replaySequence !== undefined && owner?.invocation_id === current.invocation_id && visibleTrace.child_count <= savedPage.index * 20
    ? { scope, cursors: [null], index: 0 } : savedPage;
  const childrenQuery = useQuery({ queryKey: ['family-children', owner?.invocation_id, page.cursors[page.index], localReplay?replaySequence:'asc'], meta,
    queryFn: ({ signal }) => localReplay?traceQuery<TracePage>(current.invocation_id,'family-children',{through:replaySequence,cursor:page.cursors[page.index]},signal):getExecutionItems(owner!.invocation_id, 'child', { order: 'asc' }, page.cursors[page.index], signal),
    placeholderData: (previous, query) => query?.queryKey[1] === owner?.invocation_id && query.queryKey[2] === page.cursors[page.index] ? previous : undefined,
    enabled: active && available && openPanel && Boolean(owner) && Boolean(family.data), staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  const childData = useSnapshotMetadata(JSON.stringify([owner?.invocation_id, page.cursors[page.index],localReplay?replaySequence:'live']), childrenQuery.data);
  const children = { ...childrenQuery, data: childData, isPending: childrenQuery.isPending && !childData };
  const root = family.data?.root;
  const rows = (children.data?.items ?? []).filter(row => replaySequence === undefined || owner?.invocation_id !== current.invocation_id || typeof row.created_sequence === 'number' && row.created_sequence <= replaySequence);
  rows.sort((a, b) => Number(a.created_sequence ?? 0) - Number(b.created_sequence ?? 0) || Number(a.unit_index ?? 0) - Number(b.unit_index ?? 0) || a.invocation_id.localeCompare(b.invocation_id));
  // A selected Child stays reachable even when it is outside the loaded sibling page.
  if (owner && owner.invocation_id !== current.invocation_id && !rows.some(row => row.invocation_id === current.invocation_id)) rows.push(current);
  const cards = [root && root.invocation_id !== owner?.invocation_id ? root : null, owner, ...rows].filter((row): row is TraceRow => Boolean(row));
  const strip = useRef<HTMLDivElement>(null);
  const scrolledSelection = useRef<string|null>(null);
  const motionKey = cards.map(row => row.invocation_id).join(':');
  useLayoutMotion(strip, '[data-motion-id]', motionKey, true, openPanel);
  useEffect(() => {
    const element = strip.current;
    if (!element) return;
    let frame = 0;
    let target = element.scrollLeft;
    const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
    const wheel = (event: WheelEvent) => {
      if (event.ctrlKey || element.scrollWidth <= element.clientWidth) return;
      event.preventDefault();
      const delta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
      target = Math.max(0, Math.min(element.scrollWidth - element.clientWidth, (frame ? target : element.scrollLeft) + delta * (event.deltaMode === 1 ? 18 : event.deltaMode === 2 ? element.clientWidth : 1)));
      cancelAnimationFrame(frame);
      const start = element.scrollLeft, at = performance.now();
      const tick = (time: number) => { const t = Math.min(1, (time - at) / 220); element.scrollLeft = start + (target - start) * (1 - (1 - t) ** 3); frame = t < 1 ? requestAnimationFrame(tick) : 0; };
      if (reduced) { element.scrollLeft = target; frame = 0; } else frame = requestAnimationFrame(tick);
    };
    element.addEventListener('wheel', wheel, { passive: false });
    return () => { element.removeEventListener('wheel', wheel); cancelAnimationFrame(frame); };
  }, [motionKey, openPanel, available, visibleTrace.child_count]);
  useEffect(() => {
    const element = strip.current, selected = element?.querySelector<HTMLElement>('[aria-current="true"]');
    if (!element || !selected || !openPanel || scrolledSelection.current===current.invocation_id) return;
    scrolledSelection.current=current.invocation_id;
    // Opening a handoff or reopening the same family must preserve manual scrolling.
    if(cue)return;
    const left=selected.offsetLeft,right=left+selected.offsetWidth;
    if (left < element.scrollLeft || right > element.scrollLeft+element.clientWidth) element.scrollTo({ left:left-(element.clientWidth-selected.offsetWidth)/2, behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' });
  }, [current.invocation_id, motionKey, openPanel]);
  if (!available || !current.parent_invocation_id && visibleTrace.child_count === 0) return null;
  // No surrounding panel, and no Root-only navigation while child metadata loads.
  const open = (row: TraceRow) => { if (row.invocation_id !== current.invocation_id) onOpen(row); };
  return <div className={`execution-family-bar ${openPanel ? 'is-expanded' : ''}`} aria-label="Execution family" onPointerEnter={()=>presence.pointer(true)} onPointerLeave={()=>presence.pointer(false)} onFocusCapture={()=>presence.focus(true)} onBlurCapture={event => { if (!event.currentTarget.contains(event.relatedTarget as Node | null)) presence.focus(false); }} onKeyDown={event => { if (event.key === 'Escape') { dismiss(); event.stopPropagation(); } }}>
    <div id={panelId} className="family-panel-content" aria-hidden={!openPanel} inert={!openPanel}>
      {familyQuery.isPending || children.isPending ? <div className="family-message" role="status">Loading execution family…</div> : null}
      {family.error || children.error ? <div className="family-message sidebar-error">{(family.error ?? children.error)?.message}<button onClick={() => { void family.refetch(); void children.refetch(); }}>Retry</button></div> : null}
      {family.data?.ancestry_complete === false ? <p className="family-message">Part of the parent history is unavailable.</p> : null}
      {parent && trace.child_count > 0 ? <div className="family-scope"><button aria-pressed={!showChildren} onClick={() => setScopeChoice({ iid: current.invocation_id, children: false })}>Siblings</button><button aria-pressed={showChildren} onClick={() => setScopeChoice({ iid: current.invocation_id, children: true })}>Children</button></div> : null}
      <div ref={strip} className="family-card-strip" role="group" aria-label={showChildren ? 'Child executions' : 'Sibling executions'}>
        {cards.map(row => <FamilyCard key={row.invocation_id} row={row.invocation_id === current.invocation_id ? visibleTrace.invocation : row} label={row.invocation_id === (root?.invocation_id??current.root_invocation_id) ? 'Root' : row.invocation_id === owner?.invocation_id ? 'Parent' : `Child ${Number(row.unit_index ?? 0) + 1}`} selected={row.invocation_id === current.invocation_id} replay={replaySequence !== undefined} onPick={open} />)}
      </div>
      {page.index || children.data?.next_cursor ? <nav className="family-pagination" aria-label="Family executions pagination"><button disabled={!page.index || children.isFetching} onClick={() => setPosition({ ...page, index: page.index - 1 })}>Previous</button><span>Page {page.index + 1}</span><button disabled={!children.data?.next_cursor || children.isFetching} onClick={() => setPosition({ scope, cursors: [...page.cursors.slice(0, page.index + 1), children.data!.next_cursor], index: page.index + 1 })}>Next</button></nav> : null}
    </div>
    <div className="family-trigger" role="button" tabIndex={0} aria-controls={panelId} aria-expanded={openPanel} aria-label="Execution family" title="Hover to browse related executions" onPointerDown={event => { if (event.pointerType === 'touch') reveal(); }} onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); reveal(); } }}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true"><path d="M3 3h4v4H3ZM9 9h4v4H9ZM5 7v4h4" /></svg><span className="family-trigger-line" /></div>
  </div>;
}

function FamilyCard({ row, label, selected, replay, onPick }: { row: TraceRow; label: string; selected: boolean; replay: boolean; onPick: (row: TraceRow) => void }) {
  const elapsed = row.started_at_us != null && row.completed_at_us != null ? row.completed_at_us - row.started_at_us : null;
  return <button data-motion-id={row.invocation_id} data-family-invocation={row.invocation_id} className={`family-card ${selected ? 'is-selected' : ''}`} aria-current={selected ? 'true' : undefined} title={`${row.workflow_id}\n${row.parent_occurrence_id ?? 'Root execution'}\n${row.invocation_id}`} onClick={() => onPick(row)}><span className="family-card-top"><WorkflowIcon /><span>{label}</span>{!replay && row.status ? <StatusBadge row={row} /> : null}</span><strong>{row.workflow_id}</strong><span className="family-card-bottom"><code>{row.invocation_id.slice(0, 8)}</code>{!replay && row.started_at_us!=null ? <span>{duration(elapsed)}</span> : null}</span></button>;
}
