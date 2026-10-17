import { lazy, Suspense, useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { useQuery } from '@tanstack/react-query';
import { parentRecord, recordMatches, selectedInterval, selectRecord, selectResponseNode } from '../executionSelection';
import { getExecutionRecord } from '../api';
import { traceDataset } from '../traceClient';
import { useTraceProgress } from './useTraceProgress';
import { useLiveTrace, LiveTraceStatus } from './useLiveTrace';
import { useExecutionActions } from './ExecutionActions';
import type { AcceptedExecution } from '../types';
import type { ExecutionTrace, ExecutionView, Selection, TimelineFocus, InspectorTab, ExecutionRecordSelection, TimelineRow, TraceRow, WorkflowGraphData } from "../types";
import type { Theme } from "../theme";
import { FlowLoadingSurface } from "./FlowLoadingBoundary";
import { ExecutionInspector } from "./ExecutionInspector";
import { ExecutionNavigationContext } from './ExecutionNavigation';
import { ExecutionSnapshotProvider, ExecutionSnapshotStatus, useExecutionSnapshot } from './ExecutionSnapshot';
import { GraphReplayStatus, useExecutionReplay } from './ExecutionReplay';
import { InspectorPresence } from './InspectorFrame';
import { useObservationClock } from './useObservationClock';
import type { ObservationClock } from '../liveObservation';
import { duration, StatusBadge } from "./TraceDetails";
import { OccurrenceStack, type StackTarget } from "./OccurrenceStack";
import { ExecutionFamilyBar } from './ExecutionFamilyBar';
import { PendingActionCards } from './PendingActionCards';
import { useExecutionLayout, setExecutionLayout, persistExecutionLayout } from './useExecutionLayout';
import { graphRatioAt } from '../executionLayout';
import { useViewMotion } from './useViewMotion';
import type { StackAnchor } from "../stackMotion";
const WorkflowGraph = lazy(() => import("./WorkflowGraph"));
const ExecutionTimeline = lazy(() => import('./ExecutionTimeline'));

export function ExecutionWorkspace(props: Omit<Parameters<typeof ExecutionWorkspaceBody>[0], 'metadataReady' | 'metadataStatus' | 'metadataError' | 'onRetryMetadata' | 'onApplied'> & { onRefresh: () => Promise<void> }) {
  const dataset = useQuery({ queryKey: ['trace-dataset', props.trace.invocation.invocation_id, props.trace.source_id, props.trace.frontier, props.trace.last_event?.event_id, props.trace.invocation.status],
    queryFn: ({ signal }) => traceDataset(props.trace, signal),
    placeholderData: previous => previous?.invocation.invocation_id===props.trace.invocation.invocation_id && previous.source_id===props.trace.source_id ? previous : undefined,
    enabled: props.active !== false, staleTime: Infinity, gcTime: 60_000, retry: false, refetchOnWindowFocus: false });
  const progress = useTraceProgress(props.trace.invocation.invocation_id);
  const [applied,setApplied]=useState<{base:ExecutionTrace|undefined;trace:ExecutionTrace}|null>(null);
  const onApplied=useCallback((trace:ExecutionTrace)=>setApplied(current=>current && current.base===dataset.data && current.trace===trace?current:{base:dataset.data,trace}),[dataset.data]);
  const trace = applied && applied.base===dataset.data ? applied.trace : dataset.data ?? props.trace;
  useEffect(() => { if(trace.invocation.workflow_revision_id !== props.workflow.workflow_revision_id)void props.onRefresh(); }, [trace.invocation.workflow_revision_id,props.workflow.workflow_revision_id,props.onRefresh]);
  return <ExecutionSnapshotProvider trace={trace} preparing={dataset.isFetching} onRefresh={props.onRefresh}><ExecutionWorkspaceBody {...props} trace={trace} onApplied={onApplied} metadataReady={Boolean(dataset.data)} metadataStatus={dataset.error ? 'Trace data could not be loaded.' : progress} metadataError={dataset.error?.message} onRetryMetadata={() => void dataset.refetch()} /></ExecutionSnapshotProvider>;
}
function ExecutionWorkspaceBody({ trace, workflow, selection, onSelect, onOpenExecution, view, onChangeView, theme, active = true, metadataReady, metadataStatus, metadataError, onRetryMetadata, onApplied, onRunAccepted }: {
  onRunAccepted:(receipt:AcceptedExecution)=>void;
  onApplied: (trace:ExecutionTrace) => void;
  metadataReady: boolean; metadataStatus: string; metadataError?: string; onRetryMetadata: () => void;
  trace: ExecutionTrace; workflow: WorkflowGraphData; selection: Selection;
  onSelect: (value: Selection) => void; onOpenExecution: (row: TraceRow, selection?: Selection) => void; view: ExecutionView; onChangeView: (view: ExecutionView) => void; theme: Theme; active?: boolean;
}) {
  const inv = trace.invocation;
  const snapshot = useExecutionSnapshot();
  const replay = useExecutionReplay(trace, active);
  const live = useLiveTrace(trace, metadataReady, active, replay.enabled || snapshot.refreshing, onApplied);
  const [actionNavigationError, setActionNavigationError] = useState<string | null>(null);
  const lastWaitFocus = useRef<string | null>(null);
  const openResponse = async (wait: import('../types').PendingResponse) => {
    if (replay.enabled) return;
    setActionNavigationError(null);
    try {
      const row = await getExecutionRecord(wait.invocation_id, 'wait', wait.wait_id, new AbortController().signal);
      const selected = selectResponseNode(row);
      if (!selected) throw new Error('This Wait has no recorded Node.');
      if (wait.invocation_id === inv.invocation_id) {
        lastWaitFocus.current = `${inv.invocation_id}:${wait.wait_id}`;
        setStack(null); setHoldInspector(false); onSelect(selected); setGraphFocus(current => current + 1);
      }
      else onOpenExecution(row, selected);
    } catch (failure) { setActionNavigationError((failure as Error).message); }
  };
  const actions=useExecutionActions(trace,active && metadataReady && live.visible,replay.enabled,()=>{
    onSelect({kind:'invocation',id:inv.invocation_id,section:'Actions'});
  }, () => replay.enabled ? Promise.resolve() : live.observing ? live.refresh() : snapshot.refresh(), live.observing);
  const autoSplitReplay = useRef(false);
  const endReplay = () => { replay.stop(); if (autoSplitReplay.current && view === 'split') onChangeView('timeline'); autoSplitReplay.current = false; };
  const replayControls = { ...replay, stop: endReplay };
  const graphTrace = replay.frame ?? trace;
  const seekReplay = (position: { sequence?: number; at_us?: number }) => {
    replay.seek(position);
    if (!replay.enabled && view === 'timeline') { autoSplitReplay.current = true; onChangeView('split'); }
    setStack(null); setHoldInspector(false);
  };
  const [timelineFocus, setTimelineFocus] = useState<TimelineFocus | null>(null);
  const [graphFocus, setGraphFocus] = useState(0);
  useEffect(() => {
    const waitId = selection?.record?.section === 'Actions' ? selection.record.waitId : undefined;
    if (!active || replay.enabled || !waitId) return;
    const key = `${inv.invocation_id}:${waitId}`;
    if (lastWaitFocus.current !== key) { lastWaitFocus.current = key; setGraphFocus(current => current + 1); }
    if (view === 'timeline') onChangeView('split');
  }, [active, replay.enabled, selection?.record?.waitId, selection?.record?.section, inv.invocation_id, view]);
  const timelineFocusSerial = useRef(0);
  const [timelineVisited, setTimelineVisited] = useState(view !== 'graph');
  const panes = useRef<HTMLDivElement>(null);
  const dragging = useRef<number | null>(null);
  const [resizing, setResizing] = useState(false);
  const layout = useExecutionLayout();
  useViewMotion(panes, `${view}:${layout.orientation}`, active);
  const setSplitRatio = (value: number) => setExecutionLayout({ ratio: value }, false);
  const [splitBounds, setSplitBounds] = useState({ min: .2, max: .8, vertical: false });
  useLayoutEffect(() => {
    const area = panes.current;
    if (!area || view !== 'split') return;
    const measure = () => {
      const vertical = layout.orientation === 'vertical';
      const size = Math.max(1, (vertical ? area.clientHeight : area.clientWidth) - 7);
      const min = Math.min(.45, Math.max(.2, (vertical ? 160 : 180) / size));
      const max = Math.max(.55, Math.min(.8, 1 - (vertical ? 200 : 260) / size));
      setSplitBounds(current => current.min === min && current.max === max && current.vertical === vertical ? current : { min, max, vertical });

    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(area);
    return () => observer.disconnect();
  }, [view, layout.orientation]);
  const splitRatio = Math.max(splitBounds.min, Math.min(splitBounds.max, layout.ratio));
  const resize = (clientX: number, clientY: number) => {
    const bounds = panes.current?.getBoundingClientRect();
    if (!bounds) return;
    const size = (splitBounds.vertical ? bounds.height : bounds.width) - 7;
    if (size <= 0) return;
    const position = (splitBounds.vertical ? clientY - bounds.top : clientX - bounds.left) - 3.5;
    setSplitRatio(graphRatioAt(position, size, splitBounds.min, splitBounds.max));
  };
  const stopResize = () => { if (dragging.current !== null) persistExecutionLayout(); dragging.current = null; setResizing(false); };
  const [stack, setStack] = useState<StackTarget | null>(null);
  const stackTotal = stack?.kind === 'node' ? trace.nodes.find(row => row.node_id === stack.id)?.occurrences
    : stack?.kind === 'edge' ? trace.edges.find(row => row.edge_id === stack.id)?.evaluations : undefined;
  const [holdInspector, setHoldInspector] = useState(false);
  const choose = (value: Selection, anchor?: StackAnchor) => {
    setTimelineFocus(null);
    const total = value?.kind === "node" ? graphTrace.nodes.find(row => row.node_id === value.id)?.occurrences
      : value?.kind === "edge" ? graphTrace.edges.find(row => row.edge_id === value.id)?.evaluations : 0;
    if (!replay.enabled && value && (value.kind === "node" || value.kind === "edge") && (total ?? 0) > 1) {
      setHoldInspector(Boolean(selection) || holdInspector);
      setStack({ kind: value.kind, id: value.id, total: total!, anchor });
      onSelect(null);
    } else {
      setStack(null); setHoldInspector(false);
      const occurrence = value?.kind === 'node' ? graphTrace.nodes.find(row => row.node_id === value.id)?.occurrence_id : null;
      onSelect(value && occurrence ? { ...value, record: { kind: 'node', id: occurrence } } : value);
    }
  };
  const target = selection?.record;
  const detail = useQuery({ queryKey: ['execution-record', inv.invocation_id, target?.kind, target?.id, snapshot.epoch], meta: snapshot.meta,
    queryFn: ({ signal }) => getExecutionRecord(inv.invocation_id, target!.kind, target!.id, signal),
    placeholderData: previous => selection && previous?.invocation_id === inv.invocation_id && recordMatches(selection, previous) ? previous : undefined,
    enabled: active && Boolean(target), staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  const lastRecord = useRef<TraceRow | undefined>(undefined);
  if (selection && detail.data && !detail.isPlaceholderData && recordMatches(selection, detail.data)) lastRecord.current = detail.data;
  const fallback = selection && lastRecord.current?.invocation_id === inv.invocation_id && recordMatches(selection, lastRecord.current) ? lastRecord.current : undefined;
  const selectedRecord = selection && detail.data && recordMatches(selection, detail.data) ? detail.data : fallback;
  const mismatch = Boolean(target && detail.data && !selectedRecord);
  // Locating a row is an explicit action; ordinary selection never navigates.
  const revealTimeline = () => {
    if (!target || !selectedRecord) return;
    const occurrenceId = selectedRecord.occurrence_id ?? selectedRecord.source_occurrence_id;
    const interval = selectedInterval(target.kind, selectedRecord);
    if (!occurrenceId || !interval) return;
    setTimelineVisited(true);
    setTimelineFocus({ serial: ++timelineFocusSerial.current, occurrenceId,
      kind: target.kind, nodeId: selectedRecord.node_id, interval, callId: target.kind === 'call' ? selectedRecord.call_id : target.kind === 'wait' ? selectedRecord.wait_id : undefined });
    if (view === 'graph') onChangeView('timeline');
  };
  const changeView = (mode: ExecutionView) => {
    autoSplitReplay.current = false;
    onChangeView(mode);
    if (mode !== 'graph') setTimelineVisited(true);
    setStack(null); setHoldInspector(false);
  };
  useEffect(() => { if (view !== 'graph') setTimelineVisited(true); }, [view]);
  const pickedRecord = (kind: ExecutionRecordSelection['kind'], row: TraceRow, section?: InspectorTab) => {
    const value = selectRecord(kind, row, section);
    if (value) { setTimelineFocus(null); setStack(null); setHoldInspector(false); onSelect(value); }
  };
  const pickTimeline = (row: TimelineRow) => pickedRecord(row.kind, row);
  const returnToParent = () => { setTimelineFocus(null); if (selection && selectedRecord) onSelect(parentRecord(selection, selectedRecord)); };
  const inspectorKind = target?.kind === 'node' ? 'node-execution' : target?.kind === 'edge' ? 'edge-execution' : target?.kind === 'wait' ? 'system-command' : selectedRecord?.operator_id?.startsWith('system_command:') ? 'system-command' : 'call';
  const section = (kind: string, value: InspectorTab) => {
    if (target && selection && kind === inspectorKind) onSelect({ ...selection, record: { ...target, section: value } });
    else if (selection?.kind === 'invocation' && kind === 'invocation') onSelect({ ...selection, section: value });
  };
  return <ExecutionNavigationContext.Provider value={{ active, invocationId:inv.invocation_id, replaying:replay.enabled, paused:actions.paused, onRunAccepted, sendAction:actions.send, actionBusy:actions.busy, actionError:actions.error, actionErrors:actions.fieldErrors, clearActionError:actions.clearError, clock:live.clock, respondToWait:row=>void openResponse({invocation_id:row.invocation_id,wait_id:String(row.wait_id),occurrence_id:String(row.occurrence_id),created_at_us:Number(row.created_at_us ?? 0)}), canRespond:actions.canRespond, pendingResponses:actions.pendingResponses, replaySequence: replay.enabled ? replay.marker?.sequence : undefined, openChild: onOpenExecution, pickRecord: pickedRecord, exact: Boolean(selectedRecord), inspectorKind, setSection: section, showTimeline: selectedRecord?.status !== 'skipped' ? () => revealTimeline() : undefined, showGraph: () => { if (view === 'timeline') onChangeView('graph'); setGraphFocus(current => current + 1); } }}><div className="execution-workspace"><header className="execution-header">
    <div className="execution-heading"><button className="execution-title" title="Inspect invocation" onClick={() => choose({ kind: 'invocation', id: inv.invocation_id })}>{inv.workflow_id}</button><span className="execution-revision" title={workflow.workflow_revision_id}><span>v{workflow.workflow_version}</span><code>{workflow.definition_hash.slice(0, 8)}</code></span><StatusBadge row={inv} /><ExecutionElapsed row={inv} clock={live.clock} active={active && !replay.enabled} observed={trace.observed_at_us} /></div>
    {!metadataReady ? <span className="trace-loading-status" role="status" title={metadataError}>{metadataStatus}{metadataError ? <button onClick={onRetryMetadata}>Retry</button> : null}</span> : null}{actions.toolbar}<LiveTraceStatus live={live} /><ExecutionSnapshotStatus compact trace={trace} active={active && !replay.enabled && !live.updating} /><nav className="execution-view-switch" aria-label="Execution view">{(['graph', 'timeline'] as const).map(mode => <button key={mode} aria-pressed={view === mode} onClick={() => changeView(mode)}>{mode === 'graph' ? 'Graph' : 'Timeline'}</button>)}
      <div className={`split-choice ${view === 'split' ? 'is-active' : ''}`}>
        {view === 'split' ? <button className="split-orientation-switch" role="switch" aria-checked={layout.orientation === 'vertical'} aria-label="Vertical Split" title={layout.orientation === 'vertical' ? 'Top and bottom · Switch to side by side' : 'Side by side · Switch to top and bottom'} onClick={() => setExecutionLayout({ orientation: layout.orientation === 'horizontal' ? 'vertical' : 'horizontal' })}>
          <span className="split-switch-thumb" /><svg className="split-horizontal-icon" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.3" aria-hidden="true"><rect x="2" y="3" width="14" height="12" rx="2" /><path d="M9 3v12" /></svg><svg className="split-vertical-icon" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.3" aria-hidden="true"><rect x="2" y="3" width="14" height="12" rx="2" /><path d="M2 9h14" /></svg>
        </button> : <button className="split-view-button" aria-pressed={false} onClick={() => changeView('split')}>Split</button>}
      </div>
    </nav>
  </header>
    <FlowLoadingSurface><div ref={panes} className={`execution-view-area view-${view} split-${layout.orientation} ${resizing ? 'is-resizing' : ''}`}>
      <div data-pane="graph" data-motion-id="graph" className="execution-graph-pane" style={view === 'split' ? { flex: splitRatio, order: 0 } : undefined} aria-hidden={view === 'timeline'} inert={view === 'timeline'}><Suspense fallback={null}><WorkflowGraph workflow={workflow} selection={selection} onSelect={choose} theme={theme} trace={graphTrace} selectedRecord={replay.enabled ? undefined : selectedRecord} focusRequest={graphFocus} /></Suspense><GraphReplayStatus replay={replayControls} />
        {active && !replay.enabled ? <PendingActionCards waits={actions.pendingResponses} invocationId={inv.invocation_id} selected={selection?.record?.waitId} onPick={wait => void openResponse(wait)} /> : null}
        {active && actionNavigationError && !replay.enabled ? <p className="pending-action-error" role="alert">{actionNavigationError}<button type="button" onClick={()=>setActionNavigationError(null)}>×</button></p> : null}
      </div>
      {view === 'split' ? <div className="execution-split-divider" style={{ order: 1 }} role="separator" tabIndex={0} aria-label="Resize Graph and Timeline" aria-orientation={splitBounds.vertical ? 'horizontal' : 'vertical'} aria-valuemin={Math.round(splitBounds.min * 100)} aria-valuemax={Math.round(splitBounds.max * 100)} aria-valuenow={Math.round(splitRatio * 100)} aria-valuetext={`Graph ${Math.round(splitRatio * 100)}%, Timeline ${Math.round((1 - splitRatio) * 100)}%`}
        onPointerDown={event => { if (event.button !== 0) return; dragging.current = event.pointerId; setResizing(true); event.currentTarget.setPointerCapture(event.pointerId); event.preventDefault(); }}
        onPointerMove={event => { if (dragging.current === event.pointerId) resize(event.clientX, event.clientY); }}
        onPointerUp={event => { stopResize(); if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId); }}
        onPointerCancel={stopResize} onLostPointerCapture={stopResize}
        onKeyDown={event => {
          const decrease = event.key === (splitBounds.vertical ? 'ArrowUp' : 'ArrowLeft');
          const increase = event.key === (splitBounds.vertical ? 'ArrowDown' : 'ArrowRight');
          if (!decrease && !increase && event.key !== 'Home' && event.key !== 'End') return;
          event.preventDefault();
          setSplitRatio(event.key === 'Home' ? splitBounds.min : event.key === 'End' ? splitBounds.max : Math.max(splitBounds.min, Math.min(splitBounds.max, splitRatio + (decrease ? -.025 : .025)))); persistExecutionLayout();
        }}><span /></div> : null}
      {timelineVisited ? <div data-pane="timeline" data-motion-id="timeline" className="execution-timeline-pane" style={view === 'split' ? { flex: 1 - splitRatio, order: 2 } : undefined} aria-hidden={view === 'graph'} inert={view === 'graph'}><Suspense fallback={<p className="timeline-message" role="status">Loading Timeline…</p>}><ExecutionTimeline clock={live.clock} observing={!replay.enabled && live.observing} ready={metadataReady} loadingStatus={metadataStatus} loadError={metadataError} onRetry={onRetryMetadata} replay={replayControls} onReplay={seekReplay} trace={trace} workflow={workflow} active={active && view !== 'graph'} onPick={pickTimeline} selected={target?.kind === 'edge' ? selectedRecord?.source_occurrence_id ? `node:${selectedRecord.source_occurrence_id}` : null : target ? `${target.kind}:${target.id}` : null} selectedEvent={target?.kind === 'edge' && selectedRecord ? { id: String(selectedRecord.event_id), time: selectedRecord.occurred_at_us as number, sequence: selectedRecord.sequence! } : null} focus={timelineFocus} selectedNode={!target && selection?.kind === 'node' ? selection.id : null} /></Suspense></div> : null}
    </div>
      {stack ? <OccurrenceStack key={`${stack.kind}:${stack.id}`} invocationId={inv.invocation_id} target={{ ...stack, total: stackTotal ?? stack.total }} onClose={() => { setStack(null); setHoldInspector(false); }} onPick={row => pickedRecord(stack.kind, { ...row, ...(stack.kind === 'node' ? { node_id: stack.id } : { edge_id: stack.id }) })} /> : null}
      <InspectorPresence open={Boolean(selection) || holdInspector} hidden={!active || Boolean(stack)}>{selection && !stack ? target && !selectedRecord ? <aside className="inspector"><button className="timeline-detail-close" aria-label="Close Inspector" onClick={() => choose(null)}>×</button><p role="status">{mismatch ? 'This record belongs to a different Graph element.' : detail.error ? detail.error.message : 'Loading execution details…'}</p>{detail.error ? <button onClick={() => void detail.refetch()}>Retry</button> : null}</aside> : <ExecutionInspector key={`${inv.invocation_id}:${selection.kind}:${selection.id}:${target?.kind ?? ''}:${target?.id ?? ''}`} trace={trace} workflow={workflow} selection={selection} record={selectedRecord} onSelect={choose} onReselect={() => choose(selection)} onBack={returnToParent} onClose={() => choose(null)} /> : null}</InspectorPresence>
    {actions.confirmation}
    <ExecutionFamilyBar trace={trace} visibleTrace={graphTrace} replaySequence={replay.enabled ? replay.marker?.sequence ?? 0 : undefined} active={active} onOpen={onOpenExecution} />
    </FlowLoadingSurface></div></ExecutionNavigationContext.Provider>;
}

function ExecutionElapsed({ row, clock, active, observed }: { row:TraceRow; clock:ObservationClock; active:boolean; observed:number }) {
  const now=useObservationClock(clock,active && row.completed_at_us==null,observed);
  const elapsed=row.started_at_us==null?null:Math.max(0,(row.completed_at_us ?? now)-row.started_at_us);
  return <span className="execution-duration" title="Wall time since start, including waiting, pauses and interruptions. An unfinished duration is estimated.">{row.completed_at_us==null && elapsed!=null?'~ ':''}{duration(elapsed)}</span>;
}
