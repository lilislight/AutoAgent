import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { fallbackLayout, type Layout } from '../layout';
import { layoutWorkflow } from '../layoutWorker';
import { fitGraphCamera, type GraphCamera } from '../graphCamera';
import { splitComparisonGraphs, type comparisonGraph } from '../revisionComparison';
import type { Selection, WorkflowGraphData } from '../types';
import type { Theme } from '../theme';
import { useFlowReady } from './FlowLoadingBoundary';

const WorkflowGraph = lazy(() => import('./WorkflowGraph'));
type Drawing = ReturnType<typeof comparisonGraph>;

export function RevisionSplitGraph({ base, target, drawing, selection, onSelect, theme, focusRequest }: {
  base: WorkflowGraphData; target: WorkflowGraphData; drawing: Drawing; selection: Selection;
  onSelect: (selection: Selection) => void; theme: Theme; focusRequest: number;
}) {
  const root = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });
  const [layout, setLayout] = useState<{ value: Layout; warning: boolean } | null>(null);
  const [camera, setCamera] = useState<GraphCamera | null>(null);
  const [ratio, setRatio] = useState(.5);
  const [resizing, setResizing] = useState(false);
  const pointer = useRef<number | null>(null);
  const ready = useRef(new Set<string>());
  const markReady = useFlowReady();
  const graphs = useMemo(() => splitComparisonGraphs(base, target, drawing), [base, target, drawing]);
  useEffect(() => {
    const controller = new AbortController();
    layoutWorkflow(drawing.workflow.definition_hash, drawing.workflow.definition, controller.signal)
      .then(value => { if (!controller.signal.aborted) setLayout({ value, warning: false }); })
      .catch(() => { if (!controller.signal.aborted) setLayout({ value: fallbackLayout(drawing.workflow.definition), warning: true }); });
    return () => controller.abort();
  }, [drawing.workflow]);
  useEffect(() => {
    if (!root.current) return;
    const observer = new ResizeObserver(([entry]) => setSize({ width: entry.contentRect.width, height: entry.contentRect.height }));
    observer.observe(root.current);
    return () => observer.disconnect();
  }, []);
  const fitted = useMemo(() => layout && size.width && size.height ? fitGraphCamera(layout.value, {
    width: (size.width - 8) * Math.min(ratio, 1 - ratio), height: size.height - 32,
  }) : null, [layout, size, ratio]);
  const shared = camera ?? fitted;
  const resize = (clientX: number) => {
    const bounds = root.current?.getBoundingClientRect();
    if (bounds?.width) setRatio(Math.min(.7, Math.max(.3, (clientX - bounds.left) / bounds.width)));
  };
  const focusedSide = selection && (selection.kind === 'node' ? target.definition.nodes : target.definition.edges).some(item => item.id === selection.id) ? 'target' : 'base';
  return <div ref={root} className={`comparison-split ${resizing ? 'is-resizing' : ''}`}>
    {layout && shared ? <>
      {(['base', 'target'] as const).map((side, index) => <section key={side} className="comparison-pane" aria-label={`${side === 'base' ? 'Base' : 'Target'} Revision graph`} style={{ flex: side === 'base' ? ratio : 1 - ratio, order: index * 2 }}>
        <header className="comparison-pane-heading"><strong>{side === 'base' ? 'Base' : 'Target'}</strong><span title={graphs[side].workflow.workflow_revision_id}>v{graphs[side].workflow.workflow_version} · {graphs[side].workflow.definition_hash.slice(0, 8)}</span></header>
        <div className="comparison-pane-flow"><Suspense fallback={null}><WorkflowGraph workflow={graphs[side].workflow} comparison={graphs[side].marks} selection={selection} onSelect={onSelect} theme={theme} focusRequest={focusedSide === side ? focusRequest : 0} hideLegend={side === 'target'} linkedView={{
          layout: layout.value, layoutWarning: layout.warning, camera: shared, onCamera: setCamera, onReset: () => setCamera(null),
          onReady: () => { ready.current.add(side); if (ready.current.size === 2) markReady(); },
        }} /></Suspense></div>
      </section>)}
      <div className="execution-split-divider comparison-divider" style={{ order: 1 }} role="separator" tabIndex={0} aria-label="Resize Base and Target" aria-orientation="vertical" aria-valuemin={30} aria-valuemax={70} aria-valuenow={Math.round(ratio * 100)} aria-valuetext={`Base ${Math.round(ratio * 100)}%, Target ${Math.round((1 - ratio) * 100)}%`}
        onDoubleClick={() => setRatio(.5)}
        onPointerDown={event => { if (event.button !== 0) return; pointer.current = event.pointerId; setResizing(true); event.currentTarget.setPointerCapture(event.pointerId); event.preventDefault(); }}
        onPointerMove={event => { if (pointer.current === event.pointerId) resize(event.clientX); }}
        onPointerUp={event => { pointer.current = null; setResizing(false); if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId); }}
        onLostPointerCapture={() => { pointer.current = null; setResizing(false); }}
        onPointerCancel={() => { pointer.current = null; setResizing(false); }}
        onKeyDown={event => { if (event.key === 'Home' || event.key === 'End' || event.key === 'ArrowLeft' || event.key === 'ArrowRight') { event.preventDefault(); setRatio(value => event.key === 'Home' ? .3 : event.key === 'End' ? .7 : Math.max(.3, Math.min(.7, value + (event.key === 'ArrowLeft' ? -.025 : .025)))); } }}><span /></div>
    </> : null}
  </div>;
}
