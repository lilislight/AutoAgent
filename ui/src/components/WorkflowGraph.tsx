import { useEffect, useLayoutEffect, useId, useMemo, useRef, useState, type CSSProperties } from "react";
import {
  ReactFlow, Background, Controls, ControlButton, MiniMap, Handle, Position, BaseEdge,
  MarkerType, useNodesState, getSmoothStepPath,
  type Node, type NodeProps, type EdgeProps, type ReactFlowInstance,
} from "@xyflow/react";
import { fallbackLayout, routeDefinition, NODE_HEIGHT, NODE_WIDTH, type Layout, type Point } from "../layout";
import { roundedRoutePath, routeMidpoint } from "../graphRouting";
import { useFlowReady } from "./FlowLoadingBoundary";
import { layoutWorkflow } from "../layoutWorker";
import type { ExecutionTrace, Selection, TraceRow, WorkflowGraphData } from "../types";
import type { Theme } from "../theme";
import type { ComparisonMarks } from "../revisionComparison";
import type { StackAnchor } from "../stackMotion";
import { executionGraphStates, executionStateLabel } from '../executionGraphState';
import { cameraFromViewport, sameGraphCamera, viewportFromCamera, type GraphCamera } from '../graphCamera';

export interface LinkedGraphView {
  layout: Layout; camera: GraphCamera; onCamera: (camera: GraphCamera) => void;
  onReset: () => void; onReady: () => void; layoutWarning?: boolean;
}

type DefinitionNode = Node<{
  inherited?: boolean; change?: string; label: string; kind: string; executable: string; entry: boolean; exit: boolean; mapped: boolean; callCount?: number; status?: string; occurrences?: number; recordBadge?: string; recordTitle?: string; related?: boolean;
}, "definition">;

function NodeStateIcon({ state }: { state: string }) {
  const paths: Record<string, string> = { completed: 'M3 8l3 3 7-7', failed: 'M4 4l8 8M12 4l-8 8', paused: 'M5 3v10M11 3v10', cancelled: 'M4 4h8v8H4Z', skipped: 'M3 3l5 5-5 5V3m6 0 5 5-5 5V3', running: 'M2 8h3l2-4 3 8 2-4h2', waiting: 'M4 2h8M4 14h8M5 2v3l6 6v3M11 2v3l-6 6v3', ready: 'M8 4v4l3 2', 'not-run': 'M4 8h8', 'not-started': 'M8 3a5 5 0 1 0 0 10 5 5 0 0 0 0-10' };
  return <svg className="node-state-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{state === 'ready' ? <circle cx="8" cy="8" r="6" /> : null}<path d={paths[state] ?? paths['not-started']} /></svg>;
}

function WorkflowNode({ data, selected }: NodeProps<DefinitionNode>) {
  return <div className={`workflow-node ${data.kind} ${data.change ? `comparison-${data.change}` : ""} ${selected ? "selected" : ""} ${data.related ? "record-related" : ""} ${data.status ? `execution-${data.status}` : ""}`}>
    {data.change ? <span className={`graph-change-badge change-${data.change}`} title={data.change}>{data.change === "added" ? "+" : data.change === "removed" ? "−" : "~"}</span> : null}
    {data.status ? <span className="node-state-overlay" aria-hidden="true" /> : null}
    {data.recordBadge ? <span className="graph-record-badge" title={data.recordTitle}>{data.recordBadge}</span> : null}
    <Handle type="target" position={Position.Left} />
    <div className="node-type"><svg className="node-kind-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true">
      {data.kind === "operator" ? <path d="m5 4-4 4 4 4m6-8 4 4-4 4M9 2 7 14" />
        : data.kind === "capability" ? <path d="m8 1 7 7-7 7-7-7Z M8 5v6M5 8h6" />
        : <><rect x="2" y="2" width="12" height="12" rx="3" /><path d="m6 5 3 3-3 3m4 0h2" /></>}
    </svg>{data.kind === "operator" ? "Operator" : data.kind === "capability" ? "Capability" : "System Command"}
      {data.mapped ? <span className="node-flag">Map{data.callCount !== undefined ? <small className="map-call-count" title="Operator Calls started across this Node’s occurrences">×{data.callCount}</small> : null}</span> : null}
      {data.status ? <span className={`node-state status-${data.status}`} title={`${data.occurrences ?? 0} occurrences`}><NodeStateIcon state={data.status} />{executionStateLabel(data.status)}{(data.occurrences ?? 0) > 1 ? <small className="node-occurrence-count">×{data.occurrences}</small> : null}</span> : null}</div>
    <strong title={data.label}>{data.label}</strong>
    <div className="node-bottom"><span title={data.executable}>{data.executable}</span>
      {data.inherited ? <span className="inherited-tag" title="Completed work inherited from the source execution">Inherited</span> : null}{data.entry ? <span className="entry-tag">Entry</span> : null}
      {data.exit ? <span className="exit-tag">Exit</span> : null}</div>
    <Handle type="source" position={Position.Right} />
  </div>;
}

function RoutedEdge(props: EdgeProps) {
  const points = props.data?.points as Point[] | undefined;
  const [smooth, labelX, labelY] = getSmoothStepPath(props);
  const routed = points?.length ? points : props.source === props.target ? [
    { x: props.sourceX, y: props.sourceY }, { x: props.sourceX + 32, y: props.sourceY },
    { x: props.sourceX + 32, y: props.sourceY - NODE_HEIGHT / 2 - 32 },
    { x: props.targetX - 32, y: props.sourceY - NODE_HEIGHT / 2 - 32 },
    { x: props.targetX - 32, y: props.targetY }, { x: props.targetX, y: props.targetY },
  ] : undefined;
  const path = routed ? roundedRoutePath(routed) : smooth;
  const label = routed ? routeMidpoint(routed) : { x: labelX, y: labelY };
  const evaluations = Number(props.data?.evaluations);
  const countWidth = Math.max(26, String(evaluations).length * 6.5 + 17);
  const countOffset = props.data?.conditional ? 12 + countWidth / 2 : 0;
  const crossOffset = props.data?.conditional ? -19 : evaluations > 1 ? -countWidth / 2 - 10 : 0;
  const edgeState = props.data?.executionState as string | undefined;
  return <g className={`routed-edge ${props.data?.failure ? "fail-edge" : ""}`} style={{ '--edge-color': props.data?.color } as CSSProperties}>
    <path className="edge-hover-halo" d={path} fill="none" />
    <BaseEdge id={props.id} path={path} markerEnd={props.markerEnd} style={props.style} interactionWidth={48} />
    {edgeState === 'active' ? <><path className="edge-flow-trail" d={path} fill="none" /><path className="edge-flow-overlay" d={path} fill="none" /></> : null}
    {edgeState === 'not-taken' ? <g className="edge-not-taken" transform={`translate(${label.x + crossOffset}, ${label.y})`}><title>Not taken</title><circle r="6.5" /><path d="M-2 -2 2 2M2 -2-2 2" /></g> : null}
    {props.data?.conditional ? <g className="condition-marker" transform={`translate(${label.x}, ${label.y})`}>
      <title>Conditional edge</title>
      <circle className="condition-background" r="9" />
      <path className="condition-outline" d="M0 -4.5 4.5 0 0 4.5 -4.5 0Z" />
    </g> : null}
    {props.data?.comparison ? <g className={`edge-change-marker change-${props.data.comparison}`} transform={`translate(${label.x + (props.data?.conditional ? 22 : 0)}, ${label.y})`}><rect x="-9" y="-9" width="18" height="18" rx="5" /><text textAnchor="middle" dominantBaseline="central">{props.data.comparison === "added" ? "+" : props.data.comparison === "removed" ? "−" : "~"}</text></g> : null}
    {evaluations > 1 ? <g className="edge-occurrence-count" transform={`translate(${label.x + countOffset}, ${label.y})`}>
      <title>{String(props.data?.evaluations)} evaluations · Click to explore</title>
      <rect x={-countWidth / 2} y="-9" width={countWidth} height="18" />
      <text textAnchor="middle" dominantBaseline="central">×{String(props.data?.evaluations)}</text>
    </g> : null}
  </g>;
}

const MAX_ZOOM = 1;
const FIT_VIEW_OPTIONS = { padding: 0.25, maxZoom: MAX_ZOOM };

const nodeTypes = { definition: WorkflowNode };
const edgeTypes = { routed: RoutedEdge };
const edgeColors = {
  light: { complete: "#4f6f97", fail: "#b64445" },
  dark: { complete: "#7faed5", fail: "#e48787" },
};
const executionEdgeColors = {
  light: { active: '#2874bd', waiting: '#936217', paused: '#8060ad', taken: '#2b7854', 'not-taken': '#7d8796', 'not-evaluated': '#adb7c5' },
  dark: { active: '#70b4fc', waiting: '#e3b360', paused: '#b49bdf', taken: '#7bc8a2', 'not-taken': '#778294', 'not-evaluated': '#515c6d' },
};

function GraphLegend({ execution }: { execution: boolean }) {
  const [open, setOpen] = useState(false);
  const panelId = useId();
  const [availableHeight, setAvailableHeight] = useState<number>();
  const container = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  useLayoutEffect(() => {
    const control = container.current, graph = control?.closest('.graph-shell');
    if (!open || !control || !graph) return;
    const measure = () => setAvailableHeight(Math.max(0, graph.getBoundingClientRect().bottom - control.getBoundingClientRect().top - 52));
    measure(); const observer = new ResizeObserver(measure); observer.observe(graph);
    return () => observer.disconnect();
  }, [open]);
  useEffect(() => {
    if (!open) return;
    const outside = (event: PointerEvent) => {
      if (event.target instanceof Element && event.target.closest('.execution-split-divider')) return;
      if (event.target instanceof window.Node && !container.current?.contains(event.target)) setOpen(false);
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key === "Escape") { setOpen(false); button.current?.focus(); }
    };
    document.addEventListener("pointerdown", outside);
    document.addEventListener("keydown", escape);
    return () => { document.removeEventListener("pointerdown", outside); document.removeEventListener("keydown", escape); };
  }, [open]);
  return <div className="graph-help" ref={container}>
    <button ref={button} className="graph-help-button" aria-label="Graph legend" title="Graph legend" aria-expanded={open} aria-controls={open ? panelId : undefined} onClick={() => setOpen(current => !current)}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M9.5 9a2.5 2.5 0 1 1 4.1 1.9c-.9.7-1.6 1.1-1.6 2.6" /><path d="M12 16.5v.2" strokeWidth="2.4" strokeLinecap="round" /></svg>
    </button>
    {open ? <div className="graph-legend" style={{maxHeight:availableHeight}} id={panelId} role="dialog" aria-labelledby={`${panelId}-title`}>
      <div className="graph-legend-heading"><strong id={`${panelId}-title`}>Graph legend</strong><button aria-label="Close graph legend" onClick={() => { setOpen(false); button.current?.focus(); }}>×</button></div>
      <div className="legend-row"><span className="edge-key"><i className="complete-key" style={execution ? { borderColor: 'var(--muted)' } : undefined} /></span><div><strong>Complete edge</strong><p>Solid line. Follows a completed source node.</p></div></div>
      <div className="legend-row"><span className="edge-key"><i className="fail-key" style={execution ? { borderColor: 'var(--muted)' } : undefined} /></span><div><strong>Fail edge</strong><p>Dashed line. Follows a failed source node.</p></div></div>
      <div className="legend-row"><span className="condition-key" aria-hidden="true">◇</span><div><strong>Conditional edge</strong><p>Requires its condition to match.</p></div></div>
      {execution ? <><div className="legend-row"><span className="edge-key"><i className="complete-key" style={{ borderColor: 'var(--accent)', borderTopWidth: '3px' }} /></span><div><strong>Active incoming route</strong><p>Flowing highlight follows the actual Activation. Waiting and paused paths breathe slowly.</p></div></div><div className="legend-row"><span className="edge-key"><i className="complete-key" style={{ borderColor: 'var(--success)', borderTopWidth: '3px' }} /></span><div><strong>Taken</strong><p>The recorded decision selected this route.</p></div></div><div className="legend-row"><span className="condition-key">×</span><div><strong>Not taken</strong><p>Grey with a cross. The recorded decision did not select this route.</p></div></div><div className="legend-row"><span className="edge-key"><i className="complete-key" style={{ borderColor: 'var(--line-strong)' }} /></span><div><strong>Not evaluated</strong><p>Thin grey line. No decision has been recorded.</p></div></div><div className="legend-node-states">{['not-started', 'ready', 'running', 'waiting', 'paused', 'completed', 'failed', 'skipped', 'cancelled', 'not-run'].map(state => <span key={state}><i className={`legend-node-swatch execution-${state}`} />{executionStateLabel(state)}</span>)}</div></> : null}
    </div> : null}
  </div>;
}

export default function WorkflowGraph({ workflow, selection, onSelect, theme, trace, selectedRecord, focusRequest = 0, comparison, linkedView, hideLegend = false }: {
  comparison?: ComparisonMarks; workflow: WorkflowGraphData; selection: Selection; onSelect: (selection: Selection, anchor?: StackAnchor) => void; theme: Theme; trace?: ExecutionTrace; selectedRecord?: TraceRow; focusRequest?: number;
  linkedView?: LinkedGraphView;
  hideLegend?: boolean;
}) {
  const markReady = useFlowReady();
  const flowId = useId();
  const [nodes, setNodes, onNodesChange] = useNodesState<DefinitionNode>([]);
  const [layout, setLayout] = useState<Layout | null>(null);
  const [layoutWarning, setLayoutWarning] = useState(false);
  const [moved, setMoved] = useState(false);
  const [layoutVersion, setLayoutVersion] = useState(0);
  const instance = useRef<ReactFlowInstance<DefinitionNode> | null>(null);
  const initialFitComplete = useRef(false);
  const [ready, setReady] = useState(false);
  const lastFocus = useRef(0);
  const focusedAt = useRef(-Infinity);
  const requestedFocus = useRef(focusRequest); requestedFocus.current = focusRequest;
  const container = useRef<HTMLDivElement>(null);
  const linked = useRef(linkedView); linked.current = linkedView;
  const applyLinkedCamera = () => {
    if (!linked.current || !instance.current || !container.current) return;
    const { width, height } = container.current.getBoundingClientRect();
    if (!width || !height) return;
    const next = viewportFromCamera(linked.current.camera, { width, height });
    const current = instance.current.getViewport();
    if (Math.abs(next.x-current.x)>.01 || Math.abs(next.y-current.y)>.01 || Math.abs(next.zoom-current.zoom)>.0001) void instance.current.setViewport(next);
  };

  useEffect(() => {
    if (!container.current) return;
    let previousSize: { width: number; height: number } | null = null;
    let visibleOnce = false;
    let resizeTimer: ReturnType<typeof setTimeout> | undefined;
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      const changed = previousSize && (previousSize.width !== width || previousSize.height !== height);
      const wasHidden = previousSize && (!previousSize.width || !previousSize.height);
      previousSize = { width, height };
      if (!width || !height) return;
      if (linked.current) { applyLinkedCamera(); return; }
      const returning = visibleOnce && wasHidden;
      visibleOnce = true;
      if (!changed || returning || wasHidden && requestedFocus.current) return;
      clearTimeout(resizeTimer);
      // Fit after a sidebar transition settles, without changing node positions.
      resizeTimer = setTimeout(() => {
        if (performance.now() - focusedAt.current < 650) return;
        void instance.current?.fitView({ ...FIT_VIEW_OPTIONS,
          duration: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 250 });
      }, 150);
    });
    observer.observe(container.current);
    return () => { observer.disconnect(); clearTimeout(resizeTimer); };
  }, []);

  useEffect(() => {
    if (linkedView) { setLayout(linkedView.layout); setLayoutWarning(Boolean(linkedView.layoutWarning)); setMoved(false); return; }
    const controller = new AbortController();
    setLayout(null);
    setLayoutWarning(false);
    setMoved(false);
    layoutWorkflow(workflow.definition_hash, workflow.definition, controller.signal)
      .then(value => { if (!controller.signal.aborted) setLayout(value); })
      .catch(() => {
        if (controller.signal.aborted) return;
        setLayout(fallbackLayout(workflow.definition));
        setLayoutWarning(true);
      });
    return () => controller.abort();
  }, [workflow.definition_hash, workflow.definition, linkedView?.layout, linkedView?.layoutWarning]);

  useEffect(() => { if (ready) applyLinkedCamera(); }, [linkedView?.camera, ready]);

  useEffect(() => {
    if (!layout) return;
    const entries = new Set(workflow.definition.entry_node_ids);
    const exits = new Set(workflow.definition.exit_node_ids);
    setNodes(workflow.definition.nodes.map(node => ({
      id: node.id, type: "definition", position: { ...layout.positions[node.id] },
      width: NODE_WIDTH, height: NODE_HEIGHT,
      ariaLabel: `${node.executable.kind} node: ${node.id}`,
      data: { label: node.id, kind: node.executable.kind,
        executable: node.executable.id.startsWith(`${node.executable.kind}:`)
          ? node.executable.id : `${node.executable.kind}:${node.executable.id}`,
        entry: entries.has(node.id), exit: exits.has(node.id), mapped: node.mapped },
    })));
    // Wait for React Flow to receive restored positions before fitting the viewport.
    let cancelled = false;
    const finish = () => {
      if (!cancelled) { initialFitComplete.current = true; setReady(true); if (linked.current) linked.current.onReady(); else markReady(); }
    };
    let fitFrame = 0;
    const fit = () => {
      if (!instance.current) { fitFrame = requestAnimationFrame(fit); return; }
      if (linked.current) { applyLinkedCamera(); finish(); return; }
      void instance.current.fitView({ ...FIT_VIEW_OPTIONS,
        duration: !initialFitComplete.current || window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 250 }).then(finish, finish);
    };
    const frame = requestAnimationFrame(() => {
      fitFrame = requestAnimationFrame(fit);
    });
    return () => { cancelled = true; cancelAnimationFrame(frame); cancelAnimationFrame(fitFrame); };
  }, [layout, workflow.definition, setNodes, layoutVersion, markReady]);

  const restoreLayout = () => {
    if (linked.current) { linked.current.onReset(); return; }
    setMoved(false);
    setLayoutVersion(current => current + 1);
  };

  const nodeStates = useMemo(() => new Map(trace?.nodes.map(item => [item.node_id, item])), [trace]);
  const edgeStates = useMemo(() => new Map(trace?.edges.map(item => [item.edge_id, item])), [trace]);
  const executionStates = useMemo(() => trace ? executionGraphStates(trace) : null, [trace]);
  const displayNodes = useMemo<DefinitionNode[]>(() => nodes.map(node => ({ ...node,
    selected: selection?.kind === "node" && selection.id === node.id,
    data: { ...node.data, inherited: Boolean(trace?.fork && ['completed','skipped','failed','cancelled'].includes(trace.fork.inherited_occurrences[nodeStates.get(node.id)?.occurrence_id ?? ''] ?? '')), change: comparison?.nodes[node.id], status: executionStates ? executionStates.nodes.get(node.id) ?? executionStates.unrecordedNode : undefined, occurrences: nodeStates.get(node.id)?.occurrences, callCount: trace ? nodeStates.get(node.id)?.call_count ?? 0 : undefined,
      related: selection?.record?.kind === 'edge' && selectedRecord?.node_id === node.id,
      recordBadge: selectedRecord?.node_id === node.id ? selection?.record?.kind === 'edge' ? 'Source'
        : selection?.record?.kind === 'call' ? `Unit ${selectedRecord.unit_index ?? 0}`
        : selection?.record?.kind === 'wait' ? 'Wait' : `Occurrence #${selectedRecord.ordinal ?? selectedRecord.ui_ordinal ?? '—'}` : undefined,
      recordTitle: selectedRecord?.node_id === node.id ? `${selectedRecord.occurrence_id ?? selectedRecord.source_occurrence_id}` : undefined },
  })), [nodes, selection, executionStates, nodeStates, selectedRecord, trace, comparison]);
  const routes = useMemo(() => moved
    ? routeDefinition(workflow.definition, Object.fromEntries(nodes.map(node => [node.id, node.position])))
    : layout?.routes, [moved, nodes, workflow.definition, layout]);
  const edges = useMemo(() => workflow.definition.edges.map(edge => {
    const state = executionStates ? executionStates.edges.get(edge.id) ?? 'not-evaluated' : undefined;
    const color = state ? executionEdgeColors[theme][state] : edgeColors[theme][edge.on === 'error' ? 'fail' : 'complete'];
    return {
    id: edge.id, source: edge.source, target: edge.target, type: "routed",
    selected: selection?.kind === "edge" && selection.id === (comparison?.aliases[edge.id] ?? edge.id),
    ariaLabel: `${edge.id}: ${edge.on === "error" ? "Fail" : "Complete"} edge${edge.conditional ? ", conditional" : ""}, ${edge.source} to ${edge.target}${state ? ', ' + executionStateLabel(state) : ''}`,
    className: state ? `execution-edge-${state}` : undefined,
    data: { comparison: comparison?.edges[edge.id], points: routes?.[edge.id], conditional: Boolean(edge.conditional), failure: edge.on === "error", evaluations: edgeStates.get(edge.id)?.evaluations, executionState: state, color },
    markerEnd: { type: MarkerType.ArrowClosed, color },
    style: { stroke: 'var(--edge-color)',
      strokeDasharray: edge.on === "error" ? "5 4" : undefined },
  }; }), [workflow.definition.edges, routes, selection, theme, executionStates, edgeStates, comparison]);

  useEffect(() => {
    if (!ready || !focusRequest || lastFocus.current === focusRequest || !selection) return;
    const node = nodes.find(item => item.id === (selectedRecord?.node_id ?? (selection.kind === 'node' ? selection.id : '')));
    const points = selection.kind === 'edge' ? routes?.[selection.id] : null;
    const point = points?.length ? routeMidpoint(points) : node ? { x: node.position.x + NODE_WIDTH / 2, y: node.position.y + NODE_HEIGHT / 2 } : null;
    if (!point || !instance.current) return;
    const bounds = container.current?.getBoundingClientRect();
    const presence = container.current?.closest('.flow-loading-surface')?.querySelector<HTMLElement>('.inspector-presence:not(.is-hidden)');
    const inspectorParent = presence?.offsetParent as HTMLElement | null;
    const inspectorLeft = presence && inspectorParent ? inspectorParent.getBoundingClientRect().left + presence.offsetLeft : null;
    const right = bounds && inspectorLeft != null && inspectorLeft > bounds.left && inspectorLeft < bounds.right ? inspectorLeft - 16 : bounds?.right;
    const notices = container.current?.parentElement?.querySelector('.graph-pending-actions')?.getBoundingClientRect();
    const left = bounds && notices && right && right - notices.right > 96 ? notices.right + 12 : bounds?.left;
    const visibleWidth = right != null && left != null ? right - left : undefined;
    if (!bounds || !visibleWidth) return;
    lastFocus.current = focusRequest;
    focusedAt.current = performance.now();
    const zoom = Math.min(1, Math.max(.65, instance.current.getZoom()), Math.max(.15, visibleWidth / (NODE_WIDTH + 80)));
    void instance.current.setViewport({ x: left! - bounds.left + visibleWidth / 2 - point.x * zoom, y: bounds.height / 2 - point.y * zoom, zoom }, { duration: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 220 });
  }, [ready, focusRequest, selection, selectedRecord, nodes, routes]);

  return <div ref={container} className="graph-shell" aria-label="Workflow definition graph">
    <ReactFlow<DefinitionNode> id={flowId} nodes={displayNodes} edges={edges} nodeTypes={nodeTypes} edgeTypes={edgeTypes} colorMode={theme}
      onNodesChange={changes => {
        if (changes.some(change => change.type === "position" && change.position)) setMoved(true);
        onNodesChange(changes);
      }}
      onInit={flow => { instance.current = flow; if (layout && !linked.current) void flow.fitView(FIT_VIEW_OPTIONS); }}
      onMove={(_event, viewport) => {
        if (!linked.current || !container.current) return;
        const next = cameraFromViewport(viewport, container.current.getBoundingClientRect());
        // Minimap and focus moves have no source Event. Compare world centres
        // instead, to propagate them without echoing our own linked updates.
        if (!sameGraphCamera(next, linked.current.camera)) linked.current.onCamera(next);
      }}
      onNodeClick={(event, node) => {
        const rect = event.currentTarget.getBoundingClientRect();
        onSelect({ kind: "node", id: node.id }, { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2, width: rect.width, height: rect.height });
      }}
      onEdgeClick={(event, edge) => {
        const rect = event.currentTarget.getBoundingClientRect();
        const zoom = instance.current?.getZoom() ?? 1;
        const pointer = event.clientX !== 0 || event.clientY !== 0;
        onSelect({ kind: "edge", id: edge.id }, { x: pointer ? event.clientX : rect.left + rect.width / 2,
          y: pointer ? event.clientY : rect.top + rect.height / 2, width: 36 * zoom, height: 22 * zoom });
      }}
      onPaneClick={() => onSelect(null)} nodesConnectable={false} edgesReconnectable={false} nodesDraggable={!linkedView}
      deleteKeyCode={null} minZoom={0.15} maxZoom={MAX_ZOOM} fitView={!linkedView} fitViewOptions={FIT_VIEW_OPTIONS}
      proOptions={{ hideAttribution: true }}>
      <Background color="var(--canvas-dot)" gap={22} size={1} />
      <Controls position="top-right" showZoom={false} showFitView={false} showInteractive={false}>
        <ControlButton className="restore-layout" onClick={restoreLayout} disabled={!layout} title="Reset layout and view" aria-label="Reset layout and view">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" aria-hidden="true"><path d="M3 11a9 9 0 1 1 2.6 7M3 4v7h7" /></svg>
        </ControlButton>
      </Controls>
      <MiniMap style={{ width:140, height:94 }} pannable zoomable nodeColor="var(--minimap-node)" maskColor="var(--minimap-mask)" />
    </ReactFlow>
    {layoutWarning ? <div className="canvas-notice">Automatic layout unavailable. Using a basic layout.</div> : null}
    {hideLegend ? null : <GraphLegend execution={Boolean(trace)} />}
  </div>;
}
