import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import {
  BaseEdge,
  applyNodeChanges,
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Panel,
  Position,
  ReactFlow,
  type Edge as FlowEdge,
  type EdgeProps,
  type Node as FlowNode,
  type NodeChange,
  type NodeProps,
  type ReactFlowInstance,
  useNodesState,
} from "@xyflow/react";
import {
  Boxes,
  Flag,
  Play,
  RotateCcw,
} from "lucide-react";

import {
  affectedEdgeIdsAfterNodeMove,
  GRAPH_NODE_HEIGHT,
  GRAPH_NODE_WIDTH,
  routeEdgesAroundNodes,
  type GraphRect,
} from "../graphRouting";
import {
  calculateGroupBounds,
  layoutWorkflow,
  loadAutomaticLayout,
  loadSavedLayout,
  saveAutomaticLayout,
  saveLayout,
  type EdgeRoute,
} from "../layout";
import type {
  EdgeRuntimeState,
  InvocationDetail,
  NodeExecutionView,
  RuntimeProjection,
  RuntimeState,
  TraceSelection,
  WorkflowGraphView,
  WorkflowGroupView,
} from "../types";

type TraceNodeData = {
  label: string;
  capability: string;
  state: RuntimeState;
  entry: boolean;
  exit: boolean;
  executionCount: number;
  skippedCount: number;
  latestOccurrenceState: RuntimeState | null;
  operatorSummary: string | null;
  issue: NodeIssue | null;
  preview: boolean;
};

type GroupNodeData = {
  label: string;
  nodeCount: number;
  state: RuntimeState;
  width: number;
  height: number;
};

type NodeIssue = {
  tone: "danger" | "warning" | "neutral";
  label: string;
  title: string;
};

type RoutedEdgeData = {
  route?: EdgeRoute;
};

type TraceFlowNode =
  | FlowNode<TraceNodeData, "trace">
  | FlowNode<GroupNodeData, "workflowGroup">;

const nodeTypes = { trace: TraceNode, workflowGroup: WorkflowGroupNode };
const edgeTypes = { routed: RoutedEdge };

interface WorkflowCanvasProps {
  graph: WorkflowGraphView;
  invocation: InvocationDetail;
  projection: RuntimeProjection;
  followLive: boolean;
  selection: TraceSelection;
  canInvoke: boolean;
  canResume?: boolean;
  preview?: boolean;
  onInspect: (selection: TraceSelection) => void;
  onInvoke?: (entryNodeId: string) => void;
  onResume?: (nodeId: string) => void;
}

export function WorkflowCanvas({
  graph,
  invocation,
  projection,
  followLive,
  selection,
  canInvoke,
  canResume = false,
  preview = false,
  onInspect,
  onInvoke,
  onResume,
}: WorkflowCanvasProps) {
  const [nodes, setNodes] = useNodesState<TraceFlowNode>([]);
  const [edgeRoutes, setEdgeRoutes] = useState<Record<string, EdgeRoute>>({});
  const [layoutPending, setLayoutPending] = useState(false);
  const edgeRoutesRef = useRef<Record<string, EdgeRoute>>({});
  const [hoveredEdgeId, setHoveredEdgeId] = useState<string | null>(null);
  const flow = useRef<ReactFlowInstance<TraceFlowNode, FlowEdge> | null>(null);
  const layoutRequestRef = useRef(0);
  const projectionRef = useRef(projection);
  const invocationRef = useRef(invocation);
  const selectionRef = useRef(selection);
  projectionRef.current = projection;
  invocationRef.current = invocation;
  selectionRef.current = selection;

  const buildLayout = useCallback(
    async (force = false) => {
      const request = ++layoutRequestRef.current;
      if (force) setLayoutPending(true);
      const saved = force
        ? loadAutomaticLayout(graph.definition_hash)
        : loadSavedLayout(graph.definition_hash);
      let layout = saved;
      try {
        if (layout === null) {
          layout = await layoutWorkflow(graph);
        }
        if (request !== layoutRequestRef.current) return;
        if (saved === null) {
          saveAutomaticLayout(graph.definition_hash, layout);
        }
        saveLayout(graph.definition_hash, layout);
      } finally {
        if (request === layoutRequestRef.current) {
          setLayoutPending(false);
        }
      }
      if (request !== layoutRequestRef.current || layout === null) return;
      const positions = layout.positions;
      edgeRoutesRef.current = layout.edgeRoutes;
      setEdgeRoutes(layout.edgeRoutes);
      const currentProjection = projectionRef.current;
      const currentInvocation = invocationRef.current;
      const currentSelection = selectionRef.current;
      const groupNodes = buildGroupNodes(
        graph.groups ?? [],
        currentProjection,
        layout.groupBounds,
        currentSelection,
      );
      const traceNodes: TraceFlowNode[] = graph.nodes
        .map((node) => {
          const projected = currentProjection.nodes[node.id];
          const capability = `${String(node.capability.kind ?? "operator")}:${String(
            node.capability.id ?? "unknown",
          )}`;
          const latestExecution = projected
            ? currentInvocation.node_executions.find(
                (execution) => execution.id === projected.latest_execution_id,
              )
            : undefined;
          return {
            id: node.id,
            type: "trace",
            position: positions[node.id] ?? { x: 0, y: 0 },
            data: {
              label: node.name || node.id,
              capability,
              state: projected?.state ?? "created",
              entry: node.entry,
              exit: node.exit,
              executionCount: projected?.execution_count ?? 0,
              skippedCount: projected?.skipped_count ?? 0,
              latestOccurrenceState:
                projected?.latest_occurrence_state ?? null,
              operatorSummary: nodeOperatorSummary(projected),
              issue: nodeIssue(latestExecution),
              preview,
            },
            selected:
              currentSelection?.type === "node" &&
              currentSelection.id === node.id,
            style: {
              width: GRAPH_NODE_WIDTH,
              height: GRAPH_NODE_HEIGHT,
            },
            zIndex: 100,
          };
        });
      setNodes([...groupNodes, ...traceNodes]);
      requestAnimationFrame(() => flow.current?.fitView({ padding: 0.2, duration: 320 }));
    }, [graph, invocation.node_executions, preview, projection, selection, setNodes]);

  useEffect(() => {
    void buildLayout(false);
    // Layout identity changes only with the immutable graph definition.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [graph.definition_hash]);

  useEffect(() => {
    setNodes((current) =>
      current.map((node) => {
        if (node.type === "workflowGroup") {
          const data = node.data as GroupNodeData;
          return {
            ...node,
            selected: selection?.type === "group" && selection.id === node.id,
            data: {
              ...data,
              state: groupState(
                graph.groups.find((group) => group.id === node.id),
                projection,
              ),
            },
          };
        }
        const projected = projection.nodes[node.id];
        const latestExecution = projected
          ? invocation.node_executions.find(
              (execution) => execution.id === projected.latest_execution_id,
            )
          : undefined;
        return {
          ...node,
          selected: selection?.type === "node" && selection.id === node.id,
          data: {
            ...node.data,
            state: projected?.state ?? "created",
            executionCount: projected?.execution_count ?? 0,
            skippedCount: projected?.skipped_count ?? 0,
            latestOccurrenceState:
              projected?.latest_occurrence_state ?? null,
            operatorSummary: nodeOperatorSummary(projected),
            issue: nodeIssue(latestExecution),
            preview,
          },
        };
      }),
    );
  }, [graph.groups, invocation.node_executions, preview, projection, selection, setNodes]);

  const handleNodesChange = useCallback(
    (changes: NodeChange<TraceFlowNode>[]) => {
      setNodes((current) => {
        const changed = applyNodeChanges(changes, current);
        if (!changes.some((change) => change.type === "position")) {
          return changed;
        }
        return resizeGroupNodes(
          changed,
          graph.groups ?? [],
          graph.nodes,
          edgeRoutesRef.current,
          projectionRef.current,
          selectionRef.current,
        );
      });
    },
    [graph.groups, graph.nodes, setNodes],
  );

  const edges = useMemo<FlowEdge[]>(
    () => {
      return graph.edges.map((edge) => {
        const projected = projection.edges[edge.id];
        const selected = projected?.selected ?? false;
        const inspected = selection?.type === "edge" && selection.id === edge.id;
        const hovered = hoveredEdgeId === edge.id;
        const edgeState = edgeStateClass(projected?.state, selected);
        return {
          id: edge.id,
          source: edge.from_node,
          target: edge.to_node,
          type: "routed",
          data: { route: edgeRoutes[edge.id] },
          animated:
            followLive &&
            (projected?.latest_selected ?? selected) &&
            projection.invocation_state === "running",
          label: projected && projected.evaluation_count > 1
            ? `${projected.selected_count}/${projected.evaluation_count}`
            : undefined,
          selected: inspected,
          interactionWidth: 28,
          zIndex: hovered || inspected ? 60 : selected ? 40 : 8,
          markerEnd: {
            type: MarkerType.ArrowClosed,
            color: edgeVisualColor(projected?.state, selected, inspected, hovered),
          },
          style: {
            stroke: edgeVisualColor(projected?.state, selected, inspected, hovered),
            strokeWidth: inspected || selected || hovered ? 2.9 : 1.4,
            opacity: projected ? 1 : 0.34,
            strokeDasharray: projected?.state === "skipped"
              ? "5 5"
              : projected
                ? undefined
                : "4 6",
          },
          className: `trace-edge state-${edgeState} ${inspected ? "selected" : ""} ${hovered ? "is-hovered" : ""}`,
        };
      });
    },
    [
      followLive,
      graph.edges,
      edgeRoutes,
      projection.edges,
      projection.invocation_state,
      selection,
      hoveredEdgeId,
    ],
  );

  return (
    <section className="workflow-canvas" aria-label="Workflow execution graph">
      <ReactFlow<TraceFlowNode, FlowEdge>
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={handleNodesChange}
        onNodeDragStart={(_event, node) => {
          if (node.type === "workflowGroup") return;
          // ELK routes use absolute points. Only routes connected to the
          // moving Node become invalid; unrelated obstacle-avoiding routes
          // remain stable.
          const nextRoutes = withoutIncidentEdgeRoutes(
            edgeRoutesRef.current,
            graph.edges,
            node.id,
          );
          edgeRoutesRef.current = nextRoutes;
          setEdgeRoutes(nextRoutes);
        }}
        onNodeDragStop={(_event, node) => {
          if (node.type === "workflowGroup") return;
          const current = flow.current?.getNodes() ?? [];
          const positions = traceNodePositions(current);
          const affectedEdgeIds = affectedEdgeIdsAfterNodeMove(
            edgeRoutesRef.current,
            graph.edges,
            node.id,
            positions,
          );
          const reroutedEdges = routeEdgesAroundNodes(
            graph.nodes,
            graph.edges,
            positions,
            affectedEdgeIds,
          );
          const nextRoutes = {
            ...edgeRoutesRef.current,
            ...reroutedEdges,
          };
          edgeRoutesRef.current = nextRoutes;
          setEdgeRoutes(nextRoutes);
          saveLayout(graph.definition_hash, {
            positions,
            edgeRoutes: nextRoutes,
            groupBounds: calculateGroupBounds(
              graph.groups ?? [],
              graph.nodes,
              positions,
              nextRoutes,
            ),
          });
          setNodes((existing) =>
            resizeGroupNodes(
              existing,
              graph.groups ?? [],
              graph.nodes,
              nextRoutes,
              projectionRef.current,
              selectionRef.current,
            ),
          );
        }}
        onNodeClick={(event, node) => {
          event.stopPropagation();
          const target: Exclude<TraceSelection, null> = {
            type: node.type === "workflowGroup" ? "group" : "node",
            id: node.id,
          };
          onInspect(target);
          if (
            target.type === "node" &&
            projection.nodes[node.id]?.state === "waiting" &&
            canResume
          ) {
            onResume?.(node.id);
            return;
          }
          if (
            target.type === "node" &&
            graph.entry_node_ids.includes(node.id) &&
            canInvoke
          ) {
            onInvoke?.(node.id);
            return;
          }
        }}
        onEdgeClick={(event, edge) => {
          event.stopPropagation();
          onInspect({ type: "edge", id: edge.id });
        }}
        onEdgeMouseEnter={(_event, edge) => {
          setHoveredEdgeId(edge.id);
        }}
        onEdgeMouseLeave={() => {
          setHoveredEdgeId(null);
        }}
        onPaneClick={() => onInspect(null)}
        onInit={(instance) => {
          flow.current = instance;
        }}
        nodesConnectable={false}
        nodesDraggable
        elementsSelectable
        elevateEdgesOnSelect={false}
        minZoom={0.2}
        maxZoom={1.8}
        fitView
        proOptions={{ hideAttribution: true }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={22}
          size={1.2}
          color="var(--canvas-dot)"
        />
        <Controls showInteractive={false} />
        <MiniMap
          pannable
          zoomable
          nodeColor={(node) => stateColor((node.data as TraceNodeData).state)}
          maskColor="var(--minimap-mask)"
        />
        <Panel position="top-right">
          <button
            className={`canvas-action ${layoutPending ? "is-pending" : ""}`}
            type="button"
            onClick={() => void buildLayout(true)}
            title="Restore automatic layout"
            aria-busy={layoutPending}
            disabled={layoutPending}
          >
            <RotateCcw size={14} />
            {layoutPending ? "Laying out…" : "Auto layout"}
          </button>
        </Panel>
      </ReactFlow>
    </section>
  );
}

function WorkflowGroupNode({ data, selected }: NodeProps<FlowNode<GroupNodeData, "workflowGroup">>) {
  const style = { width: data.width, height: data.height } as CSSProperties;
  return (
    <div
      className={`workflow-group-node is-expanded state-${stateClass(data.state)} ${selected ? "selected" : ""}`}
      style={style}
    >
      <div className="group-label">
        <Boxes size={14} />
        <strong>{data.label}</strong>
        <span>{data.nodeCount} nodes</span>
      </div>
    </div>
  );
}

function TraceNode({ data, selected }: NodeProps<TraceFlowNode>) {
  const traceData = data as TraceNodeData;
  return (
    <div
      className={`trace-node state-${stateClass(traceData.state)} ${
        traceData.preview ? "is-preview" : ""
      } ${selected ? "selected" : ""}`}
    >
      <Handle type="target" position={Position.Left} />
      <div className="trace-node-heading">
        <span className="state-indicator" />
        <strong>{traceData.label}</strong>
        {traceData.executionCount > 1 && (
          <span className="execution-count">×{traceData.executionCount}</span>
        )}
        {traceData.skippedCount > 0 && (
          <span
            className="execution-count"
            title={
              traceData.latestOccurrenceState === "skipped"
                ? "The latest scoped Node occurrence was skipped."
                : "One or more scoped Node occurrences were skipped."
            }
          >
            skip ×{traceData.skippedCount}
          </span>
        )}
      </div>
      <div className="trace-node-capability">{traceData.capability}</div>
      {traceData.operatorSummary && (
        <div className="trace-node-operator-summary">
          {traceData.operatorSummary}
        </div>
      )}
      <div className="trace-node-footer">
        {!traceData.preview && <span>{traceData.state}</span>}
        <span className="node-flags">
          {traceData.issue && (
            <span
              className={`node-issue tone-${traceData.issue.tone}`}
              title={traceData.issue.title}
            >
              {traceData.issue.label}
            </span>
          )}
          {traceData.entry && <Play size={12} aria-label="Entry node" />}
          {traceData.exit && <Flag size={12} aria-label="Exit node" />}
        </span>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

function RoutedEdge({
  id,
  data,
  sourceX,
  sourceY,
  targetX,
  targetY,
  markerEnd,
  style,
  interactionWidth,
  label,
}: EdgeProps<FlowEdge<RoutedEdgeData>>) {
  const points = data?.route?.points;
  const path = points && points.length >= 2
    ? orthogonalPath(points)
    : orthogonalPath([
        { x: sourceX, y: sourceY },
        { x: (sourceX + targetX) / 2, y: sourceY },
        { x: (sourceX + targetX) / 2, y: targetY },
        { x: targetX, y: targetY },
      ]);
  const labelPosition = data?.route?.label ?? {
    x: (sourceX + targetX) / 2,
    y: (sourceY + targetY) / 2,
  };
  return (
    <BaseEdge
      id={id}
      path={path}
      markerEnd={markerEnd}
      style={style}
      interactionWidth={interactionWidth}
      label={label}
      labelX={labelPosition.x}
      labelY={labelPosition.y}
      labelShowBg
      labelBgPadding={[5, 3]}
      labelBgBorderRadius={4}
    />
  );
}

function orthogonalPath(points: { x: number; y: number }[]): string {
  return points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
}

function withoutIncidentEdgeRoutes(
  routes: Record<string, EdgeRoute>,
  edges: WorkflowGraphView["edges"],
  nodeId: string,
): Record<string, EdgeRoute> {
  const incidentEdgeIds = new Set(
    edges
      .filter(
        (edge) => edge.from_node === nodeId || edge.to_node === nodeId,
      )
      .map((edge) => edge.id),
  );
  return Object.fromEntries(
    Object.entries(routes).filter(
      ([edgeId]) => !incidentEdgeIds.has(edgeId),
    ),
  );
}

function nodeOperatorSummary(
  node: RuntimeProjection["nodes"][string] | undefined,
): string | null {
  if (!node || !node.operator_call_count) return null;
  const values = [`calls ${node.operator_call_count}`];
  if (node.parallel_call_count) {
    values.push(`${node.latest_operator_kind ?? "parallel"} ×${node.parallel_call_count}`);
  }
  if (node.streaming_call_count) {
    values.push(
      `stream ${node.streaming_call_count} · chunks ${node.stream_chunk_count ?? 0}`,
    );
  }
  if (node.retry_count) values.push(`retry ${node.retry_count}`);
  if (node.fallback_count) values.push(`fallback ${node.fallback_count}`);
  if (node.timeout_count) values.push(`timeout ${node.timeout_count}`);
  if (node.failed_operator_call_count) {
    values.push(`failed ${node.failed_operator_call_count}`);
  }
  return values.join(" · ");
}

function stateClass(state: RuntimeState): string {
  return state.replaceAll("_", "-");
}

function stateColor(state: RuntimeState): string {
  const values: Record<string, string> = {
    created: "#858884",
    ready: "#147f8e",
    running: "#128a9b",
    waiting: "#c47b10",
    completed: "#24875d",
    failed: "#d04444",
    cancelled: "#8b5a65",
    interrupted: "#9a5b2d",
    skipped: "#858884",
  };
  return values[state] ?? "#737975";
}

function edgeColor(state: EdgeRuntimeState | undefined, selected: boolean): string {
  if (selected) return "var(--success)";
  if (state === "failed") return "var(--danger)";
  if (state === "skipped") return "var(--edge-skipped)";
  return "var(--edge)";
}

function edgeVisualColor(
  state: EdgeRuntimeState | undefined,
  selected: boolean,
  inspected: boolean,
  hovered: boolean,
): string {
  if (inspected) return "var(--accent-strong)";
  if (!hovered) return edgeColor(state, selected);
  if (selected) return "var(--success)";
  if (state === "failed") return "var(--danger)";
  if (state === "skipped") {
    return "color-mix(in srgb, var(--edge-skipped) 72%, var(--text) 28%)";
  }
  return "var(--accent-strong)";
}

function edgeStateClass(state: EdgeRuntimeState | undefined, selected: boolean): string {
  if (selected) return "selected";
  if (state === "failed") return "failed";
  if (state === "skipped") return "skipped";
  return state ?? "pending";
}

function nodeIssue(execution: NodeExecutionView | undefined): NodeIssue | null {
  if (!execution) return null;
  const failedCalls = execution.operator_calls.filter((call) =>
    ["failed", "interrupted"].includes(call.state),
  );
  if (failedCalls.length > 0 && execution.state === "completed") {
    const failed = failedCalls.at(-1)!;
    const recovery = execution.operator_calls.find(
      (call) =>
        call.call_no > failed.call_no &&
        call.state === "completed" &&
        ["retry", "fallback", "recovery"].includes(call.reason ?? ""),
    );
    const recoveryLabel = recovery?.reason ?? "recovered";
    return {
      tone: "warning",
      label: recoveryLabel,
      title: `${failed.operator_id} ${failed.kind} ${failed.state}: ${errorMessage(failed.error)}. Recovered by ${recoveryLabel}.`,
    };
  }
  if (failedCalls.length > 0) {
    const latest = failedCalls.at(-1)!;
    return {
      tone: "danger",
      label: "operator",
      title: `${latest.operator_id} ${latest.kind} ${latest.state}: ${errorMessage(latest.error)}`,
    };
  }
  if (!execution.error) return null;
  const code = typeof execution.error.code === "string" ? execution.error.code : "";
  return {
    tone: execution.state === "waiting" ? "warning" : "danger",
    label: issueLabel(code, execution.state),
    title: `${code || execution.state}: ${errorMessage(execution.error)}`,
  };
}

function issueLabel(code: string, state: string): string {
  if (code === "RESOURCE_LIMIT_EXCEEDED") return "policy";
  if (code.includes("MAPPING")) return "mapping";
  if (code.includes("BINDING")) return "binding";
  if (code.includes("AGGREGATION")) return "aggregate";
  if (code.includes("CONDITION")) return "condition";
  if (state === "cancelled") return "cancelled";
  if (state === "interrupted") return "interrupted";
  if (state === "waiting") return "waiting";
  return "node";
}

function errorMessage(error: Record<string, unknown> | null): string {
  if (!error) return "No structured error.";
  return typeof error.message === "string" ? error.message : JSON.stringify(error);
}

function buildGroupNodes(
  groups: WorkflowGroupView[],
  projection: RuntimeProjection,
  boundsByGroup: Record<string, GraphRect>,
  selection: TraceSelection,
): TraceFlowNode[] {
  const result: TraceFlowNode[] = [];
  for (const group of [...groups].sort((left, right) => left.workflow_path.length - right.workflow_path.length)) {
    const bounds = boundsByGroup[group.id];
    if (!bounds) continue;
    result.push({
      id: group.id,
      type: "workflowGroup",
      position: { x: bounds.left, y: bounds.top },
      selectable: true,
      draggable: false,
      className: "workflow-group-flow-node is-expanded",
      data: {
        label: group.label,
        nodeCount: group.node_ids.length,
        state: groupState(group, projection),
        width: bounds.right - bounds.left,
        height: bounds.bottom - bounds.top,
      },
      style: {
        width: bounds.right - bounds.left,
        height: bounds.bottom - bounds.top,
      },
      selected: selection?.type === "group" && selection.id === group.id,
      zIndex: -100 + group.workflow_path.length,
    });
  }
  return result;
}

function traceNodePositions(
  nodes: Pick<TraceFlowNode, "id" | "type" | "position">[],
): Record<string, { x: number; y: number }> {
  return Object.fromEntries(
    nodes
      .filter((node) => node.type !== "workflowGroup")
      .map((node) => [node.id, { ...node.position }]),
  );
}

function resizeGroupNodes(
  nodes: TraceFlowNode[],
  groups: WorkflowGroupView[],
  graphNodes: WorkflowGraphView["nodes"],
  edgeRoutes: Record<string, EdgeRoute>,
  projection: RuntimeProjection,
  selection: TraceSelection,
): TraceFlowNode[] {
  const traceNodes = nodes.filter((node) => node.type !== "workflowGroup");
  const boundsByGroup = calculateGroupBounds(
    groups,
    graphNodes,
    traceNodePositions(traceNodes),
    edgeRoutes,
  );
  return [
    ...buildGroupNodes(
      groups,
      projection,
      boundsByGroup,
      selection,
    ),
    ...traceNodes,
  ];
}

function groupState(
  group: WorkflowGroupView | undefined,
  projection: RuntimeProjection,
): RuntimeState {
  if (!group) return "created";
  const states = group.node_ids.map((nodeId) => projection.nodes[nodeId]?.state).filter(Boolean);
  if (states.includes("running")) return "running";
  if (states.includes("waiting")) return "waiting";
  if (states.includes("failed")) return "failed";
  if (states.length > 0 && states.every((state) => state === "completed" || state === "skipped")) {
    return states.includes("completed") ? "completed" : "skipped";
  }
  return states[0] ?? "created";
}
