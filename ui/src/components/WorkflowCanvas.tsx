import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import {
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
  type Node as FlowNode,
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

import { layoutWorkflow, loadSavedLayout, saveLayout } from "../layout";
import type {
  EdgeRuntimeState,
  InvocationDetail,
  NodeExecutionView,
  RuntimeProjection,
  RuntimeState,
  TraceSelection,
  WorkflowGraphView,
  WorkflowGroupView,
  WorkflowNodeView,
} from "../types";

type TraceNodeData = {
  label: string;
  capability: string;
  state: RuntimeState;
  entry: boolean;
  exit: boolean;
  executionCount: number;
  operatorSummary: string | null;
  issue: NodeIssue | null;
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

type TraceFlowNode =
  | FlowNode<TraceNodeData, "trace">
  | FlowNode<GroupNodeData, "workflowGroup">;

const nodeTypes = { trace: TraceNode, workflowGroup: WorkflowGroupNode };

interface WorkflowCanvasProps {
  graph: WorkflowGraphView;
  invocation: InvocationDetail;
  projection: RuntimeProjection;
  followLive: boolean;
  selection: TraceSelection;
  canInvoke: boolean;
  canResume?: boolean;
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
  onInspect,
  onInvoke,
  onResume,
}: WorkflowCanvasProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState<TraceFlowNode>([]);
  const [hoveredEdgeId, setHoveredEdgeId] = useState<string | null>(null);
  const flow = useRef<ReactFlowInstance<TraceFlowNode, FlowEdge> | null>(null);

  const buildLayout = useCallback(
    async (force = false) => {
      const positions =
        (!force && loadSavedLayout(graph.definition_hash)) ||
        (await layoutWorkflow(graph));
      const groupNodes = buildGroupNodes(
        graph.groups ?? [],
        graph.nodes,
        projection,
        positions,
        selection,
      );
      const traceNodes: TraceFlowNode[] = graph.nodes
        .map((node) => {
          const projected = projection.nodes[node.id];
          const capability = `${String(node.capability.kind ?? "operator")}:${String(
            node.capability.id ?? "unknown",
          )}`;
          const latestExecution = projected
            ? invocation.node_executions.find(
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
              operatorSummary: nodeOperatorSummary(projected),
              issue: nodeIssue(latestExecution),
            },
            selected: selection?.type === "node" && selection.id === node.id,
            zIndex: 10,
          };
        });
      setNodes([...groupNodes, ...traceNodes]);
      requestAnimationFrame(() => flow.current?.fitView({ padding: 0.2, duration: 320 }));
    }, [graph, invocation.node_executions, projection, selection, setNodes]);

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
            operatorSummary: nodeOperatorSummary(projected),
            issue: nodeIssue(latestExecution),
          },
        };
      }),
    );
  }, [graph.groups, invocation.node_executions, projection, selection, setNodes]);

  const edges = useMemo<FlowEdge[]>(
    () => {
      const seen = new Set<string>();
      return graph.edges.flatMap((edge) => {
        const projected = projection.edges[edge.id];
        const selected = projected?.selected ?? false;
        const inspected = selection?.type === "edge" && selection.id === edge.id;
        const hovered = hoveredEdgeId === edge.id;
        const edgeState = edgeStateClass(projected?.state, selected);
        const edgeKey = `${edge.from_node}->${edge.to_node}:${edge.id}`;
        if (seen.has(edgeKey)) return [];
        seen.add(edgeKey);
        return {
          id: edge.id,
          source: edge.from_node,
          target: edge.to_node,
          type: "smoothstep",
          animated: followLive && selected && projection.invocation_state === "running",
          label: projected && projected.evaluation_count > 1
            ? `${projected.selected_count}/${projected.evaluation_count}`
            : undefined,
          selected: inspected,
          interactionWidth: 28,
          zIndex: hovered || inspected ? 80 : selected ? 40 : 8,
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
      projection.edges,
      projection.invocation_state,
      selection,
      hoveredEdgeId,
    ],
  );

  const persistPositions = useCallback(() => {
    const positions = Object.fromEntries(
      flow.current
        ?.getNodes()
        .filter((node) => node.type === "trace")
        .map((node) => [node.id, node.position]) ?? [],
    );
    saveLayout(graph.definition_hash, positions);
  }, [graph.definition_hash]);

  const refreshGroupFrames = useCallback(
    (nextNodes: TraceFlowNode[]) =>
      refreshGroupNodes(nextNodes, graph.groups ?? [], graph.nodes, projection, selection),
    [graph.groups, graph.nodes, projection, selection],
  );

  return (
    <section className="workflow-canvas" aria-label="Workflow execution graph">
      <ReactFlow<TraceFlowNode, FlowEdge>
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onNodeDrag={(_event, draggedNode) => {
          setNodes((current) =>
            refreshGroupFrames(
              current.map((node) =>
                node.id === draggedNode.id ? { ...node, position: draggedNode.position } : node,
              ),
            ),
          );
        }}
        onNodeDragStop={() => {
          persistPositions();
          setNodes((current) => refreshGroupFrames(current));
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
        elementsSelectable
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
            className="canvas-action"
            type="button"
            onClick={() => void buildLayout(true)}
            title="Restore automatic layout"
          >
            <RotateCcw size={14} />
            Auto layout
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
    <div className={`trace-node state-${stateClass(traceData.state)} ${selected ? "selected" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="trace-node-heading">
        <span className="state-indicator" />
        <strong>{traceData.label}</strong>
        {traceData.executionCount > 1 && (
          <span className="execution-count">×{traceData.executionCount}</span>
        )}
      </div>
      <div className="trace-node-capability">{traceData.capability}</div>
      {traceData.operatorSummary && (
        <div className="trace-node-operator-summary">
          {traceData.operatorSummary}
        </div>
      )}
      <div className="trace-node-footer">
        <span>{traceData.state}</span>
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

function nodeOperatorSummary(
  node: RuntimeProjection["nodes"][string] | undefined,
): string | null {
  if (!node || !node.operator_call_count) return null;
  const values = [`calls ${node.operator_call_count}`];
  if (node.parallel_call_count) {
    values.push(`${node.latest_operator_kind ?? "parallel"} ×${node.parallel_call_count}`);
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
  graphNodes: WorkflowNodeView[],
  projection: RuntimeProjection,
  positions: Record<string, { x: number; y: number }>,
  selection: TraceSelection,
): TraceFlowNode[] {
  const result: TraceFlowNode[] = [];
  for (const group of [...groups].sort((left, right) => left.workflow_path.length - right.workflow_path.length)) {
    const bounds = groupBounds(group, graphNodes, positions);
    if (!bounds) continue;
    result.push({
      id: group.id,
      type: "workflowGroup",
      position: { x: bounds.x, y: bounds.y },
      selectable: false,
      draggable: false,
      className: "workflow-group-flow-node is-expanded",
      data: {
        label: group.label,
        nodeCount: group.node_ids.length,
        state: groupState(group, projection),
        width: bounds.width,
        height: bounds.height,
      },
      selected: selection?.type === "group" && selection.id === group.id,
      zIndex: -10,
    });
  }
  return result;
}

function refreshGroupNodes(
  current: TraceFlowNode[],
  groups: WorkflowGroupView[],
  graphNodes: WorkflowNodeView[],
  projection: RuntimeProjection,
  selection: TraceSelection,
): TraceFlowNode[] {
  const positions = Object.fromEntries(
    current
      .filter((node) => node.type === "trace")
      .map((node) => [node.id, node.position]),
  );
  const nextGroups = buildGroupNodes(groups, graphNodes, projection, positions, selection);
  const traceNodes = current.filter((node) => node.type === "trace");
  return [...nextGroups, ...traceNodes];
}

function groupBounds(
  group: WorkflowGroupView,
  nodes: WorkflowNodeView[],
  positions: Record<string, { x: number; y: number }>,
): { x: number; y: number; width: number; height: number } | null {
  const values = nodes
    .filter((node) => group.node_ids.includes(node.id))
    .map((node) => positions[node.id])
    .filter(Boolean);
  if (values.length === 0) return null;
  const minX = Math.min(...values.map((value) => value.x));
  const minY = Math.min(...values.map((value) => value.y));
  const maxX = Math.max(...values.map((value) => value.x + 224));
  const maxY = Math.max(...values.map((value) => value.y + 104));
  return {
    x: minX - 26,
    y: minY - 38,
    width: maxX - minX + 52,
    height: maxY - minY + 72,
  };
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
