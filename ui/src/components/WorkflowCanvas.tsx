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
import { Boxes, ChevronDown, ChevronRight, Flag, Play, RotateCcw } from "lucide-react";

import { layoutWorkflow, loadSavedLayout, saveLayout } from "../layout";
import type {
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
};

type GroupNodeData = {
  label: string;
  collapsed: boolean;
  nodeCount: number;
  state: RuntimeState;
  width: number;
  height: number;
  onToggle: (id: string) => void;
};

type TraceFlowNode =
  | FlowNode<TraceNodeData, "trace">
  | FlowNode<GroupNodeData, "group">;

const nodeTypes = { trace: TraceNode, group: WorkflowGroupNode };

interface WorkflowCanvasProps {
  graph: WorkflowGraphView;
  projection: RuntimeProjection;
  followLive: boolean;
  selection: TraceSelection;
  onSelect: (selection: TraceSelection, anchor?: { x: number; y: number }) => void;
}

export function WorkflowCanvas({
  graph,
  projection,
  followLive,
  selection,
  onSelect,
}: WorkflowCanvasProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState<TraceFlowNode>([]);
  const flow = useRef<ReactFlowInstance<TraceFlowNode, FlowEdge> | null>(null);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(
    () => loadCollapsedGroups(graph.definition_hash),
  );

  useEffect(() => {
    setCollapsedGroups(loadCollapsedGroups(graph.definition_hash));
  }, [graph.definition_hash]);

  const toggleGroup = useCallback(
    (id: string) => {
      setCollapsedGroups((current) => {
        const next = new Set(current);
        if (next.has(id)) next.delete(id);
        else next.add(id);
        saveCollapsedGroups(graph.definition_hash, next);
        return next;
      });
    },
    [graph.definition_hash],
  );

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
        collapsedGroups,
        selection,
        toggleGroup,
      );
      const hiddenNodeIds = hiddenByCollapsedGroups(graph.groups ?? [], collapsedGroups);
      const traceNodes: TraceFlowNode[] = graph.nodes
        .filter((node) => !hiddenNodeIds.has(node.id))
        .map((node) => {
          const projected = projection.nodes[node.id];
          const capability = `${String(node.capability.kind ?? "operator")}:${String(
            node.capability.id ?? "unknown",
          )}`;
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
            },
            selected: selection?.type === "node" && selection.id === node.id,
          };
        });
      setNodes([...groupNodes, ...traceNodes]);
      requestAnimationFrame(() => flow.current?.fitView({ padding: 0.2, duration: 320 }));
    }, [collapsedGroups, graph, projection, selection, setNodes, toggleGroup]);

  useEffect(() => {
    void buildLayout(false);
    // Layout identity changes only with the immutable graph definition.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [graph.definition_hash]);

  useEffect(() => {
    setNodes((current) =>
      current.map((node) => {
        if (node.type === "group") {
          const data = node.data as GroupNodeData;
          return {
            ...node,
            selected: selection?.type === "group" && selection.id === node.id,
            data: {
              ...data,
              collapsed: collapsedGroups.has(node.id),
              state: groupState(
                graph.groups.find((group) => group.id === node.id),
                projection,
              ),
            },
          };
        }
        const projected = projection.nodes[node.id];
        return {
          ...node,
          selected: selection?.type === "node" && selection.id === node.id,
          data: {
            ...node.data,
            state: projected?.state ?? "created",
            executionCount: projected?.execution_count ?? 0,
          },
        };
      }),
    );
  }, [collapsedGroups, graph.groups, projection, selection, setNodes]);

  const edges = useMemo<FlowEdge[]>(
    () => {
      const hiddenNodeIds = hiddenByCollapsedGroups(graph.groups ?? [], collapsedGroups);
      const endpoint = (nodeId: string) =>
        collapsedEndpoint(nodeId, graph.groups ?? [], collapsedGroups) ?? nodeId;
      const seen = new Set<string>();
      return graph.edges.flatMap((edge) => {
        const projected = projection.edges[edge.id];
        const selected = projected?.selected ?? false;
        const inspected = selection?.type === "edge" && selection.id === edge.id;
        const source = endpoint(edge.from_node);
        const target = endpoint(edge.to_node);
        if (source === target) return [];
        if (hiddenNodeIds.has(edge.from_node) && source === edge.from_node) return [];
        if (hiddenNodeIds.has(edge.to_node) && target === edge.to_node) return [];
        const edgeKey = `${source}->${target}:${edge.id}`;
        if (seen.has(edgeKey)) return [];
        seen.add(edgeKey);
        return {
          id: edge.id,
          source,
          target,
          type: "smoothstep",
          animated: followLive && selected && projection.invocation_state === "running",
          label: projected && projected.evaluation_count > 1
            ? `${projected.selected_count}/${projected.evaluation_count}`
            : undefined,
          selected: inspected,
          markerEnd: {
            type: MarkerType.ArrowClosed,
            color: inspected ? "var(--accent-strong)" : edgeColor(projected?.state, selected),
          },
          style: {
            stroke: inspected
              ? "var(--accent-strong)"
              : edgeColor(projected?.state, selected),
            strokeWidth: inspected || selected ? 2.4 : 1.4,
            opacity: projected ? 1 : 0.28,
          },
          className: `trace-edge ${inspected ? "selected" : ""}`,
        };
      });
    },
    [
      collapsedGroups,
      followLive,
      graph.edges,
      graph.groups,
      projection.edges,
      projection.invocation_state,
      selection,
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

  return (
    <section className="workflow-canvas" aria-label="Workflow execution graph">
      <ReactFlow<TraceFlowNode, FlowEdge>
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onNodeDragStop={persistPositions}
        onNodeClick={(event, node) => {
          onSelect(
            { type: node.type === "group" ? "group" : "node", id: node.id },
            { x: event.clientX, y: event.clientY },
          );
        }}
        onEdgeClick={(event, edge) =>
          onSelect({ type: "edge", id: edge.id }, { x: event.clientX, y: event.clientY })
        }
        onPaneClick={() => onSelect(null)}
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

function WorkflowGroupNode({ id, data, selected }: NodeProps<FlowNode<GroupNodeData, "group">>) {
  const style = data.collapsed
    ? undefined
    : ({ width: data.width, height: data.height } as CSSProperties);
  return (
    <div
      className={`workflow-group-node ${data.collapsed ? "is-collapsed" : "is-expanded"} state-${stateClass(data.state)} ${selected ? "selected" : ""}`}
      style={style}
    >
      <button
        type="button"
        className="group-toggle"
        onClick={(event) => {
          event.stopPropagation();
          data.onToggle(id);
        }}
        title={data.collapsed ? "Expand sub-workflow" : "Collapse sub-workflow"}
      >
        {data.collapsed ? <ChevronRight size={14} /> : <ChevronDown size={14} />}
      </button>
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
      <div className="trace-node-footer">
        <span>{traceData.state}</span>
        <span className="node-flags">
          {traceData.entry && <Play size={12} aria-label="Entry node" />}
          {traceData.exit && <Flag size={12} aria-label="Exit node" />}
        </span>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

function stateClass(state: RuntimeState): string {
  return state.replaceAll("_", "-");
}

function stateColor(state: RuntimeState): string {
  const values: Record<string, string> = {
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

function edgeColor(state: string | undefined, selected: boolean): string {
  if (selected) return "var(--success)";
  if (state === "failed") return "var(--danger)";
  if (state === "skipped") return "var(--line-strong)";
  return "var(--edge)";
}

function buildGroupNodes(
  groups: WorkflowGroupView[],
  graphNodes: WorkflowNodeView[],
  projection: RuntimeProjection,
  positions: Record<string, { x: number; y: number }>,
  collapsed: Set<string>,
  selection: TraceSelection,
  onToggle: (id: string) => void,
): TraceFlowNode[] {
  const hiddenGroups = new Set<string>();
  const result: TraceFlowNode[] = [];
  for (const group of [...groups].sort((left, right) => left.workflow_path.length - right.workflow_path.length)) {
    if (group.parent_group_id && hiddenGroups.has(group.parent_group_id)) {
      hiddenGroups.add(group.id);
      continue;
    }
    const bounds = groupBounds(group, graphNodes, positions);
    if (!bounds) continue;
    const isCollapsed = collapsed.has(group.id);
    if (isCollapsed) hiddenGroups.add(group.id);
    result.push({
      id: group.id,
      type: "group",
      position: { x: bounds.x, y: bounds.y },
      selectable: true,
      draggable: false,
      data: {
        label: group.label,
        collapsed: isCollapsed,
        nodeCount: group.node_ids.length,
        state: groupState(group, projection),
        width: isCollapsed ? 260 : bounds.width,
        height: isCollapsed ? 92 : bounds.height,
        onToggle,
      },
      selected: selection?.type === "group" && selection.id === group.id,
      style: { zIndex: isCollapsed ? 5 : -1 },
    });
  }
  return result;
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

function hiddenByCollapsedGroups(
  groups: WorkflowGroupView[],
  collapsed: Set<string>,
): Set<string> {
  const hidden = new Set<string>();
  for (const group of groups) {
    if (!collapsed.has(group.id)) continue;
    for (const nodeId of group.node_ids) hidden.add(nodeId);
  }
  return hidden;
}

function collapsedEndpoint(
  nodeId: string,
  groups: WorkflowGroupView[],
  collapsed: Set<string>,
): string | null {
  const matches = groups
    .filter((group) => collapsed.has(group.id) && group.node_ids.includes(nodeId))
    .sort((left, right) => right.workflow_path.length - left.workflow_path.length);
  return matches[0]?.id ?? null;
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

function collapsedKey(definitionHash: string): string {
  return `autoagent:groups:${definitionHash}`;
}

function loadCollapsedGroups(definitionHash: string): Set<string> {
  const raw = localStorage.getItem(collapsedKey(definitionHash));
  if (!raw) return new Set();
  try {
    return new Set(JSON.parse(raw) as string[]);
  } catch {
    return new Set();
  }
}

function saveCollapsedGroups(definitionHash: string, values: Set<string>): void {
  localStorage.setItem(collapsedKey(definitionHash), JSON.stringify([...values]));
}
