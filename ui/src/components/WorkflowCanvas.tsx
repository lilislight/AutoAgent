import { useCallback, useEffect, useMemo, useRef } from "react";
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
import { Flag, Play, RotateCcw } from "lucide-react";

import { layoutWorkflow, loadSavedLayout, saveLayout } from "../layout";
import type {
  RuntimeProjection,
  RuntimeState,
  TraceSelection,
  WorkflowGraphView,
} from "../types";

type TraceNodeData = {
  label: string;
  capability: string;
  state: RuntimeState;
  entry: boolean;
  exit: boolean;
  executionCount: number;
};

type TraceFlowNode = FlowNode<TraceNodeData, "trace">;

const nodeTypes = { trace: TraceNode };

interface WorkflowCanvasProps {
  graph: WorkflowGraphView;
  projection: RuntimeProjection;
  followLive: boolean;
  selection: TraceSelection;
  onSelect: (selection: TraceSelection) => void;
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

  const buildLayout = useCallback(
    async (force = false) => {
      const positions =
        (!force && loadSavedLayout(graph.definition_hash)) ||
        (await layoutWorkflow(graph));
      setNodes(
        graph.nodes.map((node) => {
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
        }),
      );
      requestAnimationFrame(() => flow.current?.fitView({ padding: 0.2, duration: 320 }));
    }, [graph, projection.nodes, selection, setNodes]);

  useEffect(() => {
    void buildLayout(false);
    // Layout identity changes only with the immutable graph definition.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [graph.definition_hash]);

  useEffect(() => {
    setNodes((current) =>
      current.map((node) => {
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
  }, [projection.nodes, selection, setNodes]);

  const edges = useMemo<FlowEdge[]>(
    () =>
      graph.edges.map((edge) => {
        const projected = projection.edges[edge.id];
        const selected = projected?.selected ?? false;
        const inspected = selection?.type === "edge" && selection.id === edge.id;
        return {
          id: edge.id,
          source: edge.from_node,
          target: edge.to_node,
          type: "smoothstep",
          animated: followLive && selected && projection.invocation_state === "running",
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
        };
      }),
    [followLive, graph.edges, projection.edges, projection.invocation_state, selection],
  );

  const persistPositions = useCallback(() => {
    const positions = Object.fromEntries(
      flow.current?.getNodes().map((node) => [node.id, node.position]) ?? [],
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
        onNodeClick={(_, node) => onSelect({ type: "node", id: node.id })}
        onEdgeClick={(_, edge) => onSelect({ type: "edge", id: edge.id })}
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

function TraceNode({ data, selected }: NodeProps<TraceFlowNode>) {
  return (
    <div className={`trace-node state-${stateClass(data.state)} ${selected ? "selected" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="trace-node-heading">
        <span className="state-indicator" />
        <strong>{data.label}</strong>
        {data.executionCount > 1 && (
          <span className="execution-count">×{data.executionCount}</span>
        )}
      </div>
      <div className="trace-node-capability">{data.capability}</div>
      <div className="trace-node-footer">
        <span>{data.state}</span>
        <span className="node-flags">
          {data.entry && <Play size={12} aria-label="Entry node" />}
          {data.exit && <Flag size={12} aria-label="Exit node" />}
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
  return "var(--edge)";
}
