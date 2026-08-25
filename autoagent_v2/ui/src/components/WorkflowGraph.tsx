import { GitBranch, Layers3, Radio } from "lucide-react";
import { useMemo } from "react";
import { graphGeometry, layoutGraph } from "../graph.js";
import type { TraceEvent, WorkflowSnapshot } from "../types.js";

interface Props {
  workflow: WorkflowSnapshot | null;
  events: TraceEvent[];
}

export function WorkflowGraph({ workflow, events }: Props) {
  const layout = useMemo(
    () =>
      workflow
        ? layoutGraph(workflow.definition.nodes ?? [], workflow.definition.edges ?? [])
        : null,
    [workflow],
  );
  const nodeState = useMemo(() => {
    const states = new Map<string, string>();
    for (const event of events) {
      const node = event.subject_ids.node_id;
      if (node) states.set(node, event.status ?? event.kind.split(".").at(-1) ?? "seen");
    }
    return states;
  }, [events]);

  if (!layout || !workflow) {
    return <Empty icon={<GitBranch size={18} />} text="Select a Workflow to inspect its graph." />;
  }

  const byId = new Map(layout.nodes.map((node) => [node.id, node]));
  const entries = new Set(workflow.definition.entry_node_ids ?? []);
  const exits = new Set(workflow.definition.exit_node_ids ?? []);
  const { nodeWidth, nodeHeight } = graphGeometry;
  return (
    <div className="graph-scroll">
      <svg
        className="workflow-graph"
        viewBox={`0 0 ${layout.width} ${layout.height}`}
        width={Math.max(layout.width, 620)}
        height={Math.max(layout.height, 260)}
        role="img"
        aria-label={`Workflow ${workflow.workflow_id} graph`}
      >
        <defs>
          <marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
            <path d="M0,0 L8,4 L0,8 Z" fill="currentColor" />
          </marker>
        </defs>
        {layout.edges.map((edge, index) => {
          const source = byId.get(edge.source);
          const target = byId.get(edge.target);
          if (!source || !target) return null;
          const sx = source.x + nodeWidth;
          const sy = source.y + nodeHeight / 2;
          const tx = target.x;
          const ty = target.y + nodeHeight / 2;
          const bend = Math.max(34, Math.abs(tx - sx) / 2);
          const path = `M${sx},${sy} C${sx + bend},${sy} ${tx - bend},${ty} ${tx},${ty}`;
          return (
            <g key={edge.id ?? `${edge.source}-${edge.target}-${index}`} className={`edge edge-${edge.on ?? "complete"}`}>
              <path d={path} markerEnd="url(#arrow)" />
              {edge.on === "error" && <text x={(sx + tx) / 2} y={(sy + ty) / 2 - 8}>error</text>}
            </g>
          );
        })}
        {layout.nodes.map((node) => {
          const state = nodeState.get(node.id);
          const kind = executableLabel(node.executable);
          const roles = [entries.has(node.id) ? "entry" : null, exits.has(node.id) ? "exit" : null].filter(Boolean);
          return (
            <g
              key={node.id}
              className={`graph-node ${entries.has(node.id) ? "is-entry" : ""} ${exits.has(node.id) ? "is-exit" : ""} ${state ? `node-${state}` : ""}`}
              transform={`translate(${node.x} ${node.y})`}
            >
              <rect width={nodeWidth} height={nodeHeight} rx="12" />
              <text className="node-title" x="16" y="29">{node.id}</text>
              <text className="node-subtitle" x="16" y="51">{kind}</text>
              {roles.length > 0 && <text className="node-role" x={nodeWidth - 12} y="16" textAnchor="end">{roles.join(" · ")}</text>}
              {state && <circle cx={nodeWidth - 17} cy="18" r="5" />}
            </g>
          );
        })}
      </svg>
      <div className="graph-meta">
        <span><Layers3 size={13} /> {layout.nodes.length} nodes</span>
        <span><GitBranch size={13} /> {layout.edges.length} edges</span>
        <span><Radio size={13} /> {(workflow.definition.loops ?? []).length} loops</span>
        <span><Radio size={13} /> {events.length ? "observed" : "definition"}</span>
      </div>
    </div>
  );
}

function executableLabel(value: unknown): string {
  if (typeof value === "string") return value;
  if (value && typeof value === "object") {
    const candidate = value as Record<string, unknown>;
    return String(candidate.name ?? candidate.id ?? candidate.kind ?? "executable");
  }
  return "executable";
}

function Empty({ icon, text }: { icon: React.ReactNode; text: string }) {
  return <div className="empty-state">{icon}<span>{text}</span></div>;
}
