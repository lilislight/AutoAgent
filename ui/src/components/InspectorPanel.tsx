import { useEffect, useMemo, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { Braces, ExternalLink, File, Info, ListTree, X } from "lucide-react";

import type {
  InvocationDetail,
  RuntimeEvent,
  RuntimeProjection,
  TraceSelection,
  WorkflowGraphView,
} from "../types";

interface InspectorPanelProps {
  graph: WorkflowGraphView;
  invocation: InvocationDetail;
  events: RuntimeEvent[];
  projection: RuntimeProjection;
  selection: TraceSelection;
  cursorSequence: number;
  onClose: () => void;
}

type InspectorTab = "overview" | "input" | "output" | "events";

export function InspectorPanel({
  graph,
  invocation,
  events,
  projection,
  selection,
  cursorSequence,
  onClose,
}: InspectorPanelProps) {
  const [tab, setTab] = useState<InspectorTab>("overview");
  useEffect(() => setTab("overview"), [selection]);
  const inspected = useMemo(
    () => inspectSelection(graph, invocation, events, projection, selection, cursorSequence),
    [cursorSequence, events, graph, invocation, projection, selection],
  );

  return (
    <AnimatePresence mode="wait">
      <motion.aside
        key={selection ? `${selection.type}:${selection.id}` : "summary"}
        className="inspector-panel"
        initial={{ opacity: 0, x: 18 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: 12 }}
        transition={{ duration: 0.18 }}
      >
        <div className="inspector-heading">
          <div>
            <span>{inspected.kind}</span>
            <strong>{inspected.title}</strong>
          </div>
          <button className="icon-button" type="button" onClick={onClose} title="Close inspector">
            <X size={16} />
          </button>
        </div>
        <nav className="inspector-tabs" aria-label="Inspector sections">
          <InspectorTabButton
            label="Overview"
            icon={<Info size={14} />}
            active={tab === "overview"}
            onClick={() => setTab("overview")}
          />
          <InspectorTabButton
            label="Input"
            icon={<Braces size={14} />}
            active={tab === "input"}
            onClick={() => setTab("input")}
          />
          <InspectorTabButton
            label="Output"
            icon={<Braces size={14} />}
            active={tab === "output"}
            onClick={() => setTab("output")}
          />
          <InspectorTabButton
            label="Events"
            icon={<ListTree size={14} />}
            active={tab === "events"}
            onClick={() => setTab("events")}
          />
        </nav>
        <div className="inspector-content">
          {tab === "overview" && <Overview values={inspected.overview} />}
          {tab === "input" && <JsonBlock value={inspected.input} empty="No input at this cursor." />}
          {tab === "output" && (
            <JsonBlock value={inspected.output} empty="No output at this cursor." />
          )}
          {tab === "events" && <EventList events={inspected.events} />}
        </div>
      </motion.aside>
    </AnimatePresence>
  );
}

function InspectorTabButton({
  label,
  icon,
  active,
  onClick,
}: {
  label: string;
  icon: React.ReactNode;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button className={active ? "is-active" : ""} type="button" onClick={onClick}>
      {icon}
      {label}
    </button>
  );
}

function Overview({ values }: { values: Record<string, unknown> }) {
  return (
    <dl className="overview-list">
      {Object.entries(values).map(([label, value]) => (
        <div key={label}>
          <dt>{label.replaceAll("_", " ")}</dt>
          <dd>{formatScalar(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

function JsonBlock({ value, empty }: { value: unknown; empty: string }) {
  if (value === null || value === undefined) {
    return <div className="inspector-empty">{empty}</div>;
  }
  const artifacts = findArtifacts(value);
  return (
    <div className="structured-value">
      {artifacts.length > 0 && (
        <div className="artifact-list">
          {artifacts.map((artifact, index) => (
            <ArtifactCard key={`${artifact.uri}:${index}`} artifact={artifact} />
          ))}
        </div>
      )}
      <pre className="json-block">{JSON.stringify(value, null, 2)}</pre>
    </div>
  );
}

interface ArtifactView {
  uri: string;
  media_type?: string | null;
  size_bytes?: number | null;
  sha256?: string | null;
  metadata?: Record<string, unknown>;
}

function ArtifactCard({ artifact }: { artifact: ArtifactView }) {
  const href = /^https?:\/\//i.test(artifact.uri) ? artifact.uri : null;
  return (
    <div className="artifact-card">
      <File size={17} />
      <div>
        <strong>{artifact.media_type || "Artifact"}</strong>
        {href ? (
          <a href={href} target="_blank" rel="noreferrer">
            {artifact.uri}
            <ExternalLink size={12} />
          </a>
        ) : (
          <code>{artifact.uri}</code>
        )}
        <span>
          {artifact.size_bytes == null ? "External data" : formatBytes(artifact.size_bytes)}
          {artifact.sha256 ? ` · sha256 ${artifact.sha256.slice(0, 12)}` : ""}
        </span>
      </div>
    </div>
  );
}

function findArtifacts(value: unknown): ArtifactView[] {
  if (Array.isArray(value)) return value.flatMap(findArtifacts);
  if (!value || typeof value !== "object") return [];
  const record = value as Record<string, unknown>;
  const tagged = record.__autoagent_artifact__;
  if (tagged && typeof tagged === "object" && "uri" in tagged) {
    return [tagged as ArtifactView];
  }
  return Object.values(record).flatMap(findArtifacts);
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
}

function EventList({ events }: { events: RuntimeEvent[] }) {
  if (events.length === 0) return <div className="inspector-empty">No events at this cursor.</div>;
  return (
    <ol className="event-list">
      {[...events].reverse().map((event) => (
        <li key={event.id}>
          <span className="event-sequence">{event.sequence}</span>
          <div>
            <strong>{event.type}</strong>
            <time>{formatTimestamp(event.occurred_at_ms)}</time>
          </div>
        </li>
      ))}
    </ol>
  );
}

function inspectSelection(
  graph: WorkflowGraphView,
  invocation: InvocationDetail,
  events: RuntimeEvent[],
  projection: RuntimeProjection,
  selection: TraceSelection,
  cursorSequence: number,
) {
  const visibleEvents = events.filter((event) => event.sequence <= cursorSequence);
  if (!selection) {
    return {
      kind: "Invocation",
      title: invocation.id.slice(0, 8),
      overview: {
        state: projection.invocation_state,
        workflow: invocation.workflow_id,
        version: invocation.workflow_version ?? "default",
        node_executions: Object.keys(projection.node_executions).length,
        event_cursor: cursorSequence,
      },
      input: invocation.input,
      output: projection.invocation_state === "completed" ? invocation.result : null,
      events: visibleEvents,
    };
  }
  if (selection.type === "node") {
    const node = graph.nodes.find((value) => value.id === selection.id);
    const projectedNode = projection.nodes[selection.id];
    const execution = projectedNode
      ? projection.node_executions[projectedNode.latest_execution_id]
      : undefined;
    return {
      kind: "Workflow node",
      title: node?.name || selection.id,
      overview: {
        id: selection.id,
        state: projectedNode?.state ?? "not reached",
        capability: node ? `${node.capability.kind}:${node.capability.id}` : "unknown",
        executions: projectedNode?.execution_count ?? 0,
        entry: node?.entry ?? false,
        exit: node?.exit ?? false,
      },
      input: execution?.input,
      output: execution?.output,
      events: visibleEvents.filter((event) => event.node_id === selection.id),
    };
  }
  if (selection.type === "edge") {
    const edge = graph.edges.find((value) => value.id === selection.id);
    const projected = projection.edges[selection.id];
    const latestEvent = [...visibleEvents]
      .reverse()
      .find((event) => event.edge_id === selection.id);
    return {
      kind: "Workflow edge",
      title: selection.id,
      overview: {
        from: edge?.from_node ?? "unknown",
        to: edge?.to_node ?? "unknown",
        state: projected?.state ?? "not evaluated",
        selected: projected?.selected ?? false,
        evaluations: projected?.evaluation_count ?? 0,
      },
      input: latestEvent?.payload,
      output: null,
      events: visibleEvents.filter((event) => event.edge_id === selection.id),
    };
  }
  if (selection.type === "node_execution") {
    const execution = projection.node_executions[selection.id];
    const stored = invocation.node_executions.find((value) => value.id === selection.id);
    return {
      kind: "Node execution",
      title: stored ? `${stored.node_id} #${stored.sequence}` : selection.id.slice(0, 8),
      overview: {
        state: execution?.state ?? "not reached",
        node_id: stored?.node_id ?? "unknown",
        sequence: stored?.sequence ?? "unknown",
        duration_ms: stored?.resource_usage.duration_ms ?? 0,
      },
      input: execution?.input,
      output: execution?.output,
      events: visibleEvents.filter((event) => event.entity_id === selection.id),
    };
  }
  const call = invocation.node_executions
    .flatMap((execution) => execution.operator_calls)
    .find((value) => value.id === selection.id);
  const callState = projection.operator_states[selection.id];
  const finished = callState && callState !== "running";
  return {
    kind: "Operator call",
    title: call?.operator_id || selection.id.slice(0, 8),
    overview: {
      state: callState ?? "not reached",
      kind: call?.kind ?? "unknown",
      call_no: call?.call_no ?? "unknown",
      duration_ms: call?.resource_usage.duration_ms ?? 0,
    },
    input: callState ? call?.input : null,
    output: finished ? call?.output : null,
    events: visibleEvents.filter((event) => event.entity_id === selection.id),
  };
}

function formatScalar(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formatTimestamp(value: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  }).format(value);
}
