import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { AnimatePresence, motion } from "motion/react";
import { Activity, Braces, File, FileJson, ListTree, ShieldCheck, SlidersHorizontal, X } from "lucide-react";

import type { DetailTab } from "./SelectionMenu";
import type {
  EdgeEvaluationView,
  InvocationDetail,
  NodeExecutionView,
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
  initialTab: DetailTab;
  onClose: () => void;
}

export function InspectorPanel({
  graph,
  invocation,
  events,
  projection,
  selection,
  cursorSequence,
  initialTab,
  onClose,
}: InspectorPanelProps) {
  const [tab, setTab] = useState<DetailTab>(initialTab);
  const [selectedExecutionId, setSelectedExecutionId] = useState<string | null>(null);
  const [selectedEvaluationId, setSelectedEvaluationId] = useState<string | null>(null);
  useEffect(() => {
    setTab(initialTab);
    setSelectedExecutionId(null);
    setSelectedEvaluationId(null);
  }, [initialTab, selection]);

  const inspected = useMemo(
    () => inspectSelection(graph, invocation, events, projection, selection, cursorSequence),
    [cursorSequence, events, graph, invocation, projection, selection],
  );
  const currentExecution =
    inspected.executions.find((value) => value.id === selectedExecutionId) ??
    inspected.executions.at(-1) ??
    null;
  const currentEvaluation =
    inspected.edgeEvaluations.find((value) => value.id === selectedEvaluationId) ??
    inspected.edgeEvaluations.at(-1) ??
    null;

  return (
    <AnimatePresence mode="wait">
      <motion.aside
        key={selection ? `${selection.type}:${selection.id}` : "summary"}
        className="detail-drawer"
        initial={{ opacity: 0, x: 28 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: 18 }}
        transition={{ duration: 0.18 }}
      >
        <div className="inspector-heading">
          <div>
            <span>{inspected.kind}</span>
            <strong>{inspected.title}</strong>
          </div>
          <button className="icon-button" type="button" onClick={onClose} title="Close drawer">
            <X size={16} />
          </button>
        </div>
        <nav className="inspector-tabs" aria-label="Detail sections">
          <TabButton label="Definition" icon={<FileJson size={14} />} active={tab === "definition"} onClick={() => setTab("definition")} />
          <TabButton label="History" icon={<Activity size={14} />} active={tab === "executions"} onClick={() => setTab("executions")} />
          <TabButton label="Data" icon={<Braces size={14} />} active={tab === "data"} onClick={() => setTab("data")} />
          <TabButton label="Contracts" icon={<ShieldCheck size={14} />} active={tab === "contracts"} onClick={() => setTab("contracts")} />
          <TabButton label="Policies" icon={<SlidersHorizontal size={14} />} active={tab === "policies"} onClick={() => setTab("policies")} />
          <TabButton label="Events" icon={<ListTree size={14} />} active={tab === "events"} onClick={() => setTab("events")} />
        </nav>
        <div className="inspector-content">
          {tab === "definition" && (
            <DefinitionView values={inspected.definition} />
          )}
          {tab === "executions" && (
            <HistoryView
              executions={inspected.executions}
              evaluations={inspected.edgeEvaluations}
              selectedExecutionId={currentExecution?.id ?? null}
              selectedEvaluationId={currentEvaluation?.id ?? null}
              onExecutionChange={setSelectedExecutionId}
              onEvaluationChange={setSelectedEvaluationId}
            />
          )}
          {tab === "data" && (
            <div className="structured-value">
              <FieldBlock label="Input" value={currentExecution?.input ?? inspected.input} />
              <FieldBlock
                label="Output"
                value={currentExecution?.output ?? inspected.output ?? currentEvaluation}
              />
            </div>
          )}
          {tab === "contracts" && <JsonBlock value={inspected.contracts} empty="No contracts." />}
          {tab === "policies" && <JsonBlock value={inspected.policies} empty="No policies." />}
          {tab === "events" && <EventList events={inspected.events} />}
        </div>
      </motion.aside>
    </AnimatePresence>
  );
}

function TabButton({
  label,
  icon,
  active,
  onClick,
}: {
  label: string;
  icon: ReactNode;
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

function DefinitionView({ values }: { values: unknown }) {
  return (
    <div className="definition-view">
      <Overview values={asRecord(values)} />
      <JsonBlock value={values} empty="No definition." />
    </div>
  );
}

function HistoryView({
  executions,
  evaluations,
  selectedExecutionId,
  selectedEvaluationId,
  onExecutionChange,
  onEvaluationChange,
}: {
  executions: NodeExecutionView[];
  evaluations: EdgeEvaluationView[];
  selectedExecutionId: string | null;
  selectedEvaluationId: string | null;
  onExecutionChange: (id: string) => void;
  onEvaluationChange: (id: string) => void;
}) {
  if (executions.length === 0 && evaluations.length === 0) {
    return <div className="inspector-empty">No history at this cursor.</div>;
  }
  return (
    <div className="history-view">
      {executions.length > 0 && (
        <label className="history-select">
          Node execution
          <select
            value={selectedExecutionId ?? executions.at(-1)?.id ?? ""}
            onChange={(event) => onExecutionChange(event.target.value)}
          >
            {executions.map((execution) => (
              <option key={execution.id} value={execution.id}>
                #{execution.sequence} {execution.state} {execution.id.slice(0, 8)}
              </option>
            ))}
          </select>
        </label>
      )}
      {evaluations.length > 0 && (
        <label className="history-select">
          Edge evaluation
          <select
            value={selectedEvaluationId ?? evaluations.at(-1)?.id ?? ""}
            onChange={(event) => onEvaluationChange(event.target.value)}
          >
            {evaluations.map((evaluation, index) => (
              <option key={evaluation.id} value={evaluation.id}>
                #{index + 1} {evaluation.state} from {evaluation.source_node_id}
              </option>
            ))}
          </select>
        </label>
      )}
      <JsonBlock value={{ executions, evaluations }} empty="No history." />
    </div>
  );
}

function FieldBlock({ label, value }: { label: string; value: unknown }) {
  return (
    <section className="field-block">
      <h3>{label}</h3>
      <JsonBlock value={value} empty={`No ${label.toLowerCase()}.`} />
    </section>
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

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
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
          </a>
        ) : (
          <code>{artifact.uri}</code>
        )}
        <span>
          {artifact.size_bytes == null ? "External data" : formatBytes(artifact.size_bytes)}
          {artifact.sha256 ? ` sha256 ${artifact.sha256.slice(0, 12)}` : ""}
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
  const visibleExecutionIds = new Set(
    visibleEvents
      .filter((event) => event.type === "node.execution_created" && event.entity_id)
      .map((event) => event.entity_id as string),
  );
  const allEvaluations = invocation.node_executions.flatMap((execution) =>
    execution.edge_evaluations.map((evaluation) => ({
      ...evaluation,
      source_execution_id: evaluation.source_execution_id || execution.id,
      source_node_id: evaluation.source_node_id || execution.node_id,
    })),
  );

  if (!selection) {
    return baseInvocation(invocation, visibleEvents, projection);
  }
  if (selection.type === "group") {
    const group = graph.groups.find((value) => value.id === selection.id);
    return {
      kind: "Sub-workflow",
      title: group?.label || selection.id,
      definition: group ? { ...group } : { id: selection.id },
      input: null,
      output: null,
      contracts: {},
      policies: {},
      executions: invocation.node_executions.filter(
        (execution) => group?.node_ids.includes(execution.node_id) && visibleExecutionIds.has(execution.id),
      ),
      edgeEvaluations: allEvaluations.filter((evaluation) =>
        group?.node_ids.includes(evaluation.source_node_id),
      ),
      events: visibleEvents.filter((event) => event.node_id && group?.node_ids.includes(event.node_id)),
    };
  }
  if (selection.type === "node") {
    const node = graph.nodes.find((value) => value.id === selection.id);
    const executions = invocation.node_executions.filter(
      (value) => value.node_id === selection.id && visibleExecutionIds.has(value.id),
    );
    const latest = executions.at(-1);
    return {
      kind: "Workflow node",
      title: node?.name || selection.id,
      definition: node ? { ...node } : { id: selection.id },
      input: latest?.input,
      output: latest?.output,
      contracts: node
        ? {
            input_contract: node.input_contract,
            operator_output_contract: node.operator_output_contract,
            output_contract: node.output_contract,
            operator_manifests: graph.operator_manifests.filter(
              (manifest) => String(manifest.operator_id ?? manifest.id ?? "") === String(node.capability.id ?? ""),
            ),
          }
        : {},
      policies: node
        ? {
            policy: node.policy,
            input_plan: node.input_plan,
            output_binding: node.output_binding,
          }
        : {},
      executions,
      edgeEvaluations: allEvaluations.filter((value) => value.source_node_id === selection.id),
      events: visibleEvents.filter((event) => event.node_id === selection.id),
    };
  }
  if (selection.type === "edge") {
    const edge = graph.edges.find((value) => value.id === selection.id);
    const evaluations = allEvaluations.filter((value) => value.edge_id === selection.id);
    const projected = projection.edges[selection.id];
    return {
      kind: "Workflow edge",
      title: selection.id,
      definition: edge ? { ...edge } : { id: selection.id },
      input: evaluations.at(-1) ?? null,
      output: null,
      contracts: {},
      policies: edge?.policy ? { policy: edge.policy } : {},
      executions: [],
      edgeEvaluations: evaluations,
      events: visibleEvents.filter((event) => event.edge_id === selection.id),
      overview: projected,
    };
  }
  if (selection.type === "node_execution") {
    const execution = invocation.node_executions.find((value) => value.id === selection.id);
    return {
      kind: "Node execution",
      title: execution ? `${execution.node_id} #${execution.sequence}` : selection.id.slice(0, 8),
      definition: execution ?? { id: selection.id },
      input: execution?.input,
      output: execution?.output,
      contracts: {},
      policies: execution?.resource_usage ? { resource_usage: execution.resource_usage } : {},
      executions: execution ? [execution] : [],
      edgeEvaluations: execution?.edge_evaluations ?? [],
      events: visibleEvents.filter((event) => event.entity_id === selection.id),
    };
  }
  const call = invocation.node_executions
    .flatMap((execution) => execution.operator_calls)
    .find((value) => value.id === selection.id);
  return {
    kind: "Operator call",
    title: call?.operator_id || selection.id.slice(0, 8),
    definition: call ?? { id: selection.id },
    input: call?.input,
    output: call?.output,
    contracts: {},
    policies: call?.resource_usage ? { resource_usage: call.resource_usage } : {},
    executions: [],
    edgeEvaluations: [],
    events: visibleEvents.filter((event) => event.entity_id === selection.id),
  };
}

function baseInvocation(
  invocation: InvocationDetail,
  visibleEvents: RuntimeEvent[],
  projection: RuntimeProjection,
) {
  return {
    kind: "Invocation",
    title: invocation.id.slice(0, 8),
    definition: {
      state: projection.invocation_state,
      workflow_id: invocation.workflow_id,
      workflow_version: invocation.workflow_version,
      entry_node_id: invocation.entry_node_id,
      event_cursor: projection.through_sequence,
    },
    input: invocation.input,
    output: projection.invocation_state === "completed" ? invocation.result : null,
    contracts: {},
    policies: {},
    executions: invocation.node_executions,
    edgeEvaluations: [],
    events: visibleEvents,
  };
}

function formatScalar(value: unknown): string {
  if (value === null || value === undefined) return "-";
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

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
}
