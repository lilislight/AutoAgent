import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { AnimatePresence, motion } from "motion/react";
import { Activity, Check, Copy, File, FileJson, ListTree, ShieldCheck, SlidersHorizontal, X } from "lucide-react";

import type {
  EdgeEvaluationView,
  InvocationDetail,
  NodeExecutionView,
  OperatorCallView,
  RuntimeEvent,
  RuntimeProjection,
  TraceSelection,
  WorkflowGraphView,
} from "../types";
import { useTraceUi, type RuntimeTab } from "../state";

interface InspectorPanelProps {
  graph: WorkflowGraphView;
  invocation: InvocationDetail;
  events: RuntimeEvent[];
  projection: RuntimeProjection;
  selection: TraceSelection;
  cursorSequence: number;
  onResumeWait?: (waitKey: string, output: unknown) => Promise<void>;
  resumePending?: boolean;
  resumeError?: string | null;
  resumeDisabledReason?: string | null;
  onClose: () => void;
}

export function InspectorPanel({
  graph,
  invocation,
  events,
  projection,
  selection,
  cursorSequence,
  onResumeWait,
  resumePending = false,
  resumeError = null,
  resumeDisabledReason = null,
  onClose,
}: InspectorPanelProps) {
  const tab = useTraceUi((state) => state.inspectorTab);
  const setTab = useTraceUi((state) => state.setInspectorTab);
  const runtimeTab = useTraceUi((state) => state.runtimeTab);
  const setRuntimeTab = useTraceUi((state) => state.setRuntimeTab);
  const [selectedExecutionId, setSelectedExecutionId] = useState<string | null>(null);
  const [selectedEvaluationId, setSelectedEvaluationId] = useState<string | null>(null);
  const [panelWidth, setPanelWidth] = useState(() => preferredPanelWidth());
  const [resizing, setResizing] = useState(false);
  useEffect(() => {
    setSelectedExecutionId(null);
    setSelectedEvaluationId(null);
  }, [selection]);

  useEffect(() => {
    if (!resizing) return;
    const onPointerMove = (event: PointerEvent) => {
      const nextWidth = clampPanelWidth(window.innerWidth - event.clientX - 12);
      setPanelWidth(nextWidth);
    };
    const onPointerUp = () => setResizing(false);
    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp, { once: true });
    document.body.classList.add("is-resizing-detail");
    return () => {
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
      document.body.classList.remove("is-resizing-detail");
    };
  }, [resizing]);

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
        className="detail-panel"
        style={{ width: panelWidth }}
        initial={{ opacity: 0, x: 34 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: 24 }}
        transition={{ duration: 0.18 }}
      >
        <button
          className="detail-resize-handle"
          type="button"
          aria-label="Resize detail panel"
          onPointerDown={(event) => {
            event.preventDefault();
            setResizing(true);
          }}
        />
          <div className="inspector-heading">
            <div>
              <span>{inspected.kind}</span>
              <strong>{inspected.title}</strong>
            </div>
            <button className="icon-button" type="button" onClick={onClose} title="Close detail page">
              <X size={16} />
            </button>
          </div>
          <nav className="inspector-tabs" aria-label="Detail sections">
            <TabButton label="Runtime" icon={<Activity size={14} />} active={tab === "runtime"} onClick={() => setTab("runtime")} />
            <TabButton label="Definition" icon={<FileJson size={14} />} active={tab === "definition"} onClick={() => setTab("definition")} />
            <TabButton label="Contracts" icon={<ShieldCheck size={14} />} active={tab === "contracts"} onClick={() => setTab("contracts")} />
            <TabButton label="Policies" icon={<SlidersHorizontal size={14} />} active={tab === "policies"} onClick={() => setTab("policies")} />
            <TabButton label="Events" icon={<ListTree size={14} />} active={tab === "events"} onClick={() => setTab("events")} />
          </nav>
          <div className="inspector-content">
            {tab === "runtime" && (
              <RuntimeView
                inspected={inspected}
                currentExecution={currentExecution}
                currentEvaluation={currentEvaluation}
                runtimeTab={runtimeTab}
                onRuntimeTabChange={setRuntimeTab}
                selectedExecutionId={currentExecution?.id ?? null}
                selectedEvaluationId={currentEvaluation?.id ?? null}
                onExecutionChange={setSelectedExecutionId}
                onEvaluationChange={setSelectedEvaluationId}
                onResumeWait={onResumeWait}
                resumePending={resumePending}
                resumeError={resumeError}
                resumeDisabledReason={resumeDisabledReason}
              />
            )}
            {tab === "definition" && <DefinitionView values={inspected.definition} />}
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
      <JsonBlock value={values} empty="No definition." />
    </div>
  );
}

function RuntimeView({
  inspected,
  currentExecution,
  currentEvaluation,
  runtimeTab,
  onRuntimeTabChange,
  selectedExecutionId,
  selectedEvaluationId,
  onExecutionChange,
  onEvaluationChange,
  onResumeWait,
  resumePending,
  resumeError,
  resumeDisabledReason,
}: {
  inspected: ReturnType<typeof inspectSelection>;
  currentExecution: NodeExecutionView | null;
  currentEvaluation: EdgeEvaluationView | null;
  runtimeTab: RuntimeTab;
  onRuntimeTabChange: (tab: RuntimeTab) => void;
  selectedExecutionId: string | null;
  selectedEvaluationId: string | null;
  onExecutionChange: (id: string) => void;
  onEvaluationChange: (id: string) => void;
  onResumeWait?: (waitKey: string, output: unknown) => Promise<void>;
  resumePending: boolean;
  resumeError: string | null;
  resumeDisabledReason: string | null;
}) {
  const { executions, edgeEvaluations: evaluations } = inspected;
  if (executions.length === 0 && evaluations.length === 0) {
    return (
      <div className="runtime-view">
      <FailureSummary
        inspected={inspected}
        execution={currentExecution}
        evaluation={currentEvaluation}
      />
      <WaitResumeAction
        execution={currentExecution}
        onResumeWait={onResumeWait}
        pending={resumePending}
        error={resumeError}
        disabledReason={resumeDisabledReason}
      />
      <div className="inspector-empty">No runtime history at this cursor.</div>
        <RuntimeTabs active={runtimeTab} onChange={onRuntimeTabChange} />
        <RuntimeTabContent
          active={runtimeTab}
          inspected={inspected}
          execution={currentExecution}
          evaluation={currentEvaluation}
        />
      </div>
    );
  }
  return (
    <div className="runtime-view">
      <FailureSummary
        inspected={inspected}
        execution={currentExecution}
        evaluation={currentEvaluation}
      />
      <WaitResumeAction
        execution={currentExecution}
        onResumeWait={onResumeWait}
        pending={resumePending}
        error={resumeError}
        disabledReason={resumeDisabledReason}
      />
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
      <RuntimeTabs active={runtimeTab} onChange={onRuntimeTabChange} />
      <RuntimeTabContent
        active={runtimeTab}
        inspected={inspected}
        execution={currentExecution}
        evaluation={currentEvaluation}
      />
    </div>
  );
}

function WaitResumeAction({
  execution,
  onResumeWait,
  pending,
  error,
  disabledReason,
}: {
  execution: NodeExecutionView | null;
  onResumeWait?: (waitKey: string, output: unknown) => Promise<void>;
  pending: boolean;
  error: string | null;
  disabledReason: string | null;
}) {
  const [outputText, setOutputText] = useState(
    '{\n  "approved": true,\n  "reviewer": "operator",\n  "note": "Approved from tracing UI."\n}',
  );
  const [localError, setLocalError] = useState<string | null>(null);
  if (!execution || execution.state !== "waiting") return null;

  const waitKey = waitKeyFromExecution(execution);
  const disabled = pending || !onResumeWait || Boolean(disabledReason);
  return (
    <section className="wait-resume-card">
      <div className="wait-resume-heading">
        <div>
          <strong>Waiting for external resume</strong>
          <span>wait_key: {waitKey}</span>
        </div>
      </div>
      <label>
        Resume output JSON
        <textarea
          value={outputText}
          onChange={(event) => {
            setOutputText(event.target.value);
            setLocalError(null);
          }}
          spellCheck={false}
        />
      </label>
      {disabledReason && <p className="wait-resume-error">{disabledReason}</p>}
      {(localError || error) && <p className="wait-resume-error">{localError || error}</p>}
      <button
        type="button"
        disabled={disabled}
        onClick={() => {
          try {
            const output = outputText.trim() ? JSON.parse(outputText) : {};
            setLocalError(null);
            void onResumeWait?.(waitKey, output);
          } catch (parseError) {
            setLocalError(parseError instanceof Error ? parseError.message : String(parseError));
          }
        }}
      >
        {pending ? "Resuming..." : "Resume wait"}
      </button>
    </section>
  );
}

function RuntimeTabs({
  active,
  onChange,
}: {
  active: RuntimeTab;
  onChange: (tab: RuntimeTab) => void;
}) {
  const tabs: Array<[RuntimeTab, string]> = [
    ["input", "Input"],
    ["output", "Output"],
    ["execution", "Execution"],
    ["calls", "Operator calls"],
    ["evaluations", "Edge evaluations"],
  ];
  return (
    <nav className="runtime-tabs" aria-label="Runtime data">
      {tabs.map(([id, label]) => (
        <button
          key={id}
          type="button"
          className={active === id ? "is-active" : ""}
          onClick={() => onChange(id)}
        >
          {label}
        </button>
      ))}
    </nav>
  );
}

function RuntimeTabContent({
  active,
  inspected,
  execution,
  evaluation,
}: {
  active: RuntimeTab;
  inspected: ReturnType<typeof inspectSelection>;
  execution: NodeExecutionView | null;
  evaluation: EdgeEvaluationView | null;
}) {
  if (active === "input") {
    return <FieldBlock label="Input" value={execution?.input ?? inspected.input} />;
  }
  if (active === "output") {
    return <FieldBlock label="Output" value={execution?.output ?? inspected.output ?? evaluation} />;
  }
  if (active === "execution") {
    return <FieldBlock label={execution ? "Execution" : "Edge evaluation"} value={execution ?? evaluation} />;
  }
  if (active === "calls") {
    return <FieldBlock label="Operator calls" value={execution?.operator_calls ?? []} />;
  }
  return <FieldBlock label="Edge evaluations" value={inspected.edgeEvaluations} />;
}

type FailureSummaryItem = {
  source: string;
  code?: string;
  message: string;
  detail?: unknown;
  tone: "danger" | "warning" | "neutral";
};

function FailureSummary({
  inspected,
  execution,
  evaluation,
}: {
  inspected: ReturnType<typeof inspectSelection>;
  execution: NodeExecutionView | null;
  evaluation: EdgeEvaluationView | null;
}) {
  const directCall = operatorCallFromValue(inspected.definition);
  const items = failureSummaryItems({ execution, evaluation, directCall });
  if (items.length === 0) return null;
  return (
    <section className="failure-summary" aria-label="Failure summary">
      <h3>Failure summary</h3>
      {items.map((item, index) => (
        <article
          key={`${item.source}:${item.code ?? item.message}:${index}`}
          className={`failure-card tone-${item.tone}`}
        >
          <div>
            <strong>{item.source}</strong>
            {item.code && <code>{item.code}</code>}
          </div>
          <p>{item.message}</p>
          {item.detail !== undefined && (
            <details>
              <summary>Detail</summary>
              <JsonBlock value={item.detail} empty="No detail." />
            </details>
          )}
        </article>
      ))}
    </section>
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

function waitKeyFromExecution(execution: NodeExecutionView): string {
  const input = asRecord(execution.input);
  const waitKey = input.wait_key;
  if (typeof waitKey === "string" && waitKey.trim()) return waitKey;
  return execution.id;
}

function operatorCallFromValue(value: unknown): OperatorCallView | null {
  const record = asRecord(value);
  if (
    typeof record.id === "string" &&
    typeof record.operator_id === "string" &&
    typeof record.kind === "string" &&
    typeof record.state === "string"
  ) {
    return record as unknown as OperatorCallView;
  }
  return null;
}

function failureSummaryItems({
  execution,
  evaluation,
  directCall,
}: {
  execution: NodeExecutionView | null;
  evaluation: EdgeEvaluationView | null;
  directCall: OperatorCallView | null;
}): FailureSummaryItem[] {
  const items: FailureSummaryItem[] = [];
  const failedCalls = [
    ...(directCall ? [directCall] : []),
    ...(execution?.operator_calls ?? []),
  ].filter((call, index, values) =>
    values.findIndex((value) => value.id === call.id) === index &&
    ["failed", "interrupted"].includes(call.state),
  );

  if (failedCalls.length > 0) {
    const visibleCalls =
      execution?.state === "completed" ? failedCalls.slice(0, 2) : failedCalls.slice(-2);
    for (const call of visibleCalls) {
      const error = errorRecord(call.error);
      const recovered = execution?.state === "completed";
      items.push({
        source: recovered
          ? `Recovered operator call #${call.call_no}`
          : `Operator call #${call.call_no}`,
        code: error.code,
        message: `${call.operator_id} ${call.kind} ${call.state}: ${error.message}`,
        detail: {
          operator_id: call.operator_id,
          kind: call.kind,
          item_index: call.item_index,
          replica_index: call.replica_index,
          error: call.error,
          resource_usage: call.resource_usage,
        },
        tone: recovered ? "warning" : "danger",
      });
    }
  }

  if (execution?.error) {
    const error = errorRecord(execution.error);
    const duplicatedByCall = failedCalls.some(
      (call) => errorRecord(call.error).code === error.code,
    );
    if (!duplicatedByCall || failedCalls.length === 0) {
      items.push({
        source: errorSource(error.code, execution.state),
        code: error.code,
        message: error.message,
        detail: execution.error,
        tone: execution.state === "waiting" ? "warning" : "danger",
      });
    }
  }

  if (evaluation && (evaluation.state === "failed" || evaluation.reason)) {
    items.push({
      source: evaluation.state === "failed" ? "Edge condition" : "Edge decision",
      code: evaluation.state,
      message: evaluation.reason || `Edge was ${evaluation.state}.`,
      detail: evaluation,
      tone: evaluation.state === "failed" ? "danger" : "neutral",
    });
  }

  return items;
}

function errorRecord(error: Record<string, unknown> | null): {
  code?: string;
  message: string;
} {
  if (!error) return { message: "No structured error was recorded." };
  return {
    code: typeof error.code === "string" ? error.code : undefined,
    message: typeof error.message === "string"
      ? error.message
      : stableJsonString(error),
  };
}

function errorSource(code: string | undefined, state: string): string {
  if (code === "RESOURCE_LIMIT_EXCEEDED") return "Policy / resource limit";
  if (code?.includes("MAPPING")) return "Input mapping";
  if (code?.includes("BINDING")) return "Output binding";
  if (code?.includes("AGGREGATION")) return "Output aggregation";
  if (code?.includes("CONDITION")) return "Edge condition";
  if (code?.startsWith("OPERATOR_")) return "Operator";
  if (code?.includes("WAIT")) return "Wait";
  if (state === "cancelled") return "Cancellation";
  if (state === "interrupted") return "Interruption";
  return "Node execution";
}

function JsonBlock({ value, empty }: { value: unknown; empty: string }) {
  const [copyState, setCopyState] = useState<"idle" | "copied" | "failed">("idle");
  if (value === null || value === undefined) {
    return <div className="inspector-empty">{empty}</div>;
  }
  const artifacts = findArtifacts(value);
  const copyJson = async () => {
    try {
      await copyText(stableJsonString(value));
      setCopyState("copied");
    } catch {
      setCopyState("failed");
    }
    window.setTimeout(() => setCopyState("idle"), 1400);
  };
  return (
    <div className="structured-value">
      <div className="json-toolbar">
        <button type="button" onClick={() => void copyJson()}>
          {copyState === "copied" ? <Check size={13} /> : <Copy size={13} />}
          {copyState === "copied"
            ? "Copied"
            : copyState === "failed"
              ? "Copy failed"
              : "Copy JSON"}
        </button>
      </div>
      {artifacts.length > 0 && (
        <div className="artifact-list">
          {artifacts.map((artifact, index) => (
            <ArtifactCard key={`${artifact.uri}:${index}`} artifact={artifact} />
          ))}
        </div>
      )}
      <JsonTree value={value} />
    </div>
  );
}

function JsonTree({ value }: { value: unknown }) {
  return (
    <div className="json-tree">
      <JsonTreeNode name={null} value={value} depth={0} />
    </div>
  );
}

function JsonTreeNode({
  name,
  value,
  depth,
}: {
  name: string | null;
  value: unknown;
  depth: number;
}) {
  const expandable = value !== null && typeof value === "object";
  if (!expandable) {
    return (
      <div className="json-tree-row" style={{ paddingLeft: depth * 12 }}>
        {name !== null && <span className="json-key">{name}</span>}
        <span className={`json-scalar ${typeof value}`}>{formatJsonScalar(value)}</span>
      </div>
    );
  }
  const entries = Array.isArray(value)
    ? value.map((item, index) => [String(index), item] as const)
    : Object.entries(value as Record<string, unknown>);
  const summary = Array.isArray(value) ? `Array(${value.length})` : `Object(${entries.length})`;
  return (
    <details className="json-tree-node" open={depth < 2}>
      <summary style={{ paddingLeft: depth * 12 }}>
        <span className="json-key">{name ?? "root"}</span>
        <span className="json-summary">{summary}</span>
      </summary>
      {entries.length === 0 ? (
        <div className="json-tree-row" style={{ paddingLeft: (depth + 1) * 12 }}>
          <span className="json-scalar">empty</span>
        </div>
      ) : (
        entries.map(([key, item]) => (
          <JsonTreeNode key={key} name={key} value={item} depth={depth + 1} />
        ))
      )}
    </details>
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
      {[...events].reverse().map((event, index) => {
        const localSequence = events.length - index;
        return (
        <li key={event.id}>
          <span
            className="event-sequence"
            title={`Session sequence ${event.sequence}`}
          >
            {localSequence}
          </span>
          <div>
            <strong>{event.type}</strong>
            <time>{formatTimestamp(event.occurred_at_ms)}</time>
            <details className="event-detail">
              <summary>Payload</summary>
              <JsonBlock
                value={{
                  entity_type: event.entity_type,
                  entity_id: event.entity_id,
                  node_id: event.node_id,
                  edge_id: event.edge_id,
                  payload: event.payload,
                }}
                empty="No payload."
              />
            </details>
          </div>
        </li>
        );
      })}
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
  const allEvents = events;
  const visibleExecutionIds = new Set(
    visibleEvents
      .filter((event) => event.type === "node.execution_created" && event.entity_id)
      .map((event) => event.entity_id as string),
  );
  const visibleEdgeEvaluationCounts = new Map<string, number>();
  visibleEvents
    .filter((event) => event.type === "edge.evaluated" && event.edge_id)
    .forEach((event) => {
      const edgeId = event.edge_id as string;
      visibleEdgeEvaluationCounts.set(edgeId, (visibleEdgeEvaluationCounts.get(edgeId) ?? 0) + 1);
    });
  const seenEdgeEvaluationCounts = new Map<string, number>();
  const allEvaluations = invocation.node_executions.flatMap((execution) =>
    execution.edge_evaluations.map((evaluation) => ({
      ...evaluation,
      source_execution_id: evaluation.source_execution_id || execution.id,
      source_node_id: evaluation.source_node_id || execution.node_id,
    })),
  ).filter((evaluation) => {
    const edgeId = evaluation.edge_id;
    const nextIndex = (seenEdgeEvaluationCounts.get(edgeId) ?? 0) + 1;
    seenEdgeEvaluationCounts.set(edgeId, nextIndex);
    return nextIndex <= (visibleEdgeEvaluationCounts.get(edgeId) ?? 0);
  });

  if (!selection) {
    return baseInvocation(invocation, allEvents, projection);
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
      edgeEvaluations: [],
      events: allEvents.filter((event) => event.node_id && group?.node_ids.includes(event.node_id)),
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
      edgeEvaluations: [],
      events: allEvents.filter((event) => event.node_id === selection.id),
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
      events: allEvents.filter((event) => event.edge_id === selection.id),
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
      edgeEvaluations: [],
      events: allEvents.filter((event) => event.entity_id === selection.id),
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
    events: allEvents.filter((event) => event.entity_id === selection.id),
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

function formatJsonScalar(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "undefined";
  if (typeof value === "string") return JSON.stringify(value);
  return String(value);
}

function stableJsonString(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

async function copyText(value: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  document.body.removeChild(textarea);
  if (!copied) throw new Error("Clipboard copy failed.");
}

function preferredPanelWidth(): number {
  if (typeof window === "undefined") return 460;
  return clampPanelWidth(Math.round(window.innerWidth * 0.3));
}

function clampPanelWidth(value: number): number {
  if (typeof window === "undefined") return Math.min(720, Math.max(360, value));
  const max = Math.min(760, Math.max(360, window.innerWidth - 80));
  return Math.min(max, Math.max(360, value));
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
