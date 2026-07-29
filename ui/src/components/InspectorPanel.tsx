import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { useQueries, useQuery } from "@tanstack/react-query";
import { AnimatePresence, motion } from "motion/react";
import {
  Activity,
  Check,
  Clock3,
  Copy,
  Database,
  File,
  FileJson,
  MessageSquareText,
  Route,
  X,
} from "lucide-react";

import {
  getAllUserEvents,
  getEventDetail,
  getRuntimeState,
  subscribeToUserEvents,
} from "../api";
import type {
  EdgeEvaluationView,
  InvocationDetail,
  NodeExecutionView,
  OperatorCallView,
  RuntimeEvent,
  RuntimeProjection,
  TraceSelection,
  TraceBootstrap,
  UserEvent,
  WorkflowGraphView,
} from "../types";
import { useTraceUi } from "../state";
import type { InspectorTab } from "../state";

interface InspectorPanelProps {
  graph: WorkflowGraphView;
  invocation: InvocationDetail;
  events: RuntimeEvent[];
  projection: RuntimeProjection;
  selection: TraceSelection;
  cursorSequence: number;
  capabilities?: TraceBootstrap["capabilities"];
  onClose: () => void;
}

export function InspectorPanel({
  graph,
  invocation,
  events,
  projection,
  selection,
  cursorSequence,
  capabilities,
  onClose,
}: InspectorPanelProps) {
  const tab = useTraceUi((state) => state.inspectorTab);
  const setTab = useTraceUi((state) => state.setInspectorTab);
  const setSelection = useTraceUi((state) => state.setSelection);
  const setCursor = useTraceUi((state) => state.setCursor);
  const [selectedExecutionId, setSelectedExecutionId] = useState<string | null>(null);
  const [selectedEvaluationId, setSelectedEvaluationId] = useState<string | null>(null);
  const [panelWidth, setPanelWidth] = useState(() => preferredPanelWidth());
  const [resizing, setResizing] = useState(false);
  const selectedEventSequence =
    selection?.type === "event" ? selection.sequence : null;
  const eventDetailQuery = useQuery({
    queryKey: ["event-detail", invocation.id, selectedEventSequence],
    queryFn: () => getEventDetail(invocation.id, selectedEventSequence!),
    enabled: selectedEventSequence !== null,
    staleTime: Number.POSITIVE_INFINITY,
    gcTime: Number.POSITIVE_INFINITY,
  });
  const stateQuery = useQuery({
    queryKey: ["runtime-state", invocation.id, cursorSequence],
    queryFn: () => getRuntimeState(invocation.id, cursorSequence),
    enabled: tab === "context" && Boolean(capabilities?.has_historical_runtime_state),
    staleTime: Number.POSITIVE_INFINITY,
  });
  const userEventsQuery = useQuery({
    queryKey: ["user-events", invocation.id],
    queryFn: () => getAllUserEvents(invocation.id),
    enabled: tab === "user_events",
    staleTime: Number.POSITIVE_INFINITY,
  });
  const [liveUserEvents, setLiveUserEvents] = useState<UserEvent[]>([]);
  const [userEventStreamConnected, setUserEventStreamConnected] =
    useState<boolean | null>(null);
  useEffect(() => {
    setLiveUserEvents(userEventsQuery.data ?? []);
  }, [invocation.id, userEventsQuery.data]);
  useEffect(() => {
    if (tab !== "user_events" || userEventsQuery.isLoading) return;
    const terminal = ["completed", "failed", "cancelled", "interrupted"].includes(
      invocation.state,
    );
    if (terminal) return;
    const cursor = liveUserEvents.at(-1)?.sequence ?? 0;
    return subscribeToUserEvents(
      invocation.id,
      cursor,
      (event) => {
        setLiveUserEvents((current) => {
          if (current.some((candidate) => candidate.id === event.id)) {
            return current;
          }
          return [...current, event].sort(
            (left, right) => left.sequence - right.sequence,
          );
        });
      },
      setUserEventStreamConnected,
    );
  }, [
    invocation.id,
    invocation.state,
    tab,
    userEventsQuery.isLoading,
  ]);
  useEffect(() => {
    setSelectedExecutionId(null);
    setSelectedEvaluationId(null);
    setTab("overview");
  }, [invocation.id, selection?.id, selection?.type, setTab]);
  const tabs = useMemo(
    () => inspectorTabs(selection, capabilities),
    [
      capabilities?.has_historical_runtime_state,
      selection?.type,
    ],
  );
  useEffect(() => {
    if (!tabs.some((candidate) => candidate.id === tab)) {
      setTab(tabs[0].id);
    }
  }, [setTab, tab, tabs]);

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
    () => inspectSelection(
      graph,
      invocation,
      eventDetailQuery.data
        ? events.map((event) => event.id === eventDetailQuery.data.id ? eventDetailQuery.data : event)
        : events,
      projection,
      selection,
      cursorSequence,
    ),
    [cursorSequence, eventDetailQuery.data, events, graph, invocation, projection, selection],
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
              <small>
                {projection.invocation_state} · {invocation.event_mode ?? "standard"} · event {cursorSequence}
              </small>
            </div>
            <button className="icon-button" type="button" onClick={onClose} title="Close detail page">
              <X size={16} />
            </button>
          </div>
          <nav className="inspector-tabs" aria-label="Detail sections">
            {tabs.map((candidate) => (
              <TabButton
                key={candidate.id}
                label={candidate.label}
                icon={candidate.icon}
                active={tab === candidate.id}
                onClick={() => setTab(candidate.id)}
              />
            ))}
          </nav>
          <div className="inspector-content">
            <p className="inspector-tab-description">
              {inspectorTabDescription(selection, tab)}
            </p>
            {tab === "overview" && (
              selection?.type === "event" ? (
                <EventList
                  events={inspected.events}
                  selectedEvent={eventDetailQuery.data ?? null}
                  detailLoading={eventDetailQuery.isLoading}
                  detailError={eventDetailQuery.error}
                  onSelectEvent={() => undefined}
                />
              ) : (
                <RuntimeView
                  inspected={inspected}
                  currentExecution={currentExecution}
                  currentEvaluation={currentEvaluation}
                  selectedExecutionId={currentExecution?.id ?? null}
                  selectedEvaluationId={currentEvaluation?.id ?? null}
                  onExecutionChange={setSelectedExecutionId}
                  onEvaluationChange={setSelectedEvaluationId}
                />
              )
            )}
            {tab === "data" && (
              selection?.type === "invocation" ? (
                <InvocationDataView inspected={inspected} />
              ) : (
                <DataView
                  invocationId={invocation.id}
                  inspected={inspected}
                  execution={currentExecution}
                />
              )
            )}
            {tab === "trace" && (
              <PhaseView
                events={inspected.events}
                execution={
                  selection?.type === "node" ||
                  selection?.type === "node_execution"
                    ? currentExecution
                    : null
                }
                internalPhases={Boolean(capabilities?.has_internal_phases)}
                onSelectEvent={(event) => {
                  setCursor(event.sequence, false);
                  setSelection({
                    type: "event",
                    id: event.id,
                    sequence: event.sequence,
                  });
                }}
              />
            )}
            {tab === "context" && (
              <ContextView
                enabled={Boolean(capabilities?.has_historical_runtime_state)}
                loading={stateQuery.isLoading}
                error={stateQuery.error}
                state={stateQuery.data}
                events={inspected.events}
              />
            )}
            {tab === "user_events" && (
              <UserEventView
                events={liveUserEvents}
                loading={userEventsQuery.isLoading}
                error={userEventsQuery.error}
                connected={userEventStreamConnected}
              />
            )}
            {tab === "definition" && (
              <DefinitionView values={{
                definition: inspected.definition,
                contracts: inspected.contracts,
                policies: inspected.policies,
              }} />
            )}
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

function inspectorTabs(
  selection: TraceSelection,
  capabilities?: TraceBootstrap["capabilities"],
): Array<{ id: InspectorTab; label: string; icon: ReactNode }> {
  const stateTab = capabilities?.has_historical_runtime_state
    ? [{ id: "context" as const, label: "State", icon: <Route size={14} /> }]
    : [];
  if (selection?.type === "event") {
    return [
      { id: "overview", label: "Event", icon: <Activity size={14} /> },
      ...stateTab,
    ];
  }
  if (selection?.type === "edge") {
    return [
      { id: "overview", label: "Summary", icon: <Activity size={14} /> },
      { id: "trace", label: "Evaluations", icon: <Clock3 size={14} /> },
      { id: "definition", label: "Definition", icon: <FileJson size={14} /> },
    ];
  }
  if (selection?.type === "invocation") {
    return [
      { id: "overview", label: "Summary", icon: <Activity size={14} /> },
      { id: "data", label: "Input / output", icon: <Database size={14} /> },
      ...(capabilities?.has_events
        ? [{ id: "trace" as const, label: "Trace", icon: <Clock3 size={14} /> }]
        : []),
      ...stateTab,
      {
        id: "user_events",
        label: "User events",
        icon: <MessageSquareText size={14} />,
      },
      { id: "definition", label: "Definition", icon: <FileJson size={14} /> },
    ];
  }
  if (selection?.type === "node" || selection?.type === "node_execution") {
    return [
      { id: "overview", label: "Summary", icon: <Activity size={14} /> },
      { id: "data", label: "Input / output", icon: <Database size={14} /> },
      { id: "trace", label: "Execution", icon: <Clock3 size={14} /> },
      ...stateTab,
      { id: "definition", label: "Definition", icon: <FileJson size={14} /> },
    ];
  }
  return [
    { id: "overview", label: "Summary", icon: <Activity size={14} /> },
    { id: "trace", label: "Trace", icon: <Clock3 size={14} /> },
    ...stateTab,
    { id: "definition", label: "Definition", icon: <FileJson size={14} /> },
  ];
}

function inspectorTabDescription(
  selection: TraceSelection,
  tab: InspectorTab,
): string {
  if (selection?.type === "event") {
    return tab === "context"
      ? "Runtime state reconstructed immediately after this Event."
      : "The immutable Event record, including its timing and recorded values.";
  }
  if (selection?.type === "edge") {
    if (tab === "trace") {
      return "Every evaluation of this Edge and whether its condition selected the route.";
    }
    if (tab === "definition") {
      return "The immutable Edge definition and routing policy used by this Invocation.";
    }
    return "The latest evaluation at the current replay cursor.";
  }
  if (tab === "data") {
    return selection?.type === "invocation"
      ? "The Invocation input and final result retained in every runtime mode."
      : "Mapped input, Operator calls, aggregation result, and final Node output.";
  }
  if (tab === "trace") {
    return "The ordered execution phases recorded for this selection.";
  }
  if (tab === "context") {
    return "Runtime state reconstructed at the current replay cursor.";
  }
  if (tab === "user_events") {
    return "Agent and application events emitted independently from Runtime tracing.";
  }
  if (tab === "definition") {
    return "The immutable Workflow definition, contracts, and policies used for execution.";
  }
  return "Runtime status and the selected execution at the current replay cursor.";
}

function DefinitionView({ values }: { values: unknown }) {
  return (
    <div className="definition-view">
      <JsonBlock value={values} empty="No definition." />
    </div>
  );
}

function UserEventView({
  events,
  loading,
  error,
  connected,
}: {
  events: UserEvent[];
  loading: boolean;
  error: Error | null;
  connected: boolean | null;
}) {
  if (loading) {
    return <div className="inspector-empty">Loading UserEvents…</div>;
  }
  if (error) {
    return <div className="inspector-capability-empty">{error.message}</div>;
  }
  if (events.length === 0) {
    return (
      <div className="inspector-empty">
        No UserEvents were emitted by this Invocation.
      </div>
    );
  }
  return (
    <div className="user-event-view">
      <div className="user-event-stream-state">
        {connected === true ? "Following live events" : `${events.length} events`}
      </div>
      <ol className="event-list">
        {[...events].reverse().map((event) => (
          <li key={event.id}>
            <span className="event-sequence">{event.sequence}</span>
            <div>
              <strong>{event.type}</strong>
              <time>{formatTimestamp(event.occurred_at_ms)}</time>
              <span className="event-runtime-meta">
                {event.node_id}
              </span>
              <JsonBlock value={event} empty="No UserEvent envelope." />
            </div>
          </li>
        ))}
      </ol>
    </div>
  );
}

function RuntimeView({
  inspected,
  currentExecution,
  currentEvaluation,
  selectedExecutionId,
  selectedEvaluationId,
  onExecutionChange,
  onEvaluationChange,
}: {
  inspected: ReturnType<typeof inspectSelection>;
  currentExecution: NodeExecutionView | null;
  currentEvaluation: EdgeEvaluationView | null;
  selectedExecutionId: string | null;
  selectedEvaluationId: string | null;
  onExecutionChange: (id: string) => void;
  onEvaluationChange: (id: string) => void;
}) {
  const { executions, edgeEvaluations: evaluations } = inspected;
  if (inspected.kind === "Invocation") {
    const { error, ...summary } = asRecord(inspected.definition);
    return (
      <div className="runtime-view">
        <Overview values={{
          ...summary,
          node_executions: executions.length,
          edge_evaluations: evaluations.length,
        }} />
        {error !== null && error !== undefined && (
          <FieldBlock label="Invocation error" value={error} />
        )}
        {executions.length === 0 && evaluations.length === 0 && (
          <div className="inspector-capability-empty">
            This mode retains Invocation-level runtime data without Node or Edge history.
          </div>
        )}
      </div>
    );
  }
  if (executions.length === 0 && evaluations.length === 0) {
    return (
      <div className="runtime-view">
      <FailureSummary
        inspected={inspected}
        execution={currentExecution}
        evaluation={currentEvaluation}
      />
      <div className="inspector-empty">No runtime history at this cursor.</div>
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
      <Overview
        values={
          currentExecution
            ? {
                state: currentExecution.state,
                execution_id: currentExecution.id,
                sequence: currentExecution.sequence,
                started_at: currentExecution.started_at_ms == null
                  ? null
                  : formatTimestamp(currentExecution.started_at_ms),
                ended_at: currentExecution.ended_at_ms == null
                  ? null
                  : formatTimestamp(currentExecution.ended_at_ms),
                operator_calls: currentExecution.operator_calls.length,
              }
            : currentEvaluation
              ? {
                  state: currentEvaluation.state,
                  selected: currentEvaluation.selected,
                  source_node: currentEvaluation.source_node_id,
                  target_node: currentEvaluation.target_node_id,
                  reason: currentEvaluation.reason,
                  occurred_at: formatTimestamp(currentEvaluation.created_at_ms),
                }
              : {}
        }
      />
    </div>
  );
}

function InvocationDataView({
  inspected,
}: {
  inspected: ReturnType<typeof inspectSelection>;
}) {
  return (
    <div className="inspector-data-view">
      <FieldBlock label="Invocation input" value={inspected.input} />
      <FieldBlock label="Invocation output" value={inspected.output} />
    </div>
  );
}

function DataView({
  invocationId,
  inspected,
  execution,
}: {
  invocationId: string;
  inspected: ReturnType<typeof inspectSelection>;
  execution: NodeExecutionView | null;
}) {
  const phases = phaseEvents(inspected.events, execution);
  const detailCandidates = phases.filter((event) =>
    [
      "input_mapping.completed",
      "item_selection.completed",
      "operator_call.completed",
      "aggregation.completed",
      "output_binding.completed",
    ].includes(event.event_name) &&
    (event.has_input || event.has_output),
  );
  const detailQueries = useQueries({
    queries: detailCandidates.map((event) => ({
      queryKey: ["event-detail", invocationId, event.sequence],
      queryFn: () => getEventDetail(invocationId, event.sequence),
      staleTime: Number.POSITIVE_INFINITY,
      gcTime: Number.POSITIVE_INFINITY,
    })),
  });
  const detailedById = new Map(
    detailQueries.flatMap((query) => query.data ? [[query.data.id, query.data] as const] : []),
  );
  const detailedPhases = phases.map((event) => detailedById.get(event.id) ?? event);
  const mappedInput = detailedPhases.find((event) => event.event_name === "input_mapping.completed");
  const aggregation = detailedPhases.find((event) => event.event_name === "aggregation.completed");
  const operatorCalls = detailedPhases.filter(
    (event) => event.event_name === "operator_call.completed",
  );
  const lastOperatorCall = operatorCalls.at(-1);
  const binding = [...detailedPhases].reverse().find(
    (event) => event.event_name === "output_binding.completed",
  );
  return (
    <div className="inspector-data-view">
      {detailQueries.some((query) => query.isLoading) && (
        <div className="inspector-empty">Loading recorded phase values…</div>
      )}
      {detailQueries.some((query) => query.error) && (
        <div className="inspector-capability-empty">
          {detailQueries.find((query) => query.error)?.error?.message ??
            "Recorded phase values could not be loaded."}
        </div>
      )}
      <FieldBlock label="Mapped input" value={mappedInput?.output ?? execution?.input ?? inspected.input} />
      <FieldBlock
        label="Operator calls"
        value={operatorCalls.map((event) => ({
          id: event.payload.operator_call_id ?? event.id,
          operator_id: event.payload.operator_id ?? event.subject_id,
          kind: event.payload.kind ?? "direct",
          state: event.payload.state ?? event.status,
          input: event.input,
          output: event.output,
          elapsed_ns: event.elapsed_ns,
          timing: event.timing,
          summary: event.payload.summary,
          streaming: event.payload.streaming,
          stream_chunk_count: event.payload.stream_chunk_count,
        }))}
      />
      {aggregation && <FieldBlock label="Aggregated output" value={aggregation.output ?? aggregation.input} />}
      <FieldBlock
        label="Node output"
        value={
          binding?.output ??
          aggregation?.output ??
          lastOperatorCall?.output ??
          execution?.output ??
          inspected.output
        }
      />
    </div>
  );
}

function PhaseView({
  events,
  execution,
  internalPhases,
  onSelectEvent,
}: {
  events: RuntimeEvent[];
  execution: NodeExecutionView | null;
  internalPhases: boolean;
  onSelectEvent: (event: RuntimeEvent) => void;
}) {
  const phases = phaseEvents(events, execution).filter((event) =>
    event.event_type === "phase" ||
    event.event_name.startsWith("node.") ||
    event.event_name === "edge.evaluated" ||
    event.event_name.startsWith("operator_call.") ||
    event.event_name.startsWith("wait."),
  );
  if (!internalPhases && phases.length === 0) {
    return (
      <div className="inspector-capability-empty">
        Internal phases were not recorded for this Invocation mode.
      </div>
    );
  }
  if (phases.length === 0) {
    return <div className="inspector-empty">No phases at this cursor.</div>;
  }
  return (
    <ol className="phase-pipeline">
      {phases.map((event) => (
        <li key={event.id} className={`state-${String(event.status ?? "completed")}`}>
          <i />
          <div>
            <strong>{phaseLabel(event.event_name)}</strong>
            <span>
              {event.status ?? event.event_type}
              {event.event_name === "operator_call.completed" &&
              event.payload.streaming
                ? ` · streaming · ${Number(
                    event.payload.stream_chunk_count ?? 0,
                  )} chunks`
                : ""}
            </span>
          </div>
          <time>
            {formatTimestamp(event.occurred_at_ms)}
            {event.elapsed_ns != null ? ` · ${formatDurationNs(event.elapsed_ns)}` : ""}
          </time>
          <button
            className="phase-event-trigger"
            type="button"
            onClick={() => onSelectEvent(event)}
          >
            View event
          </button>
          {Object.keys(event.timing).length > 0 && (
            <details>
              <summary>Timing breakdown</summary>
              <TimingBreakdown timing={event.timing} elapsedNs={event.elapsed_ns} />
            </details>
          )}
        </li>
      ))}
    </ol>
  );
}

function ContextView({
  enabled,
  loading,
  error,
  state,
  events,
}: {
  enabled: boolean;
  loading: boolean;
  error: Error | null;
  state: Record<string, unknown> | undefined;
  events: RuntimeEvent[];
}) {
  if (!enabled) {
    return (
      <div className="inspector-capability-empty">
        Historical Runtime Context is available only when this Invocation uses Full tracing.
      </div>
    );
  }
  if (loading) return <div className="inspector-empty">Rebuilding state at this event…</div>;
  if (error) return <div className="inspector-capability-empty">{error.message}</div>;
  const operations = events.flatMap((event) =>
    (event.operations ?? []).map((operation) => ({
      sequence: event.sequence,
      event: event.event_name,
      ...operation,
    })),
  );
  return (
    <div className="context-view">
      <FieldBlock
        label="Reconstructed runtime state"
        description="The complete Session, Invocation, and Node execution state rebuilt by the server at the selected Event sequence."
        value={state}
      />
      <FieldBlock
        label="State deltas in loaded events"
        description="Only the incremental operations carried by Full-mode Events currently loaded in this browser. This is an audit list, not another copy of the reconstructed state."
        value={operations}
      />
    </div>
  );
}

function phaseEvents(
  events: RuntimeEvent[],
  execution: NodeExecutionView | null,
): RuntimeEvent[] {
  if (!execution) return events;
  const scoped = events.filter((event) =>
    event.payload.node_execution_id === execution.id ||
    event.subject_id === execution.id,
  );
  // Older traces may not carry node_execution_id on every phase. Fall back to
  // the Node scope only when the selected execution has no explicit matches;
  // otherwise a looped Node would load values for all of its executions.
  return scoped.length > 0
    ? scoped
    : events.filter((event) => event.node_id === execution.node_id);
}

function phaseLabel(name: string): string {
  return name
    .replaceAll(".", " ")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (value) => value.toUpperCase());
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

function FieldBlock({
  label,
  value,
  description,
}: {
  label: string;
  value: unknown;
  description?: string;
}) {
  return (
    <section className="field-block">
      <h3>{label}</h3>
      {description && <p>{description}</p>}
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
      {artifacts.length > 0 && (
        <div className="artifact-list">
          {artifacts.map((artifact, index) => (
            <ArtifactCard key={`${artifact.uri}:${index}`} artifact={artifact} />
          ))}
        </div>
      )}
      <div className="json-tree-shell">
        <button
          className={`json-copy-button state-${copyState}`}
          type="button"
          onClick={() => void copyJson()}
          aria-label={
            copyState === "copied"
              ? "Copied JSON"
              : copyState === "failed"
                ? "Copy failed"
                : "Copy JSON"
          }
          title={
            copyState === "copied"
              ? "Copied"
              : copyState === "failed"
                ? "Copy failed"
                : "Copy JSON"
          }
        >
          {copyState === "copied" ? <Check size={13} /> : <Copy size={13} />}
        </button>
        <JsonTree value={value} />
      </div>
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

function EventList({
  events,
  selectedEvent,
  detailLoading,
  detailError,
  onSelectEvent,
}: {
  events: RuntimeEvent[];
  selectedEvent: RuntimeEvent | null;
  detailLoading: boolean;
  detailError: Error | null;
  onSelectEvent: (event: RuntimeEvent) => void;
}) {
  if (events.length === 0) return <div className="inspector-empty">No events at this cursor.</div>;
  if (detailLoading) {
    return <div className="inspector-empty">Loading immutable Event detail…</div>;
  }
  if (detailError) {
    return <div className="inspector-capability-empty">{detailError.message}</div>;
  }
  if (selectedEvent) {
    return (
      <article className="selected-event-view">
        <Overview values={{
          sequence: selectedEvent.sequence,
          event: selectedEvent.event_name,
          type: selectedEvent.event_type,
          subject: `${selectedEvent.subject_type}:${selectedEvent.subject_id}`,
          status: selectedEvent.status,
          occurred_at: formatTimestamp(selectedEvent.occurred_at_ms),
          duration: selectedEvent.elapsed_ns == null ? null : formatDurationNs(selectedEvent.elapsed_ns),
        }} />
        {selectedEvent.has_input && <FieldBlock label="Input" value={selectedEvent.input} />}
        {selectedEvent.has_output && <FieldBlock label="Output" value={selectedEvent.output} />}
        {selectedEvent.has_operations && (
          <FieldBlock label="Context operations" value={selectedEvent.operations} />
        )}
        {Object.keys(selectedEvent.timing).length > 0 && (
          <section className="field-block">
            <h3>Timing breakdown</h3>
            <TimingBreakdown
              timing={selectedEvent.timing}
              elapsedNs={selectedEvent.elapsed_ns}
            />
          </section>
        )}
        <details className="event-detail raw-event-detail">
          <summary>Raw Event JSON</summary>
          <JsonBlock value={selectedEvent} empty="No Event detail." />
        </details>
      </article>
    );
  }
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
            <span className="event-runtime-meta">
              {event.status ?? event.event_type}
              {event.elapsed_ns != null ? ` · ${formatDurationNs(event.elapsed_ns)}` : ""}
            </span>
            <button
              className="event-detail-trigger"
              type="button"
              onClick={() => onSelectEvent(event)}
            >
              Inspect immutable detail
            </button>
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
  const projectedExecutions = Object.values(projection.node_executions).map(
    (execution): NodeExecutionView => {
      const related = visibleEvents.filter(
        (event) =>
          event.payload.node_execution_id === execution.execution_id ||
          event.subject_id === execution.execution_id,
      );
      const mappedInput = related.find(
        (event) => event.event_name === "input_mapping.completed",
      )?.output;
      const boundOutput = [...related].reverse().find(
        (event) =>
          event.event_name === "output_binding.completed" ||
          event.event_name === "operator_call.completed",
      );
      return ({
      id: execution.execution_id,
      node_id: execution.node_id,
      sequence: execution.sequence,
      state: execution.state,
      input:
        execution.input ??
        mappedInput ??
        (
          execution.state === "waiting"
            ? { wait_key: Object.keys(projection.active_waits ?? {})[0] }
            : null
        ),
      output:
        execution.output ??
        boundOutput?.output ??
        boundOutput?.input ??
        null,
      error: execution.error,
      execution_scope: [],
      incoming_activations: [],
      edge_evaluations: [],
      operator_calls: related
        .filter(
          (event) =>
            event.event_name === "operator_call.completed" &&
            event.payload.node_execution_id === execution.execution_id,
        )
        .map((event, index): OperatorCallView => ({
          id: String(event.payload.operator_call_id ?? event.id),
          operator_id: String(
            event.payload.operator_id ??
            (event.payload.operator_ids as unknown[] | undefined)?.join(", ") ??
            "parallel",
          ),
          call_no: index + 1,
          kind: String(event.payload.kind ?? "direct"),
          reason: event.payload.reason == null
            ? null
            : String(event.payload.reason),
          item_index: null,
          replica_index: null,
          state: String(event.payload.state ?? event.status ?? "completed"),
          input: event.input,
          output: event.output,
          error:
            event.payload.error && typeof event.payload.error === "object"
              ? event.payload.error as Record<string, unknown>
              : null,
          resource_usage: {
            ...event.timing,
            elapsed_ns: event.elapsed_ns,
            summary: event.payload.summary,
          },
          started_at_ms:
            event.elapsed_ns == null
              ? null
              : event.occurred_at_ms - event.elapsed_ns / 1_000_000,
          ended_at_ms: event.occurred_at_ms,
          created_at_ms: event.occurred_at_ms,
          updated_at_ms: event.occurred_at_ms,
        })),
      resource_usage: execution.timing ?? {},
      started_at_ms: execution.started_at_ms ?? null,
      ended_at_ms: execution.ended_at_ms ?? null,
      created_at_ms: execution.started_at_ms ?? invocation.created_at_ms,
      updated_at_ms: execution.ended_at_ms ?? invocation.updated_at_ms,
    });
    },
  );
  const visibleExecutionIds = new Set(
    visibleEvents
      .filter((event) => event.event_name.startsWith("node."))
      .map((event) => event.payload.node_execution_id)
      .filter((value): value is string => typeof value === "string"),
  );
  const allEvaluations: EdgeEvaluationView[] = visibleEvents
    .filter((event) => event.event_name === "edge.evaluated" && event.edge_id)
    .map((event) => ({
      id: event.id,
      edge_id: event.edge_id!,
      source_execution_id: String(event.payload.source_execution_id ?? ""),
      source_node_id: String(event.payload.source_node_id ?? ""),
      target_node_id: String(event.payload.target_node_id ?? ""),
      state: String(event.payload.state ?? event.status ?? "evaluated"),
      selected: Boolean(event.payload.selected),
      reason: typeof event.payload.reason === "string" ? event.payload.reason : null,
      created_at_ms: event.occurred_at_ms,
    }));

  if (!selection) {
    return baseInvocation(
      { ...invocation, node_executions: projectedExecutions },
      allEvents,
      projection,
    );
  }
  if (selection.type === "invocation") {
    return baseInvocation(
      { ...invocation, node_executions: projectedExecutions },
      allEvents,
      projection,
    );
  }
  if (selection.type === "event") {
    const event = allEvents.find((value) => value.id === selection.id);
    return {
      kind: "Runtime event",
      title: event ? `#${event.sequence} ${event.event_name}` : `#${selection.sequence}`,
      definition: event ?? { id: selection.id, sequence: selection.sequence },
      input: event?.input,
      output: event?.output,
      contracts: {},
      policies: event ? { timing: event.timing } : {},
      executions: [],
      edgeEvaluations: [],
      events: event ? [event] : [],
    };
  }
  if (selection.type === "group") {
    const group = graph.groups.find((value) => value.id === selection.id);
    const groupNodeIds = new Set(group?.node_ids ?? []);
    const groupEdgeIds = new Set(
      graph.edges
        .filter((edge) => {
          if (
            edge.workflow_path &&
            group?.workflow_path &&
            group.workflow_path.every(
              (part, index) => edge.workflow_path?.[index] === part,
            )
          ) {
            return true;
          }
          return groupNodeIds.has(edge.from_node) && groupNodeIds.has(edge.to_node);
        })
        .map((edge) => edge.id),
    );
    const groupEvents = visibleEvents.filter(
      (event) =>
        (event.node_id !== null && groupNodeIds.has(event.node_id)) ||
        (event.edge_id !== null && groupEdgeIds.has(event.edge_id)),
    );
    return {
      kind: "Sub-workflow",
      title: group?.label || selection.id,
      definition: group ? { ...group } : { id: selection.id },
      input: null,
      output: null,
      contracts: {},
      policies: {},
      executions: projectedExecutions.filter(
        (execution) => group?.node_ids.includes(execution.node_id) && visibleExecutionIds.has(execution.id),
      ),
      edgeEvaluations: allEvaluations.filter((evaluation) =>
        groupEdgeIds.has(evaluation.edge_id),
      ),
      events: groupEvents,
    };
  }
  if (selection.type === "node") {
    const node = graph.nodes.find((value) => value.id === selection.id);
    const executions = projectedExecutions.filter(
      (value) => value.node_id === selection.id && visibleExecutionIds.has(value.id),
    );
    const latest = executions.at(-1);
    return {
      kind: "Workflow node",
      title: node?.name || selection.id,
      definition: node
        ? { runtime: projection.nodes[selection.id] ?? null, ...node }
        : { id: selection.id },
      input: latest?.input,
      output: latest?.output,
      contracts: node
        ? {
            input_contract: node.input_contract,
            operator_output_contract: node.operator_output_contract,
            output_contract: node.output_contract,
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
      definition: edge
        ? { runtime: projected ?? null, ...edge }
        : { id: selection.id },
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
    const execution = projectedExecutions.find((value) => value.id === selection.id);
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
      workflow_revision_id: invocation.workflow_revision_id,
      definition_hash: invocation.definition_hash,
      entry_node_id: invocation.entry_node_id,
      event_cursor: projection.through_sequence,
      event_mode: invocation.event_mode,
      live_sequence: invocation.live_sequence,
      durable_sequence: invocation.durable_sequence,
      persistence_status: invocation.persistence_status,
      created_at: formatTimestamp(invocation.created_at_ms),
      updated_at: formatTimestamp(invocation.updated_at_ms),
      elapsed: formatDurationNs(
        Math.max(0, invocation.updated_at_ms - invocation.created_at_ms) *
        1_000_000,
      ),
      error: invocation.error,
    },
    input: invocation.input,
    output: invocation.result,
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

function formatDurationNs(value: number): string {
  if (value < 1_000) return `${value} ns`;
  if (value < 1_000_000) return `${(value / 1_000).toFixed(1)} µs`;
  if (value < 1_000_000_000) return `${(value / 1_000_000).toFixed(2)} ms`;
  return `${(value / 1_000_000_000).toFixed(2)} s`;
}

function TimingBreakdown({
  timing,
  elapsedNs,
}: {
  timing: Record<string, number>;
  elapsedNs: number | null;
}) {
  const entries = Object.entries(timing)
    .filter(([, value]) => Number.isFinite(value))
    .sort((left, right) => right[1] - left[1]);
  if (entries.length === 0) {
    return <div className="inspector-empty">No timing breakdown.</div>;
  }
  const scale = Math.max(elapsedNs ?? 0, ...entries.map(([, value]) => value), 1);
  return (
    <dl className="timing-breakdown">
      {elapsedNs !== null && (
        <div className="timing-breakdown-total">
          <dt>Total elapsed</dt>
          <dd>{formatDurationNs(elapsedNs)}</dd>
        </div>
      )}
      {entries.map(([name, value]) => (
        <div className="timing-breakdown-row" key={name}>
          <dt title={name}>{timingLabel(name)}</dt>
          <dd>{formatDurationNs(value)}</dd>
          <span aria-hidden="true">
            <i style={{ width: `${Math.max(1.5, value / scale * 100)}%` }} />
          </span>
        </div>
      ))}
    </dl>
  );
}

function timingLabel(name: string): string {
  const labels: Record<string, string> = {
    concurrency_wait_ns: "Concurrency wait",
    execution_ns: "Execution",
    executor_queue_ns: "Executor queue",
    retry_backoff_ns: "Retry backoff",
    scheduler_wait_ns: "Scheduler wait",
    thread_pool_queue_ns: "Thread-pool queue",
    stream_consumption_ns: "Stream consumption",
    stream_reduction_ns: "Stream reduction",
  };
  return labels[name] ?? name
    .replace(/_ns$/, "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (value) => value.toUpperCase());
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
