import { useEffect, useMemo, useState } from "react";
import type { CSSProperties } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, GitBranch, LoaderCircle, Play, X } from "lucide-react";

import {
  createAuthenticationSession,
  getEarlierEvents,
  getTraceView,
  getHealth,
  listInvocations,
  listSessions,
  listWorkflows,
  subscribeToInvocation,
  submitInvocation,
} from "./api";
import { ExecutionTimeline } from "./components/ExecutionTimeline";
import { InspectorPanel } from "./components/InspectorPanel";
import { ScopeBar } from "./components/ScopeBar";
import { WorkflowCanvas } from "./components/WorkflowCanvas";
import { projectEvents } from "./projection";
import { useTraceUi } from "./state";
import type { RuntimeEvent } from "./types";

export default function App() {
  const queryClient = useQueryClient();
  const ui = useTraceUi();
  const [events, setEvents] = useState<RuntimeEvent[]>([]);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [authToken, setAuthToken] = useState("");
  const [authError, setAuthError] = useState<string | null>(null);
  const [authenticating, setAuthenticating] = useState(false);
  const [timelineCollapsed, setTimelineCollapsed] = useState(false);
  const [timelineHeight, setTimelineHeight] = useState(248);
  const [invokeOpen, setInvokeOpen] = useState(false);
  const [invokeInput, setInvokeInput] = useState("{}");
  const [invokeSessionKey, setInvokeSessionKey] = useState("");
  const [invokeEntryNodeId, setInvokeEntryNodeId] = useState("");
  const [invokeSubmitting, setInvokeSubmitting] = useState(false);
  const [invokeError, setInvokeError] = useState<string | null>(null);
  const [invokeMessage, setInvokeMessage] = useState<string | null>(null);
  const [darkMode, setDarkMode] = useState(
    () => localStorage.getItem("autoagent:theme") === "dark",
  );

  useEffect(() => {
    document.documentElement.dataset.theme = darkMode ? "dark" : "light";
    localStorage.setItem("autoagent:theme", darkMode ? "dark" : "light");
  }, [darkMode]);

  const healthQuery = useQuery({
    queryKey: ["trace-health"],
    queryFn: getHealth,
  });
  const authenticated = healthQuery.data?.authenticated ?? false;
  const workflowQuery = useQuery({
    queryKey: ["workflows"],
    queryFn: listWorkflows,
    enabled: authenticated,
  });
  const workflows = useMemo(() => {
    const values = workflowQuery.data ?? [];
    return [...new Map(values.map((value) => [value.workflow_id, value])).values()];
  }, [workflowQuery.data]);

  useEffect(() => {
    if (!ui.workflowId && workflows.length > 0) ui.setWorkflow(workflows[0].workflow_id);
  }, [ui, workflows]);

  const sessionQuery = useQuery({
    queryKey: ["sessions", ui.workflowId],
    queryFn: () => listSessions(ui.workflowId!),
    enabled: Boolean(ui.workflowId),
  });
  const sessions = sessionQuery.data ?? [];
  useEffect(() => {
    if (!ui.sessionId && sessions.length > 0) {
      ui.setSession(sessions.at(-1)!.id);
    }
  }, [sessions, ui]);

  const invocationQuery = useQuery({
    queryKey: ["invocations", ui.sessionId],
    queryFn: () => listInvocations(ui.sessionId!),
    enabled: Boolean(ui.sessionId),
  });
  const invocations = invocationQuery.data ?? [];
  useEffect(() => {
    if (!ui.invocationId && invocations.length > 0) {
      const current = sessions.find((value) => value.id === ui.sessionId)?.current_invocation_id;
      ui.setInvocation(current || invocations.at(-1)!.id);
    }
  }, [invocations, sessions, ui]);

  const viewQuery = useQuery({
    queryKey: ["trace-view", ui.sessionId, ui.invocationId],
    queryFn: () => getTraceView(ui.sessionId!, ui.invocationId!),
    enabled: Boolean(ui.sessionId && ui.invocationId),
  });
  const queriedView = viewQuery.data;
  const view =
    queriedView &&
    queriedView.session.id === ui.sessionId &&
    queriedView.invocation.id === ui.invocationId
      ? queriedView
      : undefined;

  useEffect(() => {
    if (!view) return;
    setEvents(view.events);
    setHistoryLoaded(view.checkpoint.through_sequence === 0);
    setHistoryError(null);
    const latest = view.projection.through_sequence;
    ui.setCursor(latest, true);
  }, [view?.invocation.id]);

  useEffect(() => {
    if (!view || !ui.sessionId || !ui.invocationId) return;
    const afterSequence = view.projection.through_sequence;
    return subscribeToInvocation(
      ui.sessionId,
      ui.invocationId,
      afterSequence,
      (event) => {
        setEvents((current) => appendEvent(current, event));
        const state = useTraceUi.getState();
        if (state.followLive) state.setCursor(event.sequence, true);
        if (
          state.followLive &&
          (
            event.type === "node.state_changed" ||
            event.type === "operator.call_finished" ||
            event.type === "invocation.state_changed"
          )
        ) {
          void queryClient.invalidateQueries({
            queryKey: ["trace-view", ui.sessionId, ui.invocationId],
          });
        }
      },
      ui.setConnected,
    );
    // The EventSource is recreated only when the selected invocation changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view?.invocation.id]);

  const cursorSequence =
    ui.cursorSequence ?? view?.projection.through_sequence ?? 0;
  const projection = useMemo(
    () => {
      if (!view) return null;
      const checkpoint =
        !historyLoaded && cursorSequence >= view.checkpoint.through_sequence
          ? view.checkpoint
          : undefined;
      return projectEvents(
        view.invocation.id,
        events,
        cursorSequence,
        checkpoint,
      );
    },
    [cursorSequence, events, historyLoaded, view],
  );

  const loadFullHistory = async () => {
    if (!view || !ui.sessionId || !ui.invocationId || historyLoading) return;
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      let collected = [...events];
      let beforeSequence =
        collected[0]?.sequence ?? view.checkpoint.through_sequence + 1;
      while (beforeSequence > 1) {
        const page = await getEarlierEvents(
          ui.sessionId,
          ui.invocationId,
          beforeSequence,
        );
        collected = mergeEvents(page.events, collected);
        if (!page.has_more || page.previous_before_sequence === null) break;
        beforeSequence = page.previous_before_sequence;
      }
      setEvents(collected);
      setHistoryLoaded(true);
    } catch (loadError) {
      setHistoryError(loadError instanceof Error ? loadError.message : String(loadError));
    } finally {
      setHistoryLoading(false);
    }
  };

  const followLatest = () => {
    const latest = events.at(-1)?.sequence ?? view?.projection.through_sequence ?? 0;
    ui.setCursor(latest, true);
    if (ui.sessionId && ui.invocationId) {
      void queryClient.invalidateQueries({
        queryKey: ["trace-view", ui.sessionId, ui.invocationId],
      });
    }
  };

  const submitFromUi = async () => {
    if (!ui.workflowId || invokeSubmitting) return;
    setInvokeSubmitting(true);
    setInvokeError(null);
    setInvokeMessage(null);
    try {
      const parsedInput = parseJsonObject(invokeInput);
      const response = await submitInvocation(ui.workflowId, {
        input: parsedInput,
        session_id: invokeSessionKey.trim() || null,
        entry_node_id: invokeEntryNodeId.trim() || null,
      });
      setInvokeMessage(
        `Created invocation ${response.invocation_id.slice(0, 8)} in session ${response.session_id.slice(0, 8)}. Select it from the Session and Invocation lists to inspect it.`,
      );
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["workflows"] }),
        queryClient.invalidateQueries({ queryKey: ["sessions", response.workflow_id] }),
        queryClient.invalidateQueries({ queryKey: ["sessions", ui.workflowId] }),
        queryClient.invalidateQueries({ queryKey: ["invocations", response.session_id] }),
        queryClient.invalidateQueries({ queryKey: ["invocations", ui.sessionId] }),
      ]);
    } catch (submitError) {
      setInvokeError(submitError instanceof Error ? submitError.message : String(submitError));
    } finally {
      setInvokeSubmitting(false);
    }
  };

  const loading =
    workflowQuery.isLoading ||
    sessionQuery.isLoading ||
    invocationQuery.isLoading ||
    viewQuery.isLoading ||
    (Boolean(ui.sessionId && ui.invocationId) && !view);
  const error = healthQuery.error || workflowQuery.error || sessionQuery.error || invocationQuery.error || viewQuery.error;

  if (healthQuery.isLoading) {
    return (
      <div className="app-shell">
        <StatusScreen
          icon={<LoaderCircle className="spin" size={28} />}
          title="Connecting to AutoAgent Server"
          detail="Checking service access."
        />
      </div>
    );
  }

  if (healthQuery.data?.authentication_required && !authenticated) {
    return (
      <AuthenticationScreen
        token={authToken}
        error={authError}
        pending={authenticating}
        onTokenChange={setAuthToken}
        onSubmit={async () => {
          setAuthenticating(true);
          setAuthError(null);
          try {
            await createAuthenticationSession(authToken);
            setAuthToken("");
            await healthQuery.refetch();
          } catch (authenticationError) {
            setAuthError(
              authenticationError instanceof Error
                ? authenticationError.message
                : String(authenticationError),
            );
          } finally {
            setAuthenticating(false);
          }
        }}
      />
    );
  }

  return (
    <div className="app-shell">
      <ScopeBar
        workflows={workflows}
        sessions={sessions}
        invocations={invocations}
        workflowId={ui.workflowId}
        sessionId={ui.sessionId}
        invocationId={ui.invocationId}
        followLive={ui.followLive}
        connected={ui.connected}
        darkMode={darkMode}
        executionEnabled={healthQuery.data?.execution_enabled ?? false}
        invoking={invokeSubmitting}
        onWorkflowChange={ui.setWorkflow}
        onSessionChange={ui.setSession}
        onInvocationChange={ui.setInvocation}
        onFollowLive={followLatest}
        onOpenInvoke={() => {
          setInvokeOpen(true);
          setInvokeError(null);
          setInvokeMessage(null);
        }}
        onToggleTheme={() => setDarkMode((value) => !value)}
      />
      {invokeOpen && (
        <InvocationLauncher
          workflowId={ui.workflowId}
          input={invokeInput}
          sessionKey={invokeSessionKey}
          entryNodeId={invokeEntryNodeId}
          error={invokeError}
          message={invokeMessage}
          submitting={invokeSubmitting}
          onInputChange={setInvokeInput}
          onSessionKeyChange={setInvokeSessionKey}
          onEntryNodeIdChange={setInvokeEntryNodeId}
          onClose={() => {
            if (!invokeSubmitting) setInvokeOpen(false);
          }}
          onSubmit={submitFromUi}
        />
      )}
      {error ? (
        <StatusScreen
          icon={<AlertTriangle size={28} />}
          title="Trace data could not be loaded"
          detail={error instanceof Error ? error.message : String(error)}
        />
      ) : loading ? (
        <StatusScreen
          icon={<LoaderCircle className="spin" size={28} />}
          title="Loading runtime history"
          detail="Reading workflow snapshots and invocation events."
        />
      ) : view && projection ? (
        <main
          className={`trace-workspace ${timelineCollapsed ? "timeline-collapsed" : ""}`}
          style={
            {
              "--timeline-height": `${timelineCollapsed ? 42 : timelineHeight}px`,
            } as CSSProperties
          }
        >
          <WorkflowCanvas
            graph={view.graph}
            invocation={view.invocation}
            projection={projection}
            followLive={ui.followLive}
            selection={ui.selection}
            onSelect={(selection) => {
              ui.setSelection(selection);
            }}
          />
          {ui.selection && (
            <InspectorPanel
              graph={view.graph}
              invocation={view.invocation}
              events={events}
              projection={projection}
              selection={ui.selection}
              cursorSequence={cursorSequence}
              onClose={() => ui.setSelection(null)}
            />
          )}
          <ExecutionTimeline
            timeline={view.timeline}
            events={events}
            cursorSequence={cursorSequence}
            onCursorChange={(sequence) => ui.setCursor(sequence, false)}
            onSelect={ui.setSelection}
            historyAvailable={!historyLoaded && view.checkpoint.through_sequence > 0}
            historyLoading={historyLoading}
            historyError={historyError}
            collapsed={timelineCollapsed}
            onCollapsedChange={setTimelineCollapsed}
            height={timelineHeight}
            onHeightChange={setTimelineHeight}
            onLoadHistory={() => void loadFullHistory()}
          />
        </main>
      ) : (
        <StatusScreen
          icon={<GitBranch size={28} />}
          title="No invocation selected"
          detail="Run a workflow or select an existing session and invocation."
        />
      )}
    </div>
  );
}

function InvocationLauncher({
  workflowId,
  input,
  sessionKey,
  entryNodeId,
  error,
  message,
  submitting,
  onInputChange,
  onSessionKeyChange,
  onEntryNodeIdChange,
  onClose,
  onSubmit,
}: {
  workflowId: string | null;
  input: string;
  sessionKey: string;
  entryNodeId: string;
  error: string | null;
  message: string | null;
  submitting: boolean;
  onInputChange: (value: string) => void;
  onSessionKeyChange: (value: string) => void;
  onEntryNodeIdChange: (value: string) => void;
  onClose: () => void;
  onSubmit: () => Promise<void>;
}) {
  return (
    <section className="invoke-panel" aria-label="Invoke workflow">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          void onSubmit();
        }}
      >
        <div className="invoke-heading">
          <div>
            <Play size={16} />
            <strong>Invoke workflow</strong>
          </div>
          <button type="button" onClick={onClose} disabled={submitting} aria-label="Close">
            <X size={15} />
          </button>
        </div>
        <label>
          Workflow
          <input value={workflowId ?? ""} readOnly />
        </label>
        <label>
          Session key
          <input
            value={sessionKey}
            onChange={(event) => onSessionKeyChange(event.target.value)}
            placeholder="Optional. Reuse one session by key."
          />
        </label>
        <label>
          Entry node id
          <input
            value={entryNodeId}
            onChange={(event) => onEntryNodeIdChange(event.target.value)}
            placeholder="Optional for single-entry workflows."
          />
        </label>
        <label>
          Input JSON
          <textarea
            value={input}
            onChange={(event) => onInputChange(event.target.value)}
            spellCheck={false}
          />
        </label>
        {message && <p className="invoke-message">{message}</p>}
        {error && <p className="invoke-error">{error}</p>}
        <button type="submit" disabled={!workflowId || submitting}>
          {submitting ? "Submitting..." : "Invoke"}
        </button>
      </form>
    </section>
  );
}

function AuthenticationScreen({
  token,
  error,
  pending,
  onTokenChange,
  onSubmit,
}: {
  token: string;
  error: string | null;
  pending: boolean;
  onTokenChange: (value: string) => void;
  onSubmit: () => Promise<void>;
}) {
  return (
    <main className="authentication-screen">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          void onSubmit();
        }}
      >
        <GitBranch size={24} />
        <div>
          <strong>AutoAgent Trace</strong>
          <span>Enter the access token configured by the service owner.</span>
        </div>
        <label>
          Access token
          <input
            type="password"
            autoComplete="current-password"
            value={token}
            onChange={(event) => onTokenChange(event.target.value)}
            autoFocus
          />
        </label>
        {error && <p>{error}</p>}
        <button type="submit" disabled={pending || token.length === 0}>
          {pending ? "Signing in..." : "Open tracing UI"}
        </button>
      </form>
    </main>
  );
}

function StatusScreen({
  icon,
  title,
  detail,
}: {
  icon: React.ReactNode;
  title: string;
  detail: string;
}) {
  return (
    <main className="status-screen">
      {icon}
      <strong>{title}</strong>
      <span>{detail}</span>
    </main>
  );
}

function appendEvent(values: RuntimeEvent[], event: RuntimeEvent): RuntimeEvent[] {
  if (values.some((value) => value.id === event.id)) return values;
  return [...values, event].sort((left, right) => left.sequence - right.sequence);
}

function mergeEvents(...groups: RuntimeEvent[][]): RuntimeEvent[] {
  const values = new Map<string, RuntimeEvent>();
  for (const group of groups) {
    for (const event of group) values.set(event.id, event);
  }
  return [...values.values()].sort((left, right) => left.sequence - right.sequence);
}

function parseJsonObject(value: string): Record<string, unknown> {
  const trimmed = value.trim();
  if (!trimmed) return {};
  const parsed = JSON.parse(trimmed) as unknown;
  if (parsed === null || Array.isArray(parsed) || typeof parsed !== "object") {
    throw new Error("Input JSON must be an object.");
  }
  return parsed as Record<string, unknown>;
}
