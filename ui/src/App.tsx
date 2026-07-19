import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { AlertTriangle, GitBranch, LoaderCircle, Play, X } from "lucide-react";

import {
  createAuthenticationSession,
  getEarlierEvents,
  getLaterEvents,
  getWorkflowGraph,
  getTraceView,
  getHealth,
  listInvocations,
  listRegisteredWorkflows,
  listSessions,
  listWorkflows,
  resumeInvocation,
  submitInvocation,
} from "./api";
import { ExecutionTimeline } from "./components/ExecutionTimeline";
import { InspectorPanel } from "./components/InspectorPanel";
import { ScopeBar } from "./components/ScopeBar";
import { WorkflowCanvas } from "./components/WorkflowCanvas";
import { projectEvents } from "./projection";
import { useTraceUi } from "./state";
import type {
  InvocationDetail,
  InvocationSummary,
  RuntimeEvent,
  RuntimeProjection,
  SessionSummary,
  TimelineView,
  WorkflowGraphView,
  WorkflowSummary,
} from "./types";

const LIVE_EVENT_POLL_MS = 500;
const LIVE_EVENT_ERROR_POLL_MS = 1500;
const LIVE_EVENT_FETCH_LIMIT = 500;

export default function App() {
  const queryClient = useQueryClient();
  const ui = useTraceUi();
  const [events, setEvents] = useState<RuntimeEvent[]>([]);
  const [eventBuffer, setEventBuffer] = useState<RuntimeEvent[]>([]);
  const [fetchAfterSequence, setFetchAfterSequence] = useState(0);
  const eventFetchInFlight = useRef(false);
  const [pendingScope, setPendingScope] = useState<{
    workflowId: string;
    sessionId: string;
    invocationId: string;
  } | null>(null);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [authToken, setAuthToken] = useState("");
  const [authError, setAuthError] = useState<string | null>(null);
  const [authenticating, setAuthenticating] = useState(false);
  const [timelineCollapsed, setTimelineCollapsed] = useState(false);
  const [timelineHeight, setTimelineHeight] = useState(248);
  const [invokeOpen, setInvokeOpen] = useState(false);
  const [invokeWorkflowId, setInvokeWorkflowId] = useState<string | null>(null);
  const [invokeInput, setInvokeInput] = useState("{}");
  const [invokeSessionKey, setInvokeSessionKey] = useState("");
  const [invokeEntryNodeId, setInvokeEntryNodeId] = useState("");
  const [invokeSubmitting, setInvokeSubmitting] = useState(false);
  const [invokeError, setInvokeError] = useState<string | null>(null);
  const [invokeMessage, setInvokeMessage] = useState<string | null>(null);
  const [resumeSubmitting, setResumeSubmitting] = useState(false);
  const [resumeError, setResumeError] = useState<string | null>(null);
  const [liveDraftGraph, setLiveDraftGraph] = useState<WorkflowGraphView | null>(null);
  const [liveDraftInvocation, setLiveDraftInvocation] = useState<InvocationDetail | null>(null);
  const [liveDraftTimeline, setLiveDraftTimeline] = useState<TimelineView | null>(null);
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
    refetchInterval: 5_000,
    refetchIntervalInBackground: true,
  });
  const authenticated = healthQuery.data?.authenticated ?? false;
  const workflowQuery = useQuery({
    queryKey: ["workflows"],
    queryFn: listWorkflows,
    enabled: authenticated,
  });
  const registeredWorkflowQuery = useQuery({
    queryKey: ["registered-workflows"],
    queryFn: listRegisteredWorkflows,
    enabled: authenticated,
  });
  const registeredWorkflows = registeredWorkflowQuery.data ?? [];
  const workflows = useMemo(() => {
    return buildWorkflowDirectory(workflowQuery.data ?? [], registeredWorkflows);
  }, [registeredWorkflows, workflowQuery.data]);
  const invokeWorkflow =
    registeredWorkflows.find(
      (value) => value.workflow_id === (invokeWorkflowId ?? ui.workflowId),
    ) ??
    registeredWorkflows[0] ??
    null;
  const invokeGraphQuery = useQuery({
    queryKey: [
      "workflow-graph",
      invokeWorkflow?.workflow_id,
      invokeWorkflow?.definition_hash,
      invokeWorkflow?.operator_manifest_hash,
    ],
    queryFn: () => getWorkflowGraph(invokeWorkflow!),
    enabled: invokeOpen && Boolean(invokeWorkflow),
  });
  const invokeGraph = invokeGraphQuery.data;
  const invokeSessionQuery = useQuery({
    queryKey: ["sessions", invokeWorkflow?.workflow_id, "invoke"],
    queryFn: () => listSessions(invokeWorkflow!.workflow_id),
    enabled: invokeOpen && Boolean(invokeWorkflow),
  });
  const invokeSessions = invokeSessionQuery.data ?? [];

  useEffect(() => {
    if (pendingScope) return;
    if (!workflowQuery.isSuccess) return;
    if (workflows.length === 0) {
      if (ui.workflowId) ui.setWorkflow(null);
      return;
    }
    if (!ui.workflowId || !workflows.some((value) => value.workflow_id === ui.workflowId)) {
      ui.setWorkflow(workflows[0].workflow_id);
    }
  }, [pendingScope, ui, workflowQuery.isSuccess, workflows]);

  const sessionQuery = useQuery({
    queryKey: ["sessions", ui.workflowId],
    queryFn: () => listSessions(ui.workflowId!),
    enabled: Boolean(ui.workflowId),
  });
  const sessions = sessionQuery.data ?? [];
  useEffect(() => {
    if (pendingScope) return;
    if (!sessionQuery.isSuccess) return;
    if (sessions.length === 0) {
      if (ui.sessionId) ui.setSession(null);
      return;
    }
    // Changing a Workflow deliberately clears the child scope. Do not silently
    // select a Session: the navigator must let the user choose the next level.
    if (ui.sessionId && !sessions.some((value) => value.id === ui.sessionId)) {
      ui.setSession(null);
    }
  }, [pendingScope, sessionQuery.isSuccess, sessions, ui]);

  const invocationQuery = useQuery({
    queryKey: ["invocations", ui.sessionId],
    queryFn: () => listInvocations(ui.sessionId!),
    enabled: Boolean(ui.sessionId),
  });
  const invocations = invocationQuery.data ?? [];
  useEffect(() => {
    if (pendingScope) return;
    if (!invocationQuery.isSuccess) return;
    if (invocations.length === 0) {
      if (ui.invocationId) ui.setInvocation(null);
      return;
    }
    // A Session change clears Invocation intentionally. Keep the graph empty
    // until the user selects a concrete invocation from the third column.
    if (ui.invocationId && !invocations.some((value) => value.id === ui.invocationId)) {
      ui.setInvocation(null);
    }
  }, [invocationQuery.isSuccess, invocations, pendingScope, ui]);

  useEffect(() => {
    if (!pendingScope) return;
    ui.setInvocationScope(
      pendingScope.workflowId,
      pendingScope.sessionId,
      pendingScope.invocationId,
    );
    if (
      sessions.some((value) => value.id === pendingScope.sessionId) &&
      invocations.some((value) => value.id === pendingScope.invocationId)
    ) {
      setPendingScope(null);
    }
  }, [invocations, pendingScope, sessions, ui]);

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
  const activeInvocation = liveDraftInvocation ?? view?.invocation ?? null;
  const pollingActive = activeInvocation ? shouldPollInvocation(activeInvocation.state) : false;
  const backendLive = (
    healthQuery.data?.status === "ok" &&
    (!pollingActive || ui.connected)
  );
  const activeSessionId = ui.sessionId;
  const activeInvocationId = activeInvocation?.id ?? null;

  useEffect(() => {
    if (!view) return;
    if (liveDraftInvocation?.id === view.invocation.id) return;
    setEvents(view.events);
    setEventBuffer([]);
    setFetchAfterSequence(view.projection.through_sequence);
    setHistoryLoaded(view.checkpoint.through_sequence === 0);
    setHistoryError(null);
    const latest = view.projection.through_sequence;
    ui.setCursor(latest, true);
  }, [liveDraftInvocation?.id, view?.invocation.id]);

  const fetchLatestEvents = useCallback(async (): Promise<boolean> => {
    if (
      !activeInvocation ||
      !shouldPollInvocation(activeInvocation.state) ||
      !activeSessionId ||
      !activeInvocationId ||
      eventFetchInFlight.current
    ) {
      return false;
    }
    eventFetchInFlight.current = true;
    try {
      const sessionId = activeSessionId;
      const invocationId = activeInvocationId;
      const page = await getLaterEvents(
        sessionId,
        invocationId,
        fetchAfterSequence,
        LIVE_EVENT_FETCH_LIMIT,
      );
      const state = useTraceUi.getState();
      if (state.sessionId !== sessionId || state.invocationId !== invocationId) return false;
      if (page.events.length > 0) {
        if (state.followLive) {
          setEvents((current) => mergeEvents(current, page.events));
          state.setCursor(page.events.at(-1)!.sequence, true);
        } else {
          setEventBuffer((current) => mergeEvents(current, page.events));
        }
        applyInvocationStateEvents(queryClient, page.events);
        setLiveDraftInvocation((current) =>
          current && current.id === invocationId
            ? applyInvocationStateToDetail(current, page.events)
            : current,
        );
        void queryClient.invalidateQueries({
          queryKey: ["trace-view", sessionId, invocationId],
        });
      }
      setFetchAfterSequence(page.next_after_sequence);
      ui.setConnected(true);
      return true;
    } catch {
      ui.setConnected(false);
      return false;
    } finally {
      eventFetchInFlight.current = false;
    }
  }, [activeInvocation, activeInvocationId, activeSessionId, fetchAfterSequence, queryClient, ui]);

  useEffect(() => {
    if (!activeInvocation || !pollingActive) return;
    let cancelled = false;
    let timeout: number | null = null;
    const poll = async () => {
      const ok = await fetchLatestEvents();
      if (cancelled) return;
      timeout = window.setTimeout(
        poll,
        ok ? LIVE_EVENT_POLL_MS : LIVE_EVENT_ERROR_POLL_MS,
      );
    };
    void poll();
    return () => {
      cancelled = true;
      if (timeout !== null) window.clearTimeout(timeout);
    };
  }, [activeInvocation, fetchLatestEvents, pollingActive]);

  const cursorSequence =
    ui.cursorSequence ?? view?.projection.through_sequence ?? 0;
  const projection = useMemo(
    () => {
      if (liveDraftInvocation) {
        return projectEvents(
          liveDraftInvocation.id,
          events,
          cursorSequence,
        );
      }
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
    [cursorSequence, events, historyLoaded, liveDraftInvocation, view],
  );
  const activeGraph = liveDraftGraph ?? view?.graph ?? null;
  const activeInvocationDetail = liveDraftInvocation ?? view?.invocation ?? null;
  const activeTimeline = view?.timeline ?? liveDraftTimeline ?? null;
  const viewedWorkflow = activeGraph ?? workflows.find(
    (workflow) => workflow.workflow_id === ui.workflowId,
  ) ?? null;
  const viewedWorkflowIsRegistered = Boolean(
    viewedWorkflow && registeredWorkflows.some(
      (workflow) =>
        workflow.workflow_id === viewedWorkflow.workflow_id &&
        workflow.definition_hash === viewedWorkflow.definition_hash &&
        workflow.operator_manifest_hash === viewedWorkflow.operator_manifest_hash,
    ),
  );
  const draftProjection = useMemo(
    () => (invokeGraph ? createDraftProjection(invokeGraph) : null),
    [invokeGraph],
  );
  const draftInvocation = useMemo(
    () => (invokeGraph ? createDraftInvocation(invokeGraph) : null),
    [invokeGraph],
  );

  useEffect(() => {
    if (!invokeOpen || !invokeGraph) return;
    if (
      invokeEntryNodeId &&
      invokeGraph.entry_node_ids.includes(invokeEntryNodeId)
    ) {
      return;
    }
    setInvokeEntryNodeId("");
  }, [invokeEntryNodeId, invokeGraph, invokeOpen]);

  const loadFullHistory = useCallback(async () => {
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
  }, [events, historyLoading, ui.invocationId, ui.sessionId, view]);

  useEffect(() => {
    if (!view || shouldPollInvocation(view.invocation.state) || historyLoaded || historyLoading) return;
    if (view.checkpoint.through_sequence <= 0) return;
    void loadFullHistory();
  }, [historyLoaded, historyLoading, loadFullHistory, view]);

  const flushBufferedEvents = useCallback(() => {
    const flushed = mergeEvents(events, eventBuffer);
    setEvents(flushed);
    setEventBuffer([]);
    const latest = flushed.at(-1);
    if (latest && ui.followLive) ui.setCursor(latest.sequence, true);
    return flushed;
  }, [eventBuffer, events, ui]);

  const followLatest = () => {
    const flushed = flushBufferedEvents();
    const latest = flushed.at(-1)?.sequence ?? view?.projection.through_sequence ?? 0;
    ui.setCursor(latest, true);
    if (view && shouldPollInvocation(view.invocation.state)) void fetchLatestEvents();
    if (ui.sessionId && ui.invocationId) {
      void queryClient.invalidateQueries({
        queryKey: ["trace-view", ui.sessionId, ui.invocationId],
      });
    }
  };

  const toggleTimelineMode = () => {
    if (!ui.followLive) {
      followLatest();
      return;
    }
    // The first Replay starts at the first available runtime event. Later
    // switches restore the last cursor selected by a timeline click or chip.
    ui.setCursor(ui.replayCursorSequence ?? events[0]?.sequence ?? 0, false);
  };

  const submitFromUi = async () => {
    const workflowId = invokeWorkflow?.workflow_id;
    if (!workflowId || !invokeEntryNodeId || !invokeGraph || invokeSubmitting) return;
    setInvokeSubmitting(true);
    setInvokeError(null);
    setInvokeMessage(null);
    try {
      const parsedInput = parseJsonObject(invokeInput);
      const response = await submitInvocation(workflowId, {
        input: parsedInput,
        session_id: invokeSessionKey.trim() || null,
        entry_node_id: invokeEntryNodeId.trim() || null,
      });
      setInvokeMessage(`Created invocation ${response.invocation_id.slice(0, 8)}.`);
      setEvents([]);
      setEventBuffer([]);
      setFetchAfterSequence(0);
      setLiveDraftGraph(invokeGraph);
      setLiveDraftInvocation(createLiveDraftInvocation(
        invokeGraph,
        response.invocation_id,
        response.state,
        invokeEntryNodeId,
        parsedInput,
      ));
      setLiveDraftTimeline(createLiveDraftTimeline(response.invocation_id));
      setPendingScope({
        workflowId: response.workflow_id,
        sessionId: response.session_id,
        invocationId: response.invocation_id,
      });
      upsertSubmittedScope(queryClient, {
        workflowId: response.workflow_id,
        sessionId: response.session_id,
        invocationId: response.invocation_id,
        sessionKey: invokeSessionKey.trim() || null,
        entryNodeId: invokeEntryNodeId,
        state: response.state,
      });
      ui.setInvocationScope(response.workflow_id, response.session_id, response.invocation_id);
      ui.setCursor(0, true);
      setInvokeOpen(false);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["workflows"] }),
        queryClient.invalidateQueries({ queryKey: ["registered-workflows"] }),
        queryClient.invalidateQueries({ queryKey: ["sessions", response.workflow_id] }),
        queryClient.invalidateQueries({ queryKey: ["invocations", response.session_id] }),
      ]);
    } catch (submitError) {
      setInvokeError(submitError instanceof Error ? submitError.message : String(submitError));
    } finally {
      setInvokeSubmitting(false);
    }
  };

  const resumeSelectedWait = async (waitKey: string, output: unknown) => {
    if (!activeGraph || !view?.session.session_key || resumeSubmitting) return;
    setResumeSubmitting(true);
    setResumeError(null);
    try {
      const response = await resumeInvocation(activeGraph.workflow_id, {
        session_id: view.session.session_key,
        wait_key: waitKey,
        output,
      });
      upsertSubmittedScope(queryClient, {
        workflowId: response.workflow_id,
        sessionId: response.session_id,
        invocationId: response.invocation_id,
        sessionKey: view.session.session_key,
        entryNodeId: activeInvocationDetail?.entry_node_id ?? "",
        state: response.state,
      });
      await Promise.all([
        queryClient.invalidateQueries({
          queryKey: ["trace-view", response.session_id, response.invocation_id],
        }),
        queryClient.invalidateQueries({ queryKey: ["sessions", response.workflow_id] }),
        queryClient.invalidateQueries({ queryKey: ["invocations", response.session_id] }),
      ]);
      void fetchLatestEvents();
    } catch (resumeError) {
      setResumeError(resumeError instanceof Error ? resumeError.message : String(resumeError));
    } finally {
      setResumeSubmitting(false);
    }
  };

  const clearLiveDraft = () => {
    setLiveDraftGraph(null);
    setLiveDraftInvocation(null);
    setLiveDraftTimeline(null);
    setEventBuffer([]);
  };

  const loading =
    workflowQuery.isLoading ||
    sessionQuery.isLoading ||
    invocationQuery.isLoading ||
    (viewQuery.isLoading && !liveDraftGraph) ||
    (Boolean(ui.sessionId && ui.invocationId) && !view && !liveDraftGraph);
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
        invocationState={projection?.invocation_state ?? view?.invocation.state ?? null}
        viewedWorkflow={viewedWorkflow}
        viewedWorkflowIsRegistered={viewedWorkflowIsRegistered}
        followLive={ui.followLive}
        backendLive={backendLive}
        darkMode={darkMode}
        executionEnabled={healthQuery.data?.execution_enabled ?? false}
        canInvoke={registeredWorkflows.length > 0}
        invoking={invokeSubmitting}
        onScopeChange={(workflowId, sessionId, invocationId) => {
          clearLiveDraft();
          ui.setInvocationScope(workflowId, sessionId, invocationId);
        }}
        onFollowLive={toggleTimelineMode}
        onOpenInvoke={() => {
          const workflowId =
            registeredWorkflows.find((workflow) => workflow.workflow_id === ui.workflowId)
              ?.workflow_id ??
            registeredWorkflows[0]?.workflow_id ??
            null;
          setInvokeWorkflowId(workflowId);
          const currentSession = sessions.find((value) => value.id === ui.sessionId);
          setInvokeSessionKey(currentSession?.session_key ?? "");
          setInvokeEntryNodeId("");
          setInvokeInput("{}");
          setInvokeOpen(true);
          setInvokeError(null);
          setInvokeMessage(null);
        }}
        onToggleTheme={() => setDarkMode((value) => !value)}
      />
      {invokeOpen && (
        <InvocationLauncher
          workflows={registeredWorkflows}
          sessions={invokeSessions}
          workflowId={invokeWorkflow?.workflow_id ?? null}
          input={invokeInput}
          sessionKey={invokeSessionKey}
          entryNodeId={invokeEntryNodeId}
          entryNodeIds={invokeGraph?.entry_node_ids ?? []}
          error={invokeError}
          message={invokeMessage}
          submitting={invokeSubmitting}
          graphLoading={invokeGraphQuery.isLoading}
          onWorkflowChange={(workflowId) => {
            setInvokeWorkflowId(workflowId);
            setInvokeEntryNodeId("");
            setInvokeError(null);
          }}
          onInputChange={setInvokeInput}
          onSessionKeyChange={setInvokeSessionKey}
          onSessionSelect={(sessionKey) => setInvokeSessionKey(sessionKey)}
          onEntryNodeIdChange={setInvokeEntryNodeId}
          onClose={() => {
            if (!invokeSubmitting) setInvokeOpen(false);
          }}
          onSubmit={submitFromUi}
        />
      )}
      {invokeOpen ? (
        invokeGraph && draftProjection && draftInvocation ? (
          <main
            className={`trace-workspace invoke-draft-workspace ${timelineCollapsed ? "timeline-collapsed" : ""}`}
            style={{ "--timeline-height": "42px" } as CSSProperties}
          >
            <WorkflowCanvas
              graph={invokeGraph}
              invocation={draftInvocation}
              projection={draftProjection}
              followLive={false}
              selection={
                invokeEntryNodeId
                  ? { type: "node", id: invokeEntryNodeId }
                  : null
              }
              onSelect={(selection) => {
                if (!selection || selection.type !== "node") return;
                if (!invokeGraph.entry_node_ids.includes(selection.id)) {
                  setInvokeError("Select an entry node to start this workflow.");
                  return;
                }
                setInvokeEntryNodeId(selection.id);
                setInvokeError(null);
              }}
            />
          </main>
        ) : (
          <StatusScreen
            icon={<LoaderCircle className="spin" size={28} />}
            title="Loading workflow graph"
            detail="Select a workflow, then click an entry node to invoke it."
          />
        )
      ) : error ? (
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
      ) : activeGraph && activeInvocationDetail && activeTimeline && projection ? (
        <main
          className={`trace-workspace ${timelineCollapsed ? "timeline-collapsed" : ""}`}
          style={
            {
              "--timeline-height": `${timelineCollapsed ? 42 : timelineHeight}px`,
            } as CSSProperties
          }
        >
          <WorkflowCanvas
            graph={activeGraph}
            invocation={activeInvocationDetail}
            projection={projection}
            followLive={ui.followLive}
            selection={ui.selection}
            onSelect={(selection) => {
              ui.setSelection(selection);
            }}
          />
          {ui.selection && (
            <InspectorPanel
              graph={activeGraph}
              invocation={activeInvocationDetail}
              events={events}
              projection={projection}
              selection={ui.selection}
              cursorSequence={cursorSequence}
              onResumeWait={resumeSelectedWait}
              resumePending={resumeSubmitting}
              resumeError={resumeError}
              resumeDisabledReason={
                view?.session.session_key
                  ? null
                  : "This invocation has no external session key, so it cannot be resumed from the UI."
              }
              onClose={() => ui.setSelection(null)}
            />
          )}
          <ExecutionTimeline
            timeline={activeTimeline}
            events={events}
            cursorSequence={cursorSequence}
            followLive={ui.followLive}
            onCursorChange={(sequence) => ui.setCursor(sequence, false)}
            onSelect={ui.setSelection}
            historyAvailable={!liveDraftGraph && !historyLoaded && (view?.checkpoint.through_sequence ?? 0) > 0}
            historyLoading={historyLoading}
            historyError={historyError}
            bufferedEventCount={eventBuffer.length}
            collapsed={timelineCollapsed}
            onCollapsedChange={setTimelineCollapsed}
            height={timelineHeight}
            onHeightChange={setTimelineHeight}
            onLoadHistory={() => void loadFullHistory()}
            onFlushBufferedEvents={flushBufferedEvents}
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
  workflows,
  sessions,
  workflowId,
  input,
  sessionKey,
  entryNodeId,
  entryNodeIds,
  error,
  message,
  submitting,
  graphLoading,
  onWorkflowChange,
  onInputChange,
  onSessionKeyChange,
  onSessionSelect,
  onEntryNodeIdChange,
  onClose,
  onSubmit,
}: {
  workflows: WorkflowSummary[];
  sessions: Array<{ id: string; session_key: string | null }>;
  workflowId: string | null;
  input: string;
  sessionKey: string;
  entryNodeId: string;
  entryNodeIds: string[];
  error: string | null;
  message: string | null;
  submitting: boolean;
  graphLoading: boolean;
  onWorkflowChange: (value: string | null) => void;
  onInputChange: (value: string) => void;
  onSessionKeyChange: (value: string) => void;
  onSessionSelect: (value: string) => void;
  onEntryNodeIdChange: (value: string) => void;
  onClose: () => void;
  onSubmit: () => Promise<void>;
}) {
  const knownSessions = sessions.filter((value) => value.session_key);
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
          <select
            value={workflowId ?? ""}
            onChange={(event) => onWorkflowChange(event.target.value || null)}
            disabled={submitting || workflows.length === 0}
          >
            <option value="">Select workflow</option>
            {workflows.map((workflow) => (
              <option key={workflow.workflow_id} value={workflow.workflow_id}>
                {workflow.name || workflow.workflow_id} · {formatWorkflowRevision(
                  workflow.workflow_version,
                  workflow.definition_hash,
                )}
              </option>
            ))}
          </select>
        </label>
        <label>
          Existing session
          <select
            value=""
            disabled={submitting || knownSessions.length === 0}
            onChange={(event) => {
              if (event.target.value) onSessionSelect(event.target.value);
            }}
          >
            <option value="">Choose to reuse, or type below</option>
            {knownSessions.map((session) => (
              <option key={session.id} value={session.session_key ?? ""}>
                {session.session_key}
              </option>
            ))}
          </select>
        </label>
        <label>
          Session key
          <input
            value={sessionKey}
            onChange={(event) => onSessionKeyChange(event.target.value)}
            placeholder="Optional. Empty creates a new session."
            disabled={submitting}
          />
        </label>
        <label>
          Entry node
          <select
            value={entryNodeId}
            onChange={(event) => onEntryNodeIdChange(event.target.value)}
            disabled={submitting || graphLoading || entryNodeIds.length === 0}
          >
            <option value="">
              {graphLoading ? "Loading graph" : "Click an entry node on the graph"}
            </option>
            {entryNodeIds.map((nodeId) => (
              <option key={nodeId} value={nodeId}>
                {nodeId}
              </option>
            ))}
          </select>
        </label>
        <label>
          Input JSON
          <textarea
            value={input}
            onChange={(event) => onInputChange(event.target.value)}
            disabled={submitting || !entryNodeId}
            spellCheck={false}
          />
        </label>
        {message && <p className="invoke-message">{message}</p>}
        {error && <p className="invoke-error">{error}</p>}
        <button type="submit" disabled={!workflowId || !entryNodeId || submitting}>
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

function mergeEvents(...groups: RuntimeEvent[][]): RuntimeEvent[] {
  const values = new Map<string, RuntimeEvent>();
  for (const group of groups) {
    for (const event of group) values.set(event.id, event);
  }
  return [...values.values()].sort((left, right) => left.sequence - right.sequence);
}

/**
 * Collapse durable revisions into one scope selector option per workflow id.
 *
 * Sessions are keyed by workflow id, while each Invocation carries the exact
 * definition hash that its trace must render.  The selector therefore groups
 * revisions for navigation, prefers the currently executable revision when it
 * exists, and leaves the exact historical revision visible in the scope badge
 * once an Invocation is selected.
 */
function buildWorkflowDirectory(
  snapshots: WorkflowSummary[],
  registered: WorkflowSummary[],
): WorkflowSummary[] {
  const registeredKeys = new Set(
    registered.map((workflow) => workflowRevisionKey(workflow)),
  );
  const grouped = new Map<string, WorkflowSummary[]>();
  for (const snapshot of snapshots) {
    const values = grouped.get(snapshot.workflow_id) ?? [];
    values.push(snapshot);
    grouped.set(snapshot.workflow_id, values);
  }
  return [...grouped.values()]
    .map((revisions) => {
      const current = revisions.find((revision) =>
        registeredKeys.has(workflowRevisionKey(revision)),
      );
      const representative = current ?? revisions[0];
      return {
        ...representative,
        revision_count: revisions.length,
        registered_in_current_app: current !== undefined,
      };
    })
    .sort((left, right) => left.workflow_id.localeCompare(right.workflow_id));
}

function workflowRevisionKey(workflow: Pick<
  WorkflowSummary,
  "workflow_id" | "definition_hash" | "operator_manifest_hash"
>): string {
  return [
    workflow.workflow_id,
    workflow.definition_hash,
    workflow.operator_manifest_hash,
  ].join("/");
}

function formatWorkflowRevision(
  version: string | number | null,
  definitionHash: string | null,
): string {
  const resolvedVersion = version === null ? "v?" : `v${version}`;
  return definitionHash
    ? `${resolvedVersion} · ${definitionHash.slice(0, 8)}`
    : resolvedVersion;
}

function applyInvocationStateEvents(
  queryClient: QueryClient,
  events: RuntimeEvent[],
): void {
  for (const event of events) {
    if (event.type !== "invocation.state_changed") continue;
    const state = String(event.payload.to ?? "");
    if (!state) continue;
    queryClient.setQueryData<InvocationSummary[]>(
      ["invocations", event.session_id],
      (current) =>
        current?.map((invocation) =>
          invocation.id === event.invocation_id
            ? {
                ...invocation,
                state,
                updated_at_ms: event.occurred_at_ms,
              }
            : invocation,
        ) ?? current,
    );
  }
}

function upsertSubmittedScope(
  queryClient: QueryClient,
  value: {
    workflowId: string;
    sessionId: string;
    invocationId: string;
    sessionKey: string | null;
    entryNodeId: string;
    state: string;
  },
): void {
  const now = Date.now();
  queryClient.setQueryData<SessionSummary[]>(
    ["sessions", value.workflowId],
    (current) => {
      const existing = current ?? [];
      const nextSession: SessionSummary = {
        id: value.sessionId,
        namespace: "default",
        workflow_id: value.workflowId,
        session_key: value.sessionKey,
        current_invocation_id: value.invocationId,
        invocation_count: 1,
        created_at_ms: now,
        updated_at_ms: now,
      };
      if (existing.some((session) => session.id === value.sessionId)) {
        return existing.map((session) =>
          session.id === value.sessionId
            ? {
                ...session,
                current_invocation_id: value.invocationId,
                invocation_count: Math.max(session.invocation_count, 1),
                updated_at_ms: now,
              }
            : session,
        );
      }
      return [...existing, nextSession];
    },
  );
  queryClient.setQueryData<InvocationSummary[]>(
    ["invocations", value.sessionId],
    (current) => {
      const existing = current ?? [];
      const nextInvocation: InvocationSummary = {
        id: value.invocationId,
        workflow_id: value.workflowId,
        workflow_version: null,
        definition_hash: null,
        operator_manifest_hash: null,
        entry_node_id: value.entryNodeId,
        state: value.state,
        created_at_ms: now,
        updated_at_ms: now,
      };
      if (existing.some((invocation) => invocation.id === value.invocationId)) {
        return existing.map((invocation) =>
          invocation.id === value.invocationId
            ? { ...invocation, state: value.state, updated_at_ms: now }
            : invocation,
        );
      }
      return [...existing, nextInvocation];
    },
  );
}

function createDraftProjection(graph: WorkflowGraphView): RuntimeProjection {
  return {
    invocation_id: "draft",
    through_sequence: 0,
    invocation_state: "created",
    node_executions: {},
    nodes: Object.fromEntries(
      graph.nodes.map((node) => [
        node.id,
        {
          node_id: node.id,
          state: "created",
          latest_execution_id: "",
          execution_count: 0,
        },
      ]),
    ),
    edges: {},
    operator_states: {},
  };
}

function createDraftInvocation(graph: WorkflowGraphView): InvocationDetail {
  return {
    id: "draft",
    workflow_id: graph.workflow_id,
    workflow_version: graph.workflow_version,
    definition_hash: graph.definition_hash,
    operator_manifest_hash: graph.operator_manifest_hash,
    entry_node_id: graph.entry_node_ids[0] ?? "",
    state: "created",
    created_at_ms: Date.now(),
    updated_at_ms: Date.now(),
    input: {},
    context: {},
    result: null,
    error: null,
    node_executions: [],
  };
}

function createLiveDraftInvocation(
  graph: WorkflowGraphView,
  invocationId: string,
  state: string,
  entryNodeId: string,
  input: Record<string, unknown>,
): InvocationDetail {
  const now = Date.now();
  return {
    id: invocationId,
    workflow_id: graph.workflow_id,
    workflow_version: graph.workflow_version,
    definition_hash: graph.definition_hash,
    operator_manifest_hash: graph.operator_manifest_hash,
    entry_node_id: entryNodeId,
    state,
    created_at_ms: now,
    updated_at_ms: now,
    input,
    context: {},
    result: null,
    error: null,
    node_executions: [],
  };
}

function createLiveDraftTimeline(invocationId: string): TimelineView {
  return {
    invocation_id: invocationId,
    started_at_ms: Date.now(),
    ended_at_ms: null,
    spans: [],
  };
}

function applyInvocationStateToDetail(
  invocation: InvocationDetail,
  events: RuntimeEvent[],
): InvocationDetail {
  const stateEvent = [...events]
    .filter((event) => event.type === "invocation.state_changed")
    .at(-1);
  if (!stateEvent) return invocation;
  return {
    ...invocation,
    state: String(stateEvent.payload.to ?? invocation.state),
    updated_at_ms: stateEvent.occurred_at_ms,
  };
}

function shouldPollInvocation(state: string): boolean {
  return ["created", "running", "waiting", "interrupted"].includes(state);
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
