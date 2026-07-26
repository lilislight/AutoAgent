import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { AlertTriangle, GitBranch, LoaderCircle, Play, X } from "lucide-react";

import {
  buildTimelineView,
  cancelInvocation,
  createAuthenticationSession,
  getLaterEvents,
  getInvocation,
  getWorkflowGraph,
  getTraceView,
  getHealth,
  getRuntimeStatus,
  listInvocations,
  listRegisteredWorkflows,
  listSessions,
  listWorkflows,
  resumeInvocation,
  subscribeToInvocation,
  subscribeToRuntimeStatus,
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
  RuntimeStatus,
  SessionSummary,
  TimelineView,
  TraceBootstrap,
  WorkflowGraphView,
  WorkflowSummary,
} from "./types";

const EVENT_PAGE_SIZE = 200;
const EVENT_HISTORY_CACHE_SIZE = 8;

export default function App() {
  const queryClient = useQueryClient();
  const ui = useTraceUi();
  const [events, setEvents] = useState<RuntimeEvent[]>([]);
  const [eventBuffer, setEventBuffer] = useState<RuntimeEvent[]>([]);
  const [pendingScope, setPendingScope] = useState<{
    workflowId: string;
    sessionId: string;
    invocationId: string;
  } | null>(null);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [refreshingLatest, setRefreshingLatest] = useState(false);
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
  const [invokeEventMode, setInvokeEventMode] = useState<
    "minimal" | "standard" | "full"
  >("standard");
  const [invokeSubmitting, setInvokeSubmitting] = useState(false);
  const [invokeError, setInvokeError] = useState<string | null>(null);
  const [invokeMessage, setInvokeMessage] = useState<string | null>(null);
  const [resumeSubmitting, setResumeSubmitting] = useState(false);
  const [resumeOpen, setResumeOpen] = useState(false);
  const [resumeWaitKey, setResumeWaitKey] = useState("");
  const [resumeNodeId, setResumeNodeId] = useState("");
  const [resumeOutput, setResumeOutput] = useState("{}");
  const [resumeError, setResumeError] = useState<string | null>(null);
  const [pendingNodeAction, setPendingNodeAction] = useState<{
    kind: "invoke" | "resume";
    nodeId: string;
  } | null>(null);
  const [cancelSubmitting, setCancelSubmitting] = useState(false);
  const [runtimeStreamConnected, setRuntimeStreamConnected] = useState<boolean | null>(null);
  const [liveDraftGraph, setLiveDraftGraph] = useState<WorkflowGraphView | null>(null);
  const [liveDraftInvocation, setLiveDraftInvocation] = useState<InvocationDetail | null>(null);
  const [liveDraftTimeline, setLiveDraftTimeline] = useState<TimelineView | null>(null);
  const [darkMode, setDarkMode] = useState(
    () => localStorage.getItem("autoagent:theme") === "dark",
  );
  const projectionCacheRef = useRef<{
    key: string;
    values: Map<number, RuntimeProjection>;
  }>({ key: "", values: new Map() });
  const eventHistoryCacheRef = useRef(new Map<string, {
    events: RuntimeEvent[];
    historyLoaded: boolean;
  }>());
  const eventOwnerRef = useRef<string | null>(null);

  useEffect(() => {
    document.documentElement.dataset.theme = darkMode ? "dark" : "light";
    localStorage.setItem("autoagent:theme", darkMode ? "dark" : "light");
  }, [darkMode]);

  const healthQuery = useQuery({
    queryKey: ["trace-health"],
    queryFn: getHealth,
    staleTime: Number.POSITIVE_INFINITY,
  });
  const authenticated = healthQuery.data?.authenticated ?? false;
  const runtimeStatusQuery = useQuery({
    queryKey: ["runtime-status"],
    queryFn: getRuntimeStatus,
    enabled: authenticated,
    staleTime: Number.POSITIVE_INFINITY,
    refetchInterval: runtimeStreamConnected === false ? 10_000 : false,
    refetchIntervalInBackground: false,
  });
  useEffect(() => {
    if (!authenticated) return;
    return subscribeToRuntimeStatus(
      (status) => queryClient.setQueryData<RuntimeStatus>(["runtime-status"], status),
      setRuntimeStreamConnected,
    );
  }, [authenticated, queryClient]);
  useEffect(() => {
    const refreshWhenVisible = () => {
      if (document.visibilityState === "visible" && authenticated) {
        void runtimeStatusQuery.refetch();
      }
    };
    document.addEventListener("visibilitychange", refreshWhenVisible);
    return () => document.removeEventListener("visibilitychange", refreshWhenVisible);
  }, [authenticated, runtimeStatusQuery.refetch]);
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
    staleTime: Number.POSITIVE_INFINITY,
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
    staleTime: Number.POSITIVE_INFINITY,
  });
  const queriedView = viewQuery.data;
  const view =
    queriedView &&
    queriedView.session.id === ui.sessionId &&
    queriedView.invocation.id === ui.invocationId
      ? queriedView
      : undefined;
  const invocationStatusQuery = useQuery({
    queryKey: ["invocation-status", ui.invocationId],
    queryFn: () => getInvocation(ui.invocationId!),
    enabled: Boolean(
      view &&
      !historyLoaded &&
      shouldStreamInvocation(view.invocation),
    ),
    refetchInterval: (query) => {
      const current = query.state.data;
      return !current || shouldStreamInvocation(current) ? 1_000 : false;
    },
    refetchIntervalInBackground: false,
  });
  const polledInvocation = invocationStatusQuery.data;
  const activeInvocation = liveDraftInvocation ?? (
    view
      ? { ...view.invocation, ...polledInvocation }
      : null
  );
  const streamEligible = activeInvocation
    ? shouldStreamInvocation(activeInvocation)
    : false;
  const activeSessionId = ui.sessionId;
  const activeInvocationId = activeInvocation?.id ?? null;
  const activeEvents = (
    activeInvocationId !== null &&
    eventOwnerRef.current === activeInvocationId
  ) ? events : [];

  useEffect(() => {
    if (!view) return;
    if (liveDraftInvocation?.id === view.invocation.id) return;
    const previousOwner = eventOwnerRef.current;
    if (previousOwner && previousOwner !== view.invocation.id) {
      cacheEventHistory(eventHistoryCacheRef.current, previousOwner, {
        events,
        historyLoaded,
      });
    }
    const cached = eventHistoryCacheRef.current.get(view.invocation.id);
    if (cached) {
      cacheEventHistory(
        eventHistoryCacheRef.current,
        view.invocation.id,
        cached,
      );
    }
    eventOwnerRef.current = view.invocation.id;
    setEvents(cached?.events ?? view.events);
    setEventBuffer([]);
    setHistoryLoaded(cached?.historyLoaded ?? !view.has_more_events);
    setHistoryError(null);
    const latest = view.projection.through_sequence;
    ui.setCursor(latest, true);
  }, [liveDraftInvocation?.id, view?.invocation.id]);

  useEffect(() => {
    if (
      !activeInvocationId ||
      !activeInvocation ||
      !activeSessionId ||
      !streamEligible ||
      !historyLoaded
    ) return;
    const cachedEvents =
      eventHistoryCacheRef.current.get(activeInvocationId)?.events ?? [];
    const startSequence = (
      eventOwnerRef.current === activeInvocationId
        ? events
        : cachedEvents
    ).at(-1)?.sequence ?? 0;
    return subscribeToInvocation(
      activeInvocationId,
      startSequence,
      (event) => {
        const state = useTraceUi.getState();
        if (state.invocationId !== activeInvocationId) return;
        if (state.followLive) {
          setEvents((current) => {
            const merged = mergeEvents(current, [event]);
            cacheEventHistory(eventHistoryCacheRef.current, activeInvocationId, {
              events: merged,
              historyLoaded: true,
            });
            return merged;
          });
          state.setCursor(event.sequence, true);
        } else {
          setEventBuffer((current) => mergeEvents(current, [event]));
        }
      },
      (status) => {
        setLiveDraftInvocation((current) =>
          current?.id === status.id ? { ...current, ...status } : current,
        );
        queryClient.setQueryData<TraceBootstrap>(
          ["trace-view", activeSessionId, activeInvocationId],
          (current) => current
            ? {
                ...current,
                invocation: { ...current.invocation, ...status },
              }
            : current,
        );
        queryClient.setQueryData<InvocationSummary[]>(
          ["invocations", activeSessionId],
          (current) => current?.map((item) =>
            item.id === status.id ? { ...item, ...status } : item,
          ),
        );
        if (!shouldStreamInvocation(status)) {
          setLiveDraftGraph(null);
          setLiveDraftInvocation(null);
          setLiveDraftTimeline(null);
          void queryClient.invalidateQueries({
            queryKey: ["trace-view", activeSessionId, activeInvocationId],
          });
        }
      },
      () => undefined,
    );
  }, [
    activeInvocationId,
    activeSessionId,
    historyLoaded,
    queryClient,
    streamEligible,
  ]);

  const cursorSequence =
    ui.cursorSequence ?? view?.projection.through_sequence ?? 0;
  const projection = useMemo(
    () => {
      if (liveDraftInvocation) {
        return cachedProjection(
          projectionCacheRef.current,
          `${liveDraftInvocation.id}:live`,
          liveDraftInvocation.id,
          activeEvents,
          cursorSequence,
        );
      }
      if (!view) return null;
      // Bootstrap already carries the authoritative state at the live cursor.
      // Using it also avoids a first paint from an empty local Event array.
      if (cursorSequence === view.projection.through_sequence) {
        return view.projection;
      }
      const checkpoint =
        !historyLoaded && cursorSequence >= view.checkpoint.through_sequence
          ? view.checkpoint
          : undefined;
      return cachedProjection(
        projectionCacheRef.current,
        `${view.invocation.id}:${historyLoaded}:${checkpoint?.through_sequence ?? 0}`,
        view.invocation.id,
        activeEvents,
        cursorSequence,
        checkpoint,
      );
    },
    [activeEvents, cursorSequence, historyLoaded, liveDraftInvocation, view],
  );
  const activeGraph = liveDraftGraph ?? view?.graph ?? null;
  const activeInvocationDetail = activeInvocation;
  const latestTimelineProjection = useMemo(() => {
    if (liveDraftInvocation) {
      return projectEvents(liveDraftInvocation.id, activeEvents);
    }
    if (!view) return null;
    return projectEvents(
      view.invocation.id,
      activeEvents,
      undefined,
      view.projection,
    );
  }, [activeEvents, liveDraftInvocation, view]);
  const activeTimeline = useMemo(
    () => (
      activeInvocationDetail && latestTimelineProjection
        ? buildTimelineView(activeInvocationDetail, latestTimelineProjection)
        : liveDraftTimeline
    ),
    [activeInvocationDetail, latestTimelineProjection, liveDraftTimeline],
  );
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
  const selectedDirectoryWorkflow =
    workflows.find((workflow) => workflow.workflow_id === ui.workflowId) ?? null;
  const selectedGraphQuery = useQuery({
    queryKey: [
      "workflow-graph",
      selectedDirectoryWorkflow?.workflow_id,
      selectedDirectoryWorkflow?.definition_hash,
      selectedDirectoryWorkflow?.operator_manifest_hash,
    ],
    queryFn: () => getWorkflowGraph(selectedDirectoryWorkflow!),
    enabled: Boolean(selectedDirectoryWorkflow && !activeGraph && !invokeOpen),
    staleTime: Number.POSITIVE_INFINITY,
  });
  const selectedGraph = selectedGraphQuery.data ?? null;
  const selectedGraphProjection = useMemo(
    () => selectedGraph ? createDraftProjection(selectedGraph) : null,
    [selectedGraph],
  );
  const selectedGraphInvocation = useMemo(
    () => selectedGraph ? createDraftInvocation(selectedGraph) : null,
    [selectedGraph],
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

  const loadNextEventPage = useCallback(async () => {
    if (!view || !ui.sessionId || !ui.invocationId || historyLoading) return;
    const requestedInvocationId = ui.invocationId;
    const cached = eventHistoryCacheRef.current.get(requestedInvocationId);
    const currentEvents = cached?.events ?? (
      eventOwnerRef.current === requestedInvocationId ? events : []
    );
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      const afterSequence = currentEvents.at(-1)?.sequence ?? 0;
      const page = await getLaterEvents(
        ui.sessionId,
        requestedInvocationId,
        afterSequence,
        EVENT_PAGE_SIZE,
      );
      const merged = mergeEvents(currentEvents, page.events);
      const loadedThrough = merged.at(-1)?.sequence ?? 0;
      const caughtUp = loadedThrough >= page.live_sequence;
      cacheEventHistory(eventHistoryCacheRef.current, requestedInvocationId, {
        events: merged,
        historyLoaded: caughtUp,
      });
      if (useTraceUi.getState().invocationId === requestedInvocationId) {
        eventOwnerRef.current = requestedInvocationId;
        setEvents(merged);
        setHistoryLoaded(caughtUp);
        if (caughtUp && useTraceUi.getState().followLive) {
          useTraceUi.getState().setCursor(loadedThrough, true);
        }
      }
      queryClient.setQueryData<TraceBootstrap>(
        ["trace-view", ui.sessionId, requestedInvocationId],
        (current) => current
          ? {
              ...current,
              invocation: {
                ...current.invocation,
                state: page.invocation_state,
                live_sequence: page.live_sequence,
              },
              has_more_events: loadedThrough < page.live_sequence,
            }
          : current,
      );
      queryClient.setQueryData<InvocationSummary[]>(
        ["invocations", ui.sessionId],
        (current) => current?.map((item) =>
          item.id === requestedInvocationId
            ? {
                ...item,
                state: page.invocation_state,
                live_sequence: page.live_sequence,
              }
            : item,
        ),
      );
    } catch (loadError) {
      setHistoryError(loadError instanceof Error ? loadError.message : String(loadError));
    } finally {
      setHistoryLoading(false);
    }
  }, [
    events,
    historyLoading,
    queryClient,
    ui.invocationId,
    ui.sessionId,
    view,
  ]);

  useEffect(() => {
    if (
      !view ||
      view.invocation.event_mode === "minimal" ||
      historyLoaded ||
      historyLoading ||
      activeEvents.length > 0
    ) return;
    void loadNextEventPage();
  }, [
    activeEvents.length,
    historyLoaded,
    historyLoading,
    loadNextEventPage,
    view?.invocation.id,
  ]);

  const refreshLatestInvocation = useCallback(async () => {
    if (!ui.sessionId || !ui.invocationId || refreshingLatest) return;
    setRefreshingLatest(true);
    try {
      const result = await viewQuery.refetch();
      if (result.data && ui.followLive) {
        ui.setCursor(result.data.projection.through_sequence, true);
      }
    } finally {
      setRefreshingLatest(false);
    }
  }, [
    refreshingLatest,
    ui,
    viewQuery,
    ui.invocationId,
    ui.sessionId,
  ]);

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
        event_mode: invokeEventMode,
      });
      setInvokeMessage(`Created invocation ${response.invocation_id.slice(0, 8)}.`);
      eventOwnerRef.current = response.invocation_id;
      setEvents([]);
      setEventBuffer([]);
      setHistoryLoaded(true);
      cacheEventHistory(eventHistoryCacheRef.current, response.invocation_id, {
        events: [],
        historyLoaded: true,
      });
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
        eventMode: invokeEventMode,
      });
      ui.setInvocationScope(response.workflow_id, response.session_id, response.invocation_id);
      ui.setCursor(0, true);
      setInvokeOpen(false);
      setPendingNodeAction(null);
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

  const stageInvoke = (entryNodeId: string) => {
    const workflowId =
      registeredWorkflows.find((workflow) => workflow.workflow_id === ui.workflowId)
        ?.workflow_id ??
      registeredWorkflows[0]?.workflow_id ??
      null;
    setInvokeWorkflowId(workflowId);
    const currentSession = sessions.find((value) => value.id === ui.sessionId);
    setInvokeSessionKey(
      activeInvocationDetail &&
      !isTerminalInvocation(activeInvocationDetail.state)
        ? ""
        : currentSession?.session_key ?? "",
    );
    setInvokeEntryNodeId(entryNodeId);
    setInvokeInput("{}");
    setInvokeOpen(false);
    setInvokeError(null);
    setInvokeMessage(null);
    setResumeOpen(false);
    setPendingNodeAction({ kind: "invoke", nodeId: entryNodeId });
  };

  const stageResume = (nodeId: string) => {
    const waits = Object.values(projection?.active_waits ?? {});
    const matching = waits.filter((wait) => wait.node_id === nodeId);
    const selected = matching[0] ?? (waits.length === 1 ? waits[0] : null);
    if (!selected) {
      setPendingNodeAction(null);
      ui.setSelection({ type: "node", id: nodeId });
      return;
    }
    setResumeNodeId(nodeId);
    setResumeWaitKey(selected.wait_key);
    setResumeOutput("{}");
    setResumeError(null);
    setResumeOpen(false);
    setInvokeOpen(false);
    setPendingNodeAction({ kind: "resume", nodeId });
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
        eventMode: activeInvocationDetail?.event_mode,
      });
      await Promise.all([
        queryClient.invalidateQueries({
          queryKey: ["trace-view", response.session_id, response.invocation_id],
        }),
        queryClient.invalidateQueries({ queryKey: ["sessions", response.workflow_id] }),
        queryClient.invalidateQueries({ queryKey: ["invocations", response.session_id] }),
      ]);
      setResumeOpen(false);
      setPendingNodeAction(null);
      await refreshLatestInvocation();
    } catch (resumeError) {
      setResumeError(resumeError instanceof Error ? resumeError.message : String(resumeError));
      throw resumeError;
    } finally {
      setResumeSubmitting(false);
    }
  };

  const cancelActiveInvocation = async () => {
    if (!activeInvocationDetail || cancelSubmitting) return;
    setCancelSubmitting(true);
    try {
      await cancelInvocation(activeInvocationDetail.id);
      await Promise.all([
        queryClient.invalidateQueries({
          queryKey: ["trace-view", activeSessionId, activeInvocationDetail.id],
        }),
        queryClient.invalidateQueries({
          queryKey: ["invocations", activeSessionId],
        }),
      ]);
    } finally {
      setCancelSubmitting(false);
    }
  };

  const clearLiveDraft = () => {
    setLiveDraftGraph(null);
    setLiveDraftInvocation(null);
    setLiveDraftTimeline(null);
    setEventBuffer([]);
  };

  useEffect(() => {
    if (
      activeInvocationDetail &&
      pendingNodeAction &&
      (
        ui.selection?.type !== "node" ||
        ui.selection.id !== pendingNodeAction.nodeId
      )
    ) {
      setPendingNodeAction(null);
      setInvokeOpen(false);
      setResumeOpen(false);
    }
  }, [activeInvocationDetail, pendingNodeAction, ui.selection]);

  const loading =
    runtimeStatusQuery.isLoading ||
    workflowQuery.isLoading ||
    registeredWorkflowQuery.isLoading ||
    sessionQuery.isLoading ||
    invocationQuery.isLoading ||
    (viewQuery.isLoading && !liveDraftGraph) ||
    (Boolean(ui.sessionId && ui.invocationId) && !view && !liveDraftGraph);
  const error =
    healthQuery.error ||
    runtimeStatusQuery.error ||
    workflowQuery.error ||
    registeredWorkflowQuery.error ||
    sessionQuery.error ||
    invocationQuery.error ||
    viewQuery.error ||
    selectedGraphQuery.error;

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
        invocations={invocations}
        workflowId={ui.workflowId}
        sessionId={ui.sessionId}
        invocationId={ui.invocationId}
        invocationState={activeInvocationDetail?.state ?? null}
        runtimeStatus={runtimeStatusQuery.data ?? null}
        darkMode={darkMode}
        refreshingInvocation={refreshingLatest}
        canCancelInvocation={Boolean(
          activeInvocationDetail &&
          !isTerminalInvocation(activeInvocationDetail.state)
        )}
        cancellingInvocation={cancelSubmitting}
        onRefreshInvocation={() => void refreshLatestInvocation()}
        onCancelInvocation={() => void cancelActiveInvocation()}
        onInspectInvocation={() => {
          if (!activeInvocationDetail) return;
          ui.setSelection({
            type: "invocation",
            id: activeInvocationDetail.id,
          });
        }}
        onScopeChange={(workflowId, sessionId, invocationId) => {
          clearLiveDraft();
          setPendingNodeAction(null);
          setInvokeOpen(false);
          setResumeOpen(false);
          ui.setInvocationScope(workflowId, sessionId, invocationId);
        }}
        onToggleTheme={() => setDarkMode((value) => !value)}
      />
      {pendingNodeAction && !invokeOpen && !resumeOpen && (
        <NodeActionPrompt
          action={pendingNodeAction.kind}
          nodeId={pendingNodeAction.nodeId}
          onOpen={() => {
            if (pendingNodeAction.kind === "invoke") setInvokeOpen(true);
            else setResumeOpen(true);
          }}
          onClose={() => setPendingNodeAction(null)}
        />
      )}
      {invokeOpen && (
        <InvocationLauncher
          workflows={registeredWorkflows}
          sessions={invokeSessions}
          workflowId={invokeWorkflow?.workflow_id ?? null}
          input={invokeInput}
          sessionKey={invokeSessionKey}
          entryNodeId={invokeEntryNodeId}
          eventMode={invokeEventMode}
          entryNodeIds={invokeGraph?.entry_node_ids ?? []}
          error={invokeError}
          message={invokeMessage}
          submitting={invokeSubmitting}
          graphLoading={invokeGraphQuery.isLoading}
          graphError={invokeGraphQuery.error}
          sessionsLoading={invokeSessionQuery.isLoading}
          sessionsError={invokeSessionQuery.error}
          onWorkflowChange={(workflowId) => {
            setInvokeWorkflowId(workflowId);
            setInvokeEntryNodeId("");
            setInvokeError(null);
          }}
          onInputChange={setInvokeInput}
          onSessionKeyChange={setInvokeSessionKey}
          onSessionSelect={(sessionKey) => setInvokeSessionKey(sessionKey)}
          onEntryNodeIdChange={setInvokeEntryNodeId}
          onEventModeChange={setInvokeEventMode}
          onClose={() => {
            if (!invokeSubmitting) setInvokeOpen(false);
          }}
          onSubmit={submitFromUi}
        />
      )}
      {resumeOpen && (
        <ResumePanel
          nodeId={resumeNodeId}
          waitKey={resumeWaitKey}
          output={resumeOutput}
          error={resumeError}
          submitting={resumeSubmitting}
          onOutputChange={setResumeOutput}
          onClose={() => {
            if (!resumeSubmitting) setResumeOpen(false);
          }}
          onSubmit={async () => {
            try {
              await resumeSelectedWait(
                resumeWaitKey,
                parseJsonValue(resumeOutput),
              );
            } catch (resumeFailure) {
              setResumeError(
                resumeFailure instanceof Error
                  ? resumeFailure.message
                  : String(resumeFailure),
              );
            }
          }}
        />
      )}
      {error ? (
        <StatusScreen
          icon={<AlertTriangle size={28} />}
          title="Trace data could not be loaded"
          detail={error instanceof Error ? error.message : String(error)}
          action={{
            label: "Retry connection",
            onClick: () => {
              void healthQuery.refetch();
              void runtimeStatusQuery.refetch();
              void workflowQuery.refetch();
            },
          }}
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
            canInvoke={Boolean(
              viewedWorkflowIsRegistered &&
              runtimeStatusQuery.data?.execution.accepting_invocations
            )}
            canResume={Boolean(
              viewedWorkflowIsRegistered &&
              projection.invocation_state === "waiting"
            )}
            onInspect={ui.setSelection}
            onInvoke={stageInvoke}
            onResume={stageResume}
          />
          {ui.selection && (
            <InspectorPanel
              graph={activeGraph}
              invocation={activeInvocationDetail}
              events={activeEvents}
              projection={projection}
              selection={ui.selection}
              cursorSequence={cursorSequence}
              capabilities={view?.capabilities}
              onClose={() => ui.setSelection(null)}
            />
          )}
          <ExecutionTimeline
            timeline={activeTimeline}
            events={activeEvents}
            cursorSequence={cursorSequence}
            followLive={ui.followLive}
            onCursorChange={(sequence) => ui.setCursor(sequence, false)}
            onSelect={ui.setSelection}
            historyAvailable={!liveDraftGraph && !historyLoaded && Boolean(view?.has_more_events)}
            historyLoading={historyLoading}
            historyError={historyError}
            bufferedEventCount={eventBuffer.length}
            totalEventCount={activeInvocationDetail.live_sequence ?? activeEvents.length}
            collapsed={timelineCollapsed}
            onCollapsedChange={setTimelineCollapsed}
            height={timelineHeight}
            onHeightChange={setTimelineHeight}
            onLoadHistory={() => void loadNextEventPage()}
            onFlushBufferedEvents={flushBufferedEvents}
            onToggleFollow={toggleTimelineMode}
          />
        </main>
      ) : selectedGraphQuery.isLoading ? (
        <StatusScreen
          icon={<LoaderCircle className="spin" size={28} />}
          title="Loading workflow graph"
          detail="Reading the selected registered or historical Workflow definition."
        />
      ) : selectedGraph && selectedGraphInvocation && selectedGraphProjection ? (
        <main
          className="trace-workspace timeline-collapsed"
          style={{ "--timeline-height": "42px" } as CSSProperties}
        >
          <WorkflowCanvas
            graph={selectedGraph}
            invocation={selectedGraphInvocation}
            projection={selectedGraphProjection}
            followLive={false}
            selection={null}
            canInvoke={Boolean(
              selectedDirectoryWorkflow?.registered_in_current_app &&
              runtimeStatusQuery.data?.execution.accepting_invocations
            )}
            onInspect={() => undefined}
            onInvoke={stageInvoke}
          />
        </main>
      ) : (
        <StatusScreen
          icon={<GitBranch size={28} />}
          title="No invocation selected"
          detail={
            ui.workflowId
              ? "Loading the selected Workflow graph."
              : "Select a Workflow to inspect or invoke it."
          }
        />
      )}
    </div>
  );
}

function NodeActionPrompt({
  action,
  nodeId,
  onOpen,
  onClose,
}: {
  action: "invoke" | "resume";
  nodeId: string;
  onOpen: () => void;
  onClose: () => void;
}) {
  return (
    <aside className="node-action-prompt" aria-label={`${action} node action`}>
      <button className="node-action-primary" type="button" onClick={onOpen}>
        <Play size={14} />
        <span>
          <small>{action === "invoke" ? "Entry node" : "Waiting node"}</small>
          <strong>{action === "invoke" ? "Invoke" : "Resume"} {nodeId}</strong>
        </span>
      </button>
      <button
        className="node-action-close"
        type="button"
        onClick={onClose}
        aria-label="Dismiss node action"
      >
        <X size={14} />
      </button>
    </aside>
  );
}

function InvocationLauncher({
  workflows,
  sessions,
  workflowId,
  input,
  sessionKey,
  entryNodeId,
  eventMode,
  entryNodeIds,
  error,
  message,
  submitting,
  graphLoading,
  graphError,
  sessionsLoading,
  sessionsError,
  onWorkflowChange,
  onInputChange,
  onSessionKeyChange,
  onSessionSelect,
  onEntryNodeIdChange,
  onEventModeChange,
  onClose,
  onSubmit,
}: {
  workflows: WorkflowSummary[];
  sessions: Array<{ id: string; session_key: string | null }>;
  workflowId: string | null;
  input: string;
  sessionKey: string;
  entryNodeId: string;
  eventMode: "minimal" | "standard" | "full";
  entryNodeIds: string[];
  error: string | null;
  message: string | null;
  submitting: boolean;
  graphLoading: boolean;
  graphError: Error | null;
  sessionsLoading: boolean;
  sessionsError: Error | null;
  onWorkflowChange: (value: string | null) => void;
  onInputChange: (value: string) => void;
  onSessionKeyChange: (value: string) => void;
  onSessionSelect: (value: string) => void;
  onEntryNodeIdChange: (value: string) => void;
  onEventModeChange: (value: "minimal" | "standard" | "full") => void;
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
            disabled={submitting || sessionsLoading || knownSessions.length === 0}
            onChange={(event) => {
              if (event.target.value) onSessionSelect(event.target.value);
            }}
          >
            <option value="">
              {sessionsLoading
                ? "Loading sessions…"
                : sessionsError
                  ? "Sessions could not be loaded"
                  : "Choose to reuse, or type below"}
            </option>
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
              {graphLoading
                ? "Loading graph…"
                : graphError
                  ? "Workflow graph could not be loaded"
                  : "Click an entry node on the graph"}
            </option>
            {entryNodeIds.map((nodeId) => (
              <option key={nodeId} value={nodeId}>
                {nodeId}
              </option>
            ))}
          </select>
        </label>
        <label>
          Trace mode
          <select
            value={eventMode}
            onChange={(event) =>
              onEventModeChange(
                event.target.value as "minimal" | "standard" | "full",
              )
            }
            disabled={submitting}
          >
            <option value="minimal">Minimal · final state only</option>
            <option value="standard">Standard · graph trace</option>
            <option value="full">Full · inputs, outputs and replay state</option>
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

function ResumePanel({
  nodeId,
  waitKey,
  output,
  error,
  submitting,
  onOutputChange,
  onClose,
  onSubmit,
}: {
  nodeId: string;
  waitKey: string;
  output: string;
  error: string | null;
  submitting: boolean;
  onOutputChange: (value: string) => void;
  onClose: () => void;
  onSubmit: () => Promise<void>;
}) {
  return (
    <section className="invoke-panel resume-panel" aria-label="Resume waiting node">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          void onSubmit();
        }}
      >
        <div className="invoke-heading">
          <div>
            <Play size={16} />
            <strong>Resume wait</strong>
          </div>
          <button type="button" onClick={onClose} disabled={submitting} aria-label="Close">
            <X size={15} />
          </button>
        </div>
        <label>
          Node
          <input value={nodeId} disabled />
        </label>
        <label>
          Wait key
          <input value={waitKey} disabled />
        </label>
        <label>
          Resume output JSON
          <textarea
            value={output}
            onChange={(event) => onOutputChange(event.target.value)}
            disabled={submitting}
            spellCheck={false}
          />
        </label>
        {error && <p className="invoke-error">{error}</p>}
        <button type="submit" disabled={!waitKey || submitting}>
          {submitting ? "Resuming..." : "Resume"}
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
  action,
}: {
  icon: React.ReactNode;
  title: string;
  detail: string;
  action?: { label: string; onClick: () => void };
}) {
  return (
    <main className="status-screen">
      {icon}
      <strong>{title}</strong>
      <span>{detail}</span>
      {action && (
        <button type="button" onClick={action.onClick}>
          {action.label}
        </button>
      )}
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

function cacheEventHistory(
  cache: Map<string, { events: RuntimeEvent[]; historyLoaded: boolean }>,
  invocationId: string,
  value: { events: RuntimeEvent[]; historyLoaded: boolean },
) {
  cache.delete(invocationId);
  cache.set(invocationId, value);
  while (cache.size > EVENT_HISTORY_CACHE_SIZE) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) break;
    cache.delete(oldest);
  }
}

function cachedProjection(
  cache: { key: string; values: Map<number, RuntimeProjection> },
  key: string,
  invocationId: string,
  events: RuntimeEvent[],
  throughSequence: number,
  checkpoint?: RuntimeProjection,
): RuntimeProjection {
  if (cache.key !== key) {
    cache.key = key;
    cache.values.clear();
  }
  let base =
    checkpoint && checkpoint.through_sequence <= throughSequence
      ? checkpoint
      : undefined;
  for (const [sequence, projection] of cache.values) {
    if (
      sequence <= throughSequence &&
      sequence > (base?.through_sequence ?? 0)
    ) {
      base = projection;
    }
  }
  const projected = projectEvents(
    invocationId,
    events,
    throughSequence,
    base,
  );
  cache.values.delete(projected.through_sequence);
  cache.values.set(projected.through_sequence, projected);
  while (cache.values.size > 256) {
    const oldest = cache.values.keys().next().value;
    if (oldest === undefined) break;
    cache.values.delete(oldest);
  }
  return projected;
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
    .sort((left, right) => {
      const sourceOrder =
        Number(Boolean(right.registered_in_current_app)) -
        Number(Boolean(left.registered_in_current_app));
      return sourceOrder || left.workflow_id.localeCompare(right.workflow_id);
    });
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

function upsertSubmittedScope(
  queryClient: QueryClient,
  value: {
    workflowId: string;
    sessionId: string;
    invocationId: string;
    sessionKey: string | null;
    entryNodeId: string;
    state: string;
    eventMode?: "minimal" | "standard" | "full";
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
        event_mode: value.eventMode,
        live_sequence: 0,
        durable_sequence: 0,
        persistence_status: "pending",
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
          latest_execution_id: null,
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

function shouldStreamInvocation(invocation: Pick<
  InvocationSummary,
  "state" | "persistence_status"
>): boolean {
  if (["created", "running", "waiting"].includes(invocation.state)) return true;
  return invocation.persistence_status === "pending";
}

function isTerminalInvocation(state: string): boolean {
  return ["completed", "failed", "cancelled", "interrupted"].includes(state);
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

function parseJsonValue(value: string): unknown {
  const trimmed = value.trim();
  return trimmed ? JSON.parse(trimmed) : null;
}
