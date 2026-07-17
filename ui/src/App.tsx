import { useEffect, useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, GitBranch, LoaderCircle } from "lucide-react";

import {
  getObservationView,
  listInvocations,
  listSessions,
  listWorkflows,
  subscribeToInvocation,
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
  const [darkMode, setDarkMode] = useState(
    () => localStorage.getItem("autoagent:theme") === "dark",
  );

  useEffect(() => {
    document.documentElement.dataset.theme = darkMode ? "dark" : "light";
    localStorage.setItem("autoagent:theme", darkMode ? "dark" : "light");
  }, [darkMode]);

  const workflowQuery = useQuery({
    queryKey: ["workflows"],
    queryFn: listWorkflows,
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
    queryKey: ["observation-view", ui.sessionId, ui.invocationId],
    queryFn: () => getObservationView(ui.sessionId!, ui.invocationId!),
    enabled: Boolean(ui.sessionId && ui.invocationId),
  });
  const view = viewQuery.data;

  useEffect(() => {
    if (!view) return;
    setEvents(view.events);
    const latest = view.events.at(-1)?.sequence ?? 0;
    ui.setCursor(latest, true);
  }, [view?.invocation.id]);

  useEffect(() => {
    if (!view || !ui.sessionId || !ui.invocationId) return;
    const afterSequence = events.at(-1)?.sequence ?? 0;
    return subscribeToInvocation(
      ui.sessionId,
      ui.invocationId,
      afterSequence,
      (event) => {
        setEvents((current) => appendEvent(current, event));
        const state = useTraceUi.getState();
        if (state.followLive) state.setCursor(event.sequence, true);
        if (
          event.type === "node.state_changed" ||
          event.type === "operator.call_finished" ||
          event.type === "invocation.state_changed"
        ) {
          void queryClient.invalidateQueries({
            queryKey: ["observation-view", ui.sessionId, ui.invocationId],
          });
        }
      },
      ui.setConnected,
    );
    // The EventSource is recreated only when the selected invocation changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view?.invocation.id]);

  const cursorSequence =
    ui.cursorSequence ?? events.at(-1)?.sequence ?? view?.projection.through_sequence ?? 0;
  const projection = useMemo(
    () =>
      view
        ? projectEvents(view.invocation.id, events, cursorSequence)
        : null,
    [cursorSequence, events, view],
  );

  const followLatest = () => {
    const latest = events.at(-1)?.sequence ?? 0;
    ui.setCursor(latest, true);
  };

  const loading = workflowQuery.isLoading || sessionQuery.isLoading || viewQuery.isLoading;
  const error = workflowQuery.error || sessionQuery.error || invocationQuery.error || viewQuery.error;

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
        onWorkflowChange={ui.setWorkflow}
        onSessionChange={ui.setSession}
        onInvocationChange={ui.setInvocation}
        onFollowLive={followLatest}
        onToggleTheme={() => setDarkMode((value) => !value)}
      />
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
        <main className="trace-workspace">
          <WorkflowCanvas
            graph={view.graph}
            projection={projection}
            followLive={ui.followLive}
            selection={ui.selection}
            onSelect={ui.setSelection}
          />
          <InspectorPanel
            graph={view.graph}
            invocation={view.invocation}
            events={events}
            projection={projection}
            selection={ui.selection}
            cursorSequence={cursorSequence}
            onClose={() => ui.setSelection(null)}
          />
          <ExecutionTimeline
            timeline={view.timeline}
            events={events}
            cursorSequence={cursorSequence}
            onCursorChange={(sequence) => ui.setCursor(sequence, false)}
            onSelect={ui.setSelection}
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
