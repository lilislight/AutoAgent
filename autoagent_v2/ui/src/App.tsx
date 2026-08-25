import {
  Activity,
  Braces,
  ChevronRight,
  Database,
  GitBranch,
  RefreshCw,
  Workflow as WorkflowIcon,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api } from "./api";
import { loadInvocationBootstrap } from "./bootstrap";
import { applyChildTrace } from "./childEvents";
import { createCoalescedRefresh } from "./coalescedRefresh";
import { JsonInspector } from "./components/JsonInspector";
import { TraceTimeline } from "./components/TraceTimeline";
import { WorkflowGraph } from "./components/WorkflowGraph";
import { switchInvocation, switchSession, switchWorkflow } from "./navigation";
import { RequestGate } from "./requestGate";
import type {
  ChildSessionSummary,
  InvocationSummary,
  RuntimeStateRecord,
  SessionSummary,
  TraceEvent,
  WorkflowSnapshot,
  WorkflowSummary,
} from "./types";

const MAX_VISIBLE_EVENTS = 2_000;
const INVOCATION_BOUNDARY_KINDS = new Set([
  "invocation.waiting",
  "invocation.completed",
  "invocation.failed",
  "invocation.cancelled",
]);
const CHILD_BOUNDARY_KINDS = new Set([
  "child_invocation.planned",
  "child_invocation.phase_changed",
]);

export default function App() {
  const [workflows, setWorkflows] = useState<WorkflowSummary[]>([]);
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [invocations, setInvocations] = useState<InvocationSummary[]>([]);
  const [children, setChildren] = useState<ChildSessionSummary[]>([]);
  const [workflowCursor, setWorkflowCursor] = useState<string | null>(null);
  const [sessionCursor, setSessionCursor] = useState<string | null>(null);
  const [invocationCursor, setInvocationCursor] = useState<string | null>(null);
  const [childCursor, setChildCursor] = useState<string | null>(null);
  const [selectedWorkflow, setSelectedWorkflow] = useState<string | null>(null);
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  const [selectedInvocation, setSelectedInvocation] = useState<string | null>(null);
  const [snapshot, setSnapshot] = useState<WorkflowSnapshot | null>(null);
  const [invocation, setInvocation] = useState<InvocationSummary | null>(null);
  const [events, setEvents] = useState<TraceEvent[]>([]);
  const [resumeCursor, setResumeCursor] = useState<string | null>(null);
  const [hasEarlierEvents, setHasEarlierEvents] = useState(false);
  const [state, setState] = useState<RuntimeStateRecord | null>(null);
  const [selectedEvent, setSelectedEvent] = useState<TraceEvent | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingMoreWorkflows, setLoadingMoreWorkflows] = useState(false);
  const [loadingMoreSessions, setLoadingMoreSessions] = useState(false);
  const [loadingMoreInvocations, setLoadingMoreInvocations] = useState(false);
  const [loadingMoreChildren, setLoadingMoreChildren] = useState(false);
  const [loadingEarlierTrace, setLoadingEarlierTrace] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [live, setLive] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const requestsRef = useRef<RequestGate | null>(null);
  if (requestsRef.current === null) requestsRef.current = new RequestGate();
  const requests = requestsRef.current;
  const selectionRef = useRef({
    workflow: null as string | null,
    session: null as string | null,
    invocation: null as string | null,
  });
  const historyExpandedRef = useRef(false);
  const childrenRef = useRef<ChildSessionSummary[]>([]);

  useEffect(() => {
    childrenRef.current = children;
  }, [children]);

  const report = useCallback((reason: unknown) => {
    if (reason instanceof DOMException && reason.name === "AbortError") return;
    setError(reason instanceof Error ? reason.message : String(reason));
  }, []);

  const clearInvocationView = useCallback(() => {
    setInvocation(null);
    setEvents([]);
    setResumeCursor(null);
    setHasEarlierEvents(false);
    setState(null);
    setSelectedEvent(null);
    childrenRef.current = [];
    setChildren([]);
    setChildCursor(null);
    setLoadingMoreChildren(false);
    setLoadingEarlierTrace(false);
    historyExpandedRef.current = false;
    setLive(false);
  }, []);

  const selectInvocation = useCallback((invocationId: string | null) => {
    const transition = switchInvocation(selectionRef.current, invocationId);
    if (!transition) return;
    for (const scope of transition.invalidatedScopes) requests.invalidate(scope);
    requests.invalidate("navigate");
    selectionRef.current = transition.selection;
    setSelectedInvocation(invocationId);
    clearInvocationView();
  }, [clearInvocationView, requests]);

  const selectSession = useCallback((sessionId: string | null) => {
    const transition = switchSession(selectionRef.current, sessionId);
    if (!transition) return;
    for (const scope of transition.invalidatedScopes) requests.invalidate(scope);
    requests.invalidate("navigate");
    selectionRef.current = transition.selection;
    setSelectedSession(sessionId);
    setSelectedInvocation(null);
    setInvocations([]);
    setInvocationCursor(null);
    setLoadingMoreInvocations(false);
    clearInvocationView();
  }, [clearInvocationView, requests]);

  const selectWorkflow = useCallback((workflowId: string | null) => {
    const transition = switchWorkflow(selectionRef.current, workflowId);
    if (!transition) return;
    for (const scope of transition.invalidatedScopes) requests.invalidate(scope);
    requests.invalidate("navigate");
    selectionRef.current = transition.selection;
    setSelectedWorkflow(workflowId);
    setSelectedSession(null);
    setSelectedInvocation(null);
    setSnapshot(null);
    setSessions([]);
    setSessionCursor(null);
    setLoadingMoreSessions(false);
    setInvocations([]);
    setInvocationCursor(null);
    setLoadingMoreInvocations(false);
    clearInvocationView();
  }, [clearInvocationView, requests]);

  useEffect(() => {
    const controller = new AbortController();
    const token = requests.start("workflows");
    setLoading(true);
    api.workflows(null, controller.signal)
      .then((page) => {
        if (!requests.isCurrent(token)) return;
        setWorkflows(page.items);
        setWorkflowCursor(page.next_cursor);
        if (selectionRef.current.workflow === null) {
          selectWorkflow(page.items[0]?.workflow_revision_id ?? null);
        }
        setError(null);
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      })
      .finally(() => {
        if (requests.finish(token)) setLoading(false);
      });
    return () => {
      controller.abort();
      if (requests.isCurrent(token)) requests.invalidate(token.scope);
    };
  }, [refreshKey, report, requests, selectWorkflow]);

  useEffect(() => {
    if (!selectedWorkflow) {
      requests.invalidate("workflow-bootstrap");
      setSnapshot(null);
      setSessions([]);
      setSessionCursor(null);
      return;
    }
    const controller = new AbortController();
    const token = requests.start("workflow-bootstrap");
    const workflowId = selectedWorkflow;
    Promise.all([
      api.workflow(workflowId, controller.signal),
      api.sessions(workflowId, null, controller.signal),
    ])
      .then(([definition, page]) => {
        if (
          !requests.isCurrent(token) ||
          selectionRef.current.workflow !== workflowId
        ) return;
        setSnapshot(definition);
        setSessions(page.items);
        setSessionCursor(page.next_cursor);
        if (selectionRef.current.session === null) {
          selectSession(page.items[0]?.session_id ?? null);
        }
        setError(null);
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      });
    return () => {
      controller.abort();
      if (requests.isCurrent(token)) requests.invalidate(token.scope);
    };
  }, [selectedWorkflow, refreshKey, report, requests, selectSession]);

  useEffect(() => {
    if (!selectedSession || !selectedWorkflow) {
      requests.invalidate("invocation-list");
      setInvocations([]);
      setInvocationCursor(null);
      return;
    }
    const controller = new AbortController();
    const token = requests.start("invocation-list");
    const sessionId = selectedSession;
    const workflowId = selectedWorkflow;
    api.invocations(sessionId, workflowId, null, controller.signal)
      .then((page) => {
        if (
          !requests.isCurrent(token) ||
          selectionRef.current.workflow !== workflowId ||
          selectionRef.current.session !== sessionId
        ) return;
        setInvocations(page.items);
        setInvocationCursor(page.next_cursor);
        if (selectionRef.current.invocation === null) {
          selectInvocation(page.items[0]?.invocation_id ?? null);
        }
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      });
    return () => {
      controller.abort();
      if (requests.isCurrent(token)) requests.invalidate(token.scope);
    };
  }, [
    selectedSession,
    selectedWorkflow,
    refreshKey,
    report,
    requests,
    selectInvocation,
  ]);

  useEffect(() => {
    requests.invalidate("sse");
    clearInvocationView();
    if (!selectedInvocation) {
      requests.invalidate("invocation-bootstrap");
      return;
    }
    const controller = new AbortController();
    const token = requests.start("invocation-bootstrap");
    const invocationId = selectedInvocation;
    void (async () => {
      const bootstrap = await loadInvocationBootstrap(
        invocationId,
        controller.signal,
        () =>
          requests.isCurrent(token) &&
          selectionRef.current.invocation === invocationId,
      );
      if (bootstrap === null) return;
      const { childPage, history, summary } = bootstrap;
      setEvents(dedupe(history.items).slice(-MAX_VISIBLE_EVENTS));
      setResumeCursor(history.resume_cursor);
      setHasEarlierEvents(history.has_earlier);
      childrenRef.current = childPage.items;
      setChildren(childPage.items);
      setChildCursor(childPage.next_cursor);
      // Invocation enables the SSE effect, so publish it only after the
      // resumable tail cursor and initial projections have been staged.
      setInvocation(summary);
      setError(null);
      const stateToken = requests.start("boundary-state");
      void api.state(invocationId, controller.signal)
        .then((response) => {
          if (
            requests.isCurrent(stateToken) &&
            selectionRef.current.invocation === invocationId
          ) setState(response.state);
        })
        .catch((reason) => {
          if (requests.isCurrent(stateToken)) report(reason);
        });
    })().catch((reason) => {
      if (requests.isCurrent(token)) report(reason);
    });
    return () => {
      controller.abort();
      if (requests.isCurrent(token)) requests.invalidate(token.scope);
    };
  }, [selectedInvocation, refreshKey, report, requests, clearInvocationView]);

  useEffect(() => {
    if (
      !selectedInvocation ||
      !invocation ||
      invocation.invocation_id !== selectedInvocation
    ) return;
    const invocationId = selectedInvocation;
    const token = requests.start("sse");
    const source = api.traceStream(invocationId, resumeCursor);
    const current = () =>
      requests.isCurrent(token) &&
      selectionRef.current.invocation === invocationId;
    const childRefresh = createCoalescedRefresh(async () => {
      if (!current()) return;
      const summaryToken = requests.start("boundary-summary");
      const stateToken = requests.start("boundary-state");
      const childToken = requests.start("boundary-children");
      const [summaryResult, stateResult, childResult] = await Promise.allSettled([
        api.invocation(invocationId),
        api.state(invocationId),
        api.childrenThroughKnown(
          invocationId,
          childrenRef.current.map((child) => child.session_id),
        ),
      ]);
      if (!current()) return;
      if (summaryResult.status === "fulfilled") {
        if (requests.isCurrent(summaryToken)) setInvocation(summaryResult.value);
      } else if (requests.isCurrent(summaryToken)) report(summaryResult.reason);
      if (stateResult.status === "fulfilled") {
        if (requests.isCurrent(stateToken)) setState(stateResult.value.state);
      } else if (requests.isCurrent(stateToken)) report(stateResult.reason);
      if (childResult.status === "fulfilled") {
        if (requests.isCurrent(childToken)) {
          setChildCursor(childResult.value.next_cursor);
          setChildren((current) => {
            const refreshedIds = new Set(
              childResult.value.items.map((child) => child.session_id),
            );
            const refreshed = [
              ...childResult.value.items,
              ...current.filter((child) => !refreshedIds.has(child.session_id)),
            ];
            childrenRef.current = refreshed;
            return refreshed;
          });
        }
      } else if (requests.isCurrent(childToken)) report(childResult.reason);
    }, report);
    const receive = (message: MessageEvent<string>) => {
      if (!current()) return;
      try {
        const event = JSON.parse(message.data) as TraceEvent;
        if (!event.id || !event.kind) return;
        if (message.lastEventId) setResumeCursor(message.lastEventId);
        setEvents((current) => {
          const merged = dedupe([...current, event]);
          if (
            merged.length > MAX_VISIBLE_EVENTS &&
            !historyExpandedRef.current
          ) {
            setHasEarlierEvents(true);
            return merged.slice(-MAX_VISIBLE_EVENTS);
          }
          return merged;
        });
        setLive(true);
        const refreshRuntimeState = () => {
          const stateToken = requests.start("boundary-state");
          api.state(invocationId)
            .then((response) => {
              if (
                requests.isCurrent(stateToken) &&
                selectionRef.current.invocation === invocationId
              ) setState(response.state);
            })
            .catch((reason) => {
              if (requests.isCurrent(stateToken)) report(reason);
            });
        };
        if (INVOCATION_BOUNDARY_KINDS.has(event.kind)) {
          const summaryToken = requests.start("boundary-summary");
          api.invocation(invocationId)
            .then((summary) => {
              if (
                requests.isCurrent(summaryToken) &&
                selectionRef.current.invocation === invocationId
              ) setInvocation(summary);
            })
            .catch((reason) => {
              if (requests.isCurrent(summaryToken)) report(reason);
            });
          refreshRuntimeState();
        }
        if (CHILD_BOUNDARY_KINDS.has(event.kind)) {
          setChildren((current) => {
            const next = applyChildTrace(current, event, invocation);
            childrenRef.current = next;
            return next;
          });
          childRefresh.request();
        }
      } catch (reason) {
        report(reason);
      }
    };
    const finish = () => {
      if (!current()) return;
      setLive(false);
      source.close();
      void childRefresh.flush().finally(() => {
        childRefresh.dispose();
        if (requests.isCurrent(token)) requests.invalidate(token.scope);
      });
    };
    source.onopen = () => {
      if (current()) setLive(true);
    };
    source.addEventListener("trace", receive as EventListener);
    source.addEventListener("stream_end", finish);
    source.onerror = () => {
      if (current()) setLive(false);
    };
    return () => {
      source.close();
      childRefresh.dispose();
      if (requests.isCurrent(token)) requests.invalidate(token.scope);
    };
    // Reconnect only when selection/history bootstrap changes. New events are
    // delivered by this EventSource and do not need to recreate it.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedInvocation, invocation?.invocation_id, report, requests]);

  const navigateToInvocation = useCallback((invocationId: string) => {
    selectInvocation(null);
    const token = requests.start("navigate");
    api.invocation(invocationId)
      .then((summary) => {
        if (!requests.isCurrent(token)) return;
        requests.finish(token);
        selectWorkflow(summary.workflow_revision_id);
        selectSession(summary.session_id);
        selectInvocation(summary.invocation_id);
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      })
      .finally(() => requests.finish(token));
  }, [report, requests, selectInvocation, selectSession, selectWorkflow]);

  const loadMoreWorkflows = useCallback(() => {
    if (!workflowCursor) return;
    const cursor = workflowCursor;
    const token = requests.tryStartExclusive("more-workflows");
    if (!token) return;
    setLoadingMoreWorkflows(true);
    api.workflows(cursor)
      .then((page) => {
        if (!requests.isCurrent(token) || workflowCursor !== cursor) return;
        setWorkflows((current) => mergeBy(current, page.items, "workflow_revision_id"));
        setWorkflowCursor(page.next_cursor);
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      })
      .finally(() => {
        if (requests.finish(token)) setLoadingMoreWorkflows(false);
      });
  }, [workflowCursor, report, requests]);

  const loadMoreSessions = useCallback(() => {
    if (!selectedWorkflow || !sessionCursor) return;
    const workflowId = selectedWorkflow;
    const cursor = sessionCursor;
    const token = requests.tryStartExclusive("more-sessions");
    if (!token) return;
    setLoadingMoreSessions(true);
    api.sessions(workflowId, cursor)
      .then((page) => {
        if (
          !requests.isCurrent(token) ||
          selectionRef.current.workflow !== workflowId ||
          sessionCursor !== cursor
        ) return;
        setSessions((current) => mergeBy(current, page.items, "session_id"));
        setSessionCursor(page.next_cursor);
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      })
      .finally(() => {
        if (requests.finish(token)) setLoadingMoreSessions(false);
      });
  }, [selectedWorkflow, sessionCursor, report, requests]);

  const loadMoreInvocations = useCallback(() => {
    if (!selectedWorkflow || !selectedSession || !invocationCursor) return;
    const workflowId = selectedWorkflow;
    const sessionId = selectedSession;
    const cursor = invocationCursor;
    const token = requests.tryStartExclusive("more-invocations");
    if (!token) return;
    setLoadingMoreInvocations(true);
    api.invocations(sessionId, workflowId, cursor)
      .then((page) => {
        if (
          !requests.isCurrent(token) ||
          selectionRef.current.workflow !== workflowId ||
          selectionRef.current.session !== sessionId ||
          invocationCursor !== cursor
        ) return;
        setInvocations((current) => mergeBy(current, page.items, "invocation_id"));
        setInvocationCursor(page.next_cursor);
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      })
      .finally(() => {
        if (requests.finish(token)) setLoadingMoreInvocations(false);
      });
  }, [selectedWorkflow, selectedSession, invocationCursor, report, requests]);

  const loadMoreChildren = useCallback(() => {
    if (!selectedInvocation || !childCursor) return;
    const invocationId = selectedInvocation;
    const cursor = childCursor;
    const token = requests.tryStartExclusive("more-children");
    if (!token) return;
    setLoadingMoreChildren(true);
    api.children(invocationId, cursor)
      .then((page) => {
        if (
          !requests.isCurrent(token) ||
          selectionRef.current.invocation !== invocationId ||
          childCursor !== cursor
        ) return;
        setChildren((current) => {
          const next = mergeBy(current, page.items, "session_id");
          childrenRef.current = next;
          return next;
        });
        setChildCursor(page.next_cursor);
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      })
      .finally(() => {
        if (requests.finish(token)) setLoadingMoreChildren(false);
      });
  }, [selectedInvocation, childCursor, report, requests]);

  const loadEarlierTrace = useCallback(() => {
    const firstSequence = events[0]?.trace_sequence;
    if (!selectedInvocation || !hasEarlierEvents || firstSequence === undefined) return;
    const invocationId = selectedInvocation;
    const token = requests.tryStartExclusive("earlier-trace");
    if (!token) return;
    setLoadingEarlierTrace(true);
    api.traceBefore(invocationId, firstSequence)
      .then((page) => {
        if (
          !requests.isCurrent(token) ||
          selectionRef.current.invocation !== invocationId
        ) return;
        historyExpandedRef.current = true;
        setEvents((current) => dedupe([...page.items, ...current]));
        setHasEarlierEvents(page.has_earlier);
      })
      .catch((reason) => {
        if (requests.isCurrent(token)) report(reason);
      })
      .finally(() => {
        if (requests.finish(token)) setLoadingEarlierTrace(false);
      });
  }, [events, selectedInvocation, hasEarlierEvents, report, requests]);

  const refresh = useCallback(() => {
    for (const scope of [
      "workflows",
      "workflow-bootstrap",
      "invocation-list",
      "invocation-bootstrap",
      "sse",
      "boundary-summary",
      "boundary-state",
      "boundary-children",
      "more-workflows",
      "more-sessions",
      "more-invocations",
      "more-children",
      "earlier-trace",
      "navigate",
    ]) requests.invalidate(scope);
    setLoadingMoreWorkflows(false);
    setLoadingMoreSessions(false);
    setLoadingMoreInvocations(false);
    setLoadingMoreChildren(false);
    setLoadingEarlierTrace(false);
    clearInvocationView();
    setRefreshKey((value) => value + 1);
  }, [clearInvocationView, requests]);

  const visibleEvents = useMemo(() => events, [events]);
  const hiddenCount = hasEarlierEvents
    ? Math.max(1, (visibleEvents[0]?.trace_sequence ?? 1) - 1)
    : 0;
  const selection = selectedEvent ?? visibleEvents.at(-1) ?? null;

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand"><span className="brand-mark"><Activity size={18} /></span><div><strong>AutoAgent</strong><small>local trace</small></div></div>
        <div className="topbar-status">
          <span className={`live-indicator ${live ? "active" : ""}`}><i />{live ? "Live" : "History"}</span>
          <button type="button" className="icon-button" onClick={refresh} title="Refresh">
            <RefreshCw size={16} className={loading ? "spinning" : ""} />
          </button>
        </div>
      </header>

      {error && <div className="error-banner">{error}<button type="button" onClick={() => setError(null)}>dismiss</button></div>}

      <aside className="navigation">
        <NavSection icon={<WorkflowIcon size={15} />} title="Workflows" count={workflows.length} hasMore={workflowCursor !== null} loadingMore={loadingMoreWorkflows} onLoadMore={loadMoreWorkflows}>
          {workflows.map((item) => (
            <NavButton key={item.workflow_revision_id} active={selectedWorkflow === item.workflow_revision_id} onClick={() => selectWorkflow(item.workflow_revision_id)}>
              <span>{item.workflow_id}</span><small>v{item.workflow_version}</small>
            </NavButton>
          ))}
        </NavSection>
        <NavSection icon={<Database size={15} />} title="Sessions" count={sessions.length} hasMore={sessionCursor !== null} loadingMore={loadingMoreSessions} onLoadMore={loadMoreSessions}>
          {sessions.map((item) => (
            <NavButton key={item.session_id} active={selectedSession === item.session_id} onClick={() => selectSession(item.session_id)}>
              <span>{shortId(item.session_id)}</span><Status value={item.status} />
            </NavButton>
          ))}
        </NavSection>
        <NavSection icon={<GitBranch size={15} />} title="Invocations" count={invocations.length} hasMore={invocationCursor !== null} loadingMore={loadingMoreInvocations} onLoadMore={loadMoreInvocations}>
          {invocations.map((item) => (
            <NavButton key={item.invocation_id} active={selectedInvocation === item.invocation_id} onClick={() => selectInvocation(item.invocation_id)}>
              <span>{shortId(item.invocation_id)}</span><Status value={item.status} />
            </NavButton>
          ))}
        </NavSection>
        {invocation && children.length > 0 && (
          <NavSection icon={<GitBranch size={15} />} title="Child tasks" count={children.length} hasMore={childCursor !== null} loadingMore={loadingMoreChildren} onLoadMore={loadMoreChildren}>
            {children.map((item) => (
              <NavButton
                key={item.session_id}
                active={selectedSession === item.session_id}
                disabled={!item.current_invocation_id}
                onClick={() => item.current_invocation_id && navigateToInvocation(item.current_invocation_id)}
              >
                <span>{`#${item.unit_index} ${item.planned_workflow_id}`}</span>
                <small>{`${item.mode} · ${item.phase}`}</small>
                <Status value={item.status} />
              </NavButton>
            ))}
          </NavSection>
        )}
      </aside>

      <section className="workspace">
        <div className="workspace-heading">
          <div>
            <p className="eyebrow">{snapshot?.workflow_id ?? "No workflow"}</p>
            <h1>{invocation ? `Invocation ${shortId(invocation.invocation_id)}` : "Runtime overview"}</h1>
          </div>
          {invocation && <div className="invocation-meta">
            {invocation.parent_invocation_id && (
              <button type="button" className="relation-button" onClick={() => navigateToInvocation(invocation.parent_invocation_id!)}>
                parent {shortId(invocation.parent_invocation_id)}
              </button>
            )}
            <Status value={invocation.status} /><span>{invocation.entry_node_id}</span>
          </div>}
        </div>
        <section className="panel graph-panel"><PanelHeading title="Workflow graph" subtitle={snapshot?.workflow_revision_id ?? "Portable definition"} /><WorkflowGraph workflow={snapshot} events={visibleEvents} /></section>
        <section className="panel timeline-panel">
          <PanelHeading title="Runtime timeline" subtitle={`${events.length.toLocaleString()} safe trace events`} />
          {hasEarlierEvents && (
            <button
              type="button"
              className="load-more timeline-load-more"
              disabled={loadingEarlierTrace}
              onClick={loadEarlierTrace}
            >
              {loadingEarlierTrace ? "Loading…" : "Load earlier trace"}
            </button>
          )}
          <TraceTimeline events={visibleEvents} hiddenCount={hiddenCount} selectedId={selection?.id ?? null} onSelect={setSelectedEvent} />
        </section>
      </section>

      <aside className="inspector">
        <div className="inspector-heading"><Braces size={15} /><strong>Inspector</strong></div>
        <JsonInspector title="Selected event" value={selection} placeholder="Select a trace event." />
        <JsonInspector title="Latest runtime state" value={state} placeholder="No Runtime State available." />
      </aside>
    </main>
  );
}

function dedupe(values: TraceEvent[]): TraceEvent[] {
  const byId = new Map<string, TraceEvent>();
  for (const value of values) byId.set(value.id, value);
  return [...byId.values()].sort((left, right) => left.trace_sequence - right.trace_sequence);
}

function mergeBy<T extends Record<K, string>, K extends keyof T>(
  current: T[],
  incoming: T[],
  key: K,
): T[] {
  const merged = new Map(current.map((item) => [item[key], item]));
  for (const item of incoming) merged.set(item[key], item);
  return [...merged.values()];
}

function NavSection({
  icon,
  title,
  count,
  children,
  hasMore = false,
  loadingMore = false,
  onLoadMore,
}: {
  icon: React.ReactNode;
  title: string;
  count: number;
  children: React.ReactNode;
  hasMore?: boolean;
  loadingMore?: boolean;
  onLoadMore?: () => void;
}) {
  return (
    <section className="nav-section">
      <header>{icon}<span>{title}</span><small>{count}</small></header>
      <div className="nav-items">
        {children}
        {hasMore && (
          <button
            type="button"
            className="load-more"
            disabled={loadingMore}
            onClick={onLoadMore}
          >
            {loadingMore ? "Loading…" : "Load more"}
          </button>
        )}
      </div>
    </section>
  );
}

function NavButton({ active, disabled = false, onClick, children }: { active: boolean; disabled?: boolean; onClick: () => void; children: React.ReactNode }) {
  return <button type="button" disabled={disabled} className={`nav-button ${active ? "active" : ""}`} onClick={onClick}>{children}<ChevronRight size={13} /></button>;
}

function PanelHeading({ title, subtitle }: { title: string; subtitle: string }) {
  return <header className="panel-heading"><strong>{title}</strong><small>{subtitle}</small></header>;
}

function Status({ value }: { value: string | null }) {
  return <small className={`status-pill status-${value ?? "unknown"}`}>{value ?? "unknown"}</small>;
}

function shortId(value: string): string {
  return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value;
}
