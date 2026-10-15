import { lazy, Suspense, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, config, getExecutionTrace, getWorkflowGraph } from "./api";
import { readLocation, writeLocation } from "./navigation";
import { FlowLoadingBoundary, FlowLoadingSurface } from "./components/FlowLoadingBoundary";
import { Inspector } from "./components/Inspector";
import { InspectorPresence } from './components/InspectorFrame';
import { WorkflowDirectory, WorkflowIcon } from "./components/WorkflowDirectory";
import { WorkflowToolbar } from "./components/WorkflowToolbar";
import { RunWorkflowPanel } from './components/RunWorkflowPanel';
import { Sidebar } from "./components/Sidebar";
import { ExecutionDirectory } from "./components/ExecutionDirectory";
import { ExecutionWorkspace } from "./components/ExecutionWorkspace";
import { useTheme, type Theme } from "./theme";
import { getExecutionLayout, setExecutionLayout } from './components/useExecutionLayout';
import brandIcon from "./autoagent.svg";
import type { AcceptedExecution, ExecutionTrace, LocationState, Selection, TraceRow, WorkflowGraphData } from "./types";

const WorkflowGraph = lazy(() => import("./components/WorkflowGraph"));

type View = LocationState["view"];
type FlowSnapshot = { key: string; workflow: WorkflowGraphData; trace: ExecutionTrace | null };
const familyGroup = (value: FlowSnapshot) => value.trace?.invocation.root_invocation_id ?? null;

function restoreLocation() {
  const state = readLocation(window.location.search);
  return state.view === 'executions' ? { ...state, executionView: state.executionView ?? getExecutionLayout().view } : state;
}
function useLocation() {
  const [location, setLocation] = useState(restoreLocation);
  const [views, setViews] = useState<Partial<Record<View, LocationState>>>(() => ({ [location.view]: location }));
  const activeView = useRef(location.view);
  useEffect(() => {
    const update = () => {
      const value = restoreLocation();
      if (value.executionView) setExecutionLayout({ view: value.executionView });
      activeView.current = value.view;
      setViews(current => ({ ...current, [value.view]: value }));
      setLocation(value);
    };
    window.addEventListener("popstate", update);
    return () => window.removeEventListener("popstate", update);
  }, []);
  const navigate = useCallback((requested: LocationState, replace = false, activate = true) => {
    const value = requested.view === 'executions' ? { ...requested, executionView: requested.executionView ?? getExecutionLayout().view } : requested;
    if (value.executionView && getExecutionLayout().view !== value.executionView) setExecutionLayout({ view: value.executionView });
    setViews(current => ({ ...current, [value.view]: value }));
    // Background panels may finish loading without changing the active tab or URL.
    if (!activate && value.view !== activeView.current) return;
    activeView.current = value.view;
    const url = new URL(window.location.href);
    url.search = writeLocation(value);
    window.history[replace ? "replaceState" : "pushState"](null, "", url);
    setLocation(value);
  }, []);
  useEffect(() => { if (location.executionView && getExecutionLayout().view !== location.executionView) setExecutionLayout({ view: location.executionView }); }, [location.executionView]);
  return [location, navigate, views] as const;
}

function BrowserWorkspace({ location, active, navigate, changeView, theme, accepted, onRunAccepted, onTraceSaved }: {
  accepted: AcceptedExecution | null; onRunAccepted: (receipt:AcceptedExecution)=>void; onTraceSaved:(iid:string)=>void;
  location: LocationState; active: boolean; theme: Theme;
  navigate: (value: LocationState, replace?: boolean) => void;
  changeView: (view: View) => void;
}) {
  const selections = useRef(new Map<string, { selection: Selection }>());
  useLayoutEffect(() => {
    if (!location.execution) return;
    selections.current.delete(location.execution);
    selections.current.set(location.execution, { selection: location.selection });
    if (selections.current.size > 20) selections.current.delete(selections.current.keys().next().value!);
  }, [location.execution, location.selection]);
  const inspectorOpen = Boolean(location.selection);
  const executionMode = location.view === "executions";
  const isAccepted = accepted?.invocation_id===location.execution;
  const [runTarget,setRunTarget]=useState<WorkflowGraphData|null>(null);
  useEffect(()=>{if(!active)setRunTarget(null);},[active]);
  useEffect(()=>setRunTarget(null),[location.workflow,location.revision]);
  const trace = useQuery({
    queryKey: ["execution-trace", location.execution],
    queryFn: ({ signal }) => getExecutionTrace(location.execution!, signal),
    enabled: executionMode && Boolean(location.execution) && (!isAccepted || active),
    refetchInterval: query=>isAccepted && !query.state.data && (!query.state.error || query.state.error instanceof ApiError && query.state.error.status===404)?Math.min(3000,500+query.state.errorUpdateCount*250):false,
    staleTime: 0, retry: false, gcTime: 60_000, refetchOnWindowFocus: false,
  });
  const activeTrace = executionMode && location.execution === trace.data?.invocation.invocation_id ? trace.data : undefined;
  useEffect(()=>{if(isAccepted && activeTrace)onTraceSaved(activeTrace.invocation.invocation_id);},[isAccepted,activeTrace,onTraceSaved]);
  const waitingForTrace=isAccepted && !activeTrace && (!trace.error || trace.error instanceof ApiError && trace.error.status===404);
  const revision = executionMode ? activeTrace?.invocation.workflow_revision_id : location.revision;
  const workflowId = executionMode ? activeTrace?.invocation.workflow_id : location.workflow;
  const detail = useQuery({
    queryKey: ["workflow", revision],
    queryFn: ({ signal }) => getWorkflowGraph(revision!, signal),
    enabled: Boolean(workflowId && revision),
    refetchOnWindowFocus: false,
    staleTime: 60_000,
    gcTime: 60_000,
  });
  const select = useCallback((row: { workflow_id: string; revision_id: string }, replace = false) => {
    navigate({ view: "workflows", execution: null, workflow: row.workflow_id, revision: row.revision_id, session: null, selection: null }, replace);
  }, [navigate]);

  const traceSession = activeTrace?.invocation.session_id;
  useEffect(() => {
    if (traceSession && traceSession !== location.session) navigate({ ...location, session: traceSession }, true);
  }, [traceSession, location, navigate]);
  const workflow = detail.data?.workflow_id === workflowId && detail.data?.workflow_revision_id === revision ? detail.data : undefined;
  const flowKey = executionMode ? location.execution ? `execution:${location.execution}` : null : location.revision ? `workflow:${location.revision}` : null;
  const flowFailed = Boolean(detail.error && !detail.isFetching || executionMode && !waitingForTrace && trace.error && !trace.data && !trace.isFetching);
  const flowPending = waitingForTrace || detail.isFetching || executionMode && trace.isFetching;
  const candidate = useMemo<FlowSnapshot | null>(() => flowKey && workflow && (!executionMode || activeTrace)
    ? { key: flowKey, workflow, trace: executionMode ? activeTrace! : null } : null, [flowKey, workflow, executionMode, activeTrace]);
  const [presented, setPresented] = useState<FlowSnapshot | null>(null);
  const presentedWorkflowId = presented?.workflow.workflow_id;
  const onPresented = useCallback((value: FlowSnapshot | null) => setPresented(value), []);
  useEffect(() => {
    if (active) document.title = presentedWorkflowId ? `${presentedWorkflowId} · AutoAgent Tracing` : "AutoAgent Tracing";
  }, [active, presentedWorkflowId]);
  const chooseObject = useCallback((selection: Selection) => {
    navigate({ ...location, selection });
  }, [location, navigate]);
  const openFamilyMember = useCallback((row: TraceRow) => {
    navigate({ ...location, execution: row.invocation_id, session: row.session_id ?? null, selection: selections.current.get(row.invocation_id)?.selection ?? null });
  }, [location, navigate]);
  const closeInspector = useCallback(() => {
    if (location.selection) navigate({ ...location, selection: null });
  }, [location, navigate]);
  useEffect(() => {
    if (!active || !inspectorOpen || runTarget) return;
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !document.querySelector("dialog[open]")) closeInspector();
    };
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [active, inspectorOpen, closeInspector, runTarget]);
  return <div className={`workspace workspace-tab ${active ? "" : "is-inactive"}`} data-workspace-view={location.view} aria-hidden={!active} inert={!active}>
      <Sidebar view={location.view} onChange={changeView}>{executionMode
        ? <ExecutionDirectory session={traceSession ?? location.session} invocation={location.execution}
          onSession={session => navigate({ ...location, session, execution: null, selection: null })}
          onInvocation={(execution, session) => navigate({ ...location, execution, session, selection: null })} />
        : <WorkflowDirectory workflowId={location.workflow} revisionId={location.revision} onSelect={select} />}</Sidebar>
      <main className="main-workspace" aria-label={presentedWorkflowId ? `Workflow graph: ${presentedWorkflowId}` : "Workflow graphs"}>
        <FlowLoadingBoundary flowKey={flowKey} candidate={candidate} pending={flowPending} failed={flowFailed} onPresented={onPresented}
          retainLimit={executionMode ? 4 : 1} retainGroup={executionMode ? familyGroup : undefined} pinnedKey={activeTrace ? `execution:${activeTrace.invocation.root_invocation_id}` : undefined}
          render={snapshot => {
            const selection = snapshot.key === flowKey ? location.selection
              : snapshot.trace ? selections.current.get(snapshot.trace.invocation.invocation_id)?.selection ?? null : null;
            return snapshot.trace ? <ExecutionWorkspace onRunAccepted={onRunAccepted} active={active && snapshot.key === flowKey} trace={snapshot.trace} workflow={snapshot.workflow} selection={selection} onSelect={chooseObject} onOpenExecution={openFamilyMember} onRefresh={async () => { await trace.refetch({ throwOnError: true, cancelRefetch: false }); }} view={location.executionView ?? getExecutionLayout().view} onChangeView={mode => navigate({ ...location, executionView: mode })} theme={theme} /> : <>
              <WorkflowToolbar workflow={snapshot.workflow} onRun={()=>setRunTarget(snapshot.workflow)} onRevision={revision => {setRunTarget(null);select({ workflow_id: snapshot.workflow.workflow_id, revision_id: revision });}} onDefinition={() => chooseObject({ kind: "workflow", id: snapshot.workflow.workflow_revision_id })} />
              <FlowLoadingSurface><Suspense fallback={null}><WorkflowGraph workflow={snapshot.workflow} selection={selection} onSelect={chooseObject} theme={theme} /></Suspense>
                <InspectorPresence open={Boolean(selection)} hidden={Boolean(runTarget)}>{selection ? <Inspector workflow={snapshot.workflow} selection={selection} onClose={closeInspector} /> : null}</InspectorPresence></FlowLoadingSurface>
            </>;
          }}
          fallback={<FlowLoadingSurface loadingLabel={waitingForTrace?'Run accepted · Waiting for saved trace':undefined}><div className="empty-state"><div className="empty-glyph"><WorkflowIcon /></div><h2>{waitingForTrace?'Waiting for saved trace':executionMode ? trace.isFetching || detail.isFetching ? "Loading execution" : trace.error || detail.error ? "Could not load execution" : "Explore workflow executions" : detail.isFetching ? "Loading workflow definition" : detail.error ? "Could not load definition" : "Explore workflow definitions"}</h2><p>{waitingForTrace?'The Run was accepted. Its trace will appear after persistence commits.':(executionMode ? trace.error?.message : null) ?? detail.error?.message ?? (executionMode ? location.session ? "Select an invocation from this session to inspect its execution." : "Select a session, then an invocation to inspect its execution." : "Select a workflow to explore its nodes, edges, and execution rules.")}</p>{!waitingForTrace && (trace.error || detail.error) ? <button className="refresh-button" onClick={() => { if (trace.error) void trace.refetch(); else void detail.refetch(); }}>Retry</button> : null}</div></FlowLoadingSurface>}
        />
        {runTarget && active?<RunWorkflowPanel key={runTarget.workflow_revision_id} workflow={runTarget} onClose={()=>setRunTarget(null)} onAccepted={receipt=>{setRunTarget(null);onRunAccepted(receipt);}} />:null}
      </main>
  </div>;
}


export default function App() {
  const [location, navigate, views] = useLocation();
  const [theme, toggleTheme] = useTheme();
  const queryClient=useQueryClient();
  const [accepted,setAccepted]=useState<AcceptedExecution|null>(null);
  const runAccepted=useCallback((receipt:AcceptedExecution)=>{
    setAccepted(receipt);
    navigate({view:'executions',execution:receipt.invocation_id,session:receipt.session_id,workflow:null,revision:null,selection:null});
  },[navigate]);
  const traceSaved=useCallback((iid:string)=>{
    setAccepted(current=>current?.invocation_id===iid?null:current);
    void queryClient.invalidateQueries({predicate:query=>['execution-sessions-page','execution-session','session-invocations-page'].includes(String(query.queryKey[0]))});
  },[queryClient]);
  const updateWorkspace = useCallback((value: LocationState, replace = false) => navigate(value, replace, false), [navigate]);
  const changeView = useCallback((view: View) => {
    if (view !== location.view) navigate(views[view] ?? { view, execution: null, workflow: null, revision: null, session: null, selection: null });
  }, [location.view, navigate, views]);
  return <div className="app-shell">
    <header className="topbar"><a className="brand" href={config.uiBase} aria-label="AutoAgent Tracing"><img className="brand-symbol" src={brandIcon} width="36" height="36" alt="" />AutoAgent Tracing</a>
      <div className="topbar-actions"><span className="local-badge"><span />Local</span>
      <button className="theme-switch" role="switch" aria-checked={theme === "dark"} aria-label="Dark theme" onClick={toggleTheme} title={`Switch to ${theme === "light" ? "dark" : "light"} theme`}>
        <svg className="theme-sun" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="4" /><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5" /></svg>
        <svg className="theme-moon" viewBox="0 0 24 24" aria-hidden="true"><path d="M20 15.5A9 9 0 0 1 8.5 4 9 9 0 1 0 20 15.5Z" /></svg>
        <span className="theme-switch-thumb" aria-hidden="true" />
      </button></div></header>
    <div className="workspace-tabs">
      {Object.values(views).map(value => <BrowserWorkspace key={value.view} location={value} active={value.view === location.view} navigate={updateWorkspace} changeView={changeView} theme={theme} accepted={accepted} onRunAccepted={runAccepted} onTraceSaved={traceSaved} />)}
    </div>
  </div>;
}
