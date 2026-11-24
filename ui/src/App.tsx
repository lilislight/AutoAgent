import { useWorkflowCatalogWatch } from './components/useWorkflowCatalogWatch';
import { lazy, Suspense, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, config, getExecutionTrace, getWorkflowGraph } from "./api";
import { isHiddenWorkflowError } from './workflowHttp';
import { readPageLocation, writePageLocation } from "./navigation";
import { FlowLoadingBoundary, FlowLoadingSurface } from "./components/FlowLoadingBoundary";
import { Inspector } from "./components/Inspector";
import { InspectorPresence } from './components/InspectorFrame';
import { WorkflowDirectory, WorkflowIcon } from "./components/WorkflowDirectory";
import { RevisionComparison } from "./components/RevisionComparison";
import { SessionInformation } from "./components/SessionInformation";
import { WorkflowToolbar } from "./components/WorkflowToolbar";
import { Sidebar } from "./components/Sidebar";
import { ExecutionDirectory } from "./components/ExecutionDirectory";
import { ExecutionWorkspace } from "./components/ExecutionWorkspace";
import { useTheme, type Theme } from "./theme";
import { getExecutionLayout, setExecutionLayout } from './components/useExecutionLayout';
import brandIcon from "./autoagent.svg";
import { MemoryMonitorButton } from './components/MemoryMonitor';
import { ApplicationSwitch } from './components/ApplicationSwitch';
import AgentWorkspace from "./agent/AgentWorkspace";
import { useAgentViewport } from './agent/useAgentViewport';
import { useAgentSidebar } from './agent/useAgentSidebar';
import type { AcceptedExecution, ExecutionTrace, LocationState, Selection, TraceRow, WorkflowGraphData } from "./types";

const WorkflowGraph = lazy(() => import("./components/WorkflowGraph"));

type View = LocationState["view"];
type FlowSnapshot = { key: string; workflow: WorkflowGraphData; trace: ExecutionTrace | null };
const familyGroup = (value: FlowSnapshot) => value.trace?.invocation.root_invocation_id ?? null;

function restoreLocation() {
  const state = readPageLocation(window.location.pathname, window.location.search, config.uiBase, config.defaultView);
  return state.view === 'executions' ? { ...state, executionView: state.executionView ?? getExecutionLayout().view } : state;
}
function useLocation() {
  const [location, setLocation] = useState(restoreLocation);
  const [views, setViews] = useState<Partial<Record<View, LocationState>>>(() => ({ [location.view]: location }));
  const activeView = useRef(location.view);
  useLayoutEffect(() => {
    if (location.view !== 'agents') return;
    const canonical = writePageLocation(location, config.uiBase, config.defaultView);
    if (window.location.pathname + window.location.search !== canonical) window.history.replaceState(null, '', canonical);
  }, [location]);
  useEffect(() => {
    const update = () => {
      const value = restoreLocation();
      if (value.executionView) setExecutionLayout({ view: value.executionView });
      if (value.view !== activeView.current && document.activeElement instanceof HTMLElement && document.activeElement.closest('.workspace-tab')) document.activeElement.blur();
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
    if (value.view !== activeView.current && document.activeElement instanceof HTMLElement && document.activeElement.closest('.workspace-tab')) document.activeElement.blur();
    activeView.current = value.view;
    const target = writePageLocation(value, config.uiBase, config.defaultView);
    const unchanged = window.location.pathname + window.location.search === target;
    window.history[replace || unchanged ? "replaceState" : "pushState"](null, "", target);
    setLocation(value);
  }, []);
  useEffect(() => { if (location.executionView && getExecutionLayout().view !== location.executionView) setExecutionLayout({ view: location.executionView }); }, [location.executionView]);
  return [location, navigate, views] as const;
}

function BrowserWorkspace({ location, active, navigate, changeView, theme, accepted, onRunAccepted, onTraceSaved, onOpenWorkflow }: {
  onOpenWorkflow: (workflow: string, revision: string) => void;
  accepted: AcceptedExecution | null; onRunAccepted: (receipt:AcceptedExecution)=>void; onTraceSaved:(iid:string)=>void;
  location: LocationState; active: boolean; theme: Theme;
  navigate: (value: LocationState, replace?: boolean) => void;
  changeView: (view: View) => void;
}) {
  const comparing = location.view === 'workflows' && Boolean(location.compareBase && location.revision && location.workflow);
  const comparisonReturn = useRef<LocationState | null>(null);
  const normalLocation = comparing ? comparisonReturn.current ?? location : location;
  const startComparison = (base: string) => { comparisonReturn.current = { ...location }; navigate({ ...location, compareBase: base, selection: null }); };
  const exitComparison = () => { const next = comparisonReturn.current ?? { ...location, compareBase: undefined, selection: null }; comparisonReturn.current = null; navigate(next); };
  const selections = useRef(new Map<string, { selection: Selection }>());
  useLayoutEffect(() => {
    if (!location.execution) return;
    selections.current.delete(location.execution);
    selections.current.set(location.execution, { selection: location.selection });
    if (selections.current.size > 20) selections.current.delete(selections.current.keys().next().value!);
  }, [location.execution, location.selection]);
  const inspectorOpen = Boolean(location.selection);
  const executionMode = location.view === "executions";
  const [directoryLoading, setDirectoryLoading] = useState(true);
  const [hiddenNotice, setHiddenNotice] = useState(false);
  const isAccepted = accepted?.invocation_id===location.execution;
  const [workflowFocus, setWorkflowFocus] = useState(0);
  const trace = useQuery({
    queryKey: ["execution-trace", location.execution],
    queryFn: ({ signal }) => getExecutionTrace(location.execution!, signal),
    enabled: executionMode && Boolean(location.execution) && (!isAccepted || active),
    refetchInterval: query=>isAccepted && !query.state.data && !isHiddenWorkflowError(query.state.error) && (!query.state.error || query.state.error instanceof ApiError && query.state.error.status===404)?Math.min(3000,500+query.state.errorUpdateCount*250):false,
    staleTime: 0, retry: false, gcTime: 60_000, refetchOnWindowFocus: false,
  });
  const activeTrace = executionMode && location.execution === trace.data?.invocation.invocation_id ? trace.data : undefined;
  useEffect(()=>{if(isAccepted && activeTrace)onTraceSaved(activeTrace.invocation.invocation_id);},[isAccepted,activeTrace,onTraceSaved]);
  const waitingForTrace=isAccepted && !activeTrace && !isHiddenWorkflowError(trace.error) && (!trace.error || trace.error instanceof ApiError && trace.error.status===404);
  const revision = executionMode ? activeTrace?.invocation.workflow_revision_id : normalLocation.revision;
  const workflowId = executionMode ? activeTrace?.invocation.workflow_id : normalLocation.workflow;
  const detail = useQuery({
    queryKey: ["workflow", revision],
    queryFn: ({ signal }) => getWorkflowGraph(revision!, signal),
    enabled: Boolean(workflowId && revision),
    refetchOnWindowFocus: false,
    staleTime: 60_000,
    gcTime: 60_000,
  });
  const hiddenSelection = isHiddenWorkflowError(trace.error) || isHiddenWorkflowError(detail.error)
    || !!location.workflow && !!config.workflowUiHiddenWorkflows?.includes(location.workflow);
  useEffect(() => {
    if (!hiddenSelection) return;
    setHiddenNotice(true);
    navigate({ ...location, workflow: null, revision: null, execution: null, session: null, selection: null, compareBase: undefined }, true);
  }, [hiddenSelection, location, navigate]);
  useEffect(() => { if ((location.execution || location.workflow) && !hiddenSelection) setHiddenNotice(false); },
    [location.execution, location.workflow, hiddenSelection]);
  const pinnedRevision = useRef(false);
  useWorkflowCatalogWatch(active, (previous, current) => {
    const id = location.workflow, next = id ? current.workflows[id] : null;
    if (!executionMode && !comparing && id && next && !pinnedRevision.current && location.revision === previous.workflows[id] && next !== location.revision)
      navigate({ ...location, revision: next, selection: null }, true);
  });
  const select = useCallback((row: { workflow_id: string; revision_id: string }, replace = false) => {
    pinnedRevision.current = false;
    navigate({ view: "workflows", execution: null, workflow: row.workflow_id, revision: row.revision_id, session: null, selection: null }, replace);
  }, [navigate]);

  const traceSession = activeTrace?.invocation.session_id;
  useEffect(() => {
    if (traceSession && traceSession !== location.session) navigate({ ...location, session: traceSession }, true);
  }, [traceSession, location, navigate]);
  const workflow = detail.data?.workflow_id === workflowId && detail.data?.workflow_revision_id === revision ? detail.data : undefined;
  const flowKey = executionMode ? location.execution ? `execution:${location.execution}` : null : revision ? `workflow:${revision}` : null;
  const flowFailed = hiddenSelection || Boolean(detail.error && !detail.data && !detail.isFetching || executionMode && !waitingForTrace && trace.error && !trace.data && !trace.isFetching);
  const flowPending = waitingForTrace || detail.isFetching || executionMode && trace.isFetching;
  const candidate = useMemo<FlowSnapshot | null>(() => !hiddenSelection && flowKey && workflow && (!executionMode || activeTrace)
    ? { key: flowKey, workflow, trace: executionMode ? activeTrace! : null } : null, [hiddenSelection, flowKey, workflow, executionMode, activeTrace]);
  const [presented, setPresented] = useState<FlowSnapshot | null>(null);
  const presentedWorkflowId = presented?.workflow.workflow_id;
  const onPresented = useCallback((value: FlowSnapshot | null) => setPresented(value), []);
  useEffect(() => {
    if (active) document.title = presentedWorkflowId ? `${presentedWorkflowId} · AutoAgent Tracing` : "AutoAgent Tracing";
  }, [active, presentedWorkflowId]);
  const chooseObject = useCallback((selection: Selection) => {
    navigate({ ...location, selection });
  }, [location, navigate]);
  const openFamilyMember = useCallback((row: TraceRow, selected?: Selection) => {
    navigate({ ...location, execution: row.invocation_id, session: row.session_id ?? null, selection: selected ?? selections.current.get(row.invocation_id)?.selection ?? null });
  }, [location, navigate]);
  const closeInspector = useCallback(() => {
    if (location.selection) navigate({ ...location, selection: null });
  }, [location, navigate]);
  useEffect(() => {
    if (!active || comparing || !inspectorOpen) return;
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !document.querySelector("dialog[open]")) closeInspector();
    };
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [active, comparing, inspectorOpen, closeInspector ]);
  return <>
    {comparing ? <RevisionComparison workflowId={location.workflow!} base={location.compareBase!} target={location.revision!} active={active} theme={theme} changeView={changeView} onPair={(base, target) => navigate({ ...location, compareBase: base, revision: target, selection: null })} onExit={exitComparison} /> : null}
    <div className={`workspace workspace-tab ${active && !comparing ? "" : "is-inactive"}`} data-workspace-view={location.view} aria-hidden={!active || comparing} inert={!active || comparing}>
      <Sidebar view={location.view} onChange={changeView}>{executionMode
        ? <ExecutionDirectory onRunAccepted={onRunAccepted} onLoadingChange={setDirectoryLoading} session={traceSession ?? location.session} invocation={location.execution}
          onSession={session => navigate({ ...location, session, execution: null, selection: null })}
          onInvocation={(execution, session) => navigate({ ...location, execution, session, selection: null })} />
        : <WorkflowDirectory workflowId={location.workflow} revisionId={location.revision} onSelect={select} onLoadingChange={setDirectoryLoading} />}</Sidebar>
      <main className="main-workspace" aria-label={presentedWorkflowId ? `Workflow graph: ${presentedWorkflowId}` : "Workflow graphs"}>
        {executionMode && location.session && !location.execution ? <SessionInformation key={location.session} session={location.session} onInvocation={openFamilyMember} onDefinition={onOpenWorkflow}
          onHidden={() => { setHiddenNotice(true); navigate({ ...location, workflow: null, revision: null, execution: null, session: null, selection: null }, true); }} /> : null}
        <div className={`workspace-flow-container ${executionMode && location.session && !location.execution ? 'is-inactive' : ''}`}>
        <FlowLoadingBoundary flowKey={flowKey} candidate={candidate} pending={flowPending} failed={flowFailed} onPresented={onPresented}
          retainLimit={executionMode ? 4 : 1} retainGroup={executionMode ? familyGroup : undefined} pinnedKey={activeTrace ? `execution:${activeTrace.invocation.root_invocation_id}` : undefined}
          render={snapshot => {
            const selection = snapshot.key === flowKey ? normalLocation.selection
              : snapshot.trace ? selections.current.get(snapshot.trace.invocation.invocation_id)?.selection ?? null : null;
            return snapshot.trace ? <ExecutionWorkspace onRunAccepted={onRunAccepted} active={active && snapshot.key === flowKey} trace={snapshot.trace} workflow={snapshot.workflow} selection={selection} onSelect={chooseObject} onOpenExecution={openFamilyMember} onRefresh={async () => { await trace.refetch({ throwOnError: true, cancelRefetch: false }); }} view={location.executionView ?? getExecutionLayout().view} onChangeView={mode => navigate({ ...location, executionView: mode })} theme={theme} /> : <>
              <WorkflowToolbar workflow={snapshot.workflow} onRevision={revision => { select({ workflow_id: snapshot.workflow.workflow_id, revision_id: revision }); pinnedRevision.current = true; }} onDefinition={() => chooseObject({ kind: "workflow", id: snapshot.workflow.workflow_revision_id })} onCompare={startComparison} />
              <FlowLoadingSurface><Suspense fallback={null}><WorkflowGraph workflow={snapshot.workflow} selection={selection} onSelect={chooseObject} theme={theme} focusRequest={workflowFocus} /></Suspense>
                <InspectorPresence open={Boolean(selection)}>{selection ? <Inspector workflow={snapshot.workflow} selection={selection} onClose={closeInspector} onRunAccepted={onRunAccepted} onEntry={id => { chooseObject({ kind: "node", id }); setWorkflowFocus(value => value + 1); }} /> : null}</InspectorPresence></FlowLoadingSurface>
            </>;
          }}
          fallback={hiddenNotice || hiddenSelection ? <FlowLoadingSurface><div className="empty-state" role="status"><h2>Agent App is hidden in this view.</h2></div></FlowLoadingSurface> : <FlowLoadingSurface pending={!flowKey && directoryLoading} loadingLabel={waitingForTrace?'Run accepted · Waiting for saved trace':undefined}><div className="empty-state"><div className="empty-glyph"><WorkflowIcon /></div><h2>{waitingForTrace?'Waiting for saved trace':executionMode ? trace.isFetching || detail.isFetching ? "Loading execution" : trace.error || detail.error ? "Could not load execution" : "Explore workflow executions" : detail.isFetching ? "Loading workflow definition" : detail.error ? "Could not load definition" : "Explore workflow definitions"}</h2><p>{waitingForTrace?'The Run was accepted. Its trace will appear after persistence commits.':(executionMode ? trace.error?.message : null) ?? detail.error?.message ?? (executionMode ? location.session ? "Select an invocation from this session to inspect its execution." : "Select a session, then an invocation to inspect its execution." : "Select a workflow to explore its nodes, edges, and execution rules.")}</p>{!waitingForTrace && (trace.error || detail.error) ? <button className="refresh-button" onClick={() => { if (trace.error) void trace.refetch(); else void detail.refetch(); }}>Retry</button> : null}</div></FlowLoadingSurface>}
        />
        </div>
      </main>
  </div></>;
}


export default function App() {
  const [location, navigate, views] = useLocation();
  const [theme, toggleTheme] = useTheme();
  const agentSidebar = useAgentSidebar(location.view === 'agents');
  const agentMobile = location.view === 'agents' && agentSidebar.narrow;
  useAgentViewport(agentMobile);
  const lastDeveloperView = useRef<'workflows' | 'executions'>(location.view === 'executions' ? 'executions' : 'workflows');
  useEffect(() => { if (location.view !== 'agents') lastDeveloperView.current = location.view; }, [location.view]);
  const previousView = useRef(location.view);
  useEffect(() => {
    if (previousView.current === location.view) return;
    previousView.current = location.view;
    const frame = requestAnimationFrame(() => document.querySelector<HTMLElement>('.workspace-tab:not(.is-inactive) .activity-button.active')?.focus({ preventScroll: true }));
    return () => cancelAnimationFrame(frame);
  }, [location.view]);
  const queryClient=useQueryClient();
  const [accepted,setAccepted]=useState<AcceptedExecution|null>(null);
  const runAccepted=useCallback((receipt:AcceptedExecution)=>{
    setAccepted(receipt);
    navigate({view:'executions',execution:receipt.invocation_id,session:receipt.session_id,workflow:null,revision:null,selection:null});
  },[navigate]);
  const traceSaved=useCallback((iid:string)=>{
    setAccepted(current=>current?.invocation_id===iid?null:current);
    void queryClient.invalidateQueries({predicate:query=>['execution-sessions-page','execution-session','session-invocations-page','session-information','session-history','session-workflows'].includes(String(query.queryKey[0]))});
  },[queryClient]);
  const openWorkflow = useCallback((workflow: string, revision: string) => {
    navigate({ view: 'workflows', workflow, revision, session: null, execution: null, selection: null });
  }, [navigate]);
  const updateWorkspace = useCallback((value: LocationState, replace = false) => navigate(value, replace, false), [navigate]);
  const changeView = useCallback((view: View) => {
    if (view !== location.view) navigate(views[view] ?? { view, execution: null, workflow: null, revision: null, session: null, selection: null });
  }, [location.view, navigate, views]);
  const developerView = location.view === 'agents' ? lastDeveloperView.current : location.view;
  return <div className={`app-shell ${location.view === 'agents' ? 'agent-mode' : ''}${agentMobile ? ' agent-mobile' : ''}${agentMobile && agentSidebar.open ? ' agent-mobile-directory-open' : ''}`}>
    <header className="topbar"><div className="topbar-leading"><a className="brand" href={config.uiBase} aria-label="AutoAgent Tracing"><img className="brand-symbol" src={brandIcon} width="36" height="36" alt="" /><span className="brand-name">AutoAgent<span className="brand-extra"> Tracing</span></span></a></div>
      <ApplicationSwitch agent={location.view === 'agents'} agentHref={writePageLocation(views.agents ?? { view: 'agents', session: null, execution: null, workflow: null, revision: null, selection: null }, config.uiBase, config.defaultView)} workflowHref={writePageLocation(views[developerView] ?? { view: developerView, session: null, execution: null, workflow: null, revision: null, selection: null }, config.uiBase, config.defaultView)} onChange={agent => changeView(agent ? 'agents' : developerView)} />
      <div className="topbar-actions"><MemoryMonitorButton /><span className="local-badge"><span />Local</span>
      <button className="theme-switch" role="switch" aria-checked={theme === "dark"} aria-label="Dark theme" onClick={toggleTheme} title={`Switch to ${theme === "light" ? "dark" : "light"} theme`}>
        <svg className="theme-sun" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="4" /><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5" /></svg>
        <svg className="theme-moon" viewBox="0 0 24 24" aria-hidden="true"><path d="M20 15.5A9 9 0 0 1 8.5 4 9 9 0 1 0 20 15.5Z" /></svg>
        <span className="theme-switch-thumb" aria-hidden="true" />
      </button></div></header>
    <div className="workspace-tabs">
      {Object.values(views).map(value => value.view === 'agents' ? <AgentWorkspace key="agents" theme={theme} sidebar={agentSidebar} session={value.session} invocation={value.execution} workspace={value.agentWorkspace} active={value.view === location.view} onNavigate={(session, execution, replace, workspace) => updateWorkspace({ view: 'agents', session, execution, workflow: null, revision: null, selection: null, agentWorkspace: workspace }, replace)} /> : <BrowserWorkspace key={value.view} location={value} active={value.view === location.view} navigate={updateWorkspace} changeView={changeView} theme={theme} accepted={accepted} onRunAccepted={runAccepted} onTraceSaved={traceSaved} onOpenWorkflow={openWorkflow} />)}
    </div>
  </div>;
}
