import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  ChevronDown,
  ChevronRight,
  CirclePause,
  Moon,
  Play,
  Radio,
  Search,
  Sun,
  Workflow,
} from "lucide-react";

import type {
  InvocationSummary,
  RuntimeState,
  SessionSummary,
  WorkflowSummary,
} from "../types";
import { listInvocations, listSessions } from "../api";

type ScopeColumn = "workflow" | "session" | "invocation";

interface ScopeBarProps {
  workflows: WorkflowSummary[];
  sessions: SessionSummary[];
  invocations: InvocationSummary[];
  workflowId: string | null;
  sessionId: string | null;
  invocationId: string | null;
  invocationState: RuntimeState | null;
  viewedWorkflow: Pick<
    WorkflowSummary,
    "workflow_id" | "workflow_version" | "definition_hash" | "operator_manifest_hash"
  > | null;
  viewedWorkflowIsRegistered: boolean;
  followLive: boolean;
  backendLive: boolean;
  darkMode: boolean;
  executionEnabled: boolean;
  canInvoke: boolean;
  invoking: boolean;
  onScopeChange: (workflowId: string, sessionId: string, invocationId: string) => void;
  onFollowLive: () => void;
  onOpenInvoke: () => void;
  onToggleTheme: () => void;
}

export function ScopeBar({
  workflows,
  sessions,
  invocations,
  workflowId,
  sessionId,
  invocationId,
  invocationState,
  viewedWorkflow,
  viewedWorkflowIsRegistered,
  followLive,
  backendLive,
  darkMode,
  executionEnabled,
  canInvoke,
  invoking,
  onScopeChange,
  onFollowLive,
  onOpenInvoke,
  onToggleTheme,
}: ScopeBarProps) {
  const selectedInvocation = invocations.find((value) => value.id === invocationId);
  return (
    <header className="scope-bar">
      <div className="brand-mark" aria-label="AutoAgent Trace">
        <Activity size={18} strokeWidth={2.2} />
        <span>AutoAgent</span>
        <strong>Trace</strong>
      </div>
      <ScopeNavigator
        workflows={workflows}
        workflowId={workflowId}
        sessionId={sessionId}
        invocationId={invocationId}
        onScopeChange={onScopeChange}
      />
      <div className="scope-actions">
        {viewedWorkflow && (
          <span
            className={`workflow-revision-pill ${viewedWorkflowIsRegistered ? "is-current" : "is-historical"}`}
            title={`Workflow ${viewedWorkflow.workflow_id}\nDefinition hash: ${viewedWorkflow.definition_hash}\nOperator manifest: ${viewedWorkflow.operator_manifest_hash}`}
          >
            {workflowRevisionLabel(
              viewedWorkflow.workflow_version,
              viewedWorkflow.definition_hash,
            )}
            <strong>{viewedWorkflowIsRegistered ? "Current app" : "History"}</strong>
          </span>
        )}
        {selectedInvocation && (
          <span className={`invocation-status-pill state-${stateClass(invocationState ?? selectedInvocation.state)}`}>
            {shortId(selectedInvocation.id)} · {invocationState ?? selectedInvocation.state}
          </span>
        )}
        <button
          className="toolbar-button"
          type="button"
          onClick={onOpenInvoke}
          disabled={!canInvoke || !executionEnabled || invoking}
          title={
            !executionEnabled
              ? "Execution API is disabled"
              : canInvoke
                ? "Invoke a workflow registered by the current App"
                : "The current App has no registered workflows"
          }
        >
          <Play size={15} />
          {invoking ? "Invoking" : "Invoke"}
        </button>
        <button
          className={`toolbar-button ${followLive ? "is-active" : ""}`}
          type="button"
          onClick={onFollowLive}
          disabled={!selectedInvocation}
          title={
            followLive
              ? "Enter replay at the previous cursor, or the pre-event graph"
              : "Return to the latest runtime event"
          }
        >
          {followLive ? <Radio size={15} /> : <CirclePause size={15} />}
          {followLive ? "Following" : "Replay"}
        </button>
        <span
          className={`connection-state ${backendLive ? "is-connected" : ""}`}
          title={backendLive ? "Backend service is reachable" : "Backend service or live polling is unavailable"}
        >
          <Radio size={13} />
          {backendLive ? "Live" : "Offline"}
        </span>
        <button
          className="icon-button"
          type="button"
          onClick={onToggleTheme}
          title={darkMode ? "Use light theme" : "Use dark theme"}
        >
          {darkMode ? <Sun size={17} /> : <Moon size={17} />}
        </button>
      </div>
    </header>
  );
}

function ScopeNavigator({
  workflows,
  workflowId,
  sessionId,
  invocationId,
  onScopeChange,
}: Pick<
  ScopeBarProps,
  | "workflows"
  | "workflowId"
  | "sessionId"
  | "invocationId"
  | "onScopeChange"
>) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [activeColumn, setActiveColumn] = useState<ScopeColumn>("workflow");
  const [draftScope, setDraftScope] = useState({ workflowId, sessionId, invocationId });
  const [queries, setQueries] = useState<Record<ScopeColumn, string>>({
    workflow: "",
    session: "",
    invocation: "",
  });
  const selectedWorkflow = workflows.find((value) => value.workflow_id === draftScope.workflowId);
  const sessionQuery = useQuery({
    queryKey: ["sessions", draftScope.workflowId],
    queryFn: () => listSessions(draftScope.workflowId!),
    enabled: Boolean(draftScope.workflowId),
  });
  const stagedSessions = sessionQuery.data ?? [];
  const selectedSession = stagedSessions.find((value) => value.id === draftScope.sessionId);
  const invocationQuery = useQuery({
    queryKey: ["invocations", draftScope.sessionId],
    queryFn: () => listInvocations(draftScope.sessionId!),
    enabled: Boolean(draftScope.sessionId),
  });
  const stagedInvocations = invocationQuery.data ?? [];
  const selectedInvocation = stagedInvocations.find((value) => value.id === draftScope.invocationId);
  const sortedWorkflows = useMemo(
    () => [...workflows].sort((left, right) => left.workflow_id.localeCompare(right.workflow_id)),
    [workflows],
  );
  const sortedSessions = useMemo(
    () => [...stagedSessions].sort((left, right) => right.updated_at_ms - left.updated_at_ms),
    [stagedSessions],
  );
  const sortedInvocations = useMemo(
    () => [...stagedInvocations].sort((left, right) => right.created_at_ms - left.created_at_ms),
    [stagedInvocations],
  );

  useEffect(() => {
    setDraftScope({ workflowId, sessionId, invocationId });
  }, [invocationId, sessionId, workflowId]);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: PointerEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  const setQuery = (column: ScopeColumn, value: string) => {
    setQueries((current) => ({ ...current, [column]: value }));
  };
  const chooseWorkflow = (id: string) => {
    setDraftScope({ workflowId: id, sessionId: null, invocationId: null });
    setActiveColumn("session");
    setQuery("session", "");
  };
  const chooseSession = (id: string) => {
    setDraftScope((current) => ({ ...current, sessionId: id, invocationId: null }));
    setActiveColumn("invocation");
    setQuery("invocation", "");
  };
  const chooseInvocation = (id: string) => {
    if (!draftScope.workflowId || !draftScope.sessionId) return;
    setDraftScope((current) => ({ ...current, invocationId: id }));
    onScopeChange(draftScope.workflowId, draftScope.sessionId, id);
    setOpen(false);
  };

  return (
    <div className="scope-navigator" ref={containerRef}>
      <button
        className={`scope-navigator-trigger ${open ? "is-open" : ""}`}
        type="button"
        aria-expanded={open}
        aria-haspopup="dialog"
        onClick={() => setOpen((current) => !current)}
        title="Select workflow, session, and invocation"
      >
        <ScopePathPart
          label="Workflow"
          value={selectedWorkflow ? workflowLabel(selectedWorkflow) : "Select workflow"}
          empty={!selectedWorkflow}
        />
        <ChevronRight className="scope-path-divider" size={14} />
        <ScopePathPart
          label="Session"
          value={selectedSession ? sessionLabel(selectedSession) : "Select session"}
          empty={!selectedSession}
        />
        <ChevronRight className="scope-path-divider" size={14} />
        <ScopePathPart
          label="Invocation"
          value={selectedInvocation ? invocationLabel(selectedInvocation) : "Select invocation"}
          empty={!selectedInvocation}
        />
        <ChevronDown className="scope-navigator-chevron" size={15} />
      </button>
      {open && (
        <section className="scope-navigator-panel" role="dialog" aria-label="Trace scope selector">
          <ScopeColumnList
            active={activeColumn === "workflow"}
            column="workflow"
            count={sortedWorkflows.length}
            emptyLabel="No workflow history is available."
            query={queries.workflow}
            onQueryChange={(value) => setQuery("workflow", value)}
            onActivate={() => setActiveColumn("workflow")}
          >
            {filterItems(sortedWorkflows, queries.workflow, workflowSearchText).map((workflow) => (
              <button
                key={workflow.workflow_id}
                className={`scope-navigator-item ${workflow.workflow_id === draftScope.workflowId ? "is-selected" : ""}`}
                type="button"
                onClick={() => chooseWorkflow(workflow.workflow_id)}
              >
                <span className="scope-navigator-item-title">
                  <Workflow size={13} />
                  {workflow.name || workflow.workflow_id}
                </span>
                <span className="scope-navigator-item-detail">
                  {shortId(workflow.workflow_id)} · {workflowRevisionLabel(workflow.workflow_version, workflow.definition_hash)}
                </span>
                <span className="scope-navigator-item-meta">
                  {workflowDirectoryLabel(workflow)}
                </span>
              </button>
            ))}
          </ScopeColumnList>
          <ScopeColumnList
            active={activeColumn === "session"}
            column="session"
            count={sortedSessions.length}
            disabled={!draftScope.workflowId}
            emptyLabel={draftScope.workflowId ? "No sessions for this workflow." : "Select a workflow first."}
            query={queries.session}
            onQueryChange={(value) => setQuery("session", value)}
            onActivate={() => setActiveColumn("session")}
          >
            {filterItems(sortedSessions, queries.session, sessionSearchText).map((session) => (
              <button
                key={session.id}
                className={`scope-navigator-item ${session.id === draftScope.sessionId ? "is-selected" : ""}`}
                type="button"
                onClick={() => chooseSession(session.id)}
              >
                <span className="scope-navigator-item-title">{session.session_key || "Unnamed session"}</span>
                <span className="scope-navigator-item-detail">{shortId(session.id)} · {formatRelativeTime(session.updated_at_ms)}</span>
                <span className="scope-navigator-item-meta">
                  {session.invocation_count} invocation{session.invocation_count === 1 ? "" : "s"}
                </span>
              </button>
            ))}
          </ScopeColumnList>
          <ScopeColumnList
            active={activeColumn === "invocation"}
            column="invocation"
            count={sortedInvocations.length}
            disabled={!draftScope.sessionId}
            emptyLabel={draftScope.sessionId ? "No invocations in this session." : "Select a session first."}
            query={queries.invocation}
            onQueryChange={(value) => setQuery("invocation", value)}
            onActivate={() => setActiveColumn("invocation")}
          >
            {filterItems(sortedInvocations, queries.invocation, invocationSearchText).map((invocation) => (
              <button
                key={invocation.id}
                className={`scope-navigator-item ${invocation.id === draftScope.invocationId ? "is-selected" : ""}`}
                type="button"
                onClick={() => chooseInvocation(invocation.id)}
              >
                <span className="scope-navigator-item-title">
                  {formatTime(invocation.created_at_ms)}
                  <em className={`scope-navigator-state state-${stateClass(invocation.state)}`}>{invocation.state}</em>
                </span>
                <span className="scope-navigator-item-detail">{shortId(invocation.id)} · {invocation.entry_node_id}</span>
                <span className="scope-navigator-item-meta">
                  {workflowRevisionLabel(invocation.workflow_version, invocation.definition_hash)}
                </span>
              </button>
            ))}
          </ScopeColumnList>
        </section>
      )}
    </div>
  );
}

function ScopePathPart({
  label,
  value,
  empty,
}: {
  label: string;
  value: string;
  empty: boolean;
}) {
  return (
    <span className={`scope-path-part ${empty ? "is-empty" : ""}`}>
      <small>{label}</small>
      <strong>{value}</strong>
    </span>
  );
}

function ScopeColumnList({
  active,
  column,
  count,
  disabled = false,
  emptyLabel,
  query,
  onQueryChange,
  onActivate,
  children,
}: {
  active: boolean;
  column: ScopeColumn;
  count: number;
  disabled?: boolean;
  emptyLabel: string;
  query: string;
  onQueryChange: (value: string) => void;
  onActivate: () => void;
  children: ReactNode;
}) {
  const label = column === "workflow" ? "Workflow" : column === "session" ? "Session" : "Invocation";
  const visible = Array.isArray(children) ? children.length : 0;
  return (
    <section
      className={`scope-navigator-column ${active ? "is-active" : ""} ${disabled ? "is-disabled" : ""}`}
      onPointerDown={onActivate}
    >
      <header>
        <span>{label}</span>
        <small>{count}</small>
      </header>
      <label className="scope-navigator-search">
        <Search size={13} />
        <input
          type="search"
          value={query}
          disabled={disabled}
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder={`Search ${label.toLowerCase()}s`}
        />
      </label>
      <div className="scope-navigator-list">
        {visible > 0 ? children : <p>{emptyLabel}</p>}
      </div>
    </section>
  );
}

function filterItems<T>(
  items: T[],
  query: string,
  text: (item: T) => string,
): T[] {
  const normalized = query.trim().toLowerCase();
  if (!normalized) return items;
  return items.filter((item) => text(item).toLowerCase().includes(normalized));
}

function workflowSearchText(workflow: WorkflowSummary): string {
  return [
    workflow.workflow_id,
    workflow.name,
    workflow.workflow_version,
    workflow.definition_hash,
    workflow.operator_manifest_hash,
  ].filter(Boolean).join(" ");
}

function sessionSearchText(session: SessionSummary): string {
  return [session.id, session.session_key, session.namespace].filter(Boolean).join(" ");
}

function invocationSearchText(invocation: InvocationSummary): string {
  return [
    invocation.id,
    invocation.entry_node_id,
    invocation.state,
    invocation.workflow_version,
    invocation.definition_hash,
  ].filter(Boolean).join(" ");
}

function workflowLabel(workflow: WorkflowSummary): string {
  return workflow.name || workflow.workflow_id;
}

function sessionLabel(session: SessionSummary): string {
  return session.session_key || shortId(session.id);
}

function invocationLabel(invocation: InvocationSummary): string {
  return `${formatTime(invocation.created_at_ms)} · ${invocation.state}`;
}

function shortId(value: string): string {
  return value.slice(0, 8);
}

function stateClass(value: RuntimeState): string {
  return String(value).replaceAll("_", "-");
}

function formatTime(value: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(value);
}

function formatRelativeTime(value: number): string {
  const deltaMs = Date.now() - value;
  if (deltaMs < 60_000) return "updated just now";
  if (deltaMs < 3_600_000) return `updated ${Math.floor(deltaMs / 60_000)}m ago`;
  if (deltaMs < 86_400_000) return `updated ${Math.floor(deltaMs / 3_600_000)}h ago`;
  return `updated ${Math.floor(deltaMs / 86_400_000)}d ago`;
}

function workflowDirectoryLabel(workflow: WorkflowSummary): string {
  const revisions = workflow.revision_count ?? 1;
  const source = workflow.registered_in_current_app ? "current" : "history";
  return `${revisions} revision${revisions === 1 ? "" : "s"} · ${source}`;
}

function workflowRevisionLabel(
  version: string | number | null,
  definitionHash: string | null,
): string {
  const resolvedVersion = version === null ? "v?" : `v${version}`;
  return definitionHash
    ? `${resolvedVersion} · ${definitionHash.slice(0, 8)}`
    : resolvedVersion;
}
