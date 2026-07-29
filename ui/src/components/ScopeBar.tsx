import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  ChevronDown,
  ChevronRight,
  Database,
  LoaderCircle,
  MessageCircle,
  Moon,
  RefreshCw,
  Search,
  Sun,
  Workflow,
} from "lucide-react";

import type {
  InvocationSummary,
  RuntimeStatus,
  RuntimeState,
  SessionSummary,
  WorkflowSummary,
} from "../types";
import { listInvocations, listSessions } from "../api";

type ScopeColumn = "workflow" | "session" | "invocation";

interface ScopeBarProps {
  workflows: WorkflowSummary[];
  invocations: InvocationSummary[];
  workflowId: string | null;
  sessionId: string | null;
  invocationId: string | null;
  invocationState: RuntimeState | null;
  runtimeStatus: RuntimeStatus | null;
  darkMode: boolean;
  refreshingInvocation: boolean;
  canCancelInvocation: boolean;
  cancellingInvocation: boolean;
  agentOpen: boolean;
  agentUnreadCount: number;
  onRefreshInvocation: () => void;
  onCancelInvocation: () => void;
  onInspectInvocation: () => void;
  onToggleAgent: () => void;
  onScopeChange: (workflowId: string, sessionId: string, invocationId: string) => void;
  onToggleTheme: () => void;
}

export function ScopeBar({
  workflows,
  invocations,
  workflowId,
  sessionId,
  invocationId,
  invocationState,
  runtimeStatus,
  darkMode,
  refreshingInvocation,
  canCancelInvocation,
  cancellingInvocation,
  agentOpen,
  agentUnreadCount,
  onRefreshInvocation,
  onCancelInvocation,
  onInspectInvocation,
  onToggleAgent,
  onScopeChange,
  onToggleTheme,
}: ScopeBarProps) {
  const selectedInvocation = invocations.find((value) => value.id === invocationId);
  const [persistenceOpen, setPersistenceOpen] = useState(false);
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
        {sessionId && (
          <button
            className={`icon-button agent-trigger ${agentOpen ? "is-active" : ""}`}
            type="button"
            onClick={onToggleAgent}
            title="Open Agent Activity for this Session"
            aria-label="Open Agent Activity"
            aria-expanded={agentOpen}
          >
            <MessageCircle size={17} />
            {agentUnreadCount > 0 && (
              <span className="agent-unread-badge">
                {agentUnreadCount > 99 ? "99+" : agentUnreadCount}
              </span>
            )}
          </button>
        )}
        {selectedInvocation && (
          <button
            className="icon-button"
            type="button"
            onClick={onRefreshInvocation}
            disabled={refreshingInvocation}
            title="Refresh latest invocation state and graph projection"
            aria-label="Refresh latest invocation"
          >
            <RefreshCw
              className={refreshingInvocation ? "spin" : undefined}
              size={16}
            />
          </button>
        )}
        {selectedInvocation && (
          <div className={`invocation-status-control state-${stateClass(invocationState ?? selectedInvocation.state)}`}>
            <button
              className="invocation-status-inspect"
              type="button"
              onClick={onInspectInvocation}
              title="Inspect this invocation"
            >
              {invocationState ?? selectedInvocation.state}
            </button>
            {canCancelInvocation && (
              <button
                className="invocation-status-cancel"
                type="button"
                onClick={onCancelInvocation}
                disabled={cancellingInvocation}
              >
                {cancellingInvocation ? "Cancelling" : "Cancel"}
              </button>
            )}
          </div>
        )}
        {selectedInvocation?.event_mode && (
          <span className="event-mode-pill">{selectedInvocation.event_mode}</span>
        )}
        <div className="persistence-control">
          <button
            className={`persistence-pill tone-${persistenceTone(runtimeStatus, selectedInvocation)}`}
            type="button"
            onClick={() => setPersistenceOpen((value) => !value)}
            aria-expanded={persistenceOpen}
          >
            <Database size={14} />
            {persistenceLabel(runtimeStatus, selectedInvocation)}
          </button>
          {persistenceOpen && (
            <PersistencePopover
              status={runtimeStatus}
              invocation={selectedInvocation ?? null}
              onClose={() => setPersistenceOpen(false)}
            />
          )}
        </div>
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

function PersistencePopover({
  status,
  invocation,
  onClose,
}: {
  status: RuntimeStatus | null;
  invocation: InvocationSummary | null;
  onClose: () => void;
}) {
  const panelRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      if (!panelRef.current?.parentElement?.contains(event.target as Node)) onClose();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [onClose]);
  const persistence = status?.persistence;
  const lag = Math.max(
    0,
    (invocation?.live_sequence ?? 0) - (invocation?.durable_sequence ?? 0),
  );
  return (
    <section className="persistence-popover" ref={panelRef}>
      <header>
        <div>
          <span>Runtime durability</span>
          <strong>{persistence?.backend_kind ?? "In-memory store"}</strong>
        </div>
        <span className={`persistence-health tone-${persistenceTone(status, invocation)}`}>
          {persistence?.health ?? "memory only"}
        </span>
      </header>
      <dl>
        <dt>Queue</dt>
        <dd>{persistence?.pending_count ?? 0} records · {formatBytes(persistence?.pending_bytes ?? 0)}</dd>
        <dt>Worker</dt>
        <dd>{persistence?.worker_state ?? "not configured"}</dd>
        <dt>Pressure</dt>
        <dd>{persistence?.pressure ?? "normal"}</dd>
        <dt>Admission</dt>
        <dd>{status?.execution.accepting_invocations === false ? "Paused" : "Accepting"}</dd>
        {invocation && (
          <>
            <dt>Invocation</dt>
            <dd>{invocation.persistence_status ?? "unknown"}</dd>
            <dt>Sequence</dt>
            <dd>{invocation.durable_sequence ?? 0} / {invocation.live_sequence ?? 0} · lag {lag}</dd>
          </>
        )}
      </dl>
      {persistence?.last_error && <p>{persistence.last_error}</p>}
      {persistence?.enabled && (
        <div className="persistence-meter">
          <i style={{
            width: `${Math.min(100, ((persistence.pending_bytes || 0) / Math.max(1, persistence.hard_watermark_bytes)) * 100)}%`,
          }} />
        </div>
      )}
    </section>
  );
}

function persistenceLabel(
  status: RuntimeStatus | null,
  invocation: InvocationSummary | undefined,
): string {
  if (!status || !status.persistence.enabled) return "Memory only";
  if (status.persistence.health === "unavailable") return "Persistence unavailable";
  if (status.persistence.pressure === "hard") return "Persistence full";
  const lag = Math.max(
    0,
    (invocation?.live_sequence ?? 0) - (invocation?.durable_sequence ?? 0),
  );
  if (lag > 0 || status.persistence.pending_count > 0) {
    return `Persisting · ${status.persistence.pending_count} queued`;
  }
  return "Durable";
}

function persistenceTone(
  status: RuntimeStatus | null,
  invocation: InvocationSummary | undefined | null,
): string {
  if (!status || !status.persistence.enabled) return "neutral";
  if (status.persistence.health === "unavailable" || invocation?.persistence_status === "degraded") {
    return "danger";
  }
  if (
    status.persistence.health === "retrying" ||
    status.persistence.pressure !== "normal" ||
    invocation?.persistence_status === "pending"
  ) {
    return "warning";
  }
  return "success";
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
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
    () => [...workflows].sort((left, right) => {
      const sourceOrder =
        Number(Boolean(right.registered_in_current_app)) -
        Number(Boolean(left.registered_in_current_app));
      return sourceOrder || left.workflow_id.localeCompare(right.workflow_id);
    }),
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
            loading={sessionQuery.isLoading || sessionQuery.isFetching}
            error={sessionQuery.error}
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
            loading={invocationQuery.isLoading || invocationQuery.isFetching}
            error={invocationQuery.error}
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
                  {invocation.event_mode && (
                    <em className="scope-navigator-mode">{invocation.event_mode}</em>
                  )}
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
  loading = false,
  error = null,
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
  loading?: boolean;
  error?: Error | null;
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
        {loading ? (
          <p className="scope-column-status">
            <LoaderCircle className="spin" size={14} />
            Loading {label.toLowerCase()}s…
          </p>
        ) : error ? (
          <p className="scope-column-status is-error">
            {error.message}
          </p>
        ) : visible > 0 ? children : <p>{emptyLabel}</p>}
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
    invocation.event_mode,
  ].filter(Boolean).join(" ");
}

function workflowLabel(workflow: WorkflowSummary): string {
  return workflow.name || workflow.workflow_id;
}

function sessionLabel(session: SessionSummary): string {
  return session.session_key || shortId(session.id);
}

function invocationLabel(invocation: InvocationSummary): string {
  return [
    formatTime(invocation.created_at_ms),
    invocation.state,
    invocation.event_mode,
  ].filter(Boolean).join(" · ");
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
  const source = workflow.registered_in_current_app
    ? "Registered"
    : "Historical only";
  return `${source} · ${revisions} revision${revisions === 1 ? "" : "s"}`;
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
