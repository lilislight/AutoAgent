import {
  Activity,
  CirclePause,
  Moon,
  Play,
  Radio,
  Sun,
} from "lucide-react";

import type {
  InvocationSummary,
  RuntimeState,
  SessionSummary,
  WorkflowSummary,
} from "../types";

const MAX_SCOPE_OPTIONS = 80;

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
  onWorkflowChange: (value: string | null) => void;
  onSessionChange: (value: string | null) => void;
  onInvocationChange: (value: string | null) => void;
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
  onWorkflowChange,
  onSessionChange,
  onInvocationChange,
  onFollowLive,
  onOpenInvoke,
  onToggleTheme,
}: ScopeBarProps) {
  const selectedInvocation = invocations.find((value) => value.id === invocationId);
  const workflowOptions = limitOptions(
    workflows.map((value) => ({
      value: value.workflow_id,
      label: `${shortId(value.workflow_id)} · ${value.name || value.workflow_id} · ${workflowDirectoryLabel(value)}`,
    })),
    workflowId,
  );
  const sessionOptions = limitOptions(
    [...sessions]
      .sort((left, right) => right.updated_at_ms - left.updated_at_ms)
      .map((value) => ({
        value: value.id,
        label: `${shortId(value.id)} · ${value.session_key || "no key"}`,
      })),
    sessionId,
  );
  const invocationOptions = limitOptions(
    [...invocations]
      .sort((left, right) => right.created_at_ms - left.created_at_ms)
      .map((value) => ({
        value: value.id,
        label: `${shortId(value.id)} · ${formatTime(value.created_at_ms)} · ${workflowRevisionLabel(value.workflow_version, value.definition_hash)} · ${value.state}`,
      })),
    invocationId,
  );
  return (
    <header className="scope-bar">
      <div className="brand-mark" aria-label="AutoAgent Trace">
        <Activity size={18} strokeWidth={2.2} />
        <span>AutoAgent</span>
        <strong>Trace</strong>
      </div>
      <div className="scope-fields">
        <ScopeSelect
          label="Workflow"
          value={workflowId}
          disabled={workflows.length === 0}
          onChange={onWorkflowChange}
          options={workflowOptions}
        />
        <ScopeSelect
          label="Session"
          value={sessionId}
          disabled={!workflowId || sessions.length === 0}
          onChange={onSessionChange}
          options={sessionOptions}
        />
        <ScopeSelect
          label="Invocation"
          value={invocationId}
          disabled={!sessionId || invocations.length === 0}
          onChange={onInvocationChange}
          options={invocationOptions}
        />
      </div>
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

function limitOptions(
  options: { value: string; label: string }[],
  selectedValue: string | null,
): { value: string; label: string }[] {
  const selected = selectedValue
    ? options.find((option) => option.value === selectedValue)
    : undefined;
  const limited = options.slice(0, MAX_SCOPE_OPTIONS);
  if (selected && !limited.some((option) => option.value === selected.value)) {
    return [selected, ...limited.slice(0, MAX_SCOPE_OPTIONS - 1)];
  }
  return limited;
}

function ScopeSelect({
  label,
  value,
  options,
  disabled,
  onChange,
}: {
  label: string;
  value: string | null;
  options: { value: string; label: string }[];
  disabled: boolean;
  onChange: (value: string | null) => void;
}) {
  const safeValue = value && options.some((option) => option.value === value)
    ? value
    : "";
  return (
    <label className="scope-select">
      <span>{label}</span>
      <select
        value={safeValue}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value || null)}
      >
        <option value="">Select</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
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
