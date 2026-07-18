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
  SessionSummary,
  WorkflowSummary,
} from "../types";

interface ScopeBarProps {
  workflows: WorkflowSummary[];
  sessions: SessionSummary[];
  invocations: InvocationSummary[];
  workflowId: string | null;
  sessionId: string | null;
  invocationId: string | null;
  followLive: boolean;
  connected: boolean;
  darkMode: boolean;
  executionEnabled: boolean;
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
  followLive,
  connected,
  darkMode,
  executionEnabled,
  invoking,
  onWorkflowChange,
  onSessionChange,
  onInvocationChange,
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
      <div className="scope-fields">
        <ScopeSelect
          label="Workflow"
          value={workflowId}
          disabled={workflows.length === 0}
          onChange={onWorkflowChange}
          options={workflows.map((value) => ({
            value: value.workflow_id,
            label: value.name || value.workflow_id,
          }))}
        />
        <ScopeSelect
          label="Session"
          value={sessionId}
          disabled={!workflowId || sessions.length === 0}
          onChange={onSessionChange}
          options={sessions.map((value) => ({
            value: value.id,
            label: value.session_key || shortId(value.id),
          }))}
        />
        <ScopeSelect
          label="Invocation"
          value={invocationId}
          disabled={!sessionId || invocations.length === 0}
          onChange={onInvocationChange}
          options={[...invocations].reverse().map((value) => ({
            value: value.id,
            label: `${formatTime(value.created_at_ms)} · ${value.state}`,
          }))}
        />
      </div>
      <div className="scope-actions">
        <span className={`connection-state ${connected ? "is-connected" : ""}`}>
          <Radio size={13} />
          {connected ? "Live link" : "Offline"}
        </span>
        <button
          className="toolbar-button"
          type="button"
          onClick={onOpenInvoke}
          disabled={!workflowId || !executionEnabled || invoking}
          title={executionEnabled ? "Invoke selected workflow" : "Execution API is disabled"}
        >
          <Play size={15} />
          {invoking ? "Invoking" : "Invoke"}
        </button>
        <button
          className={`toolbar-button ${followLive ? "is-active" : ""}`}
          type="button"
          onClick={onFollowLive}
          disabled={!selectedInvocation}
          title="Follow latest runtime event"
        >
          {followLive ? <Radio size={15} /> : <CirclePause size={15} />}
          {followLive ? "Following" : "Replay"}
        </button>
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
  return (
    <label className="scope-select">
      <span>{label}</span>
      <select
        value={value ?? ""}
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

function formatTime(value: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(value);
}
