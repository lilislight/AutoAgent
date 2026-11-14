import type { AgentSession } from './types.js';

export interface SessionActivity {
  invocation: string;
  sequence: number;
  status: string;
  control: string;
  unread: boolean;
}
export type SessionActivities = Record<string, SessionActivity>;
const terminal = new Set(['completed', 'failed', 'cancelled']);

export function sessionRunning(value: Pick<AgentSession, 'status' | 'control_state'>) {
  return ['created', 'running', 'waiting', 'settling'].includes(value.status ?? '') && value.control_state !== 'paused';
}

/** Merge Workspace/Recent snapshots monotonically; an old refresh cannot resurrect a finished run. */
export function observeSessions(previous: SessionActivities, rows: AgentSession[], viewed: string | null): SessionActivities {
  let result = previous;
  for (const row of rows) {
    if (!row.status) continue;
    const old = Object.hasOwn(result, row.session_id) ? result[row.session_id] : undefined, sequence = row.last_sequence ?? 0;
    if (old && (old.sequence > sequence || old.invocation === row.invocation_id && terminal.has(old.status) && !terminal.has(row.status))) continue;
    const finished = terminal.has(row.status);
    // Establish a quiet baseline for old chats, but notice new runs even when a
    // short task starts and ends between two polls.
    const unread = row.session_id !== viewed && finished && !!old && (
      old.unread || old.invocation !== row.invocation_id || !terminal.has(old.status));
    const next: SessionActivity = { invocation: row.invocation_id, sequence, status: row.status, control: row.control_state ?? 'active', unread };
    if (old && Object.keys(next).every(key => old[key as keyof SessionActivity] === next[key as keyof SessionActivity])) continue;
    if (result === previous) result = Object.assign(Object.create(null), previous);
    result[row.session_id] = next;
  }
  return result;
}

export function readSession(previous: SessionActivities, session: string): SessionActivities {
  return previous[session]?.unread ? { ...previous, [session]: { ...previous[session], unread: false } } : previous;
}

export function loadActivity(key: string): SessionActivities {
  try {
    const value = JSON.parse(localStorage.getItem(key) ?? 'null');
    if (value?.v !== 1 || !value.sessions || typeof value.sessions !== 'object' || Array.isArray(value.sessions)) return {};
    return Object.fromEntries(Object.entries(value.sessions).filter(([, record]) => {
      const item = record as SessionActivity | null;
      return item && typeof item.invocation === 'string' && Number.isSafeInteger(item.sequence) && item.sequence >= 0
        && typeof item.status === 'string' && typeof item.control === 'string' && typeof item.unread === 'boolean';
    })) as SessionActivities;
  } catch { return {}; }
}
