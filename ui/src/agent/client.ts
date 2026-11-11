import { newId } from './ids';
import { config, request } from '../http';
import type { ActionInput, ActionSpec, AgentCatalog, AgentEvent, AgentExecution, AgentInvocation, AgentResources, AgentSession, AgentWorkspaceInfo, Controls, Operation, Page, SessionSettings, SettingsRecord, Usage } from './types';

// All commands use Host operations. This module has no Graph/Trace dependencies.
export const agentClient = {
  queue: (sid: string, signal: AbortSignal) => request<import('./types').MessageQueue>(`/agent/queue?${new URLSearchParams({ session_id: sid })}`, signal),
  enqueue: (sid: string, message: string, requestId: string, workspace: string | null, signal: AbortSignal, sendTo?: string, initialSettings?: SessionSettings) => request<import('./types').QueuedMessage>('/agent/queue', signal, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ session_id: sid, message, request_id: requestId, workspace_id: workspace, ...(sendTo ? { send_to: sendTo } : {}), ...(initialSettings ? { initial_settings: initialSettings } : {}) }) }),
  queueAction: (sid: string, itemId: string, action: 'cancel' | 'send_now' | 'continue', signal: AbortSignal, invocationId?: string) => request<import('./types').QueuedMessage>('/agent/queue/actions', signal, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ session_id: sid, item_id: itemId, action, ...(invocationId ? { invocation_id: invocationId } : {}) }) }),
  catalog: (signal: AbortSignal) => request<AgentCatalog>('/agent/catalog', signal),
  workspaces: (signal: AbortSignal) => request<{ items: AgentWorkspaceInfo[] }>('/agent/workspaces', signal),
  createWorkspace: (value: { name: string; path: string }, signal: AbortSignal) => request<AgentWorkspaceInfo>('/agent/workspaces', signal, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(value) }),
  sessions: (workspace: string | null, cursor: string | null, signal: AbortSignal) => request<Page<AgentSession>>(`/agent/sessions?${new URLSearchParams({ ...(workspace ? { workspace_id: workspace } : {}), ...(cursor ? { cursor } : {}) })}`, signal),
  invocations: (session: string, cursor: string | null, signal: AbortSignal) => request<Page<AgentInvocation>>(`/agent/invocations?${new URLSearchParams({ session_id: session, ...(cursor ? { cursor } : {}) })}`, signal),
  controls: (iid: string, cursor: string | null, signal: AbortSignal) => request<Controls>(`/execution-controls?${new URLSearchParams({ invocation_id: iid, ...(cursor ? { cursor } : {}) })}`, signal),
  execution: (iid: string, signal: AbortSignal) => request<AgentExecution>(`/agent/execution?${new URLSearchParams({ invocation_id: iid })}`, signal),
  settings: (sid: string, signal: AbortSignal, workspace?: string | null) => request<SettingsRecord>(`/agent/settings?${new URLSearchParams({ session_id: sid, ...(workspace ? { workspace_id: workspace } : workspace === null ? { unbound: 'true' } : {}) })}`, signal),
  configure: (sid: string, settings: SessionSettings, version: number, signal: AbortSignal) => request<SettingsRecord>('/agent/settings', signal, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ session_id: sid, settings, expected_version: version }) }),
  resources: (revision: string, session: string, signal: AbortSignal, workspace?: string | null) => request<AgentResources>(`/agent/resources?${new URLSearchParams({ revision_id: revision, session_id: session, ...(workspace ? { workspace_id: workspace } : workspace === null ? { unbound: 'true' } : {}) })}`, signal),
  usage: (sid: string, iid: string | null, signal: AbortSignal) => request<{ session: Usage; invocation: Usage | null }>(`/agent/usage?${new URLSearchParams({ session_id: sid, ...(iid ? { invocation_id: iid } : {}) })}`, signal),
  async history(iid: string, signal: AbortSignal): Promise<AgentEvent[]> {
    const items: AgentEvent[] = [];
    let after = 0;
    while (true) {
      const page = await request<{ items: AgentEvent[]; next_after: number | null }>(`/agent/history?${new URLSearchParams({ invocation_id: iid, after: String(after), limit: '200' })}`, signal);
      items.push(...page.items);
      if (page.next_after === null) return items;
      if (page.next_after <= after) throw new Error('User Event history cursor did not advance.');
      after = page.next_after;
    }
  },
  watch(sid: string) { return new EventSource(`${config.apiPrefix}/agent/events/watch?${new URLSearchParams({ session_id: sid })}`); },
  run: (sid: string, message: string, signal: AbortSignal, workspace: string | null) => request<{ invocation_id: string; session_id: string }>('/agent/messages', signal, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ session_id: sid, message, workspace_id: workspace }) }),
  spec: (iid: string, action: string, options: Record<string, string>, signal: AbortSignal) => request<ActionSpec>(`/execution-action-spec?${new URLSearchParams({ invocation_id: iid, action, ...options })}`, signal),
  action: (input: ActionInput, signal: AbortSignal) => request<Operation>('/execution-actions', signal, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(input) }),
  operation: (id: string, signal: AbortSignal) => request<Operation>(`/execution-operations/${encodeURIComponent(id)}`, signal),
};

export async function performAction(input: Omit<ActionInput, 'request_id'>, signal: AbortSignal): Promise<Operation> {
  const receipt = await agentClient.action({ ...input, request_id: newId().replaceAll('-', '') }, signal);
  let current = receipt;
  for (let attempt = 0; current.status === 'running'; attempt++) {
    if (attempt >= 120) throw new Error('Operation is still running. Refresh the current state before trying again.');
    await new Promise<void>((resolve, reject) => {
      if (signal.aborted) { reject(signal.reason); return; }
      const finish = () => { signal.removeEventListener('abort', abort); resolve(); };
      const timer = setTimeout(finish, 250);
      const abort = () => { clearTimeout(timer); reject(signal.reason); };
      signal.addEventListener('abort', abort, { once: true });
    });
    current = await agentClient.operation(receipt.operation_id, signal);
  }
  if (current.status !== 'succeeded') throw Object.assign(new Error(current.error?.message ?? 'The operation result is unknown. Refresh before trying again.'), { code: current.error?.code });
  return current;
}
