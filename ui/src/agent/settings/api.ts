import { request } from '../../http';
import type { ConfigRecord, ConnectionTest, Inventory, Policy, ProviderConfig, ProviderRecord, ResourceConfig } from './types';

const write = (method: string, data?: unknown): RequestInit => ({ method, headers: { 'Content-Type': 'application/json' }, ...(data === undefined ? {} : { body: JSON.stringify(data) }) });
export const settingsApi = {
  resources: (workspace: string | null, signal: AbortSignal) => request<Inventory>(`/agent/resource-configs${workspace ? '?' + new URLSearchParams({ workspace_id: workspace }) : ''}`, signal),
  saveResource: (configuration: ResourceConfig, record?: ConfigRecord) => request<ConfigRecord>(`/agent/resource-configs${record ? '/' + encodeURIComponent(record.id) : ''}`, new AbortController().signal, write(record ? 'PUT' : 'POST', { configuration, expected_version: record?.version ?? 0 })),
  deleteResource: (record: ConfigRecord) => request<unknown>(`/agent/resource-configs/${encodeURIComponent(record.id)}?expected_version=${record.version}`, new AbortController().signal, write('DELETE')),
  testMcp: (configuration: ResourceConfig, signal: AbortSignal) => request<ConnectionTest>('/agent/mcp/test', signal, write('POST', { configuration })),
  providers: (signal: AbortSignal) => request<{ items: ProviderRecord[] }>('/agent/providers', signal),
  saveProvider: (configuration: ProviderConfig, record?: ProviderRecord) => request<ProviderRecord>(`/agent/providers${record ? '/' + encodeURIComponent(record.id) : ''}`, new AbortController().signal, write(record ? 'PUT' : 'POST', { configuration, expected_version: record?.version ?? 0 })),
  deleteProvider: (record: ProviderRecord) => request<unknown>(`/agent/providers/${encodeURIComponent(record.id)}?expected_version=${record.version}`, new AbortController().signal, write('DELETE')),
  skill: (id: string, workspace: string | null, signal: AbortSignal) => request<{ name: string; content: string }>(`/agent/skills/document?${new URLSearchParams({ management_id: id, ...(workspace ? { workspace_id: workspace } : {}) })}`, signal),
  preview: (policy: Policy, toolName: string, args: unknown, operations: unknown, signal: AbortSignal) => request<{ decision: { action: string; reason: string | null } }>('/agent/policies/preview', signal, write('POST', { policy, tool_name: toolName, arguments: args, operations })),
};
