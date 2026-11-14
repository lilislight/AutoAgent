import { request } from '../../http';
import type { ConfigRecord, ConnectionTest, Inventory, Policy, ProviderInventory, ResourceConfig } from './types';

const write = (method: string, data?: unknown): RequestInit => ({ method, headers: { 'Content-Type': 'application/json' }, ...(data === undefined ? {} : { body: JSON.stringify(data) }) });
export const settingsApi = {
  resources: (workspace: string | null, signal: AbortSignal) => request<Inventory>(`/agent/resource-configs${workspace ? '?' + new URLSearchParams({ workspace_id: workspace }) : ''}`, signal),
  saveResource: (configuration: ResourceConfig, record?: ConfigRecord) => request<ConfigRecord>(`/agent/resource-configs${record ? '/' + encodeURIComponent(record.id) : ''}`, new AbortController().signal, write(record ? 'PUT' : 'POST', { configuration, expected_version: record?.version ?? 0 })),
  deleteResource: (record: ConfigRecord) => request<unknown>(`/agent/resource-configs/${encodeURIComponent(record.id)}?expected_version=${record.version}`, new AbortController().signal, write('DELETE')),
  testMcp: (configuration: ResourceConfig, signal: AbortSignal) => request<ConnectionTest>('/agent/mcp/test', signal, write('POST', { configuration })),
  providers: (signal: AbortSignal) => request<ProviderInventory>('/providers', signal),
  reloadProviders: () => request<ProviderInventory>('/providers/reload', new AbortController().signal, write('POST')),
  skill: (id: string, workspace: string | null, signal: AbortSignal) => request<{ name: string; content: string }>(`/agent/skills/document?${new URLSearchParams({ management_id: id, ...(workspace ? { workspace_id: workspace } : {}) })}`, signal),
  parsePolicy: (text: string, format: 'json' | 'yaml', signal: AbortSignal) => request<{ policy: Policy; json: string; yaml: string }>('/agent/policies/parse', signal, write('POST', { text, format })),
  preview: (policy: Policy, toolName: string, args: unknown, operations: unknown, context: unknown, signal: AbortSignal) => request<PolicyPreview>('/agent/policies/preview', signal, write('POST', { policy, tool_name: toolName, arguments: args, operations, context })),
};
export interface PolicyPreview { decision: { action: string; reason: string | null }; operations: { default_used: boolean; matched_rules: { index: number; id: string | null; decision: string; reason: string | null }[] }[] }
