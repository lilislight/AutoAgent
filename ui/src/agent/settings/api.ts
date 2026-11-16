import { request } from '../../http';
import type { ConfigRecord, ConnectionTest, Decision, Inventory, Policy, ProviderInventory, ResourceConfig } from './types';

const write = (method: string, data?: unknown): RequestInit => ({ method, headers: { 'Content-Type': 'application/json' }, ...(data === undefined ? {} : { body: JSON.stringify(data) }) });
export const settingsApi = {
  resources: (workspace: string | null, signal: AbortSignal, scope?: 'tools' | 'mcp') => request<Inventory>(
    '/agent/resource-configs?' + new URLSearchParams({ ...(workspace ? { workspace_id: workspace } : {}), ...(scope ? { scope } : {}) }), signal),
  saveResource: (configuration: ResourceConfig, record?: ConfigRecord) => request<ConfigRecord>(`/agent/resource-configs${record ? '/' + encodeURIComponent(record.id) : ''}`, new AbortController().signal, write(record ? 'PUT' : 'POST', { configuration, expected_version: record?.version ?? 0 })),
  deleteResource: (record: ConfigRecord) => request<unknown>(`/agent/resource-configs/${encodeURIComponent(record.id)}?expected_version=${record.version}`, new AbortController().signal, write('DELETE')),
  testMcp: (configuration: ResourceConfig, signal: AbortSignal) => request<ConnectionTest>('/agent/mcp/test', signal, write('POST', { configuration })),
  acceptMcpSchema: (connectionId: string, remoteName: string, version: string) => request<void>(
    '/agent/mcp/' + encodeURIComponent(connectionId) + '/accept-schema', new AbortController().signal,
    write('POST', { remote_name: remoteName, schema_version: version })),
  providers: (signal: AbortSignal) => request<ProviderInventory>('/providers', signal),
  reloadProviders: () => request<ProviderInventory>('/providers/reload', new AbortController().signal, write('POST')),
  skill: (id: string, workspace: string | null, signal: AbortSignal) => request<{ name: string; content: string }>(`/agent/skills/document?${new URLSearchParams({ management_id: id, ...(workspace ? { workspace_id: workspace } : {}) })}`, signal),
  parsePolicy: (text: string, format: 'json' | 'yaml', signal: AbortSignal) => request<{ policy: Policy; json: string; yaml: string }>('/agent/policies/parse', signal, write('POST', { text, format, editor: true })),
  preview: (policy: Policy, toolId: string, workspace: string, args: unknown, context: unknown, signal: AbortSignal) => request<PolicyPreview>('/agent/policies/preview', signal, write('POST', { policy, tool_id: toolId, workspace_id: workspace, arguments: args, context })),
};
interface PreviewDecision { action: Decision; reason: string | null }
interface PreviewLayer { decision: Decision; source: 'code' | 'group' | 'source' | 'custom'; layer: number }
export interface PolicyPreview {
  decision: PreviewDecision;
  includes_code_callbacks: boolean;
  operations: {
    operation: { action: string; resource: string | null } | null;
    decision: PreviewDecision;
    default_used: boolean;
    matched_rules: (PreviewLayer & { index: number })[];
    defaults: PreviewLayer[];
  }[];
}
