export type Decision = 'allow' | 'ask' | 'deny';
export type Section = 'providers' | 'tools' | 'mcp' | 'skills';
export type Operator = 'equals' | 'not_equals' | 'in' | 'not_in' | 'glob' | 'exists' | 'prefix' | 'suffix' | 'contains' | 'lt' | 'lte' | 'gt' | 'gte' | 'regex' | 'any_item' | 'all_items';
export type Condition = { field: string; op: Operator; value: unknown; flags?: string } | { all: Condition[] } | { any: Condition[] } | { not: Condition };
export interface Rule { id?: string | null; when: Condition; decision: Decision; reason?: string | null }
export interface Policy { default: Decision; rules: Rule[] }
export interface Credential { source: 'env'; name: string }
export interface ResourceConfig {
  kind: 'mcp' | 'skills' | 'resource' | 'group'; name: string; enabled: boolean; permissions?: Policy | null;
  resource_id?: string; group_id?: string; path?: string; interval_seconds?: number;
  transport?: { type: 'stdio' | 'sse' | 'streamable_http'; server_id: string; command?: string; args?: string[]; cwd?: string | null; url?: string; [key: string]: unknown };
  prefix?: string; include?: string[] | null; exclude?: string[]; environment?: Record<string, Credential>; headers?: Record<string, Credential>;
  [key: string]: unknown;
}
export interface ConfigRecord { id: string; version: number; configuration: ResourceConfig; state: string; error: string | null }
export interface Resource {
  management_id: string; group_id: string; group_name: string; resource_id: string; kind: 'tool' | 'skill'; name: string;
  origin: 'builtin' | 'project' | 'mcp' | 'manual' | 'workspace' | 'shared'; source_id?: string; workspace_id?: string; remote_name?: string;
  description: string; location?: string; input_schema?: Record<string, unknown>; operation_schema?: Record<string, unknown> | null;
  available: boolean; enabled: boolean; enabled_editable?: boolean; custom_policy?: Policy | null;
  registered_policies?: { source: 'code' | 'group' | 'source'; policy: Policy }[];
  permissions_editable: boolean; policy_in_code?: boolean;
  mcp_status?: 'ready' | 'offline' | 'removed' | 'connecting' | 'review';
  schema_version?: string; approved_schema?: Record<string, unknown> | null;
}
export interface Group { id: string; name: string; origin: Resource['origin']; resources: Resource[]; enabled: boolean; permissions: Policy | null; permissions_editable: boolean }
export interface Inventory { items: ConfigRecord[]; resources: Resource[]; groups: Group[]; workspace_id: string; revision: number; edit_code_permissions: boolean; policy_schema: Record<string, unknown> }
export interface ConfiguredModel {
  id: string; name: string; enabled: boolean; url: string | null;
  reasoning_effort: { values: string[]; default: string } | null;
  model_info: { max_input_tokens?: number; max_output_tokens?: number; input_cost_per_token: string; output_cost_per_token: string; cache_read_input_token_cost: string };
}
export interface ProviderRecord { id: string; enabled: boolean; backend: 'openai' | 'litellm'; protocol: string; base_url: string | null; models: ConfiguredModel[] }
export interface ProviderInventory { items: ProviderRecord[]; path: string; version: string; default_model?: { provider: string; model: string | null } | null; compaction_model?: { provider: string; model: string | null } | null }
export interface RemoteTool { name: string; description: string | null; input_schema: Record<string, unknown> }
export interface ConnectionTest { connected: boolean; tools: RemoteTool[]; error?: string }

export const defaultPolicy = (value: Decision = 'allow'): Policy => ({ default: value, rules: [] });
export function controlFor(data: Inventory, kind: 'resource' | 'group', id: string) {
  return data.items.find(row => row.configuration.kind === kind && row.configuration[kind === 'resource' ? 'resource_id' : 'group_id'] === id);
}
export function selectedRemote(config: ResourceConfig, name: string) {
  return (config.include == null || config.include.includes(name)) && !(config.exclude ?? []).includes(name);
}
export function withRemoteSelection(config: ResourceConfig, name: string, enabled: boolean): ResourceConfig {
  if (config.include != null) return { ...config, include: enabled ? [...new Set([...config.include, name])] : config.include.filter(value => value !== name), exclude: (config.exclude ?? []).filter(value => value !== name) };
  return { ...config, exclude: enabled ? (config.exclude ?? []).filter(value => value !== name) : [...new Set([...(config.exclude ?? []), name])] };
}
