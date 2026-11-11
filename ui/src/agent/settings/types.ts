export type Decision = 'allow' | 'ask' | 'deny';
export type Section = 'providers' | 'tools' | 'mcp' | 'skills';
export type Operator = 'equals' | 'not_equals' | 'in' | 'not_in' | 'glob' | 'exists' | 'prefix' | 'contains' | 'lt' | 'lte' | 'gt' | 'gte' | 'regex';
export interface Condition { source: 'arguments' | 'operation' | 'call'; path: string; operator: Operator; value: unknown }
export interface Rule { conditions: Condition[]; match: 'all' | 'any'; decision: Decision; reason?: string | null }
export interface Policy { default: Decision; rules: Rule[] }
export interface Credential { source: 'env' | 'file'; name: string }
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
  origin: 'builtin' | 'project' | 'mcp' | 'manual' | 'workspace'; source_id?: string; workspace_id?: string; remote_name?: string;
  description: string; location?: string; input_schema?: Record<string, unknown>;
  available: boolean; enabled: boolean; permissions: Policy | null; default_decision: Decision; parameter_policy: boolean;
  permission_source: string; permissions_editable: boolean; approval_exempt?: boolean;
}
export interface Group { id: string; name: string; origin: Resource['origin']; resources: Resource[]; enabled: boolean; permissions: Policy | null; permissions_editable: boolean }
export interface Inventory { items: ConfigRecord[]; resources: Resource[]; groups: Group[]; workspace_id: string; revision: number; edit_code_permissions: boolean; policy_schema: Record<string, unknown> }
export interface ProviderConfig {
  name: string; backend: 'openai' | 'litellm'; provider_id: string; protocol: 'openai.chat_completions' | 'openai.responses';
  base_url: string | null; model_prefix: string | null; api_key: Credential | null; headers: Record<string, Credential>;
  timeout_seconds: number; models: string[]; enabled: boolean;
}
export interface ProviderRecord { id: string; alias: string; version: number | string; configuration: Partial<ProviderConfig> & { name: string; enabled: boolean }; editable: boolean }
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
