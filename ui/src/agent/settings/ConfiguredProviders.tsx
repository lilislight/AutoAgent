import { Fold, RowsSkeleton } from './Controls';
import type { ProviderInventory } from './types';

export function ConfiguredProviders({ inventory, loading }: { inventory?: ProviderInventory; loading: boolean }) {
  return <>
    <p className="settings-help settings-path">{inventory?.path ?? 'Project providers.json'}</p>
    {inventory ? <p className="settings-help">Default model: {inventory.default_model ? `${inventory.default_model.provider} / ${inventory.default_model.model}` : 'Not specified'}<br />Compaction model: {inventory.compaction_model ? `${inventory.compaction_model.provider} / ${inventory.compaction_model.model}` : 'Use conversation model'}</p> : null}
    {loading ? <RowsSkeleton metadata /> : inventory?.items.map(provider => <Fold key={provider.id} title={provider.id} count={provider.models.length} actions={<span className="settings-origin">{provider.enabled ? provider.backend === 'litellm' ? 'LiteLLM' : 'OpenAI compatible' : 'Disabled'}</span>}><p className="settings-help">{provider.protocol}{provider.base_url ? ` · ${provider.base_url}` : ''}</p>{provider.models.map(model => <div className="settings-resource-row" key={model.id}><div className="settings-row-copy"><strong className="agent-model-name">{model.name}{!model.enabled ? ' · Disabled' : ''}</strong>{model.name !== model.id ? <p>{model.id}</p> : null}<small>Input {model.model_info.max_input_tokens?.toLocaleString() ?? 'Unspecified'} · Output {model.model_info.max_output_tokens?.toLocaleString() ?? 'Unspecified'}</small><small>USD / token · Input {model.model_info.input_cost_per_token} · Output {model.model_info.output_cost_per_token} · Cached input {model.model_info.cache_read_input_token_cost}</small>{model.reasoning_effort ? <small>Reasoning · {model.reasoning_effort.values.join(' / ')} · Default {model.reasoning_effort.default}</small> : null}</div></div>)}</Fold>)}
    {inventory && !inventory.items.length ? <p className="settings-empty">No providers configured.</p> : null}</>;
}
