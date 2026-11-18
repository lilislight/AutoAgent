import { Fragment, useState } from 'react';
import { AgentIcon } from '../AgentIcon.js';
import { Tooltip } from '../../components/Tooltip.js';
import { Fold, Search, Toggle } from './Controls.js';
import { controlFor, withRemoteSelection, type ConfigRecord, type Inventory, type Policy, type Resource, type ResourceConfig } from './types.js';

export interface ResourceActions {
  pending: string | null;
  save: (config: ResourceConfig, row?: ConfigRecord) => Promise<void>;
  run: (key: string, action: () => Promise<void>) => void;
  editPolicy: (resource: Resource, save: (policy: Policy) => Promise<void>) => void;
}
export function ResourcePanel({ inventory, actions, onlyGroup }: { inventory: Inventory; actions: ResourceActions; onlyGroup?: string }) {
  const [search, setSearch] = useState('');
  const groups = inventory.groups.map(group => ({ ...group, resources: group.resources.filter(resource => resource.kind === 'tool') }))
    .filter(group => group.resources.length && (onlyGroup ? group.id === onlyGroup : group.origin !== 'mcp'));
  const saveControl = async (kind: 'resource' | 'group', id: string, name: string, patch: Partial<ResourceConfig>) => {
    const existing = controlFor(inventory, kind, id);
    await actions.save({ kind, name, enabled: true, permissions: null, ...(kind === 'resource' ? { resource_id: id } : { group_id: id }), ...existing?.configuration, ...patch }, existing);
  };
  const toggle = async (resource: Resource, enabled: boolean) => {
    if (resource.origin === 'mcp' && resource.source_id) {
      const source = inventory.items.find(row => row.id === resource.source_id);
      if (source) await actions.save(withRemoteSelection(source.configuration, resource.remote_name ?? resource.name, enabled), source);
      const existing = controlFor(inventory, 'resource', resource.management_id);
      if (!existing || existing.configuration.enabled) return;
    }
    await saveControl('resource', resource.management_id, resource.name, { enabled });
  };
  return <><Search label="Search tools" value={search} onChange={setSearch} />{groups.map(group => {
    const resources = group.resources.filter(resource => `${resource.name} ${resource.description}`.toLowerCase().includes(search.toLowerCase()));
    if (!resources.length) return null;
    const rows = resources.map(resource => {
      const savePolicy = (policy: Policy) => saveControl('resource', resource.management_id, resource.name,
        { permissions: policy.rules.length || policy.default !== 'allow' ? policy : null });
      const pending = !!actions.pending;
      const native = ['builtin/collaboration', 'builtin/interaction'].includes(resource.group_id);
      const status = resource.mcp_status === 'review' ? 'Schema review required'
        : resource.mcp_status === 'conflict' ? 'Tool name conflict'
          : resource.mcp_status === 'offline' ? 'Offline' : resource.mcp_status === 'removed' ? 'Removed from server'
          : resource.mcp_status === 'connecting' ? 'Discovering' : 'Currently unavailable';
      return <div className="settings-resource-row" key={resource.management_id}>
        <span className="settings-resource-symbol"><AgentIcon name={native ? 'agent' : 'wrench'} /></span>
        <div className="settings-row-copy"><strong>{resource.name}</strong>
          {resource.description && <p>{resource.description}</p>}
          {!resource.available && <small className={resource.mcp_status === 'review' ? 'settings-review' : undefined}>{status}</small>}
        </div>
        <div className="settings-row-actions">
          {!native && <Tooltip content={resource.mcp_status === 'review' ? 'Review the tool schema' : 'View permission rules'}>
            <button type="button" className="settings-icon" aria-label={`Rules for ${resource.name}`}
              disabled={pending} onClick={() => actions.editPolicy(resource, savePolicy)}><AgentIcon name="settings" /></button>
          </Tooltip>}
          <Toggle label={`Enable ${resource.name}`} checked={resource.enabled} disabled={pending || !group.enabled || !resource.available}
            busy={actions.pending === resource.management_id} onChange={enabled => actions.run(resource.management_id, () => toggle(resource, enabled))} />
        </div>
      </div>;
    });
    return onlyGroup ? <Fragment key={group.id}>{rows}</Fragment> : <Fold key={group.id} title={group.name} count={resources.length} actions={<>
      <span className="settings-origin">{group.origin === 'mcp' ? 'MCP' : group.origin === 'project' ? 'Project' : 'Built-in'}</span>
      <Toggle label={`Enable ${group.name}`} checked={group.enabled} disabled={!!actions.pending} busy={actions.pending === group.id}
        onChange={enabled => actions.run(group.id, () => saveControl('group', group.id, group.name, { enabled }))} />
    </>}>{rows}</Fold>;
  })}{!groups.some(group => group.resources.some(resource => `${resource.name} ${resource.description}`.toLowerCase().includes(search.toLowerCase()))) ? <p className="settings-empty">{search ? 'No tools match your search.' : 'No tools are available in this workspace.'}</p> : null}</>;
}
