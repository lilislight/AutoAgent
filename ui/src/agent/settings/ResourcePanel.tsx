import { Fragment, useState } from 'react';
import { AgentIcon } from '../AgentIcon.js';
import { Tooltip } from '../../components/Tooltip.js';
import { DecisionSelect, Fold, Search, Toggle } from './Controls.js';
import { controlFor, defaultPolicy, withRemoteSelection, type ConfigRecord, type Decision, type Group, type Inventory, type Policy, type Resource, type ResourceConfig } from './types.js';

export interface ResourceActions {
  pending: string | null;
  save: (config: ResourceConfig, row?: ConfigRecord) => Promise<void>;
  run: (key: string, action: () => Promise<void>) => void;
  editPolicy: (name: string, policy: Policy, save: (policy: Policy) => Promise<void>, resource?: Resource) => void;
  saveSourcePolicy: (row: ConfigRecord, policy: Policy) => Promise<void>;
}
export function ResourcePanel({ inventory, actions, onlyGroup }: { inventory: Inventory; actions: ResourceActions; onlyGroup?: string }) {
  const [search, setSearch] = useState('');
  const groups = inventory.groups.map(group => ({ ...group, resources: group.resources.filter(resource => resource.kind === 'tool') })).filter(group => group.resources.length && (!onlyGroup || group.id === onlyGroup));
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
  const sourcePolicy = (group: Group) => inventory.items.find(row => row.id === group.resources[0]?.source_id);
  const groupPolicy = (group: Group): Policy => group.permissions ?? sourcePolicy(group)?.configuration.permissions ?? defaultPolicy();
  const saveGroupPolicy = (group: Group, policy: Policy) => {
    const source = sourcePolicy(group);
    return group.origin === 'mcp' && source ? actions.saveSourcePolicy(source, policy) : saveControl('group', group.id, group.name, { permissions: policy });
  };
  return <><Search label="Search tools" value={search} onChange={setSearch} />{groups.map(group => {
    const resources = group.resources.filter(resource => `${resource.name} ${resource.description}`.toLowerCase().includes(search.toLowerCase()));
    if (!resources.length) return null;
    const rows = resources.map(resource => <div className="settings-resource-row" key={resource.management_id}><span className="settings-resource-symbol"><AgentIcon name={resource.approval_exempt ? 'agent' : 'wrench'} /></span><div className="settings-row-copy"><strong>{resource.name}</strong>{resource.description ? <p>{resource.description}</p> : null}{!resource.available ? <small>Currently unavailable</small> : null}</div><div className="settings-row-actions">{resource.origin !== 'builtin' && !resource.approval_exempt ? <><Tooltip content={resource.permissions_editable ? 'Parameter rules and Tool input schema' : 'Permission editing is disabled by the Host'}><button type="button" className="settings-icon" aria-label={`Rules for ${resource.name}`} disabled={!!actions.pending || !resource.permissions_editable} onClick={() => actions.editPolicy(resource.name, resource.permissions ?? defaultPolicy(resource.default_decision), policy => saveControl('resource', resource.management_id, resource.name, { permissions: policy }), resource)}><AgentIcon name="settings" /></button></Tooltip><DecisionSelect label={`${resource.name} permission`} value={resource.default_decision} disabled={!!actions.pending || !resource.permissions_editable} onChange={(value: Decision) => actions.run(resource.management_id, () => saveControl('resource', resource.management_id, resource.name, { permissions: { ...(resource.permissions ?? defaultPolicy()), default: value } }))} /></> : null}<Toggle label={`Enable ${resource.name}`} checked={resource.enabled} disabled={!!actions.pending || !group.enabled || !resource.available} busy={actions.pending === resource.management_id} onChange={enabled => actions.run(resource.management_id, () => toggle(resource, enabled))} /></div></div>);
    return onlyGroup ? <Fragment key={group.id}>{rows}</Fragment> : <Fold key={group.id} title={group.name} count={resources.length} actions={<><span className="settings-origin">{group.origin === 'mcp' ? 'MCP' : group.origin === 'project' ? 'Project' : 'Built-in'}</span>{group.origin !== 'builtin' ? <><Tooltip content={group.permissions_editable ? 'Configure the final policy for this group' : 'Permission editing is disabled by the Host'}><button type="button" className="settings-icon" aria-label={`Rules for ${group.name}`} disabled={!!actions.pending || !group.permissions_editable} onClick={() => actions.editPolicy(group.name, groupPolicy(group), policy => saveGroupPolicy(group, policy))}><AgentIcon name="settings" /></button></Tooltip><DecisionSelect label={`${group.name} permission`} value={groupPolicy(group).default} disabled={!!actions.pending || !group.permissions_editable} onChange={value => actions.run(group.id, () => saveGroupPolicy(group, { ...groupPolicy(group), default: value }))} /></> : null}<Toggle label={`Enable ${group.name}`} checked={group.enabled} disabled={!!actions.pending} busy={actions.pending === group.id} onChange={enabled => actions.run(group.id, () => saveControl('group', group.id, group.name, { enabled }))} /></>}>
      {rows}
    </Fold>;
  })}{!groups.some(group => group.resources.some(resource => `${resource.name} ${resource.description}`.toLowerCase().includes(search.toLowerCase()))) ? <p className="settings-empty">{search ? 'No tools match your search.' : 'No tools are available in this workspace.'}</p> : null}</>;
}
