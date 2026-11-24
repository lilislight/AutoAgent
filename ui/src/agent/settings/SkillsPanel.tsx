import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AgentIcon } from '../AgentIcon';
import { SkeletonText } from '../../components/LoadingState';
import { Markdown } from '../Markdown';
import { settingsApi } from './api';
import { Fold, FormActions, FormError, RowsSkeleton, Search, Toggle, errorText } from './Controls';
import { type ConfigRecord, type Inventory, type Resource, type ResourceConfig } from './types';
import type { ResourceActions } from './ResourcePanel';
import type { AgentWorkspaceInfo } from '../types';

export function DirectoryForm({ record, save, cancel }: { record?: ConfigRecord; save: (config: ResourceConfig, row?: ConfigRecord) => Promise<void>; cancel: () => void }) {
  const [name, setName] = useState(record?.configuration.name ?? ''), [path, setPath] = useState(record?.configuration.path ?? '');
  const [busy, setBusy] = useState(false), [error, setError] = useState<string | null>(null);
  return <form className="settings-form" onSubmit={async event => { event.preventDefault(); if (busy) return; setBusy(true); setError(null); try { await save({ kind: 'skills', name: name.trim(), path: path.trim(), enabled: true, ...record?.configuration, ...(record ? { name: name.trim(), path: path.trim() } : {}) }, record); } catch (error) { setError(errorText(error)); } finally { setBusy(false); } }}><fieldset disabled={busy}><label>Directory name<input required value={name} onChange={event => setName(event.target.value)} placeholder="My skills" /></label><label>Directory path<input required value={path} onChange={event => setPath(event.target.value)} placeholder="/absolute/path/to/skills" /></label></fieldset><FormError message={error} /><FormActions busy={busy} onCancel={cancel} label={record ? 'Save directory' : 'Add directory'} /></form>;
}
export function SkillDocument({ resource, workspace }: { resource: Resource; workspace: string | null }) {
  const query = useQuery({ queryKey: ['agent-skill-document', resource.management_id, workspace], queryFn: ({ signal }) => settingsApi.skill(resource.management_id, workspace, signal) });
  return <section className="settings-skill-document">{resource.location ? <p className="settings-path">{resource.location}</p> : null}{query.isPending ? <div className="settings-document-skeleton" role="status" aria-label="Loading Skill instructions"><h1><SkeletonText width="55%" /></h1>{Array.from({ length: 6 }, (_, index) => <p key={index}><SkeletonText width={index % 3 === 2 ? "68%" : "100%"} /></p>)}</div> : query.error ? <p role="alert" className="settings-error">{query.error.message}<button onClick={() => void query.refetch()}>Retry</button></p> : <Markdown text={query.data.content} />}</section>;
}
export function SkillsPanel({ inventory, workspace, workspaces, onWorkspace, actions, onDocument, onEditDirectory, onDelete }: { inventory?: Inventory; workspace: string | null; workspaces: AgentWorkspaceInfo[]; onWorkspace: (id: string) => void; actions: ResourceActions; onDocument: (resource: Resource) => void; onEditDirectory: (row: ConfigRecord) => void; onDelete: (row: ConfigRecord) => void }) {
  const [search, setSearch] = useState('');
  const skills = inventory?.resources.filter(resource => resource.kind === 'skill') ?? [];
  const row = (resource: Resource) => <div className="settings-resource-row" key={resource.management_id}><span className="settings-resource-symbol"><AgentIcon name="skills" /></span><div className="settings-row-copy"><strong>{resource.name}</strong><p>{resource.description}</p><small className="settings-path">{resource.location}</small></div><div className="settings-row-actions"><button type="button" onClick={() => onDocument(resource)}>View SKILL.md</button><Toggle label={`Enable skill ${resource.name}`} checked={resource.enabled} disabled={!!actions.pending || !resource.available || resource.enabled_editable === false} busy={actions.pending === resource.management_id} onChange={enabled => actions.run(resource.management_id, async () => { await actions.save({ kind: 'resource', name: resource.name, resource_id: resource.management_id, permissions: null, enabled }); })} /></div></div>;
  const selected = (values: Resource[]) => values.filter(skill => `${skill.name} ${skill.description}`.toLowerCase().includes(search.toLowerCase()));
  const workspaceSkills = selected(skills.filter(skill => skill.origin === 'workspace'));
  const builtinSkills = selected(skills.filter(skill => skill.origin === 'builtin'));
  const projectSkills = selected(skills.filter(skill => skill.origin === 'project'));
  const diagnostics = inventory?.skill_diagnostics ?? [];
  return <><Search label="Search skills" value={search} onChange={setSearch} /><Fold title="Built-in" count={inventory ? builtinSkills.length : undefined}>{inventory ? builtinSkills.map(row) : <RowsSkeleton metadata />}{inventory && !builtinSkills.length ? <p className="settings-empty">No built-in skills.</p> : null}</Fold>
    {skills.some(skill => skill.origin === 'project') ? <Fold title="Project" count={projectSkills.length}>{projectSkills.map(row)}</Fold> : null}
    <Fold title="Workspace" count={inventory ? workspaceSkills.length : undefined} actions={
      <select className="settings-workspace-select" aria-label="Workspace to manage skills"
        value={workspace ?? inventory?.workspace_id ?? ''} onChange={event => onWorkspace(event.target.value)}>
        {!workspaces.length && <option value="">No workspaces available</option>}
        {workspaces.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}
      </select>
    }>
      {inventory ? workspaceSkills.map(row) : <RowsSkeleton metadata />}
      {diagnostics.map(diagnostic => <div className="settings-resource-row settings-skill-diagnostic" key={diagnostic.location}>
        <span className="settings-resource-symbol"><AgentIcon name="skills" /></span>
        <div className="settings-row-copy">
          <strong className="settings-path">{diagnostic.location}</strong>
          <FormError message={diagnostic.message} />
        </div>
      </div>)}
      {inventory && !workspaceSkills.length && !diagnostics.length && <p className="settings-empty">No skills discovered in this workspace.</p>}
    </Fold>
    {skills.some(skill => skill.origin === 'shared') ? <Fold title="Shared directory" count={inventory ? selected(skills.filter(skill => skill.origin === 'shared')).length : undefined}>{inventory ? selected(skills.filter(skill => skill.origin === 'shared')).map(row) : <RowsSkeleton metadata />}</Fold> : null}
    <Fold title="Added directories" count={inventory?.items.filter(item => item.configuration.kind === 'skills').length}>{inventory?.items.filter(item => item.configuration.kind === 'skills').map(item => <section className="settings-source" key={item.id}><div className="settings-resource-row"><span className="settings-resource-symbol"><AgentIcon name="folder" /></span><div className="settings-row-copy"><strong>{item.configuration.name}</strong><p className="settings-path">{item.configuration.path}</p><small>{item.state}{item.error ? ` · ${item.error}` : ''}</small></div><div className="settings-row-actions"><button type="button" disabled={!!actions.pending} onClick={() => onEditDirectory(item)}>Edit</button><button type="button" disabled={!!actions.pending} onClick={() => onDelete(item)}>Remove</button><Toggle label={`Enable directory ${item.configuration.name}`} checked={item.configuration.enabled} disabled={!!actions.pending} busy={actions.pending === item.id} onChange={enabled => actions.run(item.id, () => actions.save({ ...item.configuration, enabled }, item))} /></div></div>{selected(skills.filter(skill => skill.source_id === item.id)).map(row)}</section>)}{inventory && !inventory.items.some(item => item.configuration.kind === 'skills') ? <p className="settings-empty">No added directories.</p> : null}</Fold>
  </>;
}
