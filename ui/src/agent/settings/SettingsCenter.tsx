import { useId, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { AgentIcon, type AgentIconName } from '../AgentIcon';
import { BusyLabel } from '../../components/LoadingState';
import { agentClient } from '../client';
import { settingsApi } from './api';
import { DecisionSelect, Fold, FormError, RowsSkeleton, Toggle, errorText } from './Controls';
import { ConfiguredProviders } from './ConfiguredProviders';
import { ConnectionForm } from './ConnectionForm';
import { RuleEditor } from './RuleEditor';
import { ResourcePanel, type ResourceActions } from './ResourcePanel';
import { DirectoryForm, SkillDocument, SkillsPanel } from './SkillsPanel';
import { controlFor, defaultPolicy, type ConfigRecord, type Policy, type Resource, type ResourceConfig, type Section } from './types';
import type { AgentWorkspaceInfo } from '../types';
import './settings.css';

const sections: { id: Section; title: string; icon: AgentIconName }[] = [
  { id: 'providers', title: 'Providers', icon: 'activity' },
  { id: 'tools', title: 'Tools', icon: 'tools' },
  { id: 'mcp', title: 'MCP servers', icon: 'terminal' },
  { id: 'skills', title: 'Skills', icon: 'skills' },
];
type Editor = { kind: 'mcp'; record?: ConfigRecord } | { kind: 'directory'; record?: ConfigRecord }
  | { kind: 'policy'; name: string; policy: Policy; resource?: Resource; save: (policy: Policy) => Promise<void> }
  | { kind: 'document'; resource: Resource };
type Removal = { kind: 'resource'; record: ConfigRecord };

export function SettingsCenter({ initial = 'providers', currentWorkspace, workspaces, onClose }: { initial?: Section; currentWorkspace: string | null; workspaces: AgentWorkspaceInfo[]; onClose: () => void }) {
  const [section, setSection] = useState(initial), [workspace, setWorkspace] = useState(currentWorkspace);
  const [editor, setEditor] = useState<Editor | null>(null), [removal, setRemoval] = useState<Removal | null>(null);
  const [pending, setPending] = useState<string | null>(null), [error, setError] = useState<string | null>(null), [notice, setNotice] = useState('');
  const dialog = useRef<HTMLDialogElement>(null), page = useRef<HTMLDivElement>(null), id = useId(), client = useQueryClient();
  const inventory = useQuery({ queryKey: ['agent-app-resources', workspace], queryFn: ({ signal }) => settingsApi.resources(workspace, signal), refetchInterval: 5000, retry: false });
  const providers = useQuery({ queryKey: ['agent-app-providers'], queryFn: ({ signal }) => settingsApi.providers(signal), refetchInterval: 10000, retry: false });
  useLayoutEffect(() => { const element = dialog.current!; const previous = document.activeElement as HTMLElement | null; element.showModal(); return () => { element.close(); if (previous?.isConnected && previous.getClientRects().length && getComputedStyle(previous).visibility !== 'hidden' && !previous.closest('[inert]')) previous.focus({ preventScroll: true }); else document.querySelector<HTMLTextAreaElement>('.agent-composer textarea')?.focus({ preventScroll: true }); }; }, []);
  useLayoutEffect(() => { page.current?.scrollTo({ top: 0 }); if (editor) page.current?.querySelector<HTMLElement>('form input, form select, form textarea, button')?.focus(); }, [editor?.kind, section]);
  const refresh = async () => { await Promise.all([client.invalidateQueries({ queryKey: ['agent-app-resources'] }), client.invalidateQueries({ queryKey: ['agent-app-providers'] }), client.invalidateQueries({ queryKey: ['agent-catalog'] }), client.invalidateQueries({ queryKey: ['agent-settings'] }), client.invalidateQueries({ queryKey: ['agent-resources'] })]); };
  const commit = async (key: string, action: () => Promise<unknown>) => { if (pending) return; setPending(key); setError(null); setNotice(''); try { await action(); await refresh(); setNotice(key === 'reload-providers' ? 'Provider configuration reloaded.' : 'Changes saved.'); } catch (error) { void refresh(); throw error; } finally { setPending(null); } };
  const run: ResourceActions['run'] = (key, action) => { void commit(key, action).catch(error => setError(errorText(error))); };
  const save = async (config: ResourceConfig, record?: ConfigRecord) => { await settingsApi.saveResource(config, record); };
  const saveForm = async (config: ResourceConfig, record?: ConfigRecord) => { await commit(record?.id ?? 'new-resource', () => save(config, record)); setEditor(null); };
  const sourcePolicy = (row: ConfigRecord) => inventory.data?.groups.find(group => group.id === 'mcp/' + row.id)?.permissions ?? row.configuration.permissions ?? defaultPolicy();
  const saveSourcePolicy = async (row: ConfigRecord, policy: Policy) => {
    const group = inventory.data ? controlFor(inventory.data, 'group', 'mcp/' + row.id) : undefined;
    if (group?.configuration.permissions) await save({ ...group.configuration, permissions: null }, group);
    await save({ ...row.configuration, permissions: policy }, row);
  };
  const actions: ResourceActions = { pending, save, run, saveSourcePolicy, editPolicy: (name, policy, save, resource) => { setEditor({ kind: 'policy', name, policy, save, resource }); setError(null); } };
  const chosen = sections.find(item => item.id === section)!;
  const configs = inventory.data?.items ?? [], connections = configs.filter(row => row.configuration.kind === 'mcp');
  const heading = editor ? editor.kind === 'mcp' ? `${editor.record ? 'Edit' : 'Add'} MCP server` : editor.kind === 'directory' ? `${editor.record ? 'Edit' : 'Add'} skill directory` : editor.kind === 'document' ? editor.resource.name : `Permissions · ${editor.name}` : chosen.title;
  const open = (kind: 'mcp' | 'directory') => { setEditor({ kind } as Editor); setError(null); setNotice(''); setRemoval(null); };
  const canClose = pending === null;
  const count = (key: Section) => key === 'providers' ? providers.data?.items.length : key === 'mcp' ? inventory.data?.items.filter(row => row.configuration.kind === 'mcp').length : inventory.data?.resources.filter(row => row.kind === (key === 'tools' ? 'tool' : 'skill')).length;
  return createPortal(<dialog ref={dialog} className="agent-settings-center" aria-labelledby={`${id}-title`} onCancel={event => { event.preventDefault(); if (!canClose) return; if (editor || removal) { setEditor(null); setRemoval(null); } else onClose(); }}>
    <header className="settings-center-header"><div><h2 id={`${id}-title`}>Agent settings</h2></div><button type="button" className="settings-icon" aria-label="Close Agent settings" disabled={!canClose} onClick={onClose}><AgentIcon name="close" /></button></header>
    <div className="settings-center-body"><nav className="settings-center-nav" aria-label="Agent settings categories">{sections.map(item => <button key={item.id} type="button" disabled={!!editor || !!pending} aria-current={section === item.id ? 'page' : undefined} onClick={() => { setSection(item.id); setError(null); setRemoval(null); }}><AgentIcon name={item.icon} /><span>{item.title}</span>{count(item.id) !== undefined ? <small>{count(item.id)}</small> : null}</button>)}</nav>
      <div className="settings-center-page" ref={page} aria-busy={pending !== null}>
        {editor ? <button type="button" className="settings-back" disabled={!!pending} onClick={() => { setEditor(null); setError(null); }}><span className="settings-left"><AgentIcon name="down" /></span>Back to {chosen.title.toLowerCase()}</button> : null}
        <div className="settings-page-heading"><h3>{heading}</h3><span className="settings-heading-actions">{!editor && section === 'providers' ? <button type="button" className="settings-primary" disabled={!!pending} onClick={() => run('reload-providers', async () => { await settingsApi.reloadProviders(); })}><BusyLabel busy={pending === 'reload-providers'} label="Reloading">Reload configuration</BusyLabel></button> : null}{!editor && section !== 'tools' && section !== 'providers' ? <button type="button" className="settings-primary" disabled={!!pending} onClick={() => open(section === 'mcp' ? 'mcp' : 'directory')}><AgentIcon name={section === 'skills' ? 'folder-add' : 'new'} />{section === 'mcp' ? 'Add server' : 'Add skill directory'}</button> : null}</span></div>
        <FormError message={error} />
        {!editor && (section === 'providers' ? providers.error : inventory.error) ? <p className="settings-error" role="alert">{errorText(section === 'providers' ? providers.error : inventory.error)} <button type="button" onClick={() => void (section === 'providers' ? providers.refetch() : inventory.refetch())}>Retry loading</button></p> : null}
        {removal ? <div className="settings-removal" role="alert"><p>Remove <strong>{removal.record.configuration.name}</strong>?</p><div><button disabled={!!pending} onClick={() => setRemoval(null)}>Keep it</button><button className="settings-danger" disabled={!!pending} onClick={() => { const selected = removal; run(selected.record.id, async () => { await settingsApi.deleteResource(selected.record); setRemoval(null); }); }}>Remove configuration</button></div></div> : null}
        {editor ? editor.kind === 'mcp' ? <ConnectionForm record={editor.record} onSave={saveForm} onCancel={() => setEditor(null)} /> : editor.kind === 'directory' ? <DirectoryForm record={editor.record} save={saveForm} cancel={() => setEditor(null)} /> : editor.kind === 'document' ? <SkillDocument resource={editor.resource} workspace={workspace} /> : <RuleEditor name={editor.name} initial={editor.policy} schema={editor.resource?.input_schema} operationSchema={editor.resource?.operation_schema} onSave={async policy => { await commit('policy', () => editor.save(policy)); setEditor(null); }} onCancel={() => setEditor(null)} />
          : section === 'providers' ? <ConfiguredProviders inventory={providers.data} loading={!providers.data && providers.isPending} />
          : section === 'tools' ? inventory.data ? <ResourcePanel inventory={inventory.data} actions={actions} /> : inventory.isPending ? <RowsSkeleton count={5} /> : null
            : section === 'mcp' ? <>{!inventory.data && inventory.isPending ? <RowsSkeleton /> : connections.map(row => <Fold key={row.id} title={row.configuration.name} count={inventory.data?.resources.filter(tool => tool.source_id === row.id).length} actions={<><span className={`settings-origin ${row.state === 'error' ? 'settings-error' : ''}`}>{row.state}</span><Toggle label={`Enable server ${row.configuration.name}`} checked={row.configuration.enabled} disabled={!!pending} busy={pending === row.id} onChange={enabled => run(row.id, () => save({ ...row.configuration, enabled }, row))} /></>}><div className="settings-source-info"><p className="settings-path">{row.configuration.transport?.type === 'stdio' ? `${row.configuration.transport.command} ${(row.configuration.transport.args ?? []).join(' ')}` : row.configuration.transport?.url}</p>{row.error ? <p className="settings-error">{row.error}</p> : null}<div className="settings-row-actions"><button disabled={!!pending} type="button" onClick={() => setEditor({ kind: 'mcp', record: row })}>Edit connection & test</button><button disabled={!!pending} type="button" onClick={() => actions.editPolicy(row.configuration.name, sourcePolicy(row), policy => saveSourcePolicy(row, policy))}>Parameter rules</button><DecisionSelect label={`${row.configuration.name} server permission`} value={sourcePolicy(row).default} disabled={!!pending} onChange={value => run(row.id, () => saveSourcePolicy(row, { ...sourcePolicy(row), default: value }))} /><button disabled={!!pending} type="button" onClick={() => setRemoval({ kind: 'resource', record: row })}>Remove</button></div></div>{inventory.data?.resources.some(tool => tool.source_id === row.id) ? <ResourcePanel inventory={inventory.data} onlyGroup={`mcp/${row.id}`} actions={actions} /> : <p className="settings-empty">{row.configuration.enabled ? 'No tools available.' : 'Server disabled.'}</p>}</Fold>)}{inventory.data && !connections.length ? <p className="settings-empty">No MCP servers connected.</p> : null}</>
              : <SkillsPanel inventory={inventory.data} workspace={workspace} workspaces={workspaces} onWorkspace={setWorkspace} actions={actions} onDocument={resource => setEditor({ kind: 'document', resource })} onEditDirectory={row => setEditor({ kind: 'directory', record: row })} onDelete={row => setRemoval({ kind: 'resource', record: row })} />}
        <div className="sr-only" role="status" aria-live="polite">{notice}</div>
      </div>
    </div>
  </dialog>, document.body);
}
