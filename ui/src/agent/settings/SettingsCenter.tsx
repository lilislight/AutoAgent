import { useId, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { AgentIcon, type AgentIconName } from '../AgentIcon';
import { BusyLabel } from '../../components/LoadingState';
import { settingsApi } from './api';
import { Fold, FormError, RowsSkeleton, Toggle, errorText } from './Controls';
import { ConfiguredProviders } from './ConfiguredProviders';
import { ConnectionForm } from './ConnectionForm';
import { RuleEditor } from './RuleEditor';
import { ResourcePanel, type ResourceActions } from './ResourcePanel';
import { DirectoryForm, SkillDocument, SkillsPanel } from './SkillsPanel';
import { defaultPolicy, type ConfigRecord, type Policy, type Resource, type ResourceConfig, type Section } from './types';
import type { AgentWorkspaceInfo } from '../types';
import './settings.css';

const sections: { id: Section; title: string; icon: AgentIconName }[] = [
  { id: 'providers', title: 'Providers', icon: 'activity' },
  { id: 'tools', title: 'Tools', icon: 'tools' },
  { id: 'mcp', title: 'MCP servers', icon: 'terminal' },
  { id: 'skills', title: 'Skills', icon: 'skills' },
];
type Editor = { kind: 'mcp'; record?: ConfigRecord } | { kind: 'directory'; record?: ConfigRecord }
  | { kind: 'policy'; resource: Resource; policy: Policy; workspace: string; save: (policy: Policy) => Promise<void> }
  | { kind: 'document'; resource: Resource };
type Removal = { kind: 'resource'; record: ConfigRecord };

export function SettingsCenter({ initial = 'providers', currentWorkspace, workspaces, onClose }: { initial?: Section; currentWorkspace: string | null; workspaces: AgentWorkspaceInfo[]; onClose: () => void }) {
  const [section, setSection] = useState(initial), [workspace, setWorkspace] = useState(currentWorkspace);
  const [editor, setEditor] = useState<Editor | null>(null), [removal, setRemoval] = useState<Removal | null>(null);
  const [pending, setPending] = useState<string | null>(null), [error, setError] = useState<string | null>(null), [notice, setNotice] = useState('');
  const dialog = useRef<HTMLDialogElement>(null), page = useRef<HTMLDivElement>(null), id = useId(), client = useQueryClient();
  const scope = section === 'tools' || section === 'mcp' ? section : undefined;
  const inventory = useQuery({ queryKey: ['agent-app-resources', workspace, scope],
    queryFn: ({ signal }) => settingsApi.resources(workspace, signal, scope), enabled: section !== 'providers', refetchInterval: 5000, retry: false });
  const providers = useQuery({ queryKey: ['agent-app-providers'], queryFn: ({ signal }) => settingsApi.providers(signal), refetchInterval: 10000, retry: false });
  useLayoutEffect(() => { const element = dialog.current!; const previous = document.activeElement as HTMLElement | null; element.showModal(); return () => { element.close(); if (previous?.isConnected && previous.getClientRects().length && getComputedStyle(previous).visibility !== 'hidden' && !previous.closest('[inert]')) previous.focus({ preventScroll: true }); else document.querySelector<HTMLTextAreaElement>('.agent-composer textarea')?.focus({ preventScroll: true }); }; }, []);
  const positions = useRef(new Map<Section, { top: number; anchor: HTMLElement | null }>());
  const savePosition = () => {
    positions.current.set(section, { top: page.current?.scrollTop ?? 0, anchor: document.activeElement as HTMLElement | null });
  };
  const openEditor = (next: Editor) => {
    savePosition();
    setEditor(next);
    setError(null);
    setRemoval(null);
  };
  const closeEditor = () => { setEditor(null); setError(null); };
  const editing = editor !== null;
  useLayoutEffect(() => {
    const element = page.current;
    if (!element) return;
    if (editing) {
      element.scrollTop = 0;
      element.querySelector<HTMLButtonElement>('.settings-back')?.focus({ preventScroll: true });
    } else {
      const position = positions.current.get(section);
      element.scrollTop = position?.top ?? 0;
      if (position?.anchor?.isConnected) position.anchor.focus({ preventScroll: true });
    }
  }, [editing, section]);
  const refresh = async () => { await Promise.all([client.invalidateQueries({ queryKey: ['agent-app-resources'] }), client.invalidateQueries({ queryKey: ['agent-app-providers'] }), client.invalidateQueries({ queryKey: ['agent-catalog'] }), client.invalidateQueries({ queryKey: ['agent-settings'] }), client.invalidateQueries({ queryKey: ['agent-resources'] })]); };
  const commit = async (key: string, action: () => Promise<unknown>) => { if (pending) return; setPending(key); setError(null); setNotice(''); try { await action(); await refresh(); setNotice(key === 'reload-providers' ? 'Provider configuration reloaded.' : 'Changes saved.'); } catch (error) { void refresh(); throw error; } finally { setPending(null); } };
  const run: ResourceActions['run'] = (key, action) => { void commit(key, action).catch(error => setError(errorText(error))); };
  const save = async (config: ResourceConfig, record?: ConfigRecord) => { await settingsApi.saveResource(config, record); };
  const saveForm = async (config: ResourceConfig, record?: ConfigRecord) => { await commit(record?.id ?? 'new-resource', () => save(config, record)); closeEditor(); };
  const actions: ResourceActions = { pending, save, run, editPolicy: (resource, save) => {
    if (!inventory.data) return;
    openEditor({ kind: 'policy', resource, workspace: inventory.data.workspace_id,
      policy: resource.custom_policy ?? defaultPolicy(), save });
  } };
  const chosen = sections.find(item => item.id === section)!;
  const configs = inventory.data?.items ?? [], connections = configs.filter(row => row.configuration.kind === 'mcp');
  const heading = editor ? editor.kind === 'mcp' ? `${editor.record ? 'Edit' : 'Add'} MCP server` : editor.kind === 'directory' ? `${editor.record ? 'Edit' : 'Add'} skill directory` : editor.kind === 'document' ? editor.resource.name : `Permissions · ${editor.resource.name}` : '';
  const open = (kind: 'mcp' | 'directory') => { openEditor({ kind }); setNotice(''); };
  const canClose = pending === null;
  return createPortal(<dialog ref={dialog} className="agent-settings-center" aria-labelledby={`${id}-title`} onCancel={event => { event.preventDefault(); if (!canClose) return; if (editor || removal) { closeEditor(); setRemoval(null); } else onClose(); }}>
    <header className="settings-center-header"><div><h2 id={`${id}-title`}>Agent settings</h2></div><button type="button" className="settings-icon" aria-label="Close Agent settings" disabled={!canClose} onClick={onClose}><AgentIcon name="close" /></button></header>
    <div className="settings-center-body"><nav className="settings-center-nav" aria-label="Agent settings categories">{sections.map(item => <button key={item.id} type="button" disabled={!!editor || !!pending} aria-current={section === item.id ? 'page' : undefined} aria-label={item.title} onClick={() => { if (item.id !== section) savePosition(); setSection(item.id); setError(null); setRemoval(null); }}>
      <AgentIcon name={item.icon} />
      {item.id === 'mcp' ? <><span className="settings-nav-wide">MCP servers</span><span className="settings-nav-narrow">MCP</span></> : <span>{item.title}</span>}
    </button>)}</nav>
      <div className="settings-center-page" ref={page} aria-busy={pending !== null}>
        {editor ? <div>
          <button type="button" className="settings-back" disabled={!!pending} onClick={closeEditor}>
            <span className="settings-left"><AgentIcon name="down" /></span>Back to {chosen.title.toLowerCase()}
          </button>
          <div className="settings-page-heading"><h3>{heading}</h3></div>
          {editor.kind === 'mcp' ? <ConnectionForm record={editor.record} onSave={saveForm} onCancel={closeEditor} />
            : editor.kind === 'directory' ? <DirectoryForm record={editor.record} save={saveForm} cancel={closeEditor} />
            : editor.kind === 'document' ? <SkillDocument resource={editor.resource} workspace={workspace} />
            : <RuleEditor resource={editor.resource} initial={editor.policy} workspace={editor.workspace}
                policySchema={inventory.data?.policy_schema} onSave={async policy => { await commit('policy', () => editor.save(policy)); closeEditor(); }}
                onAcceptSchema={editor.resource.mcp_status === 'review' ? async () => {
                  const { source_id, remote_name, schema_version } = editor.resource;
                  if (!source_id || !remote_name || !schema_version) return;
                  await commit('mcp-schema', () => settingsApi.acceptMcpSchema(source_id, remote_name, schema_version));
                  setEditor(current => current?.kind === 'policy' ? { ...current, resource: {
                    ...current.resource, mcp_status: 'ready', approved_schema: null,
                  } } : current);
                } : undefined} onCancel={closeEditor} />}
        </div> : null}
        <div hidden={!!editor}>
        <div className="settings-page-heading"><h3>{chosen.title}</h3><span className="settings-heading-actions">{!editor && section === 'providers' ? <button type="button" className="settings-primary" disabled={!!pending} onClick={() => run('reload-providers', async () => { await settingsApi.reloadProviders(); })}><BusyLabel busy={pending === 'reload-providers'} label="Reloading">Reload configuration</BusyLabel></button> : null}{!editor && section !== 'tools' && section !== 'providers' ? <button type="button" className="settings-primary" disabled={!!pending} onClick={() => open(section === 'mcp' ? 'mcp' : 'directory')}><AgentIcon name={section === 'skills' ? 'folder-add' : 'new'} />{section === 'mcp' ? 'Add server' : 'Add skill directory'}</button> : null}</span></div>
        <FormError message={error} />
        {!editor && (section === 'providers' ? providers.error : inventory.error) ? <p className="settings-error" role="alert">{errorText(section === 'providers' ? providers.error : inventory.error)} <button type="button" onClick={() => void (section === 'providers' ? providers.refetch() : inventory.refetch())}>Retry loading</button></p> : null}
        {removal ? <div className="settings-removal" role="alert"><p>Remove <strong>{removal.record.configuration.name}</strong>?</p><div><button disabled={!!pending} onClick={() => setRemoval(null)}>Keep it</button><button className="settings-danger" disabled={!!pending} onClick={() => { const selected = removal; run(selected.record.id, async () => { await settingsApi.deleteResource(selected.record); setRemoval(null); }); }}>Remove configuration</button></div></div> : null}
        {section === 'providers' ? <ConfiguredProviders inventory={providers.data} loading={!providers.data && providers.isPending} />
          : section === 'tools' ? inventory.data ? <ResourcePanel inventory={inventory.data} actions={actions} /> : inventory.isPending ? <RowsSkeleton count={5} /> : null
            : section === 'mcp' ? <>{!inventory.data && inventory.isPending ? <RowsSkeleton /> : connections.map(row => <Fold key={row.id} title={row.configuration.name} count={inventory.data?.resources.filter(tool => tool.source_id === row.id).length} actions={<><span className={`settings-origin ${row.state === 'error' ? 'settings-error' : ''}`}>{row.state}</span><Toggle label={`Enable server ${row.configuration.name}`} checked={row.configuration.enabled} disabled={!!pending} busy={pending === row.id} onChange={enabled => run(row.id, () => save({ ...row.configuration, enabled }, row))} /></>}><div className="settings-source-info"><p className="settings-path">{row.configuration.transport?.type === 'stdio' ? `${row.configuration.transport.command} ${(row.configuration.transport.args ?? []).join(' ')}` : row.configuration.transport?.url}</p>{row.error ? <p className="settings-error">{row.error}</p> : null}<div className="settings-row-actions"><button disabled={!!pending} type="button" onClick={() => openEditor({ kind: 'mcp', record: row })}>Edit connection & test</button>{row.state === 'error' ? <button disabled={!!pending} type="button" onClick={() => run(row.id, () => save(row.configuration, row))}>Retry connection</button> : null}<button disabled={!!pending} type="button" onClick={() => setRemoval({ kind: 'resource', record: row })}>Remove</button></div></div>{inventory.data?.resources.some(tool => tool.source_id === row.id) ? <ResourcePanel inventory={inventory.data} onlyGroup={`mcp/${row.id}`} actions={actions} /> : <p className="settings-empty">{row.configuration.enabled ? 'No tools available.' : 'Server disabled.'}</p>}</Fold>)}{inventory.data && !connections.length ? <p className="settings-empty">No MCP servers connected.</p> : null}</>
              : <SkillsPanel inventory={inventory.data} workspace={workspace} workspaces={workspaces} onWorkspace={setWorkspace} actions={actions} onDocument={resource => openEditor({ kind: 'document', resource })} onEditDirectory={row => openEditor({ kind: 'directory', record: row })} onDelete={row => setRemoval({ kind: 'resource', record: row })} />}
        </div>
        <div className="sr-only" role="status" aria-live="polite">{notice}</div>
      </div>
    </div>
  </dialog>, document.body);
}
