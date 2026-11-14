import { lazy, Suspense, useCallback, useEffect, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ConversationDirectory } from './ConversationDirectory';
import { AgentPageState } from './AgentPageState';
import { AgentIcon } from './AgentIcon';
import { AgentDirectory } from './AgentDirectory';
import { WorkspaceDialog } from './WorkspaceDialog';
import type { Section as SettingsSection } from './settings/types';
import { SettingsCenter } from './settings/SettingsCenter';
import { agentClient } from './client';
import { newId } from './ids';
import { config } from '../http';
import { readDraft, saveDraft } from './drafts';
import type { AgentSession as SessionRow, SettingsRecord } from './types';
import type { AgentSidebarControls } from './useAgentSidebar';
import './agent.css';

const AgentSession = lazy(() => import('./AgentSession'));
interface Props {
  sidebar: AgentSidebarControls;
  session: string | null; invocation: string | null; workspace?: string | null; active: boolean;
  onNavigate: (session: string | null, invocation: string | null, replace: boolean, workspace: string | null) => void;
}

export default function AgentWorkspace({ sidebar, session: selectedSession, invocation, workspace: requestedWorkspace, active, onNavigate }: Props) {
  const catalog = useQuery({ queryKey: ['agent-catalog'], queryFn: ({ signal }) => agentClient.catalog(signal), enabled: active, refetchInterval: active ? 10000 : false });
  const [directoryTitle, setDirectoryTitle] = useState<{ session: string; title: string } | null>(null);
  const rememberTitle = useCallback((session: string, title: string) => setDirectoryTitle(current => current?.session === session && current.title === title ? current : { session, title }), []);
  const [newSession, setNewSession] = useState(newId);
  const [addingWorkspace, setAddingWorkspace] = useState(false);
  const [settingsSection, setSettingsSection] = useState<SettingsSection | null>(null);
  const session = selectedSession ?? newSession;
  const workspaces = useQuery({ queryKey: ['agent-workspaces'], queryFn: ({ signal }) => agentClient.workspaces(signal), enabled: active, refetchInterval: active ? 15000 : false, retry: false });
  const binding = useQuery({ queryKey: ['agent-binding', selectedSession], queryFn: ({ signal }) => agentClient.settings(selectedSession!, signal), enabled: active && !!selectedSession, retry: false });
  const bindingReady = !selectedSession || !!binding.data;
  const workspace = selectedSession && binding.data ? binding.data.workspace?.workspace_id ?? null : requestedWorkspace === undefined ? catalog.data?.project_workspace_id ?? null : requestedWorkspace;
  const client = useQueryClient();
  const refreshDirectory = useCallback(() => { void client.invalidateQueries({ queryKey: ['agent-sessions'] }); }, [client]);
  const select = (sid: string, iid: string | null, targetWorkspace: string | null = workspace) => { sidebar.dismiss(); onNavigate(sid, iid, sid === selectedSession, targetWorkspace); };
  const newConversation = (targetWorkspace: string | null) => { sidebar.dismiss(); saveDraft(config.apiPrefix, 'new:' + (targetWorkspace ?? 'unbound'), ''); setNewSession(newId()); onNavigate(null, null, selectedSession === null, targetWorkspace); };
  const changeWorkspace = (target: string | null) => {
    if (selectedSession || target === workspace) return;
    const saved = client.getQueryData<SettingsRecord>(['agent-settings', session, workspace]);
    if (saved) client.setQueryData(['agent-settings', session, target], { ...saved, workspace: target ? { workspace_id: target, working_directory: '.' } : null });
    const oldKey = 'new:' + (workspace ?? 'unbound');
    saveDraft(config.apiPrefix, 'new:' + (target ?? 'unbound'), readDraft(config.apiPrefix, oldKey));
    saveDraft(config.apiPrefix, oldKey, '');
    onNavigate(null, null, true, target);
  };
  const selectConversation = (row: SessionRow, targetWorkspace: string | null) => { rememberTitle(row.session_id, row.title); select(row.session_id, row.invocation_id, targetWorkspace); };
  useEffect(() => {
    if (active && selectedSession && binding.isSuccess && requestedWorkspace !== workspace) onNavigate(selectedSession, invocation, true, workspace);
  }, [active, selectedSession, binding.isSuccess, requestedWorkspace, workspace, invocation, onNavigate]);
  const openSettings = (value: SettingsSection) => { sidebar.dismiss(); setSettingsSection(value); };
  useEffect(() => { if (!active) { setAddingWorkspace(false); setSettingsSection(null); } }, [active]);
  const agent = catalog.data?.agent;
  const title = directoryTitle?.session === session ? directoryTitle.title : selectedSession ? 'Conversation' : 'New conversation';

  return <section className={`workspace workspace-tab agent-workspace ${active ? '' : 'is-inactive'}`} data-workspace-view="agents" aria-hidden={!active} inert={!active}>
    <button type="button" className={`agent-directory-backdrop ${sidebar.narrow && sidebar.open ? 'is-open' : ''}`} aria-label="Close conversations" tabIndex={-1} aria-hidden={!sidebar.narrow || !sidebar.open} disabled={!sidebar.narrow || !sidebar.open} onClick={sidebar.dismiss} />
    <AgentDirectory active={active} open={sidebar.open} narrow={sidebar.narrow} onClose={sidebar.dismiss}>
      <ConversationDirectory key={catalog.data?.project_workspace_id ?? 'loading'} items={workspaces.data?.items} loading={workspaces.isLoading} error={workspaces.error} onRetry={() => void workspaces.refetch()} onAddWorkspace={() => { sidebar.dismiss(); setAddingWorkspace(true); }} storageScope={`${config.apiPrefix}:${catalog.data?.project_workspace_id ?? 'loading'}`} canCreate={!!agent}
        workspace={bindingReady ? workspace : requestedWorkspace ?? null} session={selectedSession} active={active} load={agentClient.sessions} onSelect={selectConversation} onNew={newConversation} onTitle={rememberTitle} />
      <div className="agent-directory-footer"><button disabled={!agent} onClick={() => openSettings('tools')}><AgentIcon name="tools" />Tools & MCP</button><button disabled={!agent} onClick={() => openSettings('skills')}><AgentIcon name="skills" />Skills</button><button disabled={!agent} onClick={() => openSettings('providers')}><AgentIcon name="settings" />Settings</button></div>
    </AgentDirectory>
    <div className="agent-app" inert={sidebar.narrow && sidebar.open}>{catalog.error && !catalog.data ? <AgentPageState title={title} error={catalog.error} retrying={catalog.isFetching} onRetry={() => void catalog.refetch()} />
      : !catalog.data ? <AgentPageState title={title} loading />
        : !agent ? <AgentPageState title={title} />
          : binding.error && !binding.data ? <AgentPageState title={title} error={binding.error} onRetry={() => void binding.refetch()} />
            : !bindingReady ? <AgentPageState title={title} loading />
              : <Suspense fallback={<AgentPageState title={title} loading />}><AgentSession key={session} session={session} existing={!!selectedSession} invocation={invocation} title={title} agent={agent} workspace={workspace} workspaces={workspaces.data?.items} workspacesLoading={workspaces.isLoading} workspaceError={workspaces.error} onRetryWorkspaces={() => void workspaces.refetch()} onWorkspaceChange={changeWorkspace} catalog={catalog.data} active={active} onAccepted={iid => { if (session === newSession) { const saved = client.getQueryData<SettingsRecord>(['agent-settings', session, workspace]); if (saved) client.setQueryData(['agent-binding', session], saved); setNewSession(newId()); } select(session, iid); }} onRefreshDirectory={refreshDirectory} /></Suspense>}
    </div>
    {addingWorkspace ? <WorkspaceDialog onClose={() => setAddingWorkspace(false)} onCreated={item => { client.setQueryData(['agent-workspaces'], (current: { items: typeof item[] } | undefined) => ({ items: [...(current?.items ?? []).filter(value => value.id !== item.id), item] })); void workspaces.refetch(); setAddingWorkspace(false); newConversation(item.id); }} /> : null}
    {settingsSection ? <SettingsCenter initial={settingsSection} currentWorkspace={workspace ?? catalog.data?.project_workspace_id ?? null} workspaces={workspaces.data?.items ?? []} onClose={() => setSettingsSection(null)} /> : null}
  </section>;
}
