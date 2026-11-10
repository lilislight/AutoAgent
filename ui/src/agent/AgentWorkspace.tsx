import { lazy, Suspense, useCallback, useEffect, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ConversationDirectory } from './ConversationDirectory';
import { Tooltip } from '../components/Tooltip';
import { AgentPageState } from './AgentPageState';
import { AgentIcon } from './AgentIcon';
import { WorkspaceDialog } from './WorkspaceDialog';
import { agentClient } from './client';
import { newId } from './ids';
import { config } from '../http';
import { saveDraft } from './drafts';
import type { DetailsTab } from './Details';
import type { AgentSession as SessionRow, SettingsRecord } from './types';
import './agent.css';

const AgentSession = lazy(() => import('./AgentSession'));
interface Props {
  session: string | null; invocation: string | null; workspace: string | null; active: boolean;
  onNavigate: (session: string | null, invocation: string | null, replace: boolean, workspace: string | null) => void;
  onTrace: (iid: string, sid: string) => void;
}

export default function AgentWorkspace({ session: selectedSession, invocation, workspace: requestedWorkspace, active, onNavigate, onTrace }: Props) {
  const catalog = useQuery({ queryKey: ['agent-catalog'], queryFn: ({ signal }) => agentClient.catalog(signal), enabled: active, refetchInterval: active ? 10000 : false });
  const [directoryTitle, setDirectoryTitle] = useState<{ session: string; title: string } | null>(null);
  const rememberTitle = useCallback((session: string, title: string) => setDirectoryTitle(current => current?.session === session && current.title === title ? current : { session, title }), []);
  const [newSession, setNewSession] = useState(newId);
  const [sidebar, setSidebar] = useState(() => window.innerWidth >= 760);
  const [panel, setPanel] = useState<DetailsTab | null>(null);
  const [addingWorkspace, setAddingWorkspace] = useState(false);
  const session = selectedSession ?? newSession;
  const workspaces = useQuery({ queryKey: ['agent-workspaces'], queryFn: ({ signal }) => agentClient.workspaces(signal), enabled: active, refetchInterval: active ? 15000 : false, retry: false });
  const binding = useQuery({ queryKey: ['agent-binding', selectedSession], queryFn: ({ signal }) => agentClient.settings(selectedSession!, signal), enabled: active && !!selectedSession, retry: false });
  const bindingReady = !selectedSession || !!binding.data;
  const workspace = selectedSession && binding.data ? binding.data.settings.workspace?.workspace_id ?? null : requestedWorkspace ?? catalog.data?.project_workspace_id ?? null;
  const client = useQueryClient();
  const refreshDirectory = useCallback(() => { void client.invalidateQueries({ queryKey: ['agent-sessions'] }); }, [client]);
  const select = (sid: string, iid: string | null, targetWorkspace: string | null = workspace) => { onNavigate(sid, iid, sid === selectedSession, targetWorkspace); if (window.innerWidth < 760) setSidebar(false); };
  const newConversation = (targetWorkspace = workspace ?? catalog.data?.project_workspace_id ?? null) => { saveDraft(config.apiPrefix, 'new:' + (targetWorkspace ?? 'unbound'), ''); setNewSession(newId()); setPanel(null); onNavigate(null, null, selectedSession === null, targetWorkspace); if (window.innerWidth < 760) setSidebar(false); };
  const selectConversation = (row: SessionRow, targetWorkspace: string | null) => { rememberTitle(row.session_id, row.title); select(row.session_id, row.invocation_id, targetWorkspace); };
  useEffect(() => {
    if (active && selectedSession && binding.isSuccess && requestedWorkspace !== workspace) onNavigate(selectedSession, invocation, true, workspace);
  }, [active, selectedSession, binding.isSuccess, requestedWorkspace, workspace, invocation, onNavigate]);
  const openPanel = (value: DetailsTab) => { setPanel(value); if (window.innerWidth < 760) setSidebar(false); };
  useEffect(() => { if (!active) setAddingWorkspace(false); }, [active]);
  useEffect(() => {
    if (!active) return;
    const escape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape' || document.querySelector('dialog[open]')) return;
      setPanel(null); if (window.innerWidth < 760) setSidebar(false);
    };
    window.addEventListener('keydown', escape);
    return () => window.removeEventListener('keydown', escape);
  }, [active]);
  const agent = catalog.data?.agent;
  const title = directoryTitle?.session === session ? directoryTitle.title : selectedSession ? 'Conversation' : 'New conversation';

  return <section className={`workspace workspace-tab agent-workspace ${active ? '' : 'is-inactive'}`} data-workspace-view="agents" aria-hidden={!active} inert={!active}>
    {sidebar ? <button className="agent-directory-backdrop" aria-label="Close conversations" onClick={() => setSidebar(false)} /> : null}
    <nav id="agent-conversations" className={`directory agent-directory ${sidebar ? '' : 'collapsed'}`} aria-label="Conversations" aria-hidden={!sidebar} inert={!sidebar}>
      <div className="agent-directory-top">
        <button className="agent-new-conversation" disabled={!catalog.data?.agent || !bindingReady} onClick={() => newConversation()}><AgentIcon name="new" />New conversation</button>
        <Tooltip content="Add workspace"><button type="button" className="agent-add-workspace" aria-label="Add workspace" onClick={() => setAddingWorkspace(true)}><AgentIcon name="folder" /></button></Tooltip>
      </div>
      <ConversationDirectory items={workspaces.data?.items} loading={workspaces.isLoading} error={workspaces.error} fetching={workspaces.isFetching} onRetry={() => void workspaces.refetch()}
        workspace={bindingReady ? workspace : requestedWorkspace} session={selectedSession} active={active} load={agentClient.sessions} onSelect={selectConversation} onNew={newConversation} onTitle={rememberTitle} />
      <div className="agent-directory-footer"><button disabled={!agent} onClick={() => openPanel('Tools')}><AgentIcon name="tools" />Tools & MCP</button><button disabled={!agent} onClick={() => openPanel('Skills')}><AgentIcon name="skills" />Skills</button><button disabled={!agent} onClick={() => openPanel('Settings')}><AgentIcon name="settings" />Settings</button></div>
    </nav>
    <div className="agent-app">{catalog.error && !catalog.data ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} error={catalog.error} retrying={catalog.isFetching} onRetry={() => void catalog.refetch()} />
      : !catalog.data ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} loading />
        : !agent ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} />
          : binding.error && !binding.data ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} error={binding.error} onRetry={() => void binding.refetch()} />
            : !bindingReady ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} loading />
              : <Suspense fallback={<AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} loading />}><AgentSession key={session} session={session} existing={!!selectedSession} invocation={invocation} title={title} agent={agent} workspace={workspace} catalog={catalog.data} active={active} panel={panel} onPanel={setPanel} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} onAccepted={iid => { if (session === newSession) { const saved = client.getQueryData<SettingsRecord>(['agent-settings', session, workspace]); if (saved) client.setQueryData(['agent-binding', session], saved); setNewSession(newId()); } select(session, iid); }} onAgent={item => select(item.session_id, item.invocation_id)} onTrace={onTrace} onRefreshDirectory={refreshDirectory} /></Suspense>}
    </div>
    {addingWorkspace ? <WorkspaceDialog onClose={() => setAddingWorkspace(false)} onCreated={item => { client.setQueryData(['agent-workspaces'], (current: { items: typeof item[] } | undefined) => ({ items: [...(current?.items ?? []).filter(value => value.id !== item.id), item] })); void workspaces.refetch(); setAddingWorkspace(false); newConversation(item.id); }} /> : null}
  </section>;
}
