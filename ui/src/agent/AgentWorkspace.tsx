import { lazy, Suspense, useCallback, useEffect, useState } from 'react';
import { useInfiniteQuery, useQuery, useQueryClient } from '@tanstack/react-query';
import { SessionIcon, SearchIcon } from '../components/Sidebar';
import { DirectorySkeleton, BusyLabel, LoadingIndicator } from '../components/LoadingState';
import { AgentPageState } from './AgentPageState';
import { AgentIcon } from './AgentIcon';
import { WorkspaceDialog } from './WorkspaceDialog';
import { agentClient } from './client';
import { newId } from './ids';
import { config } from '../http';
import { saveDraft } from './drafts';
import type { DetailsTab } from './Details';
import type { SettingsRecord } from './types';
import './agent.css';

const AgentSession = lazy(() => import('./AgentSession'));
interface Props {
  session: string | null; invocation: string | null; workspace: string | null; active: boolean;
  onNavigate: (session: string | null, invocation: string | null, replace: boolean, workspace: string | null) => void;
  onTrace: (iid: string, sid: string) => void;
}

function dateGroup(at: number): string {
  const date = new Date(at / 1000).toDateString();
  if (date === new Date().toDateString()) return 'Today';
  const yesterday = new Date(); yesterday.setDate(yesterday.getDate() - 1);
  return date === yesterday.toDateString() ? 'Yesterday' : 'Earlier';
}

export default function AgentWorkspace({ session: selectedSession, invocation, workspace: requestedWorkspace, active, onNavigate, onTrace }: Props) {
  const catalog = useQuery({ queryKey: ['agent-catalog'], queryFn: ({ signal }) => agentClient.catalog(signal), enabled: active, refetchInterval: active ? 10000 : false });
  const [search, setSearch] = useState('');
  const [newSession, setNewSession] = useState(newId);
  const [sidebar, setSidebar] = useState(() => window.innerWidth >= 760);
  const [panel, setPanel] = useState<DetailsTab | null>(null);
  const [addingWorkspace, setAddingWorkspace] = useState(false);
  const session = selectedSession ?? newSession;
  const workspaces = useQuery({ queryKey: ['agent-workspaces'], queryFn: ({ signal }) => agentClient.workspaces(signal), enabled: active, refetchInterval: active ? 15000 : false, retry: false });
  const binding = useQuery({ queryKey: ['agent-binding', selectedSession], queryFn: ({ signal }) => agentClient.settings(selectedSession!, signal), enabled: active && !!selectedSession, retry: false });
  const bindingReady = !selectedSession || !!binding.data;
  const workspace = selectedSession && binding.data ? binding.data.settings.workspace?.workspace_id ?? null : requestedWorkspace ?? catalog.data?.project_workspace_id ?? null;
  const directoryReady = !!workspaces.data && (bindingReady || !!requestedWorkspace) && !!catalog.data;
  const sessions = useInfiniteQuery({ queryKey: ['agent-sessions', workspace], queryFn: ({ pageParam, signal }) => agentClient.sessions(workspace, pageParam, signal), initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor, enabled: active && directoryReady, refetchInterval: active && directoryReady ? 15000 : false, retry: false });
  const directoryLoading = active && (!directoryReady && !workspaces.error && !binding.error || directoryReady && sessions.isLoading);
  const client = useQueryClient();
  const refreshDirectory = useCallback(() => { void client.invalidateQueries({ queryKey: ['agent-sessions'] }); }, [client]);
  const select = (sid: string, iid: string | null) => { onNavigate(sid, iid, sid === selectedSession, workspace); if (window.innerWidth < 760) setSidebar(false); };
  const selectWorkspace = (id: string) => { setNewSession(newId()); setPanel(null); setSearch(''); onNavigate(null, null, false, id); };
  const newConversation = () => { saveDraft(config.apiPrefix, 'new:' + (workspace ?? 'unbound'), ''); setNewSession(newId()); setPanel(null); onNavigate(null, null, selectedSession === null, workspace); if (window.innerWidth < 760) setSidebar(false); };
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
  const allRows = directoryReady ? sessions.data?.pages.flatMap(page => page.items) ?? [] : [];
  const rows = allRows.filter(item => item.title.toLowerCase().includes(search.toLowerCase()));
  const title = allRows.find(item => item.session_id === session)?.title ?? (selectedSession ? 'Conversation' : 'New conversation');

  return <section className={`workspace workspace-tab agent-workspace ${active ? '' : 'is-inactive'}`} data-workspace-view="agents" aria-hidden={!active} inert={!active}>
    {sidebar ? <button className="agent-directory-backdrop" aria-label="Close conversations" onClick={() => setSidebar(false)} /> : null}
    <nav id="agent-conversations" className={`directory agent-directory ${sidebar ? '' : 'collapsed'}`} aria-label="Conversations" aria-hidden={!sidebar} inert={!sidebar}>
      <div className="agent-directory-top">
        <div className="agent-workspace-label"><label htmlFor="agent-workspace-select">Workspace</label>{workspaces.isFetching ? <LoadingIndicator label="Loading workspaces" small /> : null}<button type="button" className="icon-button" aria-label="Add workspace" onClick={() => setAddingWorkspace(true)}>＋</button></div>
        <select id="agent-workspace-select" className="agent-workspace-select" aria-label="Workspace" value={workspace ?? ''} disabled={!workspaces.data || !bindingReady} onChange={event => selectWorkspace(event.target.value)}>
          {!workspaces.data ? <option value={workspace ?? ''}>Select workspace</option> : null}
          {bindingReady && !workspace ? <option value="" disabled>No workspace</option> : null}
          {workspace && workspaces.data && !workspaces.data.items.some(item => item.id === workspace) ? <option value={workspace} disabled>Unavailable workspace</option> : null}
          {workspaces.data?.items.map(item => <option key={item.id} value={item.id}>{item.name}{item.is_project ? ' · Current project' : ''}{item.available === false ? ' · Unavailable' : ''}</option>)}
        </select>
        <button className="agent-new-conversation" disabled={!directoryReady || !workspace} onClick={newConversation}><AgentIcon name="new" />New conversation</button>
      </div>
      <div className="directory-search"><div className="search-field"><SearchIcon /><input type="search" aria-label="Filter loaded conversations" placeholder="Search conversations…" value={search} onChange={event => setSearch(event.target.value)} /></div></div>
      <div className="agent-directory-heading"><span>Conversations</span>{sessions.isFetching && !sessions.isLoading ? <LoadingIndicator label="Updating conversations" small /> : null}</div>
      <div className="directory-content" aria-busy={directoryLoading}>
        {!directoryLoading ? <ul className="workflow-list">{rows.map((item, index) => <li className="workflow-group" key={item.session_id}>
          {index === 0 || dateGroup(item.updated_at_us) !== dateGroup(rows[index - 1].updated_at_us) ? <div className="directory-group-label">{dateGroup(item.updated_at_us)}</div> : null}
          <button className={`workflow-item session-item ${session === item.session_id ? 'active' : ''}`} aria-current={session === item.session_id ? 'true' : undefined} title={item.title} onClick={() => select(item.session_id, item.invocation_id)}><span className="workflow-glyph"><SessionIcon /></span><span className="workflow-item-text"><strong>{item.title}</strong><span className="workflow-item-meta">{item.invocation_count} {item.invocation_count === 1 ? 'turn' : 'turns'}</span></span></button>
        </li>)}</ul> : null}
        {directoryLoading ? <DirectorySkeleton label="Loading conversations" kind="conversation" /> : workspaces.error || binding.error || sessions.error ? <div className="sidebar-error" role="alert"><p>{(workspaces.error ?? binding.error ?? sessions.error)?.message}</p><button onClick={() => { void workspaces.refetch(); if (selectedSession) void binding.refetch(); if (directoryReady) void sessions.refetch(); }}>Retry</button></div> : !rows.length ? <p className="sidebar-message">{search ? 'No matching conversations.' : 'Start a conversation in this workspace.'}</p> : null}
        {sessions.hasNextPage ? <button className="load-more" disabled={sessions.isFetchingNextPage} onClick={() => void sessions.fetchNextPage()}><BusyLabel busy={sessions.isFetchingNextPage} label="Loading more">Load more conversations</BusyLabel></button> : null}
      </div>
      <div className="agent-directory-footer"><button disabled={!agent} onClick={() => openPanel('Tools')}><AgentIcon name="tools" />Tools & MCP</button><button disabled={!agent} onClick={() => openPanel('Skills')}><AgentIcon name="skills" />Skills</button><button disabled={!agent} onClick={() => openPanel('Settings')}><AgentIcon name="settings" />Settings</button></div>
    </nav>
    <div className="agent-app">{catalog.error && !catalog.data ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} error={catalog.error} retrying={catalog.isFetching} onRetry={() => void catalog.refetch()} />
      : !catalog.data ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} loading />
        : !agent ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} />
          : binding.error && !binding.data ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} error={binding.error} onRetry={() => void binding.refetch()} />
            : !directoryReady || !bindingReady ? <AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} loading={!(workspaces.error)} error={workspaces.error ?? undefined} onRetry={() => void workspaces.refetch()} />
              : <Suspense fallback={<AgentPageState title={title} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} loading />}><AgentSession key={session} session={session} existing={!!selectedSession} invocation={invocation} title={title} agent={agent} workspace={workspace} catalog={catalog.data} active={active} panel={panel} onPanel={setPanel} sidebar={sidebar} onSidebar={() => setSidebar(value => !value)} onAccepted={iid => { if (session === newSession) { const saved = client.getQueryData<SettingsRecord>(['agent-settings', session, workspace]); if (saved) client.setQueryData(['agent-binding', session], saved); setNewSession(newId()); } select(session, iid); }} onAgent={item => select(item.session_id, item.invocation_id)} onTrace={onTrace} onRefreshDirectory={refreshDirectory} /></Suspense>}
    </div>
    {addingWorkspace ? <WorkspaceDialog onClose={() => setAddingWorkspace(false)} onCreated={item => { client.setQueryData(['agent-workspaces'], (current: { items: typeof item[] } | undefined) => ({ items: [...(current?.items ?? []).filter(value => value.id !== item.id), item] })); void workspaces.refetch(); setAddingWorkspace(false); selectWorkspace(item.id); }} /> : null}
  </section>;
}
