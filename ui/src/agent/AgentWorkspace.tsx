import { newId } from './ids';
import { lazy, Suspense, useCallback, useState } from 'react';
import { useInfiniteQuery, useQuery, useQueryClient } from '@tanstack/react-query';
import { Sidebar, SessionIcon, SearchIcon } from '../components/Sidebar';
import { DirectorySkeleton, ConversationSkeleton, BusyLabel } from '../components/LoadingState';
import type { LocationState } from '../types';
import { agentClient } from './client';
import './agent.css';

const AgentSession = lazy(() => import('./AgentSession'));
interface Props { location: LocationState; active: boolean; navigate: (location: LocationState, replace?: boolean) => void; changeView: (view: LocationState['view']) => void; onTrace: (iid: string, sid: string) => void }

export default function AgentWorkspace({ location, active, navigate, changeView, onTrace }: Props) {
  const catalog = useQuery({ queryKey: ['agent-catalog'], queryFn: ({ signal }) => agentClient.catalog(signal), enabled: active, refetchInterval: active ? 10000 : false });
  const [search, setSearch] = useState('');
  const [newSession] = useState(() => newId());
  const session = location.session ?? newSession;
  const sessions = useInfiniteQuery({ queryKey: ['agent-sessions'], queryFn: ({ pageParam, signal }) => agentClient.sessions(pageParam, signal), initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor, enabled: active, refetchInterval: active ? 15000 : false, retry: false });
  const client = useQueryClient();
  const refreshDirectory = useCallback(() => { void client.invalidateQueries({ queryKey: ['agent-sessions'] }); }, [client]);
  const select = (sid: string, iid: string | null) => navigate({ view: 'agents', session: sid, execution: iid, workflow: null, revision: null, selection: null });
  const agent = catalog.data?.agent;
  const rows = sessions.data?.pages.flatMap(page => page.items).filter(item => item.title.toLowerCase().includes(search.toLowerCase())) ?? [];
  return <div className={`workspace workspace-tab agent-workspace ${active ? '' : 'is-inactive'}`} data-workspace-view="agents" aria-hidden={!active} inert={!active}>
    <Sidebar view="agents" onChange={changeView}>
      <div className="directory-title execution-directory-heading"><span>Conversations</span><button className="history-refresh" aria-label="New conversation" title="New conversation" onClick={() => select(newId(), null)}><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" aria-hidden="true"><path d="M12 5v14M5 12h14" /></svg></button></div>
      <div className="directory-search"><div className="search-field"><SearchIcon /><input type="search" aria-label="Filter loaded conversations" placeholder="Filter conversations…" value={search} onChange={event => setSearch(event.target.value)} /></div></div>
      <div className="directory-content" aria-busy={sessions.isLoading}>
        {!sessions.isLoading ? <ul className="workflow-list">{rows.map(item => <li className="workflow-group" key={item.session_id}><button className={`workflow-item session-item ${session === item.session_id ? 'active' : ''}`} aria-current={session === item.session_id ? 'true' : undefined} title={item.title} onClick={() => select(item.session_id, item.invocation_id)}><span className="workflow-glyph"><SessionIcon /></span><span className="workflow-item-text"><strong>{item.title}</strong><span className="workflow-item-meta">{item.invocation_count} {item.invocation_count === 1 ? 'turn' : 'turns'}</span></span></button></li>)}</ul> : null}
        {sessions.isLoading ? <DirectorySkeleton label="Loading conversations" kind="conversation" /> : sessions.error ? <div className="sidebar-error" role="alert"><p>{sessions.error.message}</p><button onClick={() => void sessions.refetch()}>Retry</button></div> : !rows.length ? <p className="sidebar-message">{search ? 'No matching conversations.' : 'Start a conversation to see it here.'}</p> : null}
        {sessions.hasNextPage ? <button className="load-more" disabled={sessions.isFetchingNextPage} onClick={() => void sessions.fetchNextPage()}><BusyLabel busy={sessions.isFetchingNextPage} label="Loading more">Load more conversations</BusyLabel></button> : null}
      </div>
    </Sidebar>
    <div className="agent-app">{catalog.error ? <div className="agent-empty"><h2>Could not load chat</h2><p>{catalog.error.message}</p><button onClick={() => void catalog.refetch()}>Retry</button></div>
      : !catalog.data ? <div className="agent-feed-shell"><div className="agent-feed"><ConversationSkeleton /></div></div>
        : !agent ? <div className="agent-empty"><h2>Chat is disabled</h2><p>Enable the SDK Agent for this project to start conversations.</p></div>
          : <Suspense fallback={<div className="agent-feed-shell"><div className="agent-feed"><ConversationSkeleton /></div></div>}><AgentSession key={session} session={session} invocation={location.execution} agent={agent} catalog={catalog.data} active={active} onAccepted={iid => select(session, iid)} onAgent={item => select(item.session_id, item.invocation_id)} onTrace={onTrace} onRefreshDirectory={refreshDirectory} /></Suspense>}
    </div>
  </div>;
}
