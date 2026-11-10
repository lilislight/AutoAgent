import { useEffect, useId, useState } from 'react';
import { useInfiniteQuery } from '@tanstack/react-query';
import { BusyLabel, LoadingIndicator, SkeletonText } from '../components/LoadingState.js';
import { SearchIcon } from '../components/Sidebar.js';
import { Tooltip } from '../components/Tooltip.js';
import { AnimatedRegion, Chevron } from './AgentDisclosure.js';
import { AgentIcon } from './AgentIcon.js';
import type { AgentSession, AgentWorkspaceInfo, Page } from './types.js';

export type SessionLoader = (workspace: string | null, cursor: string | null, signal: AbortSignal) => Promise<Page<AgentSession>>;

function SessionSkeleton({ count = 3 }: { count?: number }) {
  return <div role="status" aria-label="Loading conversations">
    {Array.from({ length: count }, (_, index) => <div key={index} aria-hidden="true" className="agent-group-session is-skeleton"><SkeletonText width={index % 2 ? '68%' : '86%'} /></div>)}
  </div>;
}

export function WorkspaceGroup({ item, selected, session, active, search, load, onSelect, onNew, onTitle, duplicateName = false }: {
  item: AgentWorkspaceInfo | null; selected: boolean; session: string | null; active: boolean; search: string;
  load: SessionLoader; onSelect: (row: AgentSession, workspace: string | null) => void;
  onNew: (workspace: string) => void; onTitle: (session: string, title: string) => void; duplicateName?: boolean;
}) {
  const [open, setOpen] = useState(selected || item === null), id = useId();
  const scope = item?.id ?? null, searching = !!search.trim(), expanded = open;
  const name = item?.name ?? 'Recent';
  const query = useInfiniteQuery({ queryKey: ['agent-sessions', scope], queryFn: ({ pageParam, signal }) => load(scope, pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    enabled: active && (expanded || selected || searching), staleTime: 15000, refetchInterval: active && expanded ? 15000 : false, retry: false });
  const rows = query.data?.pages.flatMap(page => page.items) ?? [];
  const selectedTitle = rows.find(row => row.session_id === session)?.title;
  useEffect(() => { if (selected) setOpen(true); }, [selected]);
  useEffect(() => { if (searching) setOpen(true); }, [searching]);
  useEffect(() => { if (session && selectedTitle) onTitle(session, selectedTitle); }, [session, selectedTitle, onTitle]);
  if (item === null && query.isSuccess && !rows.length && !selected && !searching) return null;
  const matches = rows.filter(row => row.title.toLocaleLowerCase().includes(search.trim().toLocaleLowerCase()));
  const description = item ? `${item.path ?? item.id}${item.is_project ? ' · Current project' : ''}${item.available === false ? ' · Unavailable' : ''}` : 'Conversations without a workspace';
  return <section className={`agent-workspace-group ${item === null ? 'is-unbound' : ''} ${selected ? 'is-active' : ''}`} aria-label={item ? `${name} workspace` : 'Conversations without a workspace'}>
    <div className="agent-workspace-group-heading">
      <Tooltip content={description}><button type="button" className="agent-workspace-group-toggle" aria-label={`${expanded ? 'Collapse' : 'Expand'} ${name}`} aria-expanded={expanded} aria-controls={id} onClick={() => setOpen(value => !value)}>
        {item ? <span className={`agent-workspace-folder ${item.is_project ? 'is-current' : ''}`}><AgentIcon name="folder" /></span> : null}
        <span className="agent-workspace-group-name">{name}{duplicateName && item?.path ? <small>{item.path}</small> : null}</span>
        <Chevron />
      </button></Tooltip>
      {query.isFetching && query.data && !query.isFetchingNextPage ? <LoadingIndicator label={`Updating ${name} conversations`} small /> : null}
      {item ? <Tooltip content={`New conversation in ${name}`}><button type="button" className="agent-workspace-new" aria-label={`New conversation in ${name}`} disabled={item.available === false} onClick={() => { setOpen(true); onNew(item.id); }}><AgentIcon name="new" /></button></Tooltip> : null}
    </div>
    <AnimatedRegion id={id} open={expanded}>
      {item?.available === false ? <p className="agent-group-notice">Workspace unavailable</p> : null}
      {query.isLoading ? <SessionSkeleton /> : null}
      <ul className="agent-group-sessions">{matches.map(row => <li key={row.session_id}><Tooltip content={row.title}><button type="button" className={`agent-group-session ${row.session_id === session ? 'is-selected' : ''}`} aria-current={row.session_id === session ? 'page' : undefined} onClick={() => onSelect(row, scope)}><span>{row.title}</span></button></Tooltip></li>)}</ul>
      {query.error ? <div className="agent-group-error" role="alert"><p>{query.error.message}</p><button type="button" onClick={() => void query.refetch()}>Retry</button></div> : null}
      {query.isSuccess && !matches.length ? <p className="agent-group-notice">{searching ? 'No matching conversations.' : 'No conversations yet.'}</p> : null}
      {query.hasNextPage ? <button type="button" className="agent-group-load-more" disabled={query.isFetchingNextPage} onClick={() => void query.fetchNextPage()}><BusyLabel busy={query.isFetchingNextPage} label="Loading more">Load more</BusyLabel></button> : null}
    </AnimatedRegion>
  </section>;
}

export function ConversationDirectory({ items, loading, error, fetching, onRetry, workspace, session, active, load, onSelect, onNew, onTitle }: {
  items?: AgentWorkspaceInfo[]; loading: boolean; error?: Error | null; fetching: boolean; onRetry: () => void;
  workspace: string | null; session: string | null; active: boolean; load: SessionLoader;
  onSelect: (row: AgentSession, workspace: string | null) => void; onNew: (workspace: string) => void;
  onTitle: (session: string, title: string) => void;
}) {
  const [search, setSearch] = useState('');
  const groups = items ?? [];
  const missing = items && workspace && !groups.some(item => item.id === workspace) ? { id: workspace, name: 'Unavailable workspace', backend: 'unknown', path: null, is_project: false, available: false } satisfies AgentWorkspaceInfo : null;
  return <>
    <div className="directory-search"><div className="search-field"><SearchIcon /><input type="search" aria-label="Filter loaded conversations" placeholder="Search conversations…" value={search} onChange={event => setSearch(event.target.value)} /></div></div>
    <div className="agent-directory-heading"><span>Workspaces</span>{fetching && items ? <LoadingIndicator label="Updating workspaces" small /> : null}</div>
    <div className="directory-content agent-group-directory" aria-busy={loading}>
      {loading ? <div className="agent-workspace-group" role="status" aria-label="Loading workspaces"><div className="agent-workspace-group-heading agent-workspace-group-toggle" aria-hidden="true"><span className="agent-workspace-folder"><AgentIcon name="folder" /></span><SkeletonText width="110px" /></div><SessionSkeleton /></div> : null}
      {error ? <div className="sidebar-error" role="alert"><p>{error.message}</p><button type="button" onClick={onRetry}>Retry</button></div> : null}
      {[...groups, ...(missing ? [missing] : [])].map(item => <WorkspaceGroup key={item.id} item={item} selected={workspace === item.id} session={session} active={active} search={search} load={load} onSelect={onSelect} onNew={onNew} onTitle={onTitle} duplicateName={groups.filter(other => other.name === item.name).length > 1} />)}
      <WorkspaceGroup item={null} selected={!!session && workspace === null} session={session} active={active} search={search} load={load} onSelect={onSelect} onNew={onNew} onTitle={onTitle} />
    </div>
  </>;
}
