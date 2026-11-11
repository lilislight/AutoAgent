import { useCallback, useEffect, useId, useState } from 'react';
import { useInfiniteQuery } from '@tanstack/react-query';
import { BusyLabel, SkeletonText } from '../components/LoadingState.js';
import { Tooltip } from '../components/Tooltip.js';
import { AnimatedRegion, Chevron } from './AgentDisclosure.js';
import { AgentIcon } from './AgentIcon.js';
import type { AgentSession, AgentWorkspaceInfo, Page } from './types.js';

/** null selects the global Recent list. */
export type SessionLoader = (workspace: string | null, cursor: string | null, signal: AbortSignal) => Promise<Page<AgentSession>>;

const CONVERSATIONS_PER_BATCH = 5;

function useDisclosure(scope: string, initial: boolean) {
  const key = `autoagent:directory:${scope}`;
  const [open, setOpen] = useState(() => {
    try { const saved = localStorage.getItem(key); return saved === null ? initial : saved === 'open'; }
    catch { return initial; }
  });
  const change = useCallback((value: boolean) => {
    setOpen(value);
    try { localStorage.setItem(key, value ? 'open' : 'closed'); } catch { /* Optional browser preference. */ }
  }, [key]);
  return [open, change] as const;
}

function SessionSkeleton({ count = 3 }: { count?: number }) {
  return <div role="status" aria-label="Loading conversations">{Array.from({ length: count }, (_, index) =>
    <div key={index} aria-hidden="true" className="agent-group-session is-skeleton"><SkeletonText width={index % 2 ? '68%' : '86%'} /></div>)}</div>;
}

export function WorkspaceGroup({ item, selected, session, active, load, onSelect, onNew, onTitle, duplicateName = false,
  workspaces = [], storageScope = '', canCreate = true }: {
  item: AgentWorkspaceInfo | null; selected: boolean; session: string | null; active: boolean;
  load: SessionLoader; onSelect: (row: AgentSession, workspace: string | null) => void;
  onNew: (workspace: string | null) => void; onTitle: (session: string, title: string) => void; duplicateName?: boolean;
  workspaces?: AgentWorkspaceInfo[]; storageScope?: string; canCreate?: boolean;
}) {
  const scope = item?.id ?? null, name = item?.name ?? 'Recent', id = useId();
  const [visibleCount, setVisibleCount] = useState(CONVERSATIONS_PER_BATCH);
  const [open, setOpen] = useDisclosure(`${storageScope}:${scope ?? 'recent'}`, selected || item === null);
  const query = useInfiniteQuery({ queryKey: ['agent-sessions', scope], queryFn: ({ pageParam, signal }) => load(scope, pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    enabled: active && open, staleTime: 15000, refetchInterval: active && open ? 15000 : false, retry: false });
  const rows = query.data?.pages.flatMap(page => page.items) ?? [];
  const visibleRows = rows.slice(0, visibleCount);
  const showMore = async () => {
    if (query.isFetchingNextPage) return;
    if (visibleCount >= rows.length && query.hasNextPage) {
      const result = await query.fetchNextPage();
      if (result.isError) return;
    }
    setVisibleCount(count => count + CONVERSATIONS_PER_BATCH);
  };
  const title = rows.find(row => row.session_id === session)?.title;
  useEffect(() => { if (selected && scope !== null) setOpen(true); }, [selected, scope, setOpen]);
  useEffect(() => { if (session && title) onTitle(session, title); }, [session, title, onTitle]);
  const description = item ? `${item.path ?? item.id}${item.is_project ? ' · Current project' : ''}${item.available === false ? ' · Unavailable' : ''}` : 'All conversations, ordered by recent activity';
  return <section className={`agent-workspace-group ${item === null ? 'is-recent' : ''} ${selected ? 'is-active' : ''}`} aria-label={item ? `${name} workspace` : 'Recent conversations'}>
    <div className="agent-workspace-group-heading">
      <Tooltip content={description}><button type="button" className="agent-workspace-group-toggle" aria-label={`${open ? 'Collapse' : 'Expand'} ${name}`} aria-expanded={open} aria-controls={id} onClick={() => setOpen(!open)}>
        {item ? <span className={`agent-workspace-folder ${item.is_project ? 'is-current' : ''}`}><AgentIcon name="folder" /></span> : <Chevron />}
        <span className="agent-workspace-group-name">{name}{duplicateName && item?.path ? <small>{item.path}</small> : null}</span>
        {item ? <Chevron /> : null}
      </button></Tooltip>
      <Tooltip content={item ? `New conversation in ${name}` : 'New conversation without a workspace'}><button type="button" className="agent-workspace-new" aria-label={item ? `New conversation in ${name}` : 'New conversation without a workspace'} disabled={!canCreate || item?.available === false} onClick={() => { setOpen(true); onNew(scope); }}><AgentIcon name="new" /></button></Tooltip>
    </div>
    <AnimatedRegion id={id} open={open}>
      {item?.available === false ? <p className="agent-group-notice">Workspace unavailable</p> : null}
      {query.isLoading ? <SessionSkeleton /> : null}
      <ul className="agent-group-sessions">{visibleRows.map(row => {
        const bound = row.workspace_id ? workspaces.find(value => value.id === row.workspace_id) : undefined;
        return <li key={row.session_id}><Tooltip content={row.title}><button type="button" className={`agent-group-session ${row.session_id === session ? 'is-selected' : ''}`} aria-current={row.session_id === session ? 'page' : undefined} onClick={() => onSelect(row, item?.id ?? row.workspace_id ?? null)}>
          <span className="agent-session-title">{row.title}</span>
          {!item && row.workspace_id ? <Tooltip content={bound ? `${bound.name} · ${bound.path ?? bound.id}` : row.workspace_id}><span className="agent-recent-workspace" role="img" aria-label={`Workspace: ${bound?.name ?? row.workspace_id}`}><AgentIcon name="folder" /></span></Tooltip> : null}
        </button></Tooltip></li>;
      })}</ul>
      {query.error ? <div className="agent-group-error" role="alert"><p>{query.error.message}</p><button type="button" onClick={() => void query.refetch()}>Retry</button></div> : null}
      {query.isSuccess && !rows.length ? <p className="agent-group-notice">No conversations yet.</p> : null}
      {rows.length > visibleCount || query.hasNextPage ? <button type="button" className="agent-group-load-more" aria-label={`Show more ${item ? name + ' conversations' : 'recent conversations'}`} disabled={query.isFetchingNextPage} onClick={() => void showMore()}><BusyLabel busy={query.isFetchingNextPage} label="Loading more">Show more<AgentIcon name="chevron" /></BusyLabel></button> : null}
    </AnimatedRegion>
  </section>;
}

export function ConversationDirectory({ items, loading, error, onRetry, workspace, session, active, load, onSelect, onNew, onTitle,
  onAddWorkspace, storageScope = '', canCreate = true }: {
  items?: AgentWorkspaceInfo[]; loading: boolean; error?: Error | null; onRetry: () => void;
  workspace: string | null; session: string | null; active: boolean; load: SessionLoader;
  onSelect: (row: AgentSession, workspace: string | null) => void; onNew: (workspace: string | null) => void;
  onTitle: (session: string, title: string) => void; onAddWorkspace: () => void; storageScope?: string; canCreate?: boolean;
}) {
  const [open, setOpen] = useDisclosure(`${storageScope}:workspaces`, true), id = useId();
  const groups = items ?? [];
  const missing = items && workspace && !groups.some(item => item.id === workspace) ? { id: workspace, name: 'Unavailable workspace', backend: 'unknown', path: null, is_project: false, available: false } satisfies AgentWorkspaceInfo : null;
  const all = [...groups, ...(missing ? [missing] : [])];
  return <div className="directory-content agent-group-directory">
    <section aria-label="Workspaces">
      <div className="agent-directory-heading"><button type="button" className="agent-section-toggle" aria-label={`${open ? 'Collapse' : 'Expand'} Workspaces`} aria-expanded={open} aria-controls={id} onClick={() => setOpen(!open)}><Chevron /><span>Workspaces</span></button>
        <Tooltip content="Add workspace"><button type="button" className="agent-workspace-new" aria-label="Add workspace" onClick={onAddWorkspace}><AgentIcon name="folder-add" /></button></Tooltip>
      </div>
      <AnimatedRegion id={id} open={open}>
        {loading ? <div className="agent-workspace-group" role="status" aria-label="Loading workspaces"><div className="agent-workspace-group-heading agent-workspace-group-toggle" aria-hidden="true"><span className="agent-workspace-folder"><AgentIcon name="folder" /></span><SkeletonText width="110px" /></div><SessionSkeleton /></div> : null}
        {error ? <div className="sidebar-error" role="alert"><p>{error.message}</p><button type="button" onClick={onRetry}>Retry</button></div> : null}
        {all.map(item => <WorkspaceGroup key={item.id} item={item} selected={workspace === item.id} session={session} active={active && open} load={load} onSelect={onSelect} onNew={onNew} onTitle={onTitle} storageScope={storageScope} canCreate={canCreate} duplicateName={groups.filter(other => other.name === item.name).length > 1} />)}
      </AnimatedRegion>
    </section>
    <WorkspaceGroup item={null} selected={false} session={session} active={active} load={load} onSelect={onSelect} onNew={onNew} onTitle={onTitle} storageScope={storageScope} canCreate={canCreate} workspaces={all} />
  </div>;
}
