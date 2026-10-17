import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { listExecutionSessions, listSessionInvocations } from "../api";
import type { AcceptedExecution, ExecutionFilters } from "../types";
import { SessionIcon, WorkflowIcon } from "./Sidebar";
import { ExecutionFilterDialog, EMPTY_EXECUTION_FILTERS, executionFilterCount } from "./ExecutionFilters";
import { RunSessionPopover } from './RunSessionPopover';
import { StatusBadge, formatTime } from "./TraceDetails";

function useCursorPage<T>(key: unknown[], fetch: (cursor: string | null, signal: AbortSignal) => Promise<T & { next_cursor: string | null }>) {
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [index, setIndex] = useState(0);
  const [generation, setGeneration] = useState(0);
  const query = useQuery({ queryKey: [...key, generation, cursors[index]], queryFn: ({ signal }) => fetch(cursors[index], signal), staleTime: 10_000, gcTime: 60_000, refetchOnWindowFocus: false });
  const next = () => {
    const cursor = query.data?.next_cursor;
    if (!cursor || query.isFetching) return;
    setCursors(current => [...current.slice(0, index + 1), cursor]); setIndex(index + 1);
  };
  return { query, index, cursor: cursors[index], next, refresh: () => { setCursors([null]); setIndex(0); setGeneration(current => current + 1); }, previous: () => setIndex(current => Math.max(0, current - 1)) };
}

function HistoryPanel({ title, count, page, onRefresh, children, action }: { action?: ReactNode; title: string; count: ReactNode; onRefresh?: () => void; page: { index: number; cursor: string | null; next: () => void; previous: () => void; fetching: boolean; hasNext: boolean }; children: ReactNode }) {
  const scroll = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => { scroll.current?.scrollTo({ top: 0 }); }, [page.cursor]);
  const refresh = () => { scroll.current?.scrollTo({ top: 0 }); onRefresh?.(); };
  return <><div className="history-section-heading"><strong>{title}</strong><div className="history-heading-actions"><span>{count}</span>{onRefresh ? <button type="button" className={`history-refresh ${page.fetching ? "is-refreshing" : ""}`} aria-label={`Refresh ${title.toLowerCase()}`} title={`Refresh ${title.toLowerCase()}`} disabled={page.fetching} onClick={refresh}><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><path d="M4 10a8 8 0 1 1 1.6 8M4 4v6h6" /></svg></button> : null}{action}</div></div>
    <div ref={scroll} className="history-scroll">{children}</div>
    <div className="history-pagination" aria-label={`${title} pagination`}><button aria-label={`Previous ${title.toLowerCase()} page`} disabled={!page.index || page.fetching} onClick={page.previous}>‹</button><span>Page {page.index + 1}</span><button aria-label={`Next ${title.toLowerCase()} page`} disabled={!page.hasNext || page.fetching} onClick={page.next}>›</button></div></>;
}
function Sessions({ filters, session, onSession, onRefresh, action }: { action: ReactNode; filters: ExecutionFilters; session: string | null; onSession: (id: string) => void; onRefresh: () => void }) {
  const page = useCursorPage(["execution-sessions-page", filters], (cursor, signal) => listExecutionSessions(cursor, signal, undefined, filters));
  const { query } = page;
  const rows = query.data?.items ?? [];
  return <HistoryPanel action={action} title="Sessions" count={query.data?.total_count ?? '—'} onRefresh={() => { page.refresh(); onRefresh(); }} page={{ ...page, fetching: query.isFetching, hasNext: Boolean(query.data?.next_cursor) }}>
    {query.isPending ? <p className="sidebar-message" role="status">Loading sessions…</p> : null}
    {query.error ? <div className="sidebar-error"><p>{query.error.message}</p><button onClick={() => void query.refetch()}>Retry</button></div> : null}
    {!query.isPending && !query.error && !rows.length ? <p className="sidebar-message">{executionFilterCount(filters) ? "No sessions match these filters." : "No saved sessions yet"}</p> : null}
    <ul className="workflow-list">{rows.map(row => <li key={row.session_id}><button className={`workflow-item session-item ${row.session_id === session ? "active" : ""}`} aria-current={row.session_id === session ? "true" : undefined} title={row.session_id} onClick={() => onSession(row.session_id)}><span className="workflow-glyph"><SessionIcon /></span><span className="workflow-item-text"><strong>{row.session_id}</strong><span className="workflow-item-meta">{row.invocation_count} {row.invocation_count === 1 ? "invocation" : "invocations"} · {row.workflow_count} {row.workflow_count === 1 ? "workflow" : "workflows"}</span><small title="Most recent persisted execution event">Last activity · {formatTime(row.updated_at_us)}</small>{executionFilterCount(filters) ? <small className="session-match-note">{row.matching_count} matching {row.matching_count === 1 ? "invocation" : "invocations"}</small> : null}</span></button></li>)}</ul>
  </HistoryPanel>;
}
function Invocations({ filters, session, invocation, count, onInvocation, action }: { action: ReactNode; filters: ExecutionFilters; session: string; invocation: string | null; count: number | undefined; onInvocation: (id: string, session: string) => void }) {
  const page = useCursorPage(["session-invocations-page", session, filters], (cursor, signal) => listSessionInvocations(session, cursor, signal, filters));
  const { query } = page;
  return <HistoryPanel action={action} title="Invocations" count={count ?? "—"} page={{ ...page, fetching: query.isFetching, hasNext: Boolean(query.data?.next_cursor) }}>
    {query.isPending ? <p className="sidebar-message" role="status">Loading invocations…</p> : null}
    {query.error ? <div className="sidebar-error"><p>{query.error.message}</p><button onClick={() => void query.refetch()}>Retry</button></div> : null}
    {!query.isPending && !query.error && !query.data?.items.length ? <p className="sidebar-message">{executionFilterCount(filters) ? "No invocations match these filters." : "No saved invocations"}</p> : null}
    <ul className="workflow-list">{query.data?.items.map(row => <li key={row.invocation_id}><button className={`workflow-item execution-item ${row.invocation_id === invocation ? "active" : ""}`} aria-current={row.invocation_id === invocation ? "true" : undefined} title={row.invocation_id} onClick={() => onInvocation(row.invocation_id, session)}><span className="workflow-glyph"><WorkflowIcon /></span><span className="workflow-item-text"><span className="invocation-row-heading"><strong>{row.workflow_id}</strong><StatusBadge row={row} /></span><span className="invocation-row-meta"><code>{row.invocation_id.slice(0, 8)}</code><span title="Invocation start time">Started · {formatTime(row.started_at_us)}</span></span></span></button></li>)}</ul>
  </HistoryPanel>;
}
export function ExecutionDirectory({ session, invocation, onSession, onInvocation, onRunAccepted }: { onRunAccepted: (receipt: AcceptedExecution) => void; session: string | null; invocation: string | null; onSession: (id: string) => void; onInvocation: (id: string, session: string) => void }) {
  const queryClient = useQueryClient();
  const [run, setRun] = useState<'new' | 'existing' | null>(null);
  const newButton = useRef<HTMLButtonElement>(null), invokeButton = useRef<HTMLButtonElement>(null);
  const create = <button ref={newButton} type="button" className="history-refresh history-create" aria-label="New Session and Run" title="New Session and Run" onClick={() => setRun('new')}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><path d="M8 2v12M2 8h12" /></svg></button>;
  const invoke = <button ref={invokeButton} type="button" className="history-refresh history-create" aria-label="Run in this Session" title="Run in this Session" onClick={() => setRun('existing')}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><path d="M8 2v12M2 8h12" /></svg></button>;
  const [filtersOpen, setFiltersOpen] = useState(false);
  const filterButton = useRef<HTMLButtonElement>(null);
  const [filters, setFilters] = useState(EMPTY_EXECUTION_FILTERS);
  const filterCount = executionFilterCount(filters);
  const [split, setSplit] = useState(.4);
  const panes = useRef<HTMLDivElement>(null);
  const dragging = useRef(false);
  const selectedSession = useQuery({ queryKey: ["execution-session", session, filters], queryFn: ({ signal }) => listExecutionSessions(null, signal, session!, filters), enabled: Boolean(session), staleTime: 10_000, gcTime: 60_000, refetchOnWindowFocus: false });
  const selected = selectedSession.data?.items[0];
  const selectSession = (id: string) => {
    void queryClient.invalidateQueries({ queryKey: ["session-invocations-page", id] });
    void queryClient.invalidateQueries({ queryKey: ["execution-session", id] });
    onSession(id);
  };
  const scope = JSON.stringify(filters);
  const resize = (clientY: number) => {
    const bounds = panes.current?.getBoundingClientRect();
    if (!bounds) return;
    const minimum = Math.max(.2, Math.min(.4, 100 / bounds.height));
    setSplit(Math.max(minimum, Math.min(1 - minimum, (clientY - bounds.top) / bounds.height)));
  };
  return <div className="execution-directory">
    <div className="execution-directory-heading"><span>Executions</span><div className="filter-controls"><button ref={filterButton} className={`filter-button ${filterCount ? "has-filters" : ""}`} aria-haspopup="dialog" aria-expanded={filtersOpen} onClick={event => { event.currentTarget.focus(); setFiltersOpen(true); }}><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" aria-hidden="true"><path d="M4 6h4m4 0h8M4 12h10m4 0h2M4 18h3m4 0h9" /><circle cx="10" cy="6" r="2" /><circle cx="16" cy="12" r="2" /><circle cx="9" cy="18" r="2" /></svg>Filter{filterCount ? <span className="filter-count">{filterCount}</span> : null}</button>{filterCount ? <button type="button" className="filter-reset" aria-label="Clear execution filters" title="Clear filters" onClick={() => { setFilters(EMPTY_EXECUTION_FILTERS); filterButton.current?.focus({ preventScroll: true }); }}>×</button> : null}</div></div>
    {filtersOpen ? <ExecutionFilterDialog filters={filters} anchor={filterButton} onApply={setFilters} onClose={() => setFiltersOpen(false)} /> : null}
    {run ? <RunSessionPopover anchor={run === 'new' ? newButton : invokeButton} session={run === 'existing' ? session ?? undefined : undefined} onClose={() => setRun(null)} onAccepted={onRunAccepted} /> : null}
    <div className="history-panes" ref={panes}>
      <section className="history-section session-section" aria-label="Sessions" style={{ flex: split }}><Sessions action={create} key={scope} filters={filters} session={session} onSession={selectSession} onRefresh={() => { if (session) void selectedSession.refetch(); }} /></section>
      <div className="history-divider" role="separator" tabIndex={0} aria-label="Resize sessions and invocations" aria-orientation="horizontal" aria-valuemin={20} aria-valuemax={80} aria-valuenow={Math.round(split * 100)} onPointerDown={event => { if (event.button !== 0) return; dragging.current = true; event.currentTarget.setPointerCapture(event.pointerId); event.preventDefault(); }} onPointerMove={event => { if (dragging.current) resize(event.clientY); }} onPointerUp={event => { dragging.current = false; event.currentTarget.releasePointerCapture(event.pointerId); }} onLostPointerCapture={() => { dragging.current = false; }} onKeyDown={event => { if (event.key === "ArrowUp" || event.key === "ArrowDown") { event.preventDefault(); setSplit(current => Math.max(.2, Math.min(.8, current + (event.key === "ArrowUp" ? -.05 : .05)))); } }}><span /></div>
      <section className="history-section invocation-section" aria-label="Invocations" style={{ flex: 1 - split }}>{session ? <Invocations action={invoke} key={`${session}:${scope}`} session={session} filters={filters} invocation={invocation} count={filterCount ? selected?.matching_count : selected?.invocation_count} onInvocation={onInvocation} /> : <><div className="history-section-heading"><strong>Invocations</strong><span>0</span></div><p className="sidebar-message">Select a session to view its invocations.</p></>}</section>
    </div>
  </div>;
}
