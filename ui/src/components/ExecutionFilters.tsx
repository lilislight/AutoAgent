import { OptionsSkeleton, BusyLabel } from './LoadingState';
import { useLayoutEffect, useRef, useState, type RefObject } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { browseWorkflows, listWorkflowRevisions } from "../api";
import type { ExecutionFilters } from "../types";
import { SearchIcon, WorkflowIcon } from "./Sidebar";
import { EnglishDateTimeInput } from './EnglishDateTimeInput';
import { parseLocalDateTime } from '../localDateTime';

export const EMPTY_EXECUTION_FILTERS: ExecutionFilters = { query: "", workflowIds: [], revisionIds: [], statuses: [], startedFrom: '', startedTo: '' };
export const executionFilterCount = (filters: ExecutionFilters) => Number(Boolean(filters.query)) + filters.workflowIds.length + filters.revisionIds.length + filters.statuses.length + Number(Boolean(filters.startedFrom || filters.startedTo));
const INVOCATION_STATUSES = ['created', 'running', 'waiting', 'paused', 'settling', 'completed', 'failed', 'cancelled'];
const toggleId = (ids: string[], id: string) => ids.includes(id) ? ids.filter(value => value !== id) : [...ids, id];
const revisionWorkflow = (id: string) => id.slice(0, id.lastIndexOf(":"));

function RevisionOptions({ workflowId, selected, onToggle }: {
  workflowId: string; selected: string[]; onToggle: (id: string) => void;
}) {
  const revisions = useInfiniteQuery({
    queryKey: ["filter-revisions", workflowId, ""],
    queryFn: ({ pageParam, signal }) => listWorkflowRevisions(workflowId, null, pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    staleTime: 60_000, gcTime: 60_000, refetchOnWindowFocus: false,
  });
  const rows = revisions.data?.pages.flatMap(page => page.items) ?? [];
  return <div className="filter-revision-group" aria-busy={revisions.isFetching}>
    <div className="filter-revision-heading" title={workflowId}><WorkflowIcon /><span>{workflowId}</span></div>
    {revisions.isLoading ? <OptionsSkeleton label="Loading revisions" variant="filter" /> : null}
    {revisions.error ? <p className="filter-hint">{revisions.error.message} <button type="button" className="text-button" onClick={() => void revisions.refetch()}>Retry</button></p> : null}
    {!revisions.isPending && !revisions.error && !rows.length ? <p className="filter-hint">No saved revisions.</p> : null}
    {rows.map(row => <label className="filter-option" key={row.revision_id} title={row.revision_id}>
      <input type="checkbox" checked={selected.includes(row.revision_id)} disabled={!selected.includes(row.revision_id) && selected.length >= 64} onChange={() => onToggle(row.revision_id)} />
      <span className="filter-revision-label"><span>v{row.workflow_version}</span><code>{row.definition_hash?.slice(0, 12) ?? row.revision_id.split(":").at(-1)?.slice(0, 12)}</code></span>
    </label>)}
    {revisions.hasNextPage ? <button type="button" className="load-more" disabled={revisions.isFetchingNextPage} onClick={() => void revisions.fetchNextPage()}><BusyLabel busy={revisions.isFetchingNextPage} label="Loading more">More revisions</BusyLabel></button> : null}
  </div>;
}

export function ExecutionFilterDialog({ filters, anchor, onApply, onClose }: {
  filters: ExecutionFilters; anchor: RefObject<HTMLButtonElement | null>;
  onApply: (value: ExecutionFilters) => void; onClose: () => void;
}) {
  const [draft, setDraft] = useState(filters);
  const invalidTime = [draft.startedFrom, draft.startedTo].some(value => {
    if (!value) return false;
    const date = parseLocalDateTime(value);
    return !date || date.getTime() < 0 || !Number.isSafeInteger(date.getTime() * 1000);
  });
  const invalidRange = Boolean(draft.startedFrom && draft.startedTo && Date.parse(draft.startedFrom) > Date.parse(draft.startedTo));
  const [expanded, setExpanded] = useState<"workflows" | "revisions" | "statuses" | null>(null);
  const dialog = useRef<HTMLDialogElement>(null);
  const searchInput = useRef<HTMLInputElement>(null);
  const workflows = useInfiniteQuery({
    queryKey: ["workflow-catalog", ""],
    queryFn: ({ pageParam, signal }) => browseWorkflows("", pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    enabled: expanded === "workflows", staleTime: 60_000, gcTime: 60_000, refetchOnWindowFocus: false,
  });
  const rows = [...new Map(workflows.data?.pages.flatMap(page => page.items).map(row => [row.workflow_id, row])).values()];
  useLayoutEffect(() => {
    const popup = dialog.current;
    const button = anchor.current;
    if (!popup || !button) return;
    const previous = document.activeElement;
    const position = () => {
      const bounds = button.getBoundingClientRect();
      const left = Math.max(12, Math.min(bounds.right + 10, window.innerWidth - popup.offsetWidth - 12));
      const top = Math.max(12, Math.min(bounds.top, window.innerHeight - popup.offsetHeight - 12));
      popup.style.left = `${left}px`;
      popup.style.top = `${top}px`;
    };
    popup.showModal();
    position();
    searchInput.current?.focus({ preventScroll: true });
    const observer = new ResizeObserver(position);
    observer.observe(popup);
    observer.observe(button);
    window.addEventListener("resize", position);
    window.addEventListener("scroll", position, true);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", position);
      window.removeEventListener("scroll", position, true);
      popup.close();
      if (previous instanceof HTMLElement && previous.isConnected) previous.focus({ preventScroll: true });
    };
  }, [anchor]);
  const toggleWorkflow = (id: string) => setDraft(current => ({ ...current,
    workflowIds: toggleId(current.workflowIds, id),
    revisionIds: current.workflowIds.includes(id) ? current.revisionIds.filter(revision => revisionWorkflow(revision) !== id) : current.revisionIds,
  }));
  return <dialog ref={dialog} className="execution-filter-dialog" aria-label="Execution filters" onCancel={event => { event.preventDefault(); onClose(); }} onKeyDown={event => {
    if (event.key === "Escape" && expanded) { event.preventDefault(); event.stopPropagation(); setExpanded(null); }
  }} onClick={event => {
    if (event.target !== event.currentTarget) return;
    const bounds = event.currentTarget.getBoundingClientRect();
    if (event.clientX < bounds.left || event.clientX > bounds.right || event.clientY < bounds.top || event.clientY > bounds.bottom) onClose();
  }}><form onSubmit={event => {
    event.preventDefault();
    if (invalidRange || invalidTime) return;
    onApply({ ...draft, query: draft.query.trim().toLowerCase(), workflowIds: [...draft.workflowIds].sort(), revisionIds: [...draft.revisionIds].sort(), statuses: [...draft.statuses].sort() });
    onClose();
  }}>
    <div className="filter-search"><SearchIcon /><input ref={searchInput} type="search" aria-label="Search session, invocation, workflow or revision" placeholder="Session, invocation, workflow, revision…" maxLength={512} value={draft.query} onChange={event => setDraft(current => ({ ...current, query: event.target.value }))} /></div>
    <div className="filter-select">
      <button type="button" className="filter-select-trigger" aria-expanded={expanded === "workflows"} aria-controls="filter-workflow-options" onClick={() => setExpanded(current => current === "workflows" ? null : "workflows")}><span>Workflows</span><small>{draft.workflowIds.length ? `${draft.workflowIds.length} selected` : "All"}</small><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m4 6 4 4 4-4" /></svg></button>
      {expanded === "workflows" ? <div className="filter-options" id="filter-workflow-options" role="group" aria-label="Workflow choices" aria-busy={workflows.isFetching}>
        {workflows.isLoading ? <OptionsSkeleton label="Loading workflows" variant="filter" /> : null}
        {workflows.error ? <p className="filter-hint">{workflows.error.message} <button type="button" className="text-button" onClick={() => void workflows.refetch()}>Retry</button></p> : null}
        {!workflows.isPending && !workflows.error && !rows.length ? <p className="filter-hint">No saved workflows.</p> : null}
        {rows.map(row => <label className="filter-option" key={row.workflow_id} title={row.workflow_id}><input type="checkbox" checked={draft.workflowIds.includes(row.workflow_id)} disabled={!draft.workflowIds.includes(row.workflow_id) && draft.workflowIds.length >= 64} onChange={() => toggleWorkflow(row.workflow_id)} /><WorkflowIcon /><span>{row.workflow_id}</span></label>)}
        {workflows.hasNextPage ? <button type="button" className="load-more" disabled={workflows.isFetchingNextPage} onClick={() => void workflows.fetchNextPage()}><BusyLabel busy={workflows.isFetchingNextPage} label="Loading more">More workflows</BusyLabel></button> : null}
      </div> : null}
    </div>
    <div className="filter-select">
      <button type="button" className="filter-select-trigger" aria-expanded={expanded === "revisions"} aria-controls="filter-revision-options" onClick={() => setExpanded(current => current === "revisions" ? null : "revisions")}><span>Revisions</span><small>{draft.revisionIds.length ? `${draft.revisionIds.length} selected` : "All"}</small><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m4 6 4 4 4-4" /></svg></button>
      {expanded === "revisions" ? <div className="filter-options" id="filter-revision-options" role="group" aria-label="Revision choices">
        {draft.workflowIds.length ? draft.workflowIds.map(id => <RevisionOptions key={id} workflowId={id} selected={draft.revisionIds} onToggle={revision => setDraft(current => ({ ...current, revisionIds: toggleId(current.revisionIds, revision) }))} />) : <p className="filter-hint">Select workflows to list their revisions.</p>}
      </div> : null}
    </div>
    <div className="filter-select">
      <button type="button" className="filter-select-trigger" aria-expanded={expanded === 'statuses'} aria-controls="filter-status-options" onClick={() => setExpanded(current => current === 'statuses' ? null : 'statuses')}><span>Invocation status</span><small>{draft.statuses.length ? `${draft.statuses.length} selected` : 'All'}</small><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m4 6 4 4 4-4" /></svg></button>
      {expanded === 'statuses' ? <div className="filter-options" id="filter-status-options" role="group" aria-label="Invocation status choices">
        {INVOCATION_STATUSES.map(status => <label className="filter-option" key={status}><input type="checkbox" checked={draft.statuses.includes(status)} onChange={() => setDraft(current => ({ ...current, statuses: toggleId(current.statuses, status) }))} /><span>{status[0].toUpperCase() + status.slice(1)}</span></label>)}
      </div> : null}
    </div>
    <fieldset className="filter-time-range"><legend>Invocation start time</legend>
      <EnglishDateTimeInput label="From" value={draft.startedFrom} onChange={value => setDraft(current => ({ ...current, startedFrom: value }))} />
      <EnglishDateTimeInput label="To" value={draft.startedTo} onChange={value => setDraft(current => ({ ...current, startedTo: value }))} />
      <small>Local time · {Intl.DateTimeFormat().resolvedOptions().timeZone}</small>
      {invalidTime ? <p role="alert">Enter a valid date and time within the supported timestamp range.</p> : null}
      {invalidRange ? <p role="alert">From must be earlier than or equal to To.</p> : null}
    </fieldset>
    <footer className="filter-dialog-footer"><button type="button" className="filter-clear" disabled={!executionFilterCount(draft) && !executionFilterCount(filters)} onClick={() => { setDraft(EMPTY_EXECUTION_FILTERS); setExpanded(null); onApply(EMPTY_EXECUTION_FILTERS); }}>Clear</button><button type="submit" className="filter-apply" disabled={invalidRange || invalidTime}>Apply</button></footer>
  </form></dialog>;
}
