import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { browseWorkflows } from "../api";
import { SearchIcon, WorkflowIcon } from "./Sidebar";
import { DirectorySkeleton, LoadingIndicator, BusyLabel } from './LoadingState';
import { Tooltip, ValueTip } from './Tooltip';
export { WorkflowIcon } from "./Sidebar";

type WorkflowChoice = { workflow_id: string; revision_id: string };

export function WorkflowDirectory({ workflowId, revisionId, onSelect, onLoadingChange }: {
  workflowId: string | null; revisionId: string | null;
  onSelect: (value: WorkflowChoice, replace?: boolean) => void;
  onLoadingChange?: (loading: boolean) => void;
}) {
  const [input, setInput] = useState("");
  const [search, setSearch] = useState("");
  const searchInput = useRef<HTMLInputElement>(null);
  useEffect(() => {
    const timeout = setTimeout(() => setSearch(input.trim().toLowerCase()), 300);
    return () => clearTimeout(timeout);
  }, [input]);
  const directory = useInfiniteQuery({
    queryKey: ["workflow-catalog", search],
    queryFn: ({ pageParam, signal }) => browseWorkflows(search, pageParam, signal),
    initialPageParam: null as string | null,
    getNextPageParam: page => page.next_cursor,
    refetchOnWindowFocus: false, staleTime: 60_000, gcTime: 60_000,
  });
  const entries = [...new Map(directory.data?.pages.flatMap(page => page.items).map(row => [row.workflow_id, row])).values()];
  const first = entries[0];
  const initialLoading = directory.isLoading || !revisionId && !!first && !search;
  useLayoutEffect(() => { onLoadingChange?.(initialLoading); }, [initialLoading, onLoadingChange]);
  useEffect(() => {
    if (!revisionId && first && !search) onSelect({ workflow_id: first.workflow_id, revision_id: first.current_revision_id ?? first.revision_id }, true);
  }, [revisionId, first, search, onSelect]);
  const searching = input.trim().toLowerCase() !== search || directory.isFetching && !directory.isFetchingNextPage;
  return <>
    <div className="directory-title">Workflows</div>
    <div className="directory-search"><div className="search-field"><SearchIcon /><input ref={searchInput} type="search" value={input} onChange={event => setInput(event.target.value)} aria-label="Search workflows or revision prefixes" placeholder="Workflow or revision prefix…" autoComplete="off" maxLength={512} />
      {input ? <button aria-label="Clear workflow search" onClick={() => { setInput(""); searchInput.current?.focus(); }}>×</button> : null}</div></div>
    <div className="directory-content" aria-busy={searching}>
      {directory.isLoading ? <DirectorySkeleton label="Loading workflows" /> : searching ? <div className="directory-search-status"><LoadingIndicator label="Searching workflows" small /></div> : null}
      {directory.error ? <div className="sidebar-error"><p>{directory.error.message}</p><button onClick={() => void directory.refetch()}>Retry</button></div> : null}
      {!directory.isPending && !directory.error && !entries.length ? <p className="sidebar-message">{search ? "No matching workflows" : "No saved workflows"}</p> : null}
      {!directory.isLoading ? <ul className="workflow-list">{entries.map((entry, index) => {
        const registered = entry.current_revision_id !== null;
        const targetRevision = entry.matched_revision_id ?? entry.current_revision_id ?? entry.revision_id;
        // Catalog versions describe the registered revision, or the saved one when unregistered.
        const versionKnown = !registered || targetRevision === entry.current_revision_id;
        const previous = entries[index - 1];
        return <li className="workflow-group" key={entry.workflow_id}>
          {!previous || (previous.current_revision_id !== null) !== registered ? <div className="directory-group-label">{registered ? "Registered" : "Unregistered"}</div> : null}
          <Tooltip content={<><ValueTip label="Workflow" value={entry.workflow_id}/>{versionKnown?<ValueTip label="Version" value={entry.workflow_version}/>:null}<ValueTip label="Revision" value={targetRevision}/></>}><button className={`workflow-item ${registered ? "registered" : "unregistered"} ${entry.workflow_id === workflowId ? "active" : ""}`} onClick={() => onSelect({ workflow_id: entry.workflow_id, revision_id: targetRevision })} aria-current={entry.workflow_id === workflowId ? "true" : undefined}>
            <span className="workflow-glyph"><WorkflowIcon /></span><span className="workflow-item-text"><strong>{entry.workflow_id}</strong><span className="workflow-item-meta"><span>{versionKnown ? `Version ${entry.workflow_version}` : `Revision ${targetRevision.split(":").at(-1)?.slice(0, 8)}`}</span></span></span>
          </button></Tooltip>
        </li>;
      })}</ul> : null}
      {directory.hasNextPage ? <button className="load-more" onClick={() => void directory.fetchNextPage()} disabled={directory.isFetchingNextPage}><BusyLabel busy={directory.isFetchingNextPage} label="Loading more">More workflows</BusyLabel></button> : null}
    </div>
  </>;
}
