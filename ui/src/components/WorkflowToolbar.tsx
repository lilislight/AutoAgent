import { useCallback, useEffect, useRef, useState } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { listWorkflowRevisions } from "../api";
import type { WorkflowGraphData } from "../types";
import { formatTime } from "./TraceDetails";

export function WorkflowToolbar({ workflow, onRevision, onDefinition }: {
  workflow: WorkflowGraphData; onRevision: (id: string) => void;
  onDefinition: () => void;
}) {
  const [open, setOpen] = useState(false);
  const container = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const close = useCallback(() => { setOpen(false); trigger.current?.focus(); }, []);
  const revisions = useInfiniteQuery({
    queryKey: ["workflow-revisions", workflow.workflow_id],
    queryFn: ({ pageParam, signal }) => listWorkflowRevisions(workflow.workflow_id, null, pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    enabled: open, refetchOnWindowFocus: false, staleTime: 60_000, gcTime: 60_000,
  });
  useEffect(() => {
    if (!open) return;
    const dismiss = (event: PointerEvent) => { if (event.target instanceof Node && !container.current?.contains(event.target)) setOpen(false); };
    const escape = (event: KeyboardEvent) => { if (event.key === "Escape") { event.stopPropagation(); close(); } };
    document.addEventListener("pointerdown", dismiss);
    document.addEventListener("keydown", escape);
    return () => { document.removeEventListener("pointerdown", dismiss); document.removeEventListener("keydown", escape); };
  }, [open, close]);
  const rows = [...new Map(revisions.data?.pages.flatMap(page => page.items).map(row => [row.revision_id, row])).values()];
  const latest = rows[0]?.revision_id;
  return <header className="workflow-toolbar">
    <div className="workflow-toolbar-heading"><button type="button" className="workflow-title" title="View workflow definition" aria-haspopup="dialog" onClick={onDefinition}>{workflow.workflow_id}</button><div className="revision-selector" ref={container}>
      <button ref={trigger} className="revision-trigger" aria-expanded={open} aria-controls="revision-picker" aria-label="Choose workflow revision" onClick={() => setOpen(current => !current)}><span>v{workflow.workflow_version}</span><code title={workflow.workflow_revision_id}>{workflow.definition_hash.slice(0, 8)}</code><svg className="revision-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="m7 10 5 5 5-5" /></svg></button><span className="revision-identity">{workflow.runnable ? "Registered revision" : workflow.workflow_revision_id === latest ? "Latest saved" : "Saved revision"}</span>
      {open ? <div id="revision-picker" className="revision-picker" role="region" aria-label="Workflow revisions"><div className="revision-picker-heading"><strong>Revisions</strong><button className="icon-button" aria-label="Close revision picker" onClick={close}>×</button></div>
        {revisions.isPending ? <p className="sidebar-message">Loading revisions…</p> : null}
        {revisions.error ? <div className="sidebar-error"><p>{revisions.error.message}</p><button onClick={() => void revisions.refetch()}>Retry</button></div> : null}
        {rows.map(row => <button key={row.revision_id} className={`revision-choice ${row.revision_id === workflow.workflow_revision_id ? "active" : ""}`} aria-pressed={row.revision_id === workflow.workflow_revision_id} title={row.revision_id} onClick={() => { onRevision(row.revision_id); close(); }}><span className="revision-choice-top"><span>v{row.workflow_version}</span><code>{row.definition_hash?.slice(0, 8) ?? row.revision_id.split(":").at(-1)?.slice(0, 8)}</code>{row.runnable ? <span className="revision-tag">Registered</span> : null}{row.revision_id === latest ? <span className="revision-tag saved">Latest saved</span> : null}</span><small>Saved {formatTime(row.registered_at_us)}{row.revision_id === workflow.workflow_revision_id ? " · Viewing" : ""}</small></button>)}
        {revisions.hasNextPage ? <button className="load-more" disabled={revisions.isFetchingNextPage} onClick={() => void revisions.fetchNextPage()}>More revisions</button> : null}
      </div> : null}
    </div></div>

  </header>;
}
