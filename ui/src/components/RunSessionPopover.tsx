import { useLayoutEffect, useRef, useState, type RefObject } from 'react';
import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { getWorkflowGraph, browseWorkflows } from '../api';
import type { AcceptedExecution } from '../types';
import { useActionDraft } from '../actionDrafts';
import { SelectMenu } from './SelectMenu';
import { ActionReference } from './ActionReference';
import { RunWorkflowForm, useDebounced } from './RunWorkflowForm';

export function RunSessionPopover({ anchor, session, onClose, onAccepted }: { anchor: RefObject<HTMLButtonElement | null>; session?: string; onClose: () => void; onAccepted: (receipt: AcceptedExecution) => void }) {
  const dialog = useRef<HTMLDialogElement>(null), [revision, setRevision] = useState('');
  const [search,setSearch] = useState(''), [choicesOpen,setChoicesOpen] = useState(false);
  const query = useDebounced(search.trim().toLowerCase());
  const [newId, setNewId] = useActionDraft('new-session:id', () => '');
  const workflows = useInfiniteQuery({ queryKey: ['run-registered-workflows',query], queryFn: ({ pageParam, signal }) => browseWorkflows(query,pageParam,signal,true), initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor, enabled: choicesOpen, staleTime: 0, refetchOnWindowFocus: false });
  const rows = workflows.data?.pages.flatMap(page => page.items) ?? [];
  const graph = useQuery({ queryKey: ['workflow', revision], queryFn: ({ signal }) => getWorkflowGraph(revision, signal), enabled: Boolean(revision), staleTime: 60_000, refetchOnWindowFocus: false });
  useLayoutEffect(() => {
    const popup = dialog.current, button = anchor.current; if (!popup || !button) return;
    const previous = document.activeElement;
    popup.showModal();
    const position = () => { const rect = button.getBoundingClientRect(); popup.style.left = `${Math.max(12, Math.min(rect.right + 10, window.innerWidth - popup.offsetWidth - 12))}px`; popup.style.top = `${Math.max(12, Math.min(rect.top, window.innerHeight - popup.offsetHeight - 12))}px`; };
    const observer = new ResizeObserver(position); observer.observe(popup); observer.observe(button); position();
    window.addEventListener('resize', position);
    return () => { observer.disconnect(); window.removeEventListener('resize', position); popup.close(); if (previous instanceof HTMLElement && previous.isConnected) previous.focus({ preventScroll: true }); };
  }, [anchor]);
  return <dialog ref={dialog} className="run-session-popover" aria-label={session ? 'Run in this Session' : 'New Session and Run'} onCancel={onClose} onClick={event => { if (event.target === dialog.current) { const rect = dialog.current.getBoundingClientRect(); if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) onClose(); } }}><header><strong>{session ? 'Run in this Session' : 'New Session and Run'}</strong><button type="button" className="icon-button" aria-label="Close Run" onClick={onClose}>×</button></header>
    {session ? <ActionReference label="Session ID" value={session} /> : <label className="run-field">Session ID <small>Optional</small><input value={newId} onChange={event => setNewId(event.target.value)} maxLength={512} autoComplete="off" placeholder="Leave empty to generate an ID" /></label>}
    <div className="run-field"><span>Workflow</span><SelectMenu label="Workflow" value={revision} onChange={setRevision} searchable onSearch={setSearch} onOpenChange={setChoicesOpen} placeholder="Search registered Workflows…"
      options={rows.map(row => ({value:row.current_revision_id!,label:row.workflow_id,description:`v${row.workflow_version} · ${row.current_revision_id?.split(':').at(-1)?.slice(0,8)}`}))}
      loading={search.trim().toLowerCase() !== query || workflows.isFetching} error={workflows.error?.message} onRetry={()=>void workflows.refetch()} hasMore={workflows.hasNextPage} onMore={()=>{if(!workflows.isFetchingNextPage)void workflows.fetchNextPage();}} /></div>
    {graph.isFetching ? <p role="status">Loading Workflow…</p> : null}{graph.error ? <p className="run-error">{graph.error.message}<button type="button" onClick={() => void graph.refetch()}>Retry</button></p> : null}
    {graph.data && graph.data.workflow_revision_id === revision ? <RunWorkflowForm key={revision} workflow={graph.data} sessionMode={session ? 'existing' : 'new'} session={session ?? newId} hideSessionField scope={session ? `session:${session}` : 'new-session'} onAccepted={receipt => { if (!session) setNewId(''); onAccepted(receipt); onClose(); }} /> : null}
  </dialog>;
}
