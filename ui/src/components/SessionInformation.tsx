import { useId, useState } from 'react';
import { useInfiniteQuery, useIsFetching, useQuery, useQueryClient } from '@tanstack/react-query';
import { getSessionHistory, getSessionInformation, getSessionWorkflows } from '../api';
import type { SessionWorkflow, TraceRow } from '../types';
import { CopyId } from './InspectorFrame';
import { formatTime, StatusBadge } from './TraceDetails';
import { SessionCheckpoints } from './SessionCheckpoints';

const tabs = ['Invocations', 'Workflows', 'Checkpoints'] as const;

export function SessionInformation({ session, onInvocation, onDefinition }: {
  session: string; onInvocation: (row: TraceRow) => void; onDefinition: (workflow: string, revision: string) => void;
}) {
  const [tab, setTab] = useState<typeof tabs[number]>('Invocations');
  const [inspectorHost, setInspectorHost] = useState<HTMLElement | null>(null);
  const client = useQueryClient();
  const checkpointFetching = useIsFetching({queryKey:['session-checkpoints',session]});
  const id = useId();
  const [revision, setRevision] = useState<string | null>(null);
  const info = useQuery({ queryKey:['session-information',session], queryFn:({signal})=>getSessionInformation(session,signal), staleTime:0, refetchOnWindowFocus:false });
  const history = useInfiniteQuery({ queryKey:['session-history',session,revision], queryFn:({pageParam,signal})=>getSessionHistory(session,pageParam,revision,signal), initialPageParam:null as string|null, getNextPageParam:page=>page.next_cursor, enabled:tab==='Invocations', staleTime:0, refetchOnWindowFocus:false });
  const workflows = useInfiniteQuery({ queryKey:['session-workflows',session], queryFn:({pageParam,signal})=>getSessionWorkflows(session,pageParam,signal), initialPageParam:null as string|null, getNextPageParam:page=>page.next_cursor, enabled:tab==='Workflows', staleTime:0, refetchOnWindowFocus:false });
  const rows = history.data?.pages.flatMap(page=>page.items) ?? [];
  const groups = new Map<string, SessionWorkflow[]>();
  for (const row of workflows.data?.pages.flatMap(page=>page.items) ?? []) groups.set(row.workflow_id, [...(groups.get(row.workflow_id) ?? []),row]);
  const query = tab==='Invocations'?history:workflows;
  return <section ref={setInspectorHost} className="session-information" aria-label="Session information">
    <header className="workflow-toolbar"><div className="workflow-toolbar-heading"><span className="session-toolbar-title">Session</span><CopyId value={session} /></div>
      <button type="button" className="toolbar-action" disabled={info.isFetching || (tab==='Checkpoints'?checkpointFetching>0:query.isFetching)} onClick={()=>{void info.refetch();if(tab==='Checkpoints'){void client.invalidateQueries({queryKey:['session-checkpoints',session]});void client.invalidateQueries({queryKey:['checkpoint-details']});}else void query.refetch();}}>Refresh</button></header>
    <div className="session-info-scroll">
      {info.error ? <div className="sidebar-error" role="alert"><p>{info.error.message}</p><button onClick={()=>void info.refetch()}>Retry</button></div> : null}
      {info.isPending ? <p className="muted" role="status">Loading Session…</p> : info.data ? <>
        <div className="session-facts"><span><strong>{info.data.invocation_count}</strong> {info.data.invocation_count===1?'Invocation':'Invocations'}</span><span><strong>{info.data.workflow_count}</strong> {info.data.workflow_count===1?'Workflow':'Workflows'}</span><span><strong>{info.data.revision_count}</strong> {info.data.revision_count===1?'Revision':'Revisions'}</span></div>
        <dl className="session-times"><div><dt>Created</dt><dd>{formatTime(info.data.created_at_us)}</dd></div><div><dt>Last activity</dt><dd>{formatTime(info.data.updated_at_us)}</dd></div>{info.data.parent_invocation_id ? <div><dt>Parent Invocation</dt><dd><button className="text-action" onClick={()=>onInvocation({invocation_id:info.data.parent_invocation_id!,values:[]})}>{info.data.parent_invocation_id}</button></dd></div> : null}</dl>
      </> : null}
      <div className="session-info-tabs" role="tablist" aria-label="Session sections">{tabs.map((value,index)=><button type="button" role="tab" key={value} aria-selected={tab===value} id={`${id}-${value}-tab`} aria-controls={`${id}-panel`} tabIndex={tab===value?0:-1} className={tab===value?'active':''} onClick={()=>setTab(value)} onKeyDown={event=>{let next:number|undefined;if(event.key==='ArrowLeft')next=(index+tabs.length-1)%tabs.length;if(event.key==='ArrowRight')next=(index+1)%tabs.length;if(event.key==='Home')next=0;if(event.key==='End')next=tabs.length-1;if(next!==undefined){event.preventDefault();setTab(tabs[next]);document.getElementById(`${id}-${tabs[next]}-tab`)?.focus();}}}>{value}</button>)}</div>
      <div id={`${id}-panel`} role="tabpanel" aria-labelledby={`${id}-${tab}-tab`}>
        {tab==='Checkpoints' ? <SessionCheckpoints session={session} inspectorHost={inspectorHost} /> : <>
        {revision && tab==='Invocations' ? <div className="session-revision-filter">Revision <code>{revision.split(':').at(-1)?.slice(0,8)}</code><button type="button" className="icon-button" aria-label="Show all Session Invocations" onClick={()=>setRevision(null)}>×</button></div> : null}
        {query.isPending ? <p className="muted" role="status">Loading {tab.toLowerCase()}…</p> : null}
        {query.error ? <div className="sidebar-error" role="alert"><p>{query.error.message}</p><button onClick={()=>void query.refetch()}>Retry</button></div> : null}
        {tab==='Invocations' ? <ol className="session-history-list">{rows.map(row=><li key={row.invocation_id}>
          <span className="session-history-order" title={row.opening_sequence===null?'Imported history begins at a Checkpoint':`Session opening Event #${row.opening_sequence}`}>#{row.ordinal}</span>
          <button className="session-history-open" onClick={()=>onInvocation(row)}><span className="session-history-title"><strong>{row.workflow_id}</strong><StatusBadge row={row} /></span><span><code>{row.invocation_id.slice(0,8)}</code><span>v{row.workflow_version} · {row.workflow_revision_id?.split(':').at(-1)?.slice(0,8)}</span></span><small>Started {formatTime(row.started_at_us)}{row.recovery_count?` · Recovered ${row.recovery_count}×`:''}</small></button>
          <button className="text-action" onClick={()=>onDefinition(row.workflow_id!,row.workflow_revision_id!)}>Definition</button>
        </li>)}</ol> : <div className="session-workflow-groups">{[...groups].map(([workflow,members])=><section key={workflow}><h3>{workflow}</h3>{members.map(row=><div className="session-workflow-revision" key={row.workflow_revision_id}>
          <div><strong>v{row.workflow_version}</strong> <code>{row.workflow_revision_id.split(':').at(-1)?.slice(0,8)}</code><span className="revision-tag">{row.runnable?'Registered':'Unregistered'}</span><small>{row.invocation_count} {row.invocation_count===1?'Invocation':'Invocations'} · {formatTime(row.first_started_at_us)}</small></div>
          <div className="session-row-actions"><button className="text-action" onClick={()=>onDefinition(workflow,row.workflow_revision_id)}>Definition</button><button className="text-action" onClick={()=>{setRevision(row.workflow_revision_id);setTab('Invocations');}}>Invocations</button></div>
        </div>)}</section>)}</div>}
        {!query.isPending && !query.error && (tab==='Invocations'?!rows.length:!groups.size) ? <p className="muted">No saved {tab.toLowerCase()}.</p> : null}
        {query.hasNextPage ? <button className="load-more" disabled={query.isFetchingNextPage} onClick={()=>void query.fetchNextPage()}>{query.isFetchingNextPage?'Loading…':'Show more'}</button> : null}
        </>}
      </div>
    </div>
  </section>;
}
