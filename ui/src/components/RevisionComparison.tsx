import { FactsSkeleton, CodeSkeleton } from './LoadingState';
import { lazy, Suspense, useEffect, useMemo, useState } from 'react';
import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { compareWorkflowRevisions, getRevisionComparisonObject, getWorkflowGraph, getWorkflowObject, getWorkflowRevision, listWorkflowRevisions } from '../api';
import { comparisonGraph, definitionDifferences } from '../revisionComparison';
import type { RevisionChange, RevisionChangeStatus, Selection, WorkflowGraphData } from '../types';
import type { Theme } from '../theme';
import { Sidebar } from './Sidebar';
import { SelectMenu } from './SelectMenu';
import { FlowLoadingBoundary, FlowLoadingSurface } from './FlowLoadingBoundary';
import { InspectorFrame, InspectorPresence } from './InspectorFrame';
import { JsonValue } from './JsonValue';
import { pickDefinition } from '../inspectorModel';

const WorkflowGraph = lazy(()=>import('./WorkflowGraph'));
const RevisionSplitGraph = lazy(()=>import('./RevisionSplitGraph').then(module=>({default:module.RevisionSplitGraph})));
const presentedComparison = () => {};
const symbols:Record<RevisionChangeStatus,string>={added:'+',removed:'−',modified:'~'};

export function RevisionComparison({ workflowId, base, target, active, theme, onPair, onExit, changeView }: {
  workflowId:string; base:string; target:string; active:boolean; theme:Theme;
  onPair:(base:string,target:string)=>void; onExit:()=>void; changeView:(view:'workflows'|'executions'|'agents')=>void;
}) {
  const [selected,setSelected]=useState<RevisionChange|null>(null);
  const [selectedGroup,setSelectedGroup]=useState<string|null>(null);
  const [visible,setVisible]=useState(50);
  const [mode,setMode]=useState<'overlay'|'split'>('overlay');
  const [focusRequest,setFocusRequest]=useState(0);
  const revisions=useInfiniteQuery({queryKey:['workflow-revisions',workflowId],queryFn:({pageParam,signal})=>listWorkflowRevisions(workflowId,null,pageParam,signal),initialPageParam:null as string|null,getNextPageParam:page=>page.next_cursor,staleTime:60_000,refetchOnWindowFocus:false});
  const left=useQuery({queryKey:['workflow',base],queryFn:({signal})=>getWorkflowGraph(base,signal),staleTime:Infinity,refetchOnWindowFocus:false});
  const right=useQuery({queryKey:['workflow',target],queryFn:({signal})=>getWorkflowGraph(target,signal),staleTime:Infinity,refetchOnWindowFocus:false});
  const comparison=useInfiniteQuery({queryKey:['revision-comparison',base,target],queryFn:({pageParam,signal})=>compareWorkflowRevisions(base,target,pageParam,signal),initialPageParam:null as string|null,getNextPageParam:page=>page.next_cursor,staleTime:Infinity,gcTime:120_000,refetchOnWindowFocus:false});
  // Download only compact change metadata. Object bodies remain on-demand.
  useEffect(()=>{if(comparison.hasNextPage&&!comparison.isFetching&&!comparison.isError)void comparison.fetchNextPage();},[comparison.hasNextPage,comparison.isFetching,comparison.isError,comparison.fetchNextPage]);
  const changes=useMemo(()=>comparison.data?.pages.flatMap(page=>page.items)??[],[comparison.data]);
  const drawing=useMemo(()=>left.data&&right.data?comparisonGraph(left.data,right.data,changes):null,[left.data,right.data,changes]);
  const rows=[...new Map(revisions.data?.pages.flatMap(page=>page.items).map(row=>[row.revision_id,row])).values()];
  const options=rows.map(row=>({value:row.revision_id,label:`v${row.workflow_version} · ${row.definition_hash?.slice(0,8) ?? row.revision_id.split(':').at(-1)?.slice(0,8)}`}));
  for(const graph of [left.data,right.data])if(graph&&!options.some(option=>option.value===graph.workflow_revision_id))options.push({value:graph.workflow_revision_id,label:`v${graph.workflow_version} · ${graph.definition_hash.slice(0,8)}`});
  const error=left.error??right.error??comparison.error;
  const loading=left.isPending||right.isPending||comparison.isPending||comparison.hasNextPage;
  const counts=comparison.data?.pages[0].counts;
  const flowKey=`compare:${base}:${target}:${mode}`;
  const candidate=useMemo(()=>drawing&&left.data&&right.data&&!loading?{key:flowKey,mode,base:left.data,target:right.data,...drawing}:null,[drawing,left.data,right.data,loading,flowKey,mode]);
  const changePair=(nextBase:string,nextTarget:string)=>{setSelected(null);setSelectedGroup(null);setVisible(50);onPair(nextBase,nextTarget);};
  const graphSelection:Selection=selectedGroup?{kind:'group',id:selectedGroup}:selected?.kind==='node'||selected?.kind==='edge'?{kind:selected.kind,id:selected.id}:null;
  const choose=(selection:Selection)=>{
    setSelectedGroup(selection?.kind==='group'?selection.id:null);
    if(selection?.kind==='group'){setSelected(null);return;}
    if(!selection){setSelected(null);return;}
    const identity=selection.kind==='edge'?drawing?.marks.aliases[selection.id]??selection.id:selection.id;
    setSelected(changes.find(row=>row.kind===selection.kind&&row.id===identity)??{kind:selection.kind as RevisionChange['kind'],id:identity,status:'modified',fields:[],ordinal:0});
  };
  useEffect(()=>{if(!active||!selected&&!selectedGroup)return;const escape=(event:KeyboardEvent)=>{if(event.key==='Escape'&&!document.querySelector('[role="listbox"]')){setSelected(null);setSelectedGroup(null);}};window.addEventListener('keydown',escape);return()=>window.removeEventListener('keydown',escape);},[active,selected,selectedGroup]);
  return <div className={`workspace workspace-tab ${active?'':'is-inactive'}`} data-workspace-view="comparison" aria-hidden={!active} inert={!active}>
    <Sidebar view="workflows" onChange={changeView}>
      <div className="directory-title">Changes <span className="comparison-total">{comparison.data?.pages[0].total_count??'—'}</span></div>
      <div className="comparison-change-list">
        {(['workflow','node','edge'] as const).map(kind=>{
          const group=changes.slice(0,visible).filter(row=>row.kind===kind);
          return group.length?<section key={kind}><h3>{kind==='workflow'?'Workflow':kind==='node'?'Nodes':'Edges'} <span>{counts?.[kind]}</span></h3>{group.map(row=><button key={`${kind}:${row.id}`} type="button" className={`comparison-change-row ${selected?.kind===kind&&selected.id===row.id?'active':''}`} onClick={()=>{setSelectedGroup(null);setSelected(row);if(kind!=='workflow')setFocusRequest(value=>value+1);}}>
            <span className={`change-symbol change-${row.status}`} aria-label={row.status}>{symbols[row.status]}</span><span><strong>{row.id}</strong><small>{row.status[0].toUpperCase()+row.status.slice(1)}{row.fields.length?` · ${row.fields.length} ${row.fields.length===1?'field':'fields'}`:''}</small></span>
          </button>)}</section>:null;
        })}
        {changes.length>visible?<button className="load-more" onClick={()=>setVisible(value=>value+50)}>Show more changes</button>:null}
        {!loading&&!error&&!changes.length?<p className="sidebar-message">Saved definitions are identical.</p>:null}
      </div>
    </Sidebar>
    <main className="main-workspace comparison-workspace" aria-label="Workflow Revision comparison">
      <header className="workflow-toolbar comparison-toolbar"><strong title={workflowId}>{workflowId}</strong><label>Base<SelectMenu label="Base Revision" value={base} options={options} onChange={value=>changePair(value,target)} hasMore={revisions.hasNextPage} onMore={()=>void revisions.fetchNextPage()} loading={revisions.isFetching} error={revisions.error?.message} onRetry={()=>void revisions.refetch()} /></label>
        <button type="button" className="toolbar-action comparison-swap" title="Swap Base and Target" aria-label="Swap Base and Target" onClick={()=>changePair(target,base)}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M2 5h12m-3-3 3 3-3 3M14 11H2m3-3-3 3 3 3" /></svg></button>
        <label>Target<SelectMenu label="Target Revision" value={target} options={options} onChange={value=>changePair(base,value)} hasMore={revisions.hasNextPage} onMore={()=>void revisions.fetchNextPage()} loading={revisions.isFetching} error={revisions.error?.message} onRetry={()=>void revisions.refetch()} /></label>
        <nav className="execution-view-switch comparison-view-switch" aria-label="Revision comparison view">{(['overlay','split'] as const).map(value=><button type="button" key={value} aria-pressed={mode===value} onClick={()=>setMode(value)}>{value==='overlay'?'Overlay':'Split'}</button>)}</nav><button className="toolbar-action" onClick={onExit}>Exit compare</button>
      </header>
      <div className="definition-workspace comparison-canvas">
        <div className="comparison-key">{(['added','removed','modified'] as const).map(status=><span key={status}><i className={`change-symbol change-${status}`}>{symbols[status]}</i>{counts?.[status]??0} {status}</span>)}</div>
        <FlowLoadingBoundary flowKey={flowKey} candidate={candidate} pending={Boolean(loading)} failed={Boolean(error)} onPresented={presentedComparison}
          render={snapshot=><FlowLoadingSurface><Suspense fallback={null}>{snapshot.mode==='split'?<RevisionSplitGraph base={snapshot.base} target={snapshot.target} drawing={snapshot} selection={graphSelection} onSelect={choose} theme={theme} focusRequest={focusRequest} />:<WorkflowGraph workflow={snapshot.workflow} comparison={snapshot.marks} selection={graphSelection} onSelect={choose} theme={theme} focusRequest={focusRequest} />}</Suspense></FlowLoadingSurface>}
          fallback={<FlowLoadingSurface>{error?<div className="empty-state" role="alert"><h2>Could not compare Revisions</h2><p>{error.message}</p><button className="toolbar-action" onClick={()=>{void left.refetch();void right.refetch();void comparison.refetch();}}>Retry</button></div>:null}</FlowLoadingSurface>}
        />
        <InspectorPresence open={Boolean(selected)||Boolean(selectedGroup)}>{selectedGroup&&left.data&&right.data?<ComparisonGroupInspector key={`${base}:${target}:${selectedGroup}`} base={left.data} target={right.data} identity={selectedGroup} onClose={()=>setSelectedGroup(null)}/>:selected?<ComparisonInspector base={base} target={target} selected={selected} onClose={()=>setSelected(null)} />:null}</InspectorPresence>
      </div>
    </main>
  </div>;
}

function ComparisonGroupInspector({base,target,identity,onClose}:{base:WorkflowGraphData;target:WorkflowGraphData;identity:string;onClose:()=>void}){
  const [side,setSide]=useState(target.definition.groups?.some(g=>g.id===identity)?'target':'base');
  const graph=side==='base'?base:target,group=graph.definition.groups?.find(g=>g.id===identity);
  return <InspectorFrame kind="inline Workflow" title={group?.label??identity} context={<span>Revision comparison</span>} tabs={['Definition']} onClose={onClose}>
    {()=> <><SelectMenu label="Definition Revision" value={side} options={[{value:'base',label:'Base'},{value:'target',label:'Target'}]} onChange={setSide}/>{group?<ComparedDefinition revision={graph.workflow_revision_id} kind="group" identity={identity}/>:<p className="inspector-empty">Absent from {side}.</p>}</>}
  </InspectorFrame>;
}

function ComparisonInspector({base,target,selected,onClose}:{base:string;target:string;selected:RevisionChange;onClose:()=>void}) {
  const identity=`${base}:${target}:${selected.kind}:${selected.id}`;
  const [sideChoice,setSideChoice]=useState({identity,side:selected.status==='removed'?'base':'target'});
  const side=sideChoice.identity===identity?sideChoice.side:selected.status==='removed'?'base':'target';
  const detail=useQuery({queryKey:['comparison-object',base,target,selected.kind,selected.id],queryFn:({signal})=>getRevisionComparisonObject(base,target,selected.kind,selected.id,signal),staleTime:Infinity,gcTime:60_000,refetchOnWindowFocus:false});
  const differences=useMemo(()=>detail.data?definitionDifferences(detail.data):[],[detail.data]);
  const [limit,setLimit]=useState(50);
  return <InspectorFrame kind="revision comparison" title={selected.id} context={<span>{selected.ordinal===0?'Unchanged':selected.status[0].toUpperCase()+selected.status.slice(1)} · {selected.kind}</span>} tabs={['Changes','Definition']} initial="Changes" onClose={onClose}>{tab=><>
    {detail.isPending?<FactsSkeleton label="Loading selected object" />:null}
    {detail.error?<div className="sidebar-error" role="alert"><p>{detail.error.message}</p><button onClick={()=>void detail.refetch()}>Retry</button></div>:null}
    {detail.data?tab==='Definition'?<><SelectMenu label="Definition Revision" value={side} options={[{value:'base',label:'Base'},{value:'target',label:'Target'}]} onChange={value=>setSideChoice({identity,side:value})}/>{detail.data[side==='base'?'base':'target'].present?<ComparedDefinition revision={side==='base'?base:target} kind={selected.kind} identity={selected.id} />:<p className="inspector-empty">Absent from {side}.</p>}</>:<>
      {!differences.length?<p className="inspector-empty">No definition changes.</p>:null}
      {differences.slice(0,limit).map(diff=><section className="definition-difference" key={diff.path}><h3>{diff.path}</h3>{(['base','target'] as const).map(which=><div className={`definition-diff-side diff-${which}`} key={which}><span>{which==='base'?'Base':'Target'}</span>{diff[which].present?<JsonValue value={diff[which].value} label={which}/>:<p className="inspector-empty">Absent</p>}</div>)}</section>)}
      {differences.length>limit?<button className="load-more" onClick={()=>setLimit(value=>value+50)}>Show more fields</button>:null}
    </>:null}
  </>}</InspectorFrame>;
}


function ComparedDefinition({revision,kind,identity}:{revision:string;kind:string;identity:string}) {
  const definition=useQuery({queryKey:['definition',revision,kind,identity],queryFn:({signal})=>kind==='workflow'?getWorkflowRevision(revision,signal):getWorkflowObject(revision,kind,identity,signal),staleTime:Infinity,gcTime:60_000,refetchOnWindowFocus:false});
  if(definition.isPending)return <CodeSkeleton label="Loading definition" />;
  if(definition.error)return <div className="sidebar-error"><p>{definition.error.message}</p><button onClick={()=>void definition.refetch()}>Retry</button></div>;
  return <JsonValue value={pickDefinition(definition.data)} label="definition"/>;
}
