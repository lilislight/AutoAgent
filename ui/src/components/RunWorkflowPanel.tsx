import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getWorkflowRunSpec, runWorkflow } from '../api';
import type { AcceptedExecution, WorkflowGraphData } from '../types';
import { runInput } from '../runInput';
import { JsonValue } from './JsonValue';

export function RunWorkflowPanel({workflow,onClose,onAccepted,initial}:{workflow:WorkflowGraphData;onClose:()=>void;onAccepted:(receipt:AcceptedExecution)=>void;initial?:{entry:string;input:unknown}}) {
  const id=useId(),close=useRef<HTMLButtonElement>(null),submitting=useRef(false),mounted=useRef(true);
  const [entry,setEntry]=useState(initial?.entry ?? (workflow.definition.entry_node_ids.length===1?workflow.definition.entry_node_ids[0]:''));
  const [text,setText]=useState(()=>initial?JSON.stringify(initial.input,null,2):'{}'),[context,setContext]=useState('{}'),[includeContext,setIncludeContext]=useState(false);
  const [sessionMode,setSessionMode]=useState<'new'|'existing'>('new'),[session,setSession]=useState(''),[debounced,setDebounced]=useState('');
  const [busy,setBusy]=useState(false),[error,setError]=useState<string|null>(null);
  useEffect(()=>{const timer=setTimeout(()=>setDebounced(session.trim()),250);return()=>clearTimeout(timer);},[session]);
  useEffect(()=>{mounted.current=true;const previous=document.activeElement;close.current?.focus({preventScroll:true});return()=>{mounted.current=false;if(previous instanceof HTMLElement && previous.isConnected)previous.focus({preventScroll:true});};},[]);
  const spec=useQuery({queryKey:['workflow-run-spec',workflow.workflow_id,workflow.workflow_revision_id,entry,sessionMode==='existing'?debounced:null],
    queryFn:({signal})=>getWorkflowRunSpec(workflow.workflow_id,workflow.workflow_revision_id,entry,sessionMode==='existing'?debounced:null,signal),
    staleTime:0,retry:false,refetchOnWindowFocus:false});
  const selected=spec.data?.entries.find(row=>row.node_id===entry);
  const firstEntry=useRef(Boolean(initial));
  useEffect(()=>{if(firstEntry.current || !selected)return;firstEntry.current=true;if(selected.input_mode==='contract' && selected.input_schema===null)setText('null');},[selected]);
  const checkingSession=sessionMode==='existing' && (!session.trim() || session.trim()!==debounced);
  const disabled=busy || !entry || !spec.data?.can_run || spec.isFetching || checkingSession;
  const submit=async(event:FormEvent)=>{
    event.preventDefault();if(disabled || submitting.current)return;
    let body;
    try{body=runInput({workflow:workflow.workflow_id,revision:workflow.workflow_revision_id,entry,text,sessionMode,session,context,includeContext});}
    catch(failure){setError((failure as Error).message);return;}
    submitting.current=true;setBusy(true);setError(null);
    try{
      // Deliberately no retry: a lost HTTP response does not mean admission failed.
      const receipt=await runWorkflow(body,new AbortController().signal);
      onAccepted(receipt);
    }catch(failure){if(mounted.current){setError((failure as Error).message);void spec.refetch();}}
    finally{submitting.current=false;if(mounted.current)setBusy(false);}
  };
  return <div className="inspector-presence run-panel-presence"><aside className="inspector run-panel" role="dialog" aria-modal="false" aria-labelledby={`${id}-title`} onKeyDown={event=>{if(event.key==='Escape'){event.stopPropagation();if(!busy)onClose();}}}>
    <div className="inspector-heading"><div><span className="eyebrow">NEW ROOT EXECUTION</span><h2 id={`${id}-title`}>{initial?'Run again':'Run workflow'}</h2><div className="inspector-context"><strong>{workflow.workflow_id}</strong><span>v{workflow.workflow_version}</span><code title={workflow.workflow_revision_id}>{workflow.definition_hash.slice(0,8)}</code></div></div><button ref={close} type="button" className="icon-button" aria-label="Close run panel" disabled={busy} onClick={onClose}>×</button></div>
    <form onSubmit={event=>void submit(event)} className="run-form">
      <div className="run-fields">
        <label className="run-field" htmlFor={`${id}-entry`}><span>Entry node</span><select id={`${id}-entry`} value={entry} disabled={busy || spec.isPending} onChange={event=>{setEntry(event.target.value);setError(null);}}><option value="" disabled>Choose an Entry node</option>{workflow.definition.entry_node_ids.map(node=><option key={node} value={node}>{node}</option>)}</select></label>
        <fieldset className="run-session-mode" disabled={busy}><legend>Session</legend><label><input type="radio" name={`${id}-session`} checked={sessionMode==='new'} onChange={()=>setSessionMode('new')} />New session</label><label><input type="radio" name={`${id}-session`} checked={sessionMode==='existing'} onChange={()=>setSessionMode('existing')} />Existing session</label></fieldset>
        {sessionMode==='existing'?<label className="run-field" htmlFor={`${id}-session-id`}><span>Session ID</span><input id={`${id}-session-id`} value={session} disabled={busy} autoComplete="off" spellCheck={false} placeholder="Paste a Session ID" onChange={event=>setSession(event.target.value)} /></label>:null}
        <label className="run-field" htmlFor={`${id}-input`}><span>Input <small>JSON</small></span><textarea id={`${id}-input`} value={text} onChange={event=>{setText(event.target.value);setError(null);}} disabled={busy} spellCheck={false} autoComplete="off" rows={10} /></label>
        {selected?.input_mode==='raw_json'?<p className="run-hint">This Entry uses input mapping. Supply the original Workflow input as JSON; the mapping validates its result during execution.</p>:null}
        {selected?.input_mode==='contract'?<details className="run-schema"><summary>Input schema</summary><JsonValue value={selected.input_schema} label="input_schema" /></details>:null}
        {sessionMode==='new'?<><label className="run-context-toggle"><input type="checkbox" checked={includeContext} onChange={event=>setIncludeContext(event.target.checked)} disabled={busy} />Set initial session context</label>{includeContext?<label className="run-field" htmlFor={`${id}-context`}><span>Session context <small>JSON object</small></span><textarea id={`${id}-context`} value={context} onChange={event=>setContext(event.target.value)} disabled={busy} spellCheck={false} rows={5}/></label>:null}</>:<p className="run-hint">Reuses this Session's context and creates a new Invocation. Its previous execution must have finished.</p>}
        {spec.isPending?<p className="run-hint" role="status">Checking registered Revision…</p>:null}
        {spec.error?<div className="run-error" role="alert">{spec.error.message}<button type="button" onClick={()=>void spec.refetch()}>Retry</button></div>:null}
        {!checkingSession && spec.data?.message?<p className="run-error" role="status">{spec.data.message}</p>:null}
        {error?<p className="run-error" role="alert">{error}</p>:null}
      </div>
      <div className="run-footer"><span>{busy?'Submitting…':'Creates a new Root Invocation'}</span><button type="button" disabled={busy} onClick={onClose}>Cancel</button><button type="submit" className="run-primary" disabled={disabled}>{busy?'Starting…':'Run'}</button></div>
    </form>
  </aside></div>;
}
