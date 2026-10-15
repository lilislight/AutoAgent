import { useEffect, useId, useRef, useState, type FormEvent, type ReactNode } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, getExecutionActionSpec, getExecutionControls, getExecutionOperation, getExecutionValue, submitExecutionAction } from '../api';
import type { AcceptedExecution, ExecutionActionInput, ExecutionActionKind, ExecutionControls, ExecutionTrace, PendingResponse, TraceRow, WorkflowGraphData } from '../types';
import { JsonValue } from './JsonValue';
import { LazyValue } from './TraceDetails';
import { RunWorkflowPanel } from './RunWorkflowPanel';

type Panel={kind:'pause'|'continue'|'cancel'|'respond'|'signal'|'run_again';wait?:PendingResponse};
const labels:Record<ExecutionActionKind,string>={pause:'Pause',continue:'Continue',cancel:'Cancel execution',respond:'Respond',signal:'Send signal'};
function savedOperation(iid:string,value?:string|null) {
  try{const key=`autoagent.operation:${iid}`;if(value===undefined)return sessionStorage.getItem(key);if(value)sessionStorage.setItem(key,value);else sessionStorage.removeItem(key);}catch{/* Receipts remain queryable on this page without browser storage. */}
  return null;
}
function signalSource(root:string) {
  try{const key=`autoagent.signal-source:${root}`;let source=sessionStorage.getItem(key);if(!source){source=`ui-${crypto.randomUUID()}`;sessionStorage.setItem(key,source);}return source;}catch{return `ui-${crypto.randomUUID()}`;}
}

export function useExecutionActions(trace:ExecutionTrace,workflow:WorkflowGraphData,active:boolean,replaying:boolean,onRunAccepted:(receipt:AcceptedExecution)=>void) {
  const iid=trace.invocation.invocation_id,client=useQueryClient();
  const [panel,setPanel]=useState<Panel|null>(null),[operationId,setOperationId]=useState<string|null>(()=>savedOperation(iid));
  const [submitting,setSubmitting]=useState(false),[error,setError]=useState<string|null>(null),submittingRef=useRef(false);
  const controls=useQuery({queryKey:['execution-controls',iid],queryFn:({signal})=>getExecutionControls(iid,signal),enabled:active,
    staleTime:1000,gcTime:30_000,retry:false,refetchOnWindowFocus:false,
    refetchInterval:query=>active && (!query.state.data || !['completed','failed','cancelled'].includes(query.state.data.status))?3000:false});
  useEffect(()=>{if(active)void controls.refetch({cancelRefetch:false});},[trace.frontier,active]);
  useEffect(()=>{if(!active || replaying)setPanel(null);},[active,replaying]);
  const operation=useQuery({queryKey:['execution-operation',operationId],queryFn:({signal})=>getExecutionOperation(operationId!,signal),enabled:active && Boolean(operationId),retry:false,
    refetchInterval:query=>active && query.state.data?.status==='running'?700:false,refetchOnWindowFocus:false,gcTime:30_000});
  useEffect(()=>{
    if(!operation.data || operation.data.status==='running')return;
    savedOperation(iid,null);
    void client.invalidateQueries({predicate:query=>['execution-controls','execution-action-spec','session-invocations-page','execution-family','family-children','inspector-items','execution-record'].includes(String(query.queryKey[0])),refetchType:'active'});
  },[operation.data,iid,client]);
  const busy=submitting || Boolean(operationId && (operation.isPending || operation.error)) || operation.data?.status==='running';
  const send=async(action:ExecutionActionKind,options:Partial<ExecutionActionInput>={})=>{
    if(submittingRef.current || replaying)return;
    submittingRef.current=true;setSubmitting(true);setError(null);
    const request_id=crypto.randomUUID(),body={...options,invocation_id:iid,action,request_id};
    try{
      const receipt=await submitExecutionAction(body,new AbortController().signal);
      client.setQueryData(['execution-operation',receipt.operation_id],receipt);
      savedOperation(iid,receipt.operation_id);setOperationId(receipt.operation_id);setPanel(null);
    }catch(failure){
      if(!(failure instanceof ApiError) || failure.status>=500){savedOperation(iid,request_id);setOperationId(request_id);setPanel(null);}
      else setError(failure.message);
      void controls.refetch();
    }
    finally{submittingRef.current=false;setSubmitting(false);}
  };
  const dismiss=()=>{setError(null);setOperationId(null);savedOperation(iid,null);};
  const choose=(value:Panel)=>{if(!replaying){setError(null);setPanel(value);}};
  const child=trace.invocation.invocation_id!==controls.data?.root_invocation_id;
  return {isOpen:Boolean(panel),canRespond:Boolean(!replaying && controls.data?.actions.respond.available),pendingResponses:controls.data?.pending_responses ?? [],
    respond:(row:TraceRow)=>choose({kind:'respond',wait:{invocation_id:row.invocation_id,wait_id:String(row.wait_id),occurrence_id:String(row.occurrence_id),created_at_us:Number(row.created_at_us ?? 0)}}),
    toolbar:<ActionToolbar controls={controls.data} child={child} disabled={replaying || !active} busy={busy} onPick={choose} onRetry={()=>void controls.refetch()} error={controls.error?.message}
      feedback={error ?? operation.error?.message ?? operation.data?.error?.message ?? (operation.data?`${labels[operation.data.action]} ${operation.data.status==='running'?'in progress…':operation.data.status==='succeeded'?'accepted':''}`:operationId?'Checking operation receipt…':null)} onDismiss={dismiss}/>,
    panel:panel?<>{panel.kind==='run_again'?<RunAgain trace={trace} workflow={workflow} onClose={()=>setPanel(null)} onAccepted={onRunAccepted}/>:<ActionPanel key={`${panel.kind}:${panel.wait?.wait_id ?? ''}`} panel={panel} trace={trace} controls={controls.data} busy={submitting} error={error} onClose={()=>setPanel(null)} onSubmit={send}/>}</>:null};
}

function ActionToolbar({controls,child,disabled,busy,onPick,onRetry,error,feedback,onDismiss}:{controls?:ExecutionControls;child:boolean;disabled:boolean;busy:boolean;onPick:(panel:Panel)=>void;onRetry:()=>void;error?:string;feedback:string|null;onDismiss:()=>void}) {
  const [open,setOpen]=useState(false),container=useRef<HTMLDivElement>(null);
  useEffect(()=>{if(disabled)setOpen(false);},[disabled]);
  useEffect(()=>{if(!open)return;const close=(event:PointerEvent)=>{if(!container.current?.contains(event.target as Node))setOpen(false);};document.addEventListener('pointerdown',close);return()=>document.removeEventListener('pointerdown',close);},[open]);
  const action=controls?.actions,terminal=controls && ['completed','failed','cancelled'].includes(controls.status);
  const primary=action?.continue_execution.available?'continue':action?.pause.available?'pause':terminal && action?.run_again.available?'run_again':null;
  const pick=(kind:Panel['kind'])=>{setOpen(false);onPick({kind});};
  return <div className="execution-actions" ref={container} onKeyDown={event=>{if(event.key==='Escape'){setOpen(false);event.stopPropagation();}}}>
    {primary?<button className="execution-action-primary" disabled={disabled || busy} title={child && primary!=='run_again'?'Controls the Root and its owned children':undefined} onClick={()=>pick(primary)}>{primary==='run_again'?'Run again':primary==='continue'?child?'Continue root':'Continue':child?'Pause root':'Pause'}</button>:null}
    {error?<button className="action-feedback" title={error} onClick={onRetry}>Retry controls</button>:null}
    <button className="execution-action-menu" aria-label="Execution actions" aria-haspopup="menu" aria-expanded={open} disabled={disabled || !controls} onClick={()=>setOpen(value=>!value)}>⋯</button>
    {open && controls?<div className="execution-action-dropdown" role="menu">
      {(['respond','signal','run_again','cancel'] as const).map(kind=><button key={kind} type="button" role="menuitem" disabled={!controls.actions[kind].available || Boolean(busy && kind!=='cancel')} title={controls.actions[kind].reason ?? undefined} className={kind==='cancel'?'is-destructive':''} onClick={()=>pick(kind)}>{kind==='respond'?`Pending responses${controls.pending_responses.length?` · ${controls.pending_responses.length}${controls.next_cursor?'+':''}`:''}`:kind==='signal'?'Send signal to root':kind==='run_again'?'Run again':child?'Cancel root execution':'Cancel execution'}</button>)}
    </div>:null}
    {feedback?<div className="execution-action-feedback" role="status"><span title={feedback}>{feedback}</span><button aria-label="Dismiss operation message" onClick={onDismiss}>×</button></div>:null}
  </div>;
}

function ActionShell({title,children,onClose,busy=false}:{title:string;children:ReactNode;onClose:()=>void;busy?:boolean}) {
  const id=useId(),close=useRef<HTMLButtonElement>(null);
  useEffect(()=>{const previous=document.activeElement;close.current?.focus({preventScroll:true});return()=>{if(previous instanceof HTMLElement && previous.isConnected && !previous.closest('[inert]'))previous.focus({preventScroll:true});};},[]);
  return <div className="inspector-presence run-panel-presence"><aside className="inspector run-panel" role="dialog" aria-modal="false" aria-labelledby={id} onKeyDown={event=>{if(event.key==='Escape'){event.stopPropagation();if(!busy)onClose();}}}><div className="inspector-heading"><div><span className="eyebrow">EXECUTION ACTION</span><h2 id={id}>{title}</h2></div><button ref={close} className="icon-button" aria-label="Close action panel" disabled={busy} onClick={onClose}>×</button></div>{children}</aside></div>;
}

function ResponsePicker({iid,onPick}:{iid:string;onPick:(wait:PendingResponse)=>void}) {
  const [cursors,setCursors]=useState<(string|null)[]>([null]),[page,setPage]=useState(0);
  const query=useQuery({queryKey:['execution-controls',iid,cursors[page]],queryFn:({signal})=>getExecutionControls(iid,signal,cursors[page]),staleTime:0,retry:false});
  return <>{query.isPending?<p role="status">Loading pending responses…</p>:null}{query.error?<p className="run-error">{query.error.message}<button type="button" onClick={()=>void query.refetch()}>Retry</button></p>:null}
    {query.data?.pending_responses.map(wait=><button key={`${wait.invocation_id}:${wait.wait_id}`} type="button" className="response-choice" onClick={()=>onPick(wait)}><strong>{wait.occurrence_id}</strong><code>{wait.invocation_id.slice(0,8)}</code><span>Respond ↗</span></button>)}
    {query.data && !query.data.pending_responses.length?<p className="run-hint">No pending external responses.</p>:null}
    {page || query.data?.next_cursor?<div className="response-pagination"><button type="button" disabled={!page || query.isFetching} onClick={()=>setPage(n=>n-1)}>Previous</button><span>Page {page+1}</span><button type="button" disabled={!query.data?.next_cursor || query.isFetching} onClick={()=>{setCursors(values=>[...values.slice(0,page+1),query.data!.next_cursor]);setPage(n=>n+1);}}>Next</button></div>:null}
  </>;
}

function ActionPanel({panel,trace,controls,busy,error,onClose,onSubmit}:{panel:Exclude<Panel,{kind:'run_again'}>;trace:ExecutionTrace;controls?:ExecutionControls;busy:boolean;error:string|null;onClose:()=>void;onSubmit:(action:ExecutionActionKind,options?:Partial<ExecutionActionInput>)=>Promise<void>}) {
  const [wait,setWait]=useState(panel.wait),[text,setText]=useState('{}'),[reason,setReason]=useState(''),[localError,setLocalError]=useState<string|null>(null);
  const [endpoint,setEndpoint]=useState(controls?.endpoints[0] ?? '');
  const [source]=useState(()=>signalSource(controls?.root_invocation_id ?? trace.invocation.root_invocation_id));
  const action=panel.kind as ExecutionActionKind,id=useId();
  const options:Record<string,string>=action==='respond' && wait?{target_invocation_id:wait.invocation_id,wait_id:wait.wait_id}:action==='signal'?{endpoint,source_id:source}:{};
  const spec=useQuery({queryKey:['execution-action-spec',trace.invocation.invocation_id,action,options],queryFn:({signal})=>getExecutionActionSpec(trace.invocation.invocation_id,action,options,signal),
    enabled:action==='respond' && Boolean(wait) || action==='signal' && Boolean(endpoint),retry:false,staleTime:0,refetchOnWindowFocus:false});
  const submit=async(event:FormEvent)=>{
    event.preventDefault();if(busy)return;setLocalError(null);
    let payload:unknown;
    if(action==='respond' || action==='signal'){
      if(!spec.data || spec.isFetching)return;
      try{payload=JSON.parse(text);}catch{setLocalError('Payload must be valid JSON.');return;}
    }
    await onSubmit(action,action==='respond'?{target_invocation_id:wait!.invocation_id,wait_id:wait!.wait_id,payload}:action==='signal'?{endpoint,source_id:source,sequence:spec.data!.next_sequence,payload}:action==='cancel'?{reason:reason.trim() || undefined}:{});
  };
  const title=action==='respond' && !wait?'Pending responses':labels[action];
  const editable=action==='signal' || action==='respond' && Boolean(wait);
  return <ActionShell title={title} onClose={onClose} busy={busy}><form className="run-form" onSubmit={event=>void submit(event)}><div className="run-fields">
    <p className="run-hint">Root <code>{controls?.root_invocation_id}</code>{action==='pause' || action==='continue' || action==='cancel'?' · Applies to the complete owned execution tree.':''}</p>
    {action==='respond' && !wait?<ResponsePicker iid={trace.invocation.invocation_id} onPick={setWait}/>:null}
    {action==='pause'?<p className="action-explanation">Stops new Node dispatch and waits for in-flight work to reach a resumable boundary. A long-running Operator may delay the pause.</p>:null}
    {action==='continue'?<p className="action-explanation">{controls?.continue_reason==='paused'?'Continues the paused execution tree.':'Restores the interrupted execution using its registered recovery rules.'} Pending external Waits still require a response.</p>:null}
    {action==='cancel'?<><p className="action-explanation">Cancels this Root and its owned child tasks.</p><label className="run-field" htmlFor={`${id}-reason`}>Reason (optional)<textarea id={`${id}-reason`} rows={3} value={reason} onChange={event=>setReason(event.target.value)} disabled={busy} maxLength={4096}/></label></>:null}
    {action==='signal'?<label className="run-field" htmlFor={`${id}-endpoint`}>Signal endpoint<select id={`${id}-endpoint`} value={endpoint} onChange={event=>setEndpoint(event.target.value)} disabled={busy}>{controls?.endpoints.map(name=><option key={name} value={name}>{name}</option>)}</select></label>:null}
    {wait?<><div className="action-target"><span>Target invocation</span><code>{wait.invocation_id}</code><span>Wait</span><code>{wait.wait_id}</code></div><LazyValue invocationId={wait.invocation_id} kind="wait" identity={wait.wait_id} field="request" label="Wait request" /></>:null}
    {editable?<><label className="run-field" htmlFor={`${id}-payload`}>{action==='respond'?'Response':'Signal payload'} <small>JSON</small><textarea id={`${id}-payload`} value={text} onChange={event=>setText(event.target.value)} rows={9} spellCheck={false} disabled={busy}/></label>{spec.data?<details className="run-schema"><summary>Payload schema</summary><JsonValue value={spec.data.input_schema} label="payload_schema"/></details>:null}{action==='signal'?<details className="action-delivery"><summary>Delivery identity</summary><dl><dt>Source ID</dt><dd><code>{source}</code></dd><dt>Sequence</dt><dd>{spec.data?.next_sequence ?? '…'}</dd></dl></details>:null}<p className="run-hint">{action==='respond'?'Responding does not release a paused execution. Continue the Root separately.':'Signal delivery acknowledges acceptance into the mailbox, not processing by a Node.'}</p></>:null}
    {spec.isFetching && editable?<p className="run-hint" role="status">Checking current action contract…</p>:null}
    {spec.error?<p className="run-error">{spec.error.message}<button type="button" onClick={()=>void spec.refetch()}>Retry</button></p>:null}
    {error || localError?<p className="run-error" role="alert">{localError ?? error}</p>:null}
  </div><div className="run-footer"><span>{busy?'Submitting…':'Uses the current execution state'}</span><button type="button" onClick={onClose} disabled={busy}>Close</button>{action!=='respond' || wait?<button type="submit" className={action==='cancel'?'run-destructive':'run-primary'} disabled={busy || editable && (!spec.data || spec.isFetching || Boolean(spec.error))}>{busy?'Submitting…':labels[action]}</button>:null}</div></form></ActionShell>;
}

function RunAgain({trace,workflow,onClose,onAccepted}:{trace:ExecutionTrace;workflow:WorkflowGraphData;onClose:()=>void;onAccepted:(receipt:AcceptedExecution)=>void}) {
  const query=useQuery({queryKey:['run-again-input',trace.invocation.invocation_id],queryFn:({signal})=>getExecutionValue(trace.invocation.invocation_id,'invocation',trace.invocation.invocation_id,'input',signal),retry:false,staleTime:Infinity,gcTime:0});
  if(query.data?.available)return <RunWorkflowPanel workflow={workflow} initial={{entry:String(trace.invocation.entry_node_id),input:query.data.value}} onClose={onClose} onAccepted={onAccepted}/>;
  return <ActionShell title="Run again" onClose={onClose}><div className="run-fields">{query.isPending?<p role="status">Loading original invocation input…</p>:<p className="run-error">{query.error?.message ?? 'The original input is unavailable.'}<button onClick={()=>void query.refetch()}>Retry</button></p>}</div></ActionShell>;
}
