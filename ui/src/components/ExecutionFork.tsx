import { useEffect, useId, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, config, forkExecution, getForkPlan, listExecutionEvents, getSessionInformation, getExecutionTrace } from '../api';
import { replayPositions } from '../replayPosition';
import type { AcceptedFork, ExecutionTrace } from '../types';

export type OpenFork = (sequence:number, anchor:HTMLButtonElement, returnFocus?:HTMLElement|null) => void;
export interface ForkTarget { sequence:number; bounds:DOMRect; returnFocus:HTMLElement|null }

export function ExecutionFork({sequence,active,onFork}:{sequence:number|null;active:boolean;onFork:OpenFork}) {
  const [open,setOpen]=useState(false);
  const button=useRef<HTMLButtonElement>(null),menu=useRef<HTMLDivElement>(null),item=useRef<HTMLButtonElement>(null);
  const [position,setPosition]=useState({left:0,top:0});
  const id=useId();
  useEffect(()=>{if(!active)setOpen(false);},[active]);
  useLayoutEffect(()=>{
    if(!open || !active || !button.current || !menu.current)return;
    const trigger=button.current,panel=menu.current;
    const update=()=>{const box=trigger.getBoundingClientRect();setPosition({left:Math.max(8,Math.min(box.right-panel.offsetWidth,window.innerWidth-panel.offsetWidth-8)),top:Math.max(8,Math.min(box.bottom+5,window.innerHeight-panel.offsetHeight-8))});};
    update();item.current?.focus({preventScroll:true});
    const observer=new ResizeObserver(update);observer.observe(trigger);observer.observe(panel);
    window.addEventListener('resize',update);window.addEventListener('scroll',update,true);
    return()=>{observer.disconnect();window.removeEventListener('resize',update);window.removeEventListener('scroll',update,true);};
  },[open,active]);
  useEffect(()=>{
    if(!open)return;
    const dismiss=(event:PointerEvent)=>{if(!button.current?.contains(event.target as Node)&&!menu.current?.contains(event.target as Node))setOpen(false);};
    document.addEventListener('pointerdown',dismiss);return()=>document.removeEventListener('pointerdown',dismiss);
  },[open]);
  const close=()=>{setOpen(false);button.current?.focus({preventScroll:true});};
  return <><button ref={button} type="button" className="toolbar-action execution-more-trigger" disabled={!active}
    title="More execution actions" aria-label="More execution actions" aria-haspopup="menu" aria-controls={open?id:undefined} aria-expanded={open}
    onClick={()=>setOpen(value=>!value)} onKeyDown={event=>{if(event.key==='ArrowDown'){event.preventDefault();setOpen(true);}if(event.key==='Escape')close();}}>
    <svg viewBox="0 0 18 18" aria-hidden="true"><circle cx="4" cy="9" r="1.3"/><circle cx="9" cy="9" r="1.3"/><circle cx="14" cy="9" r="1.3"/></svg></button>
    {open && active ? createPortal(<div ref={menu} id={id} className="select-menu execution-actions-menu" role="menu" aria-label="More execution actions" style={position}
      onKeyDown={event=>{if(event.key==='Escape'){event.preventDefault();event.stopPropagation();close();}else if(event.key==='Tab')setOpen(false);else if(['ArrowDown','ArrowUp','Home','End'].includes(event.key)){event.preventDefault();item.current?.focus();}}}>
      <button ref={item} type="button" role="menuitem" disabled={sequence===null} onClick={()=>{if(sequence!==null&&button.current){onFork(sequence,button.current);setOpen(false);}}}>
        <span><strong>Fork from current position</strong><small>{sequence===null?'No saved Runtime Events':`After Event #${sequence}`}</small></span></button>
    </div>,document.body) : null}</>;
}

export function ForkDialog({trace,target,onClose,onAccepted}:{trace:ExecutionTrace;target:ForkTarget;
  onClose:()=>void;onAccepted:(value:AcceptedFork)=>void}) {
  const {sequence,bounds,returnFocus}=target;
  const dialog=useRef<HTMLDialogElement>(null);
  const title=useId(),sessionLabel=useId();
  const [session,setSession]=useState('');
  const [generated]=useState(()=>crypto.randomUUID());
  const [submittedSession,setSubmittedSession]=useState<string|null>(null);
  const [checking,setChecking]=useState(false);
  const [checkError,setCheckError]=useState<string|null>(null);
  const client=useQueryClient();
  const iid=trace.invocation.invocation_id;
  const plan=useQuery({queryKey:['fork-plan',iid,sequence],queryFn:({signal})=>getForkPlan(iid,sequence,signal),
    retry:false,staleTime:0,refetchOnWindowFocus:false});
  const mutation=useMutation({mutationFn:()=>{const chosen=session.trim()||generated;setSubmittedSession(chosen);return forkExecution(iid,sequence,chosen,new AbortController().signal);},
    retry:false,onSuccess:value=>{void client.invalidateQueries({queryKey:['execution-sessions']});onAccepted(value);}});
  const ambiguous=Boolean(mutation.error && (!(mutation.error instanceof ApiError)||mutation.error.status>=500));
  const checkCreated=async()=>{
    if(!submittedSession)return;setChecking(true);setCheckError(null);
    try {const source=await getSessionInformation(submittedSession,new AbortController().signal);
      if(!source.latest_invocation_id)throw new Error('This Session has no recorded Invocation yet.');
      const saved=await getExecutionTrace(source.latest_invocation_id,new AbortController().signal);
      const expected=plan.data?.members?.find(member=>member.source_invocation_id===trace.invocation.root_invocation_id);
      if(!expected||saved.fork?.source_invocation_id!==expected.source_invocation_id||saved.fork.source_sequence!==expected.source_sequence)
        throw new Error('This Session does not match the requested Fork.');
      onAccepted({accepted:true,session_id:submittedSession,invocation_id:saved.invocation.invocation_id,root_invocation_id:saved.invocation.root_invocation_id,
        selected_invocation_id:saved.invocation.invocation_id,selected_session_id:submittedSession,
        workflow_id:saved.invocation.workflow_id,revision_id:saved.invocation.workflow_revision_id,source_invocation_id:iid,source_sequence:sequence});
    } catch(error) {setCheckError(error instanceof Error?error.message:'Could not check Session.');} finally {setChecking(false);}
  };
  useLayoutEffect(()=>{
    const popup=dialog.current;if(!popup)return;
    popup.showModal();
    // Event popovers close before this dialog opens, so retain the clicked
    // button's rectangle rather than measuring a detached element.
    const position=()=>{popup.style.left=`${Math.max(12,Math.min(bounds.right-popup.offsetWidth,window.innerWidth-popup.offsetWidth-12))}px`;
      popup.style.top=`${Math.max(12,Math.min(bounds.bottom+8,window.innerHeight-popup.offsetHeight-12))}px`;};
    position();const observer=new ResizeObserver(position);observer.observe(popup);
    window.addEventListener('resize',position);
    return()=>{observer.disconnect();window.removeEventListener('resize',position);popup.close();if(returnFocus?.isConnected)returnFocus.focus({preventScroll:true});};
  },[bounds,returnFocus]);
  return <dialog ref={dialog} className="fork-dialog" aria-labelledby={title}
    onCancel={event=>{if(mutation.isPending)event.preventDefault();else onClose();}}
    onClick={event=>{const box=dialog.current?.getBoundingClientRect();if(event.target===dialog.current&&box&&!mutation.isPending&&(event.clientX<box.left||event.clientX>box.right||event.clientY<box.top||event.clientY>box.bottom))onClose();}}>
    <header><strong id={title}>Fork after Event #{sequence}</strong><button type="button" aria-label="Close Fork" disabled={mutation.isPending} onClick={onClose}>×</button></header>
    <p>The Root and its related children become an independent branch. The source keeps running.</p>
    {plan.isPending ? <p role="status">Checking the historical position…</p> : null}
    {plan.error ? <p role="alert">{plan.error.message}</p> : null}
    {plan.data?.available ? <><dl className="fork-plan"><dt>Related Sessions</dt><dd>{plan.data.members?.length}</dd>
      <dt>Calls to replay</dt><dd>{plan.data.replay_calls?.length ?? 0}</dd><dt>Timers</dt><dd>Keep absolute deadlines</dd></dl>
      {plan.data.replay_calls?.map(call=><p className="fork-replay-call" key={call.call_id}><code>{call.operator_id}</code><small>Will execute again</small></p>)}
      <label htmlFor={sessionLabel}>New Root Session ID <small>Optional</small></label><input id={sessionLabel} value={session} maxLength={4096} onChange={event=>setSession(event.target.value)} placeholder="Generate automatically" disabled={mutation.isPending}/></> : null}
    {plan.data&&!plan.data.available ? <p role="alert">{plan.data.reason} Choose another Runtime Event.</p> : null}
    {mutation.error ? <p role="alert">{mutation.error.message}</p> : null}
    {ambiguous?<div className="inspector-notice"><p>Creation could not be confirmed. Check this Session before creating another Fork.</p><code>{submittedSession}</code><button type="button" disabled={checking} onClick={()=>void checkCreated()}>{checking?'Checking…':'Check created Session'}</button>{checkError?<p role="alert">{checkError}</p>:null}</div>:null}
    <footer><button type="button" disabled={mutation.isPending} onClick={onClose}>Cancel</button><button type="button" className="primary-button" disabled={!plan.data?.available||mutation.isPending||ambiguous} onClick={()=>mutation.mutate()}>{mutation.isPending?'Creating…':'Create Fork'}</button></footer>
  </dialog>;
}

export function ForkHistory({trace,onOpen}:{trace:ExecutionTrace;onOpen:(iid:string)=>void}) {
  const [expanded,setExpanded]=useState(false);
  const origin=trace.fork;
  const events=useInfiniteQuery({queryKey:['fork-source-events',origin?.source_invocation_id,origin?.source_sequence],
    queryFn:({pageParam,signal})=>listExecutionEvents(origin!.source_invocation_id,pageParam,signal),
    initialPageParam:null as string|null,getNextPageParam:page=>page.items.some(e=>Number(e.sequence)>origin!.source_sequence)?undefined:page.next_cursor,
    enabled:expanded&&Boolean(origin),staleTime:Infinity,refetchOnWindowFocus:false});
  useEffect(()=>setExpanded(false),[trace.invocation.invocation_id]);
  if(!origin)return null;
  const rows=events.data?.pages.flatMap(p=>p.items).filter(e=>Number(e.sequence)<=origin.source_sequence)??[];
  return <div className="fork-history"><div className="fork-history-heading"><button type="button" aria-expanded={expanded} onClick={()=>setExpanded(value=>!value)}>{expanded?'▾':'▸'} Inherited history · through Source Event #{origin.source_sequence}</button>
    <button type="button" onClick={()=>{replayPositions.request(`${config.apiPrefix}|${trace.source_id ?? 'unknown'}|${origin.source_invocation_id}`,{sequence:origin.source_sequence});onOpen(origin.source_invocation_id);}}>Open source at Fork ↗</button></div>
    {expanded?<div className="fork-source-events">{rows.map(e=><div key={String(e.event_id)}><code>Source #{e.sequence}</code><span>{String(e.event_name)}</span></div>)}
      {events.isPending?<p>Loading source Event headers…</p>:null}{events.error?<p role="alert">{events.error.message}</p>:null}
      {events.hasNextPage?<button disabled={events.isFetchingNextPage} onClick={()=>void events.fetchNextPage()}>More source Events</button>:null}
      <p className="fork-boundary">Fork boundary · New execution time starts here. Earlier User Events are not inherited.</p></div>:null}</div>;
}
