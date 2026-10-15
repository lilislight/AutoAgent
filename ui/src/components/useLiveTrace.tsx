import { useEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { config, request } from '../http';
import { currentTrace, retainTrace, syncTrace } from '../traceClient';
import type { ExecutionTrace, TraceHead } from '../types';

export function useLiveTrace(trace: ExecutionTrace, ready: boolean, active: boolean, paused: boolean, onApplied: (trace: ExecutionTrace) => void) {
  const iid=trace.invocation.invocation_id, client=useQueryClient();
  const [enabled,setEnabled]=useState(()=>!['completed','failed','cancelled'].includes(trace.invocation.status ?? '') || Boolean(trace.child_count || trace.invocation.parent_invocation_id));
  const [visible,setVisible]=useState(()=>!document.hidden);
  const [phase,setPhase]=useState<'snapshot'|'connecting'|'live'|'reconnecting'>('snapshot');
  const [updating,setUpdating]=useState(false);
  const [hasUpdates,setHasUpdates]=useState(false);
  const [error,setError]=useState<string|null>(null);
  const [nowUs,setNowUs]=useState(trace.observed_at_us);
  const clock=useRef({server:trace.observed_at_us,local:performance.now()});
  const latest=useRef<TraceHead|null>(null);
  const family=useRef<{store:string;version:number}|null>(null);
  const options=useRef({paused,onApplied,active,enabled,visible,ready,trace});
  options.current={paused,onApplied,active,enabled,visible,ready,trace};
  const drain=useRef<()=>void>(()=>{});
  useEffect(()=>{const update=()=>setVisible(!document.hidden);document.addEventListener('visibilitychange',update);return()=>document.removeEventListener('visibilitychange',update);},[]);
  useEffect(()=>{if(!paused)drain.current();},[paused]);
  useEffect(()=>{
    if(!active || !enabled || !visible || !ready){setPhase('snapshot');return;}
    const release=retainTrace(iid),abort=new AbortController();
    let stopped=false,busy=false,source:EventSource|null=null,poll:ReturnType<typeof setInterval>|undefined;
    const allowed=()=>!stopped && !abort.signal.aborted && options.current.active && options.current.enabled && options.current.visible && options.current.ready && !options.current.paused;
    const updateFamily=(head:TraceHead)=>{
      if(family.current && (family.current.store!==head.store_id || family.current.version<head.family_version)){
        void client.invalidateQueries({predicate:query=>['execution-family','family-children'].includes(String(query.queryKey[0])) || query.queryKey[0]==='inspector-items' && query.queryKey[2]==='child',refetchType:'active'});
      }
      family.current={store:head.store_id,version:head.family_version};
    };
    const apply=async()=>{
      if(busy || !allowed() || !latest.current)return;
      busy=true;
      try{
        while(allowed() && latest.current){
          const head:TraceHead=latest.current;const local=currentTrace(iid);
          if(!local)break;
          const changed=local.source_id!==head.store_id || (local.invocation_version ?? 0)<head.invocation_version;
          if(changed){
            setUpdating(true);
            const result=await syncTrace(iid,abort.signal,allowed);
            if(!result || !allowed())break;
            options.current.onApplied(result);
            const changedStatus=local.invocation.status!==result.invocation.status;
            if(changedStatus)void client.invalidateQueries({queryKey:['session-invocations-page',result.invocation.session_id],refetchType:'active'});
          }
          if(!changed && (local.source_id!==options.current.trace.source_id || local.last_event?.event_id!==options.current.trace.last_event?.event_id))options.current.onApplied(local);
          updateFamily(head);
          if(latest.current===head){setHasUpdates(false);break;}
        }
      }catch(failure){if(!stopped && !abort.signal.aborted){setError(failure instanceof Error?failure.message:'Could not load live updates.');setPhase('reconnecting');}}
      finally{busy=false;if(!stopped)setUpdating(false);}
    };
    drain.current=()=>void apply();
    const receive=(head:TraceHead)=>{
      if(stopped || head.invocation_id!==iid)return;
      const previous=latest.current;
      if(previous?.store_id===head.store_id && (head.invocation_version<previous.invocation_version || head.family_version<previous.family_version))return;
      latest.current=head;clock.current={server:head.server_now_us,local:performance.now()};
      if(!options.current.paused)setNowUs(current=>Math.max(current,head.server_now_us));
      setError(null);setPhase(head.active?'live':'snapshot');
      const local=currentTrace(iid);
      const changed=Boolean(local && (local.source_id!==head.store_id || (local.invocation_version ?? 0)<head.invocation_version)) || Boolean(family.current && (family.current.store!==head.store_id || family.current.version<head.family_version));
      if(changed)setHasUpdates(true);
      void apply();
      if(!head.active){source?.close();if(poll)clearInterval(poll);poll=undefined;}
    };
    const pollHead=()=>void request<TraceHead>(`/trace-head?${new URLSearchParams({invocation_id:iid})}`,abort.signal).then(receive).catch(()=>{if(!stopped)setPhase('reconnecting');});
    const fallback=()=>{if(!poll){pollHead();poll=setInterval(pollHead,2000);}};
    setPhase('connecting');
    if(typeof EventSource==='undefined')fallback();
    else{
      source=new EventSource(`${config.apiPrefix}/trace-watch?${new URLSearchParams({invocation_id:iid})}`);
      const message=(event:MessageEvent<string>)=>{try{receive(JSON.parse(event.data) as TraceHead);}catch{setError('Invalid live update. Reconnect to retry.');}};
      source.addEventListener('update',message as EventListener);source.addEventListener('heartbeat',message as EventListener);
      source.addEventListener('unavailable',()=>{setPhase('reconnecting');fallback();});
      source.onopen=()=>{if(!stopped){setPhase('live');if(poll)clearInterval(poll);poll=undefined;}};
      source.onerror=()=>{if(!stopped){setPhase('reconnecting');fallback();}};
    }
    return()=>{stopped=true;abort.abort();source?.close();if(poll)clearInterval(poll);release();drain.current=()=>{};};
  },[iid,active,enabled,visible,ready,client]);
  useEffect(()=>{
    if(!active || !enabled || !visible || paused || !ready || !['live','connecting'].includes(phase) || ['completed','failed','cancelled'].includes(trace.invocation.status ?? ''))return;
    const tick=()=>setNowUs(current=>Math.max(current,clock.current.server+(performance.now()-clock.current.local)*1000));
    tick();const timer=setInterval(tick,500);return()=>clearInterval(timer);
  },[active,enabled,visible,paused,ready,phase,trace.invocation.status]);
  const settled=latest.current?.active===false || !trace.child_count && !trace.invocation.parent_invocation_id && ['completed','failed','cancelled'].includes(trace.invocation.status ?? '');
  return {enabled,phase,updating,hasUpdates,error,nowUs,paused,settled,
    toggle:()=>setEnabled(value=>!value),retry:()=>{setError(null);drain.current();}};
}
export type LiveTrace = ReturnType<typeof useLiveTrace>;
export function LiveTraceStatus({live}:{live:LiveTrace}) {
  const [open,setOpen]=useState(false),container=useRef<HTMLDivElement>(null);
  useEffect(()=>{if(!open)return;const close=(event:PointerEvent)=>{if(!container.current?.contains(event.target as Node))setOpen(false);};document.addEventListener('pointerdown',close);return()=>document.removeEventListener('pointerdown',close);},[open]);
  const label=live.paused && live.enabled?live.hasUpdates?'Updates available':'Live held':live.phase==='connecting'?'Connecting…':live.phase==='reconnecting'?'Reconnecting…':live.phase==='live'?'Live':'Snapshot';
  return <div ref={container} className={`live-trace-status ${live.phase==='live' && !live.paused?'is-live':''}`} onKeyDown={event=>{if(event.key==='Escape'){setOpen(false);event.stopPropagation();}}}>
    <span role="status" title={live.error ?? (live.paused?'Live updates are held at the Replay boundary.':'Only committed data is shown.')}><i aria-hidden="true" />{live.updating?'Updating…':label}</span>
    {!live.settled ? <button className="live-options" aria-label="Live observation options" aria-haspopup="menu" aria-expanded={open} onClick={()=>setOpen(value=>!value)}>⌄</button> : null}
    {open && !live.settled?<div className="live-updates-dropdown" role="menu"><button role="menuitem" onClick={()=>{live.toggle();setOpen(false);}} title="Changes page updates only; does not control Workflow execution">{live.enabled?'Pause live updates':'Resume live updates'}</button></div>:null}
    {live.error?<button onClick={live.retry} title={live.error}>Retry</button>:null}
  </div>;
}
