import { useEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { config, request } from '../http';
import { currentTrace, retainTrace, syncTrace } from '../traceClient';
import { ObservationClock, observationInterval } from '../liveObservation';
import type { ExecutionTrace, TraceHead } from '../types';

export function useLiveTrace(trace: ExecutionTrace, ready: boolean, active: boolean, paused: boolean, onApplied: (trace: ExecutionTrace) => void) {
  const iid=trace.invocation.invocation_id, client=useQueryClient();
  const [visible,setVisible]=useState(()=>!document.hidden);
  const [phase,setPhase]=useState<'idle'|'connecting'|'live'|'reconnecting'>('idle');
  const [updating,setUpdating]=useState(false);
  const [hasUpdates,setHasUpdates]=useState(false);
  const [error,setError]=useState<string|null>(null);
  const [clock]=useState(()=>new ObservationClock(trace.observed_at_us));
  const [attempt,setAttempt]=useState(0);
  const latest=useRef<TraceHead|null>(null);
  const family=useRef<{store:string;version:number}|null>(null);
  const options=useRef({paused,onApplied,active,visible,ready,trace});
  options.current={paused,onApplied,active,visible,ready,trace};
  const drain=useRef<()=>void>(()=>{});
  const check=useRef<()=>Promise<void>>(async()=>{});
  useEffect(()=>{const update=()=>setVisible(!document.hidden);document.addEventListener('visibilitychange',update);return()=>document.removeEventListener('visibilitychange',update);},[]);
  useEffect(()=>{if(!paused)drain.current();},[paused]);
  useEffect(()=>{
    if(!active || !visible || !ready){setPhase('idle');return;}
    const release=retainTrace(iid),abort=new AbortController();
    let stopped=false,busy=false,source:EventSource|null=null,poll:ReturnType<typeof setTimeout>|undefined,delay=2000,polling=false;
    const allowed=()=>!stopped && !abort.signal.aborted && options.current.active && options.current.visible && options.current.ready && !options.current.paused;
    const updateFamily=(head:TraceHead)=>{
      if(family.current && (family.current.store!==head.store_id || family.current.version<head.family_version)){
        void client.invalidateQueries({predicate:query=>['execution-family','family-children','execution-controls'].includes(String(query.queryKey[0])) || query.queryKey[0]==='inspector-items' && query.queryKey[2]==='child',refetchType:'active'});
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
      latest.current=head;clock.calibrate(head.server_now_us,!options.current.paused);
      setError(null);setPhase('live');
      const local=currentTrace(iid);
      const changed=Boolean(local && (local.source_id!==head.store_id || (local.invocation_version ?? 0)<head.invocation_version)) || Boolean(family.current && (family.current.store!==head.store_id || family.current.version<head.family_version));
      if(changed)setHasUpdates(true);
      void apply();
    };
    const pollHead=async()=>{
      try { const head=await request<TraceHead>(`/trace-head?${new URLSearchParams({invocation_id:iid})}`,abort.signal);if(polling)receive(head);delay=2000; }
      catch { if(!stopped && polling)setPhase('reconnecting'); delay=Math.min(30000,delay*2); }
      finally { if(!stopped && polling)poll=setTimeout(pollHead,delay); }
    };
    const fallback=()=>{if(!polling){polling=true;poll=setTimeout(pollHead,0);}};
    check.current=async()=>{
      try { receive(await request<TraceHead>(`/trace-head?${new URLSearchParams({invocation_id:iid})}`,abort.signal));await apply(); }
      catch(failure) { if(!stopped && !abort.signal.aborted){setError(failure instanceof Error?failure.message:'Could not check live updates.');setPhase('reconnecting');fallback();} }
    };
    setPhase('connecting');
    if(typeof EventSource==='undefined')fallback();
    else{
      source=new EventSource(`${config.apiPrefix}/trace-watch?${new URLSearchParams({invocation_id:iid})}`);
      const message=(event:MessageEvent<string>)=>{try{receive(JSON.parse(event.data) as TraceHead);}catch{setError('Invalid live update. Reconnect to retry.');}};
      source.addEventListener('update',message as EventListener);source.addEventListener('heartbeat',message as EventListener);
      source.addEventListener('unavailable',()=>{setPhase('reconnecting');fallback();});
      source.onopen=()=>{if(!stopped){setPhase('live');polling=false;if(poll)clearTimeout(poll);poll=undefined;}};
      source.onerror=()=>{if(!stopped){setPhase('reconnecting');fallback();}};
    }
    return()=>{stopped=true;abort.abort();source?.close();if(poll)clearTimeout(poll);release();drain.current=()=>{};check.current=async()=>{};};
  },[iid,active,visible,ready,client,clock,attempt]);
  const interval=observationInterval(trace);
  useEffect(()=>{
    if(!active || !visible || paused || !ready || phase!=='live' || !interval)return;
    clock.tick();const timer=setInterval(clock.tick,interval);return()=>clearInterval(timer);
  },[active,visible,paused,ready,phase,interval,clock]);
  return {phase,updating,hasUpdates,error,clock,paused,visible,observing:active && visible && ready && phase==='live',
    refresh:()=>check.current(),retry:()=>{setError(null);setAttempt(value=>value+1);}};
}
export type LiveTrace = ReturnType<typeof useLiveTrace>;
export function LiveTraceStatus({live}:{live:LiveTrace}) {
  const label=live.paused ? live.hasUpdates?'Updates available':'Live held' : live.phase==='connecting'?'Connecting…':live.phase==='reconnecting'?'Reconnecting…':'Live';
  return <div className={`live-trace-status ${live.phase==='live' && !live.paused?'is-live':''}`}>
    <span role="status" title={live.error ?? (live.paused?'Updates are held at the Replay boundary.':'Observing committed data. Live does not indicate that the Runtime is executing.')}><i aria-hidden="true" />{live.updating?'Updating…':label}</span>
    {live.error?<button onClick={live.retry} title={live.error}>Retry</button>:null}
  </div>;
}
