import { useCallback, useEffect, useRef, useState } from 'react';
import { traceQuery } from '../traceClient';
import type { ExecutionTrace, FamilyHandoff } from '../types';

export interface HandoffCue {key:string;handoffs:FamilyHandoff[]}
export function useFamilyHandoff(trace:ExecutionTrace,active:boolean,ready:boolean,replaying:boolean,sequence?:number) {
  const [cue,setCue]=useState<HandoffCue|null>(null);
  const previous=useRef<{scope:string;sequence:number}|null>(null);
  const iid=trace.invocation.invocation_id,scope=`${trace.source_id}:${iid}:${replaying?'replay':'live'}`;
  const through=replaying?sequence:trace.last_event?.sequence;
  useEffect(()=>{
    if(!active || !ready || through===undefined){previous.current=null;setCue(null);return;}
    const last=previous.current;
    if(!last || last.scope!==scope){
      setCue(null);
      previous.current={scope,sequence:through};
      // Historical loads, navigation and reconnect baselines never emit a burst.
      if(!replaying)return;
    }
    if(replaying)setCue(null);
    if(!trace.child_count){previous.current={scope,sequence:through};return;}
    if(!replaying && last && through<=last.sequence)return;
    const abort=new AbortController();
    const timer=setTimeout(()=>{
      void traceQuery<FamilyHandoff[]>(iid,'family-links',{after:last?.scope===scope?last.sequence:through,through,exact:replaying},abort.signal)
        .then(handoffs=>{
          if(abort.signal.aborted)return;
          previous.current={scope,sequence:through};
          if(handoffs.length)setCue(current=>{
            if(!replaying && current){
              const merged=new Map(current.handoffs.map(item=>[item.key,item]));
              for(const item of handoffs)merged.set(item.key,item);
              return {key:current.key,handoffs:[...merged.values()]};
            }
            return {key:`${scope}:${through}`,handoffs};
          });
        }).catch(()=>{/* Optional navigation feedback never interrupts trace viewing. */});
    },replaying?110:80);
    return()=>{clearTimeout(timer);abort.abort();};
  },[scope,iid,through,active,ready,replaying,trace.child_count]);
  const finish=useCallback((key:string)=>setCue(current=>current?.key===key?null:current),[]);
  return {cue,finish};
}
