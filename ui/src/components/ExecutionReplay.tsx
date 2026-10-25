import { useEffect, useLayoutEffect, useRef, useState, type CSSProperties } from 'react';
import { replayPositions } from '../replayPosition';
import { config } from '../api';
import { useTraceProgress } from './useTraceProgress';
import { seekLocalReplay, releaseLocalReplay, localReplayReady } from '../traceClient';
import type { ExecutionTrace, LocalReplayResult } from '../types';

export function useExecutionReplay(trace: ExecutionTrace, active: boolean) {
  const [state,setState]=useState<{enabled:boolean;loading:boolean;seeking:boolean;error:string|null;result:LocalReplayResult|null}>({enabled:false,loading:false,seeking:false,error:null,result:null});
  const positionKey = `${config.apiPrefix}|${trace.source_id ?? 'unknown'}|${trace.invocation.invocation_id}`;
  const controller=useRef<AbortController|null>(null);
  const pinned=useRef<ExecutionTrace|null>(null);
  const cancel=()=>controller.current?.abort();
  useEffect(()=>()=>{cancel();releaseLocalReplay(pinned.current);},[]);
  useEffect(()=>{if(!active){cancel();setState(current=>({...current,loading:false,seeking:false}));}},[active]);
  const seek=(position:{sequence?:number;at_us?:number})=>{
    if(!active || !trace.last_event)return;
    replayPositions.write(positionKey, position);
    cancel();pinned.current??=trace;
    const abort=new AbortController();controller.current=abort;
    setState(current=>({...current,enabled:true,seeking:true,loading:!localReplayReady(pinned.current!),error:null}));
    void seekLocalReplay(pinned.current,position,abort.signal).then(result=>{
      if(!abort.signal.aborted)setState({enabled:true,loading:false,seeking:false,error:null,result});
    }).catch(error=>{if(!abort.signal.aborted)setState(current=>({...current,loading:false,seeking:false,error:error instanceof Error?error.message:'Could not load Replay.'}));});
  };
  const stop=()=>{cancel();releaseLocalReplay(pinned.current);pinned.current=null;setState({enabled:false,loading:false,seeking:false,error:null,result:null});};
  // Keep the rendered historical frame while allowing the metadata cache to catch up.
  const prepareReturn=()=>{cancel();releaseLocalReplay(pinned.current);setState(current=>({...current,loading:false,seeking:false}));};
  useEffect(()=>{if(active){const requested=replayPositions.takeRequested(positionKey);if(requested)seek(requested);}},[active,positionKey]);
  const step=(direction:-1|1)=>{const marker=direction===-1?state.result?.previous:state.result?.next;if(marker)seek({sequence:marker.sequence});};
  return {...state,resumePosition:()=>replayPositions.read(positionKey),invocationId:trace.invocation.invocation_id,frame:state.result?.frame??null,marker:state.result?.marker??null,cursorTime:state.result?.cursorTime??null,previous:state.result?.previous??null,next:state.result?.next??null,seek,stop,prepareReturn,step};
}
export type ExecutionReplay = ReturnType<typeof useExecutionReplay>;
function ReplayEventLabel({sequence,name}:{sequence:number;name:string}) {
  const container=useRef<HTMLSpanElement>(null), text=useRef<HTMLSpanElement>(null);
  const [overflow,setOverflow]=useState(0);
  useLayoutEffect(()=>{
    if(!container.current || !text.current)return;
    const measure=()=>setOverflow(Math.max(0,text.current!.scrollWidth-container.current!.clientWidth));
    measure();const observer=new ResizeObserver(measure);observer.observe(container.current);observer.observe(text.current);
    return()=>observer.disconnect();
  },[sequence,name]);
  return <span ref={container} className={`replay-event-label ${overflow>0?'is-overflowing':''}`} title={`Event #${sequence} · ${name}`} style={{'--event-scroll':`-${overflow}px`} as CSSProperties}><span className="replay-event-label-content" key={`${sequence}:${name}`} ref={text}><span>Event #{sequence}</span><code>{name}</code></span></span>;
}
export function GraphReplayStatus({ replay, returning=false }: { replay: ExecutionReplay; returning?:boolean }) {
  const progress = useTraceProgress(`replay:${replay.invocationId}`);
  const [showReturning,setShowReturning]=useState(false);
  useEffect(()=>{
    if(!returning){setShowReturning(false);return;}
    const timer=setTimeout(()=>setShowReturning(true),250);
    return()=>clearTimeout(timer);
  },[returning]);
  if (!replay.enabled) return null;
  if(returning && showReturning)return <div className="graph-replay-status" role="status">Returning to latest…</div>;
  return <div className="graph-replay-status" role="status"><span className="replay-tag">Replaying</span><div className="replay-event-navigation"><button className="replay-event-step" aria-label="Previous Replay Event" disabled={!replay.previous || replay.loading} onClick={() => replay.step(-1)}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><path d="m10 3-5 5 5 5" /></svg></button><span className="replay-event-position" title={replay.marker?.event_name}>{replay.loading && !returning ? progress : replay.error ? 'Replay unavailable' : replay.marker ? <ReplayEventLabel sequence={replay.marker.sequence} name={replay.marker.event_name} /> : 'Select an Event'}</span><button className="replay-event-step" aria-label="Next Replay Event" disabled={!replay.next || replay.loading} onClick={() => replay.step(1)}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><path d="m6 3 5 5-5 5" /></svg></button></div><button disabled={returning} onClick={replay.stop}>End replay</button>{replay.error ? <small role="alert">{replay.error}</small> : null}</div>;
}
