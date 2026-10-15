import { useEffect, useRef, useState } from 'react';
import { useTraceProgress } from './useTraceProgress';
import { seekLocalReplay, releaseLocalReplay, localReplayReady } from '../traceClient';
import type { ExecutionTrace, LocalReplayResult } from '../types';

export function useExecutionReplay(trace: ExecutionTrace, active: boolean) {
  const [state,setState]=useState<{enabled:boolean;loading:boolean;error:string|null;result:LocalReplayResult|null}>({enabled:false,loading:false,error:null,result:null});
  const controller=useRef<AbortController|null>(null);
  const pinned=useRef<ExecutionTrace|null>(null);
  const cancel=()=>controller.current?.abort();
  useEffect(()=>()=>{cancel();releaseLocalReplay(pinned.current);},[]);
  useEffect(()=>{if(!active){cancel();setState(current=>({...current,loading:false}));}},[active]);
  const seek=(position:{sequence?:number;at_us?:number})=>{
    if(!active || !trace.last_event)return;
    cancel();pinned.current??=trace;
    const abort=new AbortController();controller.current=abort;
    setState(current=>({...current,enabled:true,loading:!localReplayReady(pinned.current!),error:null}));
    void seekLocalReplay(pinned.current,position,abort.signal).then(result=>{
      if(!abort.signal.aborted)setState({enabled:true,loading:false,error:null,result});
    }).catch(error=>{if(!abort.signal.aborted)setState(current=>({...current,loading:false,error:error instanceof Error?error.message:'Could not load Replay.'}));});
  };
  const stop=()=>{cancel();releaseLocalReplay(pinned.current);pinned.current=null;setState({enabled:false,loading:false,error:null,result:null});};
  const step=(direction:-1|1)=>{const marker=direction===-1?state.result?.previous:state.result?.next;if(marker)seek({sequence:marker.sequence});};
  return {...state,invocationId:trace.invocation.invocation_id,frame:state.result?.frame??null,marker:state.result?.marker??null,cursorTime:state.result?.cursorTime??null,previous:state.result?.previous??null,next:state.result?.next??null,seek,stop,step};
}
export type ExecutionReplay = ReturnType<typeof useExecutionReplay>;
export function GraphReplayStatus({ replay }: { replay: ExecutionReplay }) {
  const progress = useTraceProgress(`replay:${replay.invocationId}`);
  if (!replay.enabled) return null;
  return <div className="graph-replay-status" role="status"><span className="replay-tag">Replay</span><div className="replay-event-navigation"><button className="replay-event-step" aria-label="Previous Replay Event" disabled={!replay.previous || replay.loading} onClick={() => replay.step(-1)}>‹</button><span className="replay-event-position" title={replay.marker?.event_name}>{replay.loading ? progress : replay.error ? 'Replay unavailable' : replay.marker ? `Event #${replay.marker.sequence}` : 'Select an Event'}</span><button className="replay-event-step" aria-label="Next Replay Event" disabled={!replay.next || replay.loading} onClick={() => replay.step(1)}>›</button></div><button onClick={replay.stop}>End replay</button>{replay.error ? <small role="alert">{replay.error}</small> : null}</div>;
}
