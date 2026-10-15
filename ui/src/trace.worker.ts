import type { TraceDataset, ReplayEvent, ReplayWindow } from './types';
import { TraceModel, LocalReplay } from './traceModel';
import { ReplayAccumulator } from './replay';
const datasets=new Map<string,TraceModel>();
const replays=new Map<string,LocalReplay>();
self.onmessage=(event:MessageEvent)=>{
  const {id,op,key,args}=event.data;
  try{
    let value:unknown;
    if(op==='load'){datasets.set(key,new TraceModel(args as TraceDataset));value=true;}
    else if(op==='drop'){datasets.delete(key);replays.delete(key);value=true;}
    else if(op==='drop-replay'){replays.delete(key);value=true;}
    else if(op==='replay-load'){
      const {manifest,events}=args;const window={baseline:{trace:manifest.initial_trace,occurrences:[],child_ids:[]},history_complete:true} as unknown as ReplayWindow;
      replays.set(key,new LocalReplay(new ReplayAccumulator(window),events as ReplayEvent[]));value=true;
    }else if(op==='replay-seek')value=replays.get(key)!.seek(args);
    else{const model=datasets.get(key);if(!model)throw new Error('Trace data is not loaded.');
      if(op==='data')value=model.data;
      else if(op==='overview')value=model.overview(args.observed);
      else if(op==='apply'){const trace=model.apply(args as TraceDataset);value={trace,manifest:model.data.manifest,bytesDelta:model.bytesDelta};}
      else if(op==='page')value=model.page(args);
      else if(op==='position')value=model.position(args);
      else if(op==='groups')value=model.groups(args);
      else if(op==='markers')value=model.markers(args);
      else throw new Error('Unknown local trace query.');
    }
    self.postMessage({id,value});
  }catch(error){self.postMessage({id,error:error instanceof Error?error.message:String(error)});}
};
