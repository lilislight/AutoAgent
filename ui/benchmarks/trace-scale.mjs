// Run `npm run benchmark:trace --prefix ui`. Each size runs in a fresh Worker.
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { TraceModel, LocalReplay } from '../.test-dist/src/traceModel.js';
import { makeTraceFixture } from './trace-fixture.mjs';
import { ReplayAccumulator } from '../.test-dist/src/replay.js';
if (isMainThread) {
  for (const count of [100_000, 1_000_000]) {
    const result = await new Promise((resolve, reject) => {
      const worker = new Worker(new URL(import.meta.url), { workerData: count });
      worker.once('message', resolve); worker.once('error', reject);
      worker.once('exit', code => { if (code) reject(new Error(`Worker exited ${code}`)); });
    });
    console.log(JSON.stringify(result));
  }
} else {
  const count=workerData, started=performance.now();
  const measure = fn => { const t=performance.now();const value=fn();return {value,ms:performance.now()-t}; };
  const {data,events,trace}=makeTraceFixture(count),{node,call}=data;
  const modelTime=measure(()=>new TraceModel(data)),model=modelTime.value;
  const fullArgs={kind:'node',start:0,end:count*100};
  const page=measure(()=>model.page(fullArgs));assert.equal(page.value.items.length,50);
  const narrowArgs={...fullArgs,start:count*40,end:count*41};
  const narrow=measure(()=>model.page(narrowArgs));assert.equal(narrow.value.items[0].id,`occ:${Math.floor(count*.08)}`);
  const groups=measure(()=>model.groups({start:0,end:count*100,buckets:256,through:count+2}));
  assert.equal(groups.value.total_count,count);assert.equal(groups.value.items.reduce((sum,g)=>sum+g.count,0),count);
  const zoomQueries=measure(()=>{
    for(let index=0;index<100;index++){
      const start=(Math.floor(count*.8)+index)*100;
      assert.equal(model.groups({start,end:start+1000,buckets:128,through:count+2}).total_count,10);
      assert.equal(model.activityBins({start,end:start+1000,buckets:128}).reduce((sum,count)=>sum+count,0),2);
    }
  });
  const initial=new ReplayAccumulator({baseline:{trace,occurrences:[],child_ids:[]}});
  const buildReplay=measure(()=>new LocalReplay(initial,events));
  const replay=buildReplay.value,seeks=[];
  for(const fraction of [1,.25,.9,.5,1]) {
    const index=Math.floor(count*fraction)-1;
    const result=measure(()=>replay.seek({sequence:events[index].sequence}));
    assert.equal(result.value.marker.sequence,index+2);
    assert.equal(result.value.frame.nodes.reduce((sum,n)=>sum+n.occurrences,0),Math.ceil((index+1)/5));
    // Each represented occurrence owns one Call; earlier Loop passes never accumulate.
    const visible=Math.min(32,Math.ceil((index+1)/5));
    assert.equal(result.value.frame.nodes.reduce((sum,n)=>sum+n.call_count,0),visible-(index%5<2?1:0));
    seeks.push({fraction,ms:+result.ms.toFixed(1)});
  }
  // A time seek and an Event seek must reconstruct the same exact graph.
  assert.deepEqual(replay.seek({at_us:count*30}).frame,replay.seek({sequence:Math.floor(count*.3)+2}).frame);
  const canonicalNodes=model.data.node,liveTimes=[];
  for(let batch=0;batch<100;batch++){
    const unit=node.length,stamp=count*100+batch*1000,id=`live:${batch}`,cursor=`live:${batch}`;
    const nodeRow={id,kind:'node',invocation_id:'i',occurrence_id:id,node_id:`node:${unit%32}`,ordinal:unit+1,start_at_us:stamp,end_at_us:stamp+10,status:'completed',values:[]};
    const next={manifest:{...model.data.manifest,cursor,base_cursor:model.data.manifest.cursor,delta:true,frontier:count+batch+1,trace:{...trace,frontier:count+batch+1},overview:{...model.data.manifest.overview,observed_at_us:stamp+20}},node:[nodeRow],call:[],wait:[],event:[{event_id:cursor,sequence:count+batch+2,occurred_at_us:stamp,event_name:'node_occurrence.completed'}],edge:[]};
    const result=measure(()=>model.apply(next));liveTimes.push(result.ms);
    assert.equal(model.data.node,canonicalNodes);
    assert.equal(result.value.nodes.reduce((sum,n)=>sum+n.occurrences,0),count/5+batch+1);
  }
  liveTimes.sort((a,b)=>a-b);
  const allocatedHeapMiB=+(process.memoryUsage().heapUsed/1048576).toFixed(1);
  globalThis.gc?.();
  parentPort.postMessage({zoomQueryMeanMs:+(zoomQueries.ms/100).toFixed(3),liveP50Ms:+liveTimes[50].toFixed(2),liveP95Ms:+liveTimes[95].toFixed(2),allocatedHeapMiB,events:count,nodeOccurrences:node.length,calls:call.length,indexMs:+modelTime.ms.toFixed(1),fullPageMs:+page.ms.toFixed(1),narrowPageMs:+narrow.ms.toFixed(1),groupsMs:+groups.ms.toFixed(1),replayIndexMs:+buildReplay.ms.toFixed(1),seeks,heapMiB:+(process.memoryUsage().heapUsed/1048576).toFixed(1),totalMs:+(performance.now()-started).toFixed(1)});
}
