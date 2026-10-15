import type { TraceDataset, TimelineRow, TimelineMarker, TimelinePage, TimelineEventGroup, ReplayEvent, LocalReplayResult } from './types.js';
import { spanGeometry } from './timeline.js';
import { ReplayAccumulator } from './replay.js';
import { LiveGraph } from './liveGraph.js';

export class TraceModel {
  readonly nodes = new Map<string, TimelineRow>();
  readonly calls = new Map<string, TimelineRow[]>();
  readonly waits = new Map<string, TimelineRow>();
  private callIndex = new Map<string,TimelineRow>();
  private eventIds = new Set<string>();
  private starts: number[] = [];
  private counts = {node:0,call:0,wait:0};
  private untimed = 0;
  private recordedEnd = 0;
  private graph: LiveGraph;
  bytesDelta = 0;
  constructor(readonly data: TraceDataset) {
    for (const row of data.node) this.nodes.set(row.id, row);
    for (const row of data.wait) this.waits.set(row.id, row);
    for (const row of data.call) {
      this.callIndex.set(row.id,row);
      const rows = this.calls.get(row.occurrence_id) ?? [];
      rows.push(row); this.calls.set(row.occurrence_id, rows);
    }
    for (const rows of this.calls.values()) rows.sort((a,b)=>a.start_at_us-b.start_at_us || a.id.localeCompare(b.id));
    data.node.sort((a,b)=>(a.ready_at_us ?? a.start_at_us)-(b.ready_at_us ?? b.start_at_us) || a.id.localeCompare(b.id));
    data.event.sort((a,b)=>a.sequence-b.sequence);
    for(const row of data.event)this.eventIds.add(row.event_id);
    this.starts=data.node.filter(row=>row.status!=='skipped' && row.start_at_us!=null).map(row=>row.start_at_us);
    for(const kind of ['node','call','wait'] as const)for(const row of data[kind])this.timing(kind,row,1);
    this.starts.sort((a,b)=>a-b);
    this.graph=new LiveGraph(data);
  }
  private lower(value:number) {
    let lo=0,hi=this.starts.length;while(lo<hi){const mid=(lo+hi)>>>1;if(this.starts[mid]<value)lo=mid+1;else hi=mid;}return lo;
  }
  private timing(kind:'node'|'call'|'wait',row:TimelineRow,sign:number) {
    if(kind==='node' && row.status==='skipped')return;
    this.counts[kind]+=sign;
    if(row.start_at_us==null)this.untimed+=sign;
    if(sign>0){this.recordedEnd=Math.max(this.recordedEnd,row.start_at_us ?? 0,row.end_at_us ?? 0);}
  }
  overview(observed=this.data.manifest.overview.observed_at_us) {
    const base=this.data.manifest.overview, inv=this.data.manifest.trace.invocation;
    const terminal=['completed','failed','cancelled'].includes(inv.status ?? '');
    const start=base.start_at_us;
    const end=start==null?null:Math.max(start,this.recordedEnd,inv.completed_at_us ?? 0,terminal?0:observed);
    const bins:number[]=[];
    if(start!=null && end!=null)for(let index=0;index<64;index++){
      const left=index?this.lower(start+(end-start)*index/64):0;
      const right=index===63?this.starts.length:this.lower(start+(end-start)*(index+1)/64);
      bins.push(right-left);
    }
    return {...base,start_at_us:start,end_at_us:end,observed_at_us:observed,counts:{...this.counts},untimed_count:this.untimed,node_start_bins:bins};
  }
  apply(delta:TraceDataset) {
    const old=this.data.manifest,next=delta.manifest;
    this.bytesDelta=0;
    if(next.cursor===old.cursor)return old.trace;
    if(!next.delta || next.base_cursor!==old.cursor || next.source_id!==old.source_id || next.frontier<old.frontier)throw new Error('Trace delta does not match the current snapshot. Reload this execution.');
    // Validate the entire batch before mutating. Duplicate/out-of-order commits
    // must never double-count Calls, occurrences or Edge evaluations.
    for(const kind of ['node','call','wait'] as const)for(const row of delta[kind])if(row.kind!==kind || row.invocation_id!==old.invocation_id)throw new Error('Trace row belongs to another execution.');
    const batchEvents=new Set<string>();
    for(const marker of delta.event){
      if(this.eventIds.has(marker.event_id) || batchEvents.has(marker.event_id))throw new Error('Trace delta repeats an already applied Event.');
      batchEvents.add(marker.event_id);
    }
    const callGroups=new Set<string>();
    for(const kind of ['node','call','wait'] as const)for(const row of delta[kind]){
      const index=kind==='node'?this.nodes:kind==='call'?this.callIndex:this.waits;
      const previous=index.get(row.id);
      this.bytesDelta+=(JSON.stringify(row).length-(previous?JSON.stringify(previous).length:0))*2;
      if(previous)this.timing(kind,previous,-1);
      if(kind==='node'){
        if(previous && previous.status!=='skipped' && previous.start_at_us!=null){const at=this.lower(previous.start_at_us);if(this.starts[at]===previous.start_at_us)this.starts.splice(at,1);}
        if(row.status!=='skipped' && row.start_at_us!=null)this.starts.splice(this.lower(row.start_at_us),0,row.start_at_us);
        this.graph.node(row,previous);
      }
      this.timing(kind,row,1);
      if(kind==='call' && previous && previous.start_at_us!==row.start_at_us)callGroups.add(row.occurrence_id);
      if(previous){if(kind==='node' && !row.activations)delete previous.activations;Object.assign(previous,row);}
      else {
        index.set(row.id,row);
        if(kind==='node'){
          const rows=this.data.node;let lo=0,hi=rows.length;const time=row.ready_at_us ?? row.start_at_us;
          while(lo<hi){const mid=(lo+hi)>>>1,other=rows[mid],stamp=other.ready_at_us ?? other.start_at_us;if(stamp<time || stamp===time && other.id<row.id)lo=mid+1;else hi=mid;}
          rows.splice(lo,0,row);
        }else this.data[kind].push(row);
        if(kind==='call'){
          const rows=this.calls.get(row.occurrence_id) ?? [];rows.push(row);callGroups.add(row.occurrence_id);this.calls.set(row.occurrence_id,rows);
          this.graph.call(row,this.nodes.get(row.occurrence_id)?.node_id);
        }
      }
    }
    for(const id of callGroups)this.calls.get(id)!.sort((a,b)=>a.start_at_us-b.start_at_us || a.id.localeCompare(b.id));
    for(const marker of delta.event){this.bytesDelta+=JSON.stringify(marker).length*2;this.eventIds.add(marker.event_id);this.data.event.push(marker);}
    this.graph.updateEdges(delta.edge ?? []);
    this.data.manifest={...next,delta:false,base_cursor:null,trace:this.graph.frame(next.trace)};
    this.data.manifest.overview=this.overview();
    return this.data.manifest.trace;
  }
  rows(args: { kind: string; start: number; end: number; occurrence?: string | null; nodeId?: string | null }) {
    const all = args.kind === 'node' ? this.data.node : args.kind === 'call' ? this.calls.get(args.occurrence ?? '') ?? [] : this.data.wait.filter(row=>row.occurrence_id===args.occurrence);
    return all.filter(row=>!(args.kind==='node' && row.status==='skipped') && (!args.nodeId || row.node_id===args.nodeId) && spanGeometry(row,{start:args.start,end:args.end},Math.max(args.end,this.data.manifest.overview.observed_at_us)).visible);
  }
  call(row: TimelineRow): TimelineRow {
    return { ...row, node_id:this.nodes.get(row.occurrence_id)?.node_id, wait:this.waits.get(row.id) ?? null };
  }
  page(args: Parameters<TraceModel['rows']>[0] & { cursor?: string | null }): TimelinePage {
    const rows=this.rows(args), size=args.kind==='node'?50:20, index=Number(args.cursor ?? 0);
    if (!Number.isSafeInteger(index) || index<0) throw new Error('Invalid local Timeline cursor.');
    const items=rows.slice(index,index+size).map(row=>args.kind==='call'?this.call(row):args.kind==='node'?{...row, call_count:this.calls.get(row.id)?.length ?? row.call_count,
      preview_calls:(this.calls.get(row.id)??[]).filter(call=>spanGeometry(call,{start:args.start,end:args.end},Math.max(args.end,this.data.manifest.overview.observed_at_us)).visible).slice(0,6).map(call=>this.call(call))}:row);
    return { items,next_cursor:index+size<rows.length?String(index+size):null,frontier:this.data.manifest.frontier };
  }
  position(args: Parameters<TraceModel['rows']>[0] & { identity: string; pageIndex?: number }) {
    const rows=this.rows(args), index=rows.findIndex(row=>row.id===args.identity), size=args.kind==='node'?50:20;
    if(index<0)throw new Error('Selected interval is not in this time window.');
    const page=args.pageIndex ?? Math.floor(index/size);
    return {cursor:page?String(page*size):null,page_index:page,frontier:this.data.manifest.frontier};
  }
  markers(args: {start:number;end:number;cursor?:string|null;through?:number}) {
    const rows=this.data.event.filter(e=>e.occurred_at_us>=args.start && e.occurred_at_us<args.end && (args.through===undefined || e.sequence<=args.through));
    const index=Number(args.cursor??0);
    return {items:rows.slice(index,index+100),next_cursor:index+100<rows.length?String(index+100):null,frontier:this.data.manifest.frontier};
  }
  groups(args: {start:number;end:number;buckets:number;through:number}) {
    const buckets=new Map<number,TimelineEventGroup>();let total=0;
    for(const e of this.data.event) {
      if(e.sequence>args.through || e.occurred_at_us<args.start || e.occurred_at_us>=args.end)continue;
      total++; const key=Math.floor((e.occurred_at_us-args.start)*args.buckets/(args.end-args.start)), old=buckets.get(key);
      if(!old)buckets.set(key,{count:1,names:{[e.event_name]:1},occurred_at_us:e.occurred_at_us,start_us:e.occurred_at_us,end_us:e.occurred_at_us+1,first_sequence:e.sequence,last_sequence:e.sequence,marker:e});
      else {old.occurred_at_us+=(e.occurred_at_us-old.occurred_at_us)/(old.count+1);old.count++;old.names[e.event_name]=(old.names[e.event_name]??0)+1;old.start_us=Math.min(old.start_us,e.occurred_at_us);old.end_us=Math.max(old.end_us,e.occurred_at_us+1);old.first_sequence=Math.min(old.first_sequence,e.sequence);old.last_sequence=Math.max(old.last_sequence,e.sequence);old.marker=null;}
    }
    return {items:[...buckets.values()].sort((a,b)=>a.occurred_at_us-b.occurred_at_us),total_count:total,frontier:this.data.manifest.frontier};
  }
}

export class LocalReplay {
  private current: ReplayAccumulator;
  private index=-1;
  private checkpoints=new Map<number,ReplayAccumulator>();
  private times: Float64Array;
  private timeIndices: Uint32Array;
  private readonly initial: ReplayAccumulator;
  constructor(initial: ReplayAccumulator, readonly events: ReplayEvent[]) {
    this.initial=initial.compact(events);
    this.current=this.initial.clone();
    // Numeric indexes avoid retaining a million tiny {time,index} objects.
    const order=Uint32Array.from({length:events.length},(_,index)=>index);
    if(!events.every((event,index)=>!index || events[index-1].occurred_at_us<=event.occurred_at_us))order.sort((a,b)=>events[a].occurred_at_us-events[b].occurred_at_us || a-b);
    this.times=new Float64Array(events.length);this.timeIndices=order;
    let maximum=0;for(let index=0;index<order.length;index++){const original=order[index];this.times[index]=events[original].occurred_at_us;maximum=Math.max(maximum,original);order[index]=maximum;}
  }
  seek(position:{sequence?:number;at_us?:number}):LocalReplayResult {
    let lo=0,hi=this.events.length;
    if(position.sequence!==undefined){while(lo<hi){const mid=(lo+hi)>>>1;if(this.events[mid].sequence<=position.sequence)lo=mid+1;else hi=mid;}}
    else {while(lo<hi){const mid=(lo+hi)>>>1;if(this.times[mid]<=(position.at_us??0))lo=mid+1;else hi=mid;}lo=lo?this.timeIndices[lo-1]+1:0;}
    const target=Math.max(0,lo-1);
    if(target<this.index) {this.current=this.initial.clone();this.index=-1;for(const [index,state] of [...this.checkpoints].sort((a,b)=>b[0]-a[0]))if(index<=target){this.current=state.clone();this.index=index;break;}}
    for(let index=this.index+1;index<=target;index++){
      this.current.apply(this.events[index]);this.index=index;
      if((index+1)%Math.max(256,Math.ceil(this.events.length/16))===0 && !this.checkpoints.has(index)){
        this.checkpoints.set(index,this.current.clone());if(this.checkpoints.size>4)this.checkpoints.delete(this.checkpoints.keys().next().value!);
      }
    }
    const marker=this.events[target];
    if(!marker)throw new Error('No saved Events are available for Replay.');
    return {frame:this.current.frame(),marker,previous:this.events[target-1]??null,next:this.events[target+1]??null,cursorTime:position.at_us===undefined?marker.occurred_at_us:Math.max(this.events[0].occurred_at_us,Math.min(this.initial.base.observed_at_us,position.at_us))};
  }
}
