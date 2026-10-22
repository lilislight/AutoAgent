import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TraceModel, LocalReplay } from '../src/traceModel.js';
import { ReplayAccumulator, replayFrame } from '../src/replay.js';
import type { ExecutionTrace, ReplayEvent, ReplayWindow, TraceDataset, TimelineRow } from '../src/types.js';
const trace:ExecutionTrace={observed_at_us:10000,last_event:null,invocation:{invocation_id:'i',workflow_id:'wf',workflow_revision_id:'r',root_invocation_id:'i',status:'completed',values:[]},nodes:[],edges:[],active_routes:[],child_count:0,frontier:99,view:'durable'};
const row=(id:string,start:number,end:number,kind:'node'|'call'|'wait'='node'):TimelineRow=>({id,kind,invocation_id:'i',occurrence_id:kind==='node'?id:'n0',node_id:'n',start_at_us:start,end_at_us:end,status:'completed',values:[]});
function dataset():TraceDataset {
  const nodes=Array.from({length:122},(_,i)=>row('n'+i,i*10,i*10+8));
  return {manifest:{cursor:'c0',base_cursor:null,delta:false,schema_version:1,source_id:'s',snapshot_id:'s',version:'v',invocation_id:'i',frontier:99,through_sequence:2000,trace,overview:{invocation_id:'i',start_at_us:0,end_at_us:2000,observed_at_us:2000,status:'completed',control_state:'active',counts:{node:122,call:1,wait:1},untimed_count:0,node_start_bins:[],frontier:99,view:'durable'},initial_trace:null,history_complete:null,replay:false,base_version:null,sections:{}},node:nodes,call:[{...row('call',1,7,'call'),operator_id:'system_command:wait'}],wait:[row('call',2,6,'wait')],event:Array.from({length:610},(_,i)=>({event_id:String(i),event_name:'node_occurrence.started',sequence:i*3+2,occurred_at_us:i*2}))};
}
test('Fork Timeline omits inherited zero spans while retaining Graph facts and genuine instantaneous work',()=>{
  const data=dataset();
  data.node=[{...row('z_before',100,100),node_id:'before',timeline_visible:false},
    {...row('a_wait',100,130),node_id:'wait',timeline_visible:true},
    {...row('m_after',130,130),node_id:'after',timeline_visible:true}];
  data.call=[{...row('old',100,100,'call'),occurrence_id:'a_wait',timeline_visible:false},
    {...row('current',100,130,'call'),occurrence_id:'a_wait',operator_id:'system_command:wait',timeline_visible:true}];
  data.wait=[{...row('current',100,130,'wait'),occurrence_id:'a_wait',timeline_visible:true}];
  data.manifest.overview={...data.manifest.overview,start_at_us:100,end_at_us:131};
  const model=new TraceModel(data),args={kind:'node',start:100,end:131};
  assert.deepEqual(model.page(args).items.map(row=>row.id),['a_wait','m_after']);
  assert.equal(model.page(args).items[0].call_count,1);
  assert.deepEqual(model.page(args).items[0].preview_calls?.map(row=>row.id),['current']);
  assert.deepEqual(model.page({...args,kind:'call',occurrence:'a_wait'}).items.map(row=>row.id),['current']);
  assert.deepEqual(model.overview().counts,{node:2,call:1,wait:1});
  assert.equal(model.overview().node_start_bins.reduce((sum,count)=>sum+count,0),2);
  assert.equal(model.nodes.get('z_before')?.status,'completed');
  assert.throws(()=>model.position({...args,identity:'z_before'}));
});
test('local pages and exact seek keep the existing Node and Call pagination without backend queries',()=>{
  const model=new TraceModel(dataset()),args={kind:'node',start:0,end:2000};
  const page=model.page(args);assert.equal(page.items.length,50);assert.equal(page.next_cursor,'50');
  assert.equal(model.page({...args,cursor:'50'}).items[0].id,'n50');
  assert.deepEqual(model.position({...args,identity:'n90'}),{cursor:'50',page_index:1,frontier:99});
  assert.equal(model.page({...args,start:601,end:609}).items[0].id,'n60');
  assert.throws(()=>model.position({...args,start:0,end:10,identity:'n90'}));
  assert.equal(page.items[0].preview_calls?.[0].wait?.id,'call');
  const calls=model.page({kind:'call',start:0,end:2000,occurrence:'n0'});
  assert.equal(calls.items[0].node_id,'n');assert.equal(calls.items[0].wait?.start_at_us,2);
});
test('local Event aggregation and detail paging cover the entire frozen history exactly',()=>{
  const model=new TraceModel(dataset());
  for(const buckets of [1,7,512]){const groups=model.groups({start:0,end:2000,buckets,through:2000});assert.equal(groups.total_count,610);assert.equal(groups.items.reduce((n,g)=>n+g.count,0),610);assert.ok(groups.items.length<=buckets);}
  const items=[];let cursor:string|null=null;
  do{const page=model.markers({start:0,end:2000,cursor});items.push(...page.items);cursor=page.next_cursor;}while(cursor);
  assert.equal(items.length,610);assert.equal(new Set(items.map(e=>e.event_id)).size,610);
  assert.equal(model.groups({start:100,end:200,buckets:10,through:2000}).total_count,50);
});
test('overview activity is re-binned at the local domain and omits skipped or inherited work',()=>{
  const data=dataset();
  data.node=[row('early',10,20),row('late-a',1_000_010,1_000_020),row('late-b',1_000_090,1_000_100),
    {...row('inherited',1_000_050,1_000_050),timeline_visible:false},{...row('skipped',1_000_050,1_000_050),status:'skipped'}];
  const model=new TraceModel(data);
  assert.deepEqual(model.activityBins({start:0,end:2_000_000,buckets:4}),[1,0,2,0]);
  assert.deepEqual(model.activityBins({start:1_000_000,end:1_000_100,buckets:4}),[1,0,0,1]);
  assert.deepEqual(model.activityBins({start:1_000_100,end:1_000_200,buckets:4}),[0,0,0,0]);
});
test('time indexes resolve unsorted timestamps, exact frontiers and same-time groups across live append',()=>{
  const data=dataset();
  data.event=[{event_id:'a',sequence:1,occurred_at_us:1000,event_name:'node_occurrence.started'},
    {event_id:'b',sequence:2,occurred_at_us:10,event_name:'operator_call.started'},
    {event_id:'c',sequence:3,occurred_at_us:10,event_name:'operator_call.completed'}];
  const model=new TraceModel(data);
  assert.deepEqual(model.markers({start:0,end:2000}).items.map(e=>e.sequence),[1,2,3]);
  assert.equal(model.groups({start:10,end:11,buckets:512,through:2}).total_count,1);
  assert.equal(model.groups({start:10,end:11,buckets:512,through:3}).items[0].count,2);
  assert.equal(model.groups({start:1000,end:1001,buckets:512,through:3}).items[0].marker?.sequence,1);
  const delta:TraceDataset={...dataset(),node:[],call:[],wait:[],event:[{event_id:'d',sequence:4,occurred_at_us:5,event_name:'routing.resolved'},
    {event_id:'e',sequence:5,occurred_at_us:1001,event_name:'node_occurrence.completed'}],
    manifest:{...data.manifest,cursor:'c1',base_cursor:'c0',delta:true,frontier:100}};
  model.apply(delta);
  assert.deepEqual(model.markers({start:0,end:1001,through:4}).items.map(e=>e.sequence),[1,2,3,4]);
  assert.equal(model.groups({start:5,end:6,buckets:512,through:5}).items[0].marker?.sequence,4);
  assert.equal(model.groups({start:1001,end:1002,buckets:1,through:5}).total_count,1);
});
test('local Replay checkpoints match full reduction across rewinds, atomic changes and sequence gaps',()=>{
  const events:ReplayEvent[]=Array.from({length:700},(_,i)=>({event_id:String(i),sequence:i*3+2,occurred_at_us:i*10,event_name:'node_occurrence.started',changes:[{entity_type:'node',entity_id:'occ'+Math.floor(i/2),node_id:'n',change:{node_id:'n',status:i%2?'completed':'running'}}],evaluations:[],routes:[],child_ids:i===500?['child']:[],call_starts:[{node_id:'n',count:1}]}));
  const baseline={trace:{...trace,invocation:{...trace.invocation,status:'planned'}},occurrences:[],child_ids:[]};
  const window={history_complete:true,baseline,events,boundary:null,previous:null,next:null,first:events[0],last:events.at(-1)!,frontier:99,time_from_us:0,time_until_us:null} as ReplayWindow;
  const local=new LocalReplay(new ReplayAccumulator(window),events);
  for(const index of [0,600,257,699,3,510,120]){const result=local.seek({sequence:events[index].sequence});assert.deepEqual(result.frame,replayFrame(window,events[index].sequence));assert.equal(result.marker.sequence,events[index].sequence);assert.equal(result.previous?.sequence,events[index-1]?.sequence);}
  assert.equal(local.seek({at_us:5005}).marker.sequence,events[500].sequence);
});
test('Replay time lookup preserves Event order even when wall-clock timestamps go backwards',()=>{
  const events:ReplayEvent[]=[{event_id:'a',sequence:2,occurred_at_us:100,event_name:'a',changes:[],evaluations:[],routes:[],child_ids:[],call_starts:[]},{event_id:'b',sequence:9,occurred_at_us:50,event_name:'b',changes:[],evaluations:[],routes:[],child_ids:[],call_starts:[]}];
  const initial={history_complete:true,baseline:{trace,occurrences:[],child_ids:[]}} as unknown as ReplayWindow;
  const local=new LocalReplay(new ReplayAccumulator(initial),events);
  assert.equal(local.seek({at_us:110}).marker.sequence,9);
  assert.equal(local.seek({at_us:60}).marker.sequence,9);
});
test('compact checkpoints preserve concurrent priority, routing, atomic changes and paused control across rewinds',()=>{
  const events:ReplayEvent[]=[];
  const delta=(id:string,node:string,status:string):ReplayEvent['changes'][number]=>({entity_type:'node',entity_id:id,node_id:node,change:{status,node_id:node}});
  for(let i=0;i<1200;i++) {
    const id='parallel'+i,sequence=i*5+2;
    events.push({event_id:String(i),sequence,occurred_at_us:i*10,event_name:'node_occurrence.changed',
      changes:[delta(id,'loop','ready'),delta(id,'loop',i%3?'completed':'running'),...(i>2?[delta('parallel'+(i-3),'loop','completed')]:[]),{entity_type:'invocation',entity_id:'i',node_id:null,change:{control_state:i%7?'active':'paused'}}],
      evaluations:[{edge_id:'next',condition_result:i%2===0}],routes:[{edge_id:'next',source_occurrence_id:'source',target_node_id:'loop',target_occurrence_id:id}],
      child_ids:i%20===0?['child'+i]:[],call_starts:[{node_id:'loop',count:2}]});
  }
  const window={history_complete:true,baseline:{trace,occurrences:[],child_ids:[]},events} as unknown as ReplayWindow;
  const replay=new LocalReplay(new ReplayAccumulator(window),events);
  for(const index of [1199,300,900,5,1180,600,0])assert.deepEqual(replay.seek({sequence:events[index].sequence}).frame,replayFrame(window,events[index].sequence));
});
test('compact Replay restores incoming routes when an earlier occurrence resumes and does not count repeated terminal deltas twice',()=>{
  const change=(id:string,status:string):ReplayEvent['changes'][number]=>({entity_type:'node',entity_id:id,node_id:'n',change:{status,node_id:'n'}});
  const event=(sequence:number,changes:ReplayEvent['changes'],routes:ReplayEvent['routes']=[]):ReplayEvent=>({event_id:String(sequence),sequence,occurred_at_us:sequence,event_name:'node_occurrence.changed',changes,routes,evaluations:[],call_starts:[],child_ids:[]});
  const events=[event(2,[change('a','ready'),change('b','ready')],[{edge_id:'b',source_occurrence_id:'s',target_occurrence_id:'b',target_node_id:'n'},{edge_id:'a',source_occurrence_id:'s',target_occurrence_id:'a',target_node_id:'n'}]),event(3,[change('a','completed'),change('b','completed'),change('a','completed')]),event(4,[change('a','running'),change('b','running')])];
  const window={history_complete:true,baseline:{trace,occurrences:[],child_ids:[]},events} as unknown as ReplayWindow;
  const replay=new LocalReplay(new ReplayAccumulator(window),events);
  for(const sequence of [4,2,3,4])assert.deepEqual(replay.seek({sequence}).frame,replayFrame(window,sequence));
});
test('compact Replay preserves logical Node order when concurrent Nodes complete out of order and terminal status changes',()=>{
  const changes=(id:string,status:string):ReplayEvent['changes'][number]=>({entity_type:'node',entity_id:id,node_id:id,change:{status}});
  const events:ReplayEvent[]=[['a','ready'],['b','ready'],['b','completed'],['a','completed'],['b','failed']].map(([id,status],index)=>({event_id:String(index),sequence:index+2,occurred_at_us:index,event_name:'node_occurrence.changed',changes:[changes(id,status)],evaluations:[],routes:[],child_ids:[],call_starts:[]}));
  const window={history_complete:true,baseline:{trace,occurrences:[],child_ids:[]},events} as unknown as ReplayWindow;
  const replay=new LocalReplay(new ReplayAccumulator(window),events);
  for(const sequence of [6,4,5,2,6])assert.deepEqual(replay.seek({sequence}).frame,replayFrame(window,sequence));
});
