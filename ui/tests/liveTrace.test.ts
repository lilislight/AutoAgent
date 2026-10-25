import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TraceModel } from '../src/traceModel.js';
import type { ExecutionTrace, TraceDataset, TimelineRow } from '../src/types.js';
const trace:ExecutionTrace={source_id:'store',observed_at_us:1000,last_event:{event_id:'first',sequence:2,event_name:'invocation.started',occurred_at_us:0},invocation:{invocation_id:'i',workflow_id:'wf',workflow_revision_id:'r',root_invocation_id:'i',status:'running',control_state:'active',started_at_us:0,completed_at_us:null,values:[]},nodes:[],edges:[],active_routes:[],child_count:0,frontier:2,view:'durable'};
const row=(id:string,node:string,status:string,start:number,end:number|null,ordinal=1):TimelineRow=>({id,kind:'node',invocation_id:'i',occurrence_id:id,node_id:node,status,start_at_us:start,ready_at_us:start,started_at_us:start,end_at_us:end,ordinal,values:[]});
function data(cursor='c0'):TraceDataset {
  return {manifest:{cursor,base_cursor:null,delta:false,schema_version:2,source_id:'store',snapshot_id:'s',version:cursor,invocation_id:'i',frontier:2,through_sequence:2,trace,overview:{invocation_id:'i',start_at_us:0,end_at_us:1000,observed_at_us:1000,status:'running',control_state:'active',counts:{node:0,call:0,wait:0},untimed_count:0,node_start_bins:[],frontier:2,view:'durable'},initial_trace:null,history_complete:null,replay:false,base_version:null,sections:{}},node:[],call:[],wait:[],event:[]};
}
function delta(base:TraceDataset,cursor:string,frontier:number,rows:Partial<TraceDataset>={}):TraceDataset {
  return {...data(cursor),...rows,manifest:{...base.manifest,cursor,base_cursor:base.manifest.cursor,delta:true,frontier,trace:{...trace,frontier},overview:{...base.manifest.overview,frontier}}};
}
test('live batches patch one canonical metadata set and reject duplicate/out-of-order boundaries without double counting',()=>{
  const initial=data(),model=new TraceModel(initial);
  const first=delta(initial,'c1',3,{node:[row('n1','n','running',10,null)],call:[{...row('call','n','running',11,null),kind:'call',occurrence_id:'n1',operator_id:'tool',unit_index:0}]});
  const graph=model.apply(first);
  assert.equal(graph.nodes[0].occurrences,1);assert.equal(graph.nodes[0].call_count,1);
  const canonical=model.nodes.get('n1');
  const second=delta(model.data,'c2',4,{node:[row('n1','n','completed',10,30)],call:[{...first.call[0],status:'completed',end_at_us:29}]});
  model.apply(second);
  assert.equal(model.nodes.get('n1'),canonical);
  assert.equal(model.data.node.length,1);assert.equal(model.data.call.length,1);
  assert.equal(model.apply(second).nodes[0].call_count,1);
  assert.equal(model.data.node[0].status,'completed');
  assert.throws(()=>model.apply(first),/does not match/);
  assert.equal(model.data.manifest.cursor,'c2');
});
test('live Graph retains concurrent active representatives, exact Edge counts and incoming routes through wait/resume',()=>{
  const model=new TraceModel(data());
  const a=row('a','loop','waiting',10,null),b=row('b','loop','completed',12,20,2);
  a.activations=[{edge_id:'incoming',source_occurrence_id:'source'}];
  const first=delta(model.data,'c1',5,{node:[a,b],edge:[{edge_id:'incoming',condition_result:true,evaluations_delta:3}]});
  let graph=model.apply(first);
  assert.equal(graph.nodes[0].occurrence_id,'a');assert.equal(graph.nodes[0].occurrences,2);
  assert.equal(graph.active_routes[0].target_occurrence_id,'a');assert.equal(graph.edges[0].evaluations,3);
  const second=delta(model.data,'c2',9,{node:[{...a,status:'completed',end_at_us:50}],edge:[{edge_id:'incoming',condition_result:false,evaluations_delta:2}]});
  graph=model.apply(second);
  assert.equal(graph.nodes[0].occurrence_id,'b');assert.equal(graph.active_routes.length,0);assert.equal(graph.edges[0].evaluations,5);
  const third=delta(model.data,'c3',10,{node:[{...a,status:'running'}]});graph=model.apply(third);
  assert.equal(graph.nodes[0].occurrence_id,'a');assert.equal(graph.active_routes[0].status,'running');assert.equal(graph.nodes[0].occurrences,2);
});
test('local live overview advances only the estimated open end and preserves exact completed timing and all histogram counts',()=>{
  const model=new TraceModel(data());
  model.apply(delta(model.data,'c1',3,{node:[row('open','n','running',10,null),row('skip','skip','skipped',10,10)]}));
  let overview=model.overview(2000);
  assert.equal(overview.end_at_us,2000);assert.equal(overview.counts.node,1);assert.equal(overview.node_start_bins.reduce((n,c)=>n+c,0),1);
  const ended=delta(model.data,'c2',4,{node:[row('open','n','completed',10,1800)]});
  ended.manifest.trace.invocation={...trace.invocation,status:'completed',completed_at_us:1800};
  ended.manifest.overview.status='completed';
  model.apply(ended);overview=model.overview(10000);
  assert.equal(overview.end_at_us,1800);assert.equal(model.data.node[0].end_at_us,1800);
});
test('metadata arriving after a snapshot appends all Event headers in Session sequence order, including gaps',()=>{
  const model=new TraceModel(data());
  const events=[{event_id:'e1',sequence:5,event_name:'operator_call.started',occurred_at_us:2},{event_id:'e2',sequence:12,event_name:'operator_call.completed',occurred_at_us:1}];
  model.apply(delta(model.data,'c1',4,{event:events}));
  assert.equal(model.groups({start:0,end:5,buckets:5,through:99}).total_count,2);
  const bad=delta(model.data,'c2',6,{node:[{...row('wrong','n','running',0,null),invocation_id:'other'}]});
  assert.throws(()=>model.apply(bad),/another execution/);assert.equal(model.data.manifest.cursor,'c1');assert.equal(model.data.node.length,0);
  const duplicate=delta(model.data,'c2',6,{node:[row('new','n','running',3,null)],event:[{...events[0],event_id:'new'},{...events[1],event_id:'new'}]});
  assert.throws(()=>model.apply(duplicate),/repeats/);assert.equal(model.data.manifest.cursor,'c1');assert.equal(model.data.node.length,0);
});
test('pause intervals survive metadata-only incremental updates and completion freezes the Timeline boundary',()=>{
  const initial=data(),model=new TraceModel(initial);
  const paused=delta(model.data,'c1',3,{event:[{event_id:'pause',sequence:3,event_name:'graph.paused',occurred_at_us:100,control_state:'paused'}]});
  paused.manifest.trace.invocation={...trace.invocation,status:'waiting',control_state:'paused'};
  model.apply(paused);
  assert.deepEqual(model.overview(150).pause_intervals,[{start_at_us:100,end_at_us:null}]);
  const resumed=delta(model.data,'c2',4,{event:[{event_id:'resume',sequence:4,event_name:'graph.resumed',occurred_at_us:200,control_state:'active'}]});
  model.apply(resumed);
  assert.deepEqual(model.overview(300).pause_intervals,[{start_at_us:100,end_at_us:200}]);
  const ended=delta(model.data,'c3',6,{event:[{event_id:'pause2',sequence:5,event_name:'graph.paused',occurred_at_us:350,control_state:'paused'},{event_id:'end',sequence:6,event_name:'invocation.cancelled',occurred_at_us:400,invocation_status:'cancelled'}]});
  ended.manifest.trace.invocation={...trace.invocation,status:'cancelled',completed_at_us:400};
  model.apply(ended);
  const overview=model.overview(100000);
  assert.equal(overview.end_at_us,400);
  assert.deepEqual(overview.pause_intervals,[{start_at_us:100,end_at_us:200},{start_at_us:350,end_at_us:400}]);
});
test('long-lived Wait and Pause snapshots keep all Timeline geometry fixed until a committed transition',()=>{
  const initial=data();
  initial.manifest.trace={...trace,invocation:{...trace.invocation,status:'waiting'},last_event:{event_id:'waiting',sequence:3,event_name:'node_occurrence.waiting',occurred_at_us:100}};
  initial.node=[row('a','first','waiting',10,null),row('b','second','waiting',20,null)];
  initial.event=[{...initial.manifest.trace.last_event!,invocation_status:'waiting'}];
  const model=new TraceModel(initial);
  const before=model.overview(1000);
  assert.equal(before.end_at_us,100);assert.equal(before.observed_at_us,100);
  assert.deepEqual(model.overview(1_000_000_000),before);
  assert.deepEqual(before.idle,{state:'waiting',since_at_us:100,at_us:100});
  // One of several approvals completes while the Invocation remains Waiting.
  const partial=delta(model.data,'partial',4,{node:[row('a','first','completed',10,200)],event:[{event_id:'partial',sequence:4,event_name:'node_occurrence.completed',occurred_at_us:200}]});
  partial.manifest.trace.invocation={...trace.invocation,status:'waiting'};
  partial.manifest.trace.last_event=partial.event[0];
  model.apply(partial);
  assert.equal(model.overview(1_000_000).end_at_us,200);
  assert.deepEqual(model.overview(2000),model.overview(1_000_000));
  const paused=delta(model.data,'paused',5,{event:[{event_id:'paused',sequence:5,event_name:'graph.paused',occurred_at_us:300,control_state:'paused'}]});
  paused.manifest.trace.invocation={...trace.invocation,status:'waiting',control_state:'paused'};
  paused.manifest.trace.last_event=paused.event[0];
  model.apply(paused);
  const frozen=model.overview(10_000);
  assert.equal(frozen.end_at_us,300);assert.equal(frozen.idle?.state,'paused');
  assert.deepEqual(model.overview(1_000_000),frozen);
  const resumed=delta(model.data,'resumed',6,{node:[row('b','second','running',20,null)],event:[{event_id:'resumed',sequence:6,event_name:'graph.resumed',occurred_at_us:400,control_state:'active',invocation_status:'running'}]});
  resumed.manifest.trace.last_event=resumed.event[0];
  model.apply(resumed);
  assert.equal(model.overview(500).idle,null);assert.equal(model.overview(500).end_at_us,500);
});
test('Map counts follow the displayed occurrence independently through live updates and terminal fallback',()=>{
  const model=new TraceModel(data());
  const a=row('a','mapped','running',10,null),b=row('b','mapped','completed',20,30,2);
  const call=(id:string,occurrence:string)=>({...row(id,'mapped','completed',21,25),kind:'call' as const,occurrence_id:occurrence,operator_id:'tool',unit_index:0});
  let graph=model.apply(delta(model.data,'c1',5,{node:[a,b],call:[call('a1','a'),call('a2','a'),call('b1','b')]}));
  assert.equal(graph.nodes[0].occurrence_id,'a');assert.equal(graph.nodes[0].call_count,2);
  graph=model.apply(delta(model.data,'c2',6,{node:[{...a,status:'completed',end_at_us:40}]}));
  assert.equal(graph.nodes[0].occurrence_id,'b');assert.equal(graph.nodes[0].call_count,1);
  const repeated=delta(model.data,'c3',7,{call:[{...call('b1','b'),status:'completed'}]});
  assert.equal(model.apply(repeated).nodes[0].call_count,1);
});
