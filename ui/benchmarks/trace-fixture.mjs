export function makeTraceFixture(count) {
  const trace={observed_at_us:count*100,last_event:null,invocation:{invocation_id:'i',workflow_id:'loop',workflow_revision_id:'r',root_invocation_id:'i',status:'running',values:[]},nodes:[],edges:[],active_routes:[],child_count:0,frontier:count,view:'durable'};
  const node=[],call=[],headers=[],events=[];
  for(let i=0;i<count;i++) {
    const unit=Math.floor(i/5),stage=i%5,id=`occ:${unit}`,nodeId=`node:${unit%32}`,time=i*100;
    const status=stage===0?'ready':stage===4?'completed':'running';
    const marker={event_id:`e:${i}`,sequence:i+2,occurred_at_us:time,event_name:['node_occurrence.created','node_occurrence.started','operator_call.started','operator_call.completed','node_occurrence.completed'][stage]};
    headers.push(marker);
    events.push({...marker,changes:stage===0||stage===1||stage===4?[{entity_type:'node',entity_id:id,node_id:nodeId,change:{status}}]:[],call_starts:stage===2?[{node_id:nodeId,count:1}]:[],evaluations:[],routes:[],child_ids:[]});
    if(stage===0)node.push({id,kind:'node',invocation_id:'i',occurrence_id:id,node_id:nodeId,start_at_us:time,end_at_us:time+400,status:'completed',values:[],ordinal:unit+1});
    if(stage===2)call.push({id:`call:${unit}`,kind:'call',invocation_id:'i',occurrence_id:id,node_id:nodeId,start_at_us:time,end_at_us:time+100,status:'completed',operator_id:'simulate',unit_index:0,values:[]});
  }
  const manifest={invocation_id:'i',schema_version:2,source_id:'benchmark',cursor:'initial',base_cursor:null,delta:false,trace,frontier:count,overview:{invocation_id:'i',start_at_us:0,end_at_us:count*100,status:'running',control_state:'active',untimed_count:0,node_start_bins:[],frontier:count,view:'durable',observed_at_us:count*100,counts:{node:node.length,call:call.length,wait:0}}};
  return {data:{manifest,node,call,wait:[],event:headers},events,trace};
}
