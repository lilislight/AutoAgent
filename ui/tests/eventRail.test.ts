import { test } from 'node:test';
import assert from 'node:assert/strict';
import { clusterEventGroups, eventGroupCategory, EVENT_MIN_GAP } from '../src/eventRail.js';
import type { TimelineEventGroup } from '../src/types.js';
const origin=1_780_000_000_000_000;
const item=(time:number,count=1,name='node_occurrence.started'):TimelineEventGroup => ({ count,names:{[name]:count},occurred_at_us:origin+time,start_us:origin+time,end_us:origin+time+1,first_sequence:time,last_sequence:time+count-1,marker:count===1?{event_id:String(time),event_name:name,sequence:time,occurred_at_us:origin+time}:null });
test('merging re-evaluates distance from the new weighted centre and keeps exact counts',()=>{
  const groups=clusterEventGroups([item(10),item(27),item(28),item(90,20)],{start:origin,end:origin+100},100);
  assert.equal(groups.length,2);
  assert.equal(groups[0].count,3);
  assert.ok(Math.abs(groups[0].left-21.6667)<.3);
  assert.equal(groups.reduce((n,g)=>n+g.count,0),23);
  assert.equal(groups[0].start_us,origin+10);assert.equal(groups[0].end_us,origin+29);
  assert.equal(groups[0].marker,null);
});
test('dense rails remain bounded and adjacent circles keep a small gap, including endpoints',()=>{
  const items=Array.from({length:610},(_,i)=>item(i*10,i%7+1));
  const groups=clusterEventGroups(items,{start:origin,end:origin+6100},403);
  assert.equal(groups.reduce((n,g)=>n+g.count,0),items.reduce((n,g)=>n+g.count,0));
  assert.ok(groups.length<=Math.floor(403/EVENT_MIN_GAP)+1);
  assert.ok(groups.slice(1).every((g,i)=>(g.left-groups[i].left)*403/100>=EVENT_MIN_GAP-1e-7));
  assert.equal(clusterEventGroups([item(0)],{start:origin,end:origin+100},400)[0].left,0);
  assert.equal(clusterEventGroups([item(100)],{start:origin,end:origin+100},400)[0].left,100);
});
test('single Events and count badges use the same 20px minimum centre distance',()=>{
  const full={start:origin,end:origin+100};
  for (const left of [1,2,100]) for (const right of [1,2,100]) {
    assert.equal(clusterEventGroups([item(0,left),item(19,right)],full,100).length,1);
    assert.equal(clusterEventGroups([item(0,left),item(20,right)],full,100).length,2);
  }
});
test('singletons retain exact replay identity and faults are visible inside mixed groups',()=>{
  const single=clusterEventGroups([item(30)],{start:origin,end:origin+100},400)[0];
  assert.equal(single.marker?.sequence,30);
  assert.equal(eventGroupCategory(single),'node');
  const merged=clusterEventGroups([item(30),item(31,2,'operator_call.failed')],{start:origin,end:origin+100},400)[0];
  assert.equal(eventGroupCategory(merged),'error');
  assert.deepEqual(merged.names,{'node_occurrence.started':1,'operator_call.failed':2});
});
test('range previews reproject cached headers without piling off-screen Events at endpoints',()=>{
  const cached=[item(0),item(20),item(50),item(80),item(100)];
  const selected={start:origin+20,end:origin+80};
  const groups=clusterEventGroups(cached,selected,400);
  assert.deepEqual(groups.map(group=>group.left),[0,50,100]);
  assert.equal(groups.reduce((sum,group)=>sum+group.count,0),3);
  assert.deepEqual(groups.map(group=>group.marker?.sequence),[20,50,80]);
  assert.equal(clusterEventGroups(cached,{start:origin+51,end:origin+79},400).length,0);
});
