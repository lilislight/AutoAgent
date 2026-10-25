import { test } from 'node:test';
import assert from 'node:assert/strict';
import { FamilyLinkIndex, familyTargetVisible } from '../src/familyHandoff.js';
import type { FamilyLink } from '../src/types.js';

function link(sequence:number,child:string,occurrence='spawn@1',kind:'spawn'|'await'='spawn'):FamilyLink {
  return {id:`${sequence}:${child}`,kind,event_id:`e${sequence}`,sequence,occurred_at_us:sequence*100,
    occurrence_id:occurrence,node_id:'spawn',wait_id:kind==='await'?'wait':null,
    target:{invocation_id:child,created_sequence:sequence,values:[]}};
}
test('each Child in a Map batch retains its own curve and destination',()=>{
  const index=new FamilyLinkIndex(),rows=Array.from({length:20},(_,i)=>link(10,`c${i}`));
  index.add(rows);index.add(rows);
  const groups=index.handoffs({after:9,through:10});
  assert.equal(groups.length,20);assert.equal(new Set(groups.map(item=>item.key)).size,20);
  assert.deepEqual(new Set(groups.map(item=>item.target.invocation_id)),new Set(rows.map(row=>row.target.invocation_id)));
  assert.deepEqual(index.handoffs({after:10,through:10}),[]);
});
test('Replay exact boundaries work in either direction and exclude future child identities',()=>{
  const index=new FamilyLinkIndex();index.add([link(10,'first'),link(20,'second','spawn@2'),{...link(30,'first','await@1','await'),target:{invocation_id:'first',created_sequence:10,values:[]}}]);
  assert.equal(index.handoffs({after:0,through:30,exact:true})[0].kind,'await');
  assert.deepEqual(index.handoffs({after:0,through:29,exact:true}),[]);
  assert.equal(index.handoffs({after:30,through:10,exact:true})[0].target.invocation_id,'first');
  assert.deepEqual(index.handoffs({after:0,through:9,exact:true}),[]);
  assert.equal(index.handoffs({after:10,through:20})[0].occurrenceId,'spawn@2');
});
test('inline Await updates only the same Child destination',()=>{
  const index=new FamilyLinkIndex();index.add([link(10,'child'),{...link(12,'child','spawn@1','await'),target:{invocation_id:'child',created_sequence:10,values:[]}}]);
  const [group]=index.handoffs({after:0,through:12});
  assert.equal(index.handoffs({after:0,through:12}).length,1);assert.equal(group.kind,'await');assert.equal(group.waitId,'wait');
});
test('Live batches do not drop independent Child destinations',()=>{
  const index=new FamilyLinkIndex();index.add(Array.from({length:100},(_,i)=>link(i+1,`c${i}`,`spawn@${i}`)));
  assert.equal(index.handoffs({after:0,through:100}).length,100);
  assert.deepEqual(index.handoffs({after:97,through:100}).map(g=>g.sequence),[100,99,98]);
});
test('Map handoff targets follow Unit order rather than random Child UUID order',()=>{
  const index=new FamilyLinkIndex();
  const first=link(10,'z-child'),second=link(10,'a-child');first.target.unit_index=0;second.target.unit_index=1;
  index.add([second,first]);
  assert.equal(index.handoffs({after:9,through:10})[0].target.invocation_id,'z-child');
});
test('Replay family pages stay local, bounded and exclude children created after the selected Event',()=>{
  const index=new FamilyLinkIndex();index.add(Array.from({length:45},(_,i)=>({...link(i<25?10:20,`c${i}`),target:{invocation_id:`c${i}`,created_sequence:i<25?10:20,unit_index:i,values:[]}})));
  assert.deepEqual(index.children(9).items,[]);
  const first=index.children(10);assert.equal(first.items.length,20);assert.equal(first.next_cursor,'20');
  const second=index.children(10,first.next_cursor);assert.equal(second.items.length,5);assert.equal(second.next_cursor,null);
  assert.equal(index.children(20,'40').items.length,5);
  assert.throws(()=>index.children(20,'invalid'));
});
test('inherited child identities remain navigable without pretending Fork imports are new Spawns',()=>{
  const index=new FamilyLinkIndex();index.add([{...link(1,'inherited'),animate:false}]);
  assert.equal(index.children(1).items[0].invocation_id,'inherited');
  assert.deepEqual(index.handoffs({after:0,through:1,exact:true}),[]);
});

test('hidden and clipped breadcrumbs are omitted, including cards outside the workspace',()=>{
  const strip={left:20,right:420,top:100,bottom:200},workspace={left:0,right:500,top:0,bottom:300};
  assert.equal(familyTargetVisible({left:30,right:150,top:110,bottom:190},strip,workspace),true);
  assert.equal(familyTargetVisible({left:410,right:530,top:110,bottom:190},strip,workspace),false);
  assert.equal(familyTargetVisible({left:-100,right:20,top:110,bottom:190},strip,workspace),false);
  assert.equal(familyTargetVisible({left:30,right:150,top:110,bottom:190},strip,{...workspace,bottom:150}),false);
  assert.equal(familyTargetVisible({left:30,right:30,top:110,bottom:190},strip,workspace),false);
});
test('Awaiting one Child never changes its siblings Spawn destinations',()=>{
  const index=new FamilyLinkIndex();index.add([link(10,'first'),link(10,'second'),
    {...link(12,'second','spawn@1','await'),target:{invocation_id:'second',created_sequence:10,values:[]}}]);
  const groups=index.handoffs({after:0,through:12});
  assert.equal(groups.length,2);
  assert.equal(groups.find(item=>item.target.invocation_id==='first')?.kind,'spawn');
  assert.equal(groups.find(item=>item.target.invocation_id==='second')?.kind,'await');
});
