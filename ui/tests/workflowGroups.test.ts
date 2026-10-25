import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {computeLayout,fallbackLayout,type LayoutEngine} from '../src/layout.js';
import {workflowGroups,groupNodeId,constrainGroupMove} from '../src/workflowGroups.js';
import type {GraphDefinition} from '../src/types.js';
const ELK=createRequire(import.meta.url)('elkjs/lib/elk.bundled.js') as new()=>LayoutEngine;
function definition():GraphDefinition {
  const edges=[['standard','review.standard'],['priority','review.priority'],['review.standard','review.checks.assess'],['review.priority','review.checks.assess'],['review.checks.assess','review.checks.approved'],['review.checks.assess','review.checks.rejected'],['review.checks.approved','dispatch'],['review.checks.rejected','manual']];
  const ids=[...new Set(edges.flat())];
  return {nodes:ids.map(id=>({id,executable:{kind:'operator',id:'test'},mapped:false})),edges:edges.map(([source,target],i)=>({id:String(i),source,target,on:'complete',conditional:false})),entry_node_ids:['standard','priority'],exit_node_ids:['dispatch','manual'],
    groups:[{id:'review',parent_group_id:null,label:'review',workflow_id:'review',workflow_version:'1',node_ids:['review.standard','review.priority']},
      {id:'checks',parent_group_id:'review',label:'checks',workflow_id:'checks',workflow_version:'1',node_ids:['review.checks.assess','review.checks.approved','review.checks.rejected']}]};
}
test('folded boundaries retain distinct entry and exit handles without inventing or merging Edges',async()=>{
  const graph=definition(),visible=workflowGroups(graph,new Set(['review']));
  assert.equal(visible.allMembers.get('review')?.size,5);assert.equal(visible.groups.length,1);
  assert.deepEqual(visible.edges.map(e=>e.id),['0','1','6','7']);
  assert.notEqual(visible.edges[0].targetHandle,visible.edges[1].targetHandle);
  assert.notEqual(visible.edges[2].sourceHandle,visible.edges[3].sourceHandle);
  const open=await computeLayout(graph,new ELK()),folded=await computeLayout(graph,new ELK(),new Set(['review']));
  const id=groupNodeId('review');assert.ok(open.sizes![id].width>folded.sizes![id].width*2);
  assert.ok(open.sizes![id].height>folded.sizes![id].height*2);
  assert.equal(folded.sizes![id].width,232);
  for(const edge of visible.edges){const source=folded.positions[edge.source],target=folded.positions[edge.target];
    assert.equal(folded.routes[edge.id][0].x,source.x+folded.sizes![edge.source].width);
    assert.equal(folded.routes[edge.id].at(-1)!.x,target.x);
  }
});
test('nested containers contain their direct members and shrink when only their child is folded',async()=>{
  const graph=definition(),open=await computeLayout(graph,new ELK()),folded=await computeLayout(graph,new ELK(),new Set(['checks']));
  for(const [id,parent] of Object.entries(open.parents!)){
    const p=open.positions[parent],n=open.positions[id],size=open.sizes![id],owner=open.sizes![parent];
    assert.ok(n.x>=p.x+28&&n.y>=p.y+54);
    assert.ok(n.x+size.width<=p.x+owner.width-28&&n.y+size.height<=p.y+owner.height-28);
  }
  assert.ok(folded.sizes![groupNodeId('review')].width<open.sizes![groupNodeId('review')].width);
  assert.ok(folded.positions['review.standard']);assert.equal(folded.positions['review.checks.assess'],undefined);
});
test('punctuation is not ownership and fallback layout still keeps explicit nested boundaries',()=>{
  const graph=definition();graph.nodes.push({id:'review.unowned',executable:{kind:'operator',id:'test'},mapped:false});
  const visible=workflowGroups(graph,new Set(['review']));assert.ok(visible.nodes.some(n=>n.id==='review.unowned'));
  const fallback=fallbackLayout(graph,new Set(['checks']));
  assert.equal(fallback.parents!['review.standard'],groupNodeId('review'));
  assert.equal(fallback.parents!['review.unowned'],undefined);
  assert.equal(fallback.positions['review.checks.assess'],undefined);
  assert.ok(fallback.routes['7'].length>=2);
});
test('Node and group dragging stays inside its direct owner without entering sibling groups',async()=>{
  const layout=await computeLayout(definition(),new ELK()),geometry={positions:layout.positions,sizes:layout.sizes!,parents:layout.parents!};
  const id='review.standard',parent=layout.positions[groupNodeId('review')];
  const clamped=constrainGroupMove(id,{x:-1000,y:-1000},geometry);
  assert.equal(clamped.x,parent.x+28);assert.equal(clamped.y,parent.y+54);
  const sibling=layout.positions[groupNodeId('checks')];
  assert.deepEqual(constrainGroupMove(id,sibling,geometry),layout.positions[id]);
  const moved=constrainGroupMove(groupNodeId('checks'),{x:-1000,y:-1000},geometry);
  assert.ok(moved.x>=parent.x+28&&moved.y>=parent.y+54);
});
test('drag rerouting keeps internal connections within their owning Workflow boundary',async()=>{
  const {rerouteGroups}=await import('../src/groupGeometry.js');
  const graph=definition(),layout=await computeLayout(graph,new ELK());
  const routes=rerouteGroups(graph,new Set(),layout),at=layout.positions[groupNodeId('review')],size=layout.sizes![groupNodeId('review')];
  for(const id of ['2','3','4','5'])for(const point of routes[id]){
    assert.ok(point.x>=at.x&&point.x<=at.x+size.width);
    assert.ok(point.y>=at.y+44&&point.y<=at.y+size.height);
  }
  const point=layout.positions['standard'];
  assert.deepEqual(constrainGroupMove('standard',at,{positions:layout.positions,sizes:layout.sizes!,parents:layout.parents!}),point);
});
test('nested Loop routes remain inside the child container and disappear only when that scope folds',async()=>{
  const graph=definition();
  graph.edges.push({id:'loop-back',source:'review.checks.approved',target:'review.checks.assess',on:'complete',conditional:true});
  const open=await computeLayout(graph,new ELK()),id=groupNodeId('checks'),at=open.positions[id],size=open.sizes![id];
  assert.ok(open.routes['loop-back'].length>=2);
  assert.ok(open.routes['loop-back'].length>=4||open.routes['4'].length>=4);
  for(const point of open.routes['loop-back'])assert.ok(point.x>=at.x&&point.x<=at.x+size.width&&point.y>=at.y+44&&point.y<=at.y+size.height);
  const folded=await computeLayout(graph,new ELK(),new Set(['checks']));
  assert.equal(folded.routes['loop-back'],undefined);assert.ok(folded.routes['6']);
});
