import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { computeLayout, fallbackLayout, routeDefinition, NODE_WIDTH, NODE_HEIGHT, type LayoutEngine } from "../src/layout.js";
import { segmentIntersectsRect } from "../src/graphRouting.js";
import type { GraphDefinition } from "../src/types.js";

const ELK = createRequire(import.meta.url)("elkjs/lib/elk.bundled.js") as new () => LayoutEngine;

function graph(): GraphDefinition {
  return {
    nodes: ["start", "branch", "finish"].map(id => ({ id, executable: { kind: "operator", id }, mapped: false })),
    edges: [
      { id: "forward", source: "start", target: "branch", on: "complete", conditional: false },
      { id: "parallel", source: "start", target: "branch", on: "error", conditional: false },
      { id: "finish", source: "branch", target: "finish", on: "complete", conditional: false },
      { id: "back", source: "finish", target: "start", on: "complete", conditional: false },
      { id: "self", source: "branch", target: "branch", on: "complete", conditional: false },
    ],
    entry_node_ids: ["start"], exit_node_ids: ["finish"],
  };
}

test("layout supports parallel edges, graph back edges and self loops with distinct routes", async () => {
  const definition = graph();
  const layout = await computeLayout(definition, new ELK());
  for (const edge of definition.edges) {
    const route = layout.routes[edge.id];
    assert.ok(route.length >= 2);
    assert.equal(route[0].x, layout.positions[edge.source].x + NODE_WIDTH);
    assert.equal(route.at(-1)!.x, layout.positions[edge.target].x);
  }
  assert.notDeepEqual(layout.routes.forward, layout.routes.parallel);
  assert.ok(layout.routes.self.length >= 4);
});

test("fallback keeps all exact node IDs including punctuation and non-ASCII names", () => {
  const definition = graph();
  definition.nodes[0].id = "嵌入.节点/开始";
  assert.deepEqual(Object.keys(fallbackLayout(definition).positions), definition.nodes.map(node => node.id));
});

test("rerouting a dragged graph avoids intervening nodes and retains distinct parallel lanes", () => {
  const definition = graph();
  const positions = { start: { x: 0, y: 0 }, branch: { x: 700, y: 0 }, finish: { x: 340, y: -20 } };
  const routes = routeDefinition(definition, positions);
  const obstacle = { left: 340, top: -20, right: 340 + NODE_WIDTH, bottom: -20 + NODE_HEIGHT };
  for (const id of ["forward", "parallel"]) {
    for (let i = 1; i < routes[id].length; i++) {
      assert.equal(segmentIntersectsRect(routes[id][i - 1], routes[id][i], obstacle), false);
    }
  }
  assert.notDeepEqual(routes.forward, routes.parallel);
});

function branchingGraph():GraphDefinition {
  // Agent-shaped control flow: fan-out, fan-in, optional waits and a return loop.
  const pairs=[['start','prepare_context'],['prepare_context','compact_context'],['prepare_context','build_request'],
    ['compact_context','build_request'],['build_request','llm_call'],['llm_call','tool_branch'],['llm_call','complete_round'],
    ['tool_branch','tool_execute'],['tool_branch','wait_approval'],['tool_branch','complete_round'],['tool_execute','complete_round'],
    ['wait_approval','tool_execute_approved'],['tool_execute_approved','complete_round'],['complete_round','prepare_context'],
    ['complete_round','finish'],['tool_branch','ask_user'],['ask_user','complete_round'],['tool_branch','prepare_wait'],
    ['prepare_wait','wait_tools'],['prepare_wait','complete_round'],['wait_tools','complete_round']];
  return {nodes:[...new Set(pairs.flat())].map(id=>({id,executable:{kind:'operator',id},mapped:false})),
    edges:pairs.map(([source,target],i)=>({id:`edge-${i}`,source,target,on:'complete',conditional:i%2===0})),entry_node_ids:['start'],exit_node_ids:['finish']};
}
function sharedSegments(routes:ReturnType<typeof routeDefinition>) {
  const segments=Object.entries(routes).flatMap(([id,points])=>points.slice(1).map((b,i)=>({id,a:points[i],b})));
  const shared:{a:string;b:string;length:number}[]=[];
  for(let i=0;i<segments.length;i++)for(let j=0;j<i;j++){
    const p=segments[i],q=segments[j];if(p.id===q.id)continue;
    let length=0;
    if(p.a.x===p.b.x && q.a.x===q.b.x && p.a.x===q.a.x)
      length=Math.min(Math.max(p.a.y,p.b.y),Math.max(q.a.y,q.b.y))-Math.max(Math.min(p.a.y,p.b.y),Math.min(q.a.y,q.b.y));
    if(p.a.y===p.b.y && q.a.y===q.b.y && p.a.y===q.a.y)
      length=Math.min(Math.max(p.a.x,p.b.x),Math.max(q.a.x,q.b.x))-Math.max(Math.min(p.a.x,p.b.x),Math.min(q.a.x,q.b.x));
    if(length>24)shared.push({a:p.id,b:q.id,length});
  }
  return shared;
}

test('Agent branches, joins and loops have separate routes and exact visible attachments',async()=>{
  const {edgePortId}=await import('../src/graphPorts.js');
  const definition=branchingGraph(),layout=await computeLayout(definition,new ELK());
  assert.deepEqual(sharedSegments(layout.routes),[]);
  for(const edge of definition.edges){
    const start=layout.routes[edge.id][0],end=layout.routes[edge.id].at(-1)!;
    const from=layout.ports![edge.source].find(p=>p.id===edgePortId(edge.id,'out'))!;
    const to=layout.ports![edge.target].find(p=>p.id===edgePortId(edge.id,'in'))!;
    assert.equal(start.y,layout.positions[edge.source].y+from.y);
    assert.equal(end.y,layout.positions[edge.target].y+to.y);
    assert.equal(start.x,layout.positions[edge.source].x+NODE_WIDTH);
    assert.equal(end.x,layout.positions[edge.target].x);
  }
  const incoming=layout.ports!.complete_round.filter(p=>p.side==='in');
  assert.equal(new Set(incoming.map(p=>p.y)).size,incoming.length);
});

test('dragged Agent routes avoid Nodes and shared corridors while preserving each Edge identity',async()=>{
  const definition=branchingGraph(),layout=await computeLayout(definition,new ELK());
  const positions:typeof layout.positions={...layout.positions,finish:{...layout.positions.finish,y:layout.positions.finish.y+180}};
  const routes=routeDefinition(definition,positions,undefined,layout.ports);
  assert.deepEqual(sharedSegments(routes),[]);
  assert.deepEqual(Object.keys(routes).sort(),definition.edges.map(e=>e.id).sort());
  for(const edge of definition.edges)for(const node of definition.nodes){
    if(node.id===edge.source || node.id===edge.target)continue;
    const at=positions[node.id],rect={left:at.x,top:at.y,right:at.x+NODE_WIDTH,bottom:at.y+NODE_HEIGHT};
    for(let i=1;i<routes[edge.id].length;i++)assert.equal(segmentIntersectsRect(routes[edge.id][i-1],routes[edge.id][i],rect),false,`${edge.id} crosses ${node.id}`);
  }
});

test('parallel complete and fail Edges and multiple self loops remain separately attached',async()=>{
  const definition=graph();
  definition.edges.push({id:'another-loop',source:'branch',target:'branch',on:'error',conditional:true});
  const layout=await computeLayout(definition,new ELK());
  assert.deepEqual(sharedSegments(layout.routes),[]);
  assert.notDeepEqual(layout.routes.self,layout.routes['another-loop']);
  const routes=routeDefinition(definition,layout.positions,undefined,layout.ports);
  assert.deepEqual(sharedSegments(routes),[]);
});

test('fallback ports are deterministic for exact IDs and distinguish both directions of a self loop',async()=>{
  const {definitionPorts,edgePortId}=await import('../src/graphPorts.js');
  const definition=graph();definition.edges[0].id='条件/edge.✓';
  const a=definitionPorts(definition),b=definitionPorts(definition);
  assert.deepEqual(a,b);
  assert.ok(a.start.some(p=>p.id===edgePortId('条件/edge.✓','out')));
  assert.notEqual(edgePortId('self','in'),edgePortId('self','out'));
  for(const ports of Object.values(a))for(const port of ports)assert.ok(port.y>0 && port.y<NODE_HEIGHT);
});

test('nearby parallel Edges have non-overlapping hit areas while isolated Edges stay easy to select',async()=>{
  const {routeInteractionWidths}=await import('../src/graphRouting.js');
  const widths=routeInteractionWidths({first:[{x:0,y:0},{x:200,y:0}],second:[{x:0,y:14},{x:200,y:14}],
    isolated:[{x:0,y:100},{x:200,y:100}]});
  assert.ok(widths.first<14 && widths.second<14);
  assert.equal(widths.isolated,48);
});
