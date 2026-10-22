import test from 'node:test';
import assert from 'node:assert/strict';
import { comparisonGraph, definitionDifferences, splitComparisonGraphs } from '../src/revisionComparison.js';
import { readLocation, writeLocation } from '../src/navigation.js';
import type { WorkflowGraphData } from '../src/types.js';

function graph(hash:string,nodes:string[],target:string):WorkflowGraphData {
  return {workflow_id:'review',workflow_version:hash,workflow_revision_id:`review:${hash}`,definition_hash:hash,schema_version:1,runnable:false,
    definition:{nodes:nodes.map(id=>({id,executable:{kind:'operator',id:'test.identity'},mapped:false})),edges:[{id:'route',source:'entry',target,on:'error',conditional:true}],entry_node_ids:['entry'],exit_node_ids:[target]}};
}

test('comparison overlays retain removed Nodes and preserve Edge meanings with collision-free old routes',()=>{
  const base=graph('a',['entry','old'],'old');
  const target=graph('b',['entry','new'],'new');
  target.definition.edges.push({id:'route:base',source:'entry',target:'new',on:'complete',conditional:false});
  const result=comparisonGraph(base,target,[{kind:'node',id:'old',status:'removed',fields:[],ordinal:1},{kind:'node',id:'new',status:'added',fields:[],ordinal:2},{kind:'edge',id:'route',status:'modified',fields:['target'],ordinal:3}]);
  assert.deepEqual(result.workflow.definition.nodes.map(node=>node.id),['entry','new','old']);
  assert.equal(result.workflow.definition.edges.length,3);
  const alias=Object.keys(result.marks.aliases)[0];
  assert.equal(result.marks.aliases[alias],'route');
  assert.notEqual(alias,'route:base');
  const removed=result.workflow.definition.edges.find(edge=>edge.id===alias)!;
  assert.equal(removed.on,'error');
  assert.equal(removed.conditional,true);
  assert.equal(removed.target,'old');
  assert.equal(base.definition.edges.length,1);
});

test('definition differences distinguish missing values from null and ignore object key order',()=>{
  const differences=definitionDifferences({base:{present:true,value:{a:null,keys:{x:1,y:2},array:[1,2]}},target:{present:true,value:{b:null,keys:{y:2,x:1},array:[2,1]}}});
  assert.deepEqual(differences.map(diff=>diff.path),['/a','/array','/b']);
  assert.equal(differences[0].base.present,true);
  assert.equal(differences[0].target.present,false);
  assert.equal(differences[2].target.value,null);
});

test('Split shows each real Revision with its own entry and exit roles, and aliased routes share the original Inspector',()=>{
  const base=graph('a',['entry','old'],'old'),target=graph('b',['entry','new'],'new');
  const drawing=comparisonGraph(base,target,[{kind:'node',id:'old',status:'removed',fields:[],ordinal:1},{kind:'node',id:'new',status:'added',fields:[],ordinal:2},{kind:'edge',id:'route',status:'modified',fields:['target'],ordinal:3}]);
  const split=splitComparisonGraphs(base,target,drawing);
  assert.deepEqual(split.base.workflow.definition.nodes.map(node=>node.id),['entry','old']);
  assert.deepEqual(split.target.workflow.definition.nodes.map(node=>node.id),['entry','new']);
  assert.deepEqual(split.base.workflow.definition.exit_node_ids,['old']);
  assert.deepEqual(split.target.workflow.definition.exit_node_ids,['new']);
  const oldEdge=split.base.workflow.definition.edges[0];
  assert.equal(oldEdge.target,'old');
  assert.equal(oldEdge.conditional,true);
  assert.equal(oldEdge.on,'error');
  assert.equal(split.base.marks.aliases[oldEdge.id],'route');
  assert.equal(split.base.marks.edges[oldEdge.id],'modified');
  assert.equal(drawing.marks.edges[oldEdge.id],'removed');
  assert.equal(base.definition.edges[0].id,'route');
});

test('comparison URLs preserve exact ordered Revisions and Session URLs do not inherit comparison state',()=>{
  const state={view:'workflows' as const,execution:null,workflow:'review',revision:'review:after',compareBase:'review:before',session:null,selection:null};
  assert.deepEqual(readLocation(writeLocation(state)),state);
  assert.equal(readLocation('?view=executions&session=shared&compare=review:before').compareBase,undefined);
});


test('arrays preserve item order while object-key order inside an array remains irrelevant',()=>{
  assert.deepEqual(definitionDifferences({base:{present:true,value:[{a:1,b:2}]},target:{present:true,value:[{b:2,a:1}]}}),[]);
});
