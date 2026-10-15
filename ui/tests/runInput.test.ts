import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runInput } from '../src/runInput.js';
const args={workflow:'wf',revision:'exact-revision',entry:'entry',text:'{"text":"hello"}',sessionMode:'new' as const,session:'',context:'{}',includeContext:false};
test('Run keeps the exact Revision and Entry, parses JSON, and excludes context when reusing a Session',()=>{
  assert.deepEqual(runInput(args),{workflow_id:'wf',revision_id:'exact-revision',entry_node_id:'entry',input:{text:'hello'},session_mode:'new'});
  assert.deepEqual(runInput({...args,sessionMode:'existing',session:' s ',includeContext:true,context:'invalid'}),{workflow_id:'wf',revision_id:'exact-revision',entry_node_id:'entry',input:{text:'hello'},session_mode:'existing',session_id:'s'});
  assert.deepEqual(runInput({...args,text:'null',context:'{"project":"test"}',includeContext:true}).session_context,{project:'test'});
  assert.throws(()=>runInput({...args,entry:''}),/Entry/);
  assert.throws(()=>runInput({...args,text:'{'}),/valid JSON/);
  assert.throws(()=>runInput({...args,sessionMode:'existing',session:' '}),/Session ID/);
  assert.throws(()=>runInput({...args,includeContext:true,context:'[]'}),/JSON object/);
});
