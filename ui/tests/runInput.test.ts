import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runInput } from '../src/runInput.js';
const args = {workflow:'wf',revision:'exact-revision',entry:'entry',text:'{"text":"hello"}',sessionMode:'new' as const,session:''};
test('Run preserves exact Revision and distinguishes generated, custom and existing Session IDs',()=>{
  assert.deepEqual(runInput(args),{workflow_id:'wf',revision_id:'exact-revision',entry_node_id:'entry',input:{text:'hello'},session_mode:'new'});
  assert.deepEqual(runInput({...args,session:' chosen '}),{workflow_id:'wf',revision_id:'exact-revision',entry_node_id:'entry',input:{text:'hello'},session_mode:'new',session_id:'chosen'});
  assert.deepEqual(runInput({...args,sessionMode:'existing',session:' s '}),{workflow_id:'wf',revision_id:'exact-revision',entry_node_id:'entry',input:{text:'hello'},session_mode:'existing',session_id:'s'});
  assert.throws(()=>runInput({...args,entry:''}),/Entry/);
  assert.throws(()=>runInput({...args,text:'{'}),/valid JSON/);
  assert.throws(()=>runInput({...args,sessionMode:'existing',session:' '}),/Session ID/);
});
