import { test } from 'node:test';
import assert from 'node:assert/strict';
import { beforeCalls, callPresentation, executionTiming, pickDefinition, stageDefinitionPath, stageTabs, systemCommandId, valueMessage } from '../src/inspectorModel.js';
import type { TraceRow } from '../src/types.js';
const row: TraceRow = { invocation_id:'run', occurrence_id:'node@root', values:[] };
test('untimed commands do not display the default zero as measured Operator execution', () => {
  assert.equal(executionTiming({...row,operator_id:'system_command:wait',execution_duration_ns:0}),null);
  assert.equal(executionTiming({...row,operator_id:'operator',execution_duration_ns:0}),0);
  assert.equal(executionTiming({...row,operator_id:'system_command:status',execution_duration_ns:1500}),1.5);
});
test('pending command presentation follows its durable Wait without changing completed Calls', () => {
  const call={...row,operator_id:'system_command:wait',status:'running'};
  assert.equal(callPresentation(call,{...row,status:'waiting'}).status,'waiting');
  assert.equal(callPresentation(call,{...row,status:'cancelled'}).status,'cancelled');
  assert.equal(callPresentation({...call,status:'completed'},{...row,status:'waiting'}).status,'completed');
  assert.equal(callPresentation({...call,operator_id:'work'},{...row,status:'waiting'}).status,'running');
});
test('missing values distinguish skipped work, Mapping failure, pending output and failed output', () => {
  assert.match(valueMessage({...row,status:'skipped'},'input'),/skipped/);
  assert.match(valueMessage({...row,status:'failed',error_phase:'input_mapping'},'input'),/Mapping failed/);
  assert.match(valueMessage({...row,status:'waiting'},'output'),/when this execution completes/);
  assert.match(valueMessage({...row,status:'failed'},'output'),/ended without/);
  assert.match(valueMessage({...row,status:'completed'},'output'),/not recorded/);
});
test('stage definitions point at the recorded hook, including faulted phases and absent hooks', () => {
  const path=stageDefinitionPath({...row,event_name:'node.faulted',error_phase:'aggregation'});
  assert.deepEqual(path,['map','aggregate']);
  assert.deepEqual(pickDefinition({map:{aggregate:{name:'merge'}}},path),{name:'merge'});
  assert.equal(pickDefinition({map:null},path),undefined);
  assert.deepEqual(stageDefinitionPath({...row,event_name:'input.mapped'}),['input_mapping']);
});
test('preparation faults stay before Calls while aggregation faults belong to result processing', () => {
  assert.equal(beforeCalls({...row,event_name:'node.faulted',error_phase:'input_mapping'}),true);
  assert.equal(beforeCalls({...row,event_name:'node.faulted',error_phase:'aggregation'}),false);
});
test('stage sections expose owned values without empty or duplicate error and routing sections', () => {
  assert.deepEqual(stageTabs({...row,event_name:'capability.resolved'}), ['Summary','Definition']);
  assert.deepEqual(stageTabs({...row,event_name:'input.mapped',values:['mapped_input']}), ['Summary','Input / Output','Definition']);
  assert.deepEqual(stageTabs({...row,event_name:'routing.resolved',values:['conditions']}), ['Summary','Execution']);
  assert.deepEqual(stageTabs({...row,event_name:'node.faulted',error_phase:'aggregation',values:['error']}), ['Summary','Definition']);
  assert.deepEqual(stageTabs({...row,event_name:'node.faulted',error_phase:'validation',values:['error']}), ['Summary']);
});
test('durable wait inspectors preserve existing command prefixes', () => {
  assert.equal(systemCommandId('timer'),'system_command:timer');
  assert.equal(systemCommandId('system_command:select'),'system_command:select');
});
