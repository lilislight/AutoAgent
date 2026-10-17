import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parentRecord, recordMatches, selectRecord, selectResponseNode, selectedInterval } from '../src/executionSelection.js';
import { readLocation, writeLocation } from '../src/navigation.js';
import type { TraceRow } from '../src/types.js';
const row: TraceRow = { invocation_id: 'run', node_id: 'review', occurrence_id: 'review@loop:3', values: [], call_id: 'call' };
test('selection links preserve exact record IDs, sections and execution views', () => {
  for (const kind of ['node', 'call', 'wait', 'edge'] as const) {
    const selected = selectRecord(kind, {...row, wait_id: 'call', edge_id: 'again', event_id:'event', result_index: 2}, 'Input / Output');
    const state = { ...readLocation('?view=executions&execution=run'), executionView: 'split' as const, selection: selected };
    assert.deepEqual(readLocation(writeLocation(state)), state);
  }
});
test('Invocation links preserve the selected section instead of resetting to Actions', () => {
  const state = readLocation('?view=executions&execution=run&invocation=run&section=Summary');
  assert.equal(state.selection?.section, 'Summary');
  assert.deepEqual(readLocation(writeLocation(state)), state);
  assert.equal(readLocation('?view=executions&execution=run&invocation=run&section=unknown').selection?.section, undefined);
});
test('records from another Graph element never satisfy an exact selection', () => {
  const selection = selectRecord('call', row)!;
  assert.equal(recordMatches(selection, row), true);
  assert.equal(recordMatches(selection, {...row, node_id:'other'}), false);
  assert.equal(recordMatches(selection, {...row, call_id:'other'}), false);
  assert.equal(readLocation('?view=executions&node=review&record_kind=edge&record=event:0').selection, null);
  assert.equal(readLocation('?view=executions&node=review&record_kind=unknown&record=call').selection, null);
});
test('Wait and Call details both return directly to their Node occurrence', () => {
  const wait = { ...row, wait_id:'call', kind:'signal' };
  assert.deepEqual(parentRecord(selectRecord('wait', wait)!, wait), selectRecord('node', row, 'Execution'));
  assert.deepEqual(parentRecord(selectRecord('call', row)!, row), selectRecord('node', row, 'Execution'));
});
test('response notifications select the exact Node occurrence and mapped Wait across navigation', () => {
  const wait = { ...row, invocation_id:'child', wait_id:'unit-2' };
  const selected = selectResponseNode(wait)!;
  assert.deepEqual(selected, { kind:'node', id:'review', record:{ kind:'node', id:row.occurrence_id, section:'Actions', waitId:'unit-2' } });
  const state = { ...readLocation('?view=executions&execution=child'), selection:selected };
  assert.deepEqual(readLocation(writeLocation(state)), state);
  assert.equal(recordMatches(selected, { ...row, call_id:undefined }), true);
  assert.equal(selectResponseNode({...wait, occurrence_id:undefined}), null);
  assert.equal(selectResponseNode({...wait, node_id:undefined}), null);
  assert.equal(selectResponseNode({...wait, wait_id:undefined}), null);
});
test('external Wait links resolve to Node details while internal timer links retain their records', () => {
  const call = { ...row, operator_id:'system_command:wait' };
  const selected = selectRecord('call', call, 'Input / Output')!;
  assert.deepEqual(selected.record, {kind:'node',id:row.occurrence_id,section:'Input / Output',waitId:'call'});
  assert.equal(recordMatches(selected, call),false);
  const wait = {...row,call_id:undefined,kind:'external',wait_id:'call'};
  assert.deepEqual(selectRecord('wait',wait)?.record,{kind:'node',id:row.occurrence_id,waitId:'call'});
  assert.equal(recordMatches(selected, wait),false);
  assert.equal(selectRecord('call',{...call,operator_id:'system_command:timer'})?.record?.kind,'call');
  assert.equal(selectRecord('wait',{...wait,kind:'timer'})?.record?.kind,'wait');
});
test('selected intervals use persisted boundaries and keep Edge evaluation as a point', () => {
  assert.deepEqual(selectedInterval('wait', {...row,created_at_us:100,end_at_us:140}), {start_at_us:100,end_at_us:140,status:undefined});
  assert.deepEqual(selectedInterval('edge', {...row,occurred_at_us:200}), {start_at_us:200,end_at_us:200,status:undefined});
  assert.equal(selectedInterval('node', row),null);
});
