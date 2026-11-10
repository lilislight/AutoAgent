import { test } from 'node:test';
import assert from 'node:assert/strict';
import { stackIndex, stackJump, stackRecords, stackStatus, stepStack } from '../src/stackSelection.js';

test('card selection is symmetric and button navigation never skips a fractional position', () => {
  assert.equal(stackIndex(.49,14),0);
  assert.equal(stackIndex(.51,14),1);
  assert.equal(stackIndex(12.49,14),12);
  assert.equal(stackIndex(12.51,14),13);
  assert.equal(stepStack(.2,1,14),1);
  assert.equal(stepStack(.8,-1,14),0);
  assert.equal(stepStack(13,1,14),13);
  assert.equal(stepStack(0,-1,14),0);
});

test('jump input uses the displayed ordinal and rejects out-of-range or non-integer values', () => {
  assert.equal(stackJump('1',14),13);
  assert.equal(stackJump('14',14),0);
  assert.equal(stackJump('5',14),9);
  for (const text of ['', '0', '15', '-1', '1.5', '1e1', 'x', 'Infinity']) assert.equal(stackJump(text,14),null);
  assert.equal(stackJump('1',0),null);
});

test('status filtering preserves original record identities and execution ordinals', () => {
  const records=stackRecords([{invocation_id:'run',occurrence_id:'new',status:'completed',values:[]},{invocation_id:'run',occurrence_id:'middle',status:'failed',values:[]},{invocation_id:'run',occurrence_id:'old',status:'completed',values:[]}],3,'node');
  assert.deepEqual(records.filter(row=>row.status==='completed').map(row=>[row.row.occurrence_id,row.ordinal,row.sourceIndex]),[['new',3,0],['old',1,2]]);
  assert.equal(stackStatus({invocation_id:'run',status:'waiting',control_state:'paused',values:[]},'node'),'paused');
  assert.equal(stackStatus({invocation_id:'run',condition_result:true,values:[]},'edge'),'selected');
  assert.equal(stackStatus({invocation_id:'run',condition_result:false,values:[]},'edge'),'skipped');
});
