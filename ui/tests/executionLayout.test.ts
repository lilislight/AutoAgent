import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readExecutionLayout, DEFAULT_EXECUTION_LAYOUT, graphRatioAt } from '../src/executionLayout.js';
test('layout cache validates corrupted values and restores the default after clearing', () => {
  assert.equal(DEFAULT_EXECUTION_LAYOUT.orientation,'vertical');
  for (const raw of [null, '{', '{}', '{"orientation":"diagonal"}']) assert.deepEqual(readExecutionLayout(raw), DEFAULT_EXECUTION_LAYOUT);
  assert.deepEqual(readExecutionLayout('{"orientation":"vertical","ratio":0.7,"view":"split"}'), { orientation:'vertical',ratio:.7,view:'split' });
  assert.equal(readExecutionLayout('{"orientation":"horizontal","ratio":8}').ratio,.8);
  assert.equal(readExecutionLayout('{"orientation":"vertical","view":"invalid"}').view,'graph');
});
test('view choice and Split direction survive independent changes', () => {
  const saved = readExecutionLayout('{"orientation":"vertical","view":"timeline","ratio":0.6}');
  assert.deepEqual(readExecutionLayout(JSON.stringify({ ...saved, view:'split' })), { orientation:'vertical',view:'split',ratio:.6 });
});
test('resizing always keeps Graph at the start of the fixed pane order', () => {
  assert.equal(graphRatioAt(300,1000,.2,.8),.3);
  assert.equal(graphRatioAt(950,1000,.2,.8),.8);
  assert.equal(graphRatioAt(50,1000,.2,.8),.2);
});
