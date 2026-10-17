import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ReplayPositions } from '../src/replayPosition.js';
test('Replay bookmarks preserve slider time and distinguish source and Invocation', () => {
  const positions = new ReplayPositions();
  positions.write('source:one', { at_us: 1234567 });
  positions.write('source:two', { sequence: 131 });
  assert.deepEqual(positions.read('source:one'), { at_us: 1234567 });
  assert.deepEqual(positions.read('source:two'), { sequence: 131 });
  assert.equal(positions.read('rebuilt-source:one'), undefined);
  const copy = positions.read('source:one')!; copy.at_us = 0;
  assert.equal(positions.read('source:one')!.at_us, 1234567);
});
test('Replay bookmarks keep recent positions without retaining an unbounded history', () => {
  const positions = new ReplayPositions(2);
  positions.write('one', { sequence: 1 }); positions.write('two', { sequence: 2 });
  positions.read('one'); positions.write('three', { sequence: 3 });
  assert.equal(positions.read('two'), undefined);
  assert.deepEqual(positions.read('one'), { sequence: 1 });
  positions.write('one', { at_us: 42 });
  assert.deepEqual(positions.read('one'), { at_us: 42 });
});
