import assert from 'node:assert/strict';
import test from 'node:test';
import { ScrollFollowing } from '../src/agent/scrollFollowing.js';
import { logDisclosure } from '../src/agent/logDisclosure.js';

test('one small upward gesture stops following before the next streaming update', () => {
  const scroll = new ScrollFollowing();
  scroll.position(500, 1000, 500);
  scroll.interrupt();
  assert.equal(scroll.record(480, 1000, 500), true);
  scroll.position(480, 1050, 500);
  assert.equal(scroll.following, false);
  assert.equal(scroll.record(480, 1100, 500), true);
  assert.equal(scroll.following, false);
});

test('a scrollbar drag stops following and only reaching the bottom resumes it', () => {
  const scroll = new ScrollFollowing();
  scroll.position(500, 1000, 500);
  assert.equal(scroll.record(490, 1000, 500), true);
  assert.equal(scroll.following, false);
  scroll.record(495, 1000, 500);
  assert.equal(scroll.following, false);
  assert.equal(scroll.record(500, 1000, 500), false);
  assert.equal(scroll.following, true);
});

test('a layout collapse does not misclassify browser clamping as upward reading', () => {
  const scroll = new ScrollFollowing();
  scroll.position(1500, 2000, 500);
  scroll.record(500, 1000, 500);
  assert.equal(scroll.following, true);
  scroll.interrupt();
  scroll.resume();
  assert.equal(scroll.following, true);
});

test('final answers collapse inspected process logs but allow explicitly reopening them', () => {
  assert.deepEqual(logDisclosure(undefined, undefined), { phase: 'process', open: true });
  assert.equal(logDisclosure('answer', { phase: 'process', open: true }).open, false);
  assert.equal(logDisclosure('answer', { phase: 'process', open: false }).open, false);
  assert.equal(logDisclosure('answer', { phase: 'final:answer', open: true }).open, true);
  assert.equal(logDisclosure('new-answer', { phase: 'final:answer', open: true }).open, false);
  assert.equal(logDisclosure(undefined, { phase: 'final:answer', open: false }).open, true);
});
