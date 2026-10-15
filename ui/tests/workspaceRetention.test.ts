import { test } from 'node:test';
import assert from 'node:assert/strict';
import { retainedViews } from '../src/workspaceRetention.js';
const root = { key: 'root', family: 'a' };
const child = (key: string, family = 'a') => ({ key, family });
const group = (value: typeof root) => value.family;

test('family views remain bounded and preserve Root after visiting many siblings', () => {
  let retained: typeof root[] = [];
  let previous = root;
  for (let index = 0; index < 50; index++) {
    const current = child(String(index));
    retained = retainedViews(current, [previous, ...retained], group, 4, 'root');
    assert.equal(retained.some(value => value.key === current.key), false);
    assert.ok(retained.length <= 3);
    assert.ok(retained.some(value => value.key === 'root'));
    previous = current;
  }
  assert.deepEqual(retained.map(value => value.key), ['48', '47', 'root']);
});
test('switching families releases other views and repeated visits never duplicate a view', () => {
  assert.deepEqual(retainedViews(child('b', 'b'), [root, child('a')], group, 4, 'root'), []);
  const updated = { ...root, version: 2 };
  const retained = retainedViews(child('a'), [updated, root, child('a'), child('b')], group, 4, 'root');
  assert.deepEqual(retained.map(value => value.key), ['root', 'b']);
  assert.equal(retained[0], updated);
  assert.deepEqual(retainedViews(child('a'), [root], group, 1), []);
});
