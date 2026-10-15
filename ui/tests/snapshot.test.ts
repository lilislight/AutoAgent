import { test } from 'node:test';
import assert from 'node:assert/strict';
import { belongsToSnapshot, metadataScope, snapshotTime } from '../src/snapshot.js';
test('refresh barriers only include this Invocation and reading cycle', () => {
  const meta = metadataScope('parent', 123);
  assert.equal(belongsToSnapshot(['execution-record','parent'], meta, 'parent',123),true);
  assert.equal(belongsToSnapshot(['family-children','child-owner'], meta, 'parent',123),true);
  assert.equal(belongsToSnapshot(['execution-record','parent'], meta, 'other',123),false);
  assert.equal(belongsToSnapshot(['execution-record','parent'], meta, 'parent',124),false);
});
test('refresh excludes business values and immutable definitions even if mislabelled', () => {
  for(const kind of ['trace-value','workflow','workflow-object','workflow-revision','execution-sessions-page']) {
    assert.equal(belongsToSnapshot([kind,'parent'], metadataScope('parent',123),'parent',123),false);
  }
  assert.equal(belongsToSnapshot(['execution-record','parent'],undefined,'parent',123),false);
  assert.equal(snapshotTime(null),'No saved events');
});
