import { test } from 'node:test';
import assert from 'node:assert/strict';
import { constrainInspectorWidth } from '../src/inspectorWidth.js';
test('Inspector width accommodates narrow workspaces and bounds large preferences', () => {
  assert.equal(constrainInspectorWidth(760, 280), 280);
  assert.equal(constrainInspectorWidth(120, 700), 320);
  assert.equal(constrainInspectorWidth(2000, 1800), 900);
  assert.equal(constrainInspectorWidth(760, 700), 700);
  assert.equal(constrainInspectorWidth(Number.NaN, 700), 460);
  assert.equal(constrainInspectorWidth(460, 0), 1);
});
