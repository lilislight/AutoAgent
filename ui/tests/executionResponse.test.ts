import assert from 'node:assert/strict';
import test from 'node:test';
import { responseEditor } from '../src/executionResponse.js';
import type { ExecutionActionSpec } from '../src/types.js';

const spec: ExecutionActionSpec = {
  root_invocation_id: 'root', action: 'respond', input_schema: {type: 'object'}, endpoints: [], next_sequence: 1,
  request: {kind: 'agent.tool_approval', revision: 'current', requests: [{id: 'current-tool'}]},
  response_example: {decisions: [{request_id: 'current-tool', approved: false}], revision: 'current'},
};

test('approval editor uses the current batch identities and requires a deliberate approval', () => {
  const editor = responseEditor(spec, {});
  assert.deepEqual(JSON.parse(editor.text), spec.response_example);
  assert.equal(editor.stale, false);
});

test('a changed approval batch blocks an earlier affirmative draft', () => {
  const previous = responseEditor(spec, {});
  const text = JSON.stringify({decisions: [{request_id: 'current-tool', approved: true}], revision: 'current'});
  const draft = {text, requestKey: previous.requestKey};
  assert.equal(responseEditor(spec, draft).stale, false);
  const next = responseEditor({...spec, request: {...spec.request as object, revision: 'new'}}, draft);
  assert.equal(next.stale, true);
  assert.equal(next.text, text);
  assert.equal(responseEditor(spec, {text}).stale, true);
});

test('ordinary Wait replies still use their registered response schema', () => {
  const editor = responseEditor({...spec, request: {question: 'Ready?'}, response_example: null,
    input_schema: {type: 'object', properties: {text: {type: 'string', default: 'reply'}}}}, {});
  assert.deepEqual(JSON.parse(editor.text), {text: 'reply'});
});
