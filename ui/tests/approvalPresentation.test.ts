import assert from 'node:assert/strict';
import test from 'node:test';
import { requiresHumanResponse, toolApprovalRecord } from '../src/agent/approvalPresentation.js';
import type { ActionSpec, Interaction } from '../src/agent/types.js';

test('only human-owned pending requests expose an approval form', () => {
  const request = { kind: 'agent.tool_approval', requests: [{ id: 'one' }] };
  for (const reviewer of ['system', 'policy', 'agent'] as const) {
    assert.equal(requiresHumanResponse({ request, review: { reviewer, requires_user_input: false } } as unknown as ActionSpec), false);
  }
  assert.equal(requiresHumanResponse({ request, review: { reviewer: 'user', requires_user_input: true } } as unknown as ActionSpec), true);
  assert.equal(requiresHumanResponse({ request } as unknown as ActionSpec), false);
  assert.equal(requiresHumanResponse({ request: { kind: 'agent.ask_user' } } as unknown as ActionSpec), true);
  assert.equal(requiresHumanResponse({ request: null } as unknown as ActionSpec), false);
});

test('mixed batch approval records are matched per Tool rather than by the batch source', () => {
  const interaction = { type: 'approval', state: 'resolved',
    request: { requests: [{ id: 'one', call: { call_id: 'c1' } }, { id: 'two', call: { call_id: 'c2' } }, { id: 'three', call: { call_id: 'c3' } }] },
    resolution: { response: { source: 'user', resolutions: [
      { request_id: 'one', source: 'system', approval: { approved: true, reason: 'Automatically approved by approval mode.' } },
      { request_id: 'two', source: 'user', approval: { approved: false, reason: 'Keep private' } },
      { request_id: 'three', source: 'policy', result: { is_error: true } },
    ] } } } as unknown as Interaction;
  assert.deepEqual(toolApprovalRecord(interaction, 'c1'), { label: 'Automatically approved' });
  assert.deepEqual(toolApprovalRecord(interaction, 'c2'), { label: 'Denied by you', reason: 'Keep private' });
  assert.deepEqual(toolApprovalRecord(interaction, 'c3'), { label: 'Blocked by policy' });
  assert.equal(toolApprovalRecord(interaction, 'missing'), null);
  assert.equal(toolApprovalRecord({ ...interaction, state: 'pending' }, 'c1'), null);
});
