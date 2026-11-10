import assert from 'node:assert/strict';
import test from 'node:test';
import { interactionReply, submitInteraction } from '../src/agent/interactionSubmission.js';
import { retryAgentRead } from '../src/agent/readFeedback.js';
import { eventRefresh } from '../src/agent/eventRefresh.js';
import type { ActionSpec, AgentEvent } from '../src/agent/types.js';

const spec = { request: { kind: 'agent.tool_approval', requests: [{ id: 'one' }, { id: 'two' }], revision: 'v1' } } as unknown as ActionSpec;
test('approval validates locally and applies the selected batch action to every request', () => {
  assert.throws(() => interactionReply(spec.request!, {}, {}, {}, 'submit'), /every tool/);
  const denied = interactionReply(spec.request!, {}, {}, { one: 'Keep private' }, 'deny') as { decisions: unknown[] };
  assert.deepEqual(denied.decisions, [{ request_id: 'one', approved: false, reason: 'Keep private' }, { request_id: 'two', approved: false }]);
});
test('questions require an answer or explicit skip before sending', () => {
  const request = { kind: 'agent.ask_user', questions: [{ id: 'q' }] };
  assert.throws(() => interactionReply(request, {}, {}, {}, 'submit'), /Answer or skip/);
  assert.deepEqual(interactionReply(request, { q: { text: 'Ignored when skipped', selected: ['x'], skipped: true } }, {}, {}, 'submit'), {
    answers: [{ question_id: 'q', status: 'skipped', text: null, selected_option_ids: [] }],
  });
});
test('a resolved or revised approval reconciles without a false error or a mutation', async () => {
  let mutations = 0;
  const port = { current: async () => ({ ...spec, request: null }), respond: async () => { mutations++; }, onSubmit() {} };
  assert.deepEqual(await submitInteraction(spec, {}, port), { state: 'resolved' });
  const changed = { ...spec, request: { ...spec.request!, revision: 'v2' } };
  assert.deepEqual(await submitInteraction(spec, {}, { ...port, current: async () => changed }), { state: 'changed', spec: changed });
  assert.equal(mutations, 0);
});
test('submission preserves exact payload and reports acceptance only after the action succeeds', async () => {
  const events: string[] = [], payload = interactionReply(spec.request!, {}, {}, {}, 'approve');
  const result = await submitInteraction(spec, payload, {
    current: async () => { events.push('checked'); return spec; },
    onSubmit: () => { events.push('submitting'); },
    respond: async value => { assert.equal(value, payload); events.push('responded'); },
  });
  assert.deepEqual(events, ['checked', 'submitting', 'responded']);
  assert.equal(result.state, 'accepted');
});
test('a conflict re-reads the batch without automatically retrying a decision', async () => {
  let reads = 0, writes = 0;
  const changed = { ...spec, request: { ...spec.request!, revision: 'v2' } };
  const result = await submitInteraction(spec, {}, {
    current: async () => ++reads === 1 ? spec : changed,
    respond: async () => { writes++; throw Object.assign(new Error('View changed'), { status: 409 }); }, onSubmit() {},
  });
  assert.deepEqual(result, { state: 'changed', spec: changed });
  assert.equal(writes, 1);
});
test('real action failures remain actionable and are not converted to success', async () => {
  const failure = Object.assign(new Error('Permission denied'), { status: 403 });
  await assert.rejects(submitInteraction(spec, {}, { current: async () => spec, respond: async () => { throw failure; }, onSubmit() {} }), error => error === failure);
});
test('Host wait resolution and coded approval conflicts reconcile without another mutation', async () => {
  const missing = Object.assign(new Error('Wait no longer pending'), { code: 'wait_unavailable' });
  assert.equal((await submitInteraction(spec, {}, { current: async () => { throw missing; }, respond: async () => assert.fail('Must not respond'), onSubmit() {} })).state, 'resolved');
  let reads = 0, writes = 0;
  const result = await submitInteraction(spec, {}, { current: async () => ++reads === 1 ? spec : { ...spec, request: null },
    respond: async () => { writes++; throw Object.assign(new Error('Approval changed'), { code: 'approval_changed' }); }, onSubmit() {} });
  assert.equal(result.state, 'resolved');
  assert.equal(writes, 1);
  const changed = Object.assign(new Error('Resources changed during approval evaluation'), { code: 'approval_changed' });
  assert.equal((await submitInteraction(spec, {}, { current: async () => { throw changed; }, respond: async () => assert.fail('Must review new permissions first'), onSubmit() {} })).state, 'refresh');
  assert.equal(retryAgentRead(0, changed), true);
});
test('brief read propagation failures retry within a bound, while invalid or forbidden reads do not', () => {
  for (const status of [404, 503]) {
    const error = Object.assign(new Error('Read unavailable'), { status });
    assert.equal(retryAgentRead(0, error), true);
    assert.equal(retryAgentRead(2, error), false);
  }
  for (const status of [400, 401, 403, 409]) assert.equal(retryAgentRead(0, Object.assign(new Error(), { status })), false);
});
test('ordinary tool and text updates render without invalidating execution and directory queries', () => {
  const tools = { kind: 'agent.tools', persist: true, payload: { tools: [{ name: 'read_file', state: 'completed' }] } } as AgentEvent;
  assert.deepEqual(eventRefresh([tools, { kind: 'agent.response.delta' } as AgentEvent]), { execution: false, invocations: false, directory: false });
  assert.deepEqual(eventRefresh([{ kind: 'agent.interaction' } as AgentEvent]), { execution: true, invocations: true, directory: false });
  assert.deepEqual(eventRefresh([{ kind: 'agent.completed' } as AgentEvent]), { execution: true, invocations: true, directory: true });
});
