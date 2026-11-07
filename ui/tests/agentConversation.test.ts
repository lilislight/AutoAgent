import { executionStatus } from '../src/agent/executionState.js';
import { newId } from '../src/agent/ids.js';
import assert from 'node:assert/strict';
import test from 'node:test';
import { applyEvents, emptyConversation, entityKey, resetTransient, sortedRows } from '../src/agent/conversationState.js';
import { approvalReply, requestSignature } from '../src/agent/replies.js';
import { mediaUrl, safeUrl } from '../src/agent/media.js';
import { effectiveSettings } from '../src/agent/settingsState.js';
import { readLocation, writeLocation } from '../src/navigation.js';
import type { AgentExecution, SettingsRecord, ActionSpec, AgentEvent, Payloads, ToolSnapshot } from '../src/agent/types.js';
let ordinal = 0;
function event<K extends keyof Payloads>(kind: K, payload: Payloads[K], persist = true, iid = 'invocation'): AgentEvent {
  ordinal++;
  return { id: `event-${ordinal}`, kind, session_id: 'session', invocation_id: iid, occurrence_id: 'node', occurred_at_us: ordinal, persist, sequence: persist ? ordinal : null, payload: { v: 1, node_id: 'llm_call', ...payload } } as AgentEvent;
}
const delta = (text: string, stream = 'stream') => event('agent.response.delta', { response_id: 'response', stream_id: stream, item_key: 'text', content_index: 0, kind: 'text', delta: text }, false);
const final = () => event('agent.response', { response_id: 'response', model: 'model', finish_reason: 'stop', usage: null, blocks: [{ type: 'text', text: 'Hello' }] });
const tool: ToolSnapshot = { tool_id: 'response:call', response_id: 'response', call_id: 'call', name: 'read', arguments: '{}', state: 'completed', result: { content: [{ type: 'text', text: 'result' }], structured_content: null, is_error: false }, progress: null };

test('live deltas become one complete response across history reconciliation', () => {
  const terminal = final();
  let state = applyEvents(emptyConversation(), [delta('Hel'), delta('lo')]);
  assert.equal(state.live[entityKey('invocation', 'response')].parts[0].text, 'Hello');
  state = applyEvents(state, [terminal, terminal, delta('stale')]);
  assert.equal(sortedRows(state).length, 1);
  assert.equal(Object.keys(state.live).length, 0);
  assert.equal(Object.keys(state.responses).length, 1);
  assert.equal(state.seen.size, 1);
});

test('a new Operator stream replaces partial failed-attempt content', () => {
  const state = applyEvents(emptyConversation(), [delta('old', 'attempt-1'), delta('new', 'attempt-2')]);
  assert.equal(state.live[entityKey('invocation', 'response')].parts[0].text, 'new');
});

test('reconnect discards live-only state and retains final tool snapshots', () => {
  const snapshot = event('agent.tools', { tools: [tool] });
  let state = applyEvents(emptyConversation(), [snapshot, delta('interrupted')]);
  state = applyEvents(state, [event('agent.tools', { tools: [{ ...tool, state: 'running', result: null }] }, false)]);
  state = resetTransient(state);
  assert.equal(state.tools[entityKey('invocation', tool.tool_id)].state, 'completed');
  assert.equal(sortedRows(state).length, 0);
  state = applyEvents(state, [final()]);
  assert.equal(sortedRows(state).length, 1);
});

test('entities are scoped by Invocation even when providers reuse call IDs', () => {
  const state = applyEvents(emptyConversation(), [event('agent.tools', { tools: [tool] }, true, 'one'), event('agent.tools', { tools: [{ ...tool, state: 'denied' }] }, true, 'two')]);
  assert.equal(Object.keys(state.tools).length, 2);
  assert.equal(state.tools[entityKey('one', tool.tool_id)].state, 'completed');
  assert.equal(state.tools[entityKey('two', tool.tool_id)].state, 'denied');
});

test('approval replies use each current request ID and require the complete batch', () => {
  const request = { request_id: 'batch-id', requests: [{ id: 'actual-first' }, { id: 'actual-second' }], revision: 'view-version' };
  assert.throws(() => approvalReply(request, { 'batch-id': true }), /every tool/);
  assert.deepEqual(approvalReply(request, { 'actual-first': true, 'actual-second': false }), { decisions: [{ request_id: 'actual-first', approved: true }, { request_id: 'actual-second', approved: false }], source: 'user', revision: 'view-version', resolutions: [] });
});

test('unsafe media and markdown links cannot become executable URLs', () => {
  assert.equal(safeUrl('javascript:alert(1)'), undefined);
  assert.equal(safeUrl('data:text/html;base64,QQ=='), undefined);
  assert.equal(safeUrl('data:image/svg+xml;base64,QQ==', true), undefined);
  assert.equal(safeUrl('data:image/png;base64,QQ==', true), 'data:image/png;base64,QQ==');
  assert.equal(safeUrl('https://example.com/file'), 'https://example.com/file');
});

test('Agent routes round-trip without Trace selections or panes', () => {
  const value = readLocation('?view=agents&session=session&execution=child&workflow=worker&node=trace-node&pane=timeline');
  assert.equal(value.selection, null);
  assert.equal(value.executionView, undefined);
  assert.equal(value.workflow, null);
  assert.equal(writeLocation(value), '?view=agents&session=session&execution=child');
  assert.deepEqual(readLocation(writeLocation(value)), value);
});

test('request identity ignores JSON key order across resident and durable reads', () => {
  const first = { request: { kind: 'agent.ask_user', questions: [{ id: 'scope', text: 'Scope?' }], request_id: 'stable' } } as unknown as ActionSpec;
  const second = { request: { request_id: 'stable', questions: [{ text: 'Scope?', id: 'scope' }], kind: 'agent.ask_user' } } as unknown as ActionSpec;
  assert.equal(requestSignature(first), requestSignature(second));
  second.request!.request_id = 'new-batch';
  assert.notEqual(requestSignature(first), requestSignature(second));
});

test('compaction completion survives late progress and reconnect', () => {
  const done = event('agent.compaction', { compaction_id: 'compact', state: 'completed', before_tokens: 1000, after_tokens: 400, view_generation: 1 });
  const late = event('agent.compaction', { compaction_id: 'compact', state: 'running', before_tokens: 1000, after_tokens: null, view_generation: 0 }, false);
  const state = resetTransient(applyEvents(emptyConversation(), [done, late]));
  assert.equal(state.compactions[entityKey('invocation', 'compact')].state, 'completed');
  assert.equal(state.compactions[entityKey('invocation', 'compact')].after_tokens, 400);
});

test('portable inline media is limited to native image/audio/video formats', () => {
  assert.equal(mediaUrl({ type: 'data', mime_type: 'audio/mpeg', data: 'QQ==' }, 'audio'), 'data:audio/mpeg;base64,QQ==');
  assert.equal(mediaUrl({ type: 'data', mime_type: 'text/html', data: 'QQ==' }, 'video'), undefined);
  assert.equal(mediaUrl({ type: 'data', mime_type: 'image/svg+xml', data: 'QQ==' }, 'image'), undefined);
  assert.equal(mediaUrl({ type: 'asset', uri: '/etc/passwd' }, 'file'), undefined);
});

test('Child settings inherit the active snapshot only until explicitly configured', () => {
  const record = { session_id: 'child', version: 0, settings: { model: { model: 'host-default' } } } as SettingsRecord;
  const snapshot = { snapshot_id: 'stage', settings_version: 0, settings: { model: { model: 'parent-model' } } } as AgentExecution['snapshot'];
  assert.equal(effectiveSettings(record, snapshot, true)?.settings.model.model, 'parent-model');
  assert.equal(effectiveSettings(record, snapshot, false), record);
  const configured = { ...record, version: 1 };
  assert.equal(effectiveSettings(configured, snapshot, true), configured);
  assert.equal(effectiveSettings(undefined, snapshot, true), undefined);
});

test('LAN HTTP sessions can generate IDs without the secure-context randomUUID API', () => {
  const original = Object.getOwnPropertyDescriptor(globalThis, 'crypto');
  Object.defineProperty(globalThis, 'crypto', { configurable: true, value: {
    getRandomValues: (bytes: Uint8Array) => bytes.fill(17),
  } });
  try {
    assert.match(newId(), /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/);
  } finally {
    if (original) Object.defineProperty(globalThis, 'crypto', original);
    else Reflect.deleteProperty(globalThis, 'crypto');
  }
});

test('execution headings distinguish paused control from Invocation lifecycle', () => {
  const paused = { control_state: 'paused', recovery_reason: 'paused' };
  assert.equal(executionStatus('running', paused), 'paused');
  assert.equal(executionStatus('waiting', paused), 'paused');
  assert.equal(executionStatus('completed', paused), 'completed');
  assert.equal(executionStatus('running', { control_state: 'active', recovery_reason: 'interrupted' }), 'interrupted');
  assert.equal(executionStatus('waiting', { control_state: 'active', recovery_reason: 'interrupted' }), 'waiting');
  assert.equal(executionStatus('failed', { control_state: 'active', recovery_reason: 'failed' }), 'failed');
});
