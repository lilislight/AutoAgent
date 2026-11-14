import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement, createRef } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { AgentComposer } from '../src/agent/AgentComposer.js';
import { UsageFacts } from '../src/agent/UsageButton.js';
import { composerAction } from '../src/agent/composerActions.js';
import { composerHeight } from '../src/agent/composerSizing.js';
import { MessageQueue } from '../src/agent/MessageQueue.js';
import type { Usage } from '../src/agent/types.js';

const defaults = { draft: '', canSend: false, running: false, lifecycle: 'completed', canPause: false, canResume: false, sending: false, pending: null } as const;
const usage: Usage = { input_tokens: 10000, output_tokens: 500, total_tokens: 10500, reasoning_tokens: 200, cache_read_tokens: 8000, call_count: 4, pending_calls: 0, unsettled_calls: 0, missing_usage_calls: 0, missing_cost_calls: 0, cost_usd: '0.001234' };

test('the single main action sends text, pauses running work, and resumes paused work', () => {
  assert.equal(composerAction({ ...defaults }).disabled, true);
  assert.equal(composerAction({ ...defaults, draft: 'Question', canSend: true }).kind, 'send');
  const pause = composerAction({ ...defaults, running: true, lifecycle: 'running', canPause: true });
  assert.deepEqual(pause, { kind: 'pause', label: 'Pause task', disabled: false, working: false });
  assert.deepEqual(composerAction({ ...defaults, running: true, lifecycle: 'running' }), { kind: 'none', label: 'Agent running', disabled: true, working: false });
  assert.equal(composerAction({ ...defaults, running: true, lifecycle: 'paused', canResume: true }).kind, 'recover');
});

test('running and paused messages queue the next turn without implying resume or cancellation', () => {
  for (const lifecycle of ['running', 'paused']) {
    const result = composerAction({ ...defaults, draft: 'Please keep the original files.', canSend: true, running: true, lifecycle, canPause: true, canResume: true });
    assert.equal(result.kind, 'send');
    assert.equal(result.disabled, false);
    assert.equal(result.label, 'Queue message');
  }
  assert.equal(composerAction({ ...defaults, running: true, lifecycle: 'paused', canResume: true, draft: '', canSend: true }).kind, 'recover');
});

test('pending control actions keep their spinner visible even with a draft', () => {
  const empty = composerAction({ ...defaults, pending: 'pause', running: true, lifecycle: 'running' });
  assert.equal(empty.label, 'Pausing task');
  assert.equal(empty.disabled, true);
  const message = composerAction({ ...defaults, pending: 'pause', running: true, lifecycle: 'running', draft: 'Update', canSend: true });
  assert.equal(message.working, true);
  assert.equal(message.disabled, true);
});

test('composer sizing grows from two lines, caps at eight, then shrinks with shorter text', () => {
  assert.deepEqual(composerHeight(20, 22, 6, 900), { height: 50, overflowing: false });
  assert.deepEqual(composerHeight(72, 22, 6, 900), { height: 72, overflowing: false });
  assert.deepEqual(composerHeight(1000, 22, 6, 900), { height: 182, overflowing: true });
  assert.deepEqual(composerHeight(50, 22, 6, 900), { height: 50, overflowing: false });
  assert.equal(composerHeight(1000, 22, 6, 300).height, 90);
});

test('the composer has two rows and its running control replaces a duplicate submit action', () => {
  const primary = composerAction({ ...defaults, running: true, lifecycle: 'running', canPause: true });
  const html = renderToStaticMarkup(createElement(AgentComposer, { primary, onControl() {}, session: 'one', draft: '', busy: false, running: true, disabled: false, canSend: true, caption: 'Running', canCancel: true, inputRef: createRef<HTMLTextAreaElement>(), onDraft() {}, onSend() {} }));
  assert.match(html, /rows="2"/);
  assert.match(html, /type="button" class="agent-send" aria-label="Pause task"/);
  assert.match(html, /aria-label="Cancel task"/);
  assert.doesNotMatch(html, /Toggle conversations|Available tools and skills|Trace|Task details/);
});

test('usage shows all recorded tokens and precise available cost without double-counting subsets', () => {
  const html = renderToStaticMarkup(createElement(UsageFacts, { usage }));
  assert.match(html, /10,500/);
  assert.match(html, /10,000/);
  assert.match(html, /8,000/);
  assert.match(html, /\$0\.001234/);
});

test('submitting shows only a spinner while an optional separate action sends an immediate update', () => {
  const primary = composerAction({ ...defaults, sending: true });
  const html = renderToStaticMarkup(createElement(AgentComposer, { primary, onControl() {}, session: 'one', draft: 'Next task', busy: true, running: true, disabled: true, canSend: true, canSendNow: true, onSendNow() {}, caption: 'Running', inputRef: createRef<HTMLTextAreaElement>(), onDraft() {}, onSend() {} }));
  const button = html.match(/<button[^>]*class="agent-send"[^>]*>(.*?)<\/button>/)?.[1];
  assert.match(button ?? '', /loading-spinner/);
  assert.doesNotMatch(button ?? '', /<svg/);
  assert.match(html, /Send now/);
});

test('queued messages expose stable ordering, exact-target send and per-item cancellation', () => {
  const html = renderToStaticMarkup(createElement(MessageQueue, { items: [{ id: 'one', session_id: 'session', position: 1, message: 'Next task', state: 'queued', invocation_id: null, error: null }], pending: null, canSendNow: true, onAction() {} }));
  assert.match(html, /Queued messages/);
  assert.match(html, /Next task/);
  assert.match(html, /Send queued message 1 now/);
  assert.match(html, /Cancel queued message 1/);
});

test('missing pricing is not displayed as free usage and partial totals are labelled', () => {
  const missing = renderToStaticMarkup(createElement(UsageFacts, { usage: { ...usage, cost_usd: '0', missing_cost_calls: 4 } }));
  assert.doesNotMatch(missing, /Cost \(USD\)|\$0/);
  assert.match(missing, /Pricing is unavailable for 4 calls/);
  const partial = renderToStaticMarkup(createElement(UsageFacts, { usage: { ...usage, missing_cost_calls: 1 } }));
  assert.match(partial, /Recorded cost \(USD\)/);
  assert.match(partial, /cost shown is partial/);
});


test('failed and interrupted tasks never expose resume even when generic controls allow it', () => {
  for (const lifecycle of ['failed', 'cancelled', 'interrupted']) {
    const result = composerAction({ ...defaults, lifecycle, canResume: true });
    assert.notEqual(result.kind, 'recover');
  }
  assert.equal(composerAction({ ...defaults, pending: 'cancel' }).label, 'Cancelling task');
});
