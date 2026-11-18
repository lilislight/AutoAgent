import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { applyEvents, emptyConversation, resetTransient } from '../src/agent/conversationState.js';
import { compactTokens, contextPercentage, contextUsage } from '../src/agent/contextUsage.js';
import { ContextFacts, ContextIndicator } from '../src/agent/ContextIndicator.js';
import type { AgentEvent, Payloads } from '../src/agent/types.js';

function event<K extends keyof Payloads>(kind: K, payload: Payloads[K], at: number, iid = 'invocation'): AgentEvent {
  return { id: `event-${at}`, kind, session_id: 'session', invocation_id: iid, occurrence_id: 'node', occurred_at_us: at,
    persist: true, sequence: at, payload: { v: 1, node_id: 'llm_call', ...payload } } as AgentEvent;
}
const response = (at: number, input: number, output: number, iid?: string) => event('agent.response', {
  response_id: `response-${at}`, model: 'test-model', finish_reason: 'stop', blocks: [],
  usage: { input_tokens: input, output_tokens: output, total_tokens: input + output, cache_read_tokens: input, reasoning_tokens: output },
}, at, iid);

test('context uses the latest call across turns, never cumulative billing or cached/reasoning subsets', () => {
  const state = applyEvents(emptyConversation(), [response(1, 100_000, 5000, 'old-turn'), response(2, 10_000, 500, 'new-turn')]);
  const value = contextUsage(state, 200_000, 1_000_000);
  assert.equal(value.usedTokens, 10_500);
  assert.equal(value.windowTokens, 200_000);
  assert.equal(contextPercentage(value), 5.25);
  // Late hydration of old history must not replace the current Session view.
  assert.deepEqual(contextUsage(applyEvents(state, [response(0, 150_000, 1000)]), 200_000), value);
});

test('compaction replaces the prior occupancy and the next measured call replaces that estimate', () => {
  let state = applyEvents(emptyConversation(), [response(1, 180_000, 1000), event('agent.compaction', {
    compaction_id: 'compact', state: 'running', before_tokens: 185_000, after_tokens: null, view_generation: 1,
  }, 2)]);
  assert.equal(contextUsage(state, 200_000).compacting, true);
  state = applyEvents(state, [event('agent.compaction', { compaction_id: 'compact', state: 'completed',
    before_tokens: 185_000, after_tokens: 50_000, view_generation: 2,
  }, 3)]);
  assert.equal(contextUsage(state, 200_000).usedTokens, 50_000);
  state = resetTransient(state);
  assert.equal(contextUsage(state, 200_000).usedTokens, 50_000);
  state = applyEvents(state, [response(4, 52_000, 1000)]);
  assert.equal(contextUsage(state, 200_000).usedTokens, 53_000);
});

test('a partial stream retains the last known occupancy and missing latest usage remains unknown', () => {
  let state = applyEvents(emptyConversation(), [response(1, 10_000, 500), event('agent.response.delta', {
    response_id: 'streaming', stream_id: 'stream', item_key: 'text', content_index: 0, kind: 'text', delta: 'Hello',
  }, 2)]);
  assert.equal(contextUsage(state, 200_000).usedTokens, 10_500);
  const firstStream = applyEvents(emptyConversation(), [event('agent.response.delta', {
    response_id: 'streaming', stream_id: 'stream', item_key: 'text', content_index: 0, kind: 'text', delta: 'Hello',
  }, 2)]);
  assert.equal(contextUsage(firstStream, 200_000).usedTokens, null);
  state = applyEvents(state, [event('agent.response', { response_id: 'streaming', model: 'test-model', finish_reason: 'stop', blocks: [], usage: null }, 3)]);
  assert.equal(contextUsage(state, 200_000).usedTokens, null);
  assert.equal(contextPercentage(contextUsage(state, 200_000)), null);
});

test('capacity respects the model limit, unknown capacity stays neutral and excess usage is not hidden', () => {
  const state = applyEvents(emptyConversation(), [response(1, 120_000, 10_000)]);
  assert.equal(contextUsage(state, 200_000, 128_000).windowTokens, 128_000);
  assert.equal(contextUsage(state, null, 128_000).windowTokens, 128_000);
  assert.equal(contextPercentage(contextUsage(state)), null);
  assert.ok(contextPercentage(contextUsage(state, 100_000))! > 100);
  const html = renderToStaticMarkup(createElement(ContextIndicator, { value: contextUsage(state, 100_000) }));
  assert.match(html, /data-level="full"/);
  assert.match(html, /stroke-dasharray="100 100"/);
  assert.equal(contextUsage(emptyConversation(), 200_000).usedTokens, 0);
});

test('the ring is a keyboard-focusable indicator with occupancy details and no dialog trigger', () => {
  const value = contextUsage(applyEvents(emptyConversation(), [response(1, 100_000, 10_000)]), 480_000);
  const html = renderToStaticMarkup(createElement(ContextIndicator, { value }));
  assert.match(html, /aria-label="Context window: 23% used"/);
  assert.match(html, /role="img" tabindex="0"/);
  assert.doesNotMatch(html, /<button|aria-haspopup|aria-expanded/);
  const details = renderToStaticMarkup(createElement(ContextFacts, { value }));
  assert.match(details, /77% remaining/);
  assert.match(details, /110K/);
  assert.match(details, /480K/);
  const loading = renderToStaticMarkup(createElement(ContextFacts, { value, loading: true }));
  assert.match(loading, /Loading…/);
  assert.doesNotMatch(loading, /77% remaining|110K/);
  assert.equal(compactTokens(null), '—');
});
