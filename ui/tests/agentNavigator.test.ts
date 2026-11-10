import assert from 'node:assert/strict';
import { createElement, createRef } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import test from 'node:test';
import { ConversationNavigator } from '../src/agent/ConversationNavigator.js';
import { conversationLandmarks, landmarkAt, messageAnchor } from '../src/agent/conversationLandmarks.js';
import { applyEvents, emptyConversation, entityKey } from '../src/agent/conversationState.js';
import type { AgentEvent } from '../src/agent/types.js';

test('landmarks show user questions and confirmed answers without exposing reasoning or Agent messages', () => {
  const state = applyEvents(emptyConversation(), [{ id: 'event', kind: 'agent.messages', session_id: 'session', invocation_id: 'run', occurred_at_us: 1, persist: true, sequence: 1, occurrence_id: null,
    payload: { v: 1, node_id: 'prepare_context', messages: [
      { message_id: 'question', source: 'user', sender_id: null, content: [{ type: 'text', text: 'What changed?' }], reply_requested: false, reply_to: null },
      { message_id: 'internal', source: 'agent', sender_id: 'child', content: [{ type: 'text', text: 'PRIVATE_MESSAGE' }], reply_requested: false, reply_to: null },
    ] } } as AgentEvent]);
  state.responses[entityKey('run', 'final')] = { response_id: 'final', model: 'offline', finish_reason: 'stop', usage: null, blocks: [
    { type: 'reasoning', text: 'PRIVATE_REASONING', summary: [] }, { type: 'text', text: 'The bug is fixed.' },
  ] };
  state.completed.run = { final_response_id: 'final', rounds: 2, at: 2 };
  const items = conversationLandmarks(state, new Map());
  assert.equal(items.length, 1);
  assert.equal(items[0].question, 'What changed?');
  assert.equal(items[0].answer, 'The bug is fixed.');
  assert.equal(items[0].anchor, messageAnchor(entityKey('run', 'question')));
  assert.doesNotMatch(JSON.stringify(items), /PRIVATE_/);
  const failed = conversationLandmarks(state, new Map([['run', { invocation_id: 'run', session_id: 'session', workflow_id: 'agent', workflow_revision_id: 'rev', status: 'failed' }]]));
  assert.match(failed[0].answer, /Task failed/);
});

test('selection remains on the preceding question through a long answer and handles boundaries', () => {
  assert.equal(landmarkAt([100, 500, 3000], 0), 0);
  assert.equal(landmarkAt([100, 500, 3000], 499), 0);
  assert.equal(landmarkAt([100, 500, 3000], 500), 1);
  assert.equal(landmarkAt([100, 500, 3000], 2800), 1);
  assert.equal(landmarkAt([100, 500, 3000], 9000), 2);
});

test('turn navigation exposes accessible anchors and stays hidden for a single question', () => {
  const props = { area: createRef<HTMLDivElement>(), content: createRef<HTMLDivElement>(), active: true, onJump() {} };
  const item = { id: 'one', anchor: messageAnchor('one'), question: 'First question', answer: 'First answer' };
  assert.equal(renderToStaticMarkup(createElement(ConversationNavigator, { ...props, items: [item] })), '');
  const html = renderToStaticMarkup(createElement(ConversationNavigator, { ...props, items: [item, { ...item, id: 'two', anchor: messageAnchor('two'), question: 'Second question' }] }));
  assert.match(html, /Conversation turns/);
  assert.match(html, /Go to question 2: Second question/);
  assert.match(html, /aria-controls="agent-message-two"/);
  assert.doesNotMatch(html, /First answer/);
});
