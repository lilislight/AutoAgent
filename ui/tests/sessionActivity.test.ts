import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { loadActivity, observeSessions, readSession, sessionRunning } from '../src/agent/sessionActivity.js';
import { SessionActivityBadge } from '../src/agent/SessionActivityBadge.js';
import type { AgentSession } from '../src/agent/types.js';

const row = (status: string, sequence = 1, invocation = 'turn-one'): AgentSession => ({
  session_id: 'session', title: 'Task', invocation_id: invocation, status, control_state: 'active',
  last_sequence: sequence, updated_at_us: 1, invocation_count: 1,
});

test('old chats establish a quiet baseline and background completion becomes unread', () => {
  assert.equal(observeSessions({}, [row('completed')], null).session.unread, false);
  let state = observeSessions({}, [row('running')], null);
  assert.equal(sessionRunning({ status: state.session.status }), true);
  state = observeSessions(state, [row('completed', 10)], null);
  assert.equal(state.session.unread, true);
  assert.equal(sessionRunning({ status: state.session.status }), false);
});

test('completion in the open chat stays read, opening a background result acknowledges both lists', () => {
  let state = observeSessions({}, [row('running')], 'session');
  state = observeSessions(state, [row('completed', 10)], 'session');
  assert.equal(state.session.unread, false);
  state = observeSessions(state, [row('completed', 20, 'turn-two')], null);
  assert.equal(state.session.unread, true);
  state = readSession(state, 'session');
  // Re-fetching the same result in Recent or Workspace cannot restore its dot.
  assert.equal(observeSessions(state, [row('completed', 20, 'turn-two')], null).session.unread, false);
});

test('fast tasks missed between polls still notify and stale directory responses cannot revert state', () => {
  let state = observeSessions({}, [row('completed', 10)], null);
  state = observeSessions(state, [row('completed', 30, 'turn-two')], null);
  assert.equal(state.session.unread, true);
  const current = state;
  assert.equal(observeSessions(state, [row('running', 20, 'turn-two')], null), current);
  assert.equal(observeSessions(state, [row('running', 30, 'turn-two')], null), current);
  state = observeSessions(state, [row('running', 40, 'turn-three')], null);
  assert.equal(state.session.unread, false);
});

test('waits remain active, paused tasks do not spin, and failed or cancelled tasks can notify', () => {
  assert.equal(sessionRunning(row('waiting')), true);
  assert.equal(sessionRunning({ ...row('running'), control_state: 'paused' }), false);
  for (const status of ['failed', 'cancelled']) {
    const state = observeSessions(observeSessions({}, [row('running')], null), [row(status, 10)], null);
    assert.equal(state.session.unread, true);
    assert.equal(sessionRunning(row(status)), false);
  }
});

test('browser-local read state survives refresh and unavailable storage never blocks the directory', () => {
  const original = Object.getOwnPropertyDescriptor(globalThis, 'localStorage');
  let saved = JSON.stringify({ v: 1, sessions: observeSessions(observeSessions({}, [row('running')], null), [row('completed', 10)], null) });
  Object.defineProperty(globalThis, 'localStorage', { configurable: true, value: { getItem: () => saved } });
  try {
    assert.equal(loadActivity('scope').session.unread, true);
    saved = JSON.stringify({ v: 1, sessions: readSession(loadActivity('scope'), 'session') });
    assert.equal(loadActivity('scope').session.unread, false);
    saved = '{invalid';
    assert.deepEqual(loadActivity('scope'), {});
  } finally {
    if (original) Object.defineProperty(globalThis, 'localStorage', original);
    else Reflect.deleteProperty(globalThis, 'localStorage');
  }
});

test('a fixed trailing slot switches between the shared spinner, unread dot and quiet read state', () => {
  const running = renderToStaticMarkup(createElement(SessionActivityBadge, { running: true, unread: false }));
  assert.match(running, /aria-label="Task running"/);
  assert.match(running, /loading-spinner/);
  const done = renderToStaticMarkup(createElement(SessionActivityBadge, { running: false, unread: true }));
  assert.match(done, /aria-label="New task result"/);
  assert.match(done, /agent-session-unread/);
  assert.doesNotMatch(done, /loading-spinner/);
  const read = renderToStaticMarkup(createElement(SessionActivityBadge, { running: false, unread: false }));
  assert.doesNotMatch(read, /agent-session-unread|loading-spinner/);
});
