import assert from 'node:assert/strict';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import test from 'node:test';
import { Conversation } from '../src/agent/Conversation.js';
import { applyEvents, emptyConversation, entityKey, resetTransient } from '../src/agent/conversationState.js';
import { conversationEntries, finalResponse, hasLogContent } from '../src/agent/conversationTurns.js';
import { presentTool, toolGroupSummary, toolStateLabel } from '../src/agent/toolPresentation.js';
import type { AgentEvent, AgentInvocation, Payloads, ResponseBlock, ToolSnapshot } from '../src/agent/types.js';

let sequence = 0;
function event<K extends keyof Payloads>(kind: K, payload: Payloads[K], invocation = 'run'): AgentEvent {
  sequence++;
  return { id: `event-${sequence}`, kind, session_id: 'session', invocation_id: invocation, occurrence_id: 'node', occurred_at_us: sequence * 1000, persist: true, sequence, payload: { v: 1, node_id: 'llm_call', ...payload } } as AgentEvent;
}
const response = (id: string, blocks: ResponseBlock[], invocation = 'run') => event('agent.response', { response_id: id, model: 'private-model-label', finish_reason: 'stop', blocks, usage: null }, invocation);
const message = (text: string, invocation = 'run') => event('agent.messages', { messages: [{ message_id: `message-${sequence}`, source: 'user', sender_id: null, content: [{ type: 'text', text }], reply_requested: false, reply_to: null }] }, invocation);
const agentMessage = () => event('agent.messages', { messages: [{ message_id: `agent-${sequence}`, source: 'agent', sender_id: 'a7027d69-child', content: [{ type: 'text', text: 'PRIVATE_COLLABORATION_BODY' }], reply_requested: true, reply_to: 'parent-message' }] });
const complete = (responseId: string, invocation = 'run') => event('agent.completed', { final_response_id: responseId, rounds: 2 }, invocation);
const tool: ToolSnapshot = { tool_id: 'first:call', response_id: 'first', call_id: 'call', name: 'read_file', arguments: '{"path":"orders.csv"}', state: 'completed', result: { content: [], structured_content: { data: 'long-result-body' }, is_error: false }, progress: null };
const invocation = (status: string): AgentInvocation => ({ invocation_id: 'run', session_id: 'session', workflow_id: 'agent', workflow_revision_id: 'revision', status, started_at_us: 1000, completed_at_us: status === 'running' ? null : 100000 });

function fixture() {
  return applyEvents(emptyConversation(), [message('Audit orders'), response('first', [
    { type: 'reasoning', text: 'Initial reasoning', summary: [] }, { type: 'text', text: 'Checking the source data.' }, { type: 'tool', tool_id: tool.tool_id },
  ]), event('agent.tools', { tools: [tool] }), response('final', [
    { type: 'reasoning', text: 'Final reasoning stays in the log.', summary: [] }, { type: 'text', text: 'The report is ready.' },
  ])]);
}
const render = (state: ReturnType<typeof fixture>, status = 'completed') => renderToStaticMarkup(createElement(Conversation, { state, invocations: [invocation(status)], active: false, loading: false, onRetry() {} }));

test('one Invocation groups all model rounds and uses only the declared final response', () => {
  const state = applyEvents(fixture(), [complete('final'), response('later', [{ type: 'text', text: 'Later text is not the final answer.' }])]);
  const entries = conversationEntries(state);
  assert.equal(entries.length, 2);
  assert.equal(entries[0].type, 'message');
  assert.equal(entries[1].type, 'turn');
  assert.equal(finalResponse(state, 'run')?.response_id, 'final');
  const html = render(state);
  assert.equal((html.match(/class="agent-turn"/g) ?? []).length, 1);
  assert.match(html, /The report is ready/);
  assert.doesNotMatch(html, /Checking the source data|Final reasoning stays|Later text is not|long-result-body|private-model-label|agent-avatar|>Assistant/);
  assert.match(html, /class="agent-turn-toggle"[^>]*aria-expanded="false"/);
});

test('completion arriving before its response keeps logs visible until the exact response is loaded', () => {
  let state = applyEvents(emptyConversation(), [response('first', [{ type: 'text', text: 'Progress' }]), complete('missing')]);
  assert.equal(finalResponse(state, 'run'), undefined);
  assert.match(render(state), /Progress/);
  state = resetTransient(applyEvents(state, [response('missing', [{ type: 'text', text: 'Final answer' }])]));
  assert.equal(state.completed.run.final_response_id, 'missing');
  assert.equal(finalResponse(state, 'run')?.response_id, 'missing');
  assert.doesNotMatch(render(state), /<p>Progress<\/p>/);
});

test('failed and cancelled Invocations expose process output rather than promoting the last response', () => {
  const state = applyEvents(fixture(), [complete('final')]);
  for (const status of ['failed', 'cancelled']) {
    assert.equal(finalResponse(state, 'run', status), undefined);
    const html = render(state, status);
    assert.match(html, /Checking the source data/);
    assert.match(html, /No final response was produced/);
    assert.match(html, /class="agent-turn-toggle"[^>]*aria-expanded="true"/);
    assert.doesNotMatch(html, /class="agent-final-response"/);
    assert.doesNotMatch(html, /long-result-body/);
  }
});

test('messages received mid-task remain separate visible bubbles in chronological order', () => {
  const state = applyEvents(fixture(), [message('Keep the originals'), response('next', [{ type: 'text', text: 'I will keep them.' }]), complete('next')]);
  const entries = conversationEntries(state), turn = entries.find(item => item.type === 'turn');
  assert.ok(turn?.type === 'turn');
  assert.equal(turn.rows.at(-2)?.type, 'message');
  const html = render(state);
  assert.match(html, /Keep the originals/);
  assert.ok(html.indexOf('Keep the originals') < html.indexOf('I will keep them.'));
  assert.equal((html.match(/class="agent-turn"/g) ?? []).length, 1);
});

test('Agent messages are collapsed process records rather than visible chat bubbles', () => {
  const state = applyEvents(fixture(), [agentMessage(), response('next', [{ type: 'text', text: 'Continuing work' }])]);
  const entries = conversationEntries(state), turn = entries.find(item => item.type === 'turn');
  assert.equal(entries.filter(item => item.type === 'message').length, 1);
  assert.ok(turn?.type === 'turn');
  const row = turn.rows.find(item => item.type === 'message' && item.message.source === 'agent');
  assert.ok(row && hasLogContent(row, state));
  const html = render(state, 'running');
  assert.match(html, /class="agent-disclosure agent-tool-group agent-agent-message"/);
  assert.match(html, /Message from Agent a7027d69/);
  assert.match(html, /Reply requested/);
  assert.doesNotMatch(html, /PRIVATE_COLLABORATION_BODY|agent-message is-agent/);
  const completed = render(applyEvents(state, [complete('next')]));
  assert.doesNotMatch(completed, /Message from Agent|PRIVATE_COLLABORATION_BODY/);
});

test('an Agent message before the first model response still belongs to the process log', () => {
  const state = applyEvents(emptyConversation(), [message('Begin'), agentMessage()]);
  const entries = conversationEntries(state, [invocation('running')]);
  assert.deepEqual(entries.map(item => item.type), ['message', 'turn']);
  assert.match(render(state, 'running'), /Message from Agent a7027d69/);
  assert.doesNotMatch(render(state, 'running'), /PRIVATE_COLLABORATION_BODY/);
});

test('separate Invocations never share final-answer references even with reused response IDs', () => {
  const state = applyEvents(emptyConversation(), [message('First', 'one'), response('shared', [{ type: 'text', text: 'First answer' }], 'one'), complete('shared', 'one'), message('Second', 'two'), response('shared', [{ type: 'text', text: 'Still running' }], 'two')]);
  assert.equal(conversationEntries(state).filter(item => item.type === 'turn').length, 2);
  assert.equal(finalResponse(state, 'one')?.blocks[0].type, 'text');
  assert.equal(finalResponse(state, 'two'), undefined);
});

test('empty or reasoning-only final responses do not create an empty final message', () => {
  for (const blocks of [[], [{ type: 'text' as const, text: ' ' }], [{ type: 'reasoning' as const, text: 'Only thoughts', summary: [] }]]) {
    const state = applyEvents(emptyConversation(), [response('empty', blocks), complete('empty')]);
    assert.equal(finalResponse(state, 'run'), undefined);
    assert.doesNotMatch(render(state), /class="agent-final-response"/);
  }
  const state = applyEvents(emptyConversation(), [response('plain', [{ type: 'text', text: 'Done' }]), complete('plain')]);
  const row = Object.values(state.rows)[0];
  assert.equal(hasLogContent(row, state, 'plain'), false);
  assert.doesNotMatch(render(state), /class="agent-turn-toggle"/);
});

test('metadata can show a running turn before the first model event without inventing a final answer', () => {
  const state = applyEvents(emptyConversation(), [message('Start')]);
  const entries = conversationEntries(state, [invocation('running')]);
  assert.deepEqual(entries.map(item => item.type), ['message', 'turn']);
  assert.match(render(state, 'running'), /Working/);
});

test('tool rows describe operations and groups without loading arguments or result bodies', () => {
  assert.equal(presentTool(tool).title, 'Read orders.csv');
  assert.equal(presentTool({ ...tool, name: 'skill_load', arguments: '{"name":"demo-guide"}' }).title, 'Loaded demo-guide');
  assert.equal(presentTool({ ...tool, arguments: 'malformed' }).title, 'Read file');
  assert.equal(toolGroupSummary([tool, tool, { ...tool, name: 'execute_command' }, { ...tool, name: 'skill_load' }]), 'Read 2 files · 1 command · 1 more');
  assert.equal(toolStateLabel({ ...tool, state: 'running' }, 'cancelled'), 'Stopped');
  assert.equal(toolStateLabel({ ...tool, state: 'waiting' }, 'failed'), 'Interrupted');
  const state = fixture();
  assert.equal(state.tools[entityKey('run', tool.tool_id)].result?.structured_content !== null, true);
  const html = render(state, 'running');
  assert.match(html, /Read 1 file/);
  assert.doesNotMatch(html, /long-result-body|<pre>/);
});

test('a media final answer stays visible without offering an empty text copy action', () => {
  const state = applyEvents(emptyConversation(), [response('image', [{ type: 'media', content: { type: 'image', source: { type: 'data', mime_type: 'image/png', data: 'QQ==' } } }]), complete('image')]);
  assert.equal(finalResponse(state, 'run')?.response_id, 'image');
  const html = render(state);
  assert.match(html, /class="agent-final-response"/);
  assert.match(html, /<img/);
  assert.doesNotMatch(html, /Copy response/);
});
