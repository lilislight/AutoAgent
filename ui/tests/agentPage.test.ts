import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { readPageLocation, writePageLocation } from '../src/navigation.js';
import { ApplicationSwitch } from '../src/components/ApplicationSwitch.js';
import { Sidebar } from '../src/components/Sidebar.js';

const emptyAgent = { view: 'agents' as const, session: null, execution: null, workflow: null, revision: null, selection: null };

test('an explicitly unbound draft survives URLs without falling back to the project workspace', () => {
  const draft = { ...emptyAgent, agentWorkspace: null };
  const url = new URL(writePageLocation(draft, '/proxy/'), 'http://test');
  assert.equal(url.searchParams.get('unbound'), 'true');
  assert.deepEqual(readPageLocation(url.pathname, url.search, '/proxy/'), draft);
});

test('Agent page paths preserve exact Session and child Invocation IDs under mounted UI roots', () => {
  const value = { ...emptyAgent, session: '聊天/session with spaces', execution: 'child/id' };
  const route = writePageLocation(value, '/proxy/tools/');
  assert.equal(route, '/proxy/tools/agent/c/%E8%81%8A%E5%A4%A9%2Fsession%20with%20spaces?invocation=child%2Fid');
  const url = new URL(route, 'http://test');
  assert.deepEqual(readPageLocation(url.pathname, url.search, '/proxy/tools/'), value);
  const localized = new URL(writePageLocation(value, '/项目/ui/'), 'http://test');
  assert.deepEqual(readPageLocation(localized.pathname, localized.search, '/项目/ui/'), value);
  assert.deepEqual(readPageLocation('/agent', '?invocation=unscoped&workflow=custom'), emptyAgent);
});

test('legacy Agent bookmarks migrate once while the developer entry remains reachable in Agent-only projects', () => {
  const state = readPageLocation('/', '?view=agents&session=saved&execution=last&node=trace');
  assert.equal(writePageLocation(state), '/agent/c/saved?invocation=last');
  assert.deepEqual(readPageLocation('/agent/c/%ZZ', '?invocation=ignored'), emptyAgent);
  const workflow = readPageLocation('/', '?view=workflows', '/', 'agents');
  assert.equal(workflow.view, 'workflows');
  assert.equal(writePageLocation(workflow, '/', 'agents'), '/?view=workflows');
  assert.equal(readPageLocation('/', '', '/', 'agents').view, 'agents');
  assert.equal(readPageLocation('/workflows', '', '/', 'agents').view, 'workflows');
});

test('product switching uses ordinary links while developer navigation contains only its two views', () => {
  const html = renderToStaticMarkup(createElement(ApplicationSwitch, { agent: true, agentHref: '/agent/c/saved', workflowHref: '/?view=executions', onChange: () => {} }));
  assert.match(html, /href="\/agent\/c\/saved" aria-current="page"/);
  assert.match(html, /href="\/\?view=executions"/);
  assert.doesNotMatch(html, /Account|Login/);
  const sidebar = renderToStaticMarkup(createElement(Sidebar, { view: 'workflows', onChange: () => {}, children: null }));
  assert.match(sidebar, /View workflows/);
  assert.match(sidebar, /View executions/);
  assert.doesNotMatch(sidebar, /View agents/);
});

test('Workspace selection survives fresh chat URLs, reloads and mounted Agent links', () => {
  const fresh = { ...emptyAgent, agentWorkspace: 'project/研究' };
  const url = new URL(writePageLocation(fresh, '/tools/'), 'http://test');
  assert.deepEqual(readPageLocation(url.pathname, url.search, '/tools/'), fresh);
  const saved = { ...fresh, session: 'saved', execution: 'turn' };
  const chat = new URL(writePageLocation(saved), 'http://test');
  assert.deepEqual(readPageLocation(chat.pathname, chat.search), saved);
  assert.equal(readPageLocation('/agent', '?workspace=').agentWorkspace, undefined);
});
