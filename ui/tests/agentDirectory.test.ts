import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConversationDirectory, WorkspaceGroup, type SessionLoader } from '../src/agent/ConversationDirectory.js';
import type { AgentSession, AgentWorkspaceInfo } from '../src/agent/types.js';

const root: AgentWorkspaceInfo = { id: 'root-one', name: 'AutoAgent', backend: 'local', path: '/projects/one/AutoAgent', is_project: true, available: true };
const other: AgentWorkspaceInfo = { ...root, id: 'root-two', path: '/projects/two/AutoAgent', is_project: false };
const row = (id: string, title: string): AgentSession => ({ session_id: id, invocation_id: id + '-run', title, updated_at_us: 1, invocation_count: 9 });
const load: SessionLoader = async () => { throw new Error('Unexpected request while rendering cached content'); };
const noop = () => {};
function cache() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  for (const [scope, items] of [[root.id, [row('one', 'Current project task')]], [other.id, [row('two', 'Other project task')]], [null, [row('loose', 'A standalone question')]]] as const) {
    client.setQueryData(['agent-sessions', scope], { pages: [{ items, next_cursor: null }], pageParams: [null] });
  }
  return client;
}

test('workspace-first navigation keeps unbound conversations separate and hides collapsed sessions', () => {
  const client = cache();
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ConversationDirectory, {
    items: [root, other], loading: false, error: null, fetching: false, onRetry: noop, workspace: root.id, session: 'one', active: false,
    load, onSelect: noop, onNew: noop, onTitle: noop,
  })));
  assert.match(html, /aria-label="Collapse AutoAgent" aria-expanded="true"/);
  assert.match(html, /aria-label="Expand AutoAgent" aria-expanded="false"/);
  assert.match(html, /Current project task/);
  assert.doesNotMatch(html, /Other project task/);
  assert.match(html, /Conversations without a workspace/);
  assert.match(html, />Recent</);
  assert.match(html, /A standalone question/);
  assert.match(html, /agent-group-session is-selected/);
  assert.match(html, /\/projects\/one\/AutoAgent/);
  assert.match(html, /\/projects\/two\/AutoAgent/);
  assert.doesNotMatch(html, /<select|9 turns|192\.168/);
  client.clear();
});

test('opening another workspace uses its own cached session scope', () => {
  const client = cache();
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(WorkspaceGroup, {
    item: other, selected: true, session: 'two', active: false, search: '', load, onSelect: noop, onNew: noop, onTitle: noop,
  })));
  assert.match(html, /Other project task/);
  assert.doesNotMatch(html, /Current project task|standalone question/);
  assert.match(html, /aria-current="page"/);
  client.clear();
});

test('workspace loading skeletonizes data rows and keeps the real search control', () => {
  const client = new QueryClient();
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ConversationDirectory, {
    loading: true, fetching: true, onRetry: noop, workspace: root.id, session: null, active: false, load, onSelect: noop, onNew: noop, onTitle: noop,
  })));
  assert.match(html, /type="search" aria-label="Filter loaded conversations"/);
  assert.match(html, /aria-label="Loading workspaces"/);
  assert.match(html, /class="agent-group-session is-skeleton"/);
  assert.doesNotMatch(html, /No conversations yet|Updating workspaces|Unavailable workspace/);
  client.clear();
});
