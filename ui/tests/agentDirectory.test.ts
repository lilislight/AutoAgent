import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConversationDirectory, WorkspaceGroup, type SessionLoader } from '../src/agent/ConversationDirectory.js';
import { WorkspacePicker } from '../src/agent/WorkspacePicker.js';
import type { AgentSession, AgentWorkspaceInfo } from '../src/agent/types.js';

const root: AgentWorkspaceInfo = { id: 'root-one', name: 'AutoAgent', backend: 'local', path: '/projects/one/AutoAgent', is_project: true, available: true };
const other: AgentWorkspaceInfo = { ...root, id: 'root-two', path: '/projects/two/AutoAgent', is_project: false };
const row = (id: string, title: string, workspace_id: string | null = null): AgentSession => ({ session_id: id, invocation_id: id + '-run', title, updated_at_us: 1, invocation_count: 9, workspace_id });
const load: SessionLoader = async () => { throw new Error('Unexpected request while rendering cached content'); };
const noop = () => {};
function cache() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  for (const [scope, items] of [[root.id, [row('one', 'Current project task', root.id)]], [other.id, [row('two', 'Other project task', other.id)]], [null, [row('one', 'Current project task', root.id), row('two', 'Other project task', other.id), row('loose', 'A standalone question')]]] as const) {
    client.setQueryData(['agent-sessions', scope], { pages: [{ items, next_cursor: null }], pageParams: [null] });
  }
  return client;
}

test('Recent includes all workspaces with icon markers while scoped groups keep independent selection', () => {
  const client = cache();
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ConversationDirectory, {
    items: [root, other], loading: false, error: null, onRetry: noop, workspace: root.id, session: 'one', active: false,
    load, onSelect: noop, onNew: noop, onTitle: noop, onAddWorkspace: noop,
  })));
  assert.match(html, /aria-label="Collapse AutoAgent" aria-expanded="true"/);
  assert.match(html, /aria-label="Expand AutoAgent" aria-expanded="false"/);
  assert.match(html, /Current project task/);
  assert.equal(html.match(/Current project task<\/span>/g)?.length, 2);
  assert.match(html, /Other project task/);
  assert.match(html, /Recent conversations/);
  assert.equal(html.match(/aria-current="page"/g)?.length, 2);
  assert.match(html, /agent-recent-workspace/);
  assert.match(html, /role="img" aria-label="Workspace: AutoAgent"/);
  assert.match(html, /class="agent-recent-workspace"[^>]*><svg[^]*?<\/svg><\/span>/);
  assert.match(html, /New conversation without a workspace/);
  assert.match(html, /Collapse Workspaces/);
  assert.match(html, />Recent</);
  assert.match(html, /A standalone question/);
  assert.match(html, /agent-group-session is-selected/);
  assert.match(html, /\/projects\/one\/AutoAgent/);
  assert.match(html, /\/projects\/two\/AutoAgent/);
  assert.doesNotMatch(html, /<select|type="search"|Updating |9 turns|192\.168/);
  client.clear();
});

test('long conversation groups show five entries and a Show more action for cached or remote results', () => {
  for (const scope of [root.id, null]) {
    const client = new QueryClient();
    client.setQueryData(['agent-sessions', scope], { pages: [{ items: Array.from({ length: 12 }, (_, i) => row(`chat-${i}`, `Chat ${i}`)), next_cursor: null }], pageParams: [null] });
    const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(WorkspaceGroup, {
      item: scope ? root : null, selected: true, session: null, active: false, load, onSelect: noop, onNew: noop, onTitle: noop,
    })));
    assert.equal(html.match(/class="agent-session-title"/g)?.length, 5);
    assert.match(html, /Show more/);
    assert.doesNotMatch(html, />Chat 5</);
    client.clear();
  }
  const client = new QueryClient();
  client.setQueryData(['agent-sessions', null], { pages: [{ items: [row('last', 'Only loaded chat')], next_cursor: 'another-page' }], pageParams: [null] });
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(WorkspaceGroup, {
    item: null, selected: false, session: null, active: false, load, onSelect: noop, onNew: noop, onTitle: noop,
  })));
  assert.match(html, /Only loaded chat/);
  assert.match(html, /Show more recent conversations/);
  client.clear();
});

test('opening another workspace uses its own cached session scope', () => {
  const client = cache();
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(WorkspaceGroup, {
    item: other, selected: true, session: 'two', active: false, load, onSelect: noop, onNew: noop, onTitle: noop,
  })));
  assert.match(html, /Other project task/);
  assert.doesNotMatch(html, /Current project task|standalone question/);
  assert.match(html, /aria-current="page"/);
  client.clear();
});

test('workspace loading keeps section actions visible and skeletonizes only data rows', () => {
  const client = new QueryClient();
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ConversationDirectory, {
    loading: true, onRetry: noop, workspace: root.id, session: null, active: false, load, onSelect: noop, onNew: noop, onTitle: noop, onAddWorkspace: noop,
  })));
  assert.match(html, /aria-label="Add workspace"/);
  assert.match(html, /New conversation without a workspace/);
  assert.match(html, /aria-label="Loading workspaces"/);
  assert.match(html, /class="agent-group-session is-skeleton"/);
  assert.doesNotMatch(html, /type="search"|No conversations yet|Updating workspaces|Unavailable workspace/);
  client.clear();
});

test('only unsent drafts expose a Workspace chooser; started chats retain a static binding', () => {
  const props = { value: null, items: [root], disabled: false, loading: false, onRetry: noop, onChange: noop };
  const draft = renderToStaticMarkup(createElement(WorkspacePicker, { ...props, locked: false }));
  assert.match(draft, /aria-label="Choose workspace: No workspace"/);
  const started = renderToStaticMarkup(createElement(WorkspacePicker, { ...props, value: root.id, locked: true }));
  assert.match(started, /agent-bound-workspace/);
  assert.match(started, />AutoAgent</);
  assert.doesNotMatch(started, /<button|Choose workspace/);
});
