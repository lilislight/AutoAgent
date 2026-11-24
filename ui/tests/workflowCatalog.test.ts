import test from 'node:test';
import assert from 'node:assert/strict';
import { QueryClient, QueryObserver } from '@tanstack/react-query';
import { refreshWorkflowCatalog } from '../src/components/workflowCatalog.js';

test('a registration notice arriving during the first read cannot leave an older catalog visible', async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  let revision = 'old', calls = 0, release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const key = ['workflow-catalog', 'search'];
  const observer = new QueryObserver(client, { queryKey: key, queryFn: async () => {
    const captured = revision;
    if (++calls === 1) await blocked;
    return captured;
  } });
  const unsubscribe = observer.subscribe(() => {});
  try {
    revision = 'new';
    const refresh = refreshWorkflowCatalog(client, () => true);
    release();
    await refresh;
    assert.equal(client.getQueryData(key), 'new');
    assert.equal(calls, 2);
  } finally { unsubscribe(); client.clear(); }
});

test('leaving the page prevents a second read after an outstanding catalog request', async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  let calls = 0, release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const observer = new QueryObserver(client, { queryKey: ['workflow-catalog'], queryFn: async () => {
    calls++;
    await blocked;
    return [];
  } });
  const unsubscribe = observer.subscribe(() => {});
  try {
    const refresh = refreshWorkflowCatalog(client, () => false);
    release();
    await refresh;
    assert.equal(calls, 1);
  } finally { unsubscribe(); client.clear(); }
});
