import { test } from 'node:test';
import assert from 'node:assert/strict';

test('HTTP mutations accept empty deletion receipts while preserving JSON and conflict errors', async () => {
  const previousDocument = Object.getOwnPropertyDescriptor(globalThis, 'document');
  const previousFetch = globalThis.fetch;
  Object.defineProperty(globalThis, 'document', { configurable: true, value: { getElementById: () => null } });
  try {
    const { request, ApiError } = await import('../src/http.js');
    let sent: { url: string; method: string; accept: string | null } | undefined;
    globalThis.fetch = async (url, init) => {
      sent = { url: String(url), method: init?.method ?? 'GET', accept: new Headers(init?.headers).get('Accept') };
      return new Response(null, { status: 204 });
    };
    assert.equal(await request<void>('/checkpoints/checkpoint', new AbortController().signal, { method: 'DELETE' }), undefined);
    assert.deepEqual(sent, { url: '/api/v2/checkpoints/checkpoint', method: 'DELETE', accept: 'application/json' });

    globalThis.fetch = async () => Response.json({ pinned: true });
    assert.deepEqual(await request('/checkpoints/checkpoint', new AbortController().signal), { pinned: true });

    globalThis.fetch = async () => Response.json({ detail: { message: 'Checkpoint is required for recovery.' } }, { status: 409 });
    await assert.rejects(request('/checkpoints/checkpoint', new AbortController().signal), error => error instanceof ApiError && error.status === 409 && error.message === 'Checkpoint is required for recovery.');
  } finally {
    globalThis.fetch = previousFetch;
    if (previousDocument) Object.defineProperty(globalThis, 'document', previousDocument);
    else Reflect.deleteProperty(globalThis, 'document');
  }
});

test('Workflow visibility scopes its own requests while Agent and SDK reads retain generic access', async () => {
  const previousFetch = globalThis.fetch;
  try {
    const { request } = await import('../src/http.js');
    const { workflowRequest, isHiddenWorkflowError } = await import('../src/workflowHttp.js');
    const seen: (string | null)[] = [];
    globalThis.fetch = async (_url, init) => {
      seen.push(new Headers(init?.headers).get('X-AutoAgent-UI'));
      return Response.json({ items: [] });
    };
    const signal = new AbortController().signal;
    await workflowRequest('/workflow-catalog', signal);
    await request('/agent/catalog', signal);
    await request('/workflow-catalog', signal);
    assert.deepEqual(seen, ['workflow', null, null]);
    globalThis.fetch = async () => Response.json({
      detail: { code: 'agent_app_hidden', message: 'Agent App is hidden in this view.' },
    }, { status: 404 });
    await assert.rejects(workflowRequest('/execution-trace', signal), isHiddenWorkflowError);
    assert.equal(isHiddenWorkflowError(new Error('Other failure')), false);
  } finally {
    globalThis.fetch = previousFetch;
  }
});
