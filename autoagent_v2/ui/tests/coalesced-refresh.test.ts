import assert from "node:assert/strict";
import test from "node:test";
import { createCoalescedRefresh } from "../src/coalescedRefresh.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function rethrow(reason: unknown): never {
  throw reason;
}

test("coalesced refresh bounds a burst to one projection read", async () => {
  let calls = 0;
  const completed = deferred();
  const refresh = createCoalescedRefresh(async () => {
    calls += 1;
    completed.resolve();
  }, rethrow, 0);
  for (let index = 0; index < 100; index += 1) refresh.request();
  await completed.promise;
  assert.equal(calls, 1);
  refresh.dispose();
});

test("coalesced refresh reruns once when events arrive during a read", async () => {
  let calls = 0;
  const first = deferred();
  const second = deferred();
  const release = deferred();
  const refresh = createCoalescedRefresh(async () => {
    calls += 1;
    if (calls === 1) {
      first.resolve();
      await release.promise;
    } else {
      second.resolve();
    }
  }, rethrow, 0);
  refresh.request();
  await first.promise;
  for (let index = 0; index < 100; index += 1) refresh.request();
  release.resolve();
  await second.promise;
  assert.equal(calls, 2);
  refresh.dispose();
});

test("disposed refresh never starts delayed work", async () => {
  let calls = 0;
  const refresh = createCoalescedRefresh(async () => {
    calls += 1;
  }, rethrow, 10);
  refresh.request();
  refresh.dispose();
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(calls, 0);
});

test("flush completes pending final projection work immediately", async () => {
  let calls = 0;
  const refresh = createCoalescedRefresh(async () => {
    calls += 1;
  }, rethrow, 10_000);
  refresh.request();
  await refresh.flush();
  assert.equal(calls, 1);
  refresh.dispose();
});
