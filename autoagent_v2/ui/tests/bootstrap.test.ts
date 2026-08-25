import assert from "node:assert/strict";
import test from "node:test";
import { loadInvocationBootstrap } from "../src/bootstrap.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test("bootstrap establishes the tail cursor before lightweight projection reads", async () => {
  const calls: string[] = [];
  const tail = deferred<any>();
  const client = {
    traceTail: () => {
      calls.push("tail");
      return tail.promise;
    },
    invocation: async () => {
      calls.push("summary");
      return {} as any;
    },
    children: async () => {
      calls.push("children");
      return {} as any;
    },
  };
  const loading = loadInvocationBootstrap(
    "invocation",
    new AbortController().signal,
    () => true,
    client,
  );
  assert.deepEqual(calls, ["tail"]);
  tail.resolve({} as any);
  await loading;
  assert.deepEqual(calls, ["tail", "summary", "children"]);
});

test("stale bootstrap stops before issuing projection reads", async () => {
  const calls: string[] = [];
  const result = await loadInvocationBootstrap(
    "invocation",
    new AbortController().signal,
    () => false,
    {
      traceTail: async () => {
        calls.push("tail");
        return {} as any;
      },
      invocation: async () => {
        calls.push("summary");
        return {} as any;
      },
      children: async () => {
        calls.push("children");
        return {} as any;
      },
    },
  );
  assert.equal(result, null);
  assert.deepEqual(calls, ["tail"]);
});
