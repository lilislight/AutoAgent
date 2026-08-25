import assert from "node:assert/strict";
import test from "node:test";
import { api, ApiError } from "../src/api.js";

test("encodes trace paths and preserves the bounded tail request", async () => {
  const original = globalThis.fetch;
  let requested = "";
  globalThis.fetch = (async (input: string | URL | Request) => {
    requested = String(input);
    return {
      ok: true,
      json: async () => ({
        items: [],
        next_cursor: null,
        resume_cursor: null,
        resume_sequence: 0,
        has_more: false,
        has_earlier: false,
      }),
    } as Response;
  }) as typeof fetch;
  try {
    await api.traceTail("invocation/one", 25);
    assert.equal(
      requested,
      "/api/v1/invocations/trace?invocation_id=invocation%2Fone&tail_limit=25",
    );
    await api.traceBefore("invocation/one", 100, 20);
    assert.equal(
      requested,
      "/api/v1/invocations/trace?invocation_id=invocation%2Fone&before_sequence=100&limit=20",
    );
  } finally {
    globalThis.fetch = original;
  }
});

test("uses the structured server diagnostic for failed requests", async () => {
  const original = globalThis.fetch;
  globalThis.fetch = (async () =>
    ({
      ok: false,
      status: 404,
      statusText: "Not Found",
      json: async () => ({ detail: { message: "Invocation was not found." } }),
    }) as Response) as typeof fetch;
  try {
    await assert.rejects(
      api.invocation("missing"),
      (error: unknown) =>
        error instanceof ApiError &&
        error.status === 404 &&
        error.message === "Invocation was not found.",
    );
  } finally {
    globalThis.fetch = original;
  }
});

test("refreshes every loaded child page instead of only the first hundred", async () => {
  const original = globalThis.fetch;
  const requested: string[] = [];
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = String(input);
    requested.push(url);
    const cursor = new URL(`http://local${url}`).searchParams.get("cursor");
    const offset = cursor === null ? 0 : 100;
    return {
      ok: true,
      json: async () => ({
        items: Array.from({ length: 100 }, (_, index) => ({
          session_id: `child-${offset + index}`,
        })),
        next_cursor: cursor === null ? "second-page" : "third-page",
      }),
    } as Response;
  }) as typeof fetch;
  try {
    const page = await api.childrenThroughKnown("parent", ["child-149"]);
    assert.equal(requested.length, 2);
    assert.match(requested[1], /cursor=second-page/);
    assert.equal(page.items.length, 200);
    assert.equal(page.next_cursor, "third-page");
  } finally {
    globalThis.fetch = original;
  }
});

test("refreshes through an unloaded backlog to a newly observed live child", async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = new URL(`http://local${String(input)}`);
    const cursor = url.searchParams.get("cursor");
    const offset = cursor === null ? 0 : Number(cursor);
    calls += 1;
    const finalPage = offset === 1_000;
    return {
      ok: true,
      json: async () => ({
        items: Array.from(
          { length: finalPage ? 1 : 100 },
          (_, index) => ({ session_id: `child-${offset + index}` }),
        ),
        next_cursor: finalPage ? null : String(offset + 100),
      }),
    } as Response;
  }) as typeof fetch;
  try {
    const page = await api.childrenThroughKnown("parent", [
      "child-0",
      "child-99",
      "child-1000",
    ]);
    assert.equal(calls, 11);
    assert.equal(page.items.at(-1)?.session_id, "child-1000");
    assert.equal(page.next_cursor, null);
  } finally {
    globalThis.fetch = original;
  }
});

test("creates a resumable EventSource URL with the opaque cursor", () => {
  const original = globalThis.EventSource;
  let requested = "";
  class FakeEventSource {
    constructor(url: string | URL) {
      requested = String(url);
    }
  }
  globalThis.EventSource = FakeEventSource as typeof EventSource;
  try {
    api.traceStream("invocation one", "opaque+/=");
    assert.equal(
      requested,
      "/api/v1/invocations/stream?invocation_id=invocation+one&cursor=opaque%2B%2F%3D",
    );
  } finally {
    globalThis.EventSource = original;
  }
});
