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

test("requests Runtime State through the selected Trace sequence", async () => {
  const original = globalThis.fetch;
  let requested = "";
  globalThis.fetch = (async (input: string | URL | Request) => {
    requested = String(input);
    return {
      ok: true,
      json: async () => ({
        invocation_id: "invocation/one",
        session_id: "session",
        through_sequence: 99,
        state: {},
      }),
    } as Response;
  }) as typeof fetch;
  try {
    await api.state("invocation/one", undefined, 41);
    assert.equal(
      requested,
      "/api/v1/invocations/state?invocation_id=invocation%2Fone&through_trace_sequence=41",
    );
  } finally {
    globalThis.fetch = original;
  }
});

test("keeps User Event tail and history URLs separate from Trace", async () => {
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
    await api.userEventTail("invocation/one", 25);
    assert.equal(
      requested,
      "/api/v1/invocations/user-events?invocation_id=invocation%2Fone&tail_limit=25",
    );
    await api.userEventsBefore("invocation/one", 17, 10);
    assert.equal(
      requested,
      "/api/v1/invocations/user-events?invocation_id=invocation%2Fone&before_sequence=17&limit=10",
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

test("refreshes every loaded Child page instead of only the first page", async () => {
  const original = globalThis.fetch;
  const requested: string[] = [];
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = String(input);
    requested.push(url);
    const cursor = new URL(`http://local${url}`).searchParams.get("cursor");
    const offset = cursor === null ? 0 : cursor === "second-page" ? 200 : 400;
    return {
      ok: true,
      json: async () => ({
        items: Array.from({ length: 200 }, (_, index) => ({
          session_id: `child-${offset + index}`,
        })),
        next_cursor:
          cursor === null
            ? "second-page"
            : cursor === "second-page"
              ? "third-page"
              : "fourth-page",
      }),
    } as Response;
  }) as typeof fetch;
  try {
    const page = await api.childrenThroughKnown("parent", ["child-299"]);
    assert.equal(requested.length, 3);
    assert.ok(requested.every((url) => url.includes("limit=200")));
    assert.match(requested[1], /cursor=second-page/);
    assert.match(requested[2], /cursor=third-page/);
    assert.equal(page.items.length, 600);
    assert.equal(page.next_cursor, "fourth-page");
  } finally {
    globalThis.fetch = original;
  }
});

test("uses the server maximum page size for the Child bootstrap page", async () => {
  const original = globalThis.fetch;
  let requested = "";
  globalThis.fetch = (async (input: string | URL | Request) => {
    requested = String(input);
    return {
      ok: true,
      json: async () => ({ items: [], next_cursor: null, has_more: false }),
    } as Response;
  }) as typeof fetch;
  try {
    await api.children("parent");
    assert.equal(
      requested,
      "/api/v1/invocations/children?invocation_id=parent&limit=200",
    );
  } finally {
    globalThis.fetch = original;
  }
});

test("discovers a child appended beyond an exact loaded page boundary", async () => {
  const original = globalThis.fetch;
  const requested: string[] = [];
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = String(input);
    requested.push(url);
    const cursor = new URL(`http://local${url}`).searchParams.get("cursor");
    return {
      ok: true,
      json: async () =>
        cursor === null
          ? {
              items: Array.from({ length: 200 }, (_, index) => ({
                session_id: `child-${index}`,
              })),
              next_cursor: "page-2",
            }
          : {
              items: [{ session_id: "child-200" }],
              next_cursor: null,
            },
    } as Response;
  }) as typeof fetch;
  try {
    const known = Array.from({ length: 200 }, (_, index) => `child-${index}`);
    const page = await api.childrenThroughKnown("parent", known);
    assert.equal(requested.length, 2);
    assert.equal(page.items.at(-1)?.session_id, "child-200");
    assert.equal(page.next_cursor, null);
  } finally {
    globalThis.fetch = original;
  }
});

test("rejects a repeated child cursor instead of requesting forever", async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = (async () => {
    calls += 1;
    return {
      ok: true,
      json: async () => ({ items: [], next_cursor: "cycle" }),
    } as Response;
  }) as typeof fetch;
  try {
    await assert.rejects(
      api.childrenThroughKnown("parent", ["missing"]),
      /repeated cursor/,
    );
    assert.equal(calls, 2);
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
          { length: finalPage ? 1 : 200 },
          (_, index) => ({ session_id: `child-${offset + index}` }),
        ),
        next_cursor: finalPage ? null : String(offset + 200),
      }),
    } as Response;
  }) as typeof fetch;
  try {
    const page = await api.childrenThroughKnown("parent", [
      "child-0",
      "child-99",
      "child-1000",
    ]);
    assert.equal(calls, 6);
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

test("creates an independent resumable User Event EventSource URL", () => {
  const original = globalThis.EventSource;
  let requested = "";
  class FakeEventSource {
    constructor(url: string | URL) {
      requested = String(url);
    }
  }
  globalThis.EventSource = FakeEventSource as typeof EventSource;
  try {
    api.userEventStream("invocation one", "user+/=");
    assert.equal(
      requested,
      "/api/v1/invocations/user-events/stream?invocation_id=invocation+one&cursor=user%2B%2F%3D",
    );
  } finally {
    globalThis.EventSource = original;
  }
});
