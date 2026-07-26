# Remaining work

This file contains only issues and design work that remain unresolved in the
current source. Completed historical bugs and superseded implementation notes
have been removed.

## Executor API surface

`WorkflowExecutor` still exposes synchronous `invoke` and `resume` wrappers.
The public `AutoAgentApp` may keep synchronous APIs, but the internal executor
should eventually become async-only if no internal caller requires the
wrappers.

## Recovery ownership

`AutoAgentApp` can still lazily rebuild an unfinished Invocation while handling
an invoke path. The intended lifecycle is not finalized: startup-owned recovery
would rebuild unfinished work during `app.start()`, while invoke/submit would
only create new Invocations and reject a Session that still has unfinished
work.

## App and Server lifecycle ownership

`AutoAgentServer` supports standalone use and exposes a FastAPI router, but its
lifespan currently starts and closes the `AutoAgentApp`. Embedded-router use
needs an explicit ownership contract so that the host application can own the
App lifecycle without double-start or premature close.

## Timing model completion

Runtime Events record `occurred_at_ms`, nanosecond elapsed durations, and
phase-specific timing fields. The proposed same-timestamp ordering field
(`offset_ms`) is not implemented, and scheduler/concurrency wait coverage still
needs a deliberate audit before the timing model is considered complete.

## User-facing Workflow events

Current Runtime Events serve tracing, replay, recovery, and debugging. A
separate user-facing emission/message contract for Agent UIs has not been
designed yet. It should remain independent from internal tracing detail and
allow each Workflow to define its own payloads.
