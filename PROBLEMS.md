# Remaining work

This file contains only issues and design work that remain unresolved in the
current source. Completed historical bugs and superseded implementation notes
have been removed.

## App and Server lifecycle ownership

`AutoAgentServer` supports standalone use and exposes a FastAPI router, but its
lifespan currently starts and closes the `AutoAgentApp`. Embedded-router use
needs an explicit ownership contract so that the host application can own the
App lifecycle without double-start or premature close.

## User-facing Workflow events

Current Runtime Events serve tracing, replay, recovery, and debugging. A
separate user-facing emission/message contract for Agent UIs has not been
designed yet. It should remain independent from internal tracing detail and
allow each Workflow to define its own payloads.
