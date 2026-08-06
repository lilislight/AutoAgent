---
code_paths:
  - autoagent/core/server/
  - ui/
tags:
  - tracing
  - server
  - ui
  - runtime-events
  - sse
---

# Tracing Server and UI

## Responsibility

Expose execution control and paged trace queries over HTTP, project live and durable Runtime evidence into inspection views, stream change notifications, and provide the embedded tracing UI.

## Current Design

AutoAgentServer wraps one AutoAgentApp as a standalone FastAPI application or embeddable Router. Execution endpoints submit, resume, and cancel registered Workflow revisions. TraceService reads RuntimeStore and optional backend loaders to list Workflow revisions, Sessions, Invocations, Runtime Events, User Events, state, and neighboring records with stable cursor pagination. TraceProjectionReducer derives graph and execution views from ordered Runtime Events, with bounded in-process caching. SSE channels notify clients about runtime health, directory changes, Invocation changes, and User Events. The React UI loads pages and detailed values on demand rather than sending complete Invocation histories at startup.

Operator Call spans are nested under their NodeExecution in the Timeline and use the Call's recorded start and end. The projection keeps at most 50 Call rows per NodeExecution and preserves exact aggregate counts plus an omitted count; the Runtime Event journal remains complete and paged. Selecting a Call or Event loads its canonical input/output from Event detail instead of copying values into the Timeline projection.

## Boundaries and Rules

- Server executes only revisions registered in its App and can disable all execution endpoints in read-only mode.
- TraceService is read-only with respect to Runtime execution; control actions go through App execution APIs.
- Historical replay is Event-derived and must not leak later live values into earlier cursors.
- Directory and Event endpoints are paginated; Event detail and Runtime state are loaded only when requested.
- Operator Call collection queries may be restricted to one NodeExecution and remain cursor-paged over the complete Event journal.
- The UI consumes the Server Runtime Event contract directly: event_name and subject_type/subject_id are canonical, without client-side compatibility aliases.
- Authentication is optional bearer/cookie protection owned by the Server, not Workflow source.
- Server shutdown first wakes long-lived streams, then gives active Invocation tasks a bounded grace period before cancellation.

## Relationships

Runtime Execution supplies live state and notifications. Runtime Persistence supplies historical and evicted data. Workflow Compilation supplies revision graph snapshots. Project and CLI Hosting starts the Server and provides the remote CLI client.
