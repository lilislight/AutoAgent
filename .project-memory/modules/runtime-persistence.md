---
code_paths:
  - autoagent/core/runtime/persistence.py
  - autoagent/core/runtime/serialization.py
  - autoagent/core/runtime/artifact.py
  - autoagent/core/runtime/retention.py
  - autoagent/core/runtime/backends/
tags:
  - runtime-persistence
  - database
  - backpressure
  - artifacts
  - recovery
---

# Runtime Persistence

## Responsibility

Provide optional bounded durability and historical loading for RuntimeStore without making database writes the owner or synchronous critical path of normal Workflow execution.

## Current Design

PersistenceCoordinator accepts immutable-by-ownership envelopes for Workflow revisions, admission snapshots, Invocation state, Runtime Events, and User Event batches. It tracks byte-based queue pressure, contiguous durable cursors, per-Invocation failures, and backend health. DatabaseBackend runs serialization and SQL work on a separate persistence RuntimeEventLoop, fairly drains Session queues, coalesces bounded batches, and supports SQLite and PostgreSQL through SQLAlchemy. User Pydantic values persist as type-neutral JSON rather than Python type identifiers. Large serialized values may be deduplicated into Artifact rows and referenced by ArtifactRef. Compact recovery state is written periodically according to Event distance and at explicit recovery boundaries.

## Boundaries and Rules

- RuntimeStore remains authoritative for current state; the backend is an optional downstream sink/source.
- Normal execution and terminal return do not wait for every Event to become durable. Explicit flush and shutdown durability barriers may wait.
- Queue watermarks are byte based: high pressure pauses new admission, low pressure reopens it, and the hard limit degrades persistence rather than blocking already-running execution.
- Runtime and User Event journals have independent contiguous durability and failure accounting.
- DatabaseBackend supports the current V1 schema directly; obsolete per-Node and per-Operator tables are not part of the model.
- Actual Operator Calls are Runtime Event rows, indexed by NodeExecution for paged inspection. Their input and output are not repeated in selector, aggregator-input, or Output Binding phase records.
- Historical and tracing reads expose type-neutral JSON without importing project model classes.
- Recovery combines a genesis or compact recovery snapshot with later ordered Runtime Events, then uses the exact registered Workflow revision's contracts to restore executable typed outputs.

## Relationships

Runtime Execution publishes boundaries and reads recovered state through RuntimeStore. Tracing Server and UI query live memory first and use backend loaders for evicted or historical data. Project and CLI Hosting builds policies and DatabaseBackend from deployment environment values.
