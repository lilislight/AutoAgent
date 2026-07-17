# AutoAgent TODO

This file tracks the implementation order for turning the current execution
core into a stable general-purpose Agent workflow framework. Complete each
stage before expanding the next one.

## 1. Freeze Core Execution Semantics

- [x] Remove the old workflow failure, routing, and join Policies. V1 derives
  routing and complete fan-in directly from edge conditions and resolutions.
- [x] Implement the current Policy candidates across compiler and executor:
  capability selection, retry/backoff, timeout, invocation resource limits,
  replication, node concurrency, and edge map.
- [x] Defer TimerPolicy until durable scheduling and resume semantics are
  designed; V1 does not expose timer-driven node execution.
- [x] Freeze the V1 public Policy set: capability selection (`default`,
  `priority`, `first_available`), retry/backoff, timeout, invocation resource
  limits, replication, node concurrency, and edge map.
- [x] Freeze retry/fallback boundaries: Operator handler exceptions, timeouts,
  and invalid Operator outputs participate; mapping, item selection, condition,
  aggregation, and binding failures fail directly.
- [x] Document execution invariants for scheduling, loops, failures, mapping,
  binding, retry, fallback, timeout, waiting, and cancellation in
  `docs/05-Execution/2 - V1 Execution Invariants.md`.
- [x] Add compiler validation and end-to-end coverage for graph execution,
  loops, branching/fan-in, map, replication, retry/fallback, mapping/binding,
  timeout, waiting/resume, concurrency, and async cancellation.
- [x] Close retained Policy edge-case coverage: backoff calculation modes and
  jitter, async timeout, synchronous timeout/cancellation late results,
  and accumulated runtime limits.
- [x] Add compiler-assisted Workflow authoring previews through
  `workflow.to_mermaid()`, `workflow.preview()`, and `app.preview()`. Generated
  directed graphs highlight valid, warning, and invalid edges using Compiler
  diagnostics without executing the Workflow.

## 2. Define Operator and Capability Contracts

- [x] Define `SchemaContract` as the canonical representation derived from
  Python Operator callables. Workflow, YAML, UI, Node, Capability, and Operator
  registration do not accept schema overrides; Workflow IR and runtime consume
  only the generated contract and its JSON Schema descriptor.
- [x] Validate every Operator against its Capability contract at registration.
- [x] Compile the effective node input and output contracts into Workflow IR.
- [x] Validate mapped inputs before Operator execution and outputs before they
  are published to downstream nodes.
- [x] Require InputMapping to return a named-argument Mapping and MapPolicy
  item_selector to return an iterable of named-argument Mappings.

## 3. Complete Async Execution and Persistence

- [x] Add native `ainvoke` and cancellation propagation.
- [x] Keep `invoke`/`resume` as synchronous adapters over the single async-first
  execution path, with synchronous Operators isolated in a shared thread pool.
- [x] Rebuild Session, Invocation, NodeExecution, OperatorCall, scheduler cursor,
  contexts, outputs, and waits from database-shaped in-memory records.
- [x] Define and implement the minimal V1 wait entrypoint. Compiler accepts
  one framework-owned `SystemCommand` wait contract, execution produces a
  waiting NodeExecution without passing through Operator resolution, and an
  end-to-end test covers `invoke -> waiting -> resume -> completed` through
  `AutoAgentApp` rather than constructing waiting Runtime objects directly.
- [x] Freeze the V1 durability guarantee before implementing a database:
  waiting Invocations survive process restarts and a consumed `wait_key` cannot
  be applied twice. Restored in-flight work is replayed automatically only when
  its Operator declares recovery support; otherwise the Invocation becomes
  terminal `interrupted` and the Session may accept a new Invocation. V1 does
  not expose a manual crash-recovery choice.
- [x] Require a stable user-authored Workflow id, compile a canonical
  `definition_hash`, and retain a `WorkflowVersionSnapshot` that excludes
  Operator implementation code from structural identity.
- [ ] Add explicit versions for executable Workflow hooks such as
  input_mapping, output_binding, conditions, map selectors, and aggregators.
  V1 hashes their module and qualified name, so changing a function body under
  the same identity requires the developer to increment `workflow.version`.
- [x] Define versioned `OperatorManifest` recovery declarations. Direct Python
  callables use the same default Operator behavior as registered Operators.
- [x] Add a safe JSON runtime serializer with explicit Pydantic/custom codecs
  and `ArtifactRef`; pickle and implicit imports are not recovery formats.
- [ ] Add an App-level startup API for registering trusted Runtime codecs and
  Pydantic model types before durable records are loaded. V1 callers can pass a
  preconfigured `JsonRuntimeSerializer` to the Store, but registration is not
  yet coordinated by `AutoAgentApp`.
- [ ] Replace execution-path snapshot saves with explicit atomic Store
  operations for Invocation admission, NodeExecution result checkpoints,
  entering waiting state, claiming a wait, applying resume output, and final
  Invocation completion. Each checkpoint must persist related OperatorCalls,
  outputs, scheduler state, InvocationContext, and changed SessionContext
  together.
- [x] Freeze the SQLite record schema, foreign keys, uniqueness constraints,
  lookup indexes, serialization format, and schema migration/version strategy.
- [x] Add async SQLAlchemy `SQLiteRuntimeStore` using the same observable behavior as
  `InMemoryRuntimeStore`, then make Store selection configurable by the App.
- [x] Add close/reopen integration tests for waiting-state restoration,
  duplicate resume rejection, completed output restoration, Workflow/Operator
  compatibility, automatic whole-node replay, and interrupted work.
- [x] Cover live Session admission, concurrent wait claiming, namespace
  isolation, and repeated synchronous SQLite calls. V1 still assumes one
  application owner for a database/namespace and does not implement leases or
  heartbeats.
- [ ] Remove the file-snapshot test helper once equivalent SQLite restart tests
  cover the persistence contract directly.
- [x] Define whole-NodeExecution replay as a new historical NodeExecution linked
  by `recovery_of_execution_id`; preserve logical input, activations, and
  idempotency key, and mark the original interrupted.
- [x] Add map and replication crash-replay integration coverage. Both recover
  by replacing the interrupted logical NodeExecution and replaying the whole
  node; partial OperatorCall replay remains outside V1.
- [ ] Persist each OperatorCall before invoking its handler, then update the row
  on completion. Current recovery stores complete calls and conservatively
  validates every selectable Operator when a process dies mid-call.
- [ ] Define an Operator execution context for explicit idempotency-key delivery.
  V1 preserves the key across whole-node replay, but a plain callable must
  implement idempotency from its own input/environment.

## 4. Unify Runtime Events

- [x] Define one ordered Runtime Event protocol with UTC millisecond timestamps,
  stable event types, entity identity, channel, visibility, and a monotonic
  Session sequence.
- [x] Persist Runtime Events atomically with materialized invocation state and
  expose the same protocol through REST and SSE.
- [x] Preserve enough execution identity and ordering for historical projection,
  replay cursors, live graph state, and timeline debugging.
- [x] Add a read-only Observation service and React tracing UI for Workflow,
  Session, Invocation, graph, timeline, input/output, and event inspection.
- [ ] Add paginated event checkpoints so very large Invocations do not require
  replaying their complete event history during Observation bootstrap.
- [ ] Add Observation authentication, payload redaction, and ArtifactRef-aware
  rendering before exposing the service outside a trusted development network.
- [ ] Feed the same Runtime Event protocol to optimizer inputs after event types
  have been exercised by real workloads.

## 5. Add Higher-Level Agent Features

- [ ] Add `AgentNode` as compile-time syntax sugar over ordinary nodes and edges.
- [ ] Add tool loops, memory integration, reusable harnesses, and dynamic
  fan-out without changing Scheduler or Runtime core semantics.

## 6. Build the Optimizer

- [ ] Start optimizer implementation only after Runtime Event and observability
  contracts are stable.
- [ ] Produce reviewable Workflow patches instead of mutating active executions.
- [ ] Evaluate patches against recorded runtime data before promotion.
