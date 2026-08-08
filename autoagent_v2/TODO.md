# AutoAgent V2 TODO

This file tracks work intentionally deferred from the first standalone V2 Core
implementation. It is not a compatibility plan for V1.

## Next Core gate

The following items were confirmed by the post-implementation V1/V2 audit and
should be resolved before building a durable V2 Server on top of Core.

### P0 correctness

- Separate Runtime Event capture and delivery from authoritative execution.
  Business state is committed first; Runtime Event construction, freezing, or
  Sink failure must only degrade tracing health and must never fail, cancel, or
  change the result of the Invocation. Checkpoint capture failure likewise
  degrades recoverability rather than business execution. Keep attached User
  Event stream semantics as a separate contract.
- Decouple `ResourcePolicy` accounting from Runtime Event capture. In the
  current implementation, `minimal` mode does not update the Invocation-wide
  Operator attempt/runtime counters, so the same Map can complete in Minimal
  and fail in Standard/Full. Capture mode must never change execution policy.
- Redesign `RuntimeSink.submit_events()` as a synchronous acceptance boundary
  in the execution flow. Core waits for it to return; return means the Sink has
  accepted ownership of the Events, normally by placing them into its queue.
  Sink-defined high/hard pressure may deliberately block that execution point.
  The ownership transfer must still be cancellation-safe so Core never retries
  an ambiguously accepted batch. Database or remote persistence after queue
  acceptance remains Sink-owned work.
- Enforce the advertised immutable handoff boundary recursively. Frozen Event
  and Checkpoint dataclasses still contain mutable dictionaries, lists, and
  values; a Sink can mutate data retained by the Invocation Handle or later
  delivered to an attached stream. Use immutable value envelopes or detached
  ownership per consumer without moving serialization back onto the executor
  hot path.
- Persist recovery-attempt progress at the pre-Node Checkpoint boundary.
  `NodeExecutionRequest.recovery_attempt` is incremented only after the current
  Checkpoint is captured, so repeated crashes can replay the same Node forever
  despite `RecoveryPolicy.max_attempts`.

### P1 completeness

- Restore the selector-less Map compile rule. A non-loop Map may omit its
  selector only with exactly one unambiguous incoming value. A loop-header Map
  must separately have one external entry value and one unambiguous back-edge
  value; multiple external or multiple possible back values require a selector.
  Replication has no item selector: it requires one logical input, so ambiguous
  incoming values must be resolved by Input Mapping. Also validate
  multi-parameter mapped inputs against the Operator argument contract when
  their structure is statically visible.
- Record cancelled/interrupted physical Operator Calls in Standard and Full.
  `OperatorCallRecord.status` already permits `cancelled`, but cancellation
  currently exits before a Call record/Event is produced. Add async/sync stream
  close tests and release closable stream sources on timeout and cancellation.
- Make Workflow Revision identity include callable parameter required/default
  semantics while continuing to require explicit Operator/Hook version changes
  for implementation-only changes. Exclude child Workflow display names from
  the semantic hash, matching top-level Workflow and Node display metadata.
- Add `SessionSnapshot`, `AutoAgentApp.release_session()`, and
  `AutoAgentApp.restore_session()`. Preserve original Session/Invocation
  timestamps across explicit recovery and reject snapshots for unregistered
  Workflow ids.
- Define bounded App shutdown when a Sink never accepts pending or cancellation
  Events. Core must expose the failed delivery boundary without allowing
  `close()` to wait forever. Enforce `admission_timeout` in Core rather than
  trusting every Sink implementation to honor the argument.
- Treat an explicitly supplied empty `session_id` deterministically (prefer a
  validation error); do not silently replace it with a generated UUID.
- Simplify Node policy composition before stabilizing the authoring API. Fold
  per-Call timeout, retry attempts, and backoff into one explicit Operator Call
  policy; move ordered fallback Operators into an explicit fallback policy;
  rename ResourcePolicy to execution limits with clear Invocation-wide
  counters; represent Map versus Replication as one mutually exclusive parallel
  policy. Compile these public settings into one immutable execution plan with
  a documented attempt order. Separately decide whether an unhandled Operator
  failure may select a failure Edge to another Node; do not conflate graph-level
  failure routing with an in-Node fallback Operator.
- Redesign Runtime/Context ownership before stabilizing Sink and public return
  contracts. Audit exactly which Runtime state, input/output values, and Context
  views are passed to hooks, Sinks, Invocation Handles, users, and persistence;
  define isolation, mutability, lifetime, and serialization at every boundary.

## Core correctness

- Lock down the implemented Checkpoint boundaries independently from Runtime
  Event boundaries: initialized scheduler state, quiescent scheduler state
  before the next Node starts, and committed Wait state. Checkpoints must remain
  latest executable state, not a tracing projection, and the tests must prove
  that incremental downstream scheduling never captures in-flight Node state.
- Persist Context path revision metadata in Checkpoints if Checkpoints are ever
  offered while concurrent Node executions are in flight. Current Core only
  offers runnable Checkpoints at quiescent scheduling boundaries.
- Define cancellation convergence for non-cooperative synchronous Operators.
  Python cannot stop a running worker thread; document idempotency expectations
  before adding retry behavior around such timeouts.
- Decide whether a recovered Wait should emit a dedicated recovery Event per
  Wait or only the current Invocation-level recovery Event.
- Add a global Invocation execution budget so a large cycle cannot evade the
  implemented per-Node safety limit by spreading work across many Nodes.

## Completed Workflow contract

- `workflow.md` is implemented by one Compiler analysis and one scope-aware
  Scheduler: all-matches DAG fan-out, complete fan-in, reducible SCC analysis,
  one Back Edge per Loop, nested and same-Header sibling Loop regions, scoped
  re-entry, cross-level Exit, and deterministic static/runtime conflicts.
- Parallel Loop boundaries stabilize before committing Back or Exit. Pending
  transitions survive Wait/Resume, conflicting Back/Exit choices fail
  atomically, and `LOOP_NO_ROUTE` detects a settled boundary with no route.
- Every Node inherits a finite Invocation-wide execution limit; Map/Replication
  units do not consume additional Node executions. The independent
  `workflow.md` conformance suite covers Compiler, Scheduler, Executor, Wait,
  cancellation, and safety behavior.

## Compiler and authoring diagnostics

- Build structured `Diagnostic` output with stable codes, locations, hints, and
  severity on top of the existing `WorkflowCompiler`; do not create a second
  compiler implementation.
- Add a deterministic structural snapshot and Mermaid preview derived from the
  same Workflow IR and compiler analysis indexes.
- Add richer cross-contract diagnostics for multi-parameter Operator inputs,
  fallback policies, and idempotency-key injection.
- Decide which analysis helpers belong to public Core versus future CLI/Skill
  packages.

## Runtime Events and tracing

- Finalize the Runtime Event schema, error/timing contract, reducer contract,
  and graph projection with the V2 Server/Tracing Service. Current Full
  Operations are not sufficient to rebuild Scheduler/Wait/counter state into a
  `RecoveryCheckpoint`, so Replay/Fork and Event-based Checkpoint repair are not
  implemented yet.
- Preserve the current mode boundary: `minimal` emits no Runtime Events,
  `standard` emits graph-level Events and every physical Operator Call without
  heavy values, and `full` adds phases, state operations, inputs, and outputs.
- Define optional Map/Replication summary Events without replacing the required
  per-call Events.
- Add Event batching and a production Sink implementation outside Core. Backlog,
  retention, database queues, and remote delivery belong to the Sink/Server,
  not the Invocation coordinator.

## Performance

- Stop retaining unused `NodeExecutionResult.phases` and
  `NodeExecutionResult.operator_calls`; progress is already delivered to the
  coordinator, and the lists duplicate heavy Map inputs/outputs until the whole
  Node finishes. Compact completed `NodeExecution` objects instead of retaining
  duplicate mapped input/output already owned by Runtime output state.
- Replace one-task-per-item Map/Replication execution with a bounded worker
  queue. A Semaphore limits active Calls but still creates a Task for every
  selected item, so very large Maps can cause an avoidable memory spike.
- Benchmark large fan-out/fan-in graphs, nested loops, large Contexts, and slow
  Sinks using `tests/benchmarks/benchmark_core.py` as the starting harness.
- Replace eager Session/Invocation Context snapshots with persistent or
  copy-on-write structures only if profiles show they dominate execution.
- Consider bounded cleanup for per-Workflow Node semaphores in `NodeExecutor`.
- Profile the remaining Loop-boundary set construction; Workflow IR now owns
  Edge-id and Loop-containment indexes, but compatibility checks still build a
  few small temporary sets per completed Node.
- Measure Checkpoint serialization separately from execution once serialization
  moves to a real Sink worker.
- Add memory benchmarks for many concurrent Sessions and many active Waits.

## Server and platform boundary

- Implement local Server, persistence, tracing query API, and UI as a separate
  V2 project layer that consumes Core Events and Checkpoints.
- Keep execution APIs (`invoke`, `submit`, `stream`, `resume`, `recover`,
  `cancel`) backed by the same Core coordinator.
- Design remote Operator, Capability, and Workflow references only after the
  local Server boundary is stable.
