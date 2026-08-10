# AutoAgent V2 TODO

This file contains only unfinished V2 work. Completed behavior is documented in
`workflow.md` and `runtime.md`; this is not a V1 compatibility plan.

## Current milestone: finish Core before Server

The Workflow control-flow contract, incremental parallel Scheduler, Hook and
Operator isolation, bounded execution concurrency, Runtime Event vocabulary,
Checkpoint boundaries, Wait/Resume basics, Sink acceptance boundary, and the
three Event modes are implemented and covered by the V2 test suite.

The internal canonical Runtime State gate is complete. Genesis plus ordered Full
Event Operations reconstructs the byte-equivalent terminal Runtime State across
serial and parallel execution, Context conflicts, fan-in, ordinary and nested
Loops, Wait/Resume, cancellation, failure, Map/Replication,
Retry/Fallback/timeout, and multiple Sessions.

## P0: canonical Runtime State

Completed:

- the exact versioned Runtime State Schema and strict JSON round trip;
- one atomic State Operation mutation path for Session, Invocation, Context,
  Scheduler, Node executions, Waits, pending advances, and counters;
- a behavior-only Scheduler whose durable cursor lives in Runtime State;
- atomic ContextPatch data and path-revision commits;
- one globally ordered Operation buffer shared by all Full Event boundaries;
- completed-Node compaction and removal of trace-only timing and replay-only
  Context baselines from terminal Node state;
- 286 passing V2 tests and replay-equivalence coverage for the scenarios above.

Intentionally deferred by the current design decision:

- replacing the public `RecoveryCheckpoint` record with the raw canonical
  Runtime State checkpoint record;
- changing `AutoAgentApp.recover()` to consume only that new record and deleting
  the current adapter path.

The current Recovery contract remains an adapter over canonical Runtime State.
Do not implement these two deferred changes until their external API is
reviewed separately.

## P0 decisions after Runtime State integration

- Define durable `RecoveryPolicy.max_attempts` accounting. Node start is not a
  Checkpoint boundary, so repeated process crashes can otherwise replay the
  same unfinished Node without consuming a durable attempt. Choose whether the
  budget is encoded at the previous durable boundary, supplied as explicit
  recovery metadata, or documented as process-local.
- Add one global Invocation execution budget. The existing per-Node limit does
  not bound a large cycle that spreads work across many different Nodes.
- Define bounded App shutdown when a Sink never accepts a pending or
  cancellation Event. Core must surface the failed acceptance boundary without
  owning Sink queue policy or persistence.

## P1: execution and authoring completeness

### Wait and recovery

- Redesign Wait as an Operator outcome in the ordinary attempt/fallback chain,
  rather than a special Node category. A fallback may enter Wait; Resume must
  continue the same logical Node Execution through response validation, latest
  Context, Output Binding, Event generation, Checkpointing, and outgoing Edge
  evaluation. Preserve multiple concurrent Wait ids.
- Add `SessionSnapshot`, `AutoAgentApp.release_session()`, and
  `AutoAgentApp.restore_session()`. Preserve timestamps and reject snapshots
  whose Workflow id is not registered.
- Document cancellation and idempotency expectations for non-cooperative sync
  Operators. Python cannot terminate an already-running worker thread.

### Workflow input and policy contracts

- Restore the selector-less Map rule. A non-loop Map may omit a Selector only
  for one unambiguous incoming value. A loop-header Map must independently have
  one unambiguous external value and one unambiguous Back-Edge value.
  Replication also requires one logical input unless Input Mapping resolves the
  ambiguity.
- Validate statically visible multi-parameter mapped input shapes against the
  Operator contract.
- Simplify Node policy composition before freezing the authoring API:
  consolidate timeout/retry/backoff into an Operator Call policy, make fallback
  an explicit policy, rename ResourcePolicy to execution limits, and represent
  Map/Replication as one mutually exclusive parallel policy compiled into an
  immutable execution plan.
- Decide whether an unhandled Operator failure may select a graph-level failure
  Edge. Keep this separate from in-Node fallback Operators.
- Include callable parameter required/default semantics in Workflow Revision
  identity. Continue requiring explicit Operator/Hook version changes for
  implementation-only changes and exclude display-only child Workflow names.
- Reject an explicitly supplied empty `session_id` instead of silently creating
  a UUID.
- Release closable sync and async stream sources on timeout and cancellation.

## P2: Compiler and Coding-Agent diagnostics

- Add structured `Diagnostic` results with stable codes, severity, locations,
  and hints on top of the existing `WorkflowCompiler`; do not build a second
  compiler.
- Derive a deterministic structural snapshot and Mermaid preview from the same
  Workflow IR and analysis indexes.
- Add cross-contract diagnostics for mapped inputs, fallback plans, and
  idempotency-key injection.
- Decide which analysis helpers are public Core APIs and which belong to the
  future CLI/Skill layer.

## P3: local Server and tracing

Start only after the canonical Runtime State gate passes.

- Implement an in-memory journal Sink. `submit_events()` only accepts immutable
  records into Sink-owned memory; persistence and remote delivery run in a
  separate Server worker.
- Implement local persistence for Events, latest Checkpoints, Workflow
  Revisions, Sessions, and Invocations without querying Core execution memory.
- Define the Server reducer and graph projection from the finalized Event and
  Operation schema.
- Expose execution APIs (`invoke`, `submit`, `stream`, `resume`, `recover`, and
  `cancel`) separately from tracing query APIs.
- Expose Sink queue pressure, accepted/durable watermarks, backend health,
  retry state, and undelivered boundaries through Server health.
- Add the local tracing UI only against Server query/stream APIs.
- Add optional Map/Replication summary projections without removing required
  physical Operator Call Events.

## Later platform work

- Add a Platform Sink/connector after the local Server boundary is stable.
- Design hosted execution, remote Workflow invocation, and remote Operator or
  Capability references as platform protocols rather than Core dependencies.
- Add Replay/Fork only after Full Operations can rebuild arbitrary supported
  Checkpoints and the Server reducer has conformance tests.

## Performance work

- Benchmark large fan-out/fan-in, nested Loops, large Contexts, slow Sinks, many
  concurrent Sessions, and many active Waits.
- Measure Event and Checkpoint capture/serialization independently with
  realistic payloads.
- Profile the current 1 ms thread-future polling path; the latest trivial
  one-Node benchmark is dominated by fixed per-Invocation overhead.
- Replace isolated Hook-value deep copies with persistent or copy-on-write
  values only if profiling proves they dominate real execution.
- Profile remaining Loop-boundary temporary set construction before adding
  caches.
- The current local reference run completes 1,000 one-Node Invocations in about
  6.0-6.2 seconds in Core-only, Standard, and Full modes. The inherited
  1,000-iteration safety Loop completes in about 7.2 seconds at roughly 35 MB
  peak RSS. Treat these as local observations, not CI thresholds.
