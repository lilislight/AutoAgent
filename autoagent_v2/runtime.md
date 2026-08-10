# AutoAgent V2 Runtime, Operations, Checkpoints, and Events

## Purpose

This document is the normative contract for V2 execution state, state changes,
recovery checkpoints, and Runtime Events. The implementation must prefer the
smallest correct representation. Derived indexes, caches, and duplicated state
are added only after a benchmark demonstrates that they are necessary.

## Core invariants

1. Each Session owns one independent Runtime State.
2. Runtime State is the only authoritative execution state.
3. Scheduler and Executor objects contain behavior and infrastructure only.
   They do not own mutable Workflow execution state.
4. Every Runtime State modification is an ordered `StateOperation` applied by
   the Session's single Runtime coordinator.
5. A batch of State Operations is validated and applied atomically.
6. A Checkpoint is the complete JSON representation of one Session Runtime
   State. `app.recover(checkpoint)` restores both the Session and its current
   Invocation.
7. Hook and Operator arguments are isolated Python copies. They are temporary
   execution data and never become Runtime State by accident.
8. Runtime Event boundaries and Checkpoint boundaries are independent from
   State Operation boundaries.

## Recovery equivalence

After encoding a Runtime State to a Checkpoint and recovering it in a new App,
Core must make the same control-flow decisions as the original process given
the same Workflow Revision and the same external Operator results.

Any value that can change post-recovery behavior belongs in Runtime State. This
includes ordered Scheduler work, unresolved activations, fan-in state, Loop
scope and iteration, Wait state, Context, Node state and committed output,
idempotency identity, retry state that must survive Node replay, cancellation,
and semantic deadlines.

Process infrastructure does not belong in Runtime State. Tasks, futures,
locks, semaphores, thread pools, HTTP clients, provider clients, and temporary
Hook or Operator inputs are recreated after recovery. Concurrency settings may
change throughput but must not change logical result ordering.

## Minimal Runtime State

The canonical Session Runtime contains only:

- `state_version` for ordered atomic commits and parallel Context conflict
  detection;
- Session identity, Workflow identity, Session Context, and Context write
  versions;
- current/latest Invocation identity, Workflow Revision, mode, state, input,
  Invocation Context, output, error, cancellation and recovery state;
- Scheduler ready work, unresolved activations, fan-in decisions, execution
  scopes, and Loop instances;
- Node executions with identity, Node identity, scope, state, incoming
  activations, start version, committed output, error, and stable idempotency
  identity;
- scoped Edge decisions needed by the Scheduler;
- active Waits.

Trace-only timing, Hook phase values, formatting, duplicated running/completed
indexes, and other derivable caches are excluded. Completed Operator input and
output are Event data, not permanent Checkpoint state. If Node-level recovery
restarts an unfinished Node, its Input Mapping, Selector, Operator calls,
Aggregation, and Output Binding are executed again.

## State Operations

V2 uses the JSON Patch-compatible subset `add`, `replace`, and `remove`.
Operation paths are stable tuple paths rooted in the canonical Runtime State.
Operations in one batch are ordered. The batch is preflighted in full and then
applied without awaiting, so either every Operation commits or none does.

Parallel Node workers never mutate Runtime State. They return results to the
single Session coordinator. A Node records `started_state_version`. A Context
write conflicts when an overlapping path was written at a version newer than
that start version. Same, ancestor, and descendant paths overlap. Serial Nodes
may overwrite a path because they start after the earlier commit.

The first implementation does not contain a general Operation optimizer.
ContextPatch duplicate or overlapping paths are rejected for correctness.
Operation coalescing is added only after measurement.

## ContextPatch

Output Binding returns the small user-facing `ContextPatch`; user code never
constructs State Operations. Core validates the patch, detects parallel write
conflicts, captures Runtime-owned values, encodes the same values for durable
use, and converts the patch plus path-write metadata into ordered Operations.
Context changes and Node terminal state commit atomically.

## Checkpoints

Core creates a complete latest Checkpoint at these boundaries:

1. Genesis, before the Invocation executes a Node;
2. after a Node enters `completed`, `failed`, `skipped`, or `cancelled`;
3. after a Wait is established;
4. after the Invocation enters a terminal state.

Resume acceptance does not create a Checkpoint. Completion of the resumed Wait
Node does. Cancel request acceptance does not create a Checkpoint. Invocation
terminal convergence does.

If a process fails after a Node-terminal Checkpoint but before outgoing Edges
are evaluated, or after they were evaluated only in process memory, recovery
starts from the completed Node and evaluates its unresolved outgoing Edges
again. The Node is not executed again. Edge evaluation is deliberately not a
Checkpoint boundary.

Concretely, that Checkpoint stores the completed Node occurrence together with
one pending control-flow advance. Recovery sees that the Node result is already
committed, consumes the pending advance, and evaluates the outgoing Edges. Any
Scheduler mutations made after the Checkpoint are absent and are therefore
recomputed. This is safe under the Workflow rule that Edge conditions are
side-effect-free decisions over their supplied Context. Core cannot prevent a
Python condition from reading or mutating process globals; doing so makes
recovery nondeterministic and is an authoring error.

A Wait boundary does not imply that the whole Invocation is waiting. If sibling
Nodes or pending control-flow advances remain runnable, the Checkpoint keeps the
Invocation state `running`, preserves the Wait, and captures every unfinished
sibling for replay. It uses `waiting` only when no runnable work remains.
Recovery must inspect the restored Scheduler cursor rather than treating the
presence of a Wait as global quiescence. Resume runs the waiting Node's
Output Binding against the latest committed Session and Invocation Context.

Every generated Checkpoint replaces `Invocation.last_checkpoint` and is also
offered to the Sink in every Event mode. A Sink may coalesce checkpoints and
persist only the latest one. In Full mode it may additionally sample
intermediate Checkpoints to accelerate replay.

Terminal Invocations retain their final Checkpoint. Recovering a terminal
Checkpoint restores the Session Context and compact Invocation Handle but does
not launch Scheduler or Executor work.

### Boundary decision table

| Runtime point | Checkpoint | Reason |
| --- | --- | --- |
| Invocation accepted, before first Node | yes | Genesis recovery source |
| Invocation changes to `running` | no | Genesis already contains the pre-run state |
| Node changes to `running` | no | an unfinished Node is replayed from its saved start baseline |
| Input Mapping, Selector, Operator Call, Aggregation, Output Binding | no | Node-internal work is not a recovery boundary |
| Node changes to `completed`, `failed`, `skipped`, or `cancelled` | yes | Node-level recovery boundary |
| Edge evaluation | no | recovery may evaluate unresolved outgoing Edges again |
| Wait is established | yes | preserves request, Wait id, Node input, and scope |
| Resume response is accepted | no | the resumed Node terminal boundary records the result |
| Cancel request is accepted | no | Node and Invocation terminal convergence records cancellation |
| Invocation becomes `completed`, `failed`, or `cancelled` | yes | final Session and Invocation state |

When one parallel Node reaches a terminal boundary while siblings are still
running, the Checkpoint includes each unfinished Node's request and isolated
start Context baseline. Recovery replays those unfinished Nodes from that
baseline. It must not silently substitute Context committed later by a faster
sibling.

## Runtime Events

Minimal mode emits no Runtime Events. Standard and Full modes use the same
semantic Event boundaries. Standard omits Operations and heavy input/output.
Full may include the ordered State Operations accumulated for that boundary and
the phase input/output required for debugging. An Event is emitted even when
its Operations are empty. Event sequence and Runtime `state_version` are
independent.

The Runtime Event vocabulary is intentionally small:

- `invocation_state_changed`;
- `node_state_changed`;
- `edge_evaluated`;
- `input_mapping_finished`;
- `item_selection_finished` when a custom Selector ran;
- `operator_call_finished` for every physical attempt, including Map,
  Replication, Retry, Fallback, timeout, and cancellation;
- `aggregation_finished` when an Aggregator ran;
- `output_binding_finished` when Output Binding ran.

There are no separate Context, Wait, Resume, Recovery, Retry, Fallback, or
Cancel Runtime Event types. Wait and Resume are visible through Node state
changes; cancellation through Node and Invocation state changes; recovery uses
the same ordinary execution events. ContextPatch details are shown by the Full
`output_binding_finished` Event. User Events remain an independent,
user-defined channel at their fixed mapping points.

Invocation and Node state Events record one occurrence timestamp in
milliseconds. Edge and Node-phase Events capture start and completion wall-clock
timestamps in milliseconds and a monotonic duration in nanoseconds. Their
timing breakdown records the applicable components, including dispatch wait,
executor-permit wait, thread-pool queue wait, handler time, stream production,
and stream delivery. Wall-clock timestamps are display data; monotonic
nanoseconds are used for durations.

### Event boundary table

| Execution point | Event | Status or important detail |
| --- | --- | --- |
| Invocation starts or terminates | `invocation_state_changed` | `running`, `completed`, `failed`, `cancelled`, or stable `waiting` |
| Node starts, waits, resumes, or terminates | `node_state_changed` | Resume is another `running` state change, not a special Event |
| Edge condition finishes | `edge_evaluated` | `selected`, `not_selected`, or `failed`; unconditional Edges are included |
| custom Input Mapping finishes | `input_mapping_finished` | one success or failure Event |
| custom Map Selector finishes | `item_selection_finished` | omitted when no custom Selector ran |
| physical Operator attempt finishes | `operator_call_finished` | one per Retry, Fallback, Map unit, or Replica, including timeout/cancel |
| custom Aggregator finishes | `aggregation_finished` | omitted when no custom Aggregator ran |
| custom Output Binding finishes | `output_binding_finished` | Full payload contains ContextPatch; no separate Context Event |

`output_binding_finished: completed` is emitted only after the coordinator has
validated parallel write conflicts and atomically committed the ContextPatch.
Its Full Operation batch owns the Context changes. A later
`node_state_changed: completed` Event owns only the Node/output state changes.
If commit validation fails, Output Binding is reported as failed and no partial
Context change is visible.

An Event boundary does not imply a Checkpoint, and a Checkpoint boundary does
not invent an Event type. Runtime Event generation and serialization failures
are logged and may create a sequence gap, but do not change Workflow business
state. Checkpoint generation or Sink checkpoint-offer failures only degrade
recoverability and do not detach Event delivery.

### Event mode matrix

| Mode | Runtime Events | Operations | phase input/output | Checkpoints | User Events |
| --- | --- | --- | --- | --- | --- |
| Minimal | none | none in Events | none | all required boundaries | unchanged |
| Standard | all semantic boundaries | omitted | heavy values omitted; timing/error retained | all required boundaries | unchanged |
| Full | all semantic boundaries | ordered batches | debugging values retained | all required boundaries | unchanged |

## Timing capture points

For a measured operation:

1. capture `scheduled_at_ms` and a monotonic scheduled timestamp when the work
   becomes eligible;
2. capture executor-permit wait around semaphore admission;
3. for synchronous callables, capture thread-pool queue wait from submission to
   handler entry;
4. capture handler start immediately before user code and handler end
   immediately after it returns or raises;
5. capture stream production and delivery independently;
6. capture `completed_at_ms` and total monotonic duration at finalization.

The sum of named timing components must not exceed total duration except for
explicitly documented overlap. Timestamps and durations are captured around the
actual operation, not around Event serialization or Sink backpressure.

State-change Events use only `occurred_at_ms`. Measured Events use
`started_at_ms`, `completed_at_ms`, and `duration_ns`. The standard timing keys
are:

- `executor_wait_ns`: waiting for the App-wide Hook/Operator permit;
- `thread_pool_wait_ns`: synchronous callable submission until worker entry;
- `handler_ns`: time in user code, including a returned Awaitable;
- `stream_ns`: stream production and reduction excluding delivery;
- `stream_delivery_ns`: time delivering emitted stream values;
- `dispatch_wait_ns`: optional Scheduler-to-execution delay when that boundary
  has an independently measurable dispatch timestamp.

Absent components are zero or omitted. Sink acceptance time is deliberately
not included in user-code duration.

## Required behavioral tests

The test suite must compare uninterrupted execution with encode, process-state
discard, `app.recover(checkpoint)`, and continuation at every supported
Checkpoint boundary. Coverage includes serial and parallel Nodes, conflicting
and non-conflicting Context writes, fan-in, conditions, Loop and nested Loop,
Map and Replication, Retry/Fallback/Backoff/timeout, Wait/Resume, cancellation,
failure policies, multiple Sessions, deterministic ordering, JSON round-trip,
State Operation atomicity and replay, ContextPatch conversion, and Event timing
capture.
