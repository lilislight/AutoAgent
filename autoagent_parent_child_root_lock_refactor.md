# AutoAgent Parent / Child Root Lock Review and Refactor Proposal

> Target branch: `refactor`  
> Inspected HEAD: `849c333ff5101ee496408d61c012e024bed99af9` (`uplift`)  
> Scope: Parent/Child coordination, Root Runtime Lock, Session commit serialization, Sink ACK, checkpoint/recovery/cancel interactions.

## 1. Executive Summary

The current AutoAgent implementation **does have a Root Runtime Lock**.

The effective path is:

```text
AutoAgentApp._emit(session_id, ...)
        │
        ├── root = _root_session_id(session_id)
        │
        └── async with _runtime_lock(root)
                 │
                 ├── scheduler planning
                 ├── RuntimeRepository.commit(session_id)
                 │       │
                 │       ├── async with _session_lock(session_id)
                 │       ├── plan Event / Delta
                 │       ├── reduce candidate State
                 │       └── await sink.append(event)
                 │
                 ├── Child ownership bookkeeping
                 └── settle runtime commit
```

This means every Session belonging to one Parent/Child graph shares the same outer Root lock.

```text
Root P
├── Child A
├── Child B
├── Child C
└── Child D
```

all resolve to:

```text
_root_session_id(P) = P
_root_session_id(A) = P
_root_session_id(B) = P
_root_session_id(C) = P
_root_session_id(D) = P
```

and therefore all Runtime Event emission goes through:

```text
_runtime_lock(P)
```

As a result, even a purely local Child event such as `Child A OperatorCallCompleted` temporarily prevents Child B, Child C, Child D, and Parent P from entering their own `_emit()` critical section.

The Root lock is understandable as a correctness-first design because it makes Parent/Child coordination, cancellation, recovery and checkpointing easier to reason about. However, it conflates two concepts:

```text
Consistency / coordination domain
```

and:

```text
Execution concurrency domain
```

The Parent/Child graph is one ownership/recovery domain, but its independent Sessions do not need to be one serialized execution domain.

**Recommended long-term direction:**

> Keep Session-local commits serialized per Session, but remove ordinary Session-local Runtime transitions from the Root-global lock. Use explicit graph coordination only for operations that truly span Parent/Child Sessions.

Do **not** simply delete `_runtime_lock`. First define cross-Session invariants and move graph-wide operations onto a clear coordination protocol.

---

## 2. Current Parent / Child Model

A Child is not a nested execution state inside the Parent RuntimeState.

Each Child is a separate:

```text
Session
+
Invocation
+
RuntimeState
+
Runtime Event sequence
```

The Parent stores a durable `ChildInvocationPlan` containing units with roughly:

```text
unit_index
child_session_id
child_invocation_id
input
phase
```

The Parent-side phase protocol is:

```text
planned
   ↓
opened
   ↓
accepted
   ↓
terminal
```

This is best understood as a **durable Parent/Child handshake protocol**.

### `planned`

Parent has durably committed its intent to create the Child.

### `opened`

The Child Session/Invocation admission has been established.

### `accepted`

Parent has durably acknowledged admission and allows the Child execution task to proceed.

### `terminal`

The Child is durably terminal and Parent has recorded that fact in its own plan.

The important causal ordering is:

```text
Child terminal durable
        ↓
Parent unit.phase = terminal
```

A crash between those two steps must be recoverable.

---

## 3. Actual Parent / Child Relationships

It is important to separate real dependencies from accidental implementation serialization.

### 3.1 Ownership

Parent durably owns Child identity:

```text
Parent ChildPlan
    ↓
Child session_id + invocation_id
```

A transient reverse index `_child_owners` supports efficient Child → Parent lookup. This is a reasonable rebuildable index.

### 3.2 Admission

Parent commits the plan, Child is opened, Parent progresses the admission marker.

### 3.3 Child execution

Once admitted, normal Child runtime execution is Session-local:

```text
InputMapped
CapabilityResolved
OperatorCallStarted
OperatorCallCompleted
OperatorCallFailed
Aggregated
OutputBound
RoutingResolved
NodeCompleted
NodeFailed
WaitRequested
WaitResumed
```

These events generally do **not** require Parent participation.

Sibling Children also do not normally depend on each other.

### 3.4 Child terminal → Parent marker

When a Child becomes terminal:

```text
Child InvocationCompleted / Failed / Cancelled
        ↓
Child durability ACK
        ↓
Parent ChildInvocationPhaseChanged(..., terminal)
```

This is a real cross-Session causal dependency.

### 3.5 Await mode

Parent suspends while waiting for its Child plan and resumes when the plan settles:

```text
ChildAwaitSuspended
        ↓
Children execute
        ↓
ChildAwaitReady
```

Parent should use incremental summaries such as:

```text
child_remaining
child_failed_count
```

rather than repeatedly scanning every Child state.

### 3.6 Spawn mode

Spawn Children continue independently. Parent retains ownership for recovery/cancel/checkpoint/unload semantics, but normal Parent execution does not wait for them.

### 3.7 Cancellation

Parent cancellation may cascade to descendants. This is a genuine graph-wide operation.

### 3.8 Recovery

Recovery must reconcile partially completed Parent/Child handshakes. This is also genuinely graph-wide.

### 3.9 Checkpoint / unload / Root replacement

These operations may need to reason about the whole ownership graph and therefore legitimately require broader coordination.

---

## 4. Current Concurrency Controls

There are multiple mechanisms and they solve different problems.

### 4.1 Repository Session Lock

`RuntimeRepository._session_lock(session_id)` serializes one Session's Event stream.

This protects:

```text
before State
sequence allocation
Event creation
pending append
Sink ACK
candidate publication
```

This lock is conceptually correct and should remain.

Invariant:

> One Session has one ordered Runtime Event stream.

### 4.2 Root Runtime Lock

`AutoAgentApp._runtime_lock(root_session_id)` serializes **every Runtime transition in the entire Parent/Child graph**.

This is the lock under review.

### 4.3 Completion Lock

`WorkflowExecutor` also uses a per-Session completion lock around the sensitive completion window:

```text
context preview
output binding
routing condition evaluation
NodeCompleted / NodeFailed
```

This is primarily same-Session context/commit protection, not a Parent/Child lock.

### 4.4 Child concurrency semaphore

Child `max_parallelism` uses a semaphore. This is resource control, not consistency control.

---

## 5. Confirmed Root Lock Problem

Current `_emit()` effectively does:

```python
root = self._root_session_id(session_id)

async with self._runtime_lock(root):
    ...
    transition = await self._repository.commit(...)
    ...
```

`RuntimeRepository.commit()` then does:

```python
async with self._session_lock(session_id):
    ...
    await self._settle_pending(session_id)
```

and `_settle_pending()` may execute:

```python
await self.sink.append(event)
```

Therefore the current lock stack is effectively:

```text
Root Lock
    ↓
Session Lock
    ↓
Sink I/O
```

This is the core performance concern.

---

## 6. Why It Matters

Suppose:

```text
Parent P
├── Child A
├── Child B
├── Child C
└── Child D
```

and a durable Sink takes 10 ms per Event.

If Child A emits `OperatorCallCompleted`, it holds `RootLock(P)` while waiting for `sink.append(A-event)`.

During that time B/C/D/P cannot enter `_emit()` even though they have separate Event streams.

The graph behaves closer to:

```text
A Event + Sink
       ↓
B Event + Sink
       ↓
C Event + Sink
       ↓
D Event + Sink
```

instead of allowing causally independent commits to overlap.

With a non-trivial DB/network Sink, graph throughput may become dominated by:

```text
number_of_events × sink_latency
```

rather than available Child concurrency.

---

## 7. Why the Root Lock Probably Existed

The design is not irrational.

A coarse graph lock makes many difficult races disappear:

```text
Child completion ↔ Parent phase update
Parent cancel ↔ descendant cancellation
recovery ↔ phase reconciliation
checkpoint / unload
Root Invocation replacement
spawned Child cleanup
```

The mental model becomes:

> Only one durable transition anywhere in one Root graph may happen at a time.

That is strong and easy to reason about.

The cost is that graph consistency and normal execution concurrency become coupled.

---

## 8. Desired Architectural Distinction

The Root graph should remain a:

```text
coordination / ownership domain
```

but should not necessarily remain a:

```text
single serialization domain
```

A Child belonging to a Parent does not imply that:

```text
Child A OperatorCallCompleted
```

must be ordered against:

```text
Child B OperatorCallCompleted
```

There is no semantic dependency between those two events.

---

## 9. Recommended Concurrency Model

Target model:

```text
                       Parent Session P
                            Lock(P)
                               │
              ┌────────────────┼────────────────┐
              │                │                │
          Child A          Child B          Child C
          Lock(A)          Lock(B)          Lock(C)
```

Ordinary events:

```text
A event → Session Lock A
B event → Session Lock B
C event → Session Lock C
```

can proceed concurrently.

Graph-level operations use an explicit graph coordination mechanism.

---

## 10. Operation Classification

### Session-local operations

Normally require only the Session lock:

```text
SessionOpened
InvocationStarted
InputMapped
CapabilityResolved
OperatorCallStarted
OperatorCallCompleted
OperatorCallFailed
Aggregated
OutputBound
RoutingResolved
NodeStarted
NodeCompleted
NodeFailed
WaitRequested
WaitResumed
```

provided the transition does not directly modify another Session.

### Causal cross-Session operations

Require ordered multi-step protocols, but not necessarily one lock spanning both durable writes:

```text
Child admission
Child terminal → Parent terminal marker
```

Preferred model:

```text
Step A durable
    ↓
Step B durable
```

with recovery repairing any gap.

### Graph coordination operations

Legitimately require broader coordination:

```text
cancel whole graph
recover graph
graph checkpoint
unload graph component
replace old Root invocation
rebuild ownership topology
```

The graph lock should protect these graph topology/control decisions, **not every Runtime Event**.

---

## 11. Recommended Refactor Direction

### 11.1 Keep Repository Session Lock

Do not remove `RuntimeRepository._session_lock(session_id)`.

It is the natural serialization boundary for one Session Event stream.

### 11.2 Remove Root Lock From Ordinary `_emit()`

Long-term target:

```python
async def _emit(...):
    state = repository.state(session_id)
    graph_delta = plan_session_local_transition(...)

    event = await repository.commit(
        session_id=session_id,
        ...
    )

    update_transient_indexes(event)
    return event
```

`repository.commit()` already serializes each Session independently.

This allows:

```text
Session A commit || Session B commit
```

when causally independent.

### 11.3 Keep Root Coordination For Graph Operations

Introduce an explicit internal concept such as:

```text
GraphCoordinator
```

or keep `_runtime_lock(root)` only around graph-wide control operations.

Possible users:

```text
_cancel_graph
_recover
_load_checkpoint
_unload_session
root Invocation replacement
ownership topology mutation
```

If retained, rename the concept to reflect responsibility, e.g.:

```text
_graph_coordination_lock
```

instead of a generic runtime lock.

---

## 12. Cross-Session Protocol Design

### 12.1 Child creation

Recommended causal sequence:

```text
1. Parent commits ChildInvocationPlanned
2. Child Session is opened durably
3. Parent commits opened
4. Parent commits accepted
5. Child execution gate opens
```

Crash recovery must tolerate interruption after every step.

Do not hold one lock across both Parent and Child durable writes.

### 12.2 Child terminal

Recommended causal sequence:

```text
1. Child commits terminal Invocation event
2. Child terminal event receives durability ACK
3. Parent commits ChildInvocationPhaseChanged(..., terminal)
4. Parent derived counters update
5. If await plan settles, Parent commits ChildAwaitReady
```

If a crash occurs after step 2 but before step 3:

```text
Child = terminal
Parent unit = accepted
```

recovery should detect this and complete step 3.

---

## 13. Prefer Idempotent Causal Protocol Over Cross-Session Atomicity

Do not try to make:

```text
Child terminal
+
Parent marker
```

one atomic cross-Session transaction.

That would couple:

```text
two Session sequences
two RuntimeStates
two Event streams
possibly two Sink writes
```

and complicate replay, fork, recovery, distributed execution and remote runners.

The existing Child phase model already supports the better direction:

> Each Session commits independently; Parent/Child consistency is maintained by a durable, idempotent, recoverable handshake.

---

## 14. Lock Ordering Rules

Removing the Root lock increases the importance of avoiding cross-Session deadlocks.

Never allow one path to acquire:

```text
Parent Lock → Child Lock
```

and another:

```text
Child Lock → Parent Lock
```

Preferred rule:

> Do not hold multiple Session locks at once.

Perform:

```text
commit A
release A
commit B
release B
```

and rely on durable causal state + recovery.

If multiple locks are ever unavoidable, use deterministic ordering, but this should be rare.

---

## 15. Sink I/O Policy

There are two distinct questions.

### Should Root lock cover Sink I/O?

Recommended:

```text
NO for ordinary Runtime Events.
```

A slow Sink should not serialize independent Sessions in one graph.

### Should Session lock cover Sink I/O?

Currently:

```text
YES.
```

This is defensible because State publication follows Sink ACK and one Session's Event sequence must remain ordered.

Changing Session-lock/Sink interaction would be a separate durability redesign and should not be mixed into the Root-lock refactor.

---

## 16. Parent/Child Derived Indexes

Removing graph-wide serialization makes incremental summaries more valuable.

Parent should avoid repeatedly inspecting every Child RuntimeState.

Maintain rebuildable derived state such as:

```text
child_remaining[creation_id]
child_failed_count[creation_id]
```

Then:

```python
all_terminal = child_remaining[creation_id] == 0
any_failed = child_failed_count[creation_id] > 0
```

Both are O(1).

The durable `ChildInvocationPlan` remains the source of truth; the index remains rebuildable.

---

## 17. Existing Child O(N) / O(N²) Risks

These are separate from the Root-lock concurrency problem.

### `has_failed_child`

If it scans every unit in a plan, one call is O(N). If called after every Child completion, total work can become O(N²).

Recommended: maintain per-plan `child_failed_count`.

### Child task wait loop

If `_wait_for_child_tasks_or_failure()` calls a full `_first_unsuccessful_child(plan)` scan after every `FIRST_COMPLETED` round, worst-case complexity is:

```text
N completion rounds × N Child scan = O(N²)
```

Recommended: map Task → Child session and inspect only completed tasks, or consume the per-plan failure index.

---

## 18. Graph-Level Operations That Should Still Be Serialized

| Operation | Session lock | Graph coordination |
|---|---:|---:|
| Child normal operator event | Yes | No |
| Child normal node completion | Yes | Usually no |
| Parent normal node event | Yes | No |
| Parent ChildPlan creation | Yes | Possibly short topology coordination |
| Child terminal event | Yes | No |
| Parent terminal marker | Yes | No; causal protocol |
| Await-ready decision | Parent lock | Usually no if based on consistent derived state |
| Cancel whole graph | Yes per Session | Yes |
| Recovery | Yes per Session | Yes |
| Graph checkpoint | Stable Session boundaries | Yes |
| Unload graph component | Yes | Yes |
| Root Invocation replacement | Yes | Yes |
| Ownership graph rebuild | N/A | Yes |

The exact list should be validated by concurrency tests.

---

## 19. Recommended Staged Refactor

### Phase 0 — Benchmark Current Root Lock

Before changing behavior, use a synthetic Sink with delays:

```text
0 ms
0.5 ms
2 ms
10 ms
```

Test:

```text
1 Session
32 parallel sibling Nodes
32 parallel Child Sessions
100 Child Sessions
```

Measure:

```text
events/sec
end-to-end latency
Root lock wait duration
Session lock wait duration
Sink duration
```

This proves whether Root lock contention is material.

### Phase 1 — Instrument Lock Timing

Add internal diagnostics:

```text
root_lock_wait_ns
root_lock_hold_ns
session_lock_wait_ns
session_lock_hold_ns
sink_append_ns
```

### Phase 2 — Extract Graph Coordinator

Separate ordinary event emission from graph-wide control operations.

### Phase 3 — Remove Root Lock From Session-Local `_emit`

Allow independent Sessions to commit concurrently. Repository Session Lock becomes the normal commit serialization mechanism.

### Phase 4 — Harden Parent/Child Handshake

Add exhaustive interleaving tests for every crash boundary:

```text
planned before Child open
Child open before Parent opened
Parent opened before accepted
Child terminal before Parent terminal marker
Parent terminal marker retry
Child failure during sibling execution
Parent cancellation during Child completion
recovery during partial Child settlement
```

### Phase 5 — Optimize Child Summaries

Add `child_failed_count` and remove remaining repeated full-plan scans.

---

## 20. Required Concurrency Invariants

### Session ordering

For one Session:

```text
sequence N
must become durable/published before
sequence N+1
```

### No double publication

A lost ACK retry must retry the exact same Event rather than generate a new Event for the same sequence boundary.

### Child terminal ordering

Parent cannot durably claim `unit.phase == terminal` unless the corresponding Child terminal state is durably accepted.

### Recovery repairability

This partial state must be valid and recoverable:

```text
Child terminal
Parent unit accepted
```

### Parent await readiness

`ChildAwaitReady` may only be committed when the relevant plan has reached its required terminal condition.

### Cancellation convergence

When graph cancellation returns a stable boundary, descendants cannot remain indefinitely in untracked active state.

### Ownership uniqueness

One Child Session cannot belong to multiple Parent plans.

### No lock-order deadlock

Remaining graph coordination must not introduce Parent→Child / Child→Parent lock inversion.

---

## 21. Required Concurrency Tests

Use barriers/events in test Sinks and executors to force interleavings.

### Independent Child commit

```text
Child A Sink blocked
Child B emits Event
```

Expected after refactor:

```text
B does not wait for A solely because they share a Root.
```

### Child terminal / Parent marker crash gap

```text
Child terminal ACK
pause before Parent terminal marker
simulate recovery
```

Expected:

```text
recovery completes Parent terminal phase.
```

### Parent cancel vs Child completion

```text
Parent cancellation
||
Child completion
```

Expected:

```text
deterministic terminal result
no orphan task
no invalid phase transition
```

### Checkpoint vs Child commits

Expected semantics must be explicitly chosen: checkpoint either waits for a stable graph boundary or captures a formally valid recoverable partial protocol state.

---

## 22. Performance Acceptance Criteria

### Independent Child scaling

With Sink delay T, N independent Child events should no longer take approximately N×T solely because of one Root lock.

### Same-Session ordering unchanged

Concurrent transitions to one Session remain serialized and deterministic.

### Replay unchanged

Replay reconstructs the same logical RuntimeState.

### Recovery unchanged

All supported partial Parent/Child durable states remain recoverable.

### No new persistence coupling

Do not introduce storage-specific refs or distributed transaction logic into Core.

---

## 23. Final Recommendation

Treat the current Root lock as:

> **a correctness scaffold that is now becoming too coarse for a high-concurrency Runtime.**

Do not remove all Root-level synchronization.

The architectural target should be:

```text
                    Graph ownership
                          │
                 GraphCoordinator
              cancel/recover/checkpoint
                          │
        ┌─────────────────┴─────────────────┐
        ▼                                   ▼
   Session A                           Session B
   SessionLock A                       SessionLock B
        │                                   │
   Event stream A                       Event stream B
        │                                   │
   Sink append A                        Sink append B
```

instead of:

```text
                 Root Lock
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
     Session A   Session B   Session C
        │           │           │
     Sink A      Sink B      Sink C

     globally serialized
```

Design principle:

> **Parent/Child ownership requires durable causal coordination, not global execution serialization.**

More concretely:

> **Serialize one Session's Event stream. Coordinate graph-wide control operations explicitly. Allow independent Parent/Child/Sibling execution transitions to proceed concurrently.**

---

## 24. Primary Files for the Refactor

```text
autoagent/core/app/app.py
    _emit
    _runtime_lock
    _root_session_id
    _parent_plan
    _settle_child
    _cancel_graph
    _cancel_descendants
    _recover
    _recover_session
    _capture_checkpoint
    _unload_session
    root Invocation replacement logic

autoagent/core/runtime/repository.py
    _session_lock
    commit
    _settle_pending
    pending Event / ACK semantics

autoagent/core/executor/workflow_executor.py
    Child admission
    Child wait/failure convergence
    _wait_for_child_tasks_or_failure
    converge_failed_child_plan

autoagent/core/runtime/_execution_index.py
    child_remaining
    future child_failed_count

autoagent/core/runtime/state.py
    ChildInvocationPlan
    ChildUnitState

autoagent/core/runtime/events.py
    ChildInvocationPlanned
    ChildInvocationPhaseChanged
    ChildAwaitSuspended
    ChildAwaitReady
```

---

## 25. Non-Goals

Do not combine this refactor with redesigns of:

```text
RuntimeEvent serialization
EventStore format
checkpoint schema
DurableValueRef
ArtifactRef
distributed transactions
remote Runner protocol
```

The immediate problem is narrower:

> **Remove unnecessary Root-wide serialization while preserving the existing durable Parent/Child protocol and Session-local Event ordering.**
