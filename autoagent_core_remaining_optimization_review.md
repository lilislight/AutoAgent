# AutoAgent Core Remaining Design / Performance Review

> Scope: `refactor` branch, `autoagent/core` only.  
> This document intentionally excludes Sink implementation, Event persistence format, batching/group-commit, database/storage layout, and Host/Tracing concerns.
>
> The purpose is to record a small set of remaining Core-side issues that are worth checking or implementing after the current in-memory performance refactor.

---

## 1. Current conclusion

The recent Core refactor has already addressed most of the previously identified high-cost in-memory paths:

- Runtime values are Core-owned immutable values and are shared by reference internally.
- `StateDelta` application no longer serializes/deserializes the entire `RuntimeState`.
- Large runtime mappings use structural sharing through `ChunkedMap`.
- Large Child unit collections use `ChunkedUnits`.
- Context conflict queries have a derived revision index.
- Runtime execution has a rebuildable `ExecutionIndex`.
- Loop scope queries can partially consume execution indexes.
- Sync execution workers/notifiers are reused.

Therefore, the remaining items in this document are **not another large architectural rewrite**. They are targeted improvements around:

1. derived runtime indexes not yet being used consistently;
2. WorkflowIR still leaving some static loop topology derivation to runtime;
3. `SessionState.updated_at_us` being represented as an explicit State mutation even though Event time already exists.

---

# 2. Issue A — RuntimeDerivedIndex is not yet the common query layer

## 2.1 Current design

Core already maintains a rebuildable execution-side index in:

```text
autoagent/core/runtime/_execution_index.py
```

Current `ExecutionIndex` contains data such as:

```text
child_remaining
started_count
occurrence_counts
waiting_count
running
occurrences_by_scope
calls_by_occurrence
waits_by_occurrence
```

The index is updated only after a Runtime Event has been acknowledged and the candidate State becomes the committed State.

This is a good property:

```text
RuntimeState = canonical state
ExecutionIndex = derived / rebuildable / non-durable state
```

Checkpoint installation can rebuild the index from `RuntimeState`, so it does not change replay semantics or durable schemas.

---

## 2.2 Remaining problem

Several Core components still query `RuntimeState` directly by scanning runtime collections even though equivalent information could be obtained from a derived index.

Representative examples currently exist in `runtime/transitions.py`.

### WaitRequested

To decide whether an Invocation should move to `waiting`, the planner may perform logic equivalent to:

```python
any(
    occurrence.id != current_occurrence_id
    and occurrence.status in {"ready", "running"}
    for occurrence in scheduler.occurrences.values()
)
```

This is proportional to the total number of retained occurrences.

### ChildAwaitSuspended

A similar active-work check is used when deciding whether the parent Invocation becomes waiting.

### Node completion / terminal convergence

Some branches still need to inspect existing occurrence statuses when no newly-ready work was produced.

Previous optimization work removed several unnecessary scans, but not every status/convergence query has moved to indexed lookup.

---

## 2.3 Why this matters

For small workflows this cost is negligible.

The problem appears when Runtime State contains a large historical number of occurrences:

```text
loops
large fan-out
large DAG
long-lived invocation
```

The question being asked is usually small:

```text
"Is there any ready/running occurrence?"
"How many waiting occurrences exist?"
"Does this loop scope still contain active work?"
```

but the current implementation can answer it by scanning:

```text
O(total occurrences)
```

instead of:

```text
O(1)
or
O(affected scope)
```

This becomes particularly undesirable because the Core already pays the memory cost of maintaining derived indexes.

---

## 2.4 Proposed direction

Generalize `ExecutionIndex` into a more explicit internal concept such as:

```text
RuntimeDerivedIndex
```

The exact name is optional.

Its responsibility should be:

> Maintain all frequently queried, rebuildable runtime projections that do not belong in canonical RuntimeState.

Possible fields:

```python
class RuntimeDerivedIndex:
    started_count: int

    occurrence_counts: dict[NodeOccurrenceStatus, int]

    ready_count: int
    running_count: int
    waiting_count: int
    failed_count: int

    active_count: int

    running: dict[str, None]

    occurrences_by_scope: ...
    calls_by_occurrence: ...
    waits_by_occurrence: ...

    child_remaining: dict[str, int]
```

Some of these are already present or derivable from `occurrence_counts`; the implementation should avoid storing redundant counters unless measurement shows value.

The important design change is not adding many fields. It is making this index the **shared runtime query surface** for:

```text
WorkflowExecutor
TransitionPlanner
Scheduler / LoopScheduler
Child lifecycle convergence
```

instead of treating it mainly as an Executor optimization.

---

## 2.5 Example optimization

Current conceptual logic:

```python
if not any(
    occurrence.id != oid
    and occurrence.status in {"ready", "running"}
    for occurrence in scheduler.occurrences.values()
):
    invocation -> waiting
```

Possible indexed logic:

```python
active = index.ready_count + index.running_count

if current_occurrence_is_running:
    active -= 1

if active == 0:
    invocation -> waiting
```

The real implementation must account for the **candidate Delta**, because the index describes committed State and must not be mutated before ACK.

Therefore the planner should use:

```text
committed index
+
local transition effects
=
candidate query result
```

rather than prematurely updating the global index.

---

## 2.6 Correctness constraints

Any implementation must preserve the following rules:

### Canonical ownership

`RuntimeState` remains the source of truth.

The index must never be required for replay correctness.

### ACK ordering

Derived indexes must only be published after the corresponding candidate State is committed.

Current Repository behavior already follows this model.

### Checkpoint/recovery

Installing a RuntimeState must be enough to reconstruct the complete index.

### Custom Planner / Scheduler compatibility

If a custom component can produce State changes that the incremental index updater does not understand, the safe fallback is:

```text
rebuild index from RuntimeState
```

rather than attempting an unsafe incremental update.

### Delta-local reasoning

Planner decisions that depend on the State *after* the current semantic transition must combine:

```text
committed index + current local changes
```

and must not simply query the pre-transition counter.

---

## 2.7 Suggested implementation order

1. Inventory all scans over:
   - `scheduler.occurrences`
   - `operator_calls`
   - `waits`
   - `child_plans`

2. Classify each scan:
   - genuinely needs all items;
   - asks only for count/existence;
   - asks only for one occurrence scope;
   - abnormal/recovery-only cold path.

3. Move only hot existence/count/scope queries to `RuntimeDerivedIndex`.

4. Keep exceptional recovery/validation scans when their frequency is low.

5. Add differential tests:

```text
indexed decision == full State scan decision
```

for randomized RuntimeState + Delta combinations.

---

## 2.8 Benchmark cases

At minimum test:

```text
100 / 1,000 / 10,000 / 50,000 retained occurrences
```

for:

- WaitRequested;
- ChildAwaitSuspended;
- NodeCompleted with no new ready node;
- Invocation waiting convergence;
- Loop scope active-work query.

Measure both:

```text
planner latency
index resident memory
```

Do not introduce additional indexes if they only improve synthetic large states while regressing ordinary workflows materially.

---

# 3. Issue B — Static Loop topology is still partially recomputed at runtime

## 3.1 Current design

`WorkflowIR` already precomputes useful indexes during compilation / IR construction:

```text
_nodes
_edges
_incoming
_outgoing
_loops
```

This is correct because these relationships depend only on the Workflow definition.

However, several loop-topology queries still iterate over `workflow.loop_regions` at runtime.

Current methods include:

```python
WorkflowIR.containing_loops(node_id)
WorkflowIR.back_loop(edge_id)
WorkflowIR.entry_loops(edge_id)
WorkflowIR.exit_loops(edge_id)
```

Conceptually they perform operations such as:

```python
[
    loop
    for loop in self.loop_regions
    if node_id in loop.node_ids
]
```

or:

```python
next(
    loop
    for loop in self.loop_regions
    if edge_id in loop.back_edge_ids
)
```

`LoopScheduler` calls these helpers while resolving transitions.

---

## 3.2 Why this is unnecessary runtime work

Loop topology is immutable for a `WorkflowIR`.

The following facts are all fully knowable when WorkflowIR is built:

```text
which loops contain node X
which loop owns back edge E
which loops use E as entry
which loops use E as exit
which loops have header node H
parent / child loop relationship
loop nesting depth
loop ancestors
node membership sets
```

Therefore repeatedly deriving this information during scheduling is unnecessary.

The general rule should be:

> If a relationship depends only on Workflow definition and not Runtime State, compute it once in Compiler / WorkflowIR construction.

---

## 3.3 Proposed indexes

`WorkflowIR` can extend its private indexes with structures similar to:

```python
_loops_by_node:
    Mapping[str, tuple[LoopRegionIR, ...]]

_back_loop_by_edge:
    Mapping[str, LoopRegionIR]

_entry_loops_by_edge:
    Mapping[str, tuple[LoopRegionIR, ...]]

_exit_loops_by_edge:
    Mapping[str, tuple[LoopRegionIR, ...]]

_loops_by_header:
    Mapping[str, tuple[LoopRegionIR, ...]]

_loop_depth:
    Mapping[str, int]

_loop_ancestors:
    Mapping[str, tuple[LoopRegionIR, ...]]

_loop_node_sets:
    Mapping[str, frozenset[str]]
```

Not all structures are necessarily needed. Implement only the ones consumed by hot scheduler logic.

---

## 3.4 API behavior can remain unchanged

Public/internal callers should not need to know whether a result comes from scanning or an index.

For example:

```python
def containing_loops(self, node_id: str) -> tuple[LoopRegionIR, ...]:
    return self._loops_by_node.get(node_id, ())
```

```python
def back_loop(self, edge_id: str) -> LoopRegionIR | None:
    return self._back_loop_by_edge.get(edge_id)
```

```python
def entry_loops(self, edge_id: str) -> tuple[LoopRegionIR, ...]:
    return self._entry_loops_by_edge.get(edge_id, ())
```

```python
def exit_loops(self, edge_id: str) -> tuple[LoopRegionIR, ...]:
    return self._exit_loops_by_edge.get(edge_id, ())
```

This keeps Scheduler code simple.

---

## 3.5 Avoid repeated set construction

Some loop scheduling logic also performs comparisons conceptually similar to:

```python
set(region.node_ids) < set(owner.node_ids)
```

during runtime.

If membership/subset operations are needed repeatedly, use a precomputed:

```python
frozenset(region.node_ids)
```

or preferably precompute the semantic relationship itself:

```text
loop ancestors
loop descendants
```

Then Scheduler code can ask:

```python
owner.id in workflow.loop_ancestors(region.id)
```

rather than infer hierarchy from node-set subset relationships at runtime.

The compiler already knows `parent_loop_region_id`, so direct ancestry indexing is usually preferable to repeated set comparisons.

---

## 3.6 Benefits

### Lower scheduler CPU

Loop transition work scales with the number of loops actually relevant to a node/edge rather than total loop regions.

### Simpler LoopScheduler

Less topology inference remains inside runtime control logic.

### Better architectural boundary

```text
Compiler / WorkflowIR
    owns static topology

Scheduler
    owns dynamic execution state
```

This is cleaner than mixing both responsibilities.

---

## 3.7 Correctness constraints

Indexes must preserve the existing ordering semantics.

For example, `containing_loops()` currently sorts loops by:

```text
(-len(node_ids), loop.id)
```

If caller behavior relies on that ordering, the precomputed tuple must preserve exactly the same result.

Likewise:

```text
entry_loops
exit_loops
```

must retain deterministic order.

Compiler validation should reject contradictory topology rather than allowing runtime indexes to silently overwrite entries.

For example, if the design guarantees one back-edge owner:

```python
_back_loop_by_edge[edge_id]
```

is valid.

If multiple owners are possible, the representation must remain a tuple.

---

## 3.8 Suggested implementation

1. Add private WorkflowIR indexes in `__post_init__`.
2. Keep existing query methods and change only their implementation.
3. Replace runtime set construction with precomputed ancestry/membership where useful.
4. Run all Loop tests unchanged.
5. Add index-vs-old-scan differential tests.
6. Benchmark nested loop workflows separately from ordinary DAGs.

---

## 3.9 Benchmark cases

Construct workflows with:

```text
10 loops
100 loops
1,000 loop regions
deeply nested loops
many sibling loops
```

Measure:

- `_target_scope`;
- `_active_boundary_regions`;
- `_expected_incoming`;
- `_validate_control`;
- complete/fail scheduling of repeated loop iterations.

Also measure WorkflowIR construction cost and memory.

This optimization intentionally trades a small amount of immutable WorkflowIR memory for lower repeated scheduler work.

---

# 4. Issue C — `SessionState.updated_at_us` is modeled as an explicit State Operation on every Event

## 4.1 Current behavior

`RuntimeEvent` already stores:

```python
occurred_at_us
sequence
```

Every committed semantic event therefore already contains the time at which the event occurred.

At the same time, `SessionState` stores:

```python
created_at_us
updated_at_us
```

and `TransitionPlanner.plan()` adds a mutation equivalent to:

```python
put(
    ("session", "updated_at_us"),
    occurred_at_us,
)
```

for almost every Invocation-level Runtime Event.

This produces an additional State Operation even when the semantic event has no meaningful Session-level mutation.

---

## 4.2 Why this is questionable

`SessionState.updated_at_us` currently means approximately:

> wall-clock timestamp of the latest committed Runtime Event for this Session.

But this information is already associated with the Event itself.

Therefore `updated_at_us` is arguably **Event metadata projected into Runtime State**, rather than domain state that needs to be independently described by a Delta.

The cost per Event is small, but it is paid for nearly every Event:

```text
extra StateOperation
extra path tuple
extra integer value
extra SessionState replace
extra encoded Delta field at external serialization boundary
```

This is a high-frequency fixed tax.

---

## 4.3 Important semantic question

Before changing it, define what `SessionState.updated_at_us` actually means.

Possible meanings:

### A. Latest Runtime Event wall time

If this is the intended meaning:

```text
updated_at_us = latest_event.occurred_at_us
```

and it does not need to exist as an explicit semantic Delta operation.

### B. Latest business/context mutation time

If `updated_at_us` means something narrower than "latest event", then the current behavior is already semantically misleading because every runtime stage updates it.

The Core should first choose one meaning.

The current implementation behaves like **A**.

---

## 4.4 Preferred optimization

Keep `updated_at_us` in materialized State if convenient for public status queries, but make it Reducer-maintained Event metadata rather than Planner-generated Delta.

Conceptually:

```python
candidate = apply_runtime_delta(state, event.delta)

candidate = replace(
    candidate,
    sequence=event.sequence,
    last_event_id=event.id,
    session=replace(
        candidate.session,
        updated_at_us=event.occurred_at_us,
    ),
)
```

The exact implementation should avoid unnecessary Session replacement when processing the SessionOpened event or invalid intermediate State.

This creates a useful distinction:

```text
Delta
    semantic State mutation

Reducer metadata application
    sequence
    last_event_id
    latest event timestamp
```

`sequence` and `last_event_id` already follow this pattern.

`updated_at_us` naturally belongs in the same category if its meaning is "latest event time".

---

## 4.5 Alternative

Remove `SessionState.updated_at_us` entirely and expose it through repository/event metadata.

This is architecturally cleaner but causes broader API/schema changes and may make reading a Session summary require retaining additional metadata outside State.

Therefore the lower-risk option is:

> keep the State field, stop representing it as a Delta operation.

---

## 4.6 Recovery and replay behavior

Replay must still produce the same value.

This is straightforward because each RuntimeEvent already persists:

```text
occurred_at_us
```

Reducer replay can deterministically set:

```text
SessionState.updated_at_us = event.occurred_at_us
```

No user code or wall-clock access is needed during replay.

---

## 4.7 Wall clock ordering

Core already documents that wall time can regress and Event sequence establishes execution order.

Therefore:

```text
updated_at_us
```

must not be interpreted as a monotonic logical clock.

Moving the value into Reducer metadata handling does not change this property.

Do not implement:

```python
max(previous_updated_at_us, event.occurred_at_us)
```

unless the semantic definition is intentionally changed.

Replay should reproduce the recorded timestamp, not invent monotonic wall time.

---

## 4.8 Validation changes

Currently State validation verifies timestamp shape but does not use wall time for execution ordering.

After this change tests should verify:

```text
State.updated_at_us == last applied Event.occurred_at_us
```

for any non-empty Session event prefix.

Also test intentionally regressing wall-clock timestamps:

```text
event sequence 10: occurred_at_us = 1000
event sequence 11: occurred_at_us = 900
```

Expected:

```text
state.sequence == 11
state.updated_at_us == 900
```

if current semantics are preserved.

---

## 4.9 Expected impact

This is not expected to produce a dramatic benchmark improvement.

Its value is:

- removing a universal redundant Delta operation;
- reducing Event/Delta noise;
- making metadata ownership clearer;
- making `StateDelta` more purely semantic.

Treat this as a cleanup / constant-factor optimization rather than a major performance project.

---

# 5. Items explicitly not included in this document

The following were reviewed but are intentionally not proposed as current Core refactors.

## Runtime Event Sink / persistence batching

Once Core hands a RuntimeEvent to a Sink boundary, batching, group commit, serialization format, database transactions, storage deduplication and other persistence behavior belong to the later Host/storage design.

Do not change current Core semantics merely to optimize a future Sink implementation unless a separate architecture decision explicitly changes that boundary.

---

## OperatorCall output duplication in live Core

Current Core ownership rules are designed so that a logical Operator output is normally represented by one Core-owned immutable Python object graph.

During live execution, references may exist simultaneously from:

```text
OperatorCallCompleted.payload.output
StateDelta operation value
OperatorCallState.output
NodeOccurrence.output
Context wrappers
```

but these are references to the same owned value where the execution path permits it.

For non-aggregated Map output, the final collection creates a new tuple/wrapper, while each item remains a reference to the corresponding accepted Call output.

Therefore:

> Multiple references must not be treated as multiple copies or an in-memory duplication bug.

Any future optimization in this area should first prove actual object reconstruction or allocation using identity/memory tests.

---

## Checkpoint encoding optimization

Checkpoint is not part of normal Node/Event execution.

In standalone Core it is mainly used for lifecycle operations such as:

```text
unload_session()
close() -> AppCheckpoint
load_checkpoint()
```

Therefore expensive checkpoint validation/encoding is a cold-path concern under the current standalone execution model.

It can be optimized later if:

- unload/load becomes frequent;
- checkpoint size becomes operationally significant;
- Core is reused in a host where checkpointing becomes periodic.

It should not currently outrank hot scheduler/runtime work.

---

# 6. Recommended priority

Recommended order for another agent:

## Priority 1

Unify runtime count/existence/scope queries behind the derived runtime index.

Reason:

```text
already has index infrastructure
low semantic risk
directly removes remaining RuntimeState scans
```

## Priority 2

Move static loop topology queries into WorkflowIR precomputed indexes.

Reason:

```text
purely static information
easy to validate
simplifies Scheduler responsibility
```

## Priority 3

Remove explicit `session.updated_at_us` mutation from StateDelta and apply it as Event metadata in Reducer.

Reason:

```text
small but universal cost
cleaner StateDelta semantics
low conceptual complexity
```

---

# 7. Definition of done

An implementation should not be accepted only because tests pass.

For each optimization require:

### Correctness

- existing Core tests pass;
- event replay produces identical RuntimeState;
- checkpoint round-trip remains valid;
- old State objects remain immutable;
- ACK-before-publication behavior remains unchanged.

### Differential verification

Where possible implement test-only reference logic using the previous full scan and assert:

```text
optimized result == reference result
```

across randomized states/workflows.

### Performance

Provide before/after measurements for both:

```text
large synthetic case
ordinary small workflow
```

Avoid accepting an index that improves a 50k-item synthetic State but materially regresses normal 5–50 node workflows.

### Memory

Report resident/index memory separately from transient peak allocations.

A faster query is not automatically better if it duplicates a large portion of RuntimeState.

---

# 8. Architectural principle

The three issues in this document follow one common rule:

```text
Static workflow facts
    -> WorkflowIR / Compiler indexes

Dynamic canonical execution facts
    -> RuntimeState

Frequently queried projections
    -> rebuildable RuntimeDerivedIndex

Event identity / ordering / occurrence time
    -> RuntimeEvent + Reducer metadata
```

Maintaining these boundaries should be preferred over adding more canonical RuntimeState fields or letting Scheduler repeatedly derive information that another Core layer already knows.
