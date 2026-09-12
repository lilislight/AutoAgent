# AutoAgent Core Runtime In-Memory Refactor Design

## 1. Document Purpose

This document defines the next refactor direction for the AutoAgent Core runtime.

The goal is not to redesign persistence, Sink encoding, durable references, or storage layout. The goal is to make **Core a high-performance, functionally correct, safe in-memory state machine**.

The central principle of this refactor is:

> **Core is an in-memory runtime. It should optimize its object model for execution, state transition, structural sharing, and correctness. Persistence must not force Core to duplicate values, encode/decode objects, freeze the same value repeatedly, or adopt storage-oriented reference models.**

Persistence is a downstream concern. A Sink may later choose JSON, MessagePack, Protobuf, content-addressed references, object stores, deduplication, compression, or any other representation. None of those concerns should distort the Core runtime object model.

---

## 2. Why This Refactor Is Needed

The current Core runtime already has a clean semantic model:

- `RuntimeEvent` describes a runtime fact.
- `StateDelta` describes a state transition.
- `RuntimeState` represents the materialized runtime state.
- `TransitionPlanner` derives transitions.
- `StateReducer` applies transitions.
- `RuntimeRepository` commits events and candidate state.
- EventStore / Sink observe committed runtime facts.

However, the current implementation still carries assumptions from a persistence-oriented immutable model.

A representative example is `OperatorCallCompleted`.

The same logical operator output can currently appear in several places:

```text
RuntimeEvent.payload.output
StateDelta.operations[*].value.output
RuntimeState.invocation.scheduler.operator_calls[*].output
```

Although these values are logically identical, they may become different Python objects because the runtime repeatedly performs operations such as:

```text
freeze
encode
freeze again
to_record
thaw
from_record
freeze again
```

This creates unnecessary container reconstruction and destroys object identity.

For a large nested output:

```python
output = {
    "records": [...],
    "metadata": {...},
}
```

the runtime should not create three separate immutable object graphs merely because the value passes through:

```text
Event -> Delta -> State
```

These layers have different **semantic responsibilities**, but they do not require different copies of the same in-memory value.

The current model therefore creates several problems.

### 2.1 Repeated value reconstruction

A value may be recursively copied/frozen multiple times during a single transition.

### 2.2 Whole-state reconstruction

If a reducer performs:

```python
state.to_record()
thaw(...)
apply(...)
RuntimeState.from_record(...)
```

then every event may reconstruct a large portion of the state tree even when only one leaf changed.

This turns a local state update into work proportional to the size of the whole runtime state.

### 2.3 Whole-object replacement

Some transitions replace a whole `OperatorCall`, `Occurrence`, or execution workspace even when only one field changed.

For example:

```python
updated_call = replace(
    call,
    status="completed",
    output=payload.output,
    completed_at_us=now,
)
```

and then replacing the entire call object causes unchanged data such as arguments, context, timing data, IDs, and metadata to be carried through the transition unnecessarily.

### 2.4 Serialization concerns leak into Core

`freeze`, `thaw`, record encoding, decoding, and storage-oriented representations influence live runtime behavior.

This is the wrong dependency direction.

The runtime representation should be designed for execution.

The storage representation should adapt to the runtime representation.

---

# 3. Core Design Goal

The Core should behave like a persistent in-memory object graph with aggressive structural sharing.

The desired model is:

```text
              one logical value
                     │
                     ▼
              Python object A
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
 RuntimeEvent     StateDelta   RuntimeState
   payload          value         field
```

All three locations may reference the **same Python object**.

For example:

```python
assert event.payload.output is delta.operations[0].value

assert delta.operations[0].value is (
    candidate.invocation.scheduler.operator_calls[call_id].output
)
```

Object identity is not required after serialization/replay across processes.

It **is** desirable during one live Core execution.

---

# 4. Core Responsibility Boundary

The runtime must explicitly distinguish three domains.

```text
External / User World
        │
        ▼
==========================
       CORE BOUNDARY
==========================
        │
        ▼
In-Memory Runtime Object Graph
        │
        ▼
==========================
       SINK BOUNDARY
==========================
        │
        ▼
Persistence / Serialization
```

The three boundaries have different rules.

---

## 4.1 Core-to-Core

Core-to-Core value movement should use normal Python object references.

Default rule:

> **Do not copy a value merely because it moves between Event, Delta, State, Scheduler, Planner, Reducer, or execution workspace structures.**

Examples:

```python
operation.value = event.payload.output
```

and:

```python
new_call.output = operation.value
```

should preserve identity.

No `freeze`.

No `deepcopy`.

No encode/decode.

No record conversion.

---

## 4.2 Core-to-User Code

User-defined code must not receive a live reference to published Core state when it is allowed to mutate its input.

For example, this is unsafe:

```python
user_mapping(runtime_state.some_value)
```

because user code may do:

```python
data["x"] = 1
```

and silently mutate historical runtime state or event data.

Therefore:

> **Mutable user-facing execution inputs must be isolated from Core-owned values.**

Default safe behavior:

```python
user_input = deepcopy(core_value)
result = user_function(user_input)
```

User code may freely mutate its private copy.

This applies to extension points such as:

- user-defined input mapping
- custom output binding
- user conditions if mutation is possible
- arbitrary user hooks
- custom operators receiving state-derived objects
- custom aggregation functions
- custom routing functions
- user-provided transformation functions

A later optimization may introduce trusted/read-only APIs, but the default contract should prioritize Core integrity.

---

## 4.3 User-Code-to-Core

When user code returns a value, the runtime should treat the returned object as an **ownership transfer**.

Example:

```python
result = await operator.execute(user_input)
```

After return:

```text
result
  │
  ▼
Core-owned runtime value
```

Default Core behavior should not recursively freeze or deep-copy this value again.

The contract is:

> **Once a value is returned to the runtime, user code must not mutate that object afterward.**

Python cannot fully enforce ownership transfer, so this is primarily an API contract.

If stronger isolation is required for untrusted plugins, it should be implemented as a separate boundary policy rather than forcing every Core transition to copy values.

---

# 5. Logical Immutability Instead of Defensive Immutability

The current design tends toward:

> every model protects itself by freezing its values again.

The new model should use:

> **logical immutability through ownership and runtime rules.**

A published runtime value may technically be represented by a normal Python `dict`, `list`, tuple, dataclass, or domain object.

What matters is:

> **Core must never mutate a published value in place.**

For example, this is forbidden:

```python
state.execution.output["status"] = "done"
```

Instead:

```python
new_output = {
    **state.execution.output,
    "status": "done",
}
```

The old object remains valid for:

- previous states
- previous events
- forks
- trace observation
- pending transitions
- concurrent readers

This preserves event-sourced semantics without forcing recursive freezing throughout the hot path.

---

# 6. Structural Sharing

Structural sharing is the primary performance model for Core state.

Suppose an operator output is:

```python
dict_a = {
    "records": [...],
    "metadata": {...},
}
```

A later output binding may create:

```python
bound_output = {
    "a": dict_a,
}
```

The correct behavior is:

```python
assert bound_output["a"] is dict_a
```

Only the new wrapper should be created.

The nested value should remain shared.

Conceptually:

```text
dict_a
  ▲
  │
  ├──────── OperatorCallCompleted.payload.output
  │
  ├──────── OperatorCallState.output
  │
  └──────── bound_output["a"]
```

If `bound_output` later becomes the occurrence output:

```python
occurrence.output = bound_output
```

then:

```python
assert occurrence.output is bound_output
assert occurrence.output["a"] is dict_a
```

No recursive copying is necessary.

---

# 7. Runtime Value Invariants

The following invariants should become explicit Core rules and should be covered by tests.

## 7.1 Reference propagation

Moving an existing value inside Core must preserve identity.

```python
assert delta_value is event_payload_value
```

and where applicable:

```python
assert state_value is delta_value
```

---

## 7.2 Wrapper-only allocation

When a transition wraps existing values:

```python
wrapped = {
    "result": existing_value,
}
```

only the wrapper is new.

```python
assert wrapped["result"] is existing_value
```

---

## 7.3 No defensive refreeze

Core dataclass construction, `replace()`, Planner operations, and Reducer operations must not recursively freeze already-Core-owned values merely because they cross a model boundary.

---

## 7.4 No mutation after publication

Once a value is stored in a `RuntimeEvent` or materialized `RuntimeState`, Core treats it as logically immutable.

No in-place changes.

---

## 7.5 Path-copy state updates

A state update should allocate only the changed object path.

Unchanged branches must preserve identity.

Example:

```text
old RuntimeState
│
├── session ---------------------------- SAME
│
└── invocation
     ├── input ------------------------- SAME
     └── scheduler
          ├── occurrences ------------- SAME
          └── operator_calls
               ├── call-A ------------- SAME
               ├── call-B ------------- SAME
               └── call-C
                    ├── args ----------- SAME
                    ├── context -------- SAME
                    └── output --------- NEW VALUE / SHARED EVENT VALUE
```

---

## 7.6 User mutation isolation

Values passed to arbitrary user code must not expose mutable aliases into published Core state.

---

# 8. RuntimeEvent Design

`RuntimeEvent` should remain a pure runtime domain object.

Its job is to represent a durable semantic fact in the runtime model.

It should not optimize itself for JSON, database rows, content-addressed storage, or Sink-specific representations.

Example:

```python
@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    event_id: EventId
    sequence: int
    occurred_at_us: int
    kind: RuntimeEventKind
    payload: RuntimeEventPayload
    delta: StateDelta
```

The event may hold references to objects also referenced by its Delta and candidate State.

That is valid and desirable in Core.

For example:

```python
output = payload.output

delta = StateDelta(
    operations=(
        Put(
            path=...,
            value=output,
        ),
    ),
)
```

should not transform `output`.

---

# 9. StateDelta and StateOperation Design

`StateDelta` should be intentionally simple.

It describes:

> what state path changes and what object should be placed there.

It does **not** need a storage-oriented value-source hierarchy such as:

```text
InlineValue
PayloadRef
StateRef
ArtifactRef
```

for Core execution.

A simple model is preferable:

```python
@dataclass(frozen=True, slots=True)
class Put:
    path: StatePath
    value: object
```

or equivalent typed forms.

Planner code can directly use:

```python
Put(
    path=("invocation", "scheduler", "operator_calls", call_id, "output"),
    value=payload.output,
)
```

Python object identity already provides the required in-memory reference behavior.

---

# 10. TransitionPlanner Refactor

The Planner should stop reconstructing complete state-domain objects when only a leaf changed.

Avoid patterns such as:

```python
updated_call = replace(
    call,
    status="completed",
    output=payload.output,
    completed_at_us=completed_at,
)

Put(
    path=("...", "operator_calls", call_id),
    value=updated_call,
)
```

Prefer leaf-level operations:

```python
Put(
    path=("...", "operator_calls", call_id, "status"),
    value=CallStatus.COMPLETED,
)

Put(
    path=("...", "operator_calls", call_id, "output"),
    value=payload.output,
)

Put(
    path=("...", "operator_calls", call_id, "completed_at_us"),
    value=completed_at,
)
```

Benefits:

- less allocation
- less copying
- smaller transition footprint
- easier debugging
- more precise event semantics
- better structural sharing
- lower memory pressure

Whole-object replacement should only be used when the whole object is genuinely created or semantically replaced.

---

# 11. Reducer Refactor

The Reducer is one of the most important parts of this optimization.

The hot path must not perform:

```text
RuntimeState
    ↓ to_record
record tree
    ↓ thaw
mutable tree
    ↓ apply
modified tree
    ↓ from_record
RuntimeState
```

This destroys structural sharing and makes transition cost proportional to the size of the entire state.

The new Reducer should operate directly on the typed runtime object graph.

Desired behavior:

```python
new_state = persistent_set(
    old_state,
    path,
    value,
)
```

where `persistent_set` reconstructs only the path from the root to the changed leaf.

For a nested update:

```text
RuntimeState
  -> invocation
  -> scheduler
  -> operator_calls
  -> call_id
  -> output
```

only those containing objects should be replaced.

Every unrelated branch remains the exact same object.

---

# 12. RuntimeState Design

`RuntimeState` should behave as a persistent snapshot.

A new state version does not mean:

> duplicate the whole previous state.

It means:

> create a new root containing references to old unchanged branches plus new changed branches.

This allows:

- cheap snapshots
- cheap event commit
- cheap forks
- safe historical references
- efficient trace inspection
- concurrent read access
- lower GC pressure

The performance target should be approximately:

```text
O(depth of changed path)
```

for a normal leaf update, rather than:

```text
O(size of RuntimeState)
```

---

# 13. `freeze()` Refactor

`freeze()` should no longer be part of ordinary Core value movement.

The existing pattern:

```text
payload construction -> freeze
operation construction -> freeze
state construction -> freeze
```

should be removed.

The Core should not ask:

> "Has this object crossed another model boundary?"

It should ask:

> "Is this object newly entering an unsafe ownership boundary?"

Only true boundary transitions may justify copying, validation, normalization, or freezing.

Potential remaining uses of `freeze()`:

- compatibility APIs
- external ingestion
- explicitly requested read-only structures
- debugging or validation mode
- certain trusted immutable domain values

It should not be automatically invoked merely because:

- a RuntimeEvent is created
- a StateOperation is created
- a dataclass is replaced
- a candidate RuntimeState is built

---

# 14. User-Defined Function Safety

Removing repeated freezing must not make user extensions capable of corrupting runtime history.

Therefore the Core must clearly classify execution paths.

## 14.1 Internal trusted transformation

Example:

```python
bound = {
    "a": operator_output,
}
```

This may reuse references directly.

---

## 14.2 Arbitrary user transformation

Example:

```python
result = user_output_binding(data)
```

The user function receives isolated input:

```python
user_data = deepcopy(data)
result = user_output_binding(user_data)
```

Its returned result becomes a new Core-owned value.

---

## 14.3 Read-only trusted internal helpers

Internal runtime functions may receive live references if their contract guarantees no mutation.

These functions must be treated as part of Core and tested accordingly.

---

# 15. Concurrency Safety

Reference sharing is safe only if published Core values are not mutated.

This is especially important because AutoAgent may execute multiple nodes/operators concurrently.

Example:

```text
               shared state value A
                  /          \
                 /            \
          Node execution 1   Node execution 2
```

Neither user execution should receive direct mutable access to `A`.

Instead:

```text
                     A
                  /     \
             deepcopy  deepcopy
               /           \
          user copy B    user copy C
```

Core readers may share `A`.

User code mutates only B or C.

This allows aggressive internal sharing without making concurrent execution unsafe.

---

# 16. Serialization and Persistence Boundary

Serialization functionality is still required.

What changes is **where responsibility lives**.

Core objects should not be shaped around persistence needs.

Avoid making runtime domain objects responsible for storage policy through APIs such as:

```python
event.to_json()
event.to_record()
RuntimeEvent.from_record(...)
```

The exact API can be decided later, but the direction should be:

```python
codec.encode(event)
codec.decode(data)
```

or:

```python
serializer.serialize(event)
serializer.deserialize(data)
```

Possible implementations may include:

```text
JsonRuntimeEventCodec
MessagePackRuntimeEventCodec
DatabaseEventCodec
CheckpointCodec
```

A Sink may additionally choose to:

- deduplicate repeated values
- replace repeated objects with references
- use content hashes
- move large payloads to artifact storage
- compress data
- persist Delta differently from Payload
- persist only selected representations

None of this should affect Core object sharing.

---

# 17. Sink Contract

The Sink receives the Core runtime event.

Conceptually:

```text
Core RuntimeEvent
      │
      ├── InMemorySink
      │      └── keep object reference
      │
      ├── JsonSink
      │      └── encode
      │
      ├── DatabaseSink
      │      └── normalize / deduplicate
      │
      └── ArtifactSink
             └── large-value references
```

If a Sink sees that:

```python
event.payload.output is event.delta.operations[x].value
```

it is free to encode that relationship however it wants.

Core does not need to know.

---

# 18. What This Refactor Does NOT Introduce

This refactor should deliberately avoid introducing storage-oriented complexity into Core.

Do not introduce, unless required by a separate future design:

```text
PayloadRef
StateRef
DurableValueRef
ArtifactRef
ValueStore
content-addressed RuntimeState
hash-based Core deduplication
storage-specific reference resolution
```

Those mechanisms may later be useful in persistence infrastructure.

They are not required to achieve efficient in-memory Core sharing because Python already uses object references.

---

# 19. Recommended Module-Level Refactor

The exact file structure may change, but the refactor should roughly follow this order.

## 19.1 `values.py`

Review the purpose of:

```text
freeze
thaw
encode
decode
```

Goals:

- remove recursive freeze from Core hot paths
- stop rebuilding values that already belong to Core
- separate boundary normalization from ordinary value propagation
- remove serialization-oriented transformations from state transition logic

Potential result:

```text
values.py
├── boundary copy / detach helpers
├── validation helpers
└── optional compatibility helpers
```

rather than being used as a mandatory transformation layer.

---

## 19.2 `operations.py`

Goals:

- `StateOperation.value` directly holds the Python object
- no automatic `_encode_runtime()`
- no automatic recursive freeze
- operation application preserves identity
- leaf-level operations are preferred

---

## 19.3 `transitions.py`

Goals:

- remove whole-call / whole-occurrence / whole-workspace replacement where unnecessary
- emit precise leaf operations
- reuse Event payload objects directly
- wrapping creates only wrappers
- no unnecessary normalization

---

## 19.4 `reducer.py`

Highest-priority performance work.

Goals:

- remove `to_record -> thaw -> from_record` from the live transition path
- implement typed persistent path-copy updates
- preserve identity for unchanged branches
- preserve identity for inserted values
- make normal update cost proportional to changed-path depth

---

## 19.5 `state.py`

Goals:

- stop `__post_init__` or constructors from defensively refreezing Core-owned values
- make state objects suitable for structural sharing
- preserve strongly typed runtime structures
- ensure historical states are never mutated in place

---

## 19.6 `events.py`

Goals:

- event payload values remain native Core Python objects
- no persistence-specific representation requirements
- no unnecessary copies during event construction
- RuntimeEvent remains a domain/runtime object

---

## 19.7 Repository / EventStore

Goals:

- continue sharing the same Event object through the live commit path where possible
- avoid copying events merely for observation
- distinguish runtime commit semantics from durable persistence encoding

---

# 20. Runtime Commit Model After Refactor

A normal transition should look approximately like this:

```text
1. Operator/User execution returns object A
                  │
                  ▼
2. RuntimeEvent.payload.output ─────── A
                  │
                  ▼
3. Planner creates Delta
      operation.value ──────────────── A
                  │
                  ▼
4. Reducer path-copies RuntimeState
      candidate....output ──────────── A
                  │
                  ▼
5. Repository commits Event + candidate State
                  │
                  ▼
6. Sink decides how to persist Event
```

At no point between steps 2 and 5 should `A` be recursively copied simply because it passes through another Core abstraction.

---

# 21. Example: Operator Completion

Input:

```python
output = {
    "text": "...",
    "metadata": {
        "tokens": 123,
    },
}
```

Event:

```python
payload.output = output
```

Delta:

```python
Put(
    path=(
        "invocation",
        "scheduler",
        "operator_calls",
        call_id,
        "output",
    ),
    value=payload.output,
)
```

Candidate state:

```python
candidate_call.output = operation.value
```

Required invariant:

```python
assert payload.output is output
assert operation.value is output
assert candidate_call.output is output
```

---

# 22. Example: Output Binding

Suppose:

```python
operator_output = A
```

The binding creates:

```python
bound_output = {
    "answer": A,
}
```

Required behavior:

```python
assert bound_output is not A
assert bound_output["answer"] is A
```

If this wrapper is then stored in state:

```python
assert candidate.execution.bound_output is bound_output
assert candidate.execution.bound_output["answer"] is A
```

No recursive freeze or deep copy occurs.

---

# 23. Example: User Mapping

Core state:

```python
source = state.execution.operator_output
```

Unsafe:

```python
result = user_mapping(source)
```

Safe default:

```python
user_input = deepcopy(source)
result = user_mapping(user_input)
```

If user code mutates:

```python
user_input["x"] = "changed"
```

then:

```python
assert source != user_input
```

and all previously published RuntimeEvent / RuntimeState data remains unchanged.

The returned result is treated as a new Core-owned value.

---

# 24. Testing Strategy

This refactor needs more than value-equality tests.

It requires **identity tests**, **mutation isolation tests**, and **structural-sharing tests**.

---

## 24.1 Event → Delta identity

```python
assert event.payload.output is delta.operations[x].value
```

---

## 24.2 Delta → State identity

```python
assert delta.operations[x].value is candidate....output
```

---

## 24.3 Wrapper structural sharing

```python
assert bound_output["a"] is operator_output
```

---

## 24.4 Unchanged branch sharing

```python
assert new_state.session is old_state.session
assert new_state.invocation.input is old_state.invocation.input
assert new_state....unmodified_call is old_state....unmodified_call
```

---

## 24.5 Previous-state immutability

After a transition:

```python
assert old_state == original_old_state
```

No previous branch may be mutated in place.

---

## 24.6 User mutation isolation

A malicious or careless user function should not mutate Core state:

```python
def user_fn(data):
    data["corrupt"] = True
    return data
```

After execution:

```python
assert "corrupt" not in original_core_value
```

---

## 24.7 Concurrency isolation

Two concurrent user functions receiving data derived from the same Core value should not observe each other's mutations.

---

## 24.8 Replay correctness

Serialization/replay does not need to preserve Python object identity.

It must preserve logical value equality and runtime semantics.

Therefore:

```python
replayed_value == original_value
```

is required.

```python
replayed_value is original_value
```

is not.

---

# 25. Performance Benchmarks

The refactor should add focused benchmarks.

Suggested measurements:

### Event commit latency

Measure:

```text
plan -> reduce -> repository commit
```

for representative events.

### State-size scaling

Run the same leaf update against progressively larger RuntimeState graphs.

Desired result:

> leaf-update cost grows primarily with path depth, not total RuntimeState size.

### Large output propagation

Use a large nested output and measure:

```text
OperatorCallCompleted
OutputBound
NodeCompleted
```

Track:

- execution time
- peak allocations
- number of copied containers
- GC pressure

### Identity checks

Ensure large nested output remains one shared object through Core transitions whenever no semantic transformation occurs.

---

# 26. Safety Model Summary

The refactor does **not** mean:

> mutate everything freely because Python uses references.

It means:

> aggressively share references inside Core while strictly controlling mutation ownership.

The safety model is:

```text
Core published value
      │
      ├── Core read/reference -------- zero-copy
      │
      ├── Core wrapping -------------- shallow new wrapper
      │
      ├── Core state update ---------- persistent path copy
      │
      └── User mutable execution ----- isolated copy
```

This provides both performance and semantic safety.

---

# 27. Non-Goals

This refactor is not responsible for:

- database schema design
- JSON representation
- Event storage deduplication
- durable value references
- artifact storage
- cross-process object identity
- compression
- Sink implementation details
- checkpoint file formats

Those systems may be designed later on top of a clean Core.

---

# 28. Acceptance Criteria

The refactor is considered successful when the following properties hold.

### Functional

- Event replay remains correct.
- Fork/recovery semantics remain correct.
- RuntimeState transitions remain deterministic.
- Existing semantic event boundaries remain intact.

### Memory / identity

- A value does not get copied merely because it moves Event → Delta → State.
- Wrapping an existing value preserves child references.
- Unchanged RuntimeState branches preserve object identity.

### Mutation safety

- Core never mutates previously published values in place.
- Arbitrary user code cannot mutate historical Core state through shared aliases.

### Architecture

- Core transition logic does not depend on persistence representation.
- Serialization is treated as an adapter/codec/Sink concern.
- RuntimeEvent remains a runtime domain object.
- StateReducer no longer rebuilds the entire state through record serialization on every event.

### Performance

- Common leaf updates scale with changed-path depth.
- Large operator outputs can flow through Core without recursive copying.
- Allocation count and GC pressure are significantly reduced.

---

# 29. Final Design Principle

The Core runtime should be designed around the following statement:

> **AutoAgent Core is a high-performance in-memory state machine. Values move through the runtime as Python object references. Published values are logically immutable, state changes use persistent structural sharing, and arbitrary user code is isolated at trust boundaries. Persistence adapts to Core; Core does not adapt to persistence.**

This principle should guide the implementation decisions in `values`, `events`, `operations`, `transitions`, `state`, `reducer`, `repository`, and future runtime modules.
