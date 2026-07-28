# Hook Contracts

This reference owns user-defined callable phases and Context behavior. It does
not define graph topology or Policy configuration.

## Typed callable Operator

A project function added directly as a Node becomes a direct Operator:

```python
def calculate_total(order: Order) -> Invoice:
    ...
```

AutoAgent validates named inputs against the signature and validates the
return value against its annotation. Both synchronous and asynchronous
callables are supported.

Operator handler exceptions, timeout, and invalid output may enter Node Retry
or capability fallback. Mapping, selection, aggregation, Condition, and Output
Binding failures do not.

## Shared read view

Read-only Hook contexts expose:

- `invocation_input`: original Invocation input mapping;
- `invocation_context`: isolated Invocation Context snapshot;
- `session_context`: isolated Session Context snapshot;
- `outputs`: indexed historical Node outputs.

`outputs.latest(node_id, default)` returns the latest visible output.
`outputs.all(node_id)` returns visible outputs for repeated executions. Prefer
`incoming` for the exact activation feeding the current Node.

Snapshots protect authoritative Runtime Context. Mutating a nested object in a
read Hook cannot commit it back to Runtime.

## Input Mapping

Signature:

```python
def map_input(ctx: InputMappingContext) -> dict[str, object]:
    ...
```

Additional fields:

- `node_id`;
- `incoming`: ordered `IncomingOutput` records with `edge_id`,
  `source_node_id`, `source_execution_id`, and `value`.

Return a mapping whose keys are target callable parameter names. AutoAgent
copies it before execution.

Use `incoming` to distinguish Loop external entries from back Edges. An Input
Mapping failure marks the Node failed and bypasses Operator Retry/fallback.

## Edge Condition

Signature:

```python
def select_edge(ctx: ConditionContext) -> bool:
    ...
```

Additional fields:

- `edge_id`;
- `source_node_id`;
- `target_node_id`;
- `source_output`.

Return a boolean. Use the Condition only for routing. Do not reshape target
arguments or write Context here. A Condition exception fails the Invocation.

## Map item selector

Signature:

```python
def select_items(
    ctx: MapItemSelectionContext,
) -> list[dict[str, object]]:
    ...
```

Additional fields:

- `node_id`;
- `input`: source value selected by the mapped Edge.

Return an iterable of mappings. Each mapping is one target callable invocation.
Without a selector, the source value itself must be an iterable of mappings.

## Map aggregator

Signature:

```python
def aggregate_items(ctx: MapAggregationContext) -> Result:
    ...
```

Additional fields:

- `node_id`;
- `item_outputs`: outputs ordered by item index.

Return the logical Node output. Without an aggregator, the ordered list is the
Node output. If any item fails, remaining work is cancelled when possible and
the aggregator is not called.

## Replication aggregator

Signature:

```python
def aggregate_replicas(ctx: ReplicationAggregationContext) -> Result:
    ...
```

Additional fields:

- `node_id`;
- `replica_outputs`: outputs ordered by replica index.

Return the logical Node output. Without an aggregator, the ordered list is
retained. A failed replica prevents aggregation.

## Output Binding

Signature:

```python
def bind_output(ctx: OutputBindingContext) -> None:
    ctx.invocation_context.data["result"] = ctx.output
```

Additional fields:

- writable `invocation_context`;
- writable `session_context`;
- `node_id`;
- isolated `output`.

Output Binding is the only user Hook that commits Context changes. Runtime
applies it transactionally: failure discards the phase changes and marks the
Node failed. It bypasses Operator Retry/fallback.

Parallel Node bindings must write disjoint Context paths. Runtime rejects
overlapping writes within the same parallel execution batch. Serial Nodes may
overwrite a previously written path.

Do not write derived values to Context when ordinary Node output and downstream
Input Mapping are sufficient.

## Async behavior

Condition, Input Mapping, Output Binding, item selector, and aggregators may be
sync or async where their declared callable type permits it. Keep CPU-bound
work out of async functions and keep external calls in typed Operators rather
than hiding them in mapping or binding phases.

## Semantic version

Use `@workflow_hook(version=...)` when changing Hook behavior without changing
its identity or graph structure. Increment the version for a semantic change
that must produce a new Workflow definition hash.
