# Workflow Design

This reference owns the translation from business requirements to Node/Edge
topology. It does not define Hook signatures, Policy fields, or CLI commands.

## Start from the contract

Before drawing the graph, identify:

1. Invocation input.
2. Final Workflow output.
3. Externally visible side effects.
4. Business decisions.
5. Work that is independent and may run concurrently.
6. Values that must be aggregated.
7. Repeated work and its stopping condition.
8. External waits.

Use a Node for a meaningful execution or external boundary. Do not create Nodes
for transient Scheduler transitions or for transformations that naturally
belong in a mapping, Condition, or binding.

## Identity

Use stable snake_case IDs:

```text
workflow: release_review
node: security_review
edge: select_security_review
```

IDs are durable trace and diagnostic identities. Do not derive them from list
positions, display labels, or random values.

## Entry and exit

The compiler infers entry Nodes from graph structure unless a Node has
`entry=True`.

- An explicit entry cannot have an incoming Edge.
- A Workflow may have multiple entries.
- A structural exit is a Node with no outgoing Edge.
- A Workflow may have multiple exits.
- When embedding a child Workflow with multiple entries or exits, select its
  boundary with `child_entry_node_id` and `child_exit_node_id`.

Do not treat a Node as an exit merely because a UI label calls it one.

## Sequential flow

Use one Edge per dependency:

```text
validate -> enrich -> publish
```

If the target callable parameters do not directly match incoming outputs, use
an Input Mapping rather than adding a pass-through Node.

## Conditional branch

Attach a boolean Condition to each conditional Edge:

```text
classify --low_risk--> auto_approve
         --high_risk-> specialist_review
```

The Condition selects control flow. It does not move data or write Context.
Targets still receive data through their normal incoming output and Input
Mapping.

Make overlapping Conditions intentional. If multiple outgoing Conditions are
true, multiple branches may run.

## Parallel work and fan-in

Multiple selected outgoing Edges make their targets independently runnable:

```text
plan -> security ----\
     -> reliability --+-> aggregate
```

The fan-in Node waits for its required selected predecessors. Give the
aggregator callable named parameters matching predecessor Node IDs when direct
binding is unambiguous, or provide an Input Mapping.

Do not rely on completion order. Parallel Nodes can finish in any order.

## Loop

Represent a natural Loop with an ordinary back Edge:

```text
start -> plan -> review -> plan
                         \-> finish
```

The Loop header must have a valid external entry path and reducible back-edge
structure. Nested Loops are allowed when each region has an unambiguous header
and entry.

Always bound repeated execution with a business stopping condition and a
`ResourcePolicy.max_node_executions_per_invocation` limit on an appropriate
Node. Never depend only on a model choosing to stop.

An external entry Edge may execute again when this Loop is nested inside a
larger Loop. Do not model a Loop as “external Edge only once, then back Edge
forever.”

## Map

Use `EdgePolicy(map=MapPolicy(...))` when one selected source output creates a
dynamic number of target Operator calls.

- Each item must become a mapping of target callable arguments.
- `item_selector` is optional when source output already contains mappings.
- Results preserve item index order when no aggregator is configured.
- `max_parallelism` limits this Map below the App-wide ceiling.

Map belongs to an Edge and produces one logical target NodeExecution.

Do not add a second Input Mapping to the mapped target; the compiler rejects
the conflicting data sources.

## Replication

Use `NodePolicy(replication=ReplicationPolicy(...))` when the same logical Node
input should execute multiple times, such as self-consistency sampling.

- `count` is required.
- Results preserve replica index order without an aggregator.
- `max_parallelism` limits replicas below the App-wide ceiling.

Do not combine Map and Replication for the same target execution.

## Wait

Use `SystemCommand(id="wait")` for a stable external input boundary:

```text
request_approval(wait) -> finalize
```

The Wait output supplied by Resume becomes the outgoing value. Map it into the
next typed callable with an Input Mapping.

The Wait Node input must be a named-argument mapping. It accepts `wait_key`,
`wait_type`, and a mapping `payload`. An entry Wait can receive these directly
from Invocation input. When omitted, `wait_key` defaults to the generated Node
execution ID and `wait_type` defaults to `external`.

Provide a stable application-level `wait_key` when a later process must address
the wait explicitly. Durable cross-process Resume additionally needs database
persistence and a non-minimal Event mode; read [cli.md](cli.md).

## Child Workflow

Pass a Workflow object to `add_node` to expand it at compile time:

```python
parent.add_node(
    review_workflow,
    node_id="review",
    child_entry_node_id="start",
    child_exit_node_id="finish",
)
```

The parent Node ID becomes the namespace for expanded child IDs. Child
Workflows are not runtime black boxes. Avoid recursive child references.

## Choose the smallest model

- Fixed known branches: normal parallel Nodes.
- Dynamic item count: Map.
- Same input repeated: Replication.
- Reusable multi-step graph: child Workflow.
- External pause: Wait.
- Data reshaping only: Input Mapping.
- Routing only: Condition.
- Context commit only: Output Binding.
