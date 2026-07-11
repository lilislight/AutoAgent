# Compilation Pipeline

This document defines the first compilation pipeline from Workflow to Workflow
IR.

```text
Workflow
  -> collect definitions
  -> validate structure
  -> build graph indexes
  -> infer entries and exits
  -> resolve capabilities
  -> compile conditions
  -> compile mappings
  -> compile policies
  -> validate target support
  -> emit Workflow IR
```

## Stage 1: Collect Definitions

Build lookup tables from Workflow definitions.

Outputs:

- Workflow metadata
- nodes by id
- edges by id
- original node and edge order

Diagnostics:

- duplicate node id
- duplicate edge id
- missing required fields

## Stage 2: Validate Structure

Check graph references before deeper compilation.

Diagnostics:

- unknown `from_node`
- unknown `to_node`
- empty Workflow when unsupported
- missing entry after inference

## Stage 3: Build Graph Indexes

Compile graph lookup indexes.

Outputs:

- incoming edges by node
- outgoing edges by node
- predecessors by node
- successors by node

The Compiler should preserve edge order because routing modes such as
`first_satisfied` depend on it.

Loops are allowed. Dead-loop detection belongs to validation; cycle metadata does
not need to be emitted into Workflow IR unless runtime components consume it.

## Stage 4: Infer Entries and Exits

Determine final entry and exit node ids.

Rules:

```text
explicit entry nodes -> use explicit entries
no explicit entries -> infer nodes with no incoming edges
exit nodes -> nodes with no outgoing edges
```

Outputs:

- `entry_node_ids`
- `exit_node_ids`
- final `IRNode.entry` / `IRNode.exit` flags

The IR does not need to record whether a flag was explicit or inferred.

## Stage 5: Resolve Capabilities

Compile each node capability into a runtime executable binding.

Outputs:

- `ResolvedCapability` per node
- capability input and output schemas when available

Diagnostics:

- missing capability
- incomplete descriptor

## Stage 6: Compile Conditions

Compile edge conditions into runtime-evaluable condition plans.

Outputs:

- `CompiledCondition` per conditional edge
- referenced paths

Diagnostics:

- parse error
- unsafe expression
- unknown static node reference

## Stage 7: Compile Mappings

Compile node input mappings and output bindings.

Outputs:

- `InputPlan` per node
- `OutputBinding` values when declared

Diagnostics:

- missing required input
- ambiguous inferred input
- invalid path
- static schema mismatch

## Stage 8: Compile Policies

Normalize Workflow and Node policies.

Outputs:

- join policy
- routing policy
- retry policy
- timeout policy
- resource policy when supported

Diagnostics:

- invalid retry count
- invalid timeout duration
- invalid join or routing configuration
- unsupported policy for target runtime

## Stage 9: Validate Target Support

Check compiled structure against the selected runtime target.

The first target should cover:

- node and edge execution
- edge conditions
- join and routing
- Runtime Session lookup by invocation session key
- Operator invocation
- retry and timeout

## Stage 10: Emit Workflow IR

Create immutable Workflow IR.

IR emission should include:

- identity and version metadata
- compiled nodes and edges
- graph indexes
- entry and exit node ids
- resolved capabilities
- compiled conditions, mappings, and policies
- labels and metadata

## Testing Strategy

Core compiler tests:

- empty Workflow
- single entry node
- sequential graph
- fan-out graph
- conditional branch
- join with `all`, `any`, and `n`
- loop with reachable exit path
- dead loop diagnostic
- missing capability
- invalid condition
- ambiguous input mapping
- unsupported policy
