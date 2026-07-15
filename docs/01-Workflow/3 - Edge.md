# Edge

This document defines the canonical Edge data model.

An Edge is a static relationship between two nodes. It describes dependency,
transition, or control-flow possibility. It does not execute computation and does
not automatically pass data.

## Python Model

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Edge:
    id: str | None
    from_node: str | "Node"
    to_node: str | "Node"

    condition: "Condition | None" = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

## Fields

### id

Optional edge identifier inside a Workflow. Compiler assigns one when omitted.

### from_node

Source node reference. It may be a node id string or a `Node` object.

### to_node

Target node reference. It may be a node id string or a `Node` object.

### condition

Optional condition that determines whether the edge is satisfied. Missing
condition means unconditional.

Conditions use the same runtime data environment as node `input_mapping`.
They may reference Runtime Context, upstream node outputs and statuses,
constants, invocation input payloads, event payloads, or other runtime data
exposed by the expression environment. A condition is not limited to the source
node's output.

Examples:

```text
nodes.classify.output.category == "bug"
nodes.call_model.status == "failed"
context.approval == "approved"
input.issue.priority in ["p0", "p1"]
```

Condition expressions only decide whether an edge is satisfied. They do not move
data into the target node. Target input still comes from the target node's
`input_mapping`, or from Compiler/builder inference when unambiguous.

### metadata

Metadata is non-semantic auxiliary data for tooling or integrations.

## Evaluation

After a source node reaches the relevant runtime state, Scheduler evaluates its
outgoing edges.

```text
edge without condition -> satisfied
edge with condition -> satisfied only if condition is true
satisfied outgoing edges -> selected by source node RoutingPolicy
```

Default routing mode is `all_satisfied`.

## Multiple Outgoing Edges

Multiple outgoing edges can represent fan-out, conditional branch, fallback, error path, or loop exit path.

Edge provides condition. Source node `RoutingPolicy` controls how satisfied edges
are selected: `all_satisfied`, `first_satisfied`, or `exclusive`.

## Multiple Incoming Edges

Multiple incoming edges can represent join, race, or threshold join.

Target node `JoinPolicy` controls readiness: `all`, `any`, or `n`.

## Data Flow

Edges do not automatically pass data.

Data flow is defined by node `input_mapping`, or inferred by Compiler/builder
when unambiguous.

```text
Edge: A must happen before B.
Input mapping: B.input.x comes from A.output.y.
```

## Entry and Exit Inference

Compiler may infer graph boundaries:

```text
node with no incoming edges -> entry candidate
node with no outgoing edges -> exit candidate
```

Explicit node markers may clarify entry behavior. Exit markers are usually not
needed.

## Common Patterns

```text
Sequential:   A -> B -> C
Fan-out:      A -> B, A -> C, A -> D
Branch:       A -> B if condition, A -> C if condition
Fallback:     A -> B if completed, A -> C if failed
Loop:         B -> A if revise, B -> C if done
```
