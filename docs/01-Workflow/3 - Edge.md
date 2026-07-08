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
    id: str
    from_node: str
    to_node: str

    condition: "Condition | None" = None
    labels: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
```

## Fields

### id

Unique edge identifier inside a Workflow. It is useful for diagnostics,
visualization, optimizer patches, runtime history, and debugging.

### from_node

Source node id.

### to_node

Target node id.

### condition

Optional condition that determines whether the edge is satisfied. Missing
condition means unconditional.

Conditions may reference source node output, source node status, Runtime Context,
event data, or other runtime state exposed by the expression environment.

Examples:

```text
nodes.classify.output.category == "bug"
nodes.call_model.status == "failed"
context.approval == "approved"
```

### labels / metadata

Labels are small string key-value annotations. Metadata is non-semantic auxiliary
data for tooling or integrations.

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

## Open Questions

- Should Edge id be required or compiler-generated when missing?
- What condition expression language should be used?
- Should priority live on Edge, RoutingPolicy, or compiled edge order?
- Should hyperedges be supported, or should relationships remain binary?
- Should future versions add explicit loop constructs, or should loops remain ordinary edges plus conditions?
