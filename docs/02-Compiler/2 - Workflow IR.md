# Workflow IR

Workflow IR is the static compiled representation of a Workflow.

It is the runtime-readable program structure consumed by Scheduler and
NodeExecutor.
It does not contain Runtime Session or Runtime Run state.

## Workflow Model vs Workflow IR

Workflow model is the authoring model. Workflow IR is the runtime model.

A Workflow node or edge may contain author-facing references, omitted defaults,
raw conditions, and implicit graph boundaries.

An IR node or edge should contain resolved capabilities, compiled input plans,
compiled conditions, normalized policies, graph indexes, and final entry/exit
flags.

```text
Workflow Node / Edge -> source program structure
IRNode / IREdge      -> compiled runtime structure
```

## WorkflowIR

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WorkflowIR:
    ir_version: str
    compiler_version: str

    workflow_id: str
    workflow_version: str

    nodes: dict[str, "IRNode"]
    edges: dict[str, "IREdge"]
    graph: "IRGraph"

    entry_node_ids: tuple[str, ...]
    exit_node_ids: tuple[str, ...]

    input_schema: "Schema | None" = None
    output_schema: "Schema | None" = None

    labels: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
```

`entry_node_ids` and `exit_node_ids` are indexes over `nodes`. They let Runtime
and Scheduler check graph boundaries without scanning all nodes.

`input_schema` describes the Workflow Invocation input payload. `output_schema`
describes the optional Workflow Run result. Node-level schemas are stored on
`IRNode`.

## IRGraph

`IRGraph` contains precomputed graph indexes.

```python
@dataclass(frozen=True)
class IRGraph:
    outgoing_edges: dict[str, tuple[str, ...]]
    incoming_edges: dict[str, tuple[str, ...]]
    predecessors: dict[str, tuple[str, ...]]
    successors: dict[str, tuple[str, ...]]
```

Scheduler uses these indexes to move from a completed node to outgoing edges, and
from a selected edge to target node readiness checks.

Workflow IR does not store cycle metadata. Loops are valid Workflow structure,
but loop validation is a Compiler responsibility.

## IRNode

`IRNode` is the compiled runtime description of a Workflow node.

```python
@dataclass(frozen=True)
class IRNode:
    id: str
    capability: "ResolvedCapability"

    input_schema: "Schema | None" = None
    output_schema: "Schema | None" = None

    input_plan: "InputPlan | None" = None
    output_bindings: tuple["OutputBinding", ...] = ()

    join_policy: "CompiledJoinPolicy | None" = None
    routing_policy: "CompiledRoutingPolicy | None" = None
    retry_policy: "CompiledRetryPolicy | None" = None
    timeout_policy: "CompiledTimeoutPolicy | None" = None
    resource_policy: "CompiledResourcePolicy | None" = None

    entry: bool = False
    exit: bool = False

    labels: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
```

Scheduler mainly uses join/routing/entry/exit fields. NodeExecutor mainly uses
capability, input plan, output bindings, and execution policies.

## IREdge

`IREdge` is the compiled runtime description of a Workflow edge.

```python
@dataclass(frozen=True)
class IREdge:
    id: str
    from_node: str
    to_node: str

    condition: "CompiledCondition | None" = None
    trigger_statuses: tuple[str, ...] = ("completed",)
    order: int = 0

    labels: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
```

`condition` is parsed by the Compiler and evaluated by Scheduler at runtime.

`trigger_statuses` defines when the edge may be considered, such as
`completed`, `failed`, or `timed_out`.

`order` preserves stable outgoing edge order for routing modes such as
`first_satisfied`.

## ResolvedCapability

The source Workflow stores a callable or string capability. Workflow IR stores a
resolved capability binding that NodeExecutor can invoke.

```python
@dataclass(frozen=True)
class ResolvedCapability:
    name: str
    descriptor: "CapabilityDescriptor"
    handler: "Callable[..., Any] | None" = None
```

The Compiler binds direct callables without registry lookup. String capabilities
are resolved through the configured execution environment.

## InputPlan

`InputPlan` describes how NodeExecutor should build node input at runtime.

```python
@dataclass(frozen=True)
class InputPlan:
    fields: dict[str, "InputSource"]
    inferred: bool = False


@dataclass(frozen=True)
class InputSource:
    kind: str
    path: str | None = None
    value: Any = None
    expression: "CompiledExpression | None" = None
```

Common source kinds:

| kind | Meaning |
| --- | --- |
| `invocation_input` | Read from Workflow Invocation input. |
| `context_path` | Read from Runtime Session or Runtime Run context. |
| `node_output` | Read from a previous node output in the current run. |
| `constant` | Use a static literal. |
| `expression` | Compute from a compiled expression. |

## OutputBinding

`OutputBinding` describes additional writes from node output.

```python
@dataclass(frozen=True)
class OutputBinding:
    target: str
    source: str | None = None
```

Every node output should already be recorded in the Runtime Run output namespace.
Output bindings are for extra writes such as durable Runtime Session context or
run result fields.

## CompiledCondition

```python
@dataclass(frozen=True)
class CompiledCondition:
    source: str
    expression: "CompiledExpression"
    referenced_paths: tuple[str, ...] = ()
```

Conditions are evaluated at runtime. The Compiler only parses, validates, and
restricts them.

## Excluded Runtime State

Workflow IR must not contain:

- Runtime Session or Runtime Run
- node or edge runtime status
- ready queues or running nodes
- Operator results
- retry counters
- resource usage
- event logs
- worker/thread state
- session keys

## Summary

Workflow IR is the compiled graph, resolved capabilities, compiled dataflow, and
compiled control policies of a Workflow.
