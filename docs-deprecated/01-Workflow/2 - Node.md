# Node

This document defines the canonical Node data model.

A Node is a static execution unit inside a Workflow. It describes a schedulable
step, but runtime execution is controlled by Scheduler, WorkflowExecutor, and
NodeExecutor.

## Python Model

```python
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Node:
    id: str | None
    capability: Callable[..., Any] | str

    name: str | None = None
    description: str | None = None
    input_schema: "Schema | None" = None
    input_mapping: "NodeInputMapping | None" = None
    output_binding: "OutputBinding | None" = None
    entry: bool | None = None
    policy: "NodePolicy | None" = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

## Fields

### id

Unique node identifier inside a Workflow.

### capability

Capability requested by the node. It may be a Python callable or a simple string
reference.

Python authoring should prefer direct functions for local capabilities. YAML,
JSON, UI-authored workflows, system capabilities, and externally registered
capabilities may use string references. The compiler resolves string references
only when execution requires a lookup.

External listeners such as API servers, Pub/Sub consumers, webhook listeners, and
cron schedulers are Input Adapters in the input adapter layer. They are not
ordinary Workflow nodes in the first design.

### input_schema

Optional node input schema. It should usually come from the referenced capability
and only be overridden at node level when needed.

### input_mapping

Optional mapping that describes how runtime input is constructed from Runtime
Context, upstream node outputs, constants, invocation input payloads, event payloads, or other runtime data.

If omitted, Compiler or builder may infer it only when unambiguous.

### output_binding

Optional additional binding from node output into a named Runtime Context path.
Every node output is already recorded by default as `nodes.<node_id>.output`
within the current Runtime Run's output namespace.

### entry

Optional explicit entry marker.

| value | Meaning |
| --- | --- |
| `True` | Explicit internal entry node for a Runtime Run. |
| `None` | Compiler may infer entry status from graph structure. |

An entry node is an internal start location. It is not a long-running external
trigger or listener.

### policy

Optional node-level runtime policy container.

### metadata

Metadata is non-semantic auxiliary data for tooling or integrations.

## Node Granularity

A Node is a management boundary, not a line-of-code boundary.

Create a separate node when the system benefits from managing that step
independently:

- independent retry or timeout
- distinct failure or fallback path
- output used by multiple downstream steps
- capability likely to be replaced
- expensive or slow enough to observe separately
- external side effect that needs audit or recovery
- parallelizable work
- human review or approval
- meaningful optimizer patch target
- understandable business or execution stage

Keep logic inside an Operator when it is ordinary implementation detail, does not
need independent scheduling, and splitting it would make the Workflow harder to
understand.

Examples that usually should stay inside an Operator:

- simple data formatting
- string manipulation
- small validation helpers
- request construction
- local parsing that does not need separate retry or observability

AutoAgent OS should not force every function call through Scheduler and
NodeExecutor. Workflow nodes should expose the steps worth managing at the OS
layer.

## NodePolicy

`NodePolicy` groups runtime policies used by Scheduler and NodeExecutor.

```python
@dataclass(frozen=True)
class NodePolicy:
    join: "JoinPolicy | None" = None
    routing: "RoutingPolicy | None" = None
    map: "MapPolicy | None" = None
    retry: "RetryPolicy | None" = None
    timeout: "TimeoutPolicy | None" = None
    resource: "ResourcePolicy | None" = None
```

Input source and session reuse are not node policy in the first design.
They belong to the input/runtime layer. Runtime may reuse a Runtime Session
when an invocation provides a stable session key, or create a fresh session key
when none is provided.

### JoinPolicy

Controls readiness when a node has multiple incoming edges.

```python
@dataclass(frozen=True)
class JoinPolicy:
    mode: Literal["all", "any", "n"] = "all"
    count: int | None = None
```

### RoutingPolicy

Controls how satisfied outgoing edges are selected.

```python
@dataclass(frozen=True)
class RoutingPolicy:
    mode: Literal["all_satisfied", "first_satisfied", "exclusive"] = "all_satisfied"
```

### MapPolicy

Supports runtime-level dynamic fan-out over a collection.

```python
@dataclass(frozen=True)
class MapPolicy:
    over: str
    item_name: str = "item"
    concurrency: int | None = None
```

### RetryPolicy

Controls node retry behavior.

```python
@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff: str | None = None
```

### TimeoutPolicy

Controls maximum execution time for a node attempt.

```python
@dataclass(frozen=True)
class TimeoutPolicy:
    duration: str
```

### ResourcePolicy

Declares resource limits or requirements.

```python
@dataclass(frozen=True)
class ResourcePolicy:
    max_invocations: int | None = None
    max_tokens: int | None = None
    max_cost: float | None = None
    max_duration: str | None = None
    max_memory_mb: int | None = None
    requires_gpu: bool | None = None
```

`RetryPolicy.max_attempts` controls retries after failed attempts.
`ResourcePolicy.max_invocations` controls how many times a node may execute in a
run, which matters for loops. These are separate because retry attempts and loop
invocations are different execution behaviors.

Resource exhaustion should be represented at runtime as node `failed` with a
structured error code such as `RESOURCE_EXHAUSTED`. Scheduler can then process
that failure through ordinary fallback edges or workflow failure policy.

## Failure and Completion Semantics

A node's completion semantics come from its capability.

For a normal function or Operator, completion usually means the computation
finished and returned a result. For a fire-and-forget side effect, completion may
mean the request, job, or message was successfully dispatched. If the Workflow
must wait for a remote result, it should model that explicitly with a wait,
callback, or polling node.

Failure behavior should be represented through retry policy, timeout policy,
edge conditions, or explicit fallback paths.

Examples:

```text
node completed -> continue
node failed -> fallback node
node failed -> continue anyway
node timed out -> retry or error branch
```

This keeps failure handling visible to Scheduler and the execution layer instead
of hiding it inside an exit policy.

## Data Flow

Edges define control flow and dependency. Node input is defined by
`input_mapping` or inferred when unambiguous.

Default node output location:

```text
nodes.<node_id>.output
```

A Runtime Session may also hold durable cross-run context such as conversation
history. A Runtime Run should keep the node and edge state for one invocation's pass
through the Workflow graph.
