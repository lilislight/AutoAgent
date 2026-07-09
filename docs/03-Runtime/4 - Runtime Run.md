# Runtime Run

A Runtime Run is one invocation's execution through a Workflow IR graph.

It belongs to exactly one Runtime Session.

## Purpose

Runtime Run stores dynamic graph execution state: which nodes are ready, running,
completed, failed, waiting, or skipped for this invocation.

Two runs may reach the same IR node, but their runtime state is separate because
state is scoped by run id.

```text
IRNode("answer")          -> static compiled node
RuntimeRun A / node answer -> state for message A
RuntimeRun B / node answer -> state for message B
```

## Model

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RuntimeRun:
    id: str
    session_id: str
    invocation_id: str

    workflow_id: str
    workflow_version: str
    entry_node_id: str

    lifecycle: str

    context: "RuntimeContext"
    node_states: dict[str, "NodeRuntimeState"] = field(default_factory=dict)
    edge_states: dict[str, "EdgeRuntimeState"] = field(default_factory=dict)

    ready_queue: list[str] = field(default_factory=list)
    running_nodes: set[str] = field(default_factory=set)
    waiting_nodes: set[str] = field(default_factory=set)
    transition_queue: list["NodeTransitionRef"] = field(default_factory=list)

    result: dict[str, Any] | None = None
    error: "RuntimeErrorInfo | None" = None

    created_at: str | None = None
    updated_at: str | None = None
```

## Node State

```python
@dataclass
class NodeRuntimeState:
    node_id: str
    status: str
    attempts: int = 0
    invocations: int = 0
    input: dict[str, Any] | None = None
    output: dict[str, Any] | None = None
    error: "RuntimeErrorInfo | None" = None
    resource_usage: "ResourceUsage | None" = None
    started_at: str | None = None
    finished_at: str | None = None
```

Common statuses include:

| Status | Meaning |
| --- | --- |
| `pending` | Node exists in the run but is not ready. |
| `ready` | Dependencies are satisfied and the node may dispatch. |
| `running` | Node has been dispatched to NodeExecutor. |
| `waiting` | Node waits for an external event, callback, timer, or human input. |
| `completed` | Node completed successfully. |
| `failed` | Node failed, including retry exhaustion, resource exhaustion, or timeout. |
| `skipped` | Node was not selected by graph routing. |
| `cancelled` | Node was stopped because the run or branch was cancelled. |

Version 1 represents timeout as `failed` with error code `TIMEOUT`.

## Node Transition Queue

`transition_queue` records nodes whose outgoing graph transitions still need to
be processed by Scheduler.

```python
@dataclass(frozen=True)
class NodeTransitionRef:
    node_id: str
    status: str
    attempt: int
```

StateManager appends to this queue when a node reaches a status that can trigger
outgoing edges, such as `completed`, `failed`, or timeout-as-failed.

Scheduler consumes this queue by proposing state changes that evaluate outgoing
edges, select routes, satisfy joins, and mark downstream nodes ready.

## Resource Usage

NodeExecutor should record resource usage reported by Operators or execution
infrastructure.

```python
@dataclass
class ResourceUsage:
    tokens: int = 0
    cost: float = 0.0
    duration_ms: int = 0
    memory_mb: int | None = None
```

Resource exhaustion should normally be represented as `failed` with a structured
error code such as `RESOURCE_EXHAUSTED`, not as a separate status.

## Edge State

```python
@dataclass
class EdgeRuntimeState:
    edge_id: str
    status: str
    evaluated: bool = False
    selected: bool = False
    reason: str | None = None
```

Scheduler proposes edge state updates when it evaluates conditions and routing
policies. StateManager applies and persists those updates.

## Completion

A run completes when Scheduler determines that no required active branch remains
and the Workflow's terminal behavior has been satisfied.

Exit nodes are ordinary nodes with terminal meaning in Workflow IR. A run may
finish after one exit branch, several joined exit branches, or a failure route,
depending on compiled policies and runtime state.
