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
    input: dict[str, Any] | None = None
    output: dict[str, Any] | None = None
    error: "RuntimeErrorInfo | None" = None
    started_at: str | None = None
    finished_at: str | None = None
```

Common statuses include `pending`, `ready`, `running`, `completed`, `failed`,
`skipped`, `waiting`, `cancelled`, and `timed_out`.

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

Scheduler updates edge state when it evaluates conditions and routing policies.

## Completion

A run completes when Scheduler determines that no required active branch remains
and the Workflow's terminal behavior has been satisfied.

Exit nodes are ordinary nodes with terminal meaning in Workflow IR. A run may
finish after one exit branch, several joined exit branches, or a failure route,
depending on compiled policies and runtime state.