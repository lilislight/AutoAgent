# Workflow Invocation

A Workflow Invocation is one explicit call into a compiled Workflow.

It is created by an Input Adapter such as an API handler, webhook listener,
Pub/Sub consumer, cron scheduler, CLI command, or test harness. The adapter may
be long-running, but the invocation is not.

## Model

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WorkflowInvocation:
    id: str

    workflow_id: str
    workflow_version: str | None
    entry_node_id: str

    input: dict[str, Any]
    session_key: str | None = None
    idempotency_key: str | None = None

    source: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None
```

## Meaning

`entry_node_id` selects the internal Workflow IR entry node where this run will
start. Only one entry node is selected for one invocation in the first design.

`input` is the payload validated against the Workflow IR input schema and then
made available to node input plans.

`session_key` controls session reuse. If it is absent, Runtime creates a fresh
session. If it is present, Runtime finds or creates the session for that key.

`idempotency_key` can be used by Runtime to deduplicate repeated delivery from
an external adapter.

## Boundary

An invocation does not contain:

- long-running listener state
- Scheduler ready queues
- node execution results
- retry counters
- conversation history
- compiled graph structure

Those belong to Input Adapters, Runtime Sessions, Runtime Runs, Workflow IR, or
runtime infrastructure.

## Admission

When Runtime receives an invocation, it should check:

- the Workflow IR exists for `workflow_id` and requested version
- `entry_node_id` belongs to `workflow_ir.entry_node_ids`
- `input` matches `workflow_ir.input_schema` where a schema exists
- the invocation is not a duplicate if `idempotency_key` is supplied
- the target session can admit a new run under current concurrency rules