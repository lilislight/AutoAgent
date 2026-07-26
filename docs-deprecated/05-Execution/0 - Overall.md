# Overall

This document defines the Execution module.

Execution is responsible for driving Runtime Runs, executing selected Workflow
nodes, and applying runtime state transitions.

## Components

```text
WorkflowExecutor
    drives the Runtime Run loop

Scheduler
    decides graph progress and dispatchable nodes

NodeExecutor
    executes selected nodes

StateManager
    applies and persists runtime state changes
```

This module contains the internal execution components used by Runtime and
AutoAgent App.

## NodeExecutor

NodeExecutor executes node ids selected by Scheduler.

It should:

- read the compiled IRNode
- check execution policies before calling the capability
- build node input from Runtime Context using the compiled input plan
- invoke the referenced capability
- collect output, error, and resource usage
- return RuntimeStateChange values

NodeExecutor should not choose the next node and should not directly mutate
RuntimeRun objects.

```python
class NodeExecutor:
    def execute_nodes(
        self,
        workflow_ir: WorkflowIR,
        session: RuntimeSession,
        run: RuntimeRun,
        node_ids: tuple[str, ...],
    ) -> "NodeExecutionResult":
        ...
```

## Execution Guards

Retry, timeout, and resource checks belong near node execution, not in Scheduler.

Scheduler decides whether a node is graph-ready. NodeExecutor decides whether a
graph-ready node is allowed to execute under execution policy.

Pre-execution guards may check:

- retry attempts
- node invocation count within the run
- token, cost, memory, or duration limits
- whether the capability supports timeout or retry
- whether the capability can still be resolved for execution

If a guard fails, NodeExecutor should not call the Operator. It should return a
node failure state change with a structured error code such as
`RETRY_EXHAUSTED` or `RESOURCE_EXHAUSTED`.

Scheduler then handles that failed node through normal graph rules: fallback edge,
failure branch, workflow failure policy, or run failure.

## Resource Usage

Node runtime state should track execution counts and usage.

```python
@dataclass
class ResourceUsage:
    tokens: int = 0
    cost: float = 0.0
    duration_ms: int = 0
    memory_mb: int | None = None
```

`RetryPolicy.max_attempts` controls retries after failed attempts.

`ResourcePolicy.max_invocations` or similar limits control how many times a node
may execute in a run, which matters for loops.

These policies should remain separate because retry attempts and loop
invocations are not the same behavior.

## StateManager

StateManager is the only component that applies runtime state transitions.

Scheduler and NodeExecutor return proposed state changes. WorkflowExecutor asks
StateManager to apply and persist them.

Useful methods may include:

```python
state.mark_node_ready(run_id, node_id)
state.mark_nodes_running(run_id, node_ids)
state.complete_node(run_id, node_id, output, usage)
state.fail_node(run_id, node_id, error, usage)
state.wait_node(run_id, node_id, wait_info)
state.skip_node(run_id, node_id, reason)
state.cancel_node(run_id, node_id, reason)
```

StateManager should maintain:

- node_states
- edge_states
- ready_queue
- running_nodes
- waiting_nodes
- transition_queue
- run lifecycle
- session context writes
- event log records

## Node Statuses

First-version node statuses:

```text
pending    exists in the run but is not ready
ready      dependencies are satisfied and the node may dispatch
running    node has been dispatched to NodeExecutor
waiting    node waits for external event, timer, callback, or human input
completed  node completed successfully
failed     node failed, including policy failures such as resource exhaustion
skipped    node was not selected by the graph path
cancelled  node was stopped because the run or branch was cancelled
```

Version 1 represents timeout as `failed` with error code `TIMEOUT`.

## skipped vs cancelled

`skipped` is normal graph behavior. A branch condition or exclusive route did
not select the node.

```text
classify -> bug_path if category == "bug"
classify -> feature_path if category == "feature"
```

If `bug_path` is selected, `feature_path` may be skipped.

`cancelled` means execution was actively stopped. Examples include user
cancellation or fail-fast propagation after an unhandled required branch failure.

## Boundary

Execution should not own Workflow authoring, compilation, graph scheduling, or
public app entrypoints.

```text
Scheduler decides graph progress.
NodeExecutor executes nodes.
StateManager applies state.
WorkflowExecutor coordinates the loop.
```
