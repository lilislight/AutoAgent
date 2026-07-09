# Overall

This document defines the Scheduler module.

Scheduler is the graph decision component. It reads Workflow IR, Runtime Session,
and Runtime Run state, then decides the next action for WorkflowExecutor.

Scheduler does not execute Operators and does not directly mutate runtime state.
It returns a scheduling decision and proposed state changes. WorkflowExecutor
applies those changes through StateManager.

## Purpose

Scheduler answers one question:

```text
Given WorkflowIR + RuntimeSession + RuntimeRun, what should happen next?
```

The first design uses one public Scheduler method:

```python
class Scheduler:
    def next(
        self,
        workflow_ir: WorkflowIR,
        session: RuntimeSession,
        run: RuntimeRun,
    ) -> "ScheduleDecision":
        ...
```

## Schedule Decision

`ScheduleDecision` has four Version 1 kinds.

```python
@dataclass(frozen=True)
class ScheduleDecision:
    kind: Literal["dispatch", "wait", "complete", "fail"]
    node_ids: tuple[str, ...] = ()
    state_changes: tuple["RuntimeStateChange", ...] = ()
    reason: str | None = None
    result: dict[str, Any] | None = None
    error: "RuntimeErrorInfo | None" = None
```

### dispatch

One or more nodes are ready to execute.

`node_ids` may contain multiple nodes so fan-out branches can be executed in
parallel when NodeExecutor and runtime configuration allow it.

### wait

The run is alive but cannot dispatch more nodes right now.

Common causes:

- one or more nodes are still running
- one or more nodes are waiting for human input
- a node is waiting for a webhook, callback, timer, or remote worker result

Waiting does not block the original thread. WorkflowExecutor returns a run
handle, and a later `receive` or `resume` call continues the saved run.

### complete

The run has reached a valid terminal state.

### fail

The run cannot continue and has not completed normally. Examples include an
unhandled failed branch, a non-exit dead end, or an invalid stalled state.

## Runtime Indexes

Scheduler should not scan the whole graph on every step.

RuntimeRun should maintain runtime indexes:

```text
ready_queue       nodes whose dependencies are satisfied but are not running
running_nodes     nodes currently executing
waiting_nodes     nodes waiting for external events or time
transition_queue  nodes that just completed, failed, or timed out
```

`transition_queue` lets Scheduler quickly process only nodes whose outgoing
edges need advancement.

```python
@dataclass(frozen=True)
class NodeTransitionRef:
    node_id: str
    status: str
    attempt: int
```

StateManager maintains these indexes when applying runtime state changes.

## next Algorithm

Scheduler.next should handle both the initial run and later execution steps.

Initial run:

```text
ready_queue = [entry_node_id]
transition_queue = []
next -> dispatch(entry_node_id)
```

After node execution:

```text
transition_queue = [A completed]
next -> process A outgoing edges -> mark B/C ready -> dispatch(B, C)
```

Pseudo-code:

```python
def next(workflow_ir, session, run):
    changes = []
    newly_ready = []

    for transition in run.transition_queue:
        transition_changes, ready_nodes = process_transition(
            workflow_ir,
            session,
            run,
            transition,
        )
        changes.extend(transition_changes)
        newly_ready.extend(ready_nodes)

    if run.transition_queue:
        changes.append(clear_transition_queue())

    dispatchable = select_dispatchable(
        run.ready_queue,
        newly_ready,
        scheduler_config,
    )

    if dispatchable:
        return ScheduleDecision(
            kind="dispatch",
            node_ids=dispatchable,
            state_changes=tuple(changes),
        )

    if run.running_nodes or run.waiting_nodes:
        return ScheduleDecision(
            kind="wait",
            state_changes=tuple(changes),
            reason="waiting_for_active_nodes",
        )

    if run_is_complete(workflow_ir, run):
        return ScheduleDecision(
            kind="complete",
            state_changes=tuple(changes),
            result=build_run_result(workflow_ir, run),
        )

    return ScheduleDecision(
        kind="fail",
        state_changes=tuple(changes),
        error=RuntimeErrorInfo(code="SCHEDULER_STALLED"),
    )
```

`select_dispatchable` may return all currently ready nodes or a limited batch
based on scheduler configuration.

## Transition Processing

Processing a node transition means advancing the graph from that node's terminal
state.

```text
node completed/failed/timed_out
    -> read outgoing edges from WorkflowIR.graph
    -> check edge trigger_statuses
    -> evaluate edge conditions
    -> apply source node routing policy
    -> mark selected edges
    -> check target node join policy
    -> mark target nodes ready
```

Workflow IR graph indexes make this local:

```text
outgoing_edges[node_id] -> candidate edges
incoming_edges[target]  -> join readiness check
```

## Failure Semantics

A node failure is not automatically a run failure.

```text
node failed
    -> failed-trigger edge exists: continue through fallback path
    -> no fallback: unhandled branch failure
```

Workflow-level failure policy decides whether an unhandled branch failure fails
the run immediately or allows other active branches to continue.

The Version 1 default is fail-fast.

## Boundaries

Scheduler should not:

- invoke Operators
- build node input values
- enforce retry or resource policy
- directly mutate RuntimeRun objects
- persist state

Scheduler should:

- select dispatchable nodes
- evaluate edge conditions
- apply routing and join policy
- propose graph state changes
- decide wait, complete, or fail when no node can dispatch
