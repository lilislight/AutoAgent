# Workflow Executor

WorkflowExecutor is the internal loop driver for one Runtime Run.

It is not the public entrypoint. AutoAgent App creates or resumes a Runtime Run,
then asks WorkflowExecutor to drive that run until it dispatches work, completes,
fails, or reaches a durable wait state.

## Position

```text
AutoAgentApp.invoke(...)
    -> create Workflow Invocation
    -> create or reuse Runtime Session
    -> create Runtime Run
    -> initialize selected entry node
    -> WorkflowExecutor.run(...)
```

WorkflowExecutor coordinates Scheduler, NodeExecutor, and StateManager. It does
not compile Workflows and it does not implement Operator business logic.

## Responsibilities

WorkflowExecutor should:

- initialize the selected entry node as ready for a new Runtime Run
- call Scheduler.next to obtain the next scheduling decision
- apply Scheduler state changes through StateManager
- mark dispatched nodes as running
- call NodeExecutor for dispatched nodes
- apply NodeExecutor state changes through StateManager
- stop when Scheduler returns wait, complete, or fail

Scheduler and NodeExecutor should not directly mutate RuntimeRun objects.

## Loop Sketch

```python
def run(workflow_ir, session, run):
    while True:
        decision = scheduler.next(workflow_ir, session, run)

        state.apply(decision.state_changes)
        session, run = state.reload(session.id, run.id)

        if decision.kind == "dispatch":
            state.mark_nodes_running(run.id, decision.node_ids)
            session, run = state.reload(session.id, run.id)

            result = node_executor.execute_nodes(
                workflow_ir=workflow_ir,
                session=session,
                run=run,
                node_ids=decision.node_ids,
            )

            state.apply(result.state_changes)
            session, run = state.reload(session.id, run.id)
            continue

        if decision.kind == "wait":
            state.mark_run_waiting(run.id, decision.reason)
            return RunHandle(run_id=run.id, status="waiting")

        if decision.kind == "complete":
            state.complete_run(run.id, decision.result)
            return RunResult(run_id=run.id, result=decision.result)

        if decision.kind == "fail":
            state.fail_run(run.id, decision.error)
            return RunError(run_id=run.id, error=decision.error)
```

This loop intentionally has one Scheduler method: `next`.

## New Run Bootstrap

In Version 1, each invocation selects exactly one entry node.

WorkflowExecutor initializes a new Runtime Run by marking that selected entry
node as ready.

```text
invocation.entry_node_id -> node pending -> ready
ready_queue = [entry_node_id]
```

WorkflowExecutor should not mark every Workflow entry as ready. Multiple entry
nodes mean the same Workflow can be entered in different ways. One invocation
selects one of them.

## Resume Flow

When a waiting node receives an external event, AutoAgent App resolves that
event to the target Runtime Run and node.

```text
external event
    -> waiting node completed or failed
    -> transition_queue append node transition
    -> WorkflowExecutor.run(...)
```

The original thread does not remain blocked while waiting for human input,
timers, webhooks, or remote callbacks. The run state is persisted, and a later
`receive` or `resume` call starts a new execution loop from saved state.
