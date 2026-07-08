# Runtime

Runtime is the service boundary for executing Workflow IR.

It accepts invocations, manages sessions and runs, persists state, and delegates
scheduling and node execution to Scheduler and Kernel.

## Interface Sketch

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Runtime:
    ir_store: "WorkflowIRStore"
    state_store: "RuntimeStateStore"
    scheduler: "Scheduler"
    kernel: "Kernel"

    def invoke(self, invocation: "WorkflowInvocation") -> "RuntimeRunHandle":
        workflow_ir = self.ir_store.load(
            invocation.workflow_id,
            invocation.workflow_version,
        )
        self.validate_invocation(workflow_ir, invocation)

        session = self.get_or_create_session(workflow_ir, invocation)
        run = self.create_run(workflow_ir, session, invocation)

        self.start_or_enqueue_run(workflow_ir, session, run)
        return RuntimeRunHandle(run_id=run.id, session_id=session.id)
```

This is an interface shape, not a final implementation commitment.

## Responsibilities

Runtime should coordinate the execution stage:

- load the right Workflow IR
- admit or reject invocations
- create or reuse Runtime Sessions
- create Runtime Runs
- initialize run state from the selected entry node
- persist lifecycle transitions and events
- call Scheduler for next decisions
- call Kernel for selected node execution
- expose run handles, results, errors, and traces

## Scheduler and Kernel Loop

Runtime owns the loop boundary, while Scheduler and Kernel own their own logic.

```python
def run_until_blocked_or_done(workflow_ir, session, run):
    while run.lifecycle in {"ready", "running"}:
        decision = scheduler.next(workflow_ir, session, run)

        if decision.kind == "execute_node":
            result = kernel.execute(workflow_ir, session, run, decision.node_id)
            runtime_state_store.apply(result.state_changes)
            continue

        if decision.kind == "wait":
            runtime_state_store.mark_waiting(run, decision.reason)
            break

        if decision.kind == "complete":
            runtime_state_store.complete_run(run, decision.result)
            break

        if decision.kind == "fail":
            runtime_state_store.fail_run(run, decision.error)
            break
```

Runtime persists changes around the loop so a run can be inspected, retried,
resumed, or recovered after process failure.

## Runtime Stage

The Runtime stage is the whole execution environment around a Workflow IR. It
includes Runtime API, state store, session/run lifecycle, Scheduler, Kernel,
Operator execution, event log, and recovery mechanisms.

The `Runtime` object is the coordinator inside that stage. It should stay thin
enough that Scheduler decisions and Kernel execution remain separately testable.