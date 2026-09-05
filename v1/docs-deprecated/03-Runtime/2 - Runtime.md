# Runtime

Runtime is the service boundary for executing Workflow IR.

It accepts invocations, manages sessions and runs, persists state, and delegates
scheduling and node execution to WorkflowExecutor.

## Interface

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Runtime:
    ir_store: "WorkflowIRStore"
    state_store: "RuntimeStateStore"
    workflow_executor: "WorkflowExecutor"

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

## Responsibilities

Runtime should coordinate the execution stage:

- load the right Workflow IR
- admit or reject invocations
- create or reuse Runtime Sessions
- create Runtime Runs
- initialize run state from the selected entry node
- persist lifecycle transitions and events
- call WorkflowExecutor to drive the run
- expose run handles, results, errors, and traces

## Execution Delegation

Runtime admits invocations and creates sessions and runs. WorkflowExecutor owns
the run loop and coordinates Scheduler, NodeExecutor, and StateManager.

Runtime persists state through the configured state store so a run can be
inspected, retried, resumed, or recovered after process failure.

## Runtime Stage

The Runtime stage is the whole execution environment around a Workflow IR. It
includes Runtime API, state store, session/run lifecycle, WorkflowExecutor,
Scheduler, NodeExecutor, Operator execution, event log, and recovery mechanisms.

The `Runtime` object is the coordinator inside that stage. It should stay thin
enough that WorkflowExecutor, Scheduler decisions, and NodeExecutor execution
remain separately testable.
