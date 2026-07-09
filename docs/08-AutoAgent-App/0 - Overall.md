# Overall

This document defines the public AutoAgent App layer.

AutoAgent App is the user-facing entrypoint for assembling and running an
AutoAgent OS application. It owns the registries, runtime services, execution
configuration, and public APIs used by application code.

## Purpose

Workflow is the program definition.

AutoAgent App is the application object that runs that program.

```text
Developer code
    -> define Operators
    -> define Workflow
    -> register both with AutoAgent App
    -> invoke Workflow through AutoAgent App
```

The common path should stay small, while advanced users can still configure the
internal Scheduler, WorkflowExecutor, NodeExecutor, StateManager, and stores.

## Public Entry

The public entrypoint is `AutoAgentApp`.

```python
from autoagent import AutoAgentApp, Workflow, operator


@operator(id="github.fetch_issue")
def fetch_issue(issue_id: int) -> dict:
    ...


workflow = Workflow(id="github_issue_triage", version="1.0.0")

workflow.node(
    id="fetch_issue",
    capability="operator:github.fetch_issue",
    entry=True,
)

app = AutoAgentApp()
app.register_operator(fetch_issue)
app.register_workflow(workflow)

result = app.invoke(
    workflow_id="github_issue_triage",
    entry_node_id="fetch_issue",
    input={"issue_id": 123},
)
```

`Workflow` remains a static definition. It does not own Runtime Session,
Runtime Run, Scheduler, execution, or persistence concerns.

## App-Owned Components

An `AutoAgentApp` instance owns or is configured with:

- Workflow registry
- Operator registry
- Workflow compiler
- Workflow IR store or cache
- Runtime service
- Runtime state store or StateManager
- Scheduler
- WorkflowExecutor
- NodeExecutor
- execution configuration

Runtime and execution components may be replaceable, but they are internal
services behind the app-level API.

## Invoke Flow

```text
AutoAgentApp.invoke(...)
    -> load or compile Workflow IR
    -> create Workflow Invocation
    -> create or reuse Runtime Session
    -> create Runtime Run
    -> initialize selected entry node as ready
    -> call WorkflowExecutor
    -> return result or run handle
```

Each invocation selects one entry node and creates one Runtime Run.

## Workflow and Operator Development

Most applications define both Workflow structure and Operator implementations.

The Compiler verifies that each node capability can resolve to a registered
Operator, system capability, or nested Workflow descriptor.

```python
app.register_operator(fetch_issue)
app.register_operator(classify_issue)
app.register_workflow(workflow)

compiled = app.compile("github_issue_triage")
```

Compilation fails with diagnostics when a node references an unknown or
incompatible capability.

## Receive and Resume

Waiting nodes need a public way to continue execution.

```python
app.receive({
    "type": "human_approval",
    "approval_id": "appr_123",
    "approved": True,
})
```

`receive` is the general external event entrypoint. It resolves an event to a
waiting Runtime Run and node, updates that node to `completed` or `failed`,
appends a transition, and calls WorkflowExecutor to continue execution.

`resume(run_id, event)` is the direct API when the caller already knows the run.

## Boundary

AutoAgent App should not make Workflow mutable runtime state. Workflow remains a
static definition that can be compiled, inspected, versioned, and reused.

```text
Workflow defines.
Compiler compiles.
AutoAgent App invokes.
WorkflowExecutor drives.
Scheduler decides.
NodeExecutor executes.
Operator computes.
StateManager persists.
```
