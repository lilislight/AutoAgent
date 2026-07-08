# Overall

This document defines the high-level design direction of the Runtime module.

Runtime executes compiled Workflow IR through Runtime Sessions and Runtime Runs.
It does not own Workflow authoring and it does not compile Workflow structure.

```text
Input Adapter -> Workflow Invocation -> Runtime -> Scheduler -> Kernel -> Operator
```

## Purpose

Runtime is the execution stage of AutoAgent OS.

It receives a Workflow Invocation, selects a Workflow IR version, finds or
creates a Runtime Session, creates a Runtime Run, and drives that run through
Scheduler and Kernel until the run reaches a terminal state.

Runtime should make the execution boundary explicit:

- Input Adapters may be long-running.
- Workflow Invocations are single calls.
- Runtime Sessions are durable context containers.
- Runtime Runs are one pass through the Workflow graph.
- Scheduler decides what can run next.
- Kernel executes selected node work.

## Core Flow

```text
Workflow Invocation
    -> load Workflow IR
    -> find or create Runtime Session
    -> create Runtime Run
    -> enqueue selected entry node
    -> scheduler/kernel loop
    -> persist run result and session context
```

The Workflow begins at one selected entry node for one Runtime Run. The external
listener that produced the invocation is not part of the Workflow Run.

## Runtime Objects

Runtime mainly owns these objects:

- `WorkflowInvocation`: one explicit input call into a Workflow.
- `Runtime`: the service boundary that admits invocations and coordinates runs.
- `RuntimeSession`: durable state for a Workflow context.
- `RuntimeRun`: execution state for one invocation inside a session.
- `RuntimeContext`: structured readable/writable data visible during execution.

Workflow IR is static program structure. Runtime objects hold dynamic execution
state.

## Boundary

Runtime should not:

- parse Python, YAML, or visual authoring formats
- infer graph structure from raw Workflow objects
- compile conditions or input plans
- decide authoring-time validation rules
- implement Operator business logic
- replace Scheduler's node selection logic
- replace Kernel's node execution logic

Runtime should:

- validate an invocation against Workflow IR boundaries
- manage session lookup and creation
- manage run creation and lifecycle
- persist node, edge, context, and event state
- enforce admission and concurrency rules
- call Scheduler and Kernel with the correct state
- expose run results, failures, and observability events

## First-Version Rule

The first design is explicit and finite:

```text
one invocation -> one runtime run -> clear terminal state
```

Session reuse is controlled by the invocation's `session_key`. If no session key
is provided, Runtime creates a fresh session. If a stable session key is
provided, Runtime reuses the matching session for future runs.