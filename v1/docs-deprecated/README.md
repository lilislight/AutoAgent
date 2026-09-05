# AutoAgent OS Documentation

> [!WARNING]
> This directory is an archived, obsolete design snapshot. It does not describe
> the current implementation. Use the source code, tests, root README files,
> and runnable examples as the current source of truth.

This directory contains an early architecture design for AutoAgent OS.

## Structure

- `00-Foundation/`: vision, core concepts, and system architecture.
- `01-Workflow/`: Workflow, Node, Edge, authoring model, and node granularity guidance.
- `02-Compiler/`: Compiler, Workflow IR, validation, and compilation pipeline.
- `03-Runtime/`: Invocation, Runtime Session, Runtime Run, context, lifecycle, and concurrency.
- `04-Scheduler/`: graph scheduling and `ScheduleDecision`.
- `05-Execution/`: WorkflowExecutor, NodeExecutor, and StateManager.
- `06-Operator/`: Operator capabilities, system capabilities, LLM ToolSet, and ReAct workflow pattern.
- `07-Optimizer/`: optimizer evidence analysis and Workflow patch boundary.
- `08-AutoAgent-App/`: public app entrypoint and invocation API.
- `09-Observability/`: runtime graph visualization, event timeline, and inspection.
- `10-Roadmap/`: staged implementation roadmap.

## Current Execution Model

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
