---
name: autoagent-author-skill
description: Author AutoAgent V2 Workflows and their business Evaluation Suites. Use for Core graphs, typed Operators, Context, waits and streaming, Agent SDK capabilities and Agent Workflows, or integrating a Workflow project through Host, CLI, and Server.
---

# Author AutoAgent Workflows

Build the smallest Workflow that satisfies the business contract. Use the
target project's installed V2 SDK and CLI. Keep Workflow definitions
importable and use the project's existing registration and dependency layout.

## Develop the Workflow

1. Identify the input, expected output, required branches, state that must
   survive between steps or Invocations, and external effects. Resolve
   ambiguities that change observable business behavior.
2. Find the project registration and verify the installed version with
   `autoagent --version`. Select the relevant references below. Take
   Workflow, Node, Edge, and Entry IDs from the actual definition.
3. Define typed Operators and connect them with explicit Edges. Use
   Input Mapping for contract changes and Output Binding for Context
   writes. Give independently scheduled branches separate write paths.
4. Add native commands for approval or coordination when the business
   flow needs them. Choose Retry and Recovery based on whether repeating
   work is safe. Use a fresh development Session for a standalone run.
5. Run static checks and exercise the affected paths. Use a business Eval
   to preserve required outcomes, routes, and intermediate invariants.
   Validate external effects with isolated fixtures when they matter.
6. Report the Workflow and Entry IDs, commands run, observed outcomes,
   and any behavior the available fixtures could not verify.

## Choose references by task

| Task | Reference |
| --- | --- |
| Find or create project registration and choose public imports | [Project setup](references/project.md) |
| Define a graph, typed Operator, Hook, Context change, Map, or loop | [Workflow graph and data contracts](references/workflow.md) |
| Add Wait, Timer, Select, Child coordination, or execution policies | [Native interaction and recovery](references/interaction-recovery.md) |
| Reduce an Operator stream or expose business notifications | [Streaming Operators and User Events](references/streaming-user-events.md) |
| Check registration, inspect the compiled plan, run an Invocation, or reply to a Wait | [Compile and run](references/validation.md) |
| Run a project Server, embed a Host, or integrate FastAPI | [Run and embed a Workflow project](references/hosting.md) |
| Add explicit model, Tool, or MCP steps to a Workflow | [Agent capabilities in a custom Workflow](references/agent-capabilities.md) |
| Build a multi-turn Agent, configure its resources, or handle approval and delegation | [Build an Agent Workflow](references/agent-workflows.md) |
| Define business Cases and compare results across versions | [Workflow business Evaluation](references/evaluation.md) |

## Copy a runnable starting point

- [Core expense approval](assets/core-workflow/README.md) uses typed
  Nodes, an external Wait, Context, and two business Cases.
- [Offline release note Agent](assets/agent-workflow/README.md) registers
  a local Provider and verifies the Tool's effect across approval and refusal.
