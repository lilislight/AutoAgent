# Overall

This document defines the staged roadmap for AutoAgent OS.

The roadmap separates Version 1 framework behavior from later runtime
coordination features. The goal is to keep Version 1 useful without
turning it into a full distributed operating system too early.

## Version 1: Framework Core

Version 1 should support reliable execution of compiled Workflows through
AutoAgent App.

Expected capabilities:

- public execution through `AutoAgentApp.invoke(...)`
- Operator registration and Workflow registration
- Workflow compilation into Workflow IR
- multiple entry nodes in one Workflow
- one selected entry node per invocation
- one Runtime Run per invocation
- Runtime Session lookup or creation by `session_key`
- serialized runs inside the same Runtime Session by default
- RuntimeRun state isolation for node state, edge state, ready queue, running
  nodes, waiting nodes, and transition queue
- Scheduler with a single `next(...)` method
- batch dispatch of multiple ready nodes
- NodeExecutor for node execution and Operator invocation
- retry, timeout, and resource policy checks near node execution
- waiting nodes for human input, callback, webhook, timer, or remote work
- `AutoAgentApp.receive(...)` or `resume(...)` for continuing waiting runs
- workflow failure policy for unhandled branch failures

The Version 1 entry rule is:

```text
one invocation -> one selected entry node -> one Runtime Run
```

Multiple entry nodes are supported as different ways to start the same Workflow.
They do not mean one invocation starts every entry.

## Version 1 Waiting and Resume

Waiting is a durable runtime state, not a blocked thread.

```text
NodeExecutor sets node waiting
WorkflowExecutor returns a run handle
external event arrives later
AutoAgentApp.receive(...) resolves the waiting run and node
waiting node becomes completed or failed
Scheduler continues from the saved transition
```

This is required for human approval, callbacks, timers, and remote worker
results.

## Version 2: Coordinated Session Entries

Version 2 should address the harder model where multiple different entries
target the same Runtime Session and coordinate through shared session state.

Examples:

```text
entry: user_message
entry: tool_callback
entry: human_feedback
entry: cancel_request
entry: manual_override
```

These entries may belong to the same conversation, ticket, task, or business
process. They are not just independent runs.

Key design questions:

- Does a new entry create a new Runtime Run or resume an existing waiting run?
- Can a later entry interrupt, cancel, or modify an active run?
- Are multiple active runs allowed inside the same Runtime Session?
- How are Runtime Context write conflicts detected or resolved?
- How are event ordering and idempotency enforced?
- Is completion/failure defined at run level, session level, or both?
- Which session context partitions are safe for parallel writes?

The Version 1 concurrency rule is conservative:

```text
same Runtime Session -> serialized Runtime Runs
different Runtime Sessions -> may run in parallel
```

Version 2 can relax this through explicit concurrency and conflict policies.

## Later Extensions

Later extensions may include:

- dynamic map/fan-out with runtime node instances
- nested Workflow execution with child Runtime Run or child Runtime Session
- checkpoint and recovery policies
- optimizer patches applied to future Workflow versions
- hot patching with state migration
- distributed scheduling
- streaming graph execution
- live migration

## Out of Core

Daemon-style listeners should stay outside the Workflow core.

API servers, webhook listeners, Pub/Sub consumers, cron schedulers, and workers
should be Input Adapters. They create invocations or resume events through
AutoAgent App instead of becoming long-running Workflow nodes.
