# When to Use Workflow

This document defines when code should become a Workflow and how to choose node
granularity.

Workflow is not a replacement for ordinary code. It is for execution processes
that should be managed by AutoAgent OS.

## Use Workflow When

Use a Workflow when the execution needs independent runtime management.

Good candidates include:

- external API calls with retry, timeout, or fallback
- LLM calls with token, cost, or quality concerns
- side effects that need audit, such as posting comments or creating tickets
- human approval, callbacks, timers, or long waits
- branches that should be visible and debuggable
- parallel work that can be scheduled independently
- long-running tasks that need recovery or resume
- processes whose runtime evidence should feed Optimizer

Examples:

- GitHub issue triage
- customer support ticket handling
- code repair agent
- data report generation
- procurement or approval flow
- explicit ReAct loop with visible tool calls

## Keep Ordinary Code When

Keep logic inside normal code or inside an Operator when it does not need
independent runtime management.

Examples:

- string formatting
- small data transformations
- private helper functions
- request construction
- local parsing
- tightly coupled algorithmic steps
- low-latency in-memory logic

These details can still be tested and maintained as ordinary code. They do not
need Scheduler, StateManager, observability, or retry policy as separate nodes.

## Node Granularity Rule

A step is a good Workflow node when one or more of these are true:

- If it fails, it should be retried or routed to a fallback.
- If it is slow, the developer should see it separately.
- If it is expensive, it needs resource policy.
- If it has side effects, it needs audit.
- If it waits, it needs durable wait/resume state.
- If its output feeds multiple downstream steps, it is a dependency boundary.
- If it can run in parallel with other work, it is a scheduling boundary.
- If Optimizer may replace, split, or tune it later, it is a management boundary.

Do not split a node only because the implementation contains several helper
functions. Split when the system gains useful control or visibility.

## Practical Test

Ask these questions:

```text
Do I want to see this step in the runtime UI?
Do I want to retry or timeout this step independently?
Do I want a fallback if this step fails?
Do I want to measure cost, latency, or output quality for this step?
Do I want this step to resume after a human or external event?
```

If the answer is mostly no, keep it inside an Operator.

If the answer is yes, make it a Workflow node.
