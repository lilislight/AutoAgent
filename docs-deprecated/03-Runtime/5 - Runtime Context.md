# Runtime Context

Runtime Context is structured data available during execution.

It is split into session context and run context.

## Session Context

Session context is durable across runs in the same Runtime Session.

Examples:

- conversation history
- user profile snapshot
- long-lived task memory
- accumulated preferences
- durable external correlation ids

Session context should be written intentionally through compiled output bindings
or explicit runtime APIs. It should not become an unstructured dump of all node
outputs.

## Run Context

Run context belongs to one Runtime Run.

Examples:

- invocation input
- node outputs for this run
- temporary derived values
- run result fields
- branch-local data

Run context is safe for Scheduler and NodeExecutor to use without leaking state
across separate invocations.

## Path Shape

The exact storage engine is open, but paths should make scope clear.

```text
invocation.input
session.chat.history
session.user.profile
run.nodes.<node_id>.input
run.nodes.<node_id>.output
run.result
```

Input plans read from these paths. Output bindings write to these paths.

## Read and Write Rules

NodeExecutor should build node input from Workflow IR input plans and Runtime
Context.

Operators should receive explicit input values. They should not freely mutate the
whole session unless the node capability is intentionally designed for that.

Node outputs should always be recorded in the run namespace. Additional writes,
such as appending to conversation history, should be expressed through compiled
output bindings.

## Context Is Not Workflow IR

Context stores values. Workflow IR stores static instructions for how to read or
write those values.

```text
Workflow IR: read session.chat.history into field "history"
Runtime Context: the actual history value
```
