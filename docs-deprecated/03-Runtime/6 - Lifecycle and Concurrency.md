# Lifecycle and Concurrency

Runtime lifecycle rules define how invocations, sessions, and runs move through
execution.

## Run Lifecycle

A Runtime Run should have a small set of lifecycle states.

```text
created -> ready -> running -> completed
                    -> waiting
                    -> failed
                    -> cancelled
```

`waiting` means the run cannot make progress until an external event, timer,
human decision, or callback arrives. A waiting node persists enough correlation
data for AutoAgent App to receive an external event, mark the node completed or
failed, append a transition, and continue the same Runtime Run.

## Session Lifecycle

A Runtime Session can outlive any single run.

```text
created -> active -> idle
                  -> closed
                  -> failed
                  -> cancelled
```

`idle` means no run is currently executing but the session context is preserved
for future invocations with the same session key.

## Default Concurrency Policy

Version 1 uses a conservative rule:

```text
same Runtime Session -> at most one active run by default
different Runtime Sessions -> may run in parallel
```

This prevents two runs in the same conversation or task context from racing to
update shared session context.

Coordinated same-session concurrency is a roadmap item. Until that model is
defined, one Runtime Session should serialize active Runtime Runs even when
different invocations select different entry nodes.

## Same Node, Different Runs

It is normal for different runs to execute the same IR node at the same time.

This is not a graph conflict because IR nodes are static definitions. Runtime
state is keyed by run and node.

```text
(workflow_ir.node_id) is shared
(run_id, node_id) state is isolated
```

The real conflict boundary is shared session context, external resources, and
Operator side effects, not the static IR node object.

## Recovery

Runtime should persist enough state to recover after process failure:

- session lifecycle
- run lifecycle
- node and edge states
- ready/running/waiting sets
- context writes
- event log entries
- idempotency records where used

After recovery, Runtime can inspect persisted state and decide whether to resume,
retry, mark failed, or wait for external intervention.
