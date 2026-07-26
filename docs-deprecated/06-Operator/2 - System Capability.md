# System Capability

This document defines the boundary between Operators and system capabilities.

A system capability is a runtime control capability. It is still referenced by a
normal Workflow node through a string capability name, but its execution
requires special AutoAgent runtime participation.

## Operator vs System Capability

An Operator is ordinary computation or side effect:

```text
explicit input -> output or error
```

A system capability is reserved for behavior that cannot be represented as
ordinary Operator input/output without special Runtime participation.

Use an Operator when the capability can complete by returning a value or raising
an error. Use a system capability only when executing the node must directly
affect Runtime Run lifecycle, node status, or wait/resume behavior.

## Version 1 System Capabilities

Version 1 system capabilities are wait/resume capabilities:

```text
system:wait_event
system:wait_human_input
system:wait_callback
system:wait_timer
```

`system:wait_event` is the general form. More specific capabilities such as
`system:wait_human_input`, `system:wait_callback`, and `system:wait_timer` may be
implemented as typed wrappers or aliases.

## Wait Flow

System wait capabilities create durable waiting points.

```text
NodeExecutor executes system wait capability
    -> StateManager marks node waiting
    -> WorkflowExecutor returns a run handle
    -> AutoAgentApp.receive(...) or resume(...) receives an event later
    -> waiting node becomes completed or failed
    -> transition_queue receives the node transition
    -> WorkflowExecutor continues the run
```

This behavior is not a blocked thread. It is persisted runtime state plus a
future resume event.

## Non-System Behavior

The following behavior should not require core system capabilities:

| Behavior | Preferred mechanism |
| --- | --- |
| Return a run result | exit node plus output binding |
| Fail a branch | node failure plus failed-trigger edge or Workflow failure policy |
| Read context | input plan |
| Write context | output binding |
| Append conversation history | output binding or explicit runtime API |
| Checkpoint state | StateManager persistence |
| No-op placeholder | ordinary no-op Operator or graph structure |

This keeps the system capability surface small and prevents overlap with
Operators.

## Registry

System capabilities should be resolved through the configured execution
environment.

Compiler may use that environment for static descriptors. NodeExecutor uses the
compiled executable binding to call the system capability handler and receive
state changes.
