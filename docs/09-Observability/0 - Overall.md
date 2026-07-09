# Overall

This document defines the Observability and Visualization module.

AutoAgent OS keeps Runtime Session, Runtime Run, node state, edge state,
resource usage, and event logs as first-class runtime data. Observability should
use that data to provide a live view of Workflow execution.

## Purpose

The observability layer should help developers inspect, debug, and operate
Workflow runs.

It should expose:

- Workflow graph state
- node status and timing
- edge evaluation and routing results
- node input, output, and errors
- retry, timeout, and resource usage
- waiting nodes and resume events
- Runtime Session context changes
- Runtime Run event timeline

## Runtime Graph View

The primary visualization should be a graph view of the compiled Workflow IR
overlaid with Runtime Run state.

```text
pending -> ready -> running -> waiting
                         -> completed
                         -> failed
                         -> cancelled
```

Edges should show whether they were evaluated, selected, skipped, or failed.

## Event Timeline

Each Runtime Run should expose an ordered event timeline.

Examples:

- invocation admitted
- run created
- node marked ready
- node dispatched
- Operator started
- Operator completed or failed
- node entered waiting
- external event received
- run completed or failed

## Boundary

Observability reads runtime state and event logs. It should not own scheduling,
node execution, or Workflow mutation.

Optimizer may use the same runtime evidence, but observability is for inspection
and operation. Optimizer is for proposing Workflow changes.
