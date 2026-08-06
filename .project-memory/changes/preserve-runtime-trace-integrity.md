---
modules:
  - runtime-execution
  - runtime-persistence
  - tracing-server-and-ui
tags:
  - runtime-events
  - backpressure
  - artifacts
  - wait-resume
related_changes:
  - actual-operator-call-tracing
---

# Preserve Runtime Trace Integrity

## Intent

Keep live execution responsive without silently losing or ambiguously presenting the Runtime evidence needed for inspection and control.

## Outcome

Hard persistence pressure now pauses running Runtime Event producers instead of dropping their journal suffix, while high pressure still stops new admission. Resume claims one waiting Invocation atomically. Operator Call Event names expose their terminal outcomes, and database Artifact values are loaded only through an explicit Invocation-scoped observation query.

## Reason

Database lag is normal asynchronous backpressure, but an Event gap permanently weakens replay and debugging. Conversely, eagerly returning large Artifact payloads would make ordinary trace queries unbounded. Process-loss recovery continues from the durable prefix without adding normal Wait or terminal durability barriers.

## Impact

Runtime and trace consumers must handle all Operator Call terminal names. Long-running Servers should pair database durability with an eviction retention policy so completed live journals do not grow independently of the bounded persistence queue.
