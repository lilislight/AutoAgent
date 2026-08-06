---
modules:
  - runtime-execution
  - runtime-persistence
  - tracing-server-and-ui
tags:
  - operator-calls
  - runtime-events
  - tracing
  - bounded-projection
---

# Trace Actual Operator Calls

## Intent

Expose every real Operator invocation, including Map and Replication units, without retaining an unbounded duplicate Call graph inside executable Runtime state.

## Outcome

Each actual attempt owns one Operator Call Event with unit identity, timing, and Full-mode input/output. NodeExecution keeps bounded summaries. Trace queries page Calls by NodeExecution, while the Timeline nests at most 50 Call spans under each NodeExecution and reports omitted rows; the Event journal remains complete.

## Reason

A single logical parallel Call hides concurrency, latency, failures, and individual values. Retaining every Call inside recovery state would instead make snapshots and database writes grow with parallel fan-out.

## Impact

Selector, aggregation-input, and Output Binding records no longer repeat Call values. Call inspection loads canonical Event detail, and recovery uses Node-level summaries plus normal Node recovery policy rather than persisted Call objects.
