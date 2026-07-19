# AutoAgent TODO

Only unfinished work is kept here. Completed stage items should be removed
instead of archived in this file.

## Stage 4: Runtime Events and Observability

- Feed the unified Runtime Event protocol to optimizer inputs after event types
  have been exercised by real workloads.

## Stage 5: Higher-Level Agent Features

- Add `AgentNode` through `add_node` as compile-time syntax sugar over an
  expandable child Workflow; do not add AgentRef.
- Add tool loops, memory integration, reusable harnesses, and dynamic fan-out
  without changing Scheduler or Runtime core semantics.

## Stage 6: Optimizer

- Start optimizer implementation only after Runtime Event and observability
  contracts are stable.
- Define optimizer hot-patch version promotion. Manual mutation after App
  compilation is rejected in V1; future optimizer patches should create a new
  workflow version/snapshot, trigger recompilation, and let the UI fetch the
  updated workflow graph/version explicitly.
- Produce reviewable Workflow patches instead of mutating active executions.
- Evaluate patches against recorded runtime data before promotion.
