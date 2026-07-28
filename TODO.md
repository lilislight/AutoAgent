# AutoAgent TODO

Only unfinished work is kept here. Completed stage items should be removed
instead of archived in this file.

## MVP 1: Agent Authoring Foundation

Complete these items in order:

1. Write the standard Authoring Skill against only the locked public API,
   project manifest, normative examples, and CLI workflow.
2. Add the MVP2 Agent-friendly Invocation report and progressive Event queries
   only after the Authoring Skill and normative examples are validated.

## Stage 4: Runtime Events and Observability

- Add validated Runtime profiles without forking Scheduler, WorkflowExecutor,
  or NodeExecutor implementations:
  - `LITE`: keep only the authoritative in-memory state during execution,
    asynchronously persist a terminal Invocation projection, and provide no
    crash recovery or durable fork history. Process-local wait/resume may
    remain available, but it is lost on restart.
  - `DURABLE`: persist the minimal node-level journal and durability barriers
    required for crash recovery and durable wait/resume, without the complete
    fork/debug boundary history.
  - `DEBUG`: persist the full fork-boundary journal used for replay, branching,
    recovery, and future trace/debug tooling.
- Introduce one Runtime `CommitPolicy` behind a shared boundary-commit API.
  Profiles may decide whether a boundary is eventized, retained, persisted, or
  used as a durability barrier; they must not create separate execution paths.
- Validate profile dependencies instead of exposing unconstrained booleans:
  fork requires genesis plus fork Events and Workflow version metadata;
  recovery requires its recovery journal/checkpoint data; durable wait/resume
  requires a persisted wait boundary and durability barrier.
- Feed the unified Runtime Event protocol to optimizer inputs after event types
  have been exercised by real workloads.
- Add terminal Invocation memory eviction only after all Runtime Events are
  durable and no local subscriber still needs the aggregate. A later lookup
  must rebuild it from the Invocation genesis/recovery state plus Events
  through RuntimeStore.

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

## Hosted Runtime (Deferred)

- Before multiple runner processes can own the same durable RuntimeStore, add
  per-Invocation leases with fencing tokens and idempotent takeover. Local V1
  intentionally has no distributed ownership protocol.
