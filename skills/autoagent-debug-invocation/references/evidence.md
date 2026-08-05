# Evidence interpretation

Read this file after the root Report and before assigning ownership.

## Interpret the primary boundary

`PRIMARY_BOUNDARY` is the most precise recorded investigation starting point,
not a root-cause verdict:

- `node`: inspect the NodeExecution, then its Operator Calls or phase Events;
- `edge`: inspect that evaluation and its source Node output/condition phase;
- `operator_call`: inspect reason, state, error, timing, Retry, and Fallback;
- `wait`: the Invocation reached an intentional asynchronous barrier;
- `recovery`: inspect recovered Nodes and any recovery prohibition;
- `invocation`: only an Invocation-level error was recorded.

Completed Invocations may have no primary boundary. For a business-incorrect
result, follow the executed Node/Edge path and compare the final result to the
business oracle instead of treating completion as correctness.

## Respect Event modes

| Mode | Expected evidence |
| --- | --- |
| Minimal | Invocation input/result/state and semantic UserEvents |
| Standard | Node states, Edge evaluations, Operator Call summaries, timing, Wait/Resume, Recovery |
| Full | Standard evidence plus Hook phases, recorded input/output, operations, and state reconstruction |

Missing Full-only detail in Standard or Minimal mode is intentional. Do not
diagnose it as database corruption.

## Read warnings before details

- `INVOCATION_STILL_RUNNING`: the 10-second observation window ended before a
  waiting or terminal boundary. Evidence is valid only through the reported
  sequence.
- `RUNTIME_EVENTS_NOT_FULLY_DURABLE`: the live Server has RuntimeEvents not yet
  written to the database. Prefer the Server; a database-only investigation may
  be incomplete.
- `USER_EVENTS_NOT_FULLY_DURABLE`: the same limitation applies to semantic
  UserEvents.
- `EXECUTION_AGGREGATES_INCOMPLETE`: the available Recovery State could not be
  advanced through a complete Event tail. Use recorded pages carefully and
  state the limitation.

Do not infer missing Events. A sequence gap, unavailable value, or non-durable
tail must remain an explicit uncertainty.

## Classify ownership

Use current code plus evidence to select one owning layer:

- **Workflow graph**: wrong Node/Edge topology, condition, Loop boundary,
  fan-in, Map/Replication, or Wait placement;
- **Hook or callable**: Input Mapping, item selection, aggregation, Output
  Binding, Condition, Tool, or direct callable behavior;
- **Policy**: Retry, Fallback, timeout, recovery, resource, or fail-fast choice;
- **Provider/configuration**: request shape, model capability, credentials,
  endpoint, structured output mode, rate limit, or transport failure;
- **External dependency**: business API, network, database, queue, or human
  response outside AutoAgent;
- **Framework**: compiler, scheduler, Runtime, persistence, Server, or CLI
  violates its documented contract with a minimal reproduction.

Do not change framework internals to mask a project-owned error. When evidence
indicates a framework defect, isolate a minimal reproduction and report it
separately from the Workflow repair.
