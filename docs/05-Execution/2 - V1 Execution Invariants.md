# V1 Execution Invariants

This document records the execution rules implemented by the current V1 code.
Compiler, Scheduler, Executor, RuntimeStore, and future integrations must
preserve these rules.

## Invocation and Ownership

- `AutoAgentApp.ainvoke()` is the native execution entry. `invoke()` is only a
  synchronous adapter and must not be called from an active event loop.
- One Session may have only one `created`, `running`, or `waiting` Invocation.
  Different Sessions may execute concurrently.
- WorkflowExecutor is the only control loop connecting Scheduler, NodeExecutor,
  runtime mutation, and persistence. Worker tasks return results and never
  mutate Invocation state directly.
- Each Invocation owns its task mailbox. Results from cancelled or failed work
  cannot be applied to another Invocation.

## Scheduling and Graph Progress

- Scheduler consumes only stable NodeExecution transitions. It never observes
  or waits on running worker tasks.
- All currently ready nodes are submitted without a batch barrier. A fast
  branch may advance while another branch is still running.
- For an acyclic source node, every outgoing condition is evaluated. Every true
  edge is selected; every false edge is skipped.
- A multi-input target is resolved only after all incoming edges are selected or
  skipped. It executes once when at least one incoming edge was selected and
  receives the exact selected activations; otherwise its path is skipped.
- A loop is a compiler-derived, single-entry region. Each completed loop
  execution must select exactly one internal or exit edge. Selecting zero or
  multiple edges fails the Invocation.
- Any unhandled NodeExecution failure is fail-fast: remaining ready, running,
  and waiting work is cancelled and detached.

## Inputs, Outputs, and Hooks

- Every OperatorCall input is a named-argument `Mapping[str, Any]`. InputMapping
  must return one Mapping. MapPolicy `item_selector` must return an iterable of
  Mappings, one per OperatorCall.
- InputMapping, item selection, condition, aggregation, and output binding may
  be synchronous or asynchronous.
- InputMapping and condition contexts are read-only. Output binding may mutate
  only InvocationContext and SessionContext data.
- A NodeExecution publishes one logical output. For map and replication, this
  output exists only after all calls succeed and aggregation completes.
- Mapping, item selection, condition, aggregation, and binding failures are
  deterministic framework-stage failures. They never enter retry or fallback.

## Operator Failure Policies

- Retry applies independently to each selected Operator. After its attempts are
  exhausted, capability fallback may try the next eligible Operator.
- Operator handler exceptions, timeout, and invalid Operator output participate
  in retry and fallback. Each call is recorded as normal, retry, fallback,
  map item, or replica.
- Map and replication are all-or-nothing. One failed call cancels remaining
  calls when possible, skips aggregation, and fails the logical NodeExecution.
- Timeout cancels an asynchronous Operator task. A synchronous Operator already
  running in the thread pool cannot be force-stopped; its late result is ignored
  and cannot modify runtime records.
- Node execution count, OperatorCall count, and measured Operator runtime limits
  are scoped by node id within one Invocation. Retry, fallback, map, and
  replication all consume OperatorCall quota.

## Waiting, Cancellation, and Persistence

- Waiting is emitted only when no further graph progress is possible. Runtime
  retains generic waiting records and `resume`/`aresume`. V1 supports only
  `SystemCommand(id="wait")`; it bypasses Operator resolution and creates no
  OperatorCall. Its named input accepts optional `wait_key`, `wait_type`, and
  `payload`. The NodeExecution UUID is used when `wait_key` is omitted.
- Resume completes the original waiting NodeExecution. The supplied resume
  output becomes its logical output, output binding runs once, and Scheduler
  continues from that node. An active wait key is unique within an Invocation
  and cannot be consumed twice. TimerPolicy remains deferred.
- Caller cancellation marks the Invocation and active NodeExecutions cancelled,
  cancels asynchronous tasks, detaches synchronous work, and persists the final
  state before re-raising `CancelledError`.
- InMemoryRuntimeStore rebuilds Sessions, Invocations, contexts, scheduler
  cursors, NodeExecutions, OperatorCalls, outputs, and waits from records.
  Durable transactions and process-restart wait/resume require the future
  SQLite RuntimeStore.
