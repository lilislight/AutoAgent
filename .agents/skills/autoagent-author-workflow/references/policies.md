# Execution Policies

This reference owns Workflow, Node, and Edge execution policy selection. It
does not define Hook signatures, graph layout, or persistence internals.

## Workflow failure

`WorkflowPolicy.failure` accepts:

- `FailurePolicy(mode="fail_fast")`: stop after an unhandled branch failure.
- `FailurePolicy(mode="continue_active_branches")`: allow already active
  independent branches to finish.

A downstream path that cannot run after its predecessor fails remains skipped.
The policy does not make dependent work recoverable.

## Capability selection

Use `NodePolicy(selection=CapabilitySelectionPolicy(...))` only with an
abstract `CapabilityRef`.

Modes:

- `default`: use the registered default Operator;
- `priority`: choose by registered Operator priority;
- `first_available`: choose the first eligible Operator deterministically.

Use `preferred_operator_ids` and `excluded_operator_ids` for explicit
constraints. `allow_fallback=False` stops after the selected Operator fails.

Do not configure capability selection for a direct callable or SystemCommand.

## Retry

`RetryPolicy.max_attempts` is the total number of calls to one selected
Operator, including the initial call.

Retry covers:

- Operator handler exception;
- Operator timeout;
- invalid Operator output.

Retry does not cover:

- Input Mapping;
- Map item selection;
- aggregation;
- Output Binding;
- Edge Condition.

Add `BackoffPolicy` when repeated external calls should be delayed. Supported
modes are `fixed`, `linear`, and `exponential`; jitter is `none`, `full`, or
`equal`.

## Fallback

Fallback applies between eligible Operators implementing one Capability. It is
not a general exception handler for deterministic Workflow Hooks.

The selected Operator exhausts its Retry attempts before capability fallback
chooses another Operator.

## Timeout

`TimeoutPolicy(timeout_ms=...)` limits one Operator attempt. Use a positive
duration based on the external service contract. A timeout may be retried when
Retry allows it.

Late results from timed-out synchronous work are discarded and cannot mutate
authoritative Runtime state.

## Crash recovery

`RecoveryPolicy.mode` accepts:

- `never`: interrupt recovery when this Node is reached;
- `replay_safe`: replay the whole Node phase;
- `idempotent`: replay with a stable logical Node idempotency contract.

Recovery is different from live Retry. Recovery may repeat Input Mapping,
Operator calls, aggregation, and Output Binding after process failure.

Use `idempotent` for an external side effect only when the Operator contract can
consume the stable idempotency key required by the compiler. Never label a
non-idempotent payment, message, or mutation as replay-safe merely to make
compilation succeed.

## Resource limits

`ResourcePolicy` provides Invocation-scoped limits:

- `max_node_executions_per_invocation`;
- `max_operator_attempts_per_invocation`;
- `max_runtime_ms_per_invocation`.

Use a Node execution limit to bound every Loop. Operator runtime excludes
mapping, binding, Condition, aggregation, and backoff time.

`NodePolicy.max_concurrency` limits concurrent logical executions of that Node
across Sessions in one App process.

## Replication

`ReplicationPolicy` fields:

- `count`;
- optional `output_aggregator`;
- optional `max_parallelism`.

Count and parallelism must be positive. Replication cannot be combined with Map
for the same target execution.

## Map

`EdgePolicy(map=MapPolicy(...))` fields:

- optional `item_selector`;
- optional `output_aggregator`;
- optional `max_parallelism`.

Map cannot target a fan-in Node, a SystemCommand, a Node with Input Mapping, or
a replicated Node. The App-wide parallel-unit setting remains the hard upper
bound even if Workflow policy requests more.

## Wait and Event mode

Process-local Wait can exist in memory. Durable cross-process Wait/Resume
requires a database and `standard` or `full` Event mode. `minimal` deliberately
does not retain the recovery data required after restart.

Keep Event mode and database selection in CLI/host configuration, not Workflow
source.
