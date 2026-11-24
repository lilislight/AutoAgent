# Native interaction and recovery

Use a native command as a Node's executable when a Workflow needs to wait for
an external response, use a deadline, or coordinate with Child Workflows.

## External approval

`Wait(Request, Response)` exposes a typed request and suspends the Node.
The submitted response becomes the Node's output. Route approval and rejection
from that output. Save any business data needed after the response via an
earlier Output Binding or include the relevant data in the response contract.
Put approved side effects on the approval path.

```python
from typing_extensions import TypedDict

from autoagent import Node, Wait, Workflow


class ApprovalRequest(TypedDict):
    order_id: str


class ApprovalResponse(TypedDict):
    approved: bool


def approval_workflow() -> Workflow:
    return Workflow(
        "approval",
        nodes=[Node("approve", Wait(ApprovalRequest, ApprovalResponse))],
    )
```

The request and response are separate input/output contracts. Use the
Invocation ID and the exact Wait ID when replying. A durable Host can retain
this waiting position between CLI calls. See [validation.md](validation.md)
for the response command.

## Timer and Select

Use `Timer()` with a mapped `TimerRequest`. Set exactly one of
`delay_us` and `deadline_at_us`. Both use microseconds, and the absolute
deadline is a Unix timestamp. The Runtime awakens the Timer at its deadline.

Use `Select` when one of several named cases should advance the Workflow.
A `SignalCase` waits for a declared Signal Endpoint. A `TimerCase` gives
the operation a deadline. A `ChildCase` observes a Child. The result's
`case` field identifies the selected branch. Give downstream Conditions
or Input Mappings an explicit interpretation of each case's value.

## Parent and Child Workflows

- Supply a Workflow and its Entry Node ID to `Spawn`. The Node returns a
  `RuntimeHandle` that subsequent Nodes can use for coordination.
- Use `Await(child_workflow, entry_node_id)` to start and observe a Child in
  one Node, or `Await()` to observe a handle from an earlier Node. Inspect
  the `RuntimeObservation` status and pending waits before interpreting its
  output. A Child's failed status can be a business branch in the Parent.
- Use typed `SignalEndpoint` declarations and `SendSignal`,
  `ReceiveSignal` or `AwaitSignal` for messages. A Signal can satisfy a
  Signal wait. An external `Wait` uses an explicit typed response.

Define ownership and termination for each Child. Keep data needed across the
boundary in typed inputs, typed messages or explicit outputs.

## Retry, timeout and recovery

| Policy | Use |
| --- | --- |
| `RetryPolicy` | Retry a known, eligible Call failure. `max_attempts` counts extra attempts. Match specific exception class names via `retry_on` and use `BackoffPolicy` for delay. |
| `TimeoutPolicy` | Bound a Node Call's execution duration. Ordinary synchronous work may continue after a deadline has been recorded. Use `TimerRequest` for a Timer deadline. |
| `ResourcePolicy` | Bound how many times a Node can execute in one Invocation. |
| `Recovery` | Control whether uncertain work can run again after interruption. The default is `Recovery(mode="never")`. |

Recovery resumes from recorded Node and Operator boundaries. Declare
`Recovery(mode="replay_safe")` only when the operation can safely run again.
Use it for deterministic computation or for a side effect protected by an
enforced idempotency key that remains stable across repeated attempts. Apply
the same side-effect reasoning before enabling Retry. Test both successful
and rejected or failed business paths.
