# AutoAgent

[中文说明](README_ZH.md)

AutoAgent executes compiled Workflows while keeping the latest Session and
Invocation state in memory. An optional database backend persists selected
runtime facts asynchronously.

Runtime detail is selected for each Invocation:

```python
app.start()
invocation = app.invoke(
    workflow,
    input={"message": "hello"},
    event_mode="standard",  # "minimal", "standard", or "full"
)
```

## Environment configuration

Normal applications do not need to construct `AutoAgentSettings` or call an
environment-loading method:

```python
app = AutoAgentApp()
app.start()
```

`AutoAgentApp()` automatically reads supported `AUTOAGENT_*` values from the
current `.env`, then lets process environment variables override the same
keys. `AutoAgentSettings(...)` is only for explicit programmatic overrides and
tests. The complete supported deployment configuration is documented in
`.env.example`; unrelated Workflow/test variables do not belong there.

Startup is explicit. Register Workflows and runtime codecs/models first, then
call `app.start()` (or `await app.astart()`). Startup initializes the backend,
rebuilds durable waits, and recovers unfinished `created`/`running`
Invocations for registered Workflows. Invoke, submit, and resume never start or
recover the App lazily.

`event_mode` is deliberately an Invocation option rather than an App option.
The same App and Session may choose different modes for different Invocations.
`memory` and `database` are RuntimeStore backend choices; they are not Event
modes.

## Runtime Event modes

| Capability | `minimal` | `standard` | `full` |
| --- | --- | --- | --- |
| Persist Invocation input, state, final result, and error | Yes | Yes | Yes |
| RuntimeEvents | None | Graph/Operator/Wait/Recovery Events | All Standard Events plus internal phase Events |
| Node or Operator intermediate input/output in Events | No | No | Yes |
| StateOperations on Events | No | No | Yes |
| Graph path and timing trace | No | Yes | Yes |
| Cross-process Wait/Resume | No | Yes | Yes |
| Cross-process crash Recovery | No | Yes | Yes |
| Rebuild Runtime State at an arbitrary Event sequence | No | No | Yes |
| Foundation for historical debugging/forking | No | No | Yes |

### `minimal`

Use `minimal` when only the public Invocation lifecycle matters.

- No RuntimeEvent is created or stored.
- The Invocation row retains its input, latest public state, final result, and
  terminal error.
- Wait/Resume works only while the current process and its in-memory
  RuntimeStore survive.
- No genesis state or RecoveryState is persisted, so a process restart cannot
  resume or recover the Invocation.

### `standard` (default)

Use `standard` for production tracing and durable execution without retaining
every intermediate value.

- Records graph movement, Node state, edge decisions, logical Operator calls,
  Wait/Resume, and Recovery facts.
- Records timestamps, elapsed time, wait/queue timing, status, and bounded
  metadata.
- Does not store Node-phase input/output or StateOperations.
- Maintains a compact RecoveryState while the Invocation is recoverable.
  Wait creation forces a recovery point. Terminal Standard Invocations retain
  their latest compact RecoveryState.
- Supports cross-process Wait/Resume and crash Recovery, but cannot reconstruct
  historical Runtime Context at an arbitrary Event.

### `full`

Use `full` when an Invocation must support detailed tracing, state replay,
debugging, or future fork tooling.

- Records every Standard Event.
- Adds user-defined execution phases: input mapping, map item selection,
  aggregation, and output binding.
- May retain phase and Operator input/output.
- Every Event contains ordered StateOperations describing the Runtime State
  delta since the preceding Event.
- Persists a sequence-zero genesis state and a periodically advanced
  RecoveryState.
- Can rebuild Session/Invocation Context and execution state through any
  contiguous Event sequence. The backend foundation exists; a public fork
  service and UI are not yet exposed.

Full mode is intentionally more expensive. Large values may be externalized as
deduplicated ArtifactRefs, but Events and their StateOperations still add CPU,
memory-queue, and database cost.

## RuntimeEvent schema

Sequences are local to one Invocation and start at `1`.

| Field | Meaning |
| --- | --- |
| `invocation_id`, `sequence` | Stable Invocation identity and contiguous local order |
| `event_type`, `event_name` | Broad family and exact semantic fact |
| `subject_type`, `subject_id` | Invocation, Node, edge, Operator call, Wait, or Recovery subject |
| `occurred_at_ms` | Wall-clock time at which the fact occurred |
| `elapsed_ns` | Monotonic duration for a completed operation, when applicable |
| `timing` | Named timing components such as execution, concurrency-slot wait, thread-pool queue, and retry backoff |
| `status` | Normalized state/result used by trace consumers |
| `payload` | Event-specific bounded metadata |
| `input`, `output` | Detailed values; populated only by Full mode when applicable |
| `operations` | Ordered Runtime State deltas; present only in Full mode |

RuntimeEvents are facts recorded after their corresponding state transition or
operation completes. User-defined phases therefore emit one completion Event
with `occurred_at_ms` and `elapsed_ns`, rather than separate start/end Events.

## Event catalogue

| Event type | Event name | Standard | Full | Meaning |
| --- | --- | :---: | :---: | --- |
| `state_change` | `invocation.running` | Yes | Yes | Admission checks and entry/input setup completed; execution starts or resumes |
| `state_change` | `invocation.completed` | Yes | Yes | All selected work completed; payload-free terminal fact |
| `state_change` | `invocation.failed` | Yes | Yes | Invocation reached a terminal failure |
| `state_change` | `invocation.cancelled` | Yes | Yes | Caller cancellation terminated active work |
| `state_change` | `node.running` | Yes | Yes | A concrete NodeExecution was created and marked running |
| `state_change` | `node.completed` | Yes | Yes | Node output and output binding completed successfully |
| `state_change` | `node.failed` | Yes | Yes | Node failed; payload identifies the failed phase when applicable |
| `state_change` | `node.waiting` | Yes | Yes | Node suspended on a Wait request |
| `state_change` | `node.skipped` | Yes | Yes | Scheduler proved this scoped Node occurrence unreachable/unselected |
| `routing` | `edge.evaluated` | Yes | Yes | One edge condition was evaluated, including selected/unselected result |
| `operator_call` | `operator_call.completed` | Yes | Yes | One direct logical call, or one logical map/replication summary, completed |
| `wait` | `wait.created` | Yes | Yes | Invocation reached a stable external Wait; forces a recovery point |
| `wait` | `wait.resumed` | Yes | Yes | A Wait payload was accepted and its NodeExecution continued |
| `recovery` | `recovery.requeued` | Yes | Yes | Interrupted recoverable NodeExecutions were requeued |
| `recovery` | `recovery.node_skipped` | Yes | Yes | Recovery policy rejected one branch while other branches may continue |
| `recovery` | `recovery.interrupted` | Yes | Yes | Recovery stopped because the Workflow or Node policy forbids continuation |
| `phase` | `input_mapping.completed` | No | Yes | Input mapping completed or failed; Full output is the mapped Node input |
| `phase` | `item_selection.completed` | No | Yes | A custom map item selector completed or failed |
| `phase` | `aggregation.completed` | No | Yes | A custom map/replication aggregator completed or failed |
| `phase` | `output_binding.completed` | No | Yes | Isolated Context binding committed, or failed and rolled back |

An absent optional hook does not emit its phase Event. For example, a MapPolicy
without a custom item selector has no `item_selection.completed` Event, and a
Node without output binding has no `output_binding.completed` Event.

## Persistence behavior

The in-memory RuntimeStore is always the authoritative latest state. With a
database backend, immutable persistence envelopes are queued and serialized on
the persistence thread; ordinary Workflow execution does not wait for each SQL
write. Database initialization must succeed before the App accepts work. A
database failure after startup is reported through persistence health/logging
while in-memory execution continues until the configured hard backlog limit
prevents new Invocation admission.

Invocation completion does not imply that every Event is already durable.
Consumers should distinguish live Invocation state from its durable persistence
cursor.

## Tracing Server and UI

`AutoAgentServer` can run as a complete service or contribute its Router to an
existing FastAPI application:

```python
from fastapi import FastAPI
from autoagent.core.server import AutoAgentServer

server = AutoAgentServer(app)
server.run()                    # standalone API + UI when ui/dist exists

host = FastAPI()
host.include_router(server.router)  # embedded API; Router owns App lifecycle
```

The V1 tracing API lives under `/api/v1`. Workflow, Session, and Invocation
directories use opaque cursor pagination. An Invocation trace bootstrap returns
the exact Workflow revision, a compact graph projection checkpoint, and at most
the requested tail page of RuntimeEvents. Older Events are loaded one page at a
time; live Events and Invocation status use SSE. Full Event input/output and
historical Runtime state are separate, on-demand requests, and StateOperations
are never included in ordinary UI Event pages.

The tracing UI preserves the Workflow/Session/Invocation scope selector, graph
canvas, resizable Inspector, and collapsible Timeline. The Inspector exposes
definition, contracts, policy, graph state, timings, Operator calls, edge
evaluations, Wait/Resume, and lazy per-Event detail. Fork and AI-assisted design
remain explicit future capabilities rather than placeholder behavior.
