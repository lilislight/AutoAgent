# Core streaming Operators and User Events

Use a streaming Operator to consume available chunks incrementally and
produce one typed result per Call. Use a User Event Mapper when application
consumers also need progress or committed business notifications.

## Define the stream contract

Import `Node`, `Stream`, and `StreamContext` from `autoagent`. Annotate
the Operator's return type as `Iterator[Chunk]` or `AsyncIterator[Chunk]`
and connect it with `Node(..., stream=Stream(reducer))`. Declare the chunk,
reducer state, and final result with the same value-contract rules used by
[ordinary Operators](workflow.md).

A reducer implements these typed methods:

| Method | Contract |
| --- | --- |
| `initial(context: StreamContext) -> State` | Create per-Call state. |
| `add(context: StreamContext, state: State, chunk: Chunk) -> State` | Fold one validated chunk into the state. Keep the return contract identical to `initial`. |
| `finish(context: StreamContext, state: State) -> Output` | Produce the final Call output. Handle empty input if the source can yield zero chunks. |

`StreamContext.input` contains the mapped input, with the corresponding
Invocation and Session Context views. Keep accumulation in the value
passed between reducer methods. Multiple Map units can share the same
reducer instance. Keep that state bounded when the final output only needs
a summary.

The reducer's final result is the Call output. On a Map Node, each Call
reduces its own stream and the Node's Map aggregation sees those final
results. Downstream Edges, Conditions, and Output Binding operate on the
completed Node output. Keep failure handling at the Node or Call boundary.
Core's automatic Retry only considers eligible failures before the Call
has emitted a chunk. See [recovery policies](interaction-recovery.md)
when choosing how interrupted work can run again.

## Map User Events explicitly

Pass `user_events=mapper` to a Node. A sync or async mapper takes a
`UserEventContext` and returns `UserEventSpec` or `None`. Returning
`None` suppresses that event. Choose a stable `kind` for each
application message and put a JSON value in `payload`.

| `context.phase` | When it is available |
| --- | --- |
| `chunk` | For each validated stream chunk. In a Map, use `unit_index` and `operator_call_id` to associate chunks with their Call. |
| `output` | Once for a successfully completed Node occurrence, after any aggregation and Output Binding. Ordinary Nodes can emit it too. |
| `waiting` | When a native Wait is confirmed. The value includes Wait identity, kind, request, and deadline when applicable. |
| `wait_resolved` | When the Wait is resolved. The value includes its outcome and available response or error. |
| `error` | After a final Node failure, with its stage, error and whether an Error Edge handled it. |

Treat mapper data as read-only. Use `persist=False` for temporary progress
such as deltas. Those User Events have no history sequence. Use the default
`persist=True` for compact business messages that need to be stored by a
configured sink. With a durable Host, an Evaluation can read persisted
messages through [the business view](evaluation.md).

Use Context patches for Workflow state and User Events for observation.
A delivered chunk is partial progress. A committed output can be observed
after the Node succeeds. Exercise both an empty stream and a source failure
when the Operator consumes variable or external input.
