# Invocation debugging CLI

Read this file when selecting or running a Report/query command.

## Resolve the evidence source

All commands run from the owning AutoAgent project. The default source order is:

1. a matching running Server that can resolve the Invocation and whose Workflow
   belongs to the current Manifest;
2. the explicit `AUTOAGENT_DATABASE_URL` durable database.

Force one source only when needed:

```bash
autoagent invocation report <invocation-id> --source server
autoagent invocation report <invocation-id> --source database
```

Use `--server-url` to override Server discovery. A database query is read-only:
it does not create a missing SQLite file, recover an Invocation, or import
historical user models.

## Start with the Report

```bash
autoagent invocation report <invocation-id>
```

The command waits through Server notifications for up to 10 seconds when the
Invocation is `created` or `running`. It returns immediately for `waiting` or a
terminal state. A still-running result includes
`INVOCATION_STILL_RUNNING`; rerun the Report later if a newer boundary matters.

Use `--report-file <path>` to tee the same stdout result to a normal file.

## Query collections

```bash
autoagent invocation query <id> nodes --through-sequence <n> --limit 20
autoagent invocation query <id> edges --through-sequence <n> --limit 20
autoagent invocation query <id> operator-calls --through-sequence <n> --limit 20
autoagent invocation query <id> runtime-events --through-sequence <n> --limit 20
autoagent invocation query <id> user-events --limit 20
```

When `HAS_MORE true`, pass the opaque `NEXT_CURSOR` to the same query:

```bash
autoagent invocation query <id> nodes --cursor '<cursor>'
```

Do not reuse a cursor with another Invocation, query kind, filter, or explicit
sequence boundary. The CLI rejects modified or mismatched cursors.

Built-in `message_delta`, `reasoning_delta`, and `tool_call_delta` events are
excluded from the normal UserEvent page. Include them only for stream transport
diagnosis:

```bash
autoagent invocation query <id> user-events --include-stream-deltas
```

## Query one detail

Use identifiers returned by a collection page:

```bash
autoagent invocation query <id> node <node-execution-id> \
  --through-sequence <n>
autoagent invocation query <id> edge <edge-evaluation-id> \
  --through-sequence <n>
autoagent invocation query <id> operator-call <operator-call-id> \
  --through-sequence <n>
autoagent invocation query <id> runtime-event <sequence> \
  --through-sequence <n>
autoagent invocation query <id> user-event <sequence>
```

List items contain bounded metadata. Detail results contain bounded value
summaries, structured errors, timing, and value-presence flags rather than an
unbounded raw Trace dump.

## Query Full Runtime State

The root query summarizes Session Context, Invocation Context, result, and Node
count at one sequence:

```bash
autoagent invocation query <id> runtime-state --through-sequence <n>
```

Address one value with a JSON pointer:

```bash
autoagent invocation query <id> runtime-state \
  --through-sequence <n> \
  --path /invocation/context/data/customer_id
```

This query is valid only for Full mode. It still returns a bounded, redacted
`ValueSummary`; it does not print an arbitrarily large raw object.

## Rerun the source boundary

After a focused repair, run the current project Revision with the original
input, entry Node, and pre-invocation Session Context:

```bash
autoagent invocation rerun <source-invocation-id>
```

Rerun creates a new isolated Session and Invocation in the source Standard or
Full mode. It executes real Operators and Tools. Do not run it without
authority when external side effects may be repeated. A Rerun stops normally
at a Wait and never reuses historical Resume responses automatically. Minimal
mode is rejected because it has no executable Genesis Session boundary.

Use `--server` to execute through the configured Server. Without it, the
current project and configured durable database provide the candidate Revision
and source boundary.

## Compare observed Invocations

```bash
autoagent invocation compare <baseline-id> <candidate-id>
```

Comparison is read-only and requires two Standard Invocations or two Full
Invocations. Minimal and mixed-mode comparisons are rejected. It reports
request, outcome, graph, Operator summary, UserEvent, and timing differences,
and aligns Loop and parallel work by semantic execution identity rather than
Event sequence. `comparable`, `partially_comparable`, and `incompatible`
describe evidence compatibility, not business success. Use the returned
evidence refs with `invocation query` for the smallest differing boundary.
