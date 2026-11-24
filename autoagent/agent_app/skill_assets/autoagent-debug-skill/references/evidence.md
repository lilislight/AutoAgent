# Read AutoAgent V2 execution evidence

## Select the evidence source

Use `autoagent --project PATH ...` with the project configuration that
points at the execution's database. Use `--env-file PATH` for a separate
local environment configuration. Use `autoagent --server API_URL ...`
for an existing Server, including its API prefix. Choose one source per
command. For machine-readable output, add `--format json`.

Local `session` and `invocation` history reads open an existing database
read-only and can inspect history even if the project source is broken.
Use the report's source and store identity to confirm follow-up reads.

For an Eval failure, use `autoagent eval show RUN --format json`.
Find the Trial in `data.results` and read its `checks`, `roots`,
and `history.database_url`. Point the local history query at that
Trial's database, for example with an `AUTOAGENT_DATABASE_URL` override.
Use the Session or Invocation ID from the Trial. A portable Eval report
can still show its verdict if the referenced execution store has moved.

Keep page sizes small and reuse the returned `next_cursor` with the same
command, filters, and source. Record exact IDs and positions instead of
treating a live paged read as a frozen snapshot. Use `--path` with an
RFC 6901 JSON Pointer to read a needed field. Keep secrets and unrelated
payloads out of the transcript and handoff.

## Follow a Session across Invocations

```bash
autoagent session show SESSION_ID
autoagent session invocations SESSION_ID --limit 20
autoagent session workflows SESSION_ID --limit 20
```

The Invocation listing is in ascending persisted opening order. Its
ordinals stay fixed under filters. Use Workflow, Revision, status, or time
filters when helpful, then inspect the relevant Invocation IDs.

1. Identify the Invocation that showed the symptom and the state or
   prior action it depended on.
2. Find the earlier Invocation expected to establish that state. Compare
   exact Workflow Revisions because a Session can contain multiple
   Workflows or Revisions.
3. Inspect the writer's Output Binding and the reader's Input Mapping.
   Compare the committed state after the write with the state and mapped
   business input at the read. Capture enough of the preceding history to
   establish where the value first diverged. Use the binding stage to
   inspect the proposed patch and historical Context after Node completion
   to establish the committed value.

## Narrow an Invocation

```bash
autoagent invocation diagnose INVOCATION_ID
autoagent invocation show INVOCATION_ID
autoagent invocation nodes INVOCATION_ID --node-id NODE_ID --limit 20
autoagent invocation calls INVOCATION_ID --occurrence-id OCCURRENCE_ID --limit 20
```

Diagnosis summarizes saved failures and waits in the selected subtree.
A completed Invocation can still violate a business rule. A pending Wait
can represent normal progress. Interpret the saved status together with
the user's expected behavior and the available evidence.

| Question | Read |
| --- | --- |
| Which exact run of a repeated Node is involved? | `invocation nodes`, then `invocation node INVOCATION_ID OCCURRENCE_ID`. A Node ID identifies the definition; an Occurrence ID identifies one run. |
| Which Operator attempt failed or ran slowly? | `invocation calls` scoped by Occurrence ID, then `invocation call INVOCATION_ID CALL_ID`. Match Map unit and retry attempt. Use queue and execution duration separately when both are recorded. |
| Which Edge was evaluated or selected? | `invocation edges INVOCATION_ID --edge-id EDGE_ID`; read one evaluation with `invocation edge INVOCATION_ID EVENT_ID:RESULT_INDEX`. Check the recorded routing outcome alongside the condition. `invocation show INVOCATION_ID --graph` gives a persisted graph snapshot, including unconditional routes. |
| Where did a value first change? | Compare the previous output, the target's mapped input, and the Call result. Use `invocation stages INVOCATION_ID OCCURRENCE_ID` and `invocation stage-value INVOCATION_ID OCCURRENCE_ID EVENT_ID --field FIELD --path POINTER` for an exact mapping, aggregation, binding, route, or fault value. |
| What did a Wait request or receive? | Find its ID with `invocation waits`; use the Wait's `request` or `response` value. Use `invocation actions` if the task also needs current action availability. |
| Did a related execution cause the symptom? | Use `invocation family` and `invocation children`. Query the specific Child Invocation by its own identity. |

To read one scoped value:

```bash
autoagent invocation value INVOCATION_ID --kind node --identity OCCURRENCE_ID --field input --path /amount_cents
autoagent invocation value INVOCATION_ID --kind wait --identity WAIT_ID --field request --path /amount_cents
```

Omit `--kind` and `--identity` for an Invocation-level value. Check
`available` as well as the value. An available JSON null and missing
evidence have different meanings.

## Verify historical Context and definitions

```bash
autoagent --project PATH invocation events INVOCATION_ID --event-name EVENT_NAME --limit 20
autoagent --project PATH invocation state INVOCATION_ID --sequence N --context session --path /preferences/limit_cents
autoagent workflow show WORKFLOW_ID --revision-id REVISION_ID --node-id NODE_ID
```

`invocation events` lists headers. Select an exact Event sequence from
the target Invocation for the local-only `invocation state` command.
The result is the state **after** that Event. Use `--context invocation`
for Invocation-local data. For a before-and-after question, identify a
valid Event on each side of the transition.

Event sequences are Session-scoped. One Invocation's listing can have
gaps. For a parent and Child in different Sessions, compare their
recorded creation, response, or settlement relationships and use each
Session's own sequence for historical reads.

Read the saved definition using the execution's Revision. If the same
store has two saved Revisions of the Workflow, use
`workflow diff BASE_REVISION_ID TARGET_REVISION_ID --node-id NODE_ID`
or select `--edge-id` for a focused definition comparison. Match saved
identity and behavior against the current source. Explain any unavailable
history, missing position, or source-version mismatch in the conclusion.
