---
name: autoagent-debug-skill
description: Diagnose and verify fixes for AutoAgent V2 Workflow behavior from a Session, Invocation, or failed business Eval. Use for incorrect results, Context carried between runs, unexpected routes, failures, waits, retries, or parent–child execution.
---

# Debug AutoAgent Workflows

Start from the user's expected behavior and the original execution evidence.
Use the target project's installed V2 CLI and the database or Server that
owns that history.

## Choose the starting point

| Starting point | First reads |
| --- | --- |
| Session ID, multi-turn behavior, or state carried between runs | `session show SESSION_ID` and a page of `session invocations SESSION_ID`. Locate the writer and reader of the relevant state. |
| Invocation ID or a problem within one execution | `invocation diagnose INVOCATION_ID`. Use the affected Node, Call, Wait, or Child IDs to narrow further. |
| Failed business Eval | `eval show RUN`. Find the failing Case and named check, then use the Trial's history location and recorded execution IDs. |
| A symptom without IDs | Search a bounded page of `session list` or `invocation list` in the known source. Confirm the match by Workflow, Revision, and time. |

Use [evidence.md](references/evidence.md) for source selection, command
syntax, and historical Context.

## Investigate and verify

1. Record the Session, Invocation, and Workflow Revision implicated in the
   report. For cross-Invocation behavior, preserve Session order and note
   the Revision used by each relevant Invocation.
2. Ask one concrete evidence question at a time. Start with metadata,
   then read the relevant Node occurrence, Call, route, Wait, or stage.
   Select the smallest needed field. Use exact historical positions for
   claims about previous state.
3. Locate the corresponding project definition. Trace the first observed
   deviation through its mapping, Operator, binding, routing, or resource
   boundary. Label incomplete evidence and remaining hypotheses.
4. Keep diagnosis read-only. When the task includes a fix, change the
   smallest responsible project code or configuration. Verify the candidate
   with an isolated Session or Eval Trial and the same business expectations.
   Follow [repair-validation.md](references/repair-validation.md).
5. Report the affected IDs and evidence positions, the cause or best
   supported hypothesis, the repair if requested, and the actual checks
   performed. For a Session issue, name both the Invocation that first
   deviated and the Invocation that exposed the symptom.
