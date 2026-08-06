---
name: autoagent-debug-invocation
description: Investigate, rerun, compare, repair, and validate an AutoAgent Workflow problem starting from an Invocation ID. Use when a user reports a failed, interrupted, cancelled, waiting, slow, expensive, or business-incorrect Invocation; asks why a Workflow took a route, retried, fell back, timed out, called a Tool, or produced an unexpected result; or wants a focused code repair backed by Invocation Report, Rerun, Comparison, and project Evaluation evidence. Do not use for general Workflow authoring without an Invocation, AutoAgent framework-internal development, Tracing UI implementation, or Fork/Replay work.
---

# Debug an AutoAgent Invocation

Use the compact Invocation Report first, then request only evidence needed to
answer the next concrete question. Treat Runtime evidence as recorded facts,
not a generated root-cause conclusion.

## Follow this workflow

1. Read repository guidance applying to the target project.
2. Obtain the exact Invocation ID. Make the Report the first AutoAgent CLI
   command in the investigation:

   ```bash
   autoagent invocation report <invocation-id>
   ```

3. If that command is unavailable or cannot resolve authoritative evidence,
   fix only the package/environment or evidence-source configuration needed to
   retry it. Do not create an App, start a Workflow, or run Project/Workflow
   checks before the initial Report.
4. Read the state, Event mode, primary boundary, persistence status, counts,
   available evidence, and every warning. If no authoritative source is found,
   ask for the matching running Server or database configuration; do not guess
   from unrelated local state.
5. Form one evidence question, such as “which Node failed?”, “which Edge was
   selected?”, “which attempt timed out?”, or “what Context path changed?”.
   Load only the relevant page or detail. Follow
   [cli.md](references/cli.md).
6. Preserve the Report's observed sequence in follow-up queries. Follow opaque
   cursors until the required item appears; do not dump every page by default.
7. Classify the owning layer using recorded evidence and current project code.
   Follow [evidence.md](references/evidence.md).
8. Inspect the smallest relevant Workflow definition, Hook, Operator, Tool,
   model/provider setup, or external dependency boundary. Reports do not need
   source-line metadata; use stable Workflow, Node, Edge, and Operator IDs to
   locate current code.
9. Make the smallest code or configuration repair in the owning layer. Follow
    [repair.md](references/repair.md). When changing Workflow authoring code,
    use the `autoagent-author-workflow` Skill if it is available.
10. When execution is safe, rerun the original start boundary against the
    current project Revision, then compare the original and candidate
    Invocations. Rerun executes real Operators and Tools; obtain authority
    before repeating external side effects. Follow [cli.md](references/cli.md).
11. Use an existing Eval Case when it expresses the business requirement. Add
    or update the smallest Case only when the requirement is absent and should
    remain a regression guard. Do not turn every incident into an Eval.
12. Run Project Check, the affected Workflow Check, Eval Check, and the
    relevant Eval Suite. Run focused unit tests only for isolated project-owned
    helper logic.
13. Report the factual cause, changed files, Rerun/Comparison evidence,
    validation, remaining uncertainty, and whether the original Invocation had
    incomplete or non-durable evidence.

Ask one focused question only when the missing source, business oracle, or
external side-effect authority blocks a safe conclusion. Otherwise continue
with the smallest evidence-driven step.

## Keep investigation bounded

- Start with `invocation report`; never begin by loading the entire Event
  journal.
- Query list metadata before one detail. Use `--limit 20` unless a smaller page
  is sufficient.
- Keep `--through-sequence` fixed while paging an active Invocation.
- Prefer semantic UserEvents over stream deltas. Load deltas only for a stream
  transport problem.
- Use Full Runtime State only for a specific JSON-pointer path or state-boundary
  question.
- Do not print secrets or large raw values. Preserve redaction and ArtifactRef
  boundaries.

## Respect current product boundaries

- `report`, `query`, and `compare` are read-only and are never persisted as
  Runtime data.
- `rerun` creates a new isolated Session and Invocation in the source Standard
  or Full mode; it never mutates the source Session or Invocation. Minimal mode
  cannot be rerun.
- `compare` requires matching Standard modes or matching Full modes. Minimal
  and mixed-mode comparisons are rejected.
- A waiting Invocation is a stable investigation boundary; do not Resume it
  without user authority and a known response.
- Minimal mode intentionally lacks Node, Edge, Operator Call, Recovery, and
  historical state evidence.
- Standard mode records graph flow and durable Recovery but not Hook-phase
  values or historical state operations.
- Full mode supports detailed phases and state reconstruction at recorded Event
  boundaries.
- Map and Replication Calls are actual paged attempts. Filter them by
  NodeExecution when investigating one Node; do not infer individual Calls
  from the bounded Node summary or the UI's 50-row Timeline projection.
- Rerun starts at the entry Node; it is not Replay or Fork and does not infer a
  root cause or business verdict.
- Do not mutate the original Invocation or database while investigating it.
- Do not weaken, delete, or rewrite an existing Eval oracle merely to make the
  repaired Workflow pass.

## Return a compact handoff

State the Invocation ID and observed sequence, the recorded failing or expensive
boundary, the owning project layer, the repair, and exact checks/Evals run. Name
warnings or evidence gaps that limit certainty. Distinguish a proven cause from
a remaining hypothesis.
