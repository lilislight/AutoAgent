# Repair and validation

Read this file after locating the owning layer.

## Choose the verification path

Use an existing Eval when it already expresses the failed business requirement.
Add or update one focused Eval Case when all are true:

- the behavior is a stable business requirement;
- the Suite does not already protect it;
- representative input can be synthetic, redacted, or safely sandboxed;
- the Evaluator has a real business oracle.

Do not add an Eval only to preserve one transient outage, nondeterministic
provider response, or implementation detail. Use a focused unit test for a
nontrivial isolated Hook/Tool helper when the complete business path does not
need repetition.

## Make the smallest repair

- Preserve public Workflow input and output unless the business contract must
  change.
- Keep stable Workflow, Node, Edge, Tool, and Evaluator IDs when semantics are
  unchanged.
- Fix graph routing in Workflow definitions, data transformation in the owning
  Hook, and retry/timeout behavior in Policy rather than adding compensating
  logic elsewhere.
- Treat external side effects as potentially non-idempotent. Do not replay,
  Resume, or retry them without an explicit safety contract.
- Keep secrets out of code, Eval fixtures, Context, CLI output, and reports.
- Do not edit persisted Runtime evidence.

## Validate deterministically

Run from the project root:

```bash
autoagent project check
autoagent workflow check <workflow-id>
autoagent eval check <suite-id>
autoagent eval run <suite-id>
```

Run focused unit tests only when project-owned helper logic needs separate
coverage. Use real model/provider behavior in Eval only when that behavior is
the subject under evaluation; otherwise use a deterministic test Provider or
sandboxed dependency.

Use `invocation rerun` only after side-effect safety is established. Then use
`invocation compare` to describe observed differences; do not call a changed
result an improvement until the relevant Eval passes. Fork and Replay remain
unsupported. Retain both Invocation IDs and their observed sequences in the
handoff.

## Handoff fields

Return:

- original Invocation ID and observed sequence;
- recorded primary boundary and structured error;
- owning layer and evidence supporting it;
- changed Workflow/project files;
- Project/Workflow/Eval commands and outcomes;
- Rerun candidate ID, Genesis boundary fidelity, and Comparison differences;
- any unverified provider, external dependency, partial durability, or missing
  Event-mode evidence.
