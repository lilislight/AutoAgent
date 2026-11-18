# Repair and verify Workflow behavior

## Match the repair to the earliest observed deviation

| Evidence | Inspect |
| --- | --- |
| The original input is correct and the mapped input is wrong | Input Mapping, its incoming Edge values, and the Context visible at that point. |
| The mapped input is correct and the result is wrong | The selected Operator or Capability implementation, its contract, and supplied resources. In an Agent Workflow, inspect the request, Tool result, or applicable policy at that boundary. |
| A later Invocation receives the wrong state | The earlier Output Binding or Session reuse, then the later mapping. Check the historical value at both positions. |
| The output is correct and the path is wrong | The actual route, relevant conditions, and the timing of state commits or effects. |

Keep the original evidence intact. Locate the smallest responsible code
or configuration change using the Workflow Revision and affected IDs.
For authoring changes, consult the `autoagent-author-skill` when available.

## Reproduce in isolation

For a single Invocation, use the original Entry contract and relevant
inputs in a fresh development Session. For a Session issue, recreate
the minimum sequence of Invocations in a separate Session. Preserve
their ordering, the important Context transitions, and any relevant
Workflow or Revision changes.

Use fixtures for external effects, human replies, and nondeterministic
services. Use real mutations only within the authority and environment
specified for the task. Resume an original Wait or recover an original
execution only when the user requests that operational change. To
study a historical cut, `invocation fork INVOCATION_ID --sequence N --plan`
can show replay requirements. An actual Fork may run Operators. Use a
fresh run against the candidate Revision to verify edited code.

## Verify the business rule

1. Run `project check` and a focused `workflow check WORKFLOW_ID` (or
   compile the affected Node or Edge). Use an existing relevant Eval Case
   or add a focused regression Case when the requirement needs one.
2. When possible, capture a baseline for the original code and a
   candidate report using the same Case inputs, expectations, fixture
   conditions, and rule versions. Keep each run in a different directory.
3. In an Eval Scenario, use the same `session` alias on successive
   `context.invoke(...)` calls to reproduce Session reuse. Capture the
   relevant observation at each step. Use `ExecutionView` for named
   business checks on output, intermediate values, routes, and Context.
4. Compare the named checks and relevant observations. A correct final
   output can coexist with incorrect intermediate behavior. Confirm the
   changed rule from direct evidence and check that other relevant
   rules still pass.

```bash
autoagent eval check SUITE_ID
autoagent eval run SUITE_ID --case CASE_ID --run-dir baseline
autoagent eval show baseline/report.json
autoagent eval run SUITE_ID --case CASE_ID --run-dir candidate
autoagent eval compare baseline/report.json candidate/report.json
```

Run the baseline and candidate against their respective code. A failed
baseline is expected when the fixture reproduces the defect. If reports
are incomparable or evidence is incomplete, report that explicitly and
state which comparison or business claim can still be verified.
