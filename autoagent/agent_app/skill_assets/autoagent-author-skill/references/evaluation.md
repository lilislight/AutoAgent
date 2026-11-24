# Evaluate Workflow business behavior

Use `autoagent.evaluation` when a requirement needs evidence from an
actual Workflow run. Start with the expected business outcome and any
intermediate invariant that would still matter after a refactor.

## Define Cases and a Scenario

| API | Use |
| --- | --- |
| `EvalCase(id, input, expected)` | Give each business situation a stable ID. Use JSON-compatible inputs and expected values. |
| `EvalSuite(id=..., cases=..., scenario=..., evaluators=..., conditions=...)` | Group Cases and rules. Declare explicit comparison conditions. |
| `async def scenario(context)` | Drive the Workflow with `ScenarioContext` and return the observation to assess. |
| `CheckResult(name, passed, reason)` | Record a stable rule name, its verdict and a short explanation with the relevant expected and actual values. |

Call `await context.invoke(workflow_id, context.case.input,
entry_node_id=entry_id)`. It returns an `ExecutionView` and defaults to
waiting for a terminal outcome. Use distinct `session` aliases for
independent Sessions or reuse one alias to exercise multi-Invocation
Session Context. Return the view that represents the business outcome
you intend to grade.

For an approval, invoke with `until="boundary"` to observe a stable
position. In a parallel flow, invoke with `until="none"` and use
`await context.wait_for(view, interaction_node=node_id)` to wait for a
specific approval. Read the actual request through
`await view.interactions(node_id=node_id)` and reply with
`await context.respond(view, response, node_id=node_id)`. Provide the
exact `wait_id` when the Node can have multiple pending Waits. If the
Scenario needs to reply to several independent Waits, choose
`until="none"` on intermediate replies and observe the terminal outcome
after the interactions are complete.

Use `context.check(CheckResult(...))` when checking a business condition
at an intermediate position in the Scenario. Use the Suite's sync or async
Evaluators for assessment against a captured observation. Keep the input and
interaction responses in the Case or its fixture, and use `expected` for
the separate business expectation.

## Read business evidence

An Evaluator uses `context.case` for the expectation and
`context.execution` for the fixed observation. Built-in rules include
`StatusIs()`, `OutputEquals()`, `NodeExecuted(node_id)`, and
`RouteTaken(edge_id)`. For a custom rule, return a named `CheckResult`.

| Business question | Query |
| --- | --- |
| What was the final value? | `await execution.output(pointer=...)` or `OutputEquals(pointer=...)` against the Case's expected value. |
| What did the Node receive and produce? | `await execution.node(node_id).one()`, then `await run.input()` or `await run.output()`. Choose `all()`, `first()`, or `last()` explicitly for repeated occurrences. Use `await run.items()` for Map units. |
| Was a route actually adopted? | `await execution.route(edge_id).was_taken()`. Use `state()` for a single decision or `decisions()` when repeated. |
| What was in Context at this step? | `past = await execution.at(run.completed)`, then `await past.context(scope="session", pointer=...)`. Use `scope="invocation"` for Invocation Context. |
| What happened in a Child or a business message? | Select Child views with `execution.children(workflow_id=...)`. Read persisted messages with `await execution.messages(kind=...)` and `await message.body()`. |

Treat the view and fixture observations as read-only. Prefer business facts
such as the approved amount, selected route, and charge count. Use JSON
Pointers for the fields a rule needs. A negative check requires sufficient
evidence. Distinguish a reached and rejected route from a source the
Workflow never reached when the business rule cares about that difference.

Use an isolated per-Trial fixture to observe real business effects such as
writes, duplicate requests, or recorded payments. A fixture can supply
resources through `context.resources`. A custom `host_factory` can bind
these resources to the Workflow and use the Trial's
`EvaluationEnvironment.database_url`. A Scenario sets up and performs
actions. An Evaluator reads the resulting facts.

Keep Workflow status and the Eval verdict distinct. An expected Workflow
failure can pass a business rule that verifies rejection. A missing
observation or an evaluator exception requires investigation. A
`CheckResult` with `passed=None` leaves its rule ungraded.

## Register, run and compare

Export an `EvalSuite` or a zero-argument factory and register its
`module:attribute` in a `[[evaluations]]` entry in `auto-agent.toml`.
Use the selected project environment with the Evaluation dependencies.

```bash
autoagent eval list
autoagent eval check SUITE_ID
autoagent eval run SUITE_ID --case CASE_ID --run-dir baseline
autoagent eval show baseline/report.json
autoagent eval run SUITE_ID --case CASE_ID --run-dir candidate
autoagent eval compare baseline/report.json candidate/report.json
```

Run the baseline and candidate against their respective Workflow versions.
Use new run directories. Keep both reports and their Trial databases when
you need to reopen execution evidence. Use the same Case IDs, inputs,
expected values, Scenario, rules, and fixture configuration. Declare
external conditions such as fixture or dataset versions in
`EvalSuite.conditions`, or use `{}` when there are no external conditions.

Compare the named rules as well as the Case verdicts. Changed rules or
conditions can make results incomparable. Missing conditions, incomplete
evidence, and unevaluated rules can make a comparison inconclusive. Keep
the underlying business expectations fixed when testing a Workflow change.
Update the Suite version when the assessment contract changes.
