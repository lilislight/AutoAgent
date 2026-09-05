# Workflow Evaluation and Unit Testing

This reference separates end-to-end business Evaluation from focused unit
tests. It does not define graph design or Compiler Diagnostics.

## Use Evaluation for Workflow behavior

Each authored Workflow should have one Manifest-registered Eval Suite. Define
one `Evaluation` subclass and one async `eval_*` method for each materially
different business outcome that must be protected:

```python
from autoagent.evaluation import EvalCase, Evaluation, evaluators


class OrderReviewEvaluation(Evaluation):
    async def eval_rejects_unavailable_inventory(self, case: EvalCase) -> None:
        await case.invoke(
            {"sku": "A-001", "quantity": 100},
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected={
                        "output": {
                            "accepted": False,
                            "reason": "out_of_stock",
                        }
                    }
                ),
            ),
        )
```

Keep small readable inputs and expected outputs directly in the Case. Use
fixture files only when values are large, shared, or easier to review outside
Python. Do not create separate input/expected directories as ceremony.

A Case may call `case.invoke(...)` more than once to test multi-turn Session
behavior. Use `case.resume(...)` after a waiting Step. Evaluators are optional
on setup Steps, but the Case must eventually assert the business behavior it is
intended to protect.

Compare stable public Invocation results, not generated IDs, timestamps,
internal ReAct Nodes, database rows, or Scheduler state.

## Use unit tests only for isolated logic

Add a focused unit test when a nontrivial Condition, mapping, Tool, Operator, or
custom Evaluator is easier to prove directly than through a complete
Invocation. Do not duplicate the same business scenario in both an Eval Case
and a unit test. Do not build a copied App/Invocation harness in project tests;
that is the Eval Runner's responsibility.

Use the project's existing test runner. Do not impose a package manager or test
framework solely because AutoAgent itself uses one.

The installed authoring examples include `tests/test_project_functions.py` as
the reference shape: a small test module that calls project functions directly
and does not construct an App or duplicate an Eval Case.

## Dependency realism

Eval always runs the real Workflow through the normal AutoAgent host. External
dependencies may still be controlled:

- use the real model when model quality, Tool selection, structured output, or
  ReAct behavior is the subject of the Evaluation;
- use a sandbox/staging dependency for integration behavior and side effects;
- use a local compatible service or recorded deterministic response when the
  dependency is costly, destructive, unavailable, or not what the Case tests;
- use redacted production-derived inputs for real distributions and synthetic
  inputs for missing boundaries.

Make material simulations obvious in source and handoff. Never put live
credentials or sensitive production values in Eval definitions or fixtures.

## Wait and Resume

An Eval Case may Invoke to `waiting` and Resume in the same Eval process. This
tests the Workflow's business conversation and uses one Case Session.

Cross-process Resume after host restart is a deployment/runtime property. Test
it separately only when the request explicitly requires that property; use a
temporary database and Standard or Full event mode. Do not make every Wait
business Evaluation repeat the framework's persistence tests.

## LLM and Tool Evaluation

Keep Workflow source Provider-neutral. Configure the ordinary CLI host through
environment variables. A local Chat Completions-compatible service can provide
deterministic model turns while still exercising the real Provider codec and
ReAct runtime.

Choose Cases from the actual business contract, not a framework repair matrix.
For example, assert parallel Tool behavior only when the requirement depends on
parallel calls, and assert a repair path only when that failure behavior is a
promised part of the Workflow.

## Commands and completion

Run:

```bash
autoagent project check
autoagent workflow list
autoagent workflow check <workflow-id>
autoagent eval check <suite-id>
autoagent eval run <suite-id>
```

During repair, `--case <eval-method-name>` may shorten an iteration. Before
handoff, run the complete Suite. Run focused unit tests only when they exist.

Completion requires passing static checks and the relevant complete Eval Suite.
If an external dependency could not be exercised, state that exact limitation;
do not replace evidence with a claim.
