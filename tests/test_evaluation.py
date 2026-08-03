from __future__ import annotations

import unittest

from autoagent.core.runtime import ContextSnapshot, InvocationContext
from autoagent.core.runtime.output import OutputIndex
from autoagent.evaluation import (
    EvalCase,
    EvalCaseResult,
    EvalError,
    EvalResult,
    EvalStepResult,
    Evaluation,
    EvaluationContext,
    EvaluatorResult,
    evaluators,
)
from autoagent.evaluation.definition import evaluation_case_methods


class _Evidence:
    async def runtime_events(self, **_: object) -> tuple[object, ...]:
        return ()


def _context(*, state: str = "completed", result: object = None) -> EvaluationContext:
    snapshot = ContextSnapshot.capture(InvocationContext())
    return EvaluationContext(
        suite_id="inventory_regression",
        case_id="eval_out_of_stock",
        step_index=1,
        action="invoke",
        workflow_id="inventory",
        workflow_revision_id="inventory:abc",
        session_id="eval-session",
        invocation_id="invocation-1",
        through_sequence=7,
        request={"sku": "A-001"},
        invocation_state=state,
        invocation_result=result,
        invocation_error=None,
        invocation_context=snapshot,
        session_context=snapshot,
        outputs=OutputIndex().view(),
        previous_steps=(),
        evidence=_Evidence(),
    )


class EvaluationDefinitionTests(unittest.TestCase):
    def test_discovers_async_case_methods_in_definition_order(self) -> None:
        class InventoryEvaluation(Evaluation):
            async def eval_second(self, case: EvalCase) -> None:
                pass

            async def helper(self) -> None:
                pass

            async def eval_first(self, case: EvalCase) -> None:
                pass

        self.assertEqual(
            ("eval_second", "eval_first"),
            tuple(name for name, _ in evaluation_case_methods(InventoryEvaluation)),
        )

    def test_public_package_exports_minimal_authoring_surface(self) -> None:
        import autoagent.evaluation as evaluation

        self.assertEqual(
            {
                "EvalCase",
                "EvalCaseResult",
                "EvalError",
                "EvalEvidenceRef",
                "EvalResult",
                "EvalStatus",
                "EvalStepResult",
                "Evaluation",
                "EvaluationContext",
                "EvaluationEvidence",
                "EvaluationLoader",
                "Evaluator",
                "EvaluatorResult",
                "LoadedEvaluation",
                "evaluators",
            },
            set(evaluation.__all__),
        )


class EvaluationResultTests(unittest.TestCase):
    def test_derives_completed_passed_failed_and_error(self) -> None:
        completed = EvalStepResult(index=1, action="invoke")
        passed = EvalStepResult(
            index=1,
            action="invoke",
            evaluator_results=(EvaluatorResult(key="quality", passed=True),),
        )
        observed = EvalStepResult(
            index=1,
            action="invoke",
            evaluator_results=(
                EvaluatorResult(key="quality", score=0.75, passed=None),
            ),
        )
        failed = EvalStepResult(
            index=1,
            action="invoke",
            evaluator_results=(EvaluatorResult(key="quality", passed=False),),
        )
        errored = EvalStepResult(
            index=1,
            action="invoke",
            error=EvalError(code="EVALUATOR_ERROR", message="broken"),
        )

        self.assertEqual("completed", completed.status)
        self.assertEqual("passed", passed.status)
        self.assertEqual("completed", observed.status)
        self.assertEqual("failed", failed.status)
        self.assertEqual("error", errored.status)

    def test_aggregates_without_copying_runtime_facts(self) -> None:
        step = EvalStepResult(
            index=1,
            action="invoke",
            invocation_id="invocation-1",
            through_sequence=9,
            evaluator_results=(EvaluatorResult(key="result", passed=True),),
        )
        case = EvalCaseResult(
            case_id="eval_happy_path",
            session_id="session-1",
            step_results=(step,),
        )
        result = EvalResult(
            suite_id="inventory_regression",
            workflow_id="inventory",
            workflow_revision_id="inventory:abc",
            case_results=(case,),
        )

        self.assertEqual("passed", case.status)
        self.assertEqual("passed", result.status)
        dumped = result.model_dump(mode="json")
        self.assertNotIn("invocation_result", dumped["case_results"][0])
        self.assertNotIn("runtime_events", dumped["case_results"][0])

    def test_rejects_evaluator_error_with_business_verdict(self) -> None:
        with self.assertRaises(ValueError):
            EvaluatorResult(
                key="quality",
                passed=False,
                error=EvalError(code="BROKEN", message="failed to evaluate"),
            )


class BuiltInEvaluatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_invocation_state_is_strict_and_evidence_backed(self) -> None:
        result = await evaluators.InvocationState(expected="completed").evaluate(
            _context(state="failed")
        )

        self.assertFalse(result.passed)
        self.assertEqual("failed", result.value)
        self.assertEqual("invocation-1", result.evidence[0].invocation_id)
        self.assertEqual(7, result.evidence[0].through_sequence)

    async def test_invocation_result_uses_exact_complete_value(self) -> None:
        expected = {"outputs": {"approved": True, "audit": "ok"}}

        passed = await evaluators.InvocationResult(expected=expected).evaluate(
            _context(result=expected)
        )
        failed = await evaluators.InvocationResult(expected=expected).evaluate(
            _context(result={"outputs": {"approved": True}})
        )

        self.assertTrue(passed.passed)
        self.assertFalse(failed.passed)


if __name__ == "__main__":
    unittest.main()
