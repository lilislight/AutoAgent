from __future__ import annotations

from pathlib import Path
import unittest

from autoagent import SystemCommand, Workflow
from autoagent.app import AutoAgentSettings
from autoagent.evaluation import (
    EvalCase,
    Evaluation,
    EvaluationContext,
    EvaluationRunner,
    EvaluatorResult,
    LoadedEvaluation,
    evaluators,
)
from autoagent.project import (
    EvalSuiteLocator,
    LoadedWorkflow,
    ProjectDefinition,
    ProjectHost,
    ProjectMetadata,
    WorkflowLocator,
)


class _SessionHistory:
    async def evaluate(self, context: EvaluationContext) -> EvaluatorResult:
        actual = list(context.session_context.data.get("history", ()))
        return EvaluatorResult(
            key="session_history",
            passed=actual == ["first", "second"],
            value=actual,
        )


class _ObservedQuality:
    async def evaluate(self, context: EvaluationContext) -> EvaluatorResult:
        return EvaluatorResult(key="quality", passed=None, score=0.8)


class _BrokenEvaluator:
    async def evaluate(self, context: EvaluationContext) -> EvaluatorResult:
        raise RuntimeError("judge unavailable")


class _RuntimeEvidenceAvailable:
    async def evaluate(self, context: EvaluationContext) -> EvaluatorResult:
        events = await context.evidence.runtime_events(limit=100)
        in_bounds = bool(events) and all(
            event.sequence <= context.through_sequence for event in events
        )
        full_operations = any(event.operations is not None for event in events)
        return EvaluatorResult(
            key="runtime_evidence",
            passed=in_bounds and full_operations,
            value={"count": len(events), "full_operations": full_operations},
        )


class EvaluationRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_multiple_invocations_share_one_case_session(self) -> None:
        workflow = _history_workflow()

        class ConversationEvaluation(Evaluation):
            async def eval_conversation(self, case: EvalCase) -> None:
                await case.invoke({"value": "first"})
                await case.invoke(
                    {"value": "second"},
                    evaluators=(
                        evaluators.InvocationState(expected="completed"),
                        evaluators.InvocationResult(expected={"output": "second"}),
                        _SessionHistory(),
                        _RuntimeEvidenceAvailable(),
                    ),
                )

        result = await _run(workflow, ConversationEvaluation)

        case = result.case_results[0]
        self.assertEqual("passed", result.status)
        self.assertEqual(2, len(case.step_results))
        self.assertNotEqual(
            case.step_results[0].invocation_id,
            case.step_results[1].invocation_id,
        )
        self.assertTrue(case.session_id.startswith("eval:"))

    async def test_wait_resume_uses_one_invocation_and_sequence_boundaries(self) -> None:
        workflow = Workflow(id="approval")
        workflow.add_node(SystemCommand(id="wait"), node_id="approval")
        workflow.add_node(
            lambda approved: "approved" if approved else "rejected",
            node_id="finish",
            input_mapping=lambda ctx: {
                "approved": ctx.incoming[0].value["approved"]
            },
        )
        workflow.add_edge("approval", "finish")

        class ApprovalEvaluation(Evaluation):
            async def eval_approved(self, case: EvalCase) -> None:
                await case.invoke(
                    {"wait_key": "approval-1"},
                    evaluators=(evaluators.InvocationState(expected="waiting"),),
                )
                await case.resume(
                    wait_key="approval-1",
                    response={"approved": True},
                    evaluators=(
                        evaluators.InvocationState(expected="completed"),
                        evaluators.InvocationResult(expected={"output": "approved"}),
                    ),
                )

        result = await _run(workflow, ApprovalEvaluation)

        steps = result.case_results[0].step_results
        self.assertEqual("passed", result.status)
        self.assertEqual(steps[0].invocation_id, steps[1].invocation_id)
        self.assertLess(steps[0].through_sequence, steps[1].through_sequence)

    async def test_gating_failure_stops_only_current_case(self) -> None:
        workflow = _history_workflow()
        reached_after_failure: list[bool] = []

        class StrictEvaluation(Evaluation):
            async def eval_fails(self, case: EvalCase) -> None:
                await case.invoke(
                    {"value": "first"},
                    evaluators=(evaluators.InvocationState(expected="failed"),),
                )
                reached_after_failure.append(True)
                await case.invoke({"value": "must-not-run"})

            async def eval_sibling(self, case: EvalCase) -> None:
                await case.invoke(
                    {"value": "second"},
                    evaluators=(evaluators.InvocationState(expected="completed"),),
                )

        result = await _run(workflow, StrictEvaluation)

        self.assertEqual("failed", result.status)
        self.assertEqual([], reached_after_failure)
        self.assertEqual("failed", result.case_results[0].status)
        self.assertEqual(1, len(result.case_results[0].step_results))
        self.assertEqual("passed", result.case_results[1].status)

    async def test_observational_evaluator_does_not_stop_case(self) -> None:
        workflow = _history_workflow()

        class ObservationalEvaluation(Evaluation):
            async def eval_score_only(self, case: EvalCase) -> None:
                await case.invoke(
                    {"value": "first"},
                    evaluators=(_ObservedQuality(),),
                )
                await case.invoke(
                    {"value": "second"},
                    evaluators=(evaluators.InvocationState(expected="completed"),),
                )

        result = await _run(workflow, ObservationalEvaluation)

        self.assertEqual("passed", result.status)
        self.assertEqual(2, len(result.case_results[0].step_results))
        self.assertEqual(
            "completed",
            result.case_results[0].step_results[0].status,
        )

    async def test_evaluator_error_is_not_business_failure(self) -> None:
        workflow = _history_workflow()

        class BrokenEvaluation(Evaluation):
            async def eval_broken(self, case: EvalCase) -> None:
                await case.invoke(
                    {"value": "first"},
                    evaluators=(_BrokenEvaluator(),),
                )

            async def eval_still_runs(self, case: EvalCase) -> None:
                await case.invoke(
                    {"value": "second"},
                    evaluators=(evaluators.InvocationState(expected="completed"),),
                )

        result = await _run(workflow, BrokenEvaluation)

        broken = result.case_results[0]
        evaluator_result = broken.step_results[0].evaluator_results[0]
        self.assertEqual("error", result.status)
        self.assertEqual("error", broken.status)
        self.assertEqual("EVALUATOR_EXECUTION_ERROR", evaluator_result.error.code)
        self.assertEqual("passed", result.case_results[1].status)

    async def test_case_exception_keeps_prior_step_and_does_not_stop_suite(self) -> None:
        workflow = _history_workflow()

        class CaseErrorEvaluation(Evaluation):
            async def eval_crashes(self, case: EvalCase) -> None:
                await case.invoke({"value": "first"})
                raise ValueError("bad fixture")

            async def eval_sibling(self, case: EvalCase) -> None:
                await case.invoke({"value": "second"})

        result = await _run(workflow, CaseErrorEvaluation, max_concurrency=2)

        self.assertEqual("error", result.status)
        self.assertEqual("EVAL_CASE_EXECUTION_ERROR", result.case_results[0].error.code)
        self.assertEqual(1, len(result.case_results[0].step_results))
        self.assertEqual("completed", result.case_results[1].status)
        self.assertNotEqual(
            result.case_results[0].session_id,
            result.case_results[1].session_id,
        )


def _history_workflow() -> Workflow:
    def remember(ctx) -> None:
        ctx.session_context.data.setdefault("history", []).append(ctx.output)

    workflow = Workflow(id="conversation")
    workflow.add_node(
        lambda value: value,
        node_id="echo",
        input_mapping=lambda ctx: {"value": ctx.invocation_input["value"]},
        output_binding=remember,
    )
    return workflow


async def _run(
    workflow: Workflow,
    evaluation_type: type[Evaluation],
    *,
    max_concurrency: int = 1,
):
    workflow_locator = WorkflowLocator(entrypoint="tests.fixture:workflow")
    eval_locator = EvalSuiteLocator(
        id="regression",
        workflow_id=workflow.id,
        entrypoint="tests.fixture:Evaluation",
    )
    project = ProjectDefinition(
        manifest_path=None,
        root=Path.cwd(),
        metadata=ProjectMetadata(name="evaluation-test", version="1"),
        workflows=(LoadedWorkflow(locator=workflow_locator, workflow=workflow),),
        eval_suites=(eval_locator,),
    )
    loaded = LoadedEvaluation(
        locator=eval_locator,
        evaluation_type=evaluation_type,
        case_ids=tuple(
            name
            for name in vars(evaluation_type)
            if name.startswith("eval_")
        ),
    )
    host = ProjectHost(
        project,
        app_settings=AutoAgentSettings(),
        environment={},
    )
    async with host:
        return await EvaluationRunner(host).run(
            loaded,
            max_concurrency=max_concurrency,
        )


if __name__ == "__main__":
    unittest.main()
