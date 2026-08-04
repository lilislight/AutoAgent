from __future__ import annotations

import asyncio
import os
from pathlib import Path
from threading import Event
from time import perf_counter
import tempfile
import unittest

from autoagent import SystemCommand, Workflow
from autoagent.app import AutoAgentSettings
from autoagent.evaluation import (
    EvalCase,
    Evaluation,
    EvaluationContext,
    EvaluatorResult,
    evaluators,
)
from autoagent.evaluation.loader import LoadedEvaluation
from autoagent.evaluation.runner import EvaluationRunner
from autoagent.project import (
    EvalSuiteLocator,
    LoadedWorkflow,
    ProjectDefinition,
    ProjectHost,
    ProjectMetadata,
    WorkflowLocator,
)


EVAL_MEMORY_SUITE_MAX_SECONDS = float(
    os.getenv("AUTOAGENT_PERF_EVAL_MEMORY_SUITE_MAX_SECONDS", "10")
)
EVAL_SQLITE_SUITE_MAX_SECONDS = float(
    os.getenv("AUTOAGENT_PERF_EVAL_SQLITE_SUITE_MAX_SECONDS", "20")
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


class _SyncOutput:
    def evaluate(self, context: EvaluationContext) -> EvaluatorResult:
        return EvaluatorResult(
            key="sync_output",
            passed=context.invocation_result == {"output": "first"},
        )


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
    async def test_sync_and_async_evaluators_share_one_runner_path(self) -> None:
        workflow = _history_workflow()

        class MixedEvaluation(Evaluation):
            async def eval_mixed(self, case: EvalCase) -> None:
                await case.invoke(
                    {"value": "first"},
                    evaluators=(_SyncOutput(), _RuntimeEvidenceAvailable()),
                )

        result = await _run(workflow, MixedEvaluation)

        evaluator_results = result.case_results[0].step_results[0].evaluator_results
        self.assertEqual("passed", result.status)
        self.assertEqual(
            ["sync_output", "runtime_evidence"],
            [item.key for item in evaluator_results],
        )

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

    async def test_case_concurrency_never_exceeds_configured_limit(self) -> None:
        workflow = _history_workflow()
        active = 0
        maximum_active = 0

        async def exercise(case: EvalCase) -> None:
            nonlocal active, maximum_active
            active += 1
            maximum_active = max(maximum_active, active)
            try:
                await asyncio.sleep(0.02)
                await case.invoke({"value": "first"})
            finally:
                active -= 1

        class ConcurrentEvaluation(Evaluation):
            async def eval_one(self, case: EvalCase) -> None:
                await exercise(case)

            async def eval_two(self, case: EvalCase) -> None:
                await exercise(case)

            async def eval_three(self, case: EvalCase) -> None:
                await exercise(case)

            async def eval_four(self, case: EvalCase) -> None:
                await exercise(case)

        result = await _run(
            workflow,
            ConcurrentEvaluation,
            max_concurrency=2,
        )

        self.assertEqual("completed", result.status)
        self.assertEqual(2, maximum_active)
        self.assertEqual(
            ["eval_one", "eval_two", "eval_three", "eval_four"],
            [case.case_id for case in result.case_results],
        )

    async def test_timeout_preserves_completed_case_and_cancels_running_case(
        self,
    ) -> None:
        started = Event()
        cancelled = Event()

        async def maybe_block(value: str) -> str:
            if value == "fast":
                return value
            started.set()
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return value

        workflow = Workflow(id="timeout")
        workflow.add_node(maybe_block, node_id="work")

        class TimeoutEvaluation(Evaluation):
            async def eval_fast(self, case: EvalCase) -> None:
                await case.invoke(
                    {"value": "fast"},
                    evaluators=(
                        evaluators.InvocationState(expected="completed"),
                    ),
                )

            async def eval_slow(self, case: EvalCase) -> None:
                await case.invoke({"value": "slow"})

        host, loaded = _fixture(workflow, TimeoutEvaluation)
        async with host:
            result = await EvaluationRunner(host).run(
                loaded,
                max_concurrency=2,
                timeout=0.05,
            )
            invocation_states = {
                invocation.state
                for invocation in host.app.runtime_store.invocations.values()
            }

        self.assertTrue(started.is_set())
        self.assertTrue(cancelled.is_set())
        self.assertEqual("error", result.status)
        self.assertEqual("EVAL_TIMEOUT", result.error.code)
        self.assertEqual("passed", result.case_results[0].status)
        self.assertEqual("EVAL_CASE_CANCELLED", result.case_results[1].error.code)
        self.assertEqual(
            "EVAL_STEP_CANCELLED",
            result.case_results[1].step_results[0].error.code,
        )
        self.assertNotIn("running", invocation_states)

    async def test_external_cancellation_cleans_up_running_cases(self) -> None:
        started = Event()
        cancelled = Event()

        async def block() -> None:
            started.set()
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        workflow = Workflow(id="cancel")
        workflow.add_node(block, node_id="block")

        class CancelEvaluation(Evaluation):
            async def eval_running(self, case: EvalCase) -> None:
                await case.invoke()

        host, loaded = _fixture(workflow, CancelEvaluation)
        async with host:
            task = asyncio.create_task(EvaluationRunner(host).run(loaded))
            await asyncio.wait_for(
                asyncio.to_thread(started.wait),
                timeout=1,
            )
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            invocation_states = {
                invocation.state
                for invocation in host.app.runtime_store.invocations.values()
            }

        self.assertTrue(cancelled.is_set())
        self.assertNotIn("running", invocation_states)

    async def test_large_suite_completes_within_smoke_budget(self) -> None:
        workflow = _history_workflow()
        started = perf_counter()
        result = await _run(
            workflow,
            _large_evaluation(100),
            max_concurrency=8,
        )
        elapsed = perf_counter() - started

        self.assertEqual("completed", result.status)
        self.assertEqual(100, len(result.case_results))
        self.assertLess(elapsed, EVAL_MEMORY_SUITE_MAX_SECONDS)

    async def test_sqlite_suite_flushes_within_smoke_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "evaluation.sqlite3"
            settings = AutoAgentSettings(
                database_url=(
                    f"sqlite+aiosqlite:///{database_path.as_posix()}"
                ),
                database_batch_max_delay_ms=0,
            )
            started = perf_counter()
            result = await _run(
                _history_workflow(),
                _large_evaluation(50),
                max_concurrency=8,
                app_settings=settings,
            )
            elapsed = perf_counter() - started
            database_created = database_path.is_file()

        self.assertEqual("completed", result.status)
        self.assertEqual(50, len(result.case_results))
        self.assertTrue(database_created)
        self.assertLess(elapsed, EVAL_SQLITE_SUITE_MAX_SECONDS)


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
    timeout: float | None = None,
    app_settings: AutoAgentSettings | None = None,
):
    host, loaded = _fixture(
        workflow,
        evaluation_type,
        app_settings=app_settings,
    )
    async with host:
        return await EvaluationRunner(host).run(
            loaded,
            max_concurrency=max_concurrency,
            timeout=timeout,
        )


def _fixture(
    workflow: Workflow,
    evaluation_type: type[Evaluation],
    *,
    app_settings: AutoAgentSettings | None = None,
) -> tuple[ProjectHost, LoadedEvaluation]:
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
        app_settings=app_settings or AutoAgentSettings(),
        environment={},
    )
    return host, loaded


def _large_evaluation(case_count: int) -> type[Evaluation]:
    async def evaluate_case(self: Evaluation, case: EvalCase) -> None:
        await case.invoke({"value": "first"})

    return type(
        "LargeEvaluation",
        (Evaluation,),
        {
            f"eval_case_{index:03d}": evaluate_case
            for index in range(case_count)
        },
    )


if __name__ == "__main__":
    unittest.main()
