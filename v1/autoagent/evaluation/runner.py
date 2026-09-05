from __future__ import annotations

import asyncio
from collections.abc import Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass
import inspect
from typing import Any
from uuid import UUID, uuid4

from autoagent.core.compiler import workflow_revision_id
from autoagent.core.runtime import ContextSnapshot, Invocation, RuntimeEvent, Session
from autoagent.evaluation.case import EvalCase
from autoagent.evaluation.definition import evaluation_case_methods
from autoagent.evaluation.evaluator import (
    EvaluationContext,
    EvaluationEvidence,
    Evaluator,
)
from autoagent.evaluation.loader import LoadedEvaluation
from autoagent.evaluation.result import (
    EvalCaseResult,
    EvalError,
    EvalResult,
    EvalStepResult,
    EvaluatorResult,
)
from autoagent.project.host import ProjectHost


class _StopCase(BaseException):
    """Internal strict-gate control flow that user ``except Exception`` ignores."""


@dataclass(frozen=True, slots=True)
class _RuntimeEvidence(EvaluationEvidence):
    host: ProjectHost
    invocation_id: UUID
    through_sequence: int

    async def runtime_events(
        self,
        *,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
    ) -> tuple[RuntimeEvent, ...]:
        upper_bound = self.through_sequence + 1
        bounded_before = (
            upper_bound
            if before_sequence is None
            else min(before_sequence, upper_bound)
        )
        store = self.host.app.runtime_store
        load = store.alist_runtime_events(
            invocation_id=self.invocation_id,
            after_sequence=after_sequence,
            before_sequence=bounded_before,
            limit=limit,
        )
        runtime_loop = self.host.app._runtime_loop
        if runtime_loop.is_current():
            return await load
        return await runtime_loop.arun(load)


class _RunningEvalCase(EvalCase):
    def __init__(
        self,
        *,
        runner: EvaluationRunner,
        loaded: LoadedEvaluation,
        case_id: str,
        session_id: str,
    ) -> None:
        self._runner = runner
        self._loaded = loaded
        self._case_id = case_id
        self._session_id = session_id
        self._step_results: list[EvalStepResult] = []

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def step_results(self) -> tuple[EvalStepResult, ...]:
        return tuple(self._step_results)

    async def invoke(
        self,
        input: dict[str, Any] | None = None,
        *,
        entry_node_id: str | None = None,
        evaluators: Iterable[Evaluator] = (),
    ) -> EvalStepResult:
        workflow = self._runner.host.workflow(self._loaded.locator.workflow_id)
        return await self._execute_step(
            action="invoke",
            request=deepcopy(input),
            evaluators=tuple(evaluators),
            operation=self._runner.host.app.ainvoke(
                workflow,
                input=input,
                session_id=self._session_id,
                entry_node_id=entry_node_id,
                event_mode="full",
            ),
        )

    async def resume(
        self,
        *,
        wait_key: str,
        response: Any,
        evaluators: Iterable[Evaluator] = (),
    ) -> EvalStepResult:
        workflow = self._runner.host.workflow(self._loaded.locator.workflow_id)
        return await self._execute_step(
            action="resume",
            request=deepcopy(response),
            evaluators=tuple(evaluators),
            operation=self._runner.host.app.aresume(
                workflow,
                session_id=self._session_id,
                wait_key=wait_key,
                output=response,
            ),
        )

    async def _execute_step(
        self,
        *,
        action: str,
        request: Any,
        evaluators: tuple[Evaluator, ...],
        operation: Any,
    ) -> EvalStepResult:
        index = len(self._step_results) + 1
        try:
            invocation = await operation
        except asyncio.CancelledError:
            self._step_results.append(
                EvalStepResult(
                    index=index,
                    action=action,
                    error=EvalError(
                        code="EVAL_STEP_CANCELLED",
                        message="Eval Step was cancelled before it completed.",
                    ),
                )
            )
            raise
        except Exception as exc:
            step = EvalStepResult(
                index=index,
                action=action,
                error=_error("EVAL_STEP_EXECUTION_ERROR", exc),
            )
            self._step_results.append(step)
            raise _StopCase

        try:
            session = self._session(invocation)
            context = self._context(
                index=index,
                action=action,
                request=request,
                session=session,
                invocation=invocation,
            )
        except Exception as exc:
            step = EvalStepResult(
                index=index,
                action=action,
                invocation_id=str(invocation.id),
                through_sequence=invocation.event_sequence,
                error=_error("EVAL_CONTEXT_ERROR", exc),
            )
            self._step_results.append(step)
            raise _StopCase

        results: list[EvaluatorResult] = []
        should_stop = False
        try:
            for evaluator in evaluators:
                try:
                    evaluated = evaluator.evaluate(context)
                    result = (
                        await evaluated
                        if inspect.isawaitable(evaluated)
                        else evaluated
                    )
                    if not isinstance(result, EvaluatorResult):
                        raise TypeError(
                            "Evaluator.evaluate() must return EvaluatorResult."
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    result = EvaluatorResult(
                        key=_evaluator_key(evaluator),
                        error=_error("EVALUATOR_EXECUTION_ERROR", exc),
                    )
                results.append(result)
                if result.error is not None or result.passed is False:
                    should_stop = True
                    break
        except asyncio.CancelledError:
            self._step_results.append(
                EvalStepResult(
                    index=index,
                    action=action,
                    invocation_id=str(invocation.id),
                    through_sequence=invocation.event_sequence,
                    evaluator_results=tuple(results),
                    error=EvalError(
                        code="EVAL_STEP_CANCELLED",
                        message="Eval Step was cancelled during evaluation.",
                    ),
                )
            )
            raise

        step = EvalStepResult(
            index=index,
            action=action,
            invocation_id=str(invocation.id),
            through_sequence=invocation.event_sequence,
            evaluator_results=tuple(results),
        )
        self._step_results.append(step)
        if should_stop:
            raise _StopCase
        return step

    def _session(self, invocation: Invocation) -> Session:
        session = self._runner.host.app.runtime_store.find_session(
            workflow_revision_id=invocation.workflow_revision_id,
            session_key=self._session_id,
        )
        if session is None:
            raise RuntimeError("Eval Session disappeared from RuntimeStore.")
        return session

    def _context(
        self,
        *,
        index: int,
        action: str,
        request: Any,
        session: Session,
        invocation: Invocation,
    ) -> EvaluationContext:
        return EvaluationContext(
            suite_id=self._loaded.locator.id,
            case_id=self._case_id,
            step_index=index,
            action=action,
            workflow_id=invocation.workflow_id,
            workflow_revision_id=invocation.workflow_revision_id,
            session_id=self._session_id,
            invocation_id=str(invocation.id),
            through_sequence=invocation.event_sequence,
            request=request,
            invocation_state=invocation.state,
            invocation_result=deepcopy(invocation.result),
            invocation_error=(
                None if invocation.error is None else invocation.error.to_record()
            ),
            invocation_context=ContextSnapshot.capture(invocation.context),
            session_context=ContextSnapshot.capture(session.context),
            outputs=invocation.outputs,
            previous_steps=tuple(self._step_results),
            evidence=_RuntimeEvidence(
                host=self._runner.host,
                invocation_id=invocation.id,
                through_sequence=invocation.event_sequence,
            ),
        )


class EvaluationRunner:
    """Execute Manifest Evaluations through the normal ProjectHost Runtime."""

    def __init__(self, host: ProjectHost) -> None:
        self.host = host

    async def run(
        self,
        loaded: LoadedEvaluation,
        *,
        case_ids: Sequence[str] | None = None,
        max_concurrency: int = 1,
        timeout: float | None = None,
    ) -> EvalResult:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive.")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive.")
        await self.host.start()
        workflow = self.host.workflow(loaded.locator.workflow_id)
        entry = self.host.app.register_workflow(workflow)
        revision_id = workflow_revision_id(
            entry.workflow_snapshot.workflow_id,
            entry.workflow_snapshot.definition_hash,
        )
        methods = dict(evaluation_case_methods(loaded.evaluation_type))
        selected_ids = loaded.case_ids if case_ids is None else tuple(case_ids)
        unknown = tuple(case_id for case_id in selected_ids if case_id not in methods)
        if unknown:
            raise KeyError(f"Unknown Eval Case: {', '.join(unknown)}")

        semaphore = asyncio.Semaphore(max_concurrency)

        async def run_one(case_id: str) -> EvalCaseResult:
            async with semaphore:
                return await self._run_case(
                    loaded,
                    case_id=case_id,
                    method=methods[case_id],
                )

        tasks = tuple(
            asyncio.create_task(
                run_one(case_id),
                name=f"autoagent-eval:{loaded.locator.id}:{case_id}",
            )
            for case_id in selected_ids
        )
        try:
            _, pending = await asyncio.wait(tasks, timeout=timeout)
        except asyncio.CancelledError:
            await _cancel_tasks(tasks)
            raise

        timed_out = bool(pending)
        if timed_out:
            await _cancel_tasks(tuple(pending))
        case_results = tuple(
            _case_task_result(case_id, task)
            for case_id, task in zip(selected_ids, tasks, strict=True)
        )
        return EvalResult(
            suite_id=loaded.locator.id,
            workflow_id=loaded.locator.workflow_id,
            workflow_revision_id=revision_id,
            case_results=case_results,
            error=(
                EvalError(
                    code="EVAL_TIMEOUT",
                    message=(
                        "Evaluation exceeded its configured timeout of "
                        f"{timeout:g} seconds."
                    ),
                    detail={"timeout_seconds": timeout},
                )
                if timed_out and timeout is not None
                else None
            ),
        )

    async def _run_case(
        self,
        loaded: LoadedEvaluation,
        *,
        case_id: str,
        method: Any,
    ) -> EvalCaseResult:
        session_id = f"eval:{loaded.locator.id}:{case_id}:{uuid4()}"
        case = _RunningEvalCase(
            runner=self,
            loaded=loaded,
            case_id=case_id,
            session_id=session_id,
        )
        evaluation = loaded.evaluation_type()
        try:
            await method(evaluation, case)
        except _StopCase:
            pass
        except asyncio.CancelledError:
            return EvalCaseResult(
                case_id=case_id,
                session_id=session_id,
                step_results=case.step_results,
                error=EvalError(
                    code="EVAL_CASE_CANCELLED",
                    message="Eval Case was cancelled before it completed.",
                ),
            )
        except Exception as exc:
            return EvalCaseResult(
                case_id=case_id,
                session_id=session_id,
                step_results=case.step_results,
                error=_error("EVAL_CASE_EXECUTION_ERROR", exc),
            )
        return EvalCaseResult(
            case_id=case_id,
            session_id=session_id,
            step_results=case.step_results,
        )


async def _cancel_tasks(tasks: Sequence[asyncio.Task[EvalCaseResult]]) -> None:
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _case_task_result(
    case_id: str,
    task: asyncio.Task[EvalCaseResult],
) -> EvalCaseResult:
    if task.cancelled():
        return EvalCaseResult(
            case_id=case_id,
            error=EvalError(
                code="EVAL_CASE_CANCELLED",
                message="Eval Case was cancelled before it started.",
            ),
        )
    return task.result()


def _evaluator_key(evaluator: Evaluator) -> str:
    value = getattr(evaluator, "key", None)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return type(evaluator).__name__


def _error(code: str, error: Exception) -> EvalError:
    message = str(error).strip() or type(error).__name__
    return EvalError(
        code=code,
        message=message,
        detail={"exception_type": type(error).__name__},
    )
