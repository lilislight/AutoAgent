from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from autoagent.core.runtime import ContextSnapshot, OutputView, RuntimeEvent
from autoagent.evaluation.result import EvalStepResult, EvaluatorResult


class EvaluationEvidence(Protocol):
    """Lazy, sequence-bounded access to authoritative Runtime evidence."""

    async def runtime_events(
        self,
        *,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
    ) -> tuple[RuntimeEvent, ...]: ...


@dataclass(frozen=True, slots=True)
class EvaluationContext:
    """Read-only Runtime view supplied to one Evaluator.

    Context values are isolated snapshots and ``outputs`` is a lazy read-only
    view. Detailed journal data remains behind ``evidence`` so an Evaluator
    pays only for the facts it reads.
    """

    suite_id: str
    case_id: str
    step_index: int
    action: Literal["invoke", "resume"]
    workflow_id: str
    workflow_revision_id: str
    session_id: str
    invocation_id: str
    through_sequence: int
    request: Any
    invocation_state: str
    invocation_result: Any
    invocation_error: Any
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    previous_steps: tuple[EvalStepResult, ...]
    evidence: EvaluationEvidence


class Evaluator(Protocol):
    """Extensible business evaluator run after an Eval Step."""

    async def evaluate(self, context: EvaluationContext) -> EvaluatorResult: ...
