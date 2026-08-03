from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from autoagent.evaluation.evaluator import EvaluationContext
from autoagent.evaluation.result import EvalEvidenceRef, EvaluatorResult


def _evidence(context: EvaluationContext) -> tuple[EvalEvidenceRef, ...]:
    return (
        EvalEvidenceRef(
            invocation_id=context.invocation_id,
            through_sequence=context.through_sequence,
        ),
    )


@dataclass(frozen=True, slots=True)
class InvocationState:
    """Require the Invocation to finish in one exact state."""

    expected: str

    async def evaluate(self, context: EvaluationContext) -> EvaluatorResult:
        actual = context.invocation_state
        passed = actual == self.expected
        return EvaluatorResult(
            key="invocation_state",
            passed=passed,
            value=actual,
            comment=(
                None
                if passed
                else f"Expected Invocation state {self.expected!r}, got {actual!r}."
            ),
            evidence=_evidence(context),
        )


@dataclass(frozen=True, slots=True)
class InvocationResult:
    """Require exact equality with the complete Invocation result."""

    expected: Any

    async def evaluate(self, context: EvaluationContext) -> EvaluatorResult:
        actual = context.invocation_result
        passed = actual == self.expected
        return EvaluatorResult(
            key="invocation_result",
            passed=passed,
            value=actual,
            comment=(
                None
                if passed
                else f"Expected Invocation result {self.expected!r}, got {actual!r}."
            ),
            evidence=_evidence(context),
        )
