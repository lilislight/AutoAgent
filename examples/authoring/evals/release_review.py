from __future__ import annotations

from autoagent.evaluation import EvalCase, Evaluation, evaluators
from workflows.orchestration import ReviewReport


class ReleaseReviewEvaluation(Evaluation):
    """Protect the two materially different release-review outcomes."""

    async def eval_high_risk_change_requires_specialist_revision(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "request": {
                    "service": "payments-api",
                    "risk": "high",
                    "change_summary": "Replace the authorization cache",
                }
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected={
                        "output": ReviewReport(
                            service="payments-api",
                            decision="approved",
                            review_rounds=2,
                            path="specialist",
                            notes=(
                                "Security controls verified.",
                                "Reliability checks verified.",
                            ),
                        )
                    }
                ),
            ),
        )

    async def eval_low_risk_change_is_approved_automatically(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "request": {
                    "service": "documentation-site",
                    "risk": "low",
                    "change_summary": "Correct a heading",
                }
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected={
                        "output": ReviewReport(
                            service="documentation-site",
                            decision="approved",
                            review_rounds=1,
                            path="automatic",
                            notes=(),
                        )
                    }
                ),
            ),
        )
