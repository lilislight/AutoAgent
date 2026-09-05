from __future__ import annotations

from autoagent.evaluation import EvalCase, Evaluation, evaluators
from workflows.wait_resume import ApprovalResult


class HumanApprovalEvaluation(Evaluation):
    """Exercise one complete external approval conversation."""

    async def eval_release_manager_approves_request(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "wait_key": "release:42",
                "wait_type": "human_approval",
                "payload": {
                    "release_id": "42",
                    "service": "payments-api",
                },
            },
            evaluators=(
                evaluators.InvocationState(expected="waiting"),
            ),
        )
        await case.resume(
            wait_key="release:42",
            response={
                "approved": True,
                "reviewer": "release-manager",
                "comment": "Proceed during the maintenance window.",
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected={
                        "output": ApprovalResult(
                            decision="approved",
                            reviewer="release-manager",
                            comment=(
                                "Proceed during the maintenance window."
                            ),
                        )
                    }
                ),
            ),
        )

    async def eval_release_manager_rejects_request(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "wait_key": "release:blocked",
                "wait_type": "human_approval",
                "payload": {"release_id": "blocked"},
            },
            evaluators=(
                evaluators.InvocationState(expected="waiting"),
            ),
        )
        await case.resume(
            wait_key="release:blocked",
            response={
                "approved": False,
                "reviewer": "release-manager",
                "comment": "Rollback plan is incomplete.",
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected={
                        "output": ApprovalResult(
                            decision="rejected",
                            reviewer="release-manager",
                            comment="Rollback plan is incomplete.",
                        )
                    }
                ),
            ),
        )
