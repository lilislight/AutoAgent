from __future__ import annotations

from typing import Literal

from autoagent.evaluation import EvalCase, Evaluation, evaluators
from workflows.fulfillment import FulfillmentDecision


def expected_decision(
    *,
    order_id: str,
    route: Literal["automatic_approval", "manual_review"],
) -> dict[str, FulfillmentDecision]:
    manual = route == "manual_review"
    return {
        "output": FulfillmentDecision(
            order_id=order_id,
            route=route,
            approved=not manual,
            reason=(
                "Manual review is required."
                if manual
                else "Order satisfies automatic approval rules."
            ),
        )
    }


class FulfillmentEvaluation(Evaluation):
    async def eval_low_value_order_is_approved(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "request": {
                    "order_id": "order-low",
                    "amount": 250,
                    "account_age_days": 180,
                    "flagged": False,
                }
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected=expected_decision(
                        order_id="order-low",
                        route="automatic_approval",
                    )
                ),
            ),
        )

    async def eval_high_value_order_requires_manual_review(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "request": {
                    "order_id": "order-high",
                    "amount": 2_500,
                    "account_age_days": 365,
                    "flagged": False,
                }
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected=expected_decision(
                        order_id="order-high",
                        route="manual_review",
                    )
                ),
            ),
        )

    async def eval_flagged_order_requires_manual_review(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "request": {
                    "order_id": "order-flagged",
                    "amount": 100,
                    "account_age_days": 365,
                    "flagged": True,
                }
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected=expected_decision(
                        order_id="order-flagged",
                        route="manual_review",
                    )
                ),
            ),
        )

    async def eval_new_account_requires_manual_review(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "request": {
                    "order_id": "order-new-account",
                    "amount": 100,
                    "account_age_days": 7,
                    "flagged": False,
                }
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected=expected_decision(
                        order_id="order-new-account",
                        route="manual_review",
                    )
                ),
            ),
        )
