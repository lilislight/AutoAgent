from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from autoagent import ConditionContext, InputMappingContext, Workflow


class FulfillmentRequest(BaseModel):
    order_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    account_age_days: int = Field(ge=0)
    flagged: bool


class FulfillmentDecision(BaseModel):
    order_id: str
    route: Literal["automatic_approval", "manual_review"]
    approved: bool
    reason: str


def receive_request(request: FulfillmentRequest) -> FulfillmentRequest:
    return request


def requires_manual_review(ctx: ConditionContext) -> bool:
    request = FulfillmentRequest.model_validate(ctx.source_output)
    return (
        request.flagged
        and request.amount > 1_000
        and request.account_age_days < 30
    )


def allows_automatic_approval(ctx: ConditionContext) -> bool:
    return not requires_manual_review(ctx)


def map_request(ctx: InputMappingContext) -> dict[str, object]:
    return {"request": ctx.incoming[0].value}


def approve_automatically(
    request: FulfillmentRequest,
) -> FulfillmentDecision:
    return FulfillmentDecision(
        order_id=request.order_id,
        route="automatic_approval",
        approved=True,
        reason="Order satisfies automatic approval rules.",
    )


def request_manual_review(
    request: FulfillmentRequest,
) -> FulfillmentDecision:
    return FulfillmentDecision(
        order_id=request.order_id,
        route="manual_review",
        approved=False,
        reason="Manual review is required.",
    )


def map_decision(ctx: InputMappingContext) -> dict[str, object]:
    return {"decision": ctx.incoming[0].value}


def finalize(decision: FulfillmentDecision) -> FulfillmentDecision:
    return decision


workflow = Workflow(
    id="fulfillment_review",
    version=1,
    name="Fulfillment review",
    description="Routes risky orders to manual review before fulfillment.",
)
workflow.add_node(receive_request, node_id="receive_request")
workflow.add_node(
    approve_automatically,
    node_id="approve_automatically",
    input_mapping=map_request,
)
workflow.add_node(
    request_manual_review,
    node_id="request_manual_review",
    input_mapping=map_request,
)
workflow.add_node(finalize, node_id="finalize", input_mapping=map_decision)

workflow.add_edge(
    "receive_request",
    "approve_automatically",
    edge_id="select_automatic_approval",
    condition=allows_automatic_approval,
)
workflow.add_edge(
    "receive_request",
    "request_manual_review",
    edge_id="select_manual_review",
    condition=requires_manual_review,
)
workflow.add_edge(
    "approve_automatically",
    "finalize",
    edge_id="complete_automatic_approval",
)
workflow.add_edge(
    "request_manual_review",
    "finalize",
    edge_id="complete_manual_review",
)
