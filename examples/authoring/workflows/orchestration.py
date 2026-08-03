from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from autoagent import (
    ConditionContext,
    InputMappingContext,
    ResourcePolicy,
    NodePolicy,
    UserEventMapping,
    Workflow,
)


class ReviewRequest(BaseModel):
    service: str = Field(min_length=1)
    risk: Literal["low", "high"]
    change_summary: str = Field(min_length=1)


class ReviewPlan(BaseModel):
    request: ReviewRequest
    round_number: int = Field(ge=1)


class ReviewFinding(BaseModel):
    request: ReviewRequest
    reviewer: Literal["security", "reliability"]
    round_number: int
    passed: bool
    note: str


class ReviewSummary(BaseModel):
    request: ReviewRequest
    round_number: int
    approved: bool
    findings: tuple[ReviewFinding, ...]


class ReviewReport(BaseModel):
    service: str
    decision: Literal["approved"]
    review_rounds: int
    path: Literal["automatic", "specialist"]
    notes: tuple[str, ...]


def receive_request(request: ReviewRequest) -> ReviewRequest:
    """Validate and expose the Invocation input."""

    return request


def map_review_plan(ctx: InputMappingContext) -> dict[str, object]:
    """Enter the loop from the request or advance it from the prior summary."""

    incoming = ctx.incoming[0]
    if incoming.edge_id == "start_review":
        return {
            "request": incoming.value,
            "round_number": 1,
        }
    previous = ReviewSummary.model_validate(incoming.value)
    return {
        "request": previous.request,
        "round_number": previous.round_number + 1,
    }


def plan_review(request: ReviewRequest, round_number: int) -> ReviewPlan:
    return ReviewPlan(request=request, round_number=round_number)


def is_low_risk(ctx: ConditionContext) -> bool:
    return ReviewPlan.model_validate(ctx.source_output).request.risk == "low"


def needs_specialists(ctx: ConditionContext) -> bool:
    return not is_low_risk(ctx)


def approve_low_risk(plan: ReviewPlan) -> ReviewSummary:
    return ReviewSummary(
        request=plan.request,
        round_number=plan.round_number,
        approved=True,
        findings=(),
    )


def map_review_plan_from_incoming(
    ctx: InputMappingContext,
) -> dict[str, object]:
    return {"plan": ctx.incoming[0].value}


def security_review(plan: ReviewPlan) -> ReviewFinding:
    passed = plan.round_number >= 2
    return ReviewFinding(
        request=plan.request,
        reviewer="security",
        round_number=plan.round_number,
        passed=passed,
        note=(
            "Security controls verified."
            if passed
            else "Add a rollback authorization check."
        ),
    )


def reliability_review(plan: ReviewPlan) -> ReviewFinding:
    passed = plan.round_number >= 2
    return ReviewFinding(
        request=plan.request,
        reviewer="reliability",
        round_number=plan.round_number,
        passed=passed,
        note=(
            "Reliability checks verified."
            if passed
            else "Add a staged rollout and health gate."
        ),
    )


def aggregate_reviews(
    security_review: ReviewFinding,
    reliability_review: ReviewFinding,
) -> ReviewSummary:
    findings = (security_review, reliability_review)
    return ReviewSummary(
        request=security_review.request,
        round_number=security_review.round_number,
        approved=all(item.passed for item in findings),
        findings=findings,
    )


def needs_another_round(ctx: ConditionContext) -> bool:
    return not ReviewSummary.model_validate(ctx.source_output).approved


def review_complete(ctx: ConditionContext) -> bool:
    return ReviewSummary.model_validate(ctx.source_output).approved


def map_final_report(ctx: InputMappingContext) -> dict[str, object]:
    return {"summary": ctx.incoming[0].value}


def finalize_report(summary: ReviewSummary) -> ReviewReport:
    return ReviewReport(
        service=summary.request.service,
        decision="approved",
        review_rounds=summary.round_number,
        path="automatic" if not summary.findings else "specialist",
        notes=tuple(item.note for item in summary.findings),
    )


def review_completed_event(report: ReviewReport) -> dict[str, object]:
    """Expose one application-specific event without changing Runtime state."""

    return {
        "service": report.service,
        "decision": report.decision,
        "review_rounds": report.review_rounds,
        "path": report.path,
    }


workflow = Workflow(
    id="release_review",
    version=1,
    name="Release review",
    description=(
        "Demonstrates conditional branching, parallel specialists, fan-in, "
        "and a bounded review loop."
    ),
)
workflow.add_node(receive_request, node_id="receive_request")
workflow.add_node(
    plan_review,
    node_id="plan_review",
    input_mapping=map_review_plan,
    policy=NodePolicy(
        resource=ResourcePolicy(max_node_executions_per_invocation=3)
    ),
)
workflow.add_node(
    approve_low_risk,
    node_id="approve_low_risk",
    input_mapping=map_review_plan_from_incoming,
)
workflow.add_node(
    security_review,
    node_id="security_review",
    input_mapping=map_review_plan_from_incoming,
)
workflow.add_node(
    reliability_review,
    node_id="reliability_review",
    input_mapping=map_review_plan_from_incoming,
)
workflow.add_node(aggregate_reviews, node_id="aggregate_reviews")
workflow.add_node(
    finalize_report,
    node_id="finalize_report",
    input_mapping=map_final_report,
    user_event_mapping=UserEventMapping(
        type="release_review_completed",
        transform=review_completed_event,
    ),
)

workflow.add_edge(
    "receive_request",
    "plan_review",
    edge_id="start_review",
)
workflow.add_edge(
    "plan_review",
    "approve_low_risk",
    edge_id="select_automatic_review",
    condition=is_low_risk,
)
workflow.add_edge(
    "plan_review",
    "security_review",
    edge_id="select_security_review",
    condition=needs_specialists,
)
workflow.add_edge(
    "plan_review",
    "reliability_review",
    edge_id="select_reliability_review",
    condition=needs_specialists,
)
workflow.add_edge(
    "security_review",
    "aggregate_reviews",
    edge_id="collect_security_review",
)
workflow.add_edge(
    "reliability_review",
    "aggregate_reviews",
    edge_id="collect_reliability_review",
)
workflow.add_edge(
    "aggregate_reviews",
    "plan_review",
    edge_id="repeat_review",
    condition=needs_another_round,
)
workflow.add_edge(
    "aggregate_reviews",
    "finalize_report",
    edge_id="complete_specialist_review",
    condition=review_complete,
)
workflow.add_edge(
    "approve_low_risk",
    "finalize_report",
    edge_id="complete_automatic_review",
)
