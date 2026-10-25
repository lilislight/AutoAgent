"""Inline Workflow composition with two entry routes and two outward exits.

submit_standard -> review.standard_in -> review.checks.assess
submit_priority -> review.priority_in -> review.checks.assess
review.checks.approved -> dispatch
review.checks.rejected -> manual_review

Both SubWorkflow levels compile into the same Root Invocation. Nothing is
spawned, and the UI currently displays the expanded, namespaced Nodes.
"""

import asyncio
from typing import Literal, cast
from typing_extensions import TypedDict

from autoagent import ConditionContext, Edge, InputMappingContext, Node, Operator, SubWorkflow, Workflow
from autoagent.host import workflow


class Submission(TypedDict):
    order_id: str
    amount: int


class ReviewData(Submission):
    lane: Literal["standard", "priority"]
    approved: bool


class ReviewReceipt(ReviewData):
    destination: Literal["dispatch", "manual_review"]


async def accept_submission(value: Submission) -> Submission:
    await asyncio.sleep(.02)
    return value


def standard_entry(value: Submission) -> ReviewData:
    return {**value, "lane": "standard", "approved": False}


def priority_entry(value: Submission) -> ReviewData:
    return {**value, "lane": "priority", "approved": False}


def selected_review(context: InputMappingContext) -> ReviewData:
    """Only the selected entry contributes to this shared assessment Node."""
    incoming = tuple(context.incoming.values())
    if len(incoming) != 1:
        raise ValueError("Exactly one review entry must be active")
    return cast(ReviewData, incoming[0])


async def assess(value: ReviewData) -> ReviewData:
    await asyncio.sleep(.03)
    threshold = 1000 if value["lane"] == "standard" else 500
    return {**value, "approved": value["amount"] <= threshold}


def approved(context: ConditionContext) -> bool:
    return cast(ReviewData, context.output)["approved"]


def rejected(context: ConditionContext) -> bool:
    return not approved(context)


def review_exit(value: ReviewData) -> ReviewData:
    return value


async def dispatch(value: ReviewData) -> ReviewReceipt:
    await asyncio.sleep(.02)
    return {**value, "destination": "dispatch"}


async def manual_review(value: ReviewData) -> ReviewReceipt:
    """Produce a review-queue receipt, without opening an external Wait."""
    await asyncio.sleep(.02)
    return {**value, "destination": "manual_review"}


@workflow
def nested_review():
    """Two Root entries connect to distinct nested entries and outward exits."""
    # Operator IDs are Host-wide, while inline Node IDs receive their prefixes
    # from the Compiler. Keep this example separate from the other demo tools.
    accept = Operator(accept_submission, id="nested_review.accept_submission")
    exit_operator = Operator(review_exit, id="nested_review.exit")
    checks = Workflow("review-checks", nodes=[
        Node("assess", Operator(assess, id="nested_review.assess"), input_mapping=selected_review),
        Node("approved", exit_operator),
        Node("rejected", exit_operator),
    ], edges=[
        Edge("assess", "approved", approved, id="approve"),
        Edge("assess", "rejected", rejected, id="reject"),
    ])
    review = Workflow("review", nodes=[
        Node("standard_in", Operator(standard_entry, id="nested_review.standard_entry")),
        Node("priority_in", Operator(priority_entry, id="nested_review.priority_entry")),
    ], sub_workflows=[SubWorkflow("checks", checks)], edges=[
        Edge("standard_in", "checks.assess", id="standard-to-checks"),
        Edge("priority_in", "checks.assess", id="priority-to-checks"),
    ])
    return Workflow("nested-review-demo", nodes=[
        Node("submit_standard", accept),
        Node("submit_priority", accept),
        Node("dispatch", Operator(dispatch, id="nested_review.dispatch")),
        Node("manual_review", Operator(manual_review, id="nested_review.manual_review")),
    ], sub_workflows=[SubWorkflow("review", review)], edges=[
        Edge("submit_standard", "review.standard_in", id="standard-entry"),
        Edge("submit_priority", "review.priority_in", id="priority-entry"),
        Edge("review.checks.approved", "dispatch", id="approved-exit"),
        Edge("review.checks.rejected", "manual_review", id="rejected-exit"),
    ])
