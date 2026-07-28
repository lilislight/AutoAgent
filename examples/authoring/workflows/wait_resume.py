from __future__ import annotations

from pydantic import BaseModel, Field

from autoagent import InputMappingContext, SystemCommand, Workflow


class ApprovalResponse(BaseModel):
    approved: bool
    reviewer: str = Field(min_length=1)
    comment: str = ""


class ApprovalResult(BaseModel):
    decision: str
    reviewer: str
    comment: str


def map_approval_response(ctx: InputMappingContext) -> dict[str, object]:
    return {"response": ctx.incoming[0].value}


def finalize_approval(response: ApprovalResponse) -> ApprovalResult:
    return ApprovalResult(
        decision="approved" if response.approved else "rejected",
        reviewer=response.reviewer,
        comment=response.comment,
    )


workflow = Workflow(
    id="human_approval",
    version=1,
    name="Human approval",
    description=(
        "Demonstrates a durable external Wait followed by typed Resume data."
    ),
)
workflow.add_node(
    SystemCommand(id="wait"),
    node_id="approval",
    name="Wait for approval",
)
workflow.add_node(
    finalize_approval,
    node_id="finalize_approval",
    input_mapping=map_approval_response,
)
workflow.add_edge(
    "approval",
    "finalize_approval",
    edge_id="approval_received",
)
