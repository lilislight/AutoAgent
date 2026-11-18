from typing import cast

from typing_extensions import TypedDict

from autoagent import Edge, InputMappingContext, Node, Wait, Workflow


class Quote(TypedDict):
    quote_id: str
    total_cents: int


class Approval(TypedDict):
    approved: bool


class Receipt(Quote):
    approved: bool


def calculate(quote: Quote) -> Quote:
    return quote


def approval_request(context: InputMappingContext) -> Quote:
    quote = cast(Quote, context.incoming["request-review"])
    return {"quote_id": quote["quote_id"], "total_cents": quote["total_cents"] * 100}


def receipt_input(context: InputMappingContext) -> Receipt:
    quote = cast(Quote, context.invocation_input)
    decision = cast(Approval, context.incoming["decision"])
    return {
        "quote_id": quote["quote_id"],
        "total_cents": quote["total_cents"],
        "approved": decision["approved"],
    }


def finish(receipt: Receipt) -> Receipt:
    return receipt


def quote_review() -> Workflow:
    return Workflow(
        "quote-review",
        nodes=[
            Node("quote", calculate),
            Node("review", Wait(Quote, Approval), input_mapping=approval_request),
            Node("finish", finish, input_mapping=receipt_input),
        ],
        edges=[
            Edge("quote", "review", id="request-review"),
            Edge("review", "finish", id="decision"),
        ],
    )
