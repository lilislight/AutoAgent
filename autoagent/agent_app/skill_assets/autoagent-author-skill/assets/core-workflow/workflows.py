from typing import Literal, cast

from typing_extensions import TypedDict

from autoagent import (
    ConditionContext,
    ContextOperation,
    ContextPatch,
    Edge,
    InputMappingContext,
    Node,
    OutputBindingContext,
    Wait,
    Workflow,
)


class Expense(TypedDict):
    expense_id: str
    amount_cents: int


class Decision(TypedDict):
    approved: bool


class Receipt(TypedDict):
    expense_id: str
    amount_cents: int
    status: Literal["recorded", "declined"]


def submit(expense: Expense) -> Expense:
    return expense


def save_request(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation=(ContextOperation.set("expense", context.output),))


def approved(context: ConditionContext) -> bool:
    return cast(Decision, context.output)["approved"]


def declined(context: ConditionContext) -> bool:
    return not approved(context)


def original_expense(context: InputMappingContext) -> Expense:
    expense = cast(Expense, context.invocation_context["expense"])
    return {"expense_id": expense["expense_id"], "amount_cents": expense["amount_cents"]}


def record(expense: Expense) -> Receipt:
    return {**expense, "status": "recorded"}


def decline(expense: Expense) -> Receipt:
    return {**expense, "status": "declined"}


def save_record(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation=(ContextOperation.set("recorded_expense", context.output),))


def final_input(context: InputMappingContext) -> Receipt:
    receipt = cast(Receipt, next(iter(context.incoming.values())))
    return {
        "expense_id": receipt["expense_id"],
        "amount_cents": receipt["amount_cents"],
        "status": receipt["status"],
    }


def finish(receipt: Receipt) -> Receipt:
    return receipt


def expense_approval() -> Workflow:
    return Workflow(
        "expense-approval",
        nodes=[
            Node("submit", submit, output_binding=save_request),
            Node("approve", Wait(Expense, Decision)),
            Node("record", record, input_mapping=original_expense, output_binding=save_record),
            Node("decline", decline, input_mapping=original_expense),
            Node("finish", finish, input_mapping=final_input),
        ],
        edges=[
            Edge("submit", "approve", id="request-approval"),
            Edge("approve", "record", approved, id="approved"),
            Edge("approve", "decline", declined, id="declined"),
            Edge("record", "finish", id="recorded-receipt"),
            Edge("decline", "finish", id="declined-receipt"),
        ],
    )
