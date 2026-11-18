from collections.abc import Mapping
from typing import cast

from typing_extensions import TypedDict

from autoagent import ContextOperation, ContextPatch, InputMappingContext, Node, OutputBindingContext, Workflow


class Limit(TypedDict):
    limit_cents: int


class Expense(TypedDict):
    amount_cents: int


class ExpenseWithLimit(Expense, Limit):
    pass


class Classification(TypedDict):
    requires_review: bool


def accept_limit(limit: Limit) -> Limit:
    return limit


def save_limit(context: OutputBindingContext) -> ContextPatch:
    value = cast(Limit, context.output)["limit_cents"]
    stored = value // 1000 * 1000
    return ContextPatch(session=(ContextOperation.set(("preferences", "limit_cents"), stored),))


def classify_input(context: InputMappingContext) -> ExpenseWithLimit:
    expense = cast(Expense, context.invocation_input)
    preferences = cast(Mapping[str, int], context.session_context["preferences"])
    return {"amount_cents": expense["amount_cents"], "limit_cents": preferences["limit_cents"]}


def classify(expense: ExpenseWithLimit) -> Classification:
    return {"requires_review": expense["amount_cents"] > expense["limit_cents"]}


def set_expense_limit() -> Workflow:
    return Workflow("set-expense-limit", nodes=[Node("save", accept_limit, output_binding=save_limit)])


def classify_expense() -> Workflow:
    return Workflow("classify-expense", nodes=[Node("classify", classify, input_mapping=classify_input)])
