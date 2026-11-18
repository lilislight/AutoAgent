from autoagent.evaluation import CheckResult, EvalCase, EvalSuite, OutputEquals, StatusIs


async def expense_in_same_session(context):
    case = context.case.input
    writer = await context.invoke(
        "set-expense-limit", case["limit"], entry_node_id="save", session="expense",
    )
    stored = await writer.context(scope="session", pointer="/preferences/limit_cents")
    expected = case["limit"]["limit_cents"]
    context.check(CheckResult("stored-limit", stored == expected, f"Stored {stored}, expected {expected}."))
    reader = await context.invoke(
        "classify-expense", case["expense"], entry_node_id="classify", session="expense",
    )
    context.check(CheckResult("same-expense-session", writer.session_id == reader.session_id))
    return reader


expense_cases = EvalSuite(
    id="expense-limit",
    cases=(
        EvalCase(
            "below-configured-limit",
            {"limit": {"limit_cents": 12500}, "expense": {"amount_cents": 12400}},
            {"requires_review": False},
        ),
    ),
    scenario=expense_in_same_session,
    evaluators=(StatusIs(), OutputEquals()),
    conditions={},
)
