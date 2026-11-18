from autoagent.evaluation import CheckResult, EvalCase, EvalSuite, OutputEquals, StatusIs


async def review(context):
    case = context.case.input
    waiting = await context.invoke("quote-review", case["quote"], entry_node_id="quote", until="boundary")
    approval, = await waiting.interactions(node_id="review")
    requested = await approval.request(pointer="/total_cents")
    expected = case["quote"]["total_cents"]
    context.check(CheckResult(
        "reviewer-sees-quoted-total",
        requested == expected,
        f"Reviewer amount {requested}, quoted amount {expected}.",
    ))
    return await context.respond(waiting, case["reply"], wait_id=approval.id)


quote_cases = EvalSuite(
    id="quote-review",
    cases=(
        EvalCase(
            "approve-quote",
            {"quote": {"quote_id": "Q-1", "total_cents": 12500}, "reply": {"approved": True}},
            {"quote_id": "Q-1", "total_cents": 12500, "approved": True},
        ),
    ),
    scenario=review,
    evaluators=(StatusIs(), OutputEquals()),
    conditions={},
)
