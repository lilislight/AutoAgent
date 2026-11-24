from autoagent.evaluation import (
    CheckResult,
    EvalCase,
    EvalSuite,
    EvaluatorContext,
    ExecutionView,
    OutputEquals,
    ScenarioContext,
    StatusIs,
)


async def review_expense(context: ScenarioContext) -> ExecutionView:
    stimulus = context.case.input
    waiting = await context.invoke(
        "expense-approval", stimulus["expense"], entry_node_id="submit", until="boundary"
    )
    approval, = await waiting.interactions(node_id="approve")
    context.check(
        CheckResult("reviewed-original-expense", await approval.request() == stimulus["expense"])
    )
    before = await waiting.context(scope="invocation")
    context.check(CheckResult("no-record-before-approval", "recorded_expense" not in before))
    return await context.respond(waiting, stimulus["decision"], wait_id=approval.id)


async def check_recording(context: EvaluatorContext) -> CheckResult:
    execution = context.execution
    approval = context.case.input["decision"]["approved"]
    actual_route = await execution.route("approved").was_taken()
    rejected_route = await execution.route("declined").was_taken()
    state = await execution.context(scope="invocation")
    expected_state = {"recorded_expense": context.case.expected} if approval else {}
    observed_state = {key: state[key] for key in ("recorded_expense",) if key in state}
    return CheckResult(
        "only-approved-expenses-are-recorded",
        actual_route == approval and rejected_route == (not approval)
        and observed_state == expected_state,
        f"Approved route: {actual_route}. Declined route: {rejected_route}. Recorded: {observed_state!r}.",
    )


expense_cases = EvalSuite(
    id="expense-approval",
    cases=(
        EvalCase(
            "approve",
            {"expense": {"expense_id": "E-1", "amount_cents": 2500}, "decision": {"approved": True}},
            {"expense_id": "E-1", "amount_cents": 2500, "status": "recorded"},
        ),
        EvalCase(
            "decline",
            {"expense": {"expense_id": "E-2", "amount_cents": 4300}, "decision": {"approved": False}},
            {"expense_id": "E-2", "amount_cents": 4300, "status": "declined"},
        ),
    ),
    scenario=review_expense,
    evaluators=(StatusIs(), OutputEquals(), check_recording),
    conditions={},
)
