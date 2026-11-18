import json
from contextlib import asynccontextmanager

from autoagent.agent import AgentApprovalRequest, AgentApprovalResponse
from autoagent.evaluation import (
    CheckResult,
    EvalCase,
    EvalSuite,
    EvaluationEnvironment,
    EvaluatorContext,
    ExecutionView,
    OutputEquals,
    ScenarioContext,
    StatusIs,
)
from autoagent.host import AutoAgentHost, HostSettings

from workflows import build_workflow, resources


@asynccontextmanager
async def fixture(context: ScenarioContext):
    context.resources["notes"] = []
    yield


def trial_host(environment: EvaluationEnvironment) -> AutoAgentHost:
    return AutoAgentHost(
        workflows=[build_workflow(environment.resources["notes"])],
        settings=HostSettings(database_url=environment.database_url),
        lifespan=resources,
        environment={},
    )


async def approve_tool_call(context: ScenarioContext) -> ExecutionView:
    stimulus = context.case.input
    waiting = await context.invoke(
        "release-note-assistant",
        {"message": stimulus["message"]},
        entry_node_id="start",
        until="boundary",
    )
    approval, = await waiting.interactions(node_id="wait_approval")
    batch = AgentApprovalRequest.model_validate_json(json.dumps(await approval.request()))
    request, = batch.requests
    context.check(CheckResult("approval-addresses-recording-tool", request.call.name == "record_release_note"))
    context.check(CheckResult("no-note-before-approval", not context.resources["notes"]))
    reply = AgentApprovalResponse(
        decisions=(request.respond(approved=stimulus["approve"]),),
        revision=batch.revision,
    )
    return await context.respond(waiting, reply.model_dump(mode="json"), wait_id=approval.id)


def observed_effect(context: EvaluatorContext) -> CheckResult:
    expected = ["Add Workflow authoring guidance."] if context.case.input["approve"] else []
    actual = context.resources["notes"]
    return CheckResult(
        "only-approved-tool-records-note",
        actual == expected,
        f"Expected note log {expected!r}. Observed {actual!r}.",
    )


release_note_cases = EvalSuite(
    id="release-note-approval",
    cases=(
        EvalCase(
            "approve",
            {"message": "Record the release note.", "approve": True},
            "Release note recorded.",
        ),
        EvalCase(
            "decline",
            {"message": "Record the release note.", "approve": False},
            "Release note declined.",
        ),
    ),
    scenario=approve_tool_call,
    evaluators=(StatusIs(), OutputEquals(pointer="/content/0/text"), observed_effect),
    fixture=fixture,
    host_factory=trial_host,
    conditions={"provider": "offline-notes-v1", "tool_effect": "per-trial-memory-v1"},
)
