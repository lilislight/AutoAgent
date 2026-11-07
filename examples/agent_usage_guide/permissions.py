"""Tool permissions and durable approval with Core Wait, entirely offline.

Run: .venv/bin/python -m examples.agent_usage_guide.permissions
"""

import asyncio
from collections.abc import Mapping
import json

from pydantic import BaseModel, ConfigDict

from autoagent import AutoAgentApp, ConditionContext, Edge, InputMappingContext, Node, Wait, Workflow
from autoagent.agent import (
    LLMToolCall, LLMToolResult, Tool, ToolApproval, ToolApprovalRequest,
    ToolContext, ToolDecision, ToolSet,
)


def plain(value):
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


class Execution(BaseModel):
    model_config = ConfigDict(extra="forbid")
    call: LLMToolCall
    approval: ToolApproval | None = None


def build_workflow(effects: list[str]) -> Workflow:
    """Applications bind services and policy; the model supplies only Tool args."""
    def publish(text: str) -> str:
        effects.append(text)
        return text

    def policy(call, context):
        if context.user_id != "alice":
            return ToolDecision(action="deny", reason="Publishing is restricted to Alice")
        return ToolDecision(action="ask", reason="Confirm publishing this text")

    tools = ToolSet((Tool(publish),), context=ToolContext(user_id="alice"), policy=policy)

    async def check(call: LLMToolCall) -> ToolDecision:
        return await tools.acheck(call)

    def approval_input(context: InputMappingContext) -> ToolApprovalRequest:
        decision = ToolDecision.model_validate(plain(next(iter(context.incoming.values()))))
        return decision.approval_request

    def execution_input(context: InputMappingContext) -> Execution:
        value = plain(next(iter(context.incoming.values())))
        return Execution(call=LLMToolCall.model_validate(plain(context.invocation_input)),
                         approval=ToolApproval.model_validate(value) if "request_id" in value else None)

    async def execute(value: Execution) -> LLMToolResult:
        # Rechecks current policy and the exact approval binding after Wait.
        return await tools.ainvoke(value.call, approval=value.approval)

    def denial_input(context: InputMappingContext) -> LLMToolResult:
        decision = ToolDecision.model_validate(plain(next(iter(context.incoming.values()))))
        call = LLMToolCall.model_validate(plain(context.invocation_input))
        return LLMToolResult(call_id=call.call_id, is_error=True,
                             structured_content={"error": {"kind": "tool_access_denied", "message": decision.reason}})

    def denied(result: LLMToolResult) -> LLMToolResult:
        return result

    def action_is(action):
        def condition(context: ConditionContext) -> bool:
            return context.output["action"] == action
        return condition

    return Workflow("tool-approval", nodes=[
        Node("check", check),
        Node("approve", Wait(ToolApprovalRequest, ToolApproval), input_mapping=approval_input),
        Node("execute", execute, input_mapping=execution_input),
        Node("deny", denied, input_mapping=denial_input),
    ], edges=[
        Edge("check", "approve", action_is("ask")),
        Edge("check", "execute", action_is("allow")),
        Edge("check", "deny", action_is("deny")),
        Edge("approve", "execute"),
    ])


async def main():
    for approved in (True, False):
        effects = []
        app = AutoAgentApp()
        try:
            waiting = await app.ainvoke(build_workflow(effects), LLMToolCall(
                name="publish", call_id="publish-1", arguments='{"text":"Reviewed report"}'))
            assert waiting.status == "waiting" and not effects, waiting
            request = ToolApprovalRequest.model_validate(plain(waiting.waits[0].request))
            # Simulate an authenticated human client. Never obtain this reply
            # from model-controlled Tool arguments. Real clients use Host.respond.
            result = await app.aresume(waiting.ref, waiting.waits[0].id,
                                      request.respond(approved=approved).model_dump(mode="json"))
            assert result.status == "completed", result.error
            assert len(effects) == int(approved)
            print(json.dumps(dict(approved=approved, output=result.output, effects=effects), ensure_ascii=False))
        finally:
            await app.aclose()


if __name__ == "__main__":
    asyncio.run(main())
