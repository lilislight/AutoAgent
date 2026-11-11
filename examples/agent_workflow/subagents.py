"""Offline delegation: .venv/bin/python -m examples.agent_workflow.subagents."""

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile

from autoagent.agent import (
    AgentInput, AgentModelSettings, AgentHostSettings, AgentSignalMessage, AgentResult,
    LLMMessage, LLMResponse, LLMStreamChunk, LLMToolCall, LLMToolResult,
    SubagentsConfig, SubagentTarget, TextContent, agent_workflow,
)
from autoagent.host import AutoAgentHost, HostSettings


def workflows():
    # Both Endpoints use AgentSignalMessage. Each Agent may choose its own names.
    root = agent_workflow(user_signal_endpoint="user.inbox", subagents=SubagentsConfig(
        enabled=True, signal_endpoint="team.inbox", targets={
            "researcher": SubagentTarget(workflow_id="researcher", description="Research a delegated question.")},
        default_wait_seconds=10, max_wait_seconds=60))
    child = agent_workflow(workflow_id="researcher", user_signal_endpoint="researcher.user",
        subagents=SubagentsConfig(enabled=True, signal_endpoint="researcher.team"))
    return [root, child]


class OfflineProvider:
    provider_id = "offline"

    async def ainvoke(self, request):
        initial = next(item for item in request.input if isinstance(item, LLMMessage) and item.role == "user")
        results = [item for item in request.input if isinstance(item, LLMToolResult)]
        if initial.content[0].text.startswith("Research:"):
            text = "The offline research task is complete."
        elif not results:
            return LLMResponse(model=request.model, finish_reason="tool_calls", output=(LLMToolCall(
                call_id="delegate", name="run_agent", arguments=json.dumps({
                    "type": "researcher", "task": "Research: explain the offline example."})),))
        else:
            text = "Delegation result: " + json.dumps(results[-1].structured_content, ensure_ascii=False)
        return LLMResponse(model=request.model, finish_reason="stop", output=(
            LLMMessage(role="assistant", content=(TextContent(text=text),)),))

    async def astream(self, request):
        yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))


@asynccontextmanager
async def lifespan(setup):
    setup.agent_context.resources.register_provider("offline", OfflineProvider())
    setup.agent_context.set_defaults(AgentHostSettings(
        model=AgentModelSettings(provider="offline", model="demo-model")))
    yield


async def send_user_message(host, invocation_id, text, *, sequence):
    """Use a monotonically increasing sequence for each source/Invocation."""
    return await host.signal(invocation_id, "user.inbox",
        AgentSignalMessage(message=text).model_dump(mode="json"), source_id="user", sequence=sequence)


async def main():
    with tempfile.TemporaryDirectory() as directory:
        async with AutoAgentHost(workflows=workflows(), lifespan=lifespan, environment={},
            settings=HostSettings(database_url="sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db"))) as host:
            execution = await host.execute("agent", AgentInput(message="Delegate the research."))
            while True:
                result = await host.join(execution)
                if result.status != "waiting":
                    break
                await asyncio.sleep(0.01)
            if result.status != "completed":
                raise RuntimeError(result.error or result.status)
            print(AgentResult.model_validate_json(json.dumps(result.output)).text)


if __name__ == "__main__":
    asyncio.run(main())
