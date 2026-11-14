"""Run from the repository root: .venv/bin/python -m examples.agent_workflow.demo.

This offline Provider demonstrates the real Agent graph and native approval
Wait. Pass --approve to explicitly approve the demonstration Tool call.
Applications can register their SDK Provider in the same lifespan instead.
"""

import argparse
import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile

from autoagent.agent import (
    AgentResources, AgentModelSelection,
    AgentAccess, AgentApprovalRequest, AgentApprovalResponse, AgentInput, AgentResult,
    AgentModelSettings, AgentHostSettings, AgentSessionSettings, LLMMessage, LLMResponse,
    LLMStreamChunk, LLMToolCall, LLMToolResult, TextContent, Tool,
    agent_workflow,
)
from autoagent.host import AutoAgentHost, HostSettings


class DemoProvider:
    provider_id = "demo"

    async def ainvoke(self, request):
        results = [item for item in request.input if isinstance(item, LLMToolResult)]
        if not results:
            return LLMResponse(model=request.model, finish_reason="tool_calls", output=(
                LLMToolCall(name="publish", call_id="publish-1", arguments='{"text":"Hello from Agent"}'),))
        text = "Published." if not results[-1].is_error else "Publication was not authorized."
        return LLMResponse(model=request.model, finish_reason="stop", output=(
            LLMMessage(role="assistant", content=(TextContent(text=text),)),))

    async def astream(self, request):
        yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))


def publish(text: str) -> str:
    """Publish text to the demonstration console."""
    print("Tool effect:", text)
    return text


@asynccontextmanager
async def lifespan(setup):
    context = setup.extensions["agent"].context
    context.services.providers.register("demo", DemoProvider(), version="1")
    # Providers and Tools remain application-owned runtime instances.
    yield


async def main(*, approve=False):
    with tempfile.TemporaryDirectory() as directory:
        async with AutoAgentHost(workflows=[agent_workflow(resources=AgentResources(tools=[publish]))], lifespan=lifespan,
            settings=HostSettings(database_url="sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db")),
            environment={}) as host:
            # Generation and resource policy belong to this Agent. Sessions select
            # only their model and approval mode.
            host.extensions["agent"].context.set_defaults(AgentHostSettings(
                model=AgentModelSettings(provider="demo", model="demo-model", streaming=True),
                access=AgentAccess(rules={"publish": "ask"})))
            await host.extensions["agent"].context.configure("demo-session", AgentSessionSettings(
                model=AgentModelSelection(provider="demo", model="demo-model")))
            execution = await host.execute("agent", AgentInput(message="Publish a greeting."), session_id="demo-session")
            result = await host.join(execution)
            if result.waits:
                wait = result.waits[0]
                batch = AgentApprovalRequest.model_validate_json(json.dumps(wait.request))
                for request in batch.requests:
                    print("Approval request:", request.call.name, request.arguments)
                allowed = approve or (await asyncio.to_thread(input, "Approve this batch? [y/N] ")).strip().lower() == "y"
                reply = AgentApprovalResponse(decisions=tuple(request.respond(approved=allowed) for request in batch.requests),
                    revision=batch.revision)
                await host.respond(execution, wait.id, reply.model_dump(mode="json"))
                result = await host.join(execution)
            if result.status != "completed":
                raise RuntimeError(result.error or result.status)
            print(AgentResult.model_validate_json(json.dumps(result.output)).text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approve", action="store_true", help="Explicitly approve the demonstration Tool call")
    asyncio.run(main(approve=parser.parse_args().approve))
