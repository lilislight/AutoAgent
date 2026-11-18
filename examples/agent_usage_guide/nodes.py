"""Each capability as a Node, and several capabilities inside one Operator.

Run: .venv/bin/python -m examples.agent_usage_guide.nodes
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import json
import threading

from autoagent import Node, Stream, TimeoutPolicy, ValueContract, Workflow
from autoagent.core.app import AutoAgentApp
from autoagent.agent import (
    EMBEDDING, LLM_CALL, embedding_operator, llm_call_operator,
    EmbeddingRequest, EmbeddingResponse, LLMRequest, LLMResponse,
    LLMToolCall, LLMToolResult, MCPGetPromptRequest, MCPPromptResponse, MCPReadResourceRequest,
    MCPResourceResponse, MCPOperations, MCPToolRegistry, MCPToolStreamChunk, MCPToolStreamReducer, OpenAIEmbeddingProvider,
    OpenAIResponsesProvider, SkillDocument, SkillLoadRequest, SkillOperations,
    SkillReadTextRequest, SkillScriptResult, SkillRunScriptRequest, SkillSet,
    SkillTextContent, ToolContext, ToolSet, tool,
)
from .mcp import local_client
from .skills import SKILL_ROOT, make_runner
from .support import connection, offline_http, request, show, user
from .tools import stock


@tool
def worker_name() -> str:
    """Show which worker runs a synchronous Tool embedded in an async Operator."""
    return threading.current_thread().name


async def main():
    llm = OpenAIResponsesProvider(connection())
    embedding = OpenAIEmbeddingProvider(connection())
    client = local_client()
    remote = MCPToolRegistry(client, prefix="remote_")
    mcp = MCPOperations(client)
    skills = SkillSet.discover([SKILL_ROOT]).freeze()
    skill = SkillOperations(skills, runner=make_runner())
    tools = ToolSet((stock, worker_name), context=ToolContext(services={"inventory": {"apple": 3}}))

    @asynccontextmanager
    async def resources():
        # Enter SDK clients on Core's Runtime loop, not the caller's event loop.
        async with llm, embedding, client:
            await remote.refresh()
            yield

    async def embedded(value: LLMRequest) -> LLMResponse:
        document = await skill.aload(SkillLoadRequest(name="stock-guide"))
        quantity = await tools.ainvoke(LLMToolCall(name="stock", call_id="embedded-stock", arguments='{"item":"apple"}'))
        worker = await tools.ainvoke(LLMToolCall(name="worker_name", call_id="worker", arguments="{}"))
        assert worker.content[0].text.startswith("autoagent-operator-")
        augmented = LLMRequest(model=value.model, input=(document.to_message(), *value.input,
            user(json.dumps(quantity.structured_content))))
        return await llm.ainvoke(augmented)

    async def slow() -> None:
        await asyncio.sleep(60)

    async def mcp_stream(call: LLMToolCall) -> AsyncIterator[MCPToolStreamChunk]:
        async for chunk in remote.snapshot["remote_add"].astream(call):
            yield chunk

    cases = (
        ("llm", Node("generate", LLM_CALL), request(), LLMResponse),
        ("llm_stream", Node("generate", LLM_CALL), request(streaming=True), LLMResponse),
        ("embedding", Node("embed", EMBEDDING), EmbeddingRequest(model="demo-embedding", input=("one", "two")), EmbeddingResponse),
        ("tool", Node("lookup", tools.as_operator()), LLMToolCall(call_id="stock", name="stock", arguments='{"item":"apple"}'), LLMToolResult),
        ("mcp_tool", Node("add", remote.as_operator()), LLMToolCall(call_id="add", name="remote_add", arguments='{"a":2,"b":3}'), LLMToolResult),
        ("mcp_stream", Node("add", mcp_stream, stream=Stream(MCPToolStreamReducer())), LLMToolCall(call_id="stream-add", name="remote_add", arguments='{"a":2,"b":3}'), LLMToolResult),
        ("mcp_resource", Node("read", mcp.as_operator("read_resource")), MCPReadResourceRequest(uri="memory://guide"), MCPResourceResponse),
        ("mcp_prompt", Node("prompt", mcp.as_operator("get_prompt")), MCPGetPromptRequest(name="summarize", arguments={"topic": "stock"}), MCPPromptResponse),
        ("skill_load", Node("load", skill.as_operator("load")), SkillLoadRequest(name="stock-guide"), SkillDocument),
        ("skill_text", Node("read", skill.as_operator("read_text")), SkillReadTextRequest(name="stock-guide", path="references/rules.md", max_lines=1), SkillTextContent),
        ("skill_script", Node("run", skill.as_operator("run_script")), SkillRunScriptRequest(name="stock-guide", path="scripts/total.py", stdin="[2,3]"), SkillScriptResult),
        ("embedded", Node("answer", embedded, timeout=TimeoutPolicy(10)), request(), LLMResponse),
    )
    app = AutoAgentApp(runtime_lifespan=resources)
    # Standalone Core App binds declarations explicitly; Host does this at startup.
    app.register_capability(LLM_CALL)
    app.register_capability(EMBEDDING)
    app.register_operator(llm_call_operator(llm), capability_id=LLM_CALL.id, default=True)
    app.register_operator(embedding_operator(embedding), capability_id=EMBEDDING.id, default=True)
    report = {}
    async with offline_http() as api:
        try:
            await app.aprepare()
            for name, node, value, output_type in cases:
                result = await app.ainvoke(Workflow(name, nodes=[node]), value)
                assert result.status == "completed", result.error
                # Runtime output is a portable record; explicitly restore the business model.
                output = ValueContract.create(output_type, location="output").restore(result.output)
                report[name] = {"output_type": type(output).__name__, "status": result.status}
                if name in {"llm", "llm_stream"}:
                    assert bool(api.requests[-1]["body"].get("stream")) == (name == "llm_stream")
                    report[name]["capability"] = LLM_CALL.id
                elif name == "embedding":
                    report[name]["capability"] = EMBEDDING.id
                await app.aunload_session(result.ref)
            timed = await app.ainvoke(Workflow("deadline", nodes=[Node("slow", slow, timeout=TimeoutPolicy(.02))]), None)
            assert timed.status == "failed" and timed.error.type == "NodeTimeoutError"
            report["deadline"] = {"status": timed.status, "error_type": timed.error.type}
            await app.aunload_session(timed.ref)
        finally:
            await app.aclose()
    show("typed Node results", report)


if __name__ == "__main__":
    asyncio.run(main())
