"""Run .venv/bin/python -m examples.agent_node_operations_demo; entirely offline.

Demonstrates independent typed Skill/MCP Nodes, a nested synchronous Tool,
and a Node execution deadline. The MCP server is a local SDK subprocess.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from contextlib import asynccontextmanager

from autoagent import AutoAgentApp, Node, TimeoutPolicy, Workflow
from autoagent.agent import (
    LLMToolCall, LLMToolResult, MCPClient, MCPStdioConfig, MCPOperations,
    MCPReadResourceRequest, MCPGetPromptRequest, MemorySkillSource, Skill, SkillSet,
    SkillOperations, SkillReadTextRequest, ToolSet, tool,
)


SERVER = """
from mcp.server import MCPServer
server = MCPServer('node-operations')
@server.resource('memory://guide')
def guide() -> str:
    return 'Read the source before answering.'
@server.prompt()
def summarize(topic: str) -> str:
    return 'Summarize ' + topic
server.run()
"""


@tool
def identify_worker() -> str:
    """Report the execution thread without external side effects."""
    return threading.current_thread().name


async def main():
    skills = SkillSet((Skill(MemorySkillSource("guide", {
        "SKILL.md": "---\nname: guide\ndescription: Example guide\n---\nRead the rules.",
        "references/rules.txt": "First rule.\nSecond rule.\n",
    })),))
    skill_operations = SkillOperations(skills)
    client = MCPClient(MCPStdioConfig(server_id="local", command=sys.executable, args=("-c", SERVER)))
    mcp_operations = MCPOperations(client)
    tools = ToolSet((identify_worker,))

    @asynccontextmanager
    async def resources():
        async with client:
            yield

    async def embedded(call: LLMToolCall) -> LLMToolResult:
        return await tools.ainvoke(call)

    async def slow() -> None:
        await asyncio.sleep(60)

    cases = (
        ("skill", Node("read", skill_operations.as_operator("read_text"), timeout=TimeoutPolicy(2)),
         SkillReadTextRequest(name="guide", path="references/rules.txt", start_line=2)),
        ("resource", Node("read", mcp_operations.as_operator("read_resource")), MCPReadResourceRequest(uri="memory://guide")),
        ("prompt", Node("get", mcp_operations.as_operator("get_prompt")), MCPGetPromptRequest(name="summarize", arguments={"topic": "the guide"})),
        ("nested_tool", Node("call", embedded), LLMToolCall(name="identify_worker", call_id="1", arguments="{}")),
        ("deadline", Node("slow", slow, timeout=TimeoutPolicy(.02)), None),
    )
    app = AutoAgentApp(runtime_lifespan=resources)
    report = {}
    try:
        await app.aprepare()
        for name, node, request in cases:
            result = await app.ainvoke(Workflow(name, nodes=[node]), request)
            expected = "failed" if name == "deadline" else "completed"
            if result.status != expected:
                raise RuntimeError(result.error)
            report[name] = {"status": result.status, "output": result.output,
                            "error_type": result.error.type if result.error else None}
        assert report["nested_tool"]["output"]["content"][0]["text"].startswith("autoagent-operator-")
        assert report["deadline"]["error_type"] == "NodeTimeoutError"
    finally:
        await app.aclose()
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
