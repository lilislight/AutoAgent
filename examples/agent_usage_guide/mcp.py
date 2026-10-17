"""Real SDK stdio connection: discovery, Tools, progress, resources and reconnect.

Run: .venv/bin/python -m examples.agent_usage_guide.mcp
"""

import asyncio
from pathlib import Path
import sys

from autoagent.agent import (
    LLMToolCall, MCPClient, MCPGetPromptRequest, MCPOperations,
    MCPReadResourceRequest, MCPStdioConfig, MCPToolRegistry, ToolSet,
)
from .support import show
from .tools import quote


def local_client():
    return MCPClient(MCPStdioConfig(server_id="local-guide", command=sys.executable,
        args=("-m", "examples.agent_usage_guide.mcp_server"),
        cwd=str(Path(__file__).resolve().parents[2]), request_timeout_seconds=10))


async def main():
    async with local_client() as client:
        show("negotiation", {"version": client.protocol_version, "server": client.server_info,
                             "capabilities": client.server_capabilities})
        registry = MCPToolRegistry(client, prefix="remote_", include=("add",))
        await registry.refresh()
        # Capture one snapshot for both the LLM request definitions and dispatch.
        remote = registry.snapshot
        mixed = ToolSet((quote, remote["remote_add"]))
        assert {d.name for d in mixed.definitions} == {"quote", "remote_add"}
        incoming = LLMToolCall(call_id="mcp-add", name="remote_add", arguments='{"a":2,"b":3}')
        result = await mixed.ainvoke(incoming)
        assert result.call_id == incoming.call_id and result.structured_content == {"sum": 5}
        progress = []
        async for chunk in remote["remote_add"].astream(incoming):
            if chunk.type == "progress":
                progress.append(chunk.progress)
            else:
                assert chunk.result.structured_content["sum"] == 5
        assert progress
        show("remote Tool", {"result": result.model_dump(), "progress": progress})

        operations = MCPOperations(client)
        resource = await operations.aread_resource(MCPReadResourceRequest(uri="memory://guide"))
        prompt = await operations.aget_prompt(MCPGetPromptRequest(name="summarize", arguments={"topic": "inventory"}))
        assert prompt.messages and resource.contents
        assert await client.list_resources() and await client.list_resource_templates() and await client.list_prompts()
        show("resource", resource)
        show("prompt messages (can be used as LLMRequest.input)", prompt)

        # Modern subscription startup explicitly marks a required state resync.
        async with client.listen_resilient(tools_list_changed=True) as subscription:
            async with asyncio.timeout(5):
                update = await anext(subscription)
            assert update.type == "connected" and update.resync_required
            show("subscription boundary", update)

        old_generation = client.generation
        await client.areconnect()
        assert client.generation > old_generation
        stale = await remote.ainvoke(incoming)
        assert stale.is_error and stale.structured_content["error"]["kind"] == "stale_tool"
        assert (await registry.snapshot.ainvoke(incoming)).structured_content["sum"] == 5
        show("reconnect", {"generation": client.generation, "registry_revision": registry.revision})


if __name__ == "__main__":
    asyncio.run(main())
