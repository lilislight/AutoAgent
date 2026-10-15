"""Run with .venv/bin/python -m examples.agent_foundations_demo (no network)."""

from __future__ import annotations

import asyncio
import json
from tempfile import TemporaryDirectory

from pydantic import BaseModel, ConfigDict

from autoagent.agent import (
    AssetMediaResolver, LLMMessage, LLMRequest, LLMResponse, LLMToolCall,
    LocalMediaStore, SkillDocument, SkillMetadata, TextContent, ToolContext,
    ToolHooks, ToolSet, media_content, mcp_prompt_response, tool,
)


class InventoryAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    count: int


@tool
async def inventory(item: str, context: ToolContext) -> dict[str, int]:
    """Look up inventory using the application's injected service."""
    return {"count": context.services["inventory"][item]}


@tool
async def reset_inventory() -> str:
    """A capability excluded by the activated Skill's declaration."""
    return "reset"


async def main():
    events = []

    def after(call, result, context):
        events.append({"tool": call.name, "call_id": context.call_id, "error": result.is_error})

    skill = SkillDocument(metadata=SkillMetadata(name="inventory", description="Read inventory", allowed_tools="inventory"),
                          instructions="Read the current quantity before answering.", revision="a" * 64)
    context = skill.restrict_tools(ToolContext(user_id="demo", services={"inventory": {"apple": 3}}))
    tools = ToolSet([inventory, reset_inventory], context=context, hooks=ToolHooks(after=after))
    result = await tools.ainvoke(LLMToolCall(call_id="lookup-1", name="inventory", arguments='{"item":"apple"}'))
    denied = await tools.ainvoke(LLMToolCall(call_id="reset-1", name="reset_inventory", arguments="{}"))

    prompt = mcp_prompt_response({"messages": [{"role": "user", "content": {"type": "text", "text": "Summarize this report."}}]})
    with TemporaryDirectory() as directory:
        store = LocalMediaStore(directory)
        source = store.put_bytes(b"apple: 3", mime_type="text/plain", filename="inventory.txt")
        restored = LocalMediaStore(directory)
        inline = await AssetMediaResolver(restored)(source)
        request = LLMRequest(model="application-selected-model", tools=tools.definitions,
                             input=(*prompt.messages, skill.to_message(),
                                    LLMMessage(role="user", content=(media_content(inline, filename="inventory.txt"),))))

        # Substitute the final model response to keep this example deterministic.
        response = LLMResponse(model=request.model, finish_reason="stop", output=(
            LLMMessage(role="assistant", content=(TextContent(text='{"count":3}'),)),))
        parsed = response.parse(InventoryAnswer)
        print(json.dumps({"parsed": parsed.model_dump(), "tool_output": result.structured_content,
                          "denied": denied.is_error, "events": events,
                          "media_restored": restored.get(source).filename, "request_items": len(request.input)},
                         ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
