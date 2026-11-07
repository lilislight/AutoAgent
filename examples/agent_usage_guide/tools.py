"""Typed Tools, schemas, dependency injection, policy/hooks and failure semantics.

Run: .venv/bin/python -m examples.agent_usage_guide.tools
"""

import asyncio
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from autoagent.agent import (
    DataSource, ImageContent, LLMToolCall, Tool, ToolBusinessError, ToolContext, ToolDecision,
    ToolExecutionError, ToolHooks, ToolOutput, ToolOutputError, ToolSet, tool,
)
from .support import show


class Stock(BaseModel):
    model_config = ConfigDict(extra="forbid")
    item: str
    quantity: int


@tool
def stock(item: str, context: ToolContext) -> Stock:
    """Read stock from an application-injected service."""
    inventory = context.services["inventory"]
    if item not in inventory:
        raise ToolBusinessError("No inventory record", code="not_found")
    return Stock(item=item, quantity=inventory[item])


@tool
async def quote(quantity: Annotated[int, Field(gt=0)]) -> dict[str, int]:
    """Calculate a fixed example quote."""
    await asyncio.sleep(0)
    return {"price": quantity * 5}


@tool
def preview() -> ToolOutput:
    """Return structured data and media in one Tool observation."""
    return ToolOutput(structured_content={"label": "example"}, content=(
        ImageContent(source=DataSource(mime_type="image/png", data="YQ==")),))


@tool
def broken() -> str:
    """Demonstrate an unexpected implementation failure."""
    raise RuntimeError("Example upstream service crashed")


@tool
def bad_output() -> int:
    """Demonstrate a handler violating its declared output type."""
    return "wrong type"  # type: ignore[return-value]


def call(name, arguments="{}", call_id=None):
    return LLMToolCall(name=name, arguments=arguments, call_id=call_id or name)


async def main():
    events = []
    context = ToolContext(user_id="alice", services={"inventory": {"apple": 3}})

    def policy(incoming, current):
        if incoming.name == "preview":
            return ToolDecision(action="deny", reason="Preview is disabled for this example user")
        return ToolDecision(action="allow")

    def before(incoming, current):
        events.append({"event": "before", "tool": incoming.name, "call": current.call_id})

    def after(incoming, result, current):
        events.append({"event": "after", "tool": incoming.name, "error": result.is_error})

    def on_error(incoming, error, current):
        events.append({"event": "exception", "type": type(error).__name__})

    tools = ToolSet((stock, quote, preview, broken, bad_output), context=context,
                    policy=policy, hooks=ToolHooks(before=before, after=after, on_error=on_error))
    schema = tools.select(include=("stock",)).definitions[0]
    assert "context" not in schema.parameters["properties"]
    show("model-facing schema (no services or identity)", schema)
    # The decorator preserves ordinary Python calls; Tool adds validated dispatch.
    assert stock("apple", context).quantity == 3
    assert Tool(stock).invoke(call("stock", '{"item":"apple"}'), context=context).structured_content["quantity"] == 3
    values = await asyncio.gather(
        tools.ainvoke(call("stock", '{"item":"apple"}')),
        tools.ainvoke(call("quote", '{"quantity":2}')),
    )
    assert values[1].structured_content == {"price": 10}
    for incoming, expected in (
        (call("stock", '{"item":"unknown"}'), "not_found"),
        (call("quote", '{"quantity":0}'), "invalid_arguments"),
        (call("missing"), "unknown_tool"),
        (call("preview"), "tool_access_denied"),
    ):
        result = await tools.ainvoke(incoming)
        assert result.is_error and result.structured_content["error"]["kind"] == expected
        show("expected observation", result)
    denied = await tools.ainvoke(call("quote", '{"quantity":2}'), context=context.restrict(("stock",)))
    assert denied.is_error and denied.structured_content["error"]["kind"] == "tool_access_denied"
    for name, error_type in (("broken", ToolExecutionError), ("bad_output", ToolOutputError)):
        try:
            await tools.ainvoke(call(name))
        except error_type as error:
            show("unexpected failure propagates", {"type": type(error).__name__})
        else:
            raise AssertionError("Expected the example failure")
    assert (await Tool(preview).ainvoke(call("preview"))).content[0].type == "image"
    show("hooks", events)


if __name__ == "__main__":
    asyncio.run(main())
