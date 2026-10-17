"""LLM invocation, both protocols, streaming, structured output and replay.

Run: .venv/bin/python -m examples.agent_usage_guide.llm
"""

import asyncio

from pydantic import BaseModel, ConfigDict

from autoagent.agent import (
    LLMJsonSchemaFormat, LLMMessage, LLMOutputOptions, LLMProviderError,
    LLMRequest, LLMResponse, LLMStructuredOutputError, TextContent, ToolSet, tool,
)
from .support import offline_provider, request, show, user


class Weather(BaseModel):
    model_config = ConfigDict(extra="forbid")
    city: str
    temperature: int


@tool
def add(a: int, b: int) -> dict[str, int]:
    """Add two integers."""
    return {"sum": a + b}


async def main():
    for protocol in ("openai.responses", "openai.chat_completions"):
        async with offline_provider(protocol=protocol) as (provider, _):
            response = await provider.ainvoke(request())
            assert response.text == "Hello from the offline SDK."
            # Requests/responses can cross a JSON storage boundary.
            assert LLMResponse.model_validate_json(response.model_dump_json()) == response
            show(protocol, {"text": response.text, "usage": response.usage.model_dump()})

            observed, terminal = [], None
            # aclosing() is useful if the consumer may stop early; here consume to completion.
            async for chunk in provider.astream(request()):
                if chunk.type == "delta" and chunk.kind == "text":
                    observed.append(chunk.delta)
                elif chunk.type == "completed":
                    terminal = chunk.response
            assert terminal is not None and terminal.text == "".join(observed)
            show("stream", {"deltas": observed, "final": terminal.text})

    async with offline_provider() as (provider, api):
        structured = request("Return weather as JSON.", output_options=LLMOutputOptions(
            text_format=LLMJsonSchemaFormat.from_type(Weather)))
        weather = (await provider.ainvoke(structured)).parse(Weather)
        assert weather.temperature == 22
        show("typed JSON", weather)
        invalid = LLMResponse(model="demo", finish_reason="stop", output=(
            LLMMessage(role="assistant", content=(TextContent(text="not JSON"),)),))
        try:
            invalid.parse(Weather)
        except LLMStructuredOutputError as error:
            assert error.kind == "invalid_json"
            show("structured error", {"kind": error.kind})
        # Providers report errors, while application/Node policy chooses whether to retry.
        before = len(api.requests)
        try:
            await provider.ainvoke(LLMRequest(model="demo-rate-limit", input=(user("Hello"),)))
        except LLMProviderError as error:
            assert error.status_code == 429 and error.retryable
            assert len(api.requests) == before + 1
            show("provider error", {"status": error.status_code, "retryable": error.retryable})
        else:
            raise AssertionError("Expected the fixture's rate-limit error")

    # One application-directed Tool round, with original reasoning/native data retained.
    tools = ToolSet((add,))
    async with offline_provider("deepseek", "openai.chat_completions") as (provider, api):
        initial = request("Calculate 2 + 3 using add.", tools=tools.definitions)
        first = await provider.ainvoke(initial)
        assert len(first.tool_calls) == 1
        results = tuple([await tools.ainvoke(call) for call in first.tool_calls])
        followup = LLMRequest(model=initial.model, tools=tools.definitions,
                              input=(*initial.input, *first.output, *results))
        final = await provider.ainvoke(followup)
        assert final.text == "The sum is 5."
        assert any("reasoning_content" in m for m in api.requests[-1]["body"]["messages"])
        show("one Tool round", {"call": first.tool_calls[0].model_dump(), "answer": final.text})


if __name__ == "__main__":
    asyncio.run(main())
