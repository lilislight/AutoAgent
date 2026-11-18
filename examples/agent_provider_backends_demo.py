"""Offline Node example: --backend deepseek or --backend litellm.

Run .venv/bin/python -m examples.agent_provider_backends_demo --backend deepseek
or select litellm after installing autoagent[litellm]. All HTTP is mocked;
no real API key or model service is used. Both backends use native Responses.
"""

from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
import asyncio
import json
import os
from unittest.mock import patch

from autoagent import Node, Stream, ValueContract, Workflow
from autoagent.core.app import AutoAgentApp
from autoagent.agent import (
    DeepSeekProvider, DeepSeekProviderConfig, LiteLLMProvider, LiteLLMProviderConfig,
    LLM_CALL, LLMMessage, LLMProviderOptions, LLMRequest, LLMResponse,
    LLMStreamReducer, TextContent, llm_call_operator,
)


async def main(backend: str):
    import httpx

    os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    if backend == "deepseek":
        provider = DeepSeekProvider(DeepSeekProviderConfig(api_key="offline-placeholder"), protocol="openai.responses")
    else:
        provider = LiteLLMProvider(LiteLLMProviderConfig(provider_id="deepseek", api_key="offline-placeholder",
            model_prefix="openai", base_url="https://api.deepseek.com"), protocol="openai.responses")

    async def send(client, request, **kwargs):
        if request.url.host != "api.deepseek.com" or request.url.path != "/responses":
            raise RuntimeError("Unexpected HTTP request in offline demo.")
        body = json.loads(request.content)
        assert body["reasoning"]["effort"] == "max"
        return httpx.Response(200, request=request, json={
            "id": "resp_demo", "object": "response", "created_at": 1, "status": "completed",
            "model": "deepseek-flash", "output": [
                {"type": "message", "id": "msg1", "role": "assistant", "status": "completed",
                 "content": [{"type": "output_text", "text": "Both backends share one model contract.", "annotations": []}]}],
            "usage": {"input_tokens": 8, "output_tokens": 9, "total_tokens": 17}})

    @asynccontextmanager
    async def resources():
        async with provider:
            yield

    app = AutoAgentApp(runtime_lifespan=resources)
    app.register_capability(LLM_CALL)
    app.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id, default=True)
    try:
        with patch.object(httpx.AsyncClient, "send", send):
            await app.aprepare()
            result = await app.ainvoke(Workflow("provider_demo", nodes=[Node("llm", LLM_CALL, stream=Stream(LLMStreamReducer()))]), LLMRequest(
                model="deepseek-flash", input=(LLMMessage(role="user", content=(TextContent(text="Hello"),)),),
                provider_options=LLMProviderOptions(protocol="openai.responses", values={"reasoning": {"effort": "max"}})))
            if result.status != "completed":
                raise RuntimeError(result.error)
            response = ValueContract.create(LLMResponse, location="output").restore(result.output)
            print(json.dumps({"backend": backend, "text": response.text, "usage": response.usage.model_dump()}, indent=2))
    finally:
        await app.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("deepseek", "litellm"), default="deepseek")
    asyncio.run(main(parser.parse_args().backend))
