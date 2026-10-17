"""Switch connection adapters without replacing LLMRequest/LLMResponse.

Offline by default. Real network calls require --live and application credentials.
Run: .venv/bin/python -m examples.agent_usage_guide.providers --backend deepseek
"""

import argparse
import asyncio
import os

from autoagent.agent import (
    DeepSeekProvider, DeepSeekProviderConfig, EmbeddingRequest, LiteLLMEmbeddingProvider,
    LiteLLMProvider, LiteLLMProviderConfig, LLMProviderOptions, LLMRequest,
    OpenAIChatCompletionsProvider, OpenAIEmbeddingProvider, OpenAIProviderConfig,
    OpenAIResponsesProvider,
)
from .support import offline_provider, request, show, user


async def main(options):
    protocol = "openai.responses" if options.protocol == "responses" else "openai.chat_completions"
    if not options.live:
        async with offline_provider(options.backend, protocol) as (provider, api):
            value = await provider.ainvoke(request())
            assert value.text == "Hello from the offline SDK."
            # LiteLLM 1.105.1 Responses iterators lack close/aclose, which the
            # current AutoAgent Provider requires. Do not present that as working.
            stream_verified = not (options.backend == "litellm" and options.protocol == "responses")
            if stream_verified:
                chunks = [chunk async for chunk in provider.astream(request())]
                assert chunks[-1].response.text == value.text
            show("offline backend", {"backend": options.backend, "protocol": protocol, "text": value.text,
                                      "requests": len(api.requests), "stream_verified": stream_verified})
        return

    if options.backend == "litellm" and options.protocol == "responses" and options.stream and not options.embedding:
        raise SystemExit("Known current limitation: LiteLLM Responses streaming fails during iterator cleanup. Use Chat streaming or a direct Responses provider.")
    if not options.model:
        raise SystemExit("--live requires --model (choose a model supported by your endpoint).")
    if options.backend == "deepseek":
        if options.embedding:
            raise SystemExit("The DeepSeek adapter does not expose an Embedding endpoint.")
        key = os.environ.get("DEEPSEEK_API_KEY")
        if not key:
            raise SystemExit("Set DEEPSEEK_API_KEY before using --live.")
        provider = DeepSeekProvider(DeepSeekProviderConfig(api_key=key,
            base_url=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")), protocol=protocol)
    elif options.backend == "litellm":
        # Prefix/model select LiteLLM's upstream route. They do not add fallback.
        # For native Responses on an OpenAI-compatible service use prefix=openai + base URL.
        config = LiteLLMProviderConfig(model_prefix=options.prefix,
            base_url=os.environ.get("LLM_BASE_URL"), api_key=os.environ.get("LLM_API_KEY"))
        provider = LiteLLMEmbeddingProvider(config) if options.embedding else LiteLLMProvider(config, protocol=protocol)
    else:
        key = os.environ.get("OPENAI_API_KEY")
        if not key:
            raise SystemExit("Set OPENAI_API_KEY before using --live.")
        config = OpenAIProviderConfig(api_key=key, base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"))
        provider = OpenAIEmbeddingProvider(config) if options.embedding else (
            OpenAIResponsesProvider if protocol == "openai.responses" else OpenAIChatCompletionsProvider)(config)

    async with provider:
        if options.embedding:
            response = await provider.ainvoke(EmbeddingRequest(model=options.model, input=options.prompt))
            show("live embeddings", {"count": len(response.data), "dimensions": len(response.vectors[0]),
                                     "usage": response.usage.model_dump() if response.usage else None})
            return
        # JSON API parameters are scoped to a protocol; credentials stay in config.
        extension = None
        if options.backend == "deepseek":
            values = {"reasoning": {"effort": "none"}} if options.protocol == "responses" else {"thinking": {"type": "disabled"}}
            extension = LLMProviderOptions(protocol=protocol, provider_id=provider.provider_id, values=values)
        value = LLMRequest(model=options.model, input=(user(options.prompt),), provider_options=extension)
        if options.stream:
            async for chunk in provider.astream(value):
                if chunk.type == "delta" and chunk.kind == "text":
                    print(chunk.delta, end="", flush=True)
                elif chunk.type == "completed":
                    print()
                    show("final usage", chunk.response.usage)
        else:
            response = await provider.ainvoke(value)
            show("live response", {"text": response.text, "usage": response.usage.model_dump() if response.usage else None})


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--backend", choices=("openai", "deepseek", "litellm"), default="openai")
    result.add_argument("--protocol", choices=("responses", "chat"), default="responses")
    result.add_argument("--live", action="store_true", help="Make actual, potentially billed provider requests")
    result.add_argument("--model")
    result.add_argument("--prefix", help="LiteLLM routing prefix, e.g. deepseek or openai")
    result.add_argument("--prompt", default="Say hello in one sentence.")
    result.add_argument("--stream", action="store_true")
    result.add_argument("--embedding", action="store_true", help="Live Embedding call; use embeddings.py for offline coverage")
    return result


if __name__ == "__main__":
    asyncio.run(main(parser().parse_args()))
