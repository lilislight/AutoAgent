"""Four input forms, base64 normalization, batching and LiteLLM embeddings.

Run: .venv/bin/python -m examples.agent_usage_guide.embeddings [--litellm]
"""

import argparse
import asyncio

from autoagent.agent import EmbeddingRequest, EmbeddingResponse, LiteLLMEmbeddingProvider, OpenAIEmbeddingProvider
from .support import connection, offline_http, show


async def main(use_litellm=False):
    backend = "litellm" if use_litellm else "openai"
    cls = LiteLLMEmbeddingProvider if use_litellm else OpenAIEmbeddingProvider
    inputs = ("one document", ("first document", "second document"), (10, 20, 30), ((10, 20), (30, 40)))
    async with offline_http():
        async with cls(connection(backend)) as provider:
            for value in inputs:
                # LiteLLM checks model names when validating optional dimensions.
                request = EmbeddingRequest(model="text-embedding-3-small", input=value, dimensions=3, encoding_format="base64")
                response = await provider.ainvoke(request)
                assert len(response.vectors) == request.input_count
                # The fixture returns indices in reverse order; providers restore input order.
                assert response.vectors[0] == (.25, .5, .75)
                assert EmbeddingResponse.model_validate_json(response.model_dump_json()) == response
                show("embedding", {"backend": backend, "input": value, "vectors": response.vectors,
                                    "usage": response.usage.model_dump()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--litellm", action="store_true", help="Requires the optional autoagent[litellm] extra")
    asyncio.run(main(parser.parse_args().litellm))
