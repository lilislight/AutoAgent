"""Host's default LLM/Embedding Capabilities with real SDKs over offline HTTP.

Run: .venv/bin/python -m examples.agent_usage_guide.capabilities
No credentials or external network access are required. Ordinary and streaming
LLM implementations use the same Workflow definition and return LLMResponse.
"""

import asyncio
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

from autoagent import Node, ValueContract, Workflow
from autoagent.agent import (
    EMBEDDING, LLM_CALL, EmbeddingRequest, EmbeddingResponse, LLMResponse,
    OpenAIEmbeddingProvider, OpenAIResponsesProvider,
    embedding_operator, llm_call_operator,
)
from autoagent.host import AutoAgentHost, HostSettings
from .support import connection, offline_http, request, show


async def main():
    workflows = [
        Workflow("generate", nodes=[Node("call", LLM_CALL)]),
        Workflow("embed", nodes=[Node("call", EMBEDDING)]),
    ]
    revisions = []
    async with offline_http() as api:
        with tempfile.TemporaryDirectory() as directory:
            settings = HostSettings(database_url="sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db"))
            for streaming in (False, True):
                @asynccontextmanager
                async def resources(setup):
                    # Host already registers both Capability declarations.
                    # This binds implementations; no Resolver is needed.
                    async with OpenAIResponsesProvider(connection()) as llm, OpenAIEmbeddingProvider(connection()) as embeddings:
                        setup.register_operator(llm_call_operator(llm),
                                                capability_id=LLM_CALL.id, default=True)
                        setup.register_operator(embedding_operator(embeddings), capability_id=EMBEDDING.id, default=True)
                        yield

                async with AutoAgentHost(workflows=workflows, settings=settings, lifespan=resources, environment={}) as host:
                    generated = await host.join(await host.execute("generate", request(streaming=streaming)))
                    embedded = await host.join(await host.execute("embed", EmbeddingRequest(model="demo-embedding", input=("one", "two"))))
                    assert generated.status == "completed", generated.error
                    assert embedded.status == "completed", embedded.error
                    answer = ValueContract.create(LLMResponse, location="output").restore(generated.output)
                    vectors = ValueContract.create(EmbeddingResponse, location="output").restore(embedded.output)
                    revisions.append(generated.ref.workflow_revision_id)
                    show("streamed" if streaming else "ordinary", {"text": answer.text, "vectors": vectors.vectors})
            assert revisions[0] == revisions[1]
            assert len(api.requests) == 4


if __name__ == "__main__":
    asyncio.run(main())
