"""Canonical Agent Capabilities and borrowed Provider implementation adapters.

Declarations create no Provider clients and import no optional SDK. Bind an
implementation with register_operator in either Core App or HostSetup; Provider
creation and cleanup remain the application's lifespan responsibility.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from autoagent.core.operators import Operator, OperatorContract, ValueContract
from autoagent.core.workflow import Capability, Stream
from .llm import LLMStreamReducer

from ..models import (
    EmbeddingRequest, EmbeddingResponse, LLMRequest, LLMResponse, LLMStreamChunk,
)
from ..providers.base import EmbeddingProvider, LLMProvider


LLM_CALL = Capability(
    "llm.call",
    OperatorContract(
        input=ValueContract.create(LLMRequest, location="llm.call input"),
        output=None,
        stream_chunk=ValueContract.create(LLMStreamChunk, location="llm.call chunk"),
    ),
    stream=Stream(LLMStreamReducer()),
)
"""One LLMRequest -> LLMResponse Node for ordinary and streaming calls.

The default Reducer retains the complete terminal response. Node.stream may
override it; User Events require a separate, explicit Node.user_events Mapper.
Ordinary implementations emit just the completed chunk; streaming ones forward
Provider observations. Core handles Retry, cancellation and User Events.
"""

EMBEDDING = Capability(
    "embedding.call",
    OperatorContract(
        input=ValueContract.create(EmbeddingRequest, location="embedding.call input"),
        output=ValueContract.create(EmbeddingResponse, location="embedding.call output"),
    ),
)
"""One EmbeddingRequest -> EmbeddingResponse contract, without streaming."""


def llm_call_operator(
    provider: LLMProvider, *, id: str = "llm.call", streaming: bool = False,
) -> Operator:
    """Borrow a Provider and adapt either call style to the single LLM contract.

    This adapter never closes the Provider. It always closes a consumed stream,
    including when Core cancels, times out or rejects a chunk. Select streaming
    at registration time; it is not a model API option inside LLMRequest.
    """
    if type(streaming) is not bool:
        raise TypeError("streaming must be bool.")
    method = "astream" if streaming else "ainvoke"
    if not callable(getattr(provider, method, None)):
        raise TypeError(f"LLM Provider must implement {method}.")

    async def invoke(request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        if not streaming:
            response = LLMResponse.model_validate(await provider.ainvoke(request))
            yield LLMStreamChunk(type="completed", response=response)
            return
        source = provider.astream(request)
        try:
            async for chunk in source:
                yield chunk
        finally:
            close = getattr(source, "aclose", None)
            if close is not None:
                await close()

    return Operator(invoke, id=id)


def embedding_operator(
    provider: EmbeddingProvider, *, id: str = "embedding.call",
) -> Operator:
    """Borrow a Provider for the canonical Embedding contract; do not own its lifecycle."""
    if not callable(getattr(provider, "ainvoke", None)):
        raise TypeError("Embedding Provider must implement ainvoke.")

    async def invoke(request: EmbeddingRequest) -> EmbeddingResponse:
        return await provider.ainvoke(request)

    return Operator(invoke, id=id)


__all__ = ["LLM_CALL", "EMBEDDING", "llm_call_operator", "embedding_operator"]
