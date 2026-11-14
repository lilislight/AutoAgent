"""Canonical Agent Capabilities and borrowed Provider implementation adapters.

Declarations create no Provider clients and import no optional SDK. Bind an
implementation with register_operator in either Core App or HostSetup; Provider
creation and cleanup remain the application's lifespan responsibility.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
import inspect
from typing import Awaitable, Protocol

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
Non-streaming calls emit just the completed chunk; streaming calls forward
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


def _check_provider(provider):
    for method in ("ainvoke", "astream"):
        if not callable(getattr(provider, method, None)):
            raise TypeError(f"LLM Provider must implement {method}.")


@dataclass(frozen=True, slots=True)
class LLMSelection:
    """Runtime-only route result; the caller owns the selected Provider's lifespan."""

    provider: LLMProvider
    model: str

    def __post_init__(self):
        _check_provider(self.provider)
        if not isinstance(self.model, str) or not self.model.strip():
            raise ValueError("LLM selection requires a nonempty model.")


class LLMResolver(Protocol):
    """Application hook selecting a Provider/model, without a built-in routing policy."""

    def __call__(self, request: LLMRequest) -> LLMSelection | Awaitable[LLMSelection]: ...


def llm_call_operator(
    provider: LLMProvider | None = None, *, id: str = "llm.call",
    resolver: LLMResolver | None = None,
) -> Operator:
    """Borrow a Provider or resolve one per call, using request-level streaming.

    This adapter never closes the Provider. It always closes a consumed stream,
    including when Core cancels, times out or rejects a chunk. A resolver must
    preserve an explicitly requested model. Each Operator attempt resolves once;
    routing hints stay in AutoAgent and are never model API parameters.
    """
    if (provider is None) == (resolver is None):
        raise ValueError("Pass exactly one of provider or resolver.")
    if provider is not None:
        _check_provider(provider)
    elif not callable(resolver):
        raise TypeError("LLM resolver must be callable.")

    async def invoke(request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        selected_provider = provider
        if resolver is not None:
            selection = resolver(request)
            if inspect.isawaitable(selection):
                selection = await selection
            if not isinstance(selection, LLMSelection):
                raise TypeError("LLM resolver must return LLMSelection.")
            if request.model is not None and request.model != selection.model:
                raise ValueError("LLM resolver cannot replace an explicit model.")
            selected_provider = selection.provider
            request = LLMRequest.model_validate({**request.model_dump(mode="python"), "model": selection.model})
        source = _invoke_provider(selected_provider, request)
        try:
            async for chunk in source:
                yield chunk
        finally:
            await source.aclose()

    return Operator(invoke, id=id)


async def _invoke_provider(selected_provider, request):
    """Run one resolved Provider without re-resolving it during stream consumption."""
    if request.model is None:
        raise ValueError("Set a model on LLMRequest or supply an LLM resolver.")
    if not request.streaming:
        response = LLMResponse.model_validate(await selected_provider.ainvoke(request))
        yield LLMStreamChunk(type="completed", response=response)
        return
    source = selected_provider.astream(request)
    try:
        async for chunk in source:
            yield chunk
    finally:
        close = getattr(source, "aclose", None)
        if close is not None:
            await close()


def embedding_operator(
    provider: EmbeddingProvider, *, id: str = "embedding.call",
) -> Operator:
    """Borrow a Provider for the canonical Embedding contract; do not own its lifecycle."""
    if not callable(getattr(provider, "ainvoke", None)):
        raise TypeError("Embedding Provider must implement ainvoke.")

    async def invoke(request: EmbeddingRequest) -> EmbeddingResponse:
        return await provider.ainvoke(request)

    return Operator(invoke, id=id)


__all__ = ["LLM_CALL", "EMBEDDING", "LLMSelection", "LLMResolver", "llm_call_operator", "embedding_operator"]
