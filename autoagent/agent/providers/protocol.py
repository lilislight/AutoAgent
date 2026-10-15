"""Replaceable OpenAI wire adapters; transport stays in OpenAIProvider."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from ..models import LLMRequest, LLMResponse
from ..models.streaming import LLMStreamChunk


class OpenAIProtocolAdapter(ABC):
    """A provider-specific variant can override just encoding or decoding.

    Adapters must be stateless across calls. Streaming accumulation belongs to
    each decode_stream invocation, so concurrent model calls remain isolated.
    """

    protocol: str

    def stream_parameters(self, params: dict) -> dict:
        return {**params, "stream": True}

    @abstractmethod
    def encode_request(self, request: LLMRequest, *, provider_id: str) -> dict: ...

    @abstractmethod
    def decode_response(
        self, raw: dict, *, provider_id: str, request: LLMRequest | None = None,
    ) -> LLMResponse: ...

    @abstractmethod
    def decode_stream(
        self, source: AsyncIterator[object], *, request: LLMRequest, provider_id: str,
    ) -> AsyncIterator[LLMStreamChunk]: ...


__all__ = ["OpenAIProtocolAdapter"]
