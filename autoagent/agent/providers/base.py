"""Provider interface and failures, independent of a particular SDK."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from ..models import EmbeddingRequest, EmbeddingResponse, LLMRequest, LLMResponse
from ..models.streaming import LLMStreamChunk


class ProviderError(RuntimeError):
    """A transport, remote execution, or response-protocol failure."""

    def __init__(
        self, message: str, *, provider_id: str, protocol: str,
        retryable: bool | None = None, status_code: int | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.provider_id = provider_id
        self.protocol = protocol
        self.retryable = retryable
        self.status_code = status_code
        self.request_id = request_id


class LLMProviderError(ProviderError):
    """A generation transport, remote execution, or protocol failure."""


class EmbeddingProviderError(ProviderError):
    """An embedding transport, remote execution, or protocol failure."""


class LLMUnsupportedRequest(LLMProviderError):
    """A request cannot be represented by the selected endpoint or profile."""

    def __init__(self, message: str, *, provider_id: str, protocol: str) -> None:
        super().__init__(
            message, provider_id=provider_id, protocol=protocol, retryable=False
        )


class LLMProvider(ABC):
    @abstractmethod
    async def ainvoke(self, request: LLMRequest) -> LLMResponse:
        """Return one terminal model response."""

    @abstractmethod
    def astream(self, request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        """Observe deltas and receive exactly one terminal response on success."""

    @abstractmethod
    async def aclose(self) -> None:
        """Release resources owned by this Provider after calls settle."""


class EmbeddingProvider(ABC):
    @abstractmethod
    async def ainvoke(self, request: EmbeddingRequest) -> EmbeddingResponse:
        """Return vectors for every input; embeddings have no delta stream."""

    @abstractmethod
    async def aclose(self) -> None:
        """Release resources owned by this Provider after calls settle."""


__all__ = [
    "EmbeddingProvider", "EmbeddingProviderError", "LLMProvider",
    "LLMProviderError", "LLMUnsupportedRequest", "ProviderError",
]
