from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from autoagent.ai.models.llm import LLMRequest, LLMResponse, LLMStreamChunk


class LLMProvider(ABC):
    """Provider-neutral interface used by the llm_call Operator."""

    provider_name = "llm"

    @abstractmethod
    async def ainvoke(self, request: LLMRequest) -> LLMResponse:
        """Execute one normalized LLM request."""

    @abstractmethod
    def astream(
        self,
        request: LLMRequest,
    ) -> AsyncIterator[LLMStreamChunk]:
        """Stream one normalized LLM request."""

    async def aclose(self) -> None:
        """Release resources owned by this Provider."""


class LLMProviderError(RuntimeError):
    """Normalized failure raised by an LLM Provider."""

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        retryable: bool | None = None,
        status_code: int | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.retryable = retryable
        self.status_code = status_code
        self.request_id = request_id
