"""SDK resource ownership shared by generation and embedding Providers."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Self

from .base import ProviderError
from .config import OpenAIProviderConfig

if TYPE_CHECKING:
    from openai import AsyncOpenAI
    from ..recording import LLMCallRecorder


class OpenAIClientOwner:
    protocol: str
    error_type: type[ProviderError]

    def __init__(
        self, config: OpenAIProviderConfig | None = None, *,
        client: AsyncOpenAI | None = None,
        recorder: LLMCallRecorder | None = None, record_calls: bool = True,
    ) -> None:
        self.config = config or OpenAIProviderConfig()
        self._client = client
        self._owns_client = client is None
        self._loop = None
        self._active = 0
        self._closed = False
        if type(record_calls) is not bool:
            raise TypeError("record_calls must be a boolean.")
        if recorder is not None:
            from ..recording import LLMCallRecorder
            if not isinstance(recorder, LLMCallRecorder):
                raise TypeError("Recorder must implement record_llm_call.")
        self.recorder = recorder
        self.record_calls = record_calls

    @property
    def provider_id(self) -> str:
        return self.config.provider_id

    def _bind_loop(self) -> None:
        if self._closed:
            raise RuntimeError("Provider is closed.")
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise RuntimeError("Provider must be used and closed on its owning event loop.")
        self._loop = loop

    def _get_client(self):
        self._bind_loop()
        if self._client is None:
            try:
                from openai import AsyncOpenAI
            except ImportError as error:
                raise ImportError("Install autoagent[agent] to use the OpenAI SDK Provider.") from error
            self._client = AsyncOpenAI(
                api_key=self.config.api_key.get_secret_value() if self.config.api_key is not None else None,
                base_url=self.config.base_url, timeout=self.config.timeout_seconds,
                default_headers=self.config.headers or None, max_retries=0,
            )
        return self._client

    async def aprepare(self) -> None:
        """Create an owned SDK client on the calling event loop."""
        self._get_client()

    async def __aenter__(self) -> Self:
        await self.aprepare()
        return self

    async def __aexit__(self, *_args) -> None:
        await self.aclose()

    def _error(self, error: Exception) -> ProviderError:
        from autoagent.core.usage import UsageAccountingError
        if isinstance(error, UsageAccountingError):
            return error
        retryable = False
        status = getattr(error, "status_code", None)
        request_id = getattr(error, "request_id", None)
        try:
            from openai import APIConnectionError, APIStatusError, APITimeoutError
            retryable = isinstance(error, (APIConnectionError, APITimeoutError)) or (
                isinstance(error, APIStatusError) and (status in {408, 409, 429} or status >= 500)
            )
        except ImportError:
            pass
        return self.error_type(
            str(error), provider_id=self.provider_id, protocol=self.protocol,
            retryable=retryable, status_code=status, request_id=request_id,
        )

    async def aclose(self) -> None:
        if self._closed:
            return
        if self._loop is not None:
            self._bind_loop()
        if self._active:
            raise RuntimeError("Settle or close active model calls before closing the Provider.")
        if self._owns_client and self._client is not None:
            await self._client.close()
        self._closed = True
