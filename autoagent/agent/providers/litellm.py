"""One optional LiteLLM conversion layer, independent of upstream vendors."""

from __future__ import annotations

import importlib
from typing import Literal

from ..models import FileContent, ImageContent, VideoContent
from ..models._base import NonEmpty
from ._common import check_native, raw_object, source_value, unsupported
from ._extended_chat import ExtendedChatAdapter
from .config import OpenAIProviderConfig
from .embedding import OpenAIEmbeddingProvider
from .openai import OpenAIProvider
from .responses import ResponsesAdapter


class LiteLLMProviderConfig(OpenAIProviderConfig):
    """Connection settings; model_prefix selects a LiteLLM route, not a fallback.

    With no prefix, request.model is passed unchanged. An explicit `openai`
    prefix plus base_url selects an OpenAI-compatible upstream. SDK-managed
    connection pools remain owned by LiteLLM; this wrapper never closes global
    pools that may be shared by other Providers.
    """

    provider_id: NonEmpty = "litellm"
    base_url: str | None = None
    model_prefix: NonEmpty | None = None


class LiteLLMChatCompletionsAdapter(ExtendedChatAdapter):
    """Map the SDK's OpenAI-shaped content and reasoning without vendor branches."""

    def _part(self, part, provider_id):
        if isinstance(part, VideoContent):
            check_native(part.native, provider_id, self.protocol)
            if part.start_seconds is not None or part.end_seconds is not None:
                unsupported("Extract the requested video interval before sending it to LiteLLM.", provider_id, self.protocol)
            kind, value = source_value(part.source, provider_id, self.protocol)
            if kind == "id":
                unsupported("LiteLLM video input requires a URL or inline data.", provider_id, self.protocol)
            return {"type": "video_url", "video_url": {"url": value}}
        if isinstance(part, FileContent):
            check_native(part.native, provider_id, self.protocol)
            if part.detail is not None:
                unsupported("LiteLLM Chat file input has no generic detail setting.", provider_id, self.protocol)
            kind, value = source_value(part.source, provider_id, self.protocol)
            file = {"file_id" if kind == "id" else "file_data": value}
            if part.filename is not None:
                file["filename"] = part.filename
            return {"type": "file", "file": file}
        if isinstance(part, ImageContent) and part.detail == "original":
            check_native(part.native, provider_id, self.protocol)
            kind, value = source_value(part.source, provider_id, self.protocol)
            if kind == "id":
                unsupported("Image file references require the Responses interface.", provider_id, self.protocol)
            return {"type": "image_url", "image_url": {"url": value, "detail": "original"}}
        return super()._part(part, provider_id)

    def _message(self, item, provider_id):
        if item.role == "assistant" and any(isinstance(part, ImageContent) for part in item.content):
            result = {"role": "assistant", "content": [self._part(part, provider_id) for part in item.content]}
            return result, check_native(item.native, provider_id, self.protocol).get("_group_id")
        return super()._message(item, provider_id)


class _LiteLLMTransport:
    def _get_client(self):
        self._bind_loop()
        if self._sdk is None:
            try:
                self._sdk = importlib.import_module("litellm")
            except ImportError as error:
                raise ImportError("Install autoagent[litellm] to use the LiteLLM Provider.") from error
        return self._sdk

    async def _call_sdk(self, operation, params):
        sdk = self._get_client()
        params = dict(params)
        params.update(params.pop("extra_body", {}))
        if self.config.model_prefix is not None:
            params["model"] = self.config.model_prefix.rstrip("/") + "/" + params["model"]
        params.update(timeout=self.config.timeout_seconds, num_retries=0, max_retries=0, drop_params=False)
        if self.config.api_key is not None:
            params["api_key"] = self.config.api_key.get_secret_value()
        if self.config.base_url is not None:
            params["api_base"] = self.config.base_url
        if self.config.headers:
            params["extra_headers"] = dict(self.config.headers)
        if self._sdk_client is not None:
            params["client"] = self._sdk_client
        return await getattr(sdk, operation)(**params)

    def _error(self, error):
        from autoagent.core.usage import UsageAccountingError
        if isinstance(error, UsageAccountingError):
            return error
        status = getattr(error, "status_code", None)
        return self.error_type(str(error), provider_id=self.provider_id, protocol=self.protocol,
            status_code=status, request_id=getattr(error, "request_id", None),
            retryable=(status in {408, 409, 429} or status >= 500) if isinstance(status, int) else None)


class LiteLLMProvider(_LiteLLMTransport, OpenAIProvider):
    """Borrow the LiteLLM runtime; own per-call streams and enforce one event loop.

    `sdk` and `client` are optional caller-owned injection points. Importing or
    constructing this Provider never imports LiteLLM. Use `aprepare()` or its
    async context manager on the Runtime loop to resolve the optional SDK.
    """

    def __init__(self, config: LiteLLMProviderConfig | None = None, *,
                 protocol: Literal["openai.chat_completions", "openai.responses"] = "openai.chat_completions",
                 sdk=None, client=None, **kwargs):
        self._sdk, self._sdk_client = sdk, client
        if protocol == "openai.chat_completions":
            adapter = LiteLLMChatCompletionsAdapter()
        elif protocol == "openai.responses":
            adapter = ResponsesAdapter()
        else:
            raise ValueError("LiteLLM requires Chat Completions or Responses.")
        super().__init__(config or LiteLLMProviderConfig(), adapter=adapter, **kwargs)

    async def _create(self, params):
        return await self._call_sdk("aresponses" if self.protocol == "openai.responses" else "acompletion", params)

    async def _close_stream(self, source):
        # Responses iterators do not expose close methods. The Chat bridge
        # owns a CustomStreamWrapper; native Responses owns an HTTP response.
        if not callable(getattr(source, "aclose", None)) and not callable(getattr(source, "close", None)):
            source = getattr(source, "litellm_custom_stream_wrapper", None) or getattr(source, "response", None)
        if source is not None:
            await super()._close_stream(source)


class LiteLLMEmbeddingProvider(_LiteLLMTransport, OpenAIEmbeddingProvider):
    """Normalize LiteLLM vectors through the same portable embedding contract."""

    def __init__(self, config: LiteLLMProviderConfig | None = None, *, sdk=None, client=None, **kwargs):
        self._sdk, self._sdk_client = sdk, client
        super().__init__(config or LiteLLMProviderConfig(), **kwargs)

    async def _raw_embedding(self, params):
        value = await self._call_sdk("aembedding", params)
        return raw_object(value), getattr(value, "_request_id", None)


__all__ = ["LiteLLMProviderConfig", "LiteLLMProvider", "LiteLLMEmbeddingProvider", "LiteLLMChatCompletionsAdapter"]
