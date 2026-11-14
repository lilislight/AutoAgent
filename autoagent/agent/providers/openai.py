"""OpenAI SDK transport shared by independently replaceable wire adapters."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
import inspect
from typing import TYPE_CHECKING

from ..models import (
    AssetSource, DataSource, LLMMessage, LLMRequest, LLMResponse, LLMToolResult,
    ProviderSource, URLSource,
)
from ..models.streaming import LLMStreamChunk
from ._common import check_native, raw_object, require_model, unsupported
from .base import LLMProvider, LLMProviderError
from ._openai_client import OpenAIClientOwner
from .chat_completions import ChatCompletionsAdapter
from .config import LLMModelProfile, OpenAIProviderConfig
from .protocol import OpenAIProtocolAdapter
from .responses import ResponsesAdapter
from ..models.structured import LLMStructuredOutputError
from ..recording import start_llm_call, finish_llm_call

if TYPE_CHECKING:
    from openai import AsyncOpenAI
    from ..recording import LLMCallRecorder


MediaResolver = Callable[[AssetSource], Awaitable[URLSource | DataSource | ProviderSource]]


class OpenAIProvider(OpenAIClientOwner, LLMProvider):
    """One connection, one protocol adapter and optional per-model constraints.

    Clients are created lazily on the calling event loop. Use this Provider's
    async context manager inside Core runtime_lifespan for same-loop creation
    and shutdown. Injected SDK clients remain caller-owned.
    """

    error_type = LLMProviderError

    def __init__(
        self, config: OpenAIProviderConfig | None = None, *,
        adapter: OpenAIProtocolAdapter | None = None, client: AsyncOpenAI | None = None,
        model_profiles: Mapping[str, LLMModelProfile] | None = None,
        media_resolver: MediaResolver | None = None,
        recorder: LLMCallRecorder | None = None, record_calls: bool = True,
    ) -> None:
        super().__init__(config, client=client, recorder=recorder, record_calls=record_calls)
        self.adapter = adapter or ResponsesAdapter()
        if self.adapter.protocol not in {"openai.responses", "openai.chat_completions"}:
            raise ValueError("OpenAIProvider requires an OpenAI protocol adapter.")
        self._profiles = {
            name: LLMModelProfile.model_validate(profile)
            for name, profile in (model_profiles or {}).items()
        }
        self._media_resolver = media_resolver

    @property
    def protocol(self) -> str:
        return self.adapter.protocol

    def context_message_role(self, model: str | None = None) -> str:
        from urllib.parse import urlsplit
        return "system" if urlsplit(self.config.base_url).hostname == "api.openai.com" else "user"

    async def _prepare_request(self, request: LLMRequest) -> LLMRequest:
        profile = self._profiles.get(request.model)
        if profile is not None:
            profile.check(request, provider_id=self.provider_id, protocol=self.protocol)
        inputs = []
        resolved = {}
        for item in request.input:
            check_native(item.native, self.provider_id, self.protocol)
            if isinstance(item, (LLMMessage, LLMToolResult)):
                content = []
                for part in item.content:
                    check_native(part.native, self.provider_id, self.protocol)
                    source = getattr(part, "source", None)
                    if isinstance(source, AssetSource):
                        if self._media_resolver is None:
                            unsupported("AssetSource requires a media resolver.", self.provider_id, self.protocol)
                        key = (source.uri, source.mime_type)
                        if key not in resolved:
                            value = await self._media_resolver(source)
                            if not isinstance(value, (URLSource, DataSource, ProviderSource)):
                                raise TypeError("Media resolver must return a resolved media source.")
                            resolved[key] = type(value).model_validate(value)
                        # Only the validated source changes; the call boundary
                        # already validated and detached the complete request.
                        part = part.model_copy(update={"source": resolved[key]})
                    content.append(part)
                if any(before is not after for before, after in zip(item.content, content)):
                    item = item.model_copy(update={"content": tuple(content)})
            inputs.append(item)
        if any(before is not after for before, after in zip(request.input, inputs)):
            return request.model_copy(update={"input": tuple(inputs)})
        return request

    def _endpoint(self, client):
        return client.responses if self.protocol == "openai.responses" else client.chat.completions

    async def _create(self, params):
        return await self._endpoint(self._get_client()).create(**params)

    async def _close_stream(self, source):
        close = getattr(source, "aclose", None) or source.close
        result = close()
        if inspect.isawaitable(result):
            await result

    async def ainvoke(self, request: LLMRequest) -> LLMResponse:
        self._bind_loop()
        self._active += 1
        recording = response = failure = None
        request_id = None
        try:
            request = LLMRequest.model_validate(request)
            require_model(request, self.provider_id, self.protocol)
            recording = await start_llm_call(self, request)
            request = await self._prepare_request(request)
            params = self.adapter.encode_request(request, provider_id=self.provider_id)
            value = await self._create(params)
            raw = raw_object(value)
            request_id = getattr(value, "_request_id", None)
            if request_id is not None:
                raw["_request_id"] = request_id
            response = self.adapter.decode_response(raw, provider_id=self.provider_id, request=request)
            self._validate_output(request, response)
            return response
        except BaseException as error:
            failure = error if isinstance(error, (LLMProviderError, LLMStructuredOutputError, ImportError)) or not isinstance(error, Exception) else self._error(error)
            if failure is error:
                raise
            raise failure from error
        finally:
            try:
                await finish_llm_call(recording, response=response, error=failure, request_id=request_id)
            finally:
                self._active -= 1

    @staticmethod
    def _validate_output(request: LLMRequest, response: LLMResponse) -> None:
        # Function calls precede the final structured answer and have no JSON text.
        if request.output_options is not None and response.finish_reason != "tool_calls":
            from ..models import LLMJsonSchemaFormat
            from ._common import api_schema

            text_format = request.output_options.text_format
            if isinstance(text_format, LLMJsonSchemaFormat):
                text_format = LLMJsonSchemaFormat.model_validate({
                    **text_format.model_dump(mode="python"),
                    "json_schema": api_schema(text_format.json_schema, strict=text_format.strict),
                })
            response.validate_output(text_format)

    async def astream(self, request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        self._bind_loop()
        self._active += 1
        source = decoder = None
        failed = False
        recording = response = failure = None
        request_id = None
        try:
            request = LLMRequest.model_validate(request)
            require_model(request, self.provider_id, self.protocol)
            recording = await start_llm_call(self, request, streaming=True)
            request = await self._prepare_request(request)
            params = self.adapter.encode_request(request, provider_id=self.provider_id)
            params = self.adapter.stream_parameters(params)
            source = await self._create(params)
            http_response = getattr(source, "response", None)
            request_id = http_response.headers.get("x-request-id") if http_response is not None else None
            decoder = self.adapter.decode_stream(source, request=request, provider_id=self.provider_id)
            completed = False
            async for chunk in decoder:
                chunk = LLMStreamChunk.model_validate(chunk)
                if completed:
                    raise ValueError("Protocol adapter emitted data after completion.")
                if chunk.type == "completed":
                    response = chunk.response
                    completed = True
                    if request_id is not None:
                        response = chunk.response.model_copy(update={
                            "provider_metadata": {**chunk.response.provider_metadata, "_request_id": request_id}
                        })
                        chunk = LLMStreamChunk(type="completed", response=response)
                    self._validate_output(request, chunk.response)
                yield chunk
            if not completed:
                raise ValueError("Protocol adapter ended without a terminal response.")
        except BaseException as error:
            failed = True
            failure = error if isinstance(error, (LLMProviderError, LLMStructuredOutputError, ImportError)) or not isinstance(error, Exception) else self._error(error)
            if failure is error:
                raise
            raise failure from error
        finally:
            try:
                try:
                    if decoder is not None:
                        await decoder.aclose()
                finally:
                    if source is not None:
                        await self._close_stream(source)
            except BaseException as error:
                if not failed:
                    failure = error
                    raise
            finally:
                try:
                    await finish_llm_call(recording, response=response, error=failure, request_id=request_id)
                finally:
                    self._active -= 1


class OpenAIResponsesProvider(OpenAIProvider):
    def __init__(self, config: OpenAIProviderConfig | None = None, **kwargs) -> None:
        kwargs.setdefault("adapter", ResponsesAdapter())
        super().__init__(config, **kwargs)
        if self.protocol != "openai.responses":
            raise ValueError("OpenAIResponsesProvider requires a Responses adapter.")


class OpenAIChatCompletionsProvider(OpenAIProvider):
    def __init__(self, config: OpenAIProviderConfig | None = None, **kwargs) -> None:
        kwargs.setdefault("adapter", ChatCompletionsAdapter())
        super().__init__(config, **kwargs)
        if self.protocol != "openai.chat_completions":
            raise ValueError("OpenAIChatCompletionsProvider requires a Chat Completions adapter.")


__all__ = ["MediaResolver", "OpenAIProvider", "OpenAIResponsesProvider", "OpenAIChatCompletionsProvider"]
