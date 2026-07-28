from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    OpenAIError,
)

from autoagent.ai.models.llm import (
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
)
from autoagent.ai.providers.base import LLMProvider, LLMProviderError
from autoagent.ai.providers.chat_completions.codec import (
    decode_response,
    encode_request,
    resolve_structured_output_mode,
)
from autoagent.ai.providers.chat_completions.config import ChatCompletionsConfig


class ChatCompletionsProvider(LLMProvider):
    """SDK-backed Provider for OpenAI-style Chat Completions endpoints."""

    provider_name = "chat_completions"

    def __init__(
        self,
        config: ChatCompletionsConfig,
        *,
        client: Any | None = None,
    ) -> None:
        self.config = config
        self._owns_client = client is None
        self._client = client or AsyncOpenAI(
            api_key=config.api_key.get_secret_value(),
            base_url=config.base_url,
            timeout=config.timeout_ms / 1_000,
            max_retries=0,
            default_headers=config.headers or None,
        )

    async def ainvoke(self, request: LLMRequest) -> LLMResponse:
        params = self._request_params(request)
        try:
            completion = await self._client.chat.completions.create(**params)
        except OpenAIError as exc:
            raise self._normalize_error(exc) from exc
        return self._decode_response(
            _model_dump(completion, provider=self.provider_name)
        )

    async def astream(
        self,
        request: LLMRequest,
    ) -> AsyncIterator[LLMStreamChunk]:
        params = self._request_params(request)
        try:
            stream = await self._client.chat.completions.create(
                **params,
                stream=True,
            )
            accumulator = self._create_stream_accumulator(
                fallback_model=str(params["model"]),
            )
            async for sdk_chunk in stream:
                raw = _model_dump(
                    sdk_chunk,
                    provider=self.provider_name,
                )
                for chunk in accumulator.apply(raw):
                    yield chunk
            yield LLMStreamChunk(
                type="completed",
                response=accumulator.response(),
            )
        except OpenAIError as exc:
            raise self._normalize_error(exc) from exc

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.close()

    def _request_params(self, request: LLMRequest) -> dict[str, Any]:
        params = encode_request(
            request,
            default_model=self.config.default_model,
            structured_output_mode=resolve_structured_output_mode(self.config),
        )
        if request.provider_options:
            params["extra_body"] = dict(request.provider_options)
        return params

    def _decode_response(self, raw: Mapping[str, Any]) -> LLMResponse:
        return decode_response(raw, provider=self.provider_name)

    def _create_stream_accumulator(
        self,
        *,
        fallback_model: str,
    ) -> _ChatCompletionAccumulator:
        return _ChatCompletionAccumulator(
            fallback_model=fallback_model,
            provider=self.provider_name,
        )

    def _normalize_error(self, exc: OpenAIError) -> LLMProviderError:
        status_code = (
            exc.status_code if isinstance(exc, APIStatusError) else None
        )
        request_id = getattr(exc, "request_id", None)
        retryable = isinstance(
            exc,
            (APIConnectionError, APITimeoutError),
        ) or (
            status_code is not None
            and (
                status_code in {408, 409, 429}
                or status_code >= 500
            )
        )
        return LLMProviderError(
            str(exc),
            provider=self.provider_name,
            retryable=retryable,
            status_code=status_code,
            request_id=request_id,
        )


class _ChatCompletionAccumulator:
    def __init__(self, *, fallback_model: str, provider: str) -> None:
        self.provider = provider
        self.id: str | None = None
        self.model = fallback_model
        self.finish_reason: str | None = None
        self.content: list[str] = []
        self.tool_calls: dict[int, dict[str, str]] = {}
        self.usage: Mapping[str, Any] | None = None

    def apply(
        self,
        raw: Mapping[str, Any],
    ) -> tuple[LLMStreamChunk, ...]:
        if raw.get("id"):
            self.id = str(raw["id"])
        if raw.get("model"):
            self.model = str(raw["model"])
        if isinstance(raw.get("usage"), Mapping):
            self.usage = raw["usage"]

        choices = raw.get("choices")
        if not isinstance(choices, list) or not choices:
            return ()
        choice = choices[0]
        if not isinstance(choice, Mapping):
            return ()
        if choice.get("finish_reason") is not None:
            self.finish_reason = str(choice["finish_reason"])
        delta = choice.get("delta")
        if not isinstance(delta, Mapping):
            return ()

        chunks: list[LLMStreamChunk] = []
        content = delta.get("content")
        if isinstance(content, str) and content:
            self.content.append(content)
            chunks.append(
                LLMStreamChunk(type="text_delta", text_delta=content)
            )

        raw_calls = delta.get("tool_calls")
        if isinstance(raw_calls, list):
            for raw_call in raw_calls:
                if not isinstance(raw_call, Mapping):
                    continue
                index = raw_call.get("index")
                if not isinstance(index, int):
                    continue
                target = self.tool_calls.setdefault(
                    index,
                    {"id": "", "name": "", "arguments": ""},
                )
                call_id = raw_call.get("id")
                if isinstance(call_id, str):
                    target["id"] += call_id
                function = raw_call.get("function")
                name_delta = None
                arguments_delta = None
                if isinstance(function, Mapping):
                    if isinstance(function.get("name"), str):
                        name_delta = function["name"]
                        target["name"] += name_delta
                    if isinstance(function.get("arguments"), str):
                        arguments_delta = function["arguments"]
                        target["arguments"] += arguments_delta
                chunks.append(
                    LLMStreamChunk(
                        type="tool_call_delta",
                        tool_call_index=index,
                        tool_call_id=(
                            str(call_id) if call_id is not None else None
                        ),
                        tool_name=name_delta,
                        tool_arguments_delta=arguments_delta,
                    )
                )
        return tuple(chunks)

    def response(self) -> LLMResponse:
        raw_calls = [
            {
                "id": call["id"],
                "type": "function",
                "function": {
                    "name": call["name"],
                    "arguments": call["arguments"],
                },
            }
            for _, call in sorted(self.tool_calls.items())
        ]
        raw: dict[str, Any] = {
            "id": self.id,
            "model": self.model,
            "choices": [
                {
                    "finish_reason": self.finish_reason,
                    "message": {
                        "role": "assistant",
                        "content": "".join(self.content) or None,
                        "tool_calls": raw_calls,
                    },
                }
            ],
        }
        if self.usage is not None:
            raw["usage"] = self.usage
        return decode_response(raw, provider=self.provider)


def _model_dump(
    value: Any,
    *,
    provider: str,
) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="python")
        if isinstance(dumped, Mapping):
            return dumped
    raise LLMProviderError(
        "OpenAI SDK returned an unsupported response object.",
        provider=provider,
        retryable=False,
    )
