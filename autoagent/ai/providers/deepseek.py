from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from autoagent.ai.models.llm import (
    LLMMessage,
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMUsage,
)
from autoagent.ai.providers.base import LLMProviderError
from autoagent.ai.providers.chat_completions import (
    ChatCompletionsConfig,
    ChatCompletionsProvider,
    StructuredOutputMode,
)
from autoagent.ai.providers.chat_completions.provider import (
    _ChatCompletionAccumulator,
)


class DeepSeekConfig(ChatCompletionsConfig):
    """Connection settings for DeepSeek's Chat Completions API."""

    base_url: str = "https://api.deepseek.com"
    structured_output_mode: StructuredOutputMode = "json_object"


class DeepSeekProvider(ChatCompletionsProvider):
    """DeepSeek Provider implemented through the official OpenAI SDK."""

    provider_name = "deepseek"

    def __init__(
        self,
        config: DeepSeekConfig,
        *,
        client: object | None = None,
    ) -> None:
        super().__init__(config, client=client)

    def _request_params(self, request: LLMRequest) -> dict[str, Any]:
        params = super()._request_params(request)
        max_tokens = params.pop("max_completion_tokens", None)
        if max_tokens is not None:
            params["max_tokens"] = max_tokens
        messages = params["messages"]
        encoded_request_messages = (
            messages[-len(request.messages) :] if request.messages else ()
        )
        for message, encoded in zip(
            request.messages,
            encoded_request_messages,
            strict=True,
        ):
            if (
                message.reasoning_content is not None
                and message.tool_calls
            ):
                encoded["reasoning_content"] = message.reasoning_content
        extra_body = params.get("extra_body")
        if (
            isinstance(extra_body, dict)
            and "reasoning_effort" in extra_body
        ):
            params["reasoning_effort"] = extra_body.pop("reasoning_effort")
            if not extra_body:
                params.pop("extra_body")
        return params

    def _decode_response(self, raw: Mapping[str, Any]) -> LLMResponse:
        response = super()._decode_response(raw)
        return _with_deepseek_fields(response, raw)

    def _create_stream_accumulator(
        self,
        *,
        fallback_model: str,
    ) -> _DeepSeekAccumulator:
        return _DeepSeekAccumulator(
            fallback_model=fallback_model,
            provider=self.provider_name,
        )


class _DeepSeekAccumulator(_ChatCompletionAccumulator):
    def __init__(self, *, fallback_model: str, provider: str) -> None:
        super().__init__(
            fallback_model=fallback_model,
            provider=provider,
        )
        self.reasoning_content: list[str] = []

    def apply(
        self,
        raw: Mapping[str, Any],
    ) -> tuple[LLMStreamChunk, ...]:
        reasoning_chunks: tuple[LLMStreamChunk, ...] = ()
        choices = raw.get("choices")
        if isinstance(choices, list) and choices:
            choice = choices[0]
            if isinstance(choice, Mapping):
                delta = choice.get("delta")
                if isinstance(delta, Mapping):
                    reasoning = delta.get("reasoning_content")
                    if isinstance(reasoning, str) and reasoning:
                        self.reasoning_content.append(reasoning)
                        reasoning_chunks = (
                            LLMStreamChunk(
                                type="reasoning_delta",
                                reasoning_delta=reasoning,
                            ),
                        )
        return reasoning_chunks + super().apply(raw)

    def response(self) -> LLMResponse:
        response = super().response()
        reasoning_content = "".join(self.reasoning_content) or None
        message = response.message.model_copy(
            update={"reasoning_content": reasoning_content}
        )
        usage = _deepseek_usage(self.usage, fallback=response.usage)
        return response.model_copy(
            update={"message": message, "usage": usage}
        )


def _with_deepseek_fields(
    response: LLMResponse,
    raw: Mapping[str, Any],
) -> LLMResponse:
    raw_message = _first_message(raw)
    reasoning_content = raw_message.get("reasoning_content")
    if reasoning_content is not None and not isinstance(reasoning_content, str):
        raise LLMProviderError(
            "DeepSeek reasoning_content must be a string or null.",
            provider="deepseek",
            retryable=False,
        )
    message = response.message.model_copy(
        update={"reasoning_content": reasoning_content}
    )
    raw_usage = raw.get("usage")
    usage = _deepseek_usage(
        raw_usage if isinstance(raw_usage, Mapping) else None,
        fallback=response.usage,
    )
    return response.model_copy(update={"message": message, "usage": usage})


def _first_message(raw: Mapping[str, Any]) -> Mapping[str, Any]:
    choices = raw.get("choices")
    if not isinstance(choices, list) or not choices:
        return {}
    choice = choices[0]
    if not isinstance(choice, Mapping):
        return {}
    message = choice.get("message")
    return message if isinstance(message, Mapping) else {}


def _deepseek_usage(
    raw: Mapping[str, Any] | None,
    *,
    fallback: LLMUsage | None,
) -> LLMUsage | None:
    if raw is None:
        return fallback
    completion_details = raw.get("completion_tokens_details")
    return LLMUsage(
        input_tokens=_optional_int(raw.get("prompt_tokens")),
        output_tokens=_optional_int(raw.get("completion_tokens")),
        total_tokens=_optional_int(raw.get("total_tokens")),
        prompt_cache_hit_tokens=_optional_int(
            raw.get("prompt_cache_hit_tokens")
        ),
        prompt_cache_miss_tokens=_optional_int(
            raw.get("prompt_cache_miss_tokens")
        ),
        reasoning_tokens=(
            _optional_int(completion_details.get("reasoning_tokens"))
            if isinstance(completion_details, Mapping)
            else None
        ),
    )


def _optional_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None
