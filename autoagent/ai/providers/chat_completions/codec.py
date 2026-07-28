from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal
from urllib.parse import urlparse

from autoagent.ai.models.llm import (
    LLMMessage,
    LLMNamedToolChoice,
    LLMRequest,
    LLMResponse,
    LLMToolCall,
    LLMUsage,
)
from autoagent.ai.providers.base import LLMProviderError
from autoagent.ai.providers.chat_completions.config import ChatCompletionsConfig


def encode_request(
    request: LLMRequest,
    *,
    default_model: str,
    structured_output_mode: Literal["json_schema", "json_object", "prompt"] = (
        "json_schema"
    ),
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": request.model or default_model,
        "messages": [encode_message(message) for message in request.messages],
    }
    if request.tools:
        payload["tools"] = [
            {
                "type": "function",
                "function": {
                    "name": item.name,
                    "description": item.description,
                    "parameters": item.input_schema,
                },
            }
            for item in request.tools
        ]
    if request.tool_choice is not None:
        if isinstance(request.tool_choice, LLMNamedToolChoice):
            payload["tool_choice"] = {
                "type": "function",
                "function": {"name": request.tool_choice.name},
            }
        else:
            payload["tool_choice"] = request.tool_choice
    response_format = request.response_format_spec
    if response_format is not None:
        if structured_output_mode == "json_schema":
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": response_format.name,
                    "schema": response_format.json_schema,
                    "strict": response_format.strict,
                },
            }
        else:
            may_call_tools = _may_call_tools(request)
            payload["messages"].insert(
                0,
                _json_output_instruction(
                    response_format.json_schema,
                    may_call_tools=may_call_tools,
                ),
            )
            if structured_output_mode == "json_object" and not may_call_tools:
                payload["response_format"] = {"type": "json_object"}
    if request.temperature is not None:
        payload["temperature"] = request.temperature
    if request.max_output_tokens is not None:
        payload["max_completion_tokens"] = request.max_output_tokens
    return payload


def resolve_structured_output_mode(
    config: ChatCompletionsConfig,
) -> Literal["json_schema", "json_object", "prompt"]:
    if config.structured_output_mode != "auto":
        return config.structured_output_mode
    hostname = (urlparse(config.base_url).hostname or "").lower()
    if hostname == "api.deepseek.com" or hostname.endswith(".deepseek.com"):
        return "json_object"
    return "json_schema"


def encode_message(message: LLMMessage) -> dict[str, Any]:
    payload: dict[str, Any] = {"role": message.role, "content": message.content}
    if message.name is not None:
        payload["name"] = message.name
    if message.tool_call_id is not None:
        payload["tool_call_id"] = message.tool_call_id
    if message.tool_calls:
        payload["tool_calls"] = [
            {
                "id": call.id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": call.raw_arguments,
                },
            }
            for call in message.tool_calls
        ]
    return payload


def decode_response(
    raw: Mapping[str, Any],
    *,
    provider: str = "chat_completions",
) -> LLMResponse:
    choices = raw.get("choices")
    if not isinstance(choices, list) or not choices:
        raise _response_error(
            "Chat Completions response has no choices.",
            provider=provider,
        )
    choice = choices[0]
    if not isinstance(choice, Mapping):
        raise _response_error(
            "Chat Completions choice must be an object.",
            provider=provider,
        )
    raw_message = choice.get("message")
    if not isinstance(raw_message, Mapping):
        raise _response_error(
            "Chat Completions choice has no message.",
            provider=provider,
        )
    content = raw_message.get("content")
    if content is not None and not isinstance(content, str):
        raise _response_error(
            "V1 supports only text assistant content.",
            provider=provider,
        )
    calls: list[LLMToolCall] = []
    for raw_call in raw_message.get("tool_calls") or ():
        if not isinstance(raw_call, Mapping):
            raise _response_error(
                "Tool call must be an object.",
                provider=provider,
            )
        function = raw_call.get("function")
        if not isinstance(function, Mapping):
            raise _response_error(
                "V1 supports only function tool calls.",
                provider=provider,
            )
        calls.append(
            LLMToolCall(
                id=str(raw_call.get("id") or ""),
                name=str(function.get("name") or ""),
                raw_arguments=str(function.get("arguments") or ""),
            )
        )
    raw_usage = raw.get("usage")
    usage = None
    if isinstance(raw_usage, Mapping):
        usage = LLMUsage(
            input_tokens=_optional_int(raw_usage.get("prompt_tokens")),
            output_tokens=_optional_int(raw_usage.get("completion_tokens")),
            total_tokens=_optional_int(raw_usage.get("total_tokens")),
        )
    return LLMResponse(
        message=LLMMessage(
            role="assistant",
            content=content,
            tool_calls=tuple(calls),
        ),
        finish_reason=(
            str(choice["finish_reason"])
            if choice.get("finish_reason") is not None
            else None
        ),
        model=str(raw.get("model") or ""),
        usage=usage,
        provider_request_id=str(raw.get("id") or "") or None,
    )


def _may_call_tools(request: LLMRequest) -> bool:
    return bool(request.tools) and request.tool_choice != "none"


def _json_output_instruction(
    json_schema: Mapping[str, Any],
    *,
    may_call_tools: bool,
) -> dict[str, Any]:
    schema = json.dumps(json_schema, ensure_ascii=False, separators=(",", ":"))
    response_rule = (
        "You have exactly two permitted response forms: (1) call one or more "
        "provided tools with tool_calls, or (2) return the final answer as "
        "assistant content containing exactly one valid JSON value matching "
        "the JSON Schema below. When returning a final answer, do not include "
        "natural-language prose, Markdown fences, labels, or explanatory text "
        "outside the JSON value."
        if may_call_tools
        else
        "Your entire assistant response must contain exactly one valid JSON "
        "value matching the JSON Schema below. Do not include natural-language "
        "prose, Markdown fences, labels, or explanatory text outside the JSON "
        "value."
    )
    return {
        "role": "system",
        "content": (
            f"{response_rule} This output constraint is mandatory even if "
            "another message asks for a prose answer.\n"
            f"JSON Schema: {schema}"
        ),
    }


def _optional_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _response_error(
    message: str,
    *,
    provider: str,
) -> LLMProviderError:
    return LLMProviderError(
        message,
        provider=provider,
        retryable=False,
    )
