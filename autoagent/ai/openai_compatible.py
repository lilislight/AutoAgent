from __future__ import annotations

import json
import os
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from dotenv import dotenv_values

from autoagent.ai.llm import (
    LLM_CALL_CONTRACT,
    LLM_CALL_CAPABILITY_ID,
    LLMMessage,
    LLMNamedToolChoice,
    LLMRequest,
    LLMResponse,
    LLMToolCall,
    LLMUsage,
)
from autoagent.core.operators import Operator

if TYPE_CHECKING:
    from autoagent.core.app import AutoAgentApp


OPENAI_COMPATIBLE_ENV_KEYS = frozenset(
    {
        "AUTOAGENT_OPENAI_BASE_URL",
        "AUTOAGENT_OPENAI_API_KEY",
        "AUTOAGENT_OPENAI_MODEL",
        "AUTOAGENT_OPENAI_TIMEOUT_MS",
        "AUTOAGENT_OPENAI_STRUCTURED_OUTPUT_MODE",
    }
)

StructuredOutputMode = Literal["auto", "json_schema", "json_object", "prompt"]


class OpenAICompatibleConfig(BaseModel):
    """Connection settings for one OpenAI-compatible Chat Completions service."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    base_url: str = "https://api.openai.com/v1"
    api_key: SecretStr
    default_model: str
    timeout_ms: int = Field(default=60_000, gt=0)
    structured_output_mode: StructuredOutputMode = "auto"
    headers: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        *,
        env_file: str | Path | None = ".env",
        environ: Mapping[str, str] | None = None,
    ) -> OpenAICompatibleConfig:
        values: dict[str, str] = {}
        if env_file is not None:
            values.update(
                {
                    key: value
                    for key, value in dotenv_values(env_file).items()
                    if value is not None
                }
            )
        values.update(os.environ if environ is None else environ)
        api_key = values.get("AUTOAGENT_OPENAI_API_KEY", "").strip()
        model = values.get("AUTOAGENT_OPENAI_MODEL", "").strip()
        if not api_key:
            raise ValueError("AUTOAGENT_OPENAI_API_KEY is required.")
        if not model:
            raise ValueError("AUTOAGENT_OPENAI_MODEL is required.")
        return cls(
            base_url=values.get(
                "AUTOAGENT_OPENAI_BASE_URL",
                "https://api.openai.com/v1",
            ),
            api_key=api_key,
            default_model=model,
            timeout_ms=int(
                values.get("AUTOAGENT_OPENAI_TIMEOUT_MS", "60000")
            ),
            structured_output_mode=values.get(
                "AUTOAGENT_OPENAI_STRUCTURED_OUTPUT_MODE",
                "auto",
            ).strip(),
        )


class OpenAICompatibleError(RuntimeError):
    """Transport, HTTP, or response-shape failure from a compatible endpoint."""


JSONTransport = Callable[
    [str, Mapping[str, str], Mapping[str, Any], float],
    Awaitable[Mapping[str, Any]],
]


def create_openai_compatible_operator(
    config: OpenAICompatibleConfig,
    *,
    operator_id: str = "openai_compatible.chat_completions",
    transport: JSONTransport | None = None,
) -> Operator:
    """Create an Operator implementing ``llm_call``."""

    selected_transport = transport or _post_json

    async def handler(request: LLMRequest) -> LLMResponse:
        payload = _request_payload(
            request,
            default_model=config.default_model,
            structured_output_mode=_resolve_structured_output_mode(config),
        )
        headers = {
            "authorization": f"Bearer {config.api_key.get_secret_value()}",
            "content-type": "application/json",
            **config.headers,
        }
        raw = await selected_transport(
            f"{config.base_url.rstrip('/')}/chat/completions",
            headers,
            payload,
            config.timeout_ms / 1000,
        )
        return _parse_response(raw)

    return Operator(
        id=operator_id,
        handler=handler,
        capability_id=LLM_CALL_CAPABILITY_ID,
    )


def register_openai_compatible_operator(
    app: AutoAgentApp,
    config: OpenAICompatibleConfig,
    *,
    operator_id: str = "openai_compatible.chat_completions",
    default: bool = True,
    transport: JSONTransport | None = None,
) -> Operator:
    """Register ``llm_call`` and one compatible implementation on an App."""

    if not app.capability_registry.contains(LLM_CALL_CAPABILITY_ID):
        app.register_capability(
            LLM_CALL_CAPABILITY_ID,
            contract=LLM_CALL_CONTRACT,
            description="Perform one provider-neutral language model call.",
        )
    operator = create_openai_compatible_operator(
        config,
        operator_id=operator_id,
        transport=transport,
    )
    return app.operator_registry.register(operator, default=default)


async def _post_json(
    url: str,
    headers: Mapping[str, str],
    payload: Mapping[str, Any],
    timeout_seconds: float,
) -> Mapping[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(url, headers=dict(headers), json=dict(payload))
    except httpx.HTTPError as exc:
        raise OpenAICompatibleError(str(exc)) from exc
    if response.is_error:
        detail = response.text.strip()
        if len(detail) > 4_096:
            detail = f"{detail[:4_096]}..."
        request_id = response.headers.get("x-request-id")
        message = f"Provider returned HTTP {response.status_code}"
        if request_id:
            message += f" (request_id={request_id})"
        if detail:
            message += f": {detail}"
        raise OpenAICompatibleError(message)
    try:
        body = response.json()
    except ValueError as exc:
        raise OpenAICompatibleError(
            "Chat Completions response is not valid JSON."
        ) from exc
    if not isinstance(body, Mapping):
        raise OpenAICompatibleError("Chat Completions response must be a JSON object.")
    return body


def _request_payload(
    request: LLMRequest,
    *,
    default_model: str,
    structured_output_mode: Literal["json_schema", "json_object", "prompt"] = (
        "json_schema"
    ),
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": request.model or default_model,
        "messages": [_message_payload(message) for message in request.messages],
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
            # Some compatible providers reject response_format=json_object
            # when Tool calling is enabled. Keep the provider option deferred,
            # but never defer the schema instruction: otherwise a ReAct model
            # commonly returns prose once before the repair turn constrains it.
            if structured_output_mode == "json_object" and not may_call_tools:
                payload["response_format"] = {"type": "json_object"}
    if request.temperature is not None:
        payload["temperature"] = request.temperature
    if request.max_output_tokens is not None:
        payload["max_completion_tokens"] = request.max_output_tokens
    payload.update(request.provider_options)
    return payload


def _may_call_tools(request: LLMRequest) -> bool:
    return bool(request.tools) and request.tool_choice != "none"


def _resolve_structured_output_mode(
    config: OpenAICompatibleConfig,
) -> Literal["json_schema", "json_object", "prompt"]:
    if config.structured_output_mode != "auto":
        return config.structured_output_mode
    hostname = (urlparse(config.base_url).hostname or "").lower()
    if hostname == "api.deepseek.com" or hostname.endswith(".deepseek.com"):
        return "json_object"
    return "json_schema"


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


def _message_payload(message: LLMMessage) -> dict[str, Any]:
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


def _parse_response(raw: Mapping[str, Any]) -> LLMResponse:
    choices = raw.get("choices")
    if not isinstance(choices, list) or not choices:
        raise OpenAICompatibleError("Chat Completions response has no choices.")
    choice = choices[0]
    if not isinstance(choice, Mapping):
        raise OpenAICompatibleError("Chat Completions choice must be an object.")
    raw_message = choice.get("message")
    if not isinstance(raw_message, Mapping):
        raise OpenAICompatibleError("Chat Completions choice has no message.")
    content = raw_message.get("content")
    if content is not None and not isinstance(content, str):
        raise OpenAICompatibleError("V1 supports only text assistant content.")
    calls: list[LLMToolCall] = []
    for raw_call in raw_message.get("tool_calls") or ():
        if not isinstance(raw_call, Mapping):
            raise OpenAICompatibleError("Tool call must be an object.")
        function = raw_call.get("function")
        if not isinstance(function, Mapping):
            raise OpenAICompatibleError("V1 supports only function tool calls.")
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


def _optional_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None
