"""DeepSeek's OpenAI-compatible APIs with explicit capability and replay rules."""

from __future__ import annotations

from typing import Literal
import os

from pydantic import SecretStr

from ..models import (
    ImageContent, LLMJsonSchemaFormat, LLMMessage, LLMNamedToolChoice,
    LLMReasoning, LLMToolResult, TextContent,
)
from ..models._base import NonEmpty
from ._common import check_native, source_value, unsupported
from ._extended_chat import ExtendedChatAdapter
from .config import OpenAIProviderConfig
from .openai import OpenAIProvider
from .responses import ResponsesAdapter


class DeepSeekProviderConfig(OpenAIProviderConfig):
    provider_id: NonEmpty = "deepseek"
    base_url: str = "https://api.deepseek.com"


def _check(request, provider_id, protocol, *, beta=False):
    options = request.provider_options.values if request.provider_options is not None else {}
    chat = protocol == "openai.chat_completions"
    allowed = ({"thinking", "reasoning_effort", "stop", "logprobs", "top_logprobs", "user_id"}
               if chat else {"reasoning", "top_logprobs", "user"})
    if set(options) - allowed:
        unsupported("DeepSeek does not support these options: " + ", ".join(sorted(set(options) - allowed)), provider_id, protocol)
    if chat:
        thinking = options.get("thinking", {"type": "enabled"})
        if not isinstance(thinking, dict) or set(thinking) != {"type"} or thinking["type"] not in ("enabled", "disabled"):
            unsupported("DeepSeek thinking must specify type enabled or disabled.", provider_id, protocol)
        enabled = thinking["type"] == "enabled"
        effort = options.get("reasoning_effort", "high")
        valid_efforts = {"minimal", "low", "medium", "high", "xhigh", "max", "ultra"}
    else:
        reasoning = options.get("reasoning", {})
        if not isinstance(reasoning, dict) or set(reasoning) - {"effort"}:
            unsupported("DeepSeek Responses reasoning supports effort only.", provider_id, protocol)
        effort = reasoning.get("effort", "high")
        enabled = effort != "none"
        valid_efforts = {"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}
    if not isinstance(effort, str) or effort not in valid_efforts:
        unsupported("Unsupported DeepSeek reasoning effort.", provider_id, protocol)
    if enabled and request.temperature is not None:
        unsupported("DeepSeek ignores temperature in thinking mode; disable thinking to set it.", provider_id, protocol)
    if request.top_p is not None:
        if (request.model == "deepseek-flash" and (not enabled or request.top_p < .95)) or (request.model == "deepseek-v4-pro" and enabled):
            unsupported("DeepSeek would ignore or change this top_p in the selected mode.", provider_id, protocol)
    if chat and enabled and (request.tool_choice == "required" or isinstance(request.tool_choice, LLMNamedToolChoice)):
        unsupported("DeepSeek Chat cannot force Tool choice in thinking mode.", provider_id, protocol)
    if request.output_options is not None:
        if request.output_options.modalities != ("text",):
            unsupported("DeepSeek supports text output only.", provider_id, protocol)
        if chat and isinstance(request.output_options.text_format, LLMJsonSchemaFormat):
            unsupported("DeepSeek JSON Schema output requires Responses.", provider_id, protocol)
    if chat and any(tool.strict is True for tool in request.tools) and not beta:
        unsupported("DeepSeek strict Chat Tools require a /beta connection.", provider_id, protocol)
    if chat and any(tool.strict is True for tool in request.tools):
        if not all(tool.strict is True for tool in request.tools):
            unsupported("DeepSeek strict mode requires every Tool to be strict.", provider_id, protocol)
        def check_schema(value):
            if isinstance(value, dict):
                if set(value) & {"minLength", "maxLength", "minItems", "maxItems"}:
                    unsupported("DeepSeek strict Tools do not support string/array length constraints.", provider_id, protocol)
                for key in ("properties", "$defs", "definitions", "patternProperties"):
                    for child in value.get(key, {}).values():
                        check_schema(child)
                for key in ("items", "additionalProperties", "contains", "not", "if", "then", "else", "allOf", "anyOf", "oneOf", "prefixItems"):
                    check_schema(value.get(key))
            elif isinstance(value, list):
                for child in value:
                    check_schema(child)
        for tool in request.tools:
            check_schema(tool.parameters)
    for item in request.input:
        if isinstance(item, LLMReasoning) and (item.summary or item.encrypted_content is not None):
            unsupported("DeepSeek cannot replay encrypted reasoning or summaries.", provider_id, protocol)
        if not isinstance(item, (LLMMessage, LLMToolResult)):
            continue
        for part in item.content:
            if isinstance(part, TextContent):
                continue
            if not isinstance(part, ImageContent):
                unsupported("DeepSeek input supports text and images, not audio/video/documents.", provider_id, protocol)
            if request.model != "deepseek-flash":
                unsupported("Image input requires the deepseek-flash model.", provider_id, protocol)
            if isinstance(item, LLMMessage) and item.role not in {"user", "developer"}:
                unsupported("DeepSeek accepts images in user/developer messages only.", provider_id, protocol)
            if chat and isinstance(item, LLMToolResult):
                unsupported("DeepSeek multimodal Tool results require Responses.", provider_id, protocol)


class DeepSeekChatCompletionsAdapter(ExtendedChatAdapter):
    def __init__(self, *, beta: bool = False):
        super().__init__(token_parameter="max_tokens")
        self.beta = beta

    def _part(self, part, provider_id):
        if isinstance(part, ImageContent):
            check_native(part.native, provider_id, self.protocol)
            kind, value = source_value(part.source, provider_id, self.protocol)
            if kind == "id":
                return {"type": "file", "file_id": value}
            return {"type": "image_url", "image_url": {"url": value, "detail": part.detail or "auto"}}
        return super()._part(part, provider_id)

    def _message(self, item, provider_id):
        result, group = super()._message(item, provider_id)
        if item.role == "developer":
            result["role"] = "system"
        if all(isinstance(part, TextContent) for part in item.content):
            result["content"] = "".join(part.text for part in item.content)
        extra = check_native(item.native, provider_id, self.protocol)
        if "prefix" in extra:
            if not self.beta or item.role != "assistant" or type(extra["prefix"]) is not bool:
                unsupported("Chat prefix requires a beta assistant message and boolean prefix.", provider_id, self.protocol)
            result["prefix"] = extra["prefix"]
        return result, group

    def encode_request(self, request, *, provider_id):
        _check(request, provider_id, self.protocol, beta=self.beta)
        params = super().encode_request(request, provider_id=provider_id)
        params.pop("modalities", None)
        for index, message in enumerate(params["messages"]):
            if message.get("prefix") and index != len(params["messages"]) - 1:
                unsupported("Chat prefix must be the last message.", provider_id, self.protocol)
            if message["role"] == "assistant" and message.get("content") is None:
                message["content"] = ""
            thinking = (request.provider_options.values if request.provider_options else {}).get("thinking", {"type": "enabled"})
            if request.tools and thinking["type"] == "enabled" and message["role"] == "assistant" and "reasoning_content" not in message:
                unsupported("DeepSeek Tool history must retain the original reasoning_content; it cannot be fabricated.", provider_id, self.protocol)
        return params


class DeepSeekResponsesAdapter(ResponsesAdapter):
    def encode_request(self, request, *, provider_id):
        _check(request, provider_id, self.protocol)
        params = super().encode_request(request, provider_id=provider_id)
        for item in params["input"]:
            if item.get("role") == "developer":
                item["role"] = "system"  # Preserve instruction priority; upstream treats developer as user.
        return params


class DeepSeekProvider(OpenAIProvider):
    """Direct SDK transport; use its async context manager in runtime_lifespan."""

    def __init__(self, config: DeepSeekProviderConfig | None = None, *,
                 protocol: Literal["openai.chat_completions", "openai.responses"] = "openai.chat_completions", **kwargs):
        config = config or DeepSeekProviderConfig()
        if protocol == "openai.chat_completions":
            adapter = DeepSeekChatCompletionsAdapter(beta=config.base_url.rstrip("/").endswith("/beta"))
        elif protocol == "openai.responses":
            adapter = DeepSeekResponsesAdapter()
        else:
            raise ValueError("DeepSeek requires Chat Completions or Responses.")
        super().__init__(config, adapter=adapter, **kwargs)

    def _get_client(self):
        self._bind_loop()
        if self._client is None and self.config.api_key is None:
            key = os.environ.get("DEEPSEEK_API_KEY")
            if not key:
                raise ValueError("Configure a DeepSeek API key or set DEEPSEEK_API_KEY.")
            self.config = self.config.model_copy(update={"api_key": SecretStr(key)})
        return super()._get_client()


__all__ = ["DeepSeekProviderConfig", "DeepSeekProvider", "DeepSeekChatCompletionsAdapter", "DeepSeekResponsesAdapter"]
