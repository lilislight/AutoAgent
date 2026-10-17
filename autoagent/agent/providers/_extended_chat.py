"""OpenAI-shaped SDK extensions shared by LiteLLM and vendor adapters."""

from __future__ import annotations

from copy import deepcopy
import json

from ..models import (
    DataSource, ImageContent, LLMMessage, LLMNativeData, LLMProviderItem,
    LLMReasoning, LLMResponse, LLMStreamChunk, LLMToolCall, LLMToolResult,
    TextContent, URLSource,
)
from ._common import check_native, unsupported
from .chat_completions import ChatCompletionsAdapter


_REPLAY_FIELDS = ("reasoning_content", "thinking_blocks", "reasoning_items", "provider_specific_fields")


class ExtendedChatAdapter(ChatCompletionsAdapter):
    """Preserve SDK reasoning blocks and their opaque replay fields as one group."""

    def _messages(self, request, provider_id):
        messages = []
        last_group = None
        pending_reasoning = False
        for item in request.input:
            extra = check_native(item.native, provider_id, self.protocol)
            group = extra.get("_group_id")
            if isinstance(item, LLMReasoning):
                fields = deepcopy(extra.get("_chat_fields", {}))
                original = fields.get("reasoning_content")
                if original is None and fields.get("thinking_blocks"):
                    original = "".join(b.get("thinking", "") for b in fields["thinking_blocks"])
                if original is not None and item.text != original:
                    unsupported("Original Chat reasoning must be replayed unmodified.", provider_id, self.protocol)
                if item.text is not None:
                    fields["reasoning_content"] = item.text
                if item.encrypted_content is not None or item.summary:
                    unsupported("Chat reasoning replay needs its original SDK blocks, not Responses summary/encrypted data.", provider_id, self.protocol)
                if not messages or messages[-1]["role"] != "assistant" or (group is not None and group != last_group):
                    messages.append({"role": "assistant", "content": None})
                messages[-1].update(fields)
                last_group, pending_reasoning = group, True
            elif isinstance(item, LLMMessage):
                message, group = self._message(item, provider_id)
                for field in _REPLAY_FIELDS:
                    if extra.get(field) is not None:
                        message[field] = extra[field]
                if pending_reasoning and message["role"] == "assistant" and (group is None or last_group is None or group == last_group):
                    messages[-1].update(message)
                else:
                    messages.append(message)
                last_group, pending_reasoning = group, False
            elif isinstance(item, LLMToolCall):
                if not messages or messages[-1]["role"] != "assistant" or (group is not None and group != last_group):
                    messages.append({"role": "assistant", "content": None})
                for field, value in extra.get("_message_fields", {}).items():
                    if field in _REPLAY_FIELDS:
                        messages[-1][field] = value
                messages[-1].setdefault("tool_calls", []).append({"id": item.call_id, "type": "function",
                    "function": {"name": item.name, "arguments": item.arguments},
                    **{k: v for k, v in extra.items() if not k.startswith("_") and k not in {"id", "type", "function"}}})
                last_group, pending_reasoning = group, False
            elif isinstance(item, LLMToolResult):
                if all(isinstance(part, TextContent) for part in item.content):
                    content = "".join(part.text for part in item.content)
                    for part in item.content:
                        check_native(part.native, provider_id, self.protocol)
                    if item.structured_content_present:
                        content += json.dumps(item.structured_content, ensure_ascii=False)
                    if item.is_error:
                        content = "Tool execution failed. " + content
                else:
                    content = [self._part(part, provider_id) for part in item.content]
                    if item.structured_content_present:
                        content.append({"type": "text", "text": json.dumps(item.structured_content, ensure_ascii=False)})
                    if item.is_error:
                        content.insert(0, {"type": "text", "text": "Tool execution failed."})
                messages.append({"role": "tool", "tool_call_id": item.call_id, "content": content})
                last_group, pending_reasoning = None, False
            elif isinstance(item, LLMProviderItem):
                if item.provider_type != "custom" or extra.get("type") != "custom":
                    unsupported("Chat cannot replay this provider item.", provider_id, self.protocol)
                if not messages or messages[-1]["role"] != "assistant" or (group is not None and group != last_group):
                    messages.append({"role": "assistant", "content": None})
                messages[-1].setdefault("tool_calls", []).append({k: v for k, v in extra.items() if not k.startswith("_")})
                last_group, pending_reasoning = group, False
        return messages

    def decode_response(self, raw, *, provider_id, request=None):
        raw = deepcopy(raw)
        if raw.get("error") or not raw.get("choices"):
            return super().decode_response(raw, provider_id=provider_id, request=request)
        message = raw["choices"][0]["message"]
        fields = {key: message[key] for key in _REPLAY_FIELDS if message.get(key) is not None}
        images = message.get("images") or []
        synthetic = message.get("content") is None and not message.get("refusal") and not message.get("audio")
        if synthetic and (fields or images):
            message["content"] = ""
        response = super().decode_response(raw, provider_id=provider_id, request=request)
        output = list(response.output)
        group = raw.get("id") or next((i.native.payload.get("_group_id") for i in output if i.native), None)
        if synthetic and (fields or images):
            output = [i for i in output if not isinstance(i, LLMMessage)]
        if images:
            parts = []
            for image in images:
                url = image["image_url"]["url"]
                if url.startswith("data:"):
                    header, data = url.split(",", 1)
                    if not header.endswith(";base64"):
                        raise ValueError("Image output must use base64 data URLs.")
                    source = DataSource(mime_type=header[5:-7], data=data)
                else:
                    source = URLSource(url=url)
                parts.append(ImageContent(source=source))
            index = next((n for n, i in enumerate(output) if isinstance(i, LLMMessage)), None)
            if index is None:
                output.insert(0, LLMMessage(role="assistant", content=tuple(parts), native=LLMNativeData(
                    provider_id=provider_id, protocol=self.protocol, payload={"_group_id": group, "images": images})))
            else:
                output[index] = output[index].model_copy(update={"content": output[index].content + tuple(parts)})
        if fields:
            text = fields.get("reasoning_content")
            if text is None and fields.get("thinking_blocks"):
                text = "".join(b.get("thinking", "") for b in fields["thinking_blocks"])
            output.insert(0, LLMReasoning(text=text, native=LLMNativeData(provider_id=provider_id,
                protocol=self.protocol, payload={"_group_id": group, "_chat_fields": fields})))
        return LLMResponse.model_validate({**response.model_dump(mode="python"), "output": tuple(output)})

    def _has_extra_stream_output(self, delta):
        return any(delta.get(field) for field in (*_REPLAY_FIELDS, "images"))

    def _extra_tool_delta(self, target, call):
        for key, value in call.items():
            if key not in {"index", "id", "type", "function"} and value is not None:
                if isinstance(value, dict):
                    target.setdefault(key, {}).update(deepcopy(value))
                else:
                    target[key] = deepcopy(value)

    def _extra_stream_delta(self, message, delta, provider_id):
        observations = []
        if delta.get("reasoning_content") is not None:
            value = delta["reasoning_content"]
            message["reasoning_content"] = message.get("reasoning_content", "") + value
            observations.append(LLMStreamChunk(type="delta", item_key="reasoning", kind="reasoning", delta=value))
        for block in delta.get("thinking_blocks") or ():
            blocks = message.setdefault("thinking_blocks", [])
            if block.get("type") == "redacted_thinking":
                blocks.append(deepcopy(block))
                continue
            if not blocks or blocks[-1].get("type") != "thinking" or (blocks[-1].get("signature") and block.get("thinking")):
                blocks.append({"type": "thinking", "thinking": "", "signature": ""})
            for key in ("thinking", "signature"):
                if block.get(key):
                    blocks[-1][key] += block[key]
            if block.get("thinking") and delta.get("reasoning_content") is None:
                observations.append(LLMStreamChunk(type="delta", item_key="reasoning", kind="reasoning", delta=block["thinking"]))
        for field in ("reasoning_items", "images"):
            if delta.get(field):
                message.setdefault(field, []).extend(deepcopy(delta[field]))
        if delta.get("provider_specific_fields"):
            message.setdefault("provider_specific_fields", {}).update(deepcopy(delta["provider_specific_fields"]))
        return observations
