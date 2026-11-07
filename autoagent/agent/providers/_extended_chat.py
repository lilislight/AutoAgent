"""OpenAI-shaped SDK extensions shared by LiteLLM and vendor adapters."""

from __future__ import annotations

from copy import deepcopy
import json

from ..models import (
    DataSource, ImageContent, LLMMessage, LLMNativeData, LLMProviderItem,
    LLMReasoning, LLMStreamChunk, LLMToolCall, LLMToolResult,
    TextContent, URLSource,
)
from ._common import check_native, native_fields, replay_digest, unsupported
from .chat_completions import ChatCompletionsAdapter


_REPLAY_FIELDS = ("reasoning_content", "thinking_blocks", "reasoning_items", "provider_specific_fields")
_REASONING_FIELDS = ("reasoning_content", "thinking_blocks", "reasoning_items")


class ExtendedChatAdapter(ChatCompletionsAdapter):
    """Preserve SDK reasoning blocks and their opaque replay fields as one group."""

    tool_media = True

    def _messages(self, request, provider_id):
        messages = []
        last_group = None
        pending_reasoning = False
        for item in request.input:
            extra = check_native(item.native, provider_id, self.protocol)
            group = extra.get("_group_id")
            if isinstance(item, LLMReasoning):
                fields = deepcopy(extra.get("_chat_fields", {}))
                if extra.get("_text_digest") is not None and replay_digest(item.text) != extra["_text_digest"]:
                    unsupported("Original Chat reasoning must be replayed unmodified.", provider_id, self.protocol)
                for index, start, end in extra.get("_thinking_text", []):
                    fields["thinking_blocks"][index]["thinking"] = item.text[start:end]
                if item.text is not None and ("_text_digest" not in extra or extra.get("_reasoning_content")):
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
                for field, value in extra.get("_message_fields", {}).items():
                    if field in _REPLAY_FIELDS:
                        messages[-1][field] = value
                messages[-1].setdefault("tool_calls", []).append({k: v for k, v in extra.items() if not k.startswith("_")})
                last_group, pending_reasoning = group, False
        return messages

    def _decode_output(self, message, *, provider_id, request=None):
        message = deepcopy(message)
        fields = {key: message.pop(key) for key in _REASONING_FIELDS if message.get(key) is not None}
        images = message.pop("images", None) or []
        provider_fields = message.get("provider_specific_fields")
        if isinstance(provider_fields, dict):
            message["provider_specific_fields"] = {key: value for key, value in provider_fields.items() if value is not None} or None
        output = super()._decode_output(message, provider_id=provider_id, request=request)
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
                # Extensions from a tool-only message move to the media message,
                # which becomes their sole owner when reconstructing the turn.
                data = native_fields(message, {"role", "content", "refusal", "audio", "tool_calls", "annotations"}, provider_id, self.protocol)
                payload = data.payload if data else {}
                if output and output[0].native:
                    data = deepcopy(output[0].native.payload)
                    payload = data.pop("_message_fields", {})
                    output[0] = output[0].model_copy(update={"native": LLMNativeData(
                        provider_id=provider_id, protocol=self.protocol, payload=data) if data else None})
                output.insert(0, LLMMessage(role="assistant", content=tuple(parts), native=LLMNativeData(
                    provider_id=provider_id, protocol=self.protocol, payload=payload) if payload else None))
            else:
                output[index] = output[index].model_copy(update={"content": output[index].content + tuple(parts)})
        if any(fields.get(key) for key in _REASONING_FIELDS):
            if not output and message.get("provider_specific_fields") is not None:
                fields["provider_specific_fields"] = message["provider_specific_fields"]
            text = fields.get("reasoning_content")
            if text is None and fields.get("thinking_blocks"):
                text = "".join(b.get("thinking", "") for b in fields["thinking_blocks"])
            payload = {"_text_digest": replay_digest(text)}
            if "reasoning_content" in fields:
                fields.pop("reasoning_content")
                payload["_reasoning_content"] = True
            # Keep block signatures and boundaries, referencing the canonical
            # text instead of storing another copy inside each thinking block.
            references = []
            for index, block in enumerate(fields.get("thinking_blocks") or []):
                value = block.get("thinking")
                if isinstance(value, str) and text is not None:
                    start = text.find(value)
                    if start >= 0:
                        references.append([index, start, start + len(value)])
                        block.pop("thinking")
            if references:
                payload["_thinking_text"] = references
            if fields:
                payload["_chat_fields"] = fields
            output.insert(0, LLMReasoning(text=text, native=LLMNativeData(provider_id=provider_id,
                protocol=self.protocol, payload=payload)))
        return output

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
