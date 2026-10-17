"""OpenAI Chat Completions codec, including terminal streaming reduction."""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator
import json
import time
from uuid import uuid4

from ..models import (
    AudioContent, DataSource, FileContent, ImageContent, LLMMessage,
    LLMNamedToolChoice, LLMNativeData, LLMProviderItem, LLMReasoning, LLMRequest,
    LLMResponse, LLMToolCall, LLMToolResult, RefusalContent, TextContent,
)
from ..models.streaming import LLMStreamChunk
from ._common import (
    check_native, common_parameters, function_schema, native_fields, raw_object, remote_failure,
    source_value, text_format, unsupported, usage,
)
from .base import LLMProviderError
from .protocol import OpenAIProtocolAdapter


_AUDIO_MIME = {
    "wav": "audio/wav", "aac": "audio/aac", "mp3": "audio/mpeg",
    "flac": "audio/flac", "opus": "audio/opus", "pcm16": "audio/pcm",
}


class ChatCompletionsAdapter(OpenAIProtocolAdapter):
    protocol = "openai.chat_completions"

    def __init__(self, *, token_parameter: str = "max_completion_tokens", include_stream_usage: bool = True) -> None:
        if token_parameter not in {"max_completion_tokens", "max_tokens"}:
            raise ValueError("Invalid Chat Completions token parameter.")
        self.token_parameter = token_parameter
        self.include_stream_usage = include_stream_usage

    def stream_parameters(self, params: dict) -> dict:
        result = super().stream_parameters(params)
        if self.include_stream_usage:
            result["stream_options"] = {"include_usage": True}
        return result

    def _part(self, part, provider_id: str) -> dict:
        check_native(part.native, provider_id, self.protocol)
        if isinstance(part, TextContent):
            return {"type": "text", "text": part.text}
        if isinstance(part, ImageContent):
            kind, value = source_value(part.source, provider_id, self.protocol)
            if kind == "id" or part.detail == "original":
                unsupported("Chat image input does not support file IDs or original detail.", provider_id, self.protocol)
            return {"type": "image_url", "image_url": {"url": value, "detail": part.detail or "auto"}}
        if isinstance(part, FileContent):
            kind, value = source_value(part.source, provider_id, self.protocol)
            mime = part.source.mime_type
            if kind == "url" or part.detail is not None or (mime is not None and mime.split(";", 1)[0] != "application/pdf"):
                unsupported("Chat file input requires a PDF file ID or inline data, without detail.", provider_id, self.protocol)
            if mime is None and part.filename is not None and not part.filename.lower().endswith(".pdf"):
                unsupported("Chat Completions file inputs support PDF only.", provider_id, self.protocol)
            file = {"file_id" if kind == "id" else "file_data": value}
            if part.filename is not None:
                file["filename"] = part.filename
            return {"type": "file", "file": file}
        if isinstance(part, AudioContent) and isinstance(part.source, DataSource):
            format = {"audio/wav": "wav", "audio/x-wav": "wav", "audio/mpeg": "mp3"}.get(part.source.mime_type.split(";", 1)[0])
            if format is not None:
                return {"type": "input_audio", "input_audio": {"data": part.source.data, "format": format}}
        unsupported(f"Chat Completions cannot encode {part.type} in this position.", provider_id, self.protocol)

    def _message(self, item: LLMMessage, provider_id: str) -> tuple[dict, str | None]:
        extra = check_native(item.native, provider_id, self.protocol)
        result = {"role": item.role}
        if extra.get("name") is not None:
            result["name"] = extra["name"]
        if item.role != "assistant":
            result["content"] = [self._part(part, provider_id) for part in item.content]
        else:
            parts = []
            for part in item.content:
                data = check_native(part.native, provider_id, self.protocol)
                if isinstance(part, TextContent):
                    parts.append({"type": "text", "text": part.text})
                elif isinstance(part, RefusalContent):
                    parts.append({"type": "refusal", "refusal": part.text})
                elif isinstance(part, AudioContent) and data.get("id"):
                    if data.get("expires_at") is not None and data["expires_at"] <= time.time():
                        unsupported("Audio continuation reference has expired.", provider_id, self.protocol)
                    result["audio"] = {"id": data["id"]}
                else:
                    unsupported("Assistant history supports text, refusal and audio continuation IDs.", provider_id, self.protocol)
            result["content"] = parts or None
        return result, extra.get("_group_id")

    def _messages(self, request: LLMRequest, provider_id: str) -> list[dict]:
        messages = []
        last_group = None
        for item in request.input:
            extra = check_native(item.native, provider_id, self.protocol)
            if isinstance(item, LLMMessage):
                message, last_group = self._message(item, provider_id)
                messages.append(message)
            elif isinstance(item, (LLMToolCall, LLMProviderItem)):
                group = extra.pop("_group_id", None)
                if not messages or messages[-1]["role"] != "assistant" or (group is not None and group != last_group):
                    messages.append({"role": "assistant", "content": None})
                last_group = group
                if isinstance(item, LLMToolCall):
                    call = {"id": item.call_id, "type": "function", "function": {"name": item.name, "arguments": item.arguments}}
                else:
                    if item.provider_type != "custom" or extra.get("type") != "custom":
                        unsupported("Chat adapter cannot replay this provider item.", provider_id, self.protocol)
                    call = extra
                messages[-1].setdefault("tool_calls", []).append(call)
            elif isinstance(item, LLMToolResult):
                if any(not isinstance(part, TextContent) for part in item.content):
                    unsupported("Chat Tool results must be text; multimodal Tool results require Responses.", provider_id, self.protocol)
                for part in item.content:
                    check_native(part.native, provider_id, self.protocol)
                text = "".join(part.text for part in item.content)
                if item.structured_content_present:
                    text += json.dumps(item.structured_content, ensure_ascii=False)
                if item.is_error:
                    text = "Tool execution failed. " + text
                messages.append({"role": "tool", "tool_call_id": item.call_id, "content": text})
                last_group = None
            elif isinstance(item, LLMReasoning):
                unsupported("Standard Chat Completions has no reasoning-item replay field.", provider_id, self.protocol)
        return messages

    def encode_request(self, request: LLMRequest, *, provider_id: str) -> dict:
        params = common_parameters(request, self.protocol, provider_id)
        params["messages"] = self._messages(request, provider_id)
        if request.max_output_tokens is not None:
            params[self.token_parameter] = request.max_output_tokens
        if request.tools:
            params["tools"] = [{"type": "function", "function": function_schema(tool)} for tool in request.tools]
        if request.tool_choice is not None:
            params["tool_choice"] = {"type": "function", "function": {"name": request.tool_choice.name}} if isinstance(request.tool_choice, LLMNamedToolChoice) else request.tool_choice
        if request.output_options is not None:
            options = request.output_options
            format = text_format(options.text_format)
            params["response_format"] = {"type": "json_schema", "json_schema": {key: value for key, value in format.items() if key != "type"}} if format["type"] == "json_schema" else format
            params["modalities"] = list(options.modalities)
            if options.audio is not None:
                params["audio"] = options.audio.model_dump(mode="json")
        return params

    def decode_response(self, raw: dict, *, provider_id: str, request: LLMRequest | None = None) -> LLMResponse:
        if raw.get("error"):
            remote_failure(raw, provider_id, self.protocol)
        choices = raw.get("choices")
        if not isinstance(choices, list) or len(choices) != 1:
            raise ValueError("Chat response must contain exactly one candidate.")
        choice = choices[0]
        if choice.get("finish_reason") is None:
            raise ValueError("Chat response has no terminal finish reason.")
        message = choice["message"]
        group = raw.get("id") or str(uuid4())
        parts = []
        if isinstance(message.get("content"), str):
            parts.append(TextContent(text=message["content"], annotations=tuple(message.get("annotations") or ())))
        elif message.get("content") is not None:
            raise ValueError("Unsupported Chat assistant content shape.")
        if message.get("refusal") is not None:
            parts.append(RefusalContent(text=message["refusal"]))
        if message.get("audio") is not None:
            audio = message["audio"]
            if request is None or request.output_options is None or request.output_options.audio is None:
                raise ValueError("Audio response decoding requires the request audio format.")
            format = request.output_options.audio.format
            parts.append(AudioContent(source=DataSource(data=audio["data"], mime_type=_AUDIO_MIME[format]), transcript=audio.get("transcript"), native=native_fields(audio, {"data", "transcript"}, provider_id, self.protocol)))
        output = []
        if parts:
            data = native_fields(message, {"role", "content", "refusal", "audio", "tool_calls", "annotations"}, provider_id, self.protocol)
            data.payload["_group_id"] = group
            output.append(LLMMessage(role=message["role"], content=tuple(parts), native=data))
        for call in message.get("tool_calls") or ():
            if call["type"] == "function":
                data = native_fields(call, {"id", "type", "function"}, provider_id, self.protocol)
                data.payload["_group_id"] = group
                data.payload["_message_fields"] = {
                    key: value for key, value in message.items()
                    if key not in {"role", "content", "refusal", "audio", "tool_calls", "annotations"}
                }
                output.append(LLMToolCall(call_id=call["id"], name=call["function"]["name"], arguments=call["function"]["arguments"], native=data))
            else:
                output.append(LLMProviderItem(id=call.get("id"), provider_type=call["type"], native=LLMNativeData(provider_id=provider_id, protocol=self.protocol, payload={**call, "_group_id": group})))
        if not output and choice["finish_reason"] not in {"length", "content_filter"}:
            raise ValueError("Chat response has no usable output.")
        finish = choice["finish_reason"]
        return LLMResponse(
            model=raw["model"], output=tuple(output),
            finish_reason=finish if finish in {"stop", "tool_calls", "length", "content_filter"} else "other",
            usage=usage(raw.get("usage"), chat=True), provider_response_id=raw.get("id"),
            provider_metadata={**{key: value for key, value in raw.items() if key not in {"choices", "usage", "model", "id"}}, "finish_reason": finish},
        )

    def _extra_stream_delta(self, message: dict, delta: dict, provider_id: str):
        return ()

    def _has_extra_stream_output(self, delta: dict) -> bool:
        return False

    def _extra_tool_delta(self, target: dict, call: dict) -> None:
        pass

    async def decode_stream(self, source: AsyncIterator[object], *, request: LLMRequest, provider_id: str) -> AsyncIterator[LLMStreamChunk]:
        message = {"role": "assistant", "content": None}
        raw = {"model": request.model, "choices": [{"message": message, "finish_reason": None}]}
        calls = {}
        audio_data = bytearray()
        terminal = False
        async for value in source:
            chunk = raw_object(value)
            if chunk.get("error"):
                remote_failure(chunk, provider_id, self.protocol)
            for field in ("id", "model", "usage"):
                if chunk.get(field) is not None:
                    raw[field] = chunk[field]
            choices = chunk.get("choices") or []
            if not choices:
                continue  # A usage-only final chunk is part of the same call.
            if len(choices) != 1 or choices[0].get("index", 0) != 0:
                raise ValueError("Chat stream must contain one candidate.")
            choice = choices[0]
            delta = choice.get("delta") or {}
            if terminal and (any(delta.get(field) for field in ("content", "refusal", "tool_calls", "audio")) or self._has_extra_stream_output(delta)):
                raise ValueError("Chat stream emitted output after its finish reason.")
            for observation in self._extra_stream_delta(message, delta, provider_id):
                yield observation
            for field, kind in (("content", "text"), ("refusal", "refusal")):
                if delta.get(field) is not None:
                    message[field] = (message.get(field) or "") + delta[field]
                    # Chat has named delta fields, not content-part indices.
                    yield LLMStreamChunk(type="delta", item_key="message", kind=kind, delta=delta[field])
            for call in delta.get("tool_calls") or ():
                index = call["index"]
                target = calls.setdefault(index, {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                self._extra_tool_delta(target, call)
                if call.get("type") not in {None, "function"}:
                    raise ValueError("Chat streaming adapter supports function calls only.")
                if call.get("id"):
                    if target["id"] and target["id"] != call["id"]:
                        raise ValueError("Chat Tool call identity changed during streaming.")
                    target["id"] = call["id"]
                function = call.get("function") or {}
                if function.get("name") and function["name"] != target["function"]["name"]:
                    target["function"]["name"] += function["name"]
                if function.get("arguments") is not None:
                    target["function"]["arguments"] += function["arguments"]
                    yield LLMStreamChunk(type="delta", item_key=f"tool:{index}", kind="tool_arguments", delta=function["arguments"])
            if delta.get("audio") is not None:
                audio = delta["audio"]
                target = message.setdefault("audio", {})
                for field in ("id", "expires_at"):
                    if audio.get(field) is not None:
                        target[field] = audio[field]
                for field, kind in (("data", "audio"), ("transcript", "audio_transcript")):
                    if audio.get(field) is not None:
                        if field == "data":
                            audio_data.extend(base64.b64decode(audio[field], validate=True))
                        else:
                            target[field] = target.get(field, "") + audio[field]
                        yield LLMStreamChunk(type="delta", item_key="message", kind=kind, delta=audio[field])
            if choice.get("finish_reason") is not None:
                terminal = True
                raw["choices"][0]["finish_reason"] = choice["finish_reason"]
        if not terminal:
            raise LLMProviderError("Chat stream ended without a finish reason.", provider_id=provider_id, protocol=self.protocol, retryable=False)
        if calls:
            message["tool_calls"] = [calls[index] for index in sorted(calls)]
        if "audio" in message:
            message["audio"]["data"] = base64.b64encode(audio_data).decode("ascii")
        response = self.decode_response(raw, provider_id=provider_id, request=request)
        call_indices = iter(sorted(calls))
        for item in response.output:
            key = (f"tool:{next(call_indices)}" if isinstance(item, (LLMToolCall, LLMProviderItem))
                   else "reasoning" if isinstance(item, LLMReasoning) else "message")
            yield LLMStreamChunk(type="item", item_key=key, item=item)
        yield LLMStreamChunk(type="completed", response=response)


__all__ = ["ChatCompletionsAdapter"]
