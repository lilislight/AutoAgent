"""Standard OpenAI Responses request, response and SSE semantics."""

from __future__ import annotations

from collections.abc import AsyncIterator
import json

from ..models import (
    FileContent, ImageContent, LLMMessage, LLMNamedToolChoice, LLMNativeData,
    LLMProviderItem, LLMReasoning, LLMRequest, LLMResponse, LLMToolCall,
    LLMToolResult, RefusalContent, TextContent,
)
from ..models.streaming import LLMStreamChunk
from ._common import (
    check_native, common_parameters, function_schema, native_fields, raw_object, remote_failure,
    source_value, text_format, unsupported, usage,
)
from .base import LLMProviderError
from .protocol import OpenAIProtocolAdapter


class ResponsesAdapter(OpenAIProtocolAdapter):
    protocol = "openai.responses"

    def _part(self, part, provider_id: str, *, output: bool = False) -> dict:
        check_native(part.native, provider_id, self.protocol)
        if isinstance(part, TextContent):
            result = {"type": "output_text" if output else "input_text", "text": part.text}
            if output:
                result["annotations"] = list(part.annotations)
            return result
        if isinstance(part, RefusalContent) and output:
            return {"type": "refusal", "refusal": part.text}
        if isinstance(part, (ImageContent, FileContent)) and not output:
            kind, value = source_value(part.source, provider_id, self.protocol)
            result = {"type": "input_image" if isinstance(part, ImageContent) else "input_file"}
            if isinstance(part, ImageContent):
                result["file_id" if kind == "id" else "image_url"] = value
                result["detail"] = part.detail or "auto"
            else:
                result[{"id": "file_id", "url": "file_url", "data": "file_data"}[kind]] = value
                if part.filename is not None:
                    result["filename"] = part.filename
                if part.detail is not None:
                    result["detail"] = part.detail
            return result
        unsupported(f"Responses cannot encode {part.type} in this content position.", provider_id, self.protocol)

    def _item(self, item, provider_id: str) -> dict:
        extra = check_native(item.native, provider_id, self.protocol)
        if isinstance(item, LLMMessage):
            output = item.role == "assistant" and item.id is not None
            result = {
                "type": "message", "role": item.role,
                "content": [self._part(part, provider_id, output=output) for part in item.content],
            }
            if output:
                result.update(id=item.id, status=extra.get("status", "completed"))
            return result
        if isinstance(item, LLMToolCall):
            result = {"type": "function_call", "call_id": item.call_id, "name": item.name, "arguments": item.arguments}
            if item.id is not None:
                result["id"] = item.id
            for name in ("status", "namespace", "caller"):
                if extra.get(name) is not None:
                    result[name] = extra[name]
            return result
        if isinstance(item, LLMToolResult):
            content = [self._part(part, provider_id) for part in item.content]
            if item.structured_content_present:
                content.append({"type": "input_text", "text": json.dumps(item.structured_content, ensure_ascii=False)})
            if item.is_error:
                content.insert(0, {"type": "input_text", "text": "Tool execution failed."})
            return {"type": "function_call_output", "call_id": item.call_id, "output": content or ""}
        if isinstance(item, LLMReasoning):
            if extra.get("type") == "reasoning":
                original = self._decode_item(extra, provider_id)
                for name in ("id", "text", "summary", "encrypted_content"):
                    if getattr(original, name) != getattr(item, name):
                        unsupported("Original reasoning must be replayed unmodified.", provider_id, self.protocol)
                return extra
            if item.id is None:
                unsupported("Responses reasoning replay requires an item ID.", provider_id, self.protocol)
            result = {"type": "reasoning", "id": item.id, "summary": [{"type": "summary_text", "text": text} for text in item.summary]}
            if item.encrypted_content is not None:
                result["encrypted_content"] = item.encrypted_content
            if item.text is not None:
                result["content"] = [{"type": "reasoning_text", "text": item.text}]
            return result
        if isinstance(item, LLMProviderItem):
            if extra.get("type") != item.provider_type or (item.id is not None and extra.get("id") != item.id):
                unsupported("Provider item identity does not match its original payload.", provider_id, self.protocol)
            return extra
        raise TypeError("Invalid LLM input item.")

    def encode_request(self, request: LLMRequest, *, provider_id: str) -> dict:
        params = common_parameters(request, self.protocol, provider_id)
        params["input"] = [self._item(item, provider_id) for item in request.input]
        if request.max_output_tokens is not None:
            params["max_output_tokens"] = request.max_output_tokens
        if request.tools:
            params["tools"] = [dict(type="function", **function_schema(tool)) for tool in request.tools]
        if request.tool_choice is not None:
            params["tool_choice"] = request.tool_choice.model_dump() if isinstance(request.tool_choice, LLMNamedToolChoice) else request.tool_choice
        if request.output_options is not None:
            if request.output_options.modalities != ("text",):
                unsupported("Responses adapter does not accept audio output options.", provider_id, self.protocol)
            params["text"] = {"format": text_format(request.output_options.text_format)}
        return params

    def _decode_item(self, raw: dict, provider_id: str):
        kind = raw["type"]
        if kind == "message" and raw.get("content"):
            parts = []
            for part in raw["content"]:
                if part["type"] == "output_text":
                    parts.append(TextContent(text=part["text"], annotations=tuple(part.get("annotations") or ()), native=native_fields(part, {"type", "text", "annotations"}, provider_id, self.protocol)))
                elif part["type"] == "refusal":
                    parts.append(RefusalContent(text=part["refusal"]))
                else:
                    # Preserve the entire item if its content has no public mapping.
                    return LLMProviderItem(id=raw.get("id"), provider_type=kind, native=LLMNativeData(provider_id=provider_id, protocol=self.protocol, payload=raw))
            return LLMMessage(id=raw.get("id"), role=raw["role"], content=tuple(parts), native=native_fields(raw, {"type", "id", "role", "content"}, provider_id, self.protocol))
        if kind == "function_call":
            return LLMToolCall(id=raw.get("id"), call_id=raw["call_id"], name=raw["name"], arguments=raw["arguments"], native=native_fields(raw, {"type", "id", "call_id", "name", "arguments"}, provider_id, self.protocol))
        if kind == "reasoning":
            return LLMReasoning(
                id=raw.get("id"), summary=tuple(part["text"] for part in raw.get("summary") or ()),
                text="".join(part["text"] for part in raw.get("content") or ()) if raw.get("content") is not None else None,
                encrypted_content=raw.get("encrypted_content"),
                native=LLMNativeData(provider_id=provider_id, protocol=self.protocol, payload=raw),
            )
        return LLMProviderItem(id=raw.get("id"), provider_type=kind, native=LLMNativeData(provider_id=provider_id, protocol=self.protocol, payload=raw))

    def decode_response(self, raw: dict, *, provider_id: str, request: LLMRequest | None = None) -> LLMResponse:
        if raw.get("error") or raw.get("status") == "failed":
            remote_failure(raw, provider_id, self.protocol)
        status = raw.get("status")
        if status not in {"completed", "incomplete"}:
            raise ValueError("Responses result is not terminal.")
        output = tuple(self._decode_item(item, provider_id) for item in raw["output"])
        reason = "tool_calls" if any(isinstance(item, LLMToolCall) for item in output) else "stop"
        if status == "incomplete":
            detail = (raw.get("incomplete_details") or {}).get("reason")
            reason = {"max_output_tokens": "length", "content_filter": "content_filter"}.get(detail, "other")
        return LLMResponse(
            model=raw["model"], output=output, finish_reason=reason,
            usage=usage(raw.get("usage"), chat=False), provider_response_id=raw.get("id"),
            provider_metadata={key: value for key, value in raw.items() if key not in {"output", "usage", "model", "id"}},
        )

    async def decode_stream(self, source: AsyncIterator[object], *, request: LLMRequest, provider_id: str) -> AsyncIterator[LLMStreamChunk]:
        kinds = {
            "response.output_text.delta": "text", "response.refusal.delta": "refusal",
            "response.reasoning_text.delta": "reasoning",
            "response.reasoning_summary_text.delta": "reasoning_summary",
            "response.function_call_arguments.delta": "tool_arguments",
            "response.audio.delta": "audio", "response.audio.transcript.delta": "audio_transcript",
        }
        async for value in source:
            event = raw_object(value)
            kind = event.get("type")
            if kind in kinds:
                key = f"item:{event['output_index']}" if "output_index" in event else "audio"
                yield LLMStreamChunk(type="delta", item_key=key, kind=kinds[kind], delta=event["delta"], content_index=event.get("content_index", event.get("summary_index")))
            elif kind == "response.output_item.done":
                yield LLMStreamChunk(type="item", item_key=f"item:{event['output_index']}", item=self._decode_item(event["item"], provider_id))
            elif kind in {"response.completed", "response.incomplete"}:
                response = self.decode_response(event["response"], provider_id=provider_id)
                yield LLMStreamChunk(type="completed", response=response)
                return
            elif kind == "response.failed":
                remote_failure(event["response"], provider_id, self.protocol)
            elif kind == "error":
                remote_failure({"error": event}, provider_id, self.protocol)
        raise LLMProviderError("Responses stream ended without a terminal event.", provider_id=provider_id, protocol=self.protocol, retryable=False)


__all__ = ["ResponsesAdapter"]
