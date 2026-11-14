"""Model-facing history projection without changing the original conversation.

Exact route/model matches may replay opaque protocol state. Other routes receive
portable messages and Tool exchanges, with explicit descriptions of opaque state.
Provider-scoped media must be supplied through a portable asset to transfer bytes.
"""

import json
import hashlib

from ..models import LLMMessage, LLMProviderItem, LLMReasoning, LLMToolResult, ProviderSource, RefusalContent, TextContent


class HistoryProjectionError(ValueError):
    """A historical resource needs application-owned media resolution."""


PROJECTION_VERSION = "2"


def _text(value):
    return LLMMessage(role="assistant", content=(TextContent(text=json.dumps(
        value, ensure_ascii=False, separators=(",", ":"))),))


def _media_reference(part):
    source = part.source.model_dump(mode="json", exclude_none=True)
    if "data" in source:
        source["data_sha256"] = hashlib.sha256(source.pop("data").encode()).hexdigest()
    content = part.model_dump(mode="json", exclude={"native", "source"}, exclude_none=True)
    return TextContent(text=json.dumps({"historical_media_reference": {
        **content, "source": source,
        "note": "Reference only; original media is retained in conversation history, not rendered as text.",
    }}, ensure_ascii=False, separators=(",", ":")))


def _part(part, target, *, role=None, tool=False):
    source = getattr(part, "source", None)
    if source is not None and (role == "assistant" or (tool and target is not None
            and target.protocol == "openai.chat_completions" and not target.tool_media)):
        return _media_reference(part)
    if isinstance(source, ProviderSource) and (target is None or source.provider_id != target.provider_id):
        raise HistoryProjectionError(
            "Historical media belongs to another Provider; supply an AssetSource "
            "and a media resolver or portable URL/data instead of reusing its file ID.")
    if isinstance(part, RefusalContent):
        return TextContent(text=json.dumps({"historical_refusal": part.text}, ensure_ascii=False))
    if isinstance(part, TextContent) and part.annotations:
        return TextContent(text=part.text + "\n" + json.dumps(
            {"historical_annotations": part.annotations}, ensure_ascii=False, separators=(",", ":")))
    return part.model_copy(update={"native": None}) if part.native is not None else part


def project_item(item, target, origin=None):
    """Keep native replay only with positive evidence of the same execution route."""
    if target is not None and target.accepts(origin):
        return (item,)
    if isinstance(item, LLMReasoning):
        content = {}
        if item.text and item.text.strip():
            content["text"] = item.text
        if any(text.strip() for text in item.summary):
            content["summary"] = item.summary
        if item.encrypted_content is not None:
            content["opaque_content"] = "Retained in original history; unavailable on this model route."
        if not content:
            return ()
        return (_text({"historical_reasoning": content}),)
    if isinstance(item, LLMProviderItem):
        return (_text({"historical_provider_activity": {
            "provider_type": item.provider_type, "id": item.id,
            "payload": item.native.payload,
            "note": "Historical data, not an executable request on the current route.",
        }}),)
    changes = {"id": None, "native": None}
    if isinstance(item, (LLMMessage, LLMToolResult)):
        changes["content"] = tuple(_part(part, target, role=getattr(item, "role", None),
            tool=isinstance(item, LLMToolResult)) for part in item.content)
    return (item.model_copy(update=changes),)


def project_items(items, target):
    """Project synthetic/fixed input with no claimed original route."""
    return tuple(value for item in items for value in project_item(item, target))


def project_history(conversation, target, *, start=0):
    """Walk the current view and its response origins; never reopen summaries."""
    result, cursor = [], 0
    origins = conversation.origins
    for segment in conversation.view.segments:
        if segment.end <= start:
            continue
        if segment.replacement is not None:
            if start > segment.start:
                raise ValueError("A token anchor cannot split a Context replacement.")
            result.extend(project_items(segment.replacement, target))
            continue
        for index in range(max(start, segment.start), segment.end):
            while cursor < len(origins) and origins[cursor].end <= index:
                cursor += 1
            origin = origins[cursor] if cursor < len(origins) and origins[cursor].start <= index else None
            result.extend(project_item(conversation.items[index], target, origin.route if origin else None))
            if (timestamp := conversation.runtime_times.get(str(index + 1))) is not None:
                from .runtime_context import time_message
                result.append(time_message(timestamp, target))
    return tuple(result)


__all__ = ["HistoryProjectionError", "project_history"]
