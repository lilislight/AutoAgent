"""Loss-aware MCP content conversion without fetching linked resources."""

from __future__ import annotations

from copy import deepcopy
from typing import Literal

from pydantic import Field, JsonValue

from ..models import (
    AssetSource, AudioContent, DataSource, FileContent, ImageContent, LLMContent,
    LLMMessage, TextContent, URLSource, VideoContent,
)
from ..models._base import Model, NonEmpty


class MCPContentError(ValueError):
    """SDK content cannot be represented faithfully by the Agent models."""


def _raw(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(value, dict):
        return deepcopy(value)
    raise MCPContentError("Expected an MCP model or JSON object.")


def _binary(data, mime, *, filename=None):
    source = DataSource(data=data, mime_type=mime or "application/octet-stream")
    media_type = source.mime_type.split(";", 1)[0]
    if media_type.startswith("image/"):
        return ImageContent(source=source)
    if media_type.startswith("audio/"):
        return AudioContent(source=source)
    if media_type.startswith("video/"):
        return VideoContent(source=source)
    return FileContent(source=source, filename=filename)


def mcp_content(value) -> LLMContent:
    """Convert one MCP content block; URLs remain references and are not read."""
    block = _raw(value)
    try:
        kind = block.get("type")
        if kind == "text":
            return TextContent(text=block["text"])
        if kind in {"image", "audio"}:
            source = DataSource(data=block["data"], mime_type=block["mimeType"])
            return ImageContent(source=source) if kind == "image" else AudioContent(source=source)
        if kind == "resource_link":
            uri = block["uri"]
            mime = block.get("mimeType")
            source = URLSource(url=uri, mime_type=mime) if uri.startswith(("https://", "http://")) else AssetSource(uri=uri, mime_type=mime)
            return FileContent(source=source, filename=block.get("name"))
        if kind == "resource":
            return _resource(block["resource"]).content
        raise ValueError(f"Unsupported MCP content block: {kind!r}.")
    except (KeyError, TypeError, ValueError) as error:
        raise MCPContentError("Invalid or unsupported MCP content.") from error


def content_metadata(block):
    value = {key: item for key, item in block.items() if key not in {"text", "data"}}
    if "resource" in value:
        value["resource"] = {key: item for key, item in value["resource"].items() if key not in {"text", "blob"}}
    return value


class MCPResourceContent(Model):
    uri: NonEmpty
    mime_type: NonEmpty | None = None
    content: LLMContent
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class MCPResourceResponse(Model):
    contents: tuple[MCPResourceContent, ...]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @property
    def content(self) -> tuple[LLMContent, ...]:
        return tuple(item.content for item in self.contents)

    def to_message(self, *, role: Literal["user", "developer"] = "user") -> LLMMessage:
        return LLMMessage(role=role, content=self.content)


class MCPPromptResponse(Model):
    description: str | None = None
    messages: tuple[LLMMessage, ...]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    message_metadata: tuple[dict[str, JsonValue], ...] = ()


def _resource(resource):
    if ("text" in resource) == ("blob" in resource):
        raise MCPContentError("MCP resources require exactly one text or blob value.")
    content = TextContent(text=resource["text"]) if "text" in resource else _binary(resource["blob"], resource.get("mimeType"))
    return MCPResourceContent(uri=resource["uri"], mime_type=resource.get("mimeType"), content=content,
                              metadata={key: value for key, value in resource.items() if key not in {"uri", "mimeType", "text", "blob"}})


def mcp_resource_response(value) -> MCPResourceResponse:
    raw = _raw(value)
    try:
        return MCPResourceResponse(contents=tuple(_resource(item) for item in raw["contents"]),
                                   metadata={key: value for key, value in raw.items() if key != "contents"})
    except (KeyError, TypeError, ValueError) as error:
        raise MCPContentError("Invalid MCP resource response.") from error


def mcp_prompt_response(value) -> MCPPromptResponse:
    raw = _raw(value)
    try:
        messages = []
        metadata = []
        for item in raw["messages"]:
            if item["role"] not in {"user", "assistant"}:
                raise ValueError("MCP Prompt roles must be user or assistant.")
            messages.append(LLMMessage(role=item["role"], content=(mcp_content(item["content"]),)))
            metadata.append({key: value for key, value in item.items() if key not in {"role", "content"}} |
                            {"content": content_metadata(item["content"])})
        return MCPPromptResponse(description=raw.get("description"), messages=tuple(messages), message_metadata=tuple(metadata),
                                 metadata={key: value for key, value in raw.items() if key not in {"description", "messages"}})
    except (KeyError, TypeError, ValueError) as error:
        raise MCPContentError("Invalid MCP Prompt response.") from error


__all__ = ["MCPContentError", "MCPResourceContent", "MCPResourceResponse", "MCPPromptResponse",
           "mcp_content", "mcp_resource_response", "mcp_prompt_response"]
