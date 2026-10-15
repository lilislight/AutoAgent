"""Ordered multimodal content shared by messages and Tool results."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, JsonValue, model_validator

from ._base import Model, NonEmpty, OpenAIProtocol
from .sources import LLMSource


class LLMNativeData(Model):
    """Protocol data required for faithful interpretation or later replay.

    provider_id identifies a configured connection, not just a vendor name.
    Adapters own payload interpretation, grouping and compatibility checks.
    """

    provider_id: NonEmpty
    protocol: OpenAIProtocol
    payload: dict[str, JsonValue] = Field(default_factory=dict)


class TextContent(Model):
    """Visible text; citations retain the original annotation objects."""

    type: Literal["text"] = "text"
    text: str
    annotations: tuple[dict[str, JsonValue], ...] = ()
    native: LLMNativeData | None = None


class _MediaContent(Model):
    source: LLMSource
    native: LLMNativeData | None = None


class ImageContent(_MediaContent):
    """An image input or output; detail is checked by the selected adapter."""

    type: Literal["image"] = "image"
    detail: Literal["auto", "low", "high", "original"] | None = None


class AudioContent(_MediaContent):
    """Audio and its optional transcript.

    OpenAI audio identity and expires_at can be retained in native.payload for
    subsequent turns; the transcript is not a replacement for the audio bytes.
    """

    type: Literal["audio"] = "audio"
    transcript: str | None = None


class VideoContent(_MediaContent):
    """A video resource, optionally limited to a time interval.

    Representation does not imply that an OpenAI endpoint accepts video input.
    Frame extraction or audio processing must be explicitly selected elsewhere.
    """

    type: Literal["video"] = "video"
    start_seconds: float | None = Field(default=None, ge=0)
    end_seconds: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _interval(self) -> VideoContent:
        if self.end_seconds is not None and self.end_seconds <= (self.start_seconds or 0):
            raise ValueError("Video end_seconds must be greater than start_seconds.")
        return self


class FileContent(_MediaContent):
    """A document such as PDF; text extraction is a separate operation."""

    type: Literal["file"] = "file"
    filename: NonEmpty | None = None
    detail: Literal["auto", "low", "high"] | None = None


class RefusalContent(Model):
    """A model refusal, kept distinct from ordinary assistant text."""

    type: Literal["refusal"] = "refusal"
    text: str
    native: LLMNativeData | None = None


LLMContent = Annotated[
    TextContent | ImageContent | AudioContent | VideoContent | FileContent | RefusalContent,
    Field(discriminator="type"),
]

LLMMediaContent = Annotated[
    ImageContent | AudioContent | VideoContent | FileContent,
    Field(discriminator="type"),
]

__all__ = [
    "AudioContent", "FileContent", "ImageContent", "LLMContent",
    "LLMMediaContent", "LLMNativeData", "RefusalContent", "TextContent", "VideoContent",
]
