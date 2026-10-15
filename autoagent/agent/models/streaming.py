"""Portable streaming observations with an authoritative terminal response."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from ._base import Model, NonEmpty
from .items import LLMOutputItem
from .llm import LLMResponse


class LLMStreamChunk(Model):
    """A delta, completed item, or completed call.

    item_key is stable within one stream and does not assume that Chat
    Completions exposes Responses-style output indices. content_index is used
    when the protocol supplies one; Chat leaves it unset. Deltas are observations;
    completed.response is the authoritative result for both protocols.
    """

    type: Literal["delta", "item", "completed"]
    item_key: NonEmpty | None = None
    content_index: int | None = Field(default=None, ge=0)
    kind: Literal[
        "text", "refusal", "reasoning", "reasoning_summary", "tool_arguments", "audio", "audio_transcript"
    ] | None = None
    delta: str | None = None
    item: LLMOutputItem | None = None
    response: LLMResponse | None = None

    @model_validator(mode="after")
    def _payload(self) -> LLMStreamChunk:
        if self.type == "delta":
            valid = (
                self.item_key is not None and self.kind is not None
                and self.delta is not None and self.item is None and self.response is None
            )
        elif self.type == "item":
            valid = (
                self.item_key is not None and self.item is not None
                and self.kind is None and self.delta is None and self.response is None
                and self.content_index is None
            )
        else:
            valid = (
                self.response is not None and self.item is None and self.item_key is None
                and self.kind is None and self.delta is None and self.content_index is None
            )
        if not valid:
            raise ValueError(f"Invalid payload for {self.type} LLM stream chunk.")
        return self


__all__ = ["LLMStreamChunk"]
