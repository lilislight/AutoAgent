"""A Tool's observation before the framework assigns its call identity."""

from __future__ import annotations

from pydantic import Field, JsonValue

from ._base import StructuredContentModel
from .content import LLMContent


class ToolOutput(StructuredContentModel):
    """Explicit content, structured data or a business error from a Tool.

    Return this for media or mixed content. Ordinary typed Python return values
    are converted automatically. call_id is assigned from the incoming call.
    """

    content: tuple[LLMContent, ...] = ()
    is_error: bool = False
    tool_metadata: dict[str, JsonValue] = Field(default_factory=dict)


__all__ = ["ToolOutput"]
