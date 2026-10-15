"""Portable MCP progress observations and final Tool results."""

from __future__ import annotations

from typing import Literal

from pydantic import model_validator

from ..models import LLMToolResult
from ..models._base import Model, NonEmpty


class MCPToolStreamChunk(Model):
    type: Literal["progress", "completed"]
    server_id: NonEmpty
    tool_name: NonEmpty
    call_id: NonEmpty
    progress: float | None = None
    total: float | None = None
    message: str | None = None
    result: LLMToolResult | None = None

    @model_validator(mode="after")
    def _payload(self):
        if self.type == "progress":
            if self.progress is None or self.result is not None:
                raise ValueError("Progress requires a numeric value and no final result.")
        elif self.result is None or any(value is not None for value in (self.progress, self.total, self.message)):
            raise ValueError("Completed requires a final result and no progress fields.")
        if self.result is not None and self.result.call_id != self.call_id:
            raise ValueError("Stream result call_id does not match the stream.")
        return self


__all__ = ["MCPToolStreamChunk"]
