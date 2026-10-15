"""Interaction items preserve model output order and protocol replay data."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, JsonValue, model_validator

from ._base import Model, NonEmpty, StructuredContentModel
from .content import LLMContent, LLMNativeData, RefusalContent, TextContent


class _Item(Model):
    id: NonEmpty | None = None
    native: LLMNativeData | None = None


class LLMMessage(_Item):
    """A message with ordered content, including system/developer instructions."""

    type: Literal["message"] = "message"
    role: Literal["system", "developer", "user", "assistant"]
    content: tuple[LLMContent, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _role_content(self) -> LLMMessage:
        if self.role in {"system", "developer"} and any(
            not isinstance(part, TextContent) for part in self.content
        ):
            raise ValueError("System and developer messages require text content.")
        if self.role != "assistant" and any(
            isinstance(part, RefusalContent) for part in self.content
        ):
            raise ValueError("Refusal content belongs to assistant messages.")
        return self


class LLMToolCall(_Item):
    """A function call; arguments remain untrusted, possibly invalid JSON.

    id is the provider item identity. call_id correlates Tool results and is not
    an application idempotency key. An adapter must not parse arguments eagerly.
    """

    type: Literal["tool_call"] = "tool_call"
    call_id: NonEmpty
    name: NonEmpty
    arguments: str


class LLMToolResult(_Item, StructuredContentModel):
    """A Tool observation supplied to the next model request."""

    type: Literal["tool_result"] = "tool_result"
    call_id: NonEmpty
    content: tuple[LLMContent, ...] = ()
    is_error: bool = False
    tool_metadata: dict[str, JsonValue] = Field(default_factory=dict)


class LLMReasoning(_Item):
    """OpenAI reasoning text/summary and opaque encrypted replay content."""

    type: Literal["reasoning"] = "reasoning"
    text: str | None = None
    summary: tuple[str, ...] = ()
    encrypted_content: str | None = None


class LLMProviderItem(_Item):
    """An OpenAI-specific item interpreted by its protocol adapter.

    Hosted tool activity is not converted to a local LLMToolCall automatically.
    """

    type: Literal["provider_item"] = "provider_item"
    provider_type: NonEmpty
    native: LLMNativeData


LLMItem = Annotated[
    LLMMessage | LLMToolCall | LLMToolResult | LLMReasoning | LLMProviderItem,
    Field(discriminator="type"),
]

LLMOutputItem = Annotated[
    LLMMessage | LLMToolCall | LLMReasoning | LLMProviderItem,
    Field(discriminator="type"),
]

__all__ = [
    "LLMItem", "LLMMessage", "LLMOutputItem", "LLMProviderItem",
    "LLMReasoning", "LLMToolCall", "LLMToolResult",
]
