"""One bounded model request and response, based on OpenAI API semantics.

These models perform no SDK calls, media resolution or Tool execution.
Endpoint/model capability checks belong to the selected Provider adapter.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, JsonValue, TypeAdapter, model_validator

from ._base import Model, NonEmpty, OpenAIProtocol
from .content import (
    AudioContent,
    FileContent,
    ImageContent,
    LLMMediaContent,
    TextContent,
    VideoContent,
)
from .items import LLMItem, LLMMessage, LLMOutputItem, LLMToolCall


ToolName = Annotated[
    str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
]


class LLMToolDefinition(Model):
    """Serializable function schema sent to OpenAI, without an executable."""

    name: ToolName
    parameters: dict[str, JsonValue]
    description: str | None = None
    strict: bool | None = None

    @model_validator(mode="after")
    def _object_parameters(self) -> LLMToolDefinition:
        if self.parameters.get("type") != "object":
            raise ValueError("Function parameters must declare an object JSON Schema.")
        return self


class LLMNamedToolChoice(Model):
    """Select one advertised function by name."""

    type: Literal["function"] = "function"
    name: ToolName


LLMToolChoice = Literal["auto", "none", "required"] | LLMNamedToolChoice


class LLMTextFormat(Model):
    type: Literal["text"] = "text"


class LLMJsonFormat(Model):
    type: Literal["json_object"] = "json_object"


class LLMJsonSchemaFormat(Model):
    """A portable output schema; Python annotations are compiled before use.

    Provider adapters validate the endpoint's supported JSON Schema subset.
    """

    type: Literal["json_schema"] = "json_schema"
    name: ToolName
    json_schema: dict[str, JsonValue]
    strict: bool = True

    @model_validator(mode="after")
    def _object_schema(self) -> LLMJsonSchemaFormat:
        if self.json_schema.get("type") != "object":
            raise ValueError("Structured output must declare an object JSON Schema.")
        return self

    @classmethod
    def from_type(
        cls, annotation: object, *, name: str | None = None, strict: bool = True
    ) -> LLMJsonSchemaFormat:
        """Compile a Python type without storing it in a model request."""

        schema = TypeAdapter(annotation).json_schema()
        resolved_name = name or getattr(annotation, "__name__", None) or "response"
        return cls(name=resolved_name, json_schema=schema, strict=strict)


LLMTextOutputFormat = Annotated[
    LLMTextFormat | LLMJsonFormat | LLMJsonSchemaFormat,
    Field(discriminator="type"),
]


class LLMVoiceReference(Model):
    """An OpenAI custom voice ID."""

    id: NonEmpty


class LLMAudioOutput(Model):
    """Chat Completions audio generation options."""

    voice: NonEmpty | LLMVoiceReference
    format: Literal["wav", "aac", "mp3", "flac", "opus", "pcm16"]


class LLMOutputOptions(Model):
    """Requested output; media returned by hosted tools remains in output items."""

    modalities: tuple[Literal["text", "audio"], ...] = ("text",)
    text_format: LLMTextOutputFormat = Field(default_factory=LLMTextFormat)
    audio: LLMAudioOutput | None = None

    @model_validator(mode="after")
    def _audio_options(self) -> LLMOutputOptions:
        if not self.modalities or len(set(self.modalities)) != len(self.modalities):
            raise ValueError("Output modalities must be nonempty and distinct.")
        if ("audio" in self.modalities) != (self.audio is not None):
            raise ValueError("Audio output requires audio modality and audio options together.")
        return self


_RESERVED_OPTIONS = frozenset({
    "model", "input", "messages", "instructions", "tools", "tool_choice",
    "max_output_tokens", "max_tokens", "max_completion_tokens", "temperature",
    "top_p", "modalities", "audio", "response_format", "text", "stream",
    "stream_options", "n", "extra_body",
    "api_key", "api_base", "base_url", "client", "timeout", "headers",
    "extra_headers", "num_retries", "max_retries", "fallbacks", "drop_params",
    "custom_llm_provider", "mock_response", "caching", "logger_fn",
})


class LLMProviderOptions(Model):
    """OpenAI protocol-specific JSON options that cannot replace common fields.

    These are API options, not credentials, SDK configuration or Python objects.
    The adapter determines which options its endpoint and model can accept.
    """

    protocol: OpenAIProtocol
    provider_id: NonEmpty | None = None
    values: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _reserved_fields(self) -> LLMProviderOptions:
        overlap = _RESERVED_OPTIONS.intersection(self.values)
        if overlap:
            raise ValueError(
                "Provider options cannot override normalized fields: "
                + ", ".join(sorted(overlap))
            )
        return self


class LLMRequest(Model):
    """One generation with ordered input and per-call execution settings.

    model=None delegates model selection to an application LLM resolver. Providers
    require a resolved model. LLM_CALL uses streaming to choose the Provider
    method; direct Provider calls use ainvoke/astream explicitly. routing contains
    application hints only and is never encoded as model API parameters.
    """

    model: NonEmpty | None = None
    input: tuple[LLMItem, ...] = Field(min_length=1)
    streaming: bool = False
    routing: dict[str, JsonValue] = Field(default_factory=dict)
    tools: tuple[LLMToolDefinition, ...] = ()
    tool_choice: LLMToolChoice | None = None
    max_output_tokens: int | None = Field(default=None, gt=0)
    temperature: float | None = Field(default=None, ge=0, le=2)
    top_p: float | None = Field(default=None, ge=0, le=1)
    output_options: LLMOutputOptions | None = None
    provider_options: LLMProviderOptions | None = None

    @model_validator(mode="after")
    def _tool_selection(self) -> LLMRequest:
        names = [tool.name for tool in self.tools]
        if len(names) != len(set(names)):
            raise ValueError("Advertised Tool names must be unique.")
        if self.tool_choice == "required" and not self.tools:
            raise ValueError("Required Tool choice needs at least one advertised Tool.")
        if (
            isinstance(self.tool_choice, LLMNamedToolChoice)
            and self.tool_choice.name not in names
        ):
            raise ValueError("Named Tool choice must select an advertised Tool.")
        return self


class LLMUsage(Model):
    """Reported token counts; absent measurements remain None."""

    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    reasoning_tokens: int | None = Field(default=None, ge=0)
    cache_read_tokens: int | None = Field(default=None, ge=0)
    native: dict[str, JsonValue] = Field(default_factory=dict)


LLMFinishReason = Literal["stop", "tool_calls", "length", "content_filter", "other"]


class LLMResponse(Model):
    """A terminal single-candidate response; output preserves the full item order.

    Request failures are raised by Providers. A truncated response can still
    contain output, with finish_reason='length'. Raw status/reason details may
    be retained in provider_metadata without changing the normalized reason.
    """

    model: NonEmpty
    output: tuple[LLMOutputItem, ...]
    finish_reason: LLMFinishReason
    usage: LLMUsage | None = None
    provider_response_id: NonEmpty | None = None
    provider_metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _assistant_output(self) -> LLMResponse:
        if any(
            isinstance(item, LLMMessage) and item.role != "assistant"
            for item in self.output
        ):
            raise ValueError("Model output messages must have the assistant role.")
        return self

    @property
    def text(self) -> str:
        """Concatenate visible assistant text, excluding reasoning and refusals."""

        return "".join(
            part.text
            for item in self.output
            if isinstance(item, LLMMessage)
            for part in item.content
            if isinstance(part, TextContent)
        )

    @property
    def tool_calls(self) -> tuple[LLMToolCall, ...]:
        return tuple(item for item in self.output if isinstance(item, LLMToolCall))

    @property
    def media(self) -> tuple[LLMMediaContent, ...]:
        return tuple(
            part
            for item in self.output
            if isinstance(item, LLMMessage)
            for part in item.content
            if isinstance(part, (ImageContent, AudioContent, VideoContent, FileContent))
        )

    def parse(self, annotation: object, *, strict: bool = True):
        """Parse a complete response using a caller-owned Python annotation."""
        from .structured import parse_response

        return parse_response(self, annotation, strict=strict)

    def validate_output(self, text_format: LLMTextOutputFormat):
        """Validate generated JSON against a portable requested output format."""
        from .structured import validate_response

        return validate_response(self, text_format)


__all__ = [
    "LLMAudioOutput", "LLMFinishReason", "LLMJsonFormat", "LLMJsonSchemaFormat",
    "LLMNamedToolChoice", "LLMOutputOptions", "LLMProviderOptions", "LLMRequest",
    "LLMResponse", "LLMTextFormat", "LLMTextOutputFormat", "LLMToolChoice",
    "LLMToolDefinition", "LLMUsage", "LLMVoiceReference",
]
