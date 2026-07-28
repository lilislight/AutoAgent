from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_serializer,
    field_validator,
    model_validator,
)


_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,63}$")


class LLMToolCall(BaseModel):
    """One model-requested function call with untrusted raw arguments."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    name: str
    raw_arguments: str


class LLMMessage(BaseModel):
    """Provider-neutral text Chat Completions message."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    role: Literal["system", "user", "assistant", "tool"]
    content: str | None = None
    reasoning_content: str | None = None
    tool_calls: tuple[LLMToolCall, ...] = ()
    tool_call_id: str | None = None
    name: str | None = None

    @model_validator(mode="after")
    def validate_reasoning_content(self) -> LLMMessage:
        if self.reasoning_content is not None and self.role != "assistant":
            raise ValueError(
                "reasoning_content is supported only on assistant messages."
            )
        return self


class LLMToolDefinition(BaseModel):
    """Function Tool definition sent to a model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    name: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not _NAME_PATTERN.fullmatch(value):
            raise ValueError(
                "LLM Tool name must start with a letter or underscore and contain "
                "only letters, digits, underscores, or hyphens (max 64 characters)."
            )
        return value


class LLMNamedToolChoice(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str


LLMToolChoice = Literal["auto", "none", "required"] | LLMNamedToolChoice


class LLMResponseFormat(BaseModel):
    """Serializable JSON Schema compiled from a user-provided Python type."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    json_schema: dict[str, Any]
    strict: bool = True


class LLMUsage(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    prompt_cache_hit_tokens: int | None = None
    prompt_cache_miss_tokens: int | None = None
    reasoning_tokens: int | None = None


class LLMRequest(BaseModel):
    """One provider-neutral model request."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    messages: tuple[LLMMessage, ...]
    model: str | None = None
    tools: tuple[LLMToolDefinition, ...] = ()
    tool_choice: LLMToolChoice | None = None
    response_format: Any | None = None
    temperature: float | None = None
    max_output_tokens: int | None = Field(default=None, ge=1)
    provider_options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_provider_options(self) -> LLMRequest:
        reserved = {
            "model",
            "messages",
            "tools",
            "tool_choice",
            "response_format",
            "temperature",
            "max_completion_tokens",
            "stream",
            "stream_options",
            "extra_body",
        }
        overlap = reserved.intersection(self.provider_options)
        if overlap:
            raise ValueError(
                "provider_options cannot override normalized LLMRequest fields: "
                + ", ".join(sorted(overlap))
            )
        return self

    @field_validator("response_format")
    @classmethod
    def validate_response_format(cls, value: Any) -> Any:
        if value is None or isinstance(value, LLMResponseFormat):
            return value
        if isinstance(value, dict) and {"name", "json_schema"} <= value.keys():
            return LLMResponseFormat.model_validate(value)
        response_format_from_type(value)
        return value

    @field_serializer("response_format")
    def serialize_response_format(self, value: Any) -> Any:
        if value is None:
            return None
        return response_format_from_type(value).model_dump(mode="python")

    @property
    def response_format_spec(self) -> LLMResponseFormat | None:
        if self.response_format is None:
            return None
        return response_format_from_type(self.response_format)


class LLMResponse(BaseModel):
    """Normalized response from one model call."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    message: LLMMessage
    finish_reason: str | None = None
    model: str
    usage: LLMUsage | None = None
    provider_request_id: str | None = None


class LLMStreamChunk(BaseModel):
    """One normalized increment from a streaming LLM call."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal[
        "text_delta",
        "reasoning_delta",
        "tool_call_delta",
        "completed",
    ]
    text_delta: str | None = None
    reasoning_delta: str | None = None
    tool_call_index: int | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None
    tool_arguments_delta: str | None = None
    response: LLMResponse | None = None

    @model_validator(mode="after")
    def validate_payload(self) -> LLMStreamChunk:
        if self.type == "text_delta":
            if self.text_delta is None:
                raise ValueError("text_delta chunk requires text_delta.")
        elif self.type == "reasoning_delta":
            if self.reasoning_delta is None:
                raise ValueError(
                    "reasoning_delta chunk requires reasoning_delta."
                )
        elif self.type == "tool_call_delta":
            if self.tool_call_index is None:
                raise ValueError(
                    "tool_call_delta chunk requires tool_call_index."
                )
        elif self.response is None:
            raise ValueError("completed chunk requires response.")
        return self


def response_format_from_type(value: Any) -> LLMResponseFormat:
    if isinstance(value, LLMResponseFormat):
        return value
    try:
        adapter = TypeAdapter(value)
        schema = adapter.json_schema()
    except Exception as exc:
        raise TypeError(
            "response_format must be a Pydantic model, dataclass, TypedDict, "
            "or another type supported by pydantic.TypeAdapter."
        ) from exc
    name = getattr(value, "__name__", None) or schema.get("title") or "response"
    return LLMResponseFormat(name=_schema_name(str(name)), json_schema=schema)


def _schema_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]", "_", value)[:64]
    if not normalized or not re.match(r"^[A-Za-z_]", normalized):
        normalized = f"response_{normalized}"[:64]
    return normalized
