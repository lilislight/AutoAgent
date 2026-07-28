from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from autoagent.ai.models.llm import (
    LLMMessage,
    LLMRequest,
    LLMResponse,
    LLMToolCall,
)


class ConversationUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["initial", "tool_results", "output_repair"]
    messages: tuple[LLMMessage, ...]
    provider_options: dict[str, Any] = Field(default_factory=dict)
    mode: Literal["invoke", "stream"] = "invoke"


class PreparedLLMCall(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request: LLMRequest
    mode: Literal["invoke", "stream"] = "invoke"


class ParsedToolCall(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    call: LLMToolCall
    tool_id: str
    arguments: dict[str, Any]


class InvalidToolCall(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    call: LLMToolCall
    error: str


class ToolCallBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    response: LLMResponse
    valid_calls: tuple[ParsedToolCall, ...] = ()
    invalid_calls: tuple[InvalidToolCall, ...] = ()


class ToolExecutionError(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    type: str
    message: str


class ToolExecutionResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)

    tool_call_id: str
    tool_id: str
    output: Any = None
    error: ToolExecutionError | None = None


class ToolInvocationOutcome(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)

    output: Any = None
    error: ToolExecutionError | None = None


class ToolExecutionBatch(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)

    results: tuple[ToolExecutionResult, ...]


class OutputValidationResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)

    response: LLMResponse
    valid: bool
    value: Any = None
    error: str | None = None
