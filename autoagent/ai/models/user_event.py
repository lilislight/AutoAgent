from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from autoagent.ai.models.llm import LLMResponse
from autoagent.ai.models.react import ToolExecutionResult


class ReactUserEventPayload(BaseModel):
    """Extensible V1 payload base for framework-defined ReAct UserEvents."""

    model_config = ConfigDict(extra="allow", frozen=True)


class MessageDeltaPayload(ReactUserEventPayload):
    delta: str


class ReasoningDeltaPayload(ReactUserEventPayload):
    delta: str


class ToolCallDeltaPayload(ReactUserEventPayload):
    tool_call_index: int
    tool_call_id: str | None = None
    tool_name: str | None = None
    arguments_delta: str | None = None


class MessageCompletedPayload(LLMResponse):
    """Provider-neutral completed response, never a Provider's raw object."""

    model_config = ConfigDict(extra="allow", frozen=True)


class ToolCallRequestedCall(ReactUserEventPayload):
    tool_call_id: str
    name: str
    raw_arguments: str


class ToolCallRequestedPayload(ReactUserEventPayload):
    calls: tuple[ToolCallRequestedCall, ...]
    reasoning_content: str | None = None


class ToolResultPayload(ReactUserEventPayload):
    results: tuple[ToolExecutionResult, ...]


class AgentOutputPayload(ReactUserEventPayload):
    output: Any


class MessageAbortedPayload(ReactUserEventPayload):
    error_type: str
    message: str


class AgentFailedPayload(ReactUserEventPayload):
    code: str
    message: str
    detail: dict[str, Any]


__all__ = [
    "AgentFailedPayload",
    "AgentOutputPayload",
    "MessageAbortedPayload",
    "MessageCompletedPayload",
    "MessageDeltaPayload",
    "ReactUserEventPayload",
    "ReasoningDeltaPayload",
    "ToolCallDeltaPayload",
    "ToolCallRequestedCall",
    "ToolCallRequestedPayload",
    "ToolResultPayload",
]
