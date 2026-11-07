"""Versioned presentation contracts for the SDK Agent Workflow's User Events.

These snapshots do not replace Conversation, execution controls, or the usage
ledger. Transient updates and committed snapshots share object identities.
"""

from typing import Annotated, Literal
from types import MappingProxyType

from pydantic import Field, JsonValue

from ..models._base import Model, NonEmpty


class AgentEventPayload(Model):
    v: Literal[1] = 1
    node_id: NonEmpty


class AgentEventMessage(Model):
    message_id: NonEmpty
    source: Literal["user", "agent"]
    sender_id: NonEmpty | None = None
    content: tuple[dict[str, JsonValue], ...] = Field(min_length=1)
    reply_requested: bool = False
    reply_to: NonEmpty | None = None


class AgentMessagesEvent(AgentEventPayload):
    messages: tuple[AgentEventMessage, ...] = Field(min_length=1)


class AgentResponseDeltaEvent(AgentEventPayload):
    response_id: NonEmpty
    stream_id: NonEmpty
    item_key: NonEmpty
    content_index: int | None = Field(default=None, ge=0)
    kind: Literal["text", "refusal", "reasoning", "reasoning_summary", "tool_arguments", "audio", "audio_transcript"]
    delta: str


class AgentTextBlock(Model):
    type: Literal["text", "refusal"]
    text: NonEmpty
    annotations: tuple[dict[str, JsonValue], ...] = ()


class AgentReasoningBlock(Model):
    type: Literal["reasoning"] = "reasoning"
    text: str | None = None
    summary: tuple[str, ...] = ()


class AgentToolBlock(Model):
    type: Literal["tool"] = "tool"
    tool_id: NonEmpty


class AgentMediaBlock(Model):
    type: Literal["media"] = "media"
    content: dict[str, JsonValue]


AgentResponseBlock = Annotated[
    AgentTextBlock | AgentReasoningBlock | AgentToolBlock | AgentMediaBlock,
    Field(discriminator="type"),
]


class AgentResponseEvent(AgentEventPayload):
    response_id: NonEmpty
    model: NonEmpty
    finish_reason: Literal["stop", "tool_calls", "length", "content_filter", "other"]
    blocks: tuple[AgentResponseBlock, ...] = ()
    usage: dict[str, int | None] | None = None


class AgentToolResultView(Model):
    content: tuple[dict[str, JsonValue], ...] = ()
    structured_content: JsonValue = None
    is_error: bool = False


class AgentToolSnapshot(Model):
    tool_id: NonEmpty
    response_id: NonEmpty
    call_id: NonEmpty
    name: NonEmpty
    arguments: str
    state: Literal["queued", "running", "waiting", "completed", "failed", "denied", "cancelled"]
    result: AgentToolResultView | None = None
    progress: JsonValue = None


class AgentToolsEvent(AgentEventPayload):
    tools: tuple[AgentToolSnapshot, ...] = Field(min_length=1)


class AgentEventError(Model):
    type: NonEmpty
    message: str


class AgentInteractionResolution(Model):
    outcome: Literal["answered", "awakened", "timed_out", "cancelled"]
    response: JsonValue = None
    error: AgentEventError | None = None


class AgentInteractionEvent(AgentEventPayload):
    interaction_id: NonEmpty
    type: Literal["question", "approval"]
    state: Literal["pending", "resolved"]
    tool_ids: tuple[NonEmpty, ...]
    request: dict[str, JsonValue]
    resolution: AgentInteractionResolution | None = None


class AgentCompactionEvent(AgentEventPayload):
    compaction_id: NonEmpty
    state: Literal["running", "completed"]
    before_tokens: int | None = Field(default=None, ge=0)
    after_tokens: int | None = Field(default=None, ge=0)
    view_generation: int = Field(ge=0)


class AgentErrorEvent(AgentEventPayload):
    error: AgentEventError
    stage: str | None = None
    handled: bool


class AgentCompletedEvent(AgentEventPayload):
    final_response_id: NonEmpty
    rounds: int = Field(ge=1)


AGENT_EVENT_PAYLOADS = MappingProxyType({
    "agent.messages": AgentMessagesEvent,
    "agent.response.delta": AgentResponseDeltaEvent,
    "agent.response": AgentResponseEvent,
    "agent.tools": AgentToolsEvent,
    "agent.interaction": AgentInteractionEvent,
    "agent.compaction": AgentCompactionEvent,
    "agent.error": AgentErrorEvent,
    "agent.completed": AgentCompletedEvent,
})


__all__ = ["AgentEventPayload", "AgentEventMessage", "AgentMessagesEvent", "AgentResponseDeltaEvent",
    "AgentTextBlock", "AgentReasoningBlock", "AgentToolBlock", "AgentMediaBlock", "AgentResponseBlock",
    "AgentResponseEvent", "AgentToolResultView", "AgentToolSnapshot", "AgentToolsEvent", "AgentEventError",
    "AgentInteractionResolution", "AgentInteractionEvent", "AgentCompactionEvent", "AgentErrorEvent",
    "AgentCompletedEvent", "AGENT_EVENT_PAYLOADS"]
