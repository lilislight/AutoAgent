from __future__ import annotations

from typing import Any

from autoagent.ai.capabilities.llm_call import LLM_CALL_CAPABILITY_ID
from autoagent.ai.models.llm import LLMResponse, LLMStreamChunk
from autoagent.ai.models.user_event import (
    MessageCompletedPayload,
    MessageDeltaPayload,
    ReasoningDeltaPayload,
    ToolCallDeltaPayload,
    ToolCallRequestedCall,
    ToolCallRequestedPayload,
)
from autoagent.core.workflow import (
    CapabilityRef,
    InputMapping,
    Node,
    NodePolicy,
    OutputBinding,
    UserEventMapping,
)


def _text_delta(chunk: LLMStreamChunk) -> dict[str, Any] | None:
    if chunk.type != "text_delta":
        return None
    return MessageDeltaPayload(
        delta=chunk.text_delta or "",
    ).model_dump(mode="json")


def _reasoning_delta(chunk: LLMStreamChunk) -> dict[str, Any] | None:
    if chunk.type != "reasoning_delta":
        return None
    return ReasoningDeltaPayload(
        delta=chunk.reasoning_delta or "",
    ).model_dump(mode="json")


def _tool_call_delta(chunk: LLMStreamChunk) -> dict[str, Any] | None:
    if chunk.type != "tool_call_delta" or chunk.tool_call_index is None:
        return None
    return ToolCallDeltaPayload(
        tool_call_index=chunk.tool_call_index,
        tool_call_id=chunk.tool_call_id,
        tool_name=chunk.tool_name,
        arguments_delta=chunk.tool_arguments_delta,
    ).model_dump(mode="json")


def _completed_message(response: LLMResponse) -> dict[str, Any] | None:
    if response.message.tool_calls:
        return None
    return MessageCompletedPayload.model_validate(
        response.model_dump(mode="python")
    ).model_dump(mode="json")


def _requested_tool_calls(
    response: LLMResponse,
) -> dict[str, Any] | None:
    if not response.message.tool_calls:
        return None
    return ToolCallRequestedPayload(
        calls=tuple(
            ToolCallRequestedCall(
                tool_call_id=call.id,
                name=call.name,
                raw_arguments=call.raw_arguments,
            )
            for call in response.message.tool_calls
        ),
        reasoning_content=response.message.reasoning_content,
    ).model_dump(mode="json")


LLM_CALL_STREAM_USER_EVENT_MAPPINGS = (
    UserEventMapping(type="message_delta", transform=_text_delta),
    UserEventMapping(type="reasoning_delta", transform=_reasoning_delta),
    UserEventMapping(type="tool_call_delta", transform=_tool_call_delta),
)

LLM_CALL_USER_EVENT_MAPPINGS = (
    UserEventMapping(type="message_completed", transform=_completed_message),
    UserEventMapping(type="tool_call_requested", transform=_requested_tool_calls),
)


def llm_call_node(
    *,
    id: str,
    name: str | None = None,
    description: str | None = None,
    input_mapping: InputMapping | None = None,
    output_binding: OutputBinding | None = None,
    entry: bool | None = None,
    policy: NodePolicy | None = None,
    metadata: dict[str, Any] | None = None,
) -> Node:
    """Create an LLM Call Node with AutoAgent's stable UserEvent contract.

    Use this helper instead of a raw ``CapabilityRef("llm_call")`` when an LLM
    call should appear in Agent Activity. ReActWorkflow uses the same helper.
    """

    resolved_metadata = dict(metadata or {})
    resolved_metadata.setdefault("_autoagent_user_event_stream", "message")
    return Node(
        id=id,
        capability=CapabilityRef(id=LLM_CALL_CAPABILITY_ID),
        name=name,
        description=description,
        input_mapping=input_mapping,
        output_binding=output_binding,
        stream_user_event_mapping=LLM_CALL_STREAM_USER_EVENT_MAPPINGS,
        user_event_mapping=LLM_CALL_USER_EVENT_MAPPINGS,
        entry=entry,
        policy=policy,
        metadata=resolved_metadata,
    )


__all__ = [
    "LLM_CALL_STREAM_USER_EVENT_MAPPINGS",
    "LLM_CALL_USER_EVENT_MAPPINGS",
    "llm_call_node",
]
