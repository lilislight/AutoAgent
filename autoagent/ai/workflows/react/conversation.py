from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from autoagent.ai.models.llm import (
    LLMMessage,
    LLMRequest,
    LLMResponseFormat,
    LLMToolDefinition,
)
from autoagent.ai.models.react import (
    ConversationUpdate,
    OutputValidationResult,
    PreparedLLMCall,
)


def initial_messages_from_value(source: Any) -> tuple[LLMMessage, ...]:
    if isinstance(source, LLMRequest):
        raw_messages: Any = source.messages
    elif isinstance(source, str):
        raw_messages: Any = [LLMMessage(role="user", content=source)]
    elif isinstance(source, LLMMessage):
        raw_messages = [source]
    elif isinstance(source, Mapping):
        raw_messages = source.get("messages")
        if raw_messages is None and "input" in source:
            raw_messages = [
                LLMMessage(role="user", content=str(source["input"]))
            ]
        if raw_messages is None:
            raise ValueError(
                "ReAct Workflow input requires 'input' or 'messages'."
            )
    else:
        raw_messages = source
    return tuple(
        item if isinstance(item, LLMMessage) else LLMMessage.model_validate(item)
        for item in raw_messages
    )


@dataclass(frozen=True, slots=True)
class ConversationPlan:
    instructions: str
    model: str | None
    tools: tuple[LLMToolDefinition, ...]
    response_format: Any | LLMResponseFormat | None

    def start(
        self,
        initial_messages: tuple[LLMMessage, ...],
        provider_options: dict[str, Any] | None = None,
        mode: Literal["invoke", "stream"] = "invoke",
    ) -> ConversationUpdate:
        return ConversationUpdate(
            kind="initial",
            messages=initial_messages,
            provider_options=dict(provider_options or {}),
            mode=mode,
        )

    def prepare(
        self,
        previous: PreparedLLMCall | None,
        update: ConversationUpdate,
    ) -> PreparedLLMCall:
        previous_request = previous.request if previous is not None else None
        messages = list(
            previous_request.messages if previous_request is not None else ()
        )
        if previous is None:
            messages.append(
                LLMMessage(role="system", content=self.instructions)
            )
        messages.extend(update.messages)
        return PreparedLLMCall(
            request=LLMRequest(
                messages=tuple(messages),
                model=self.model,
                tools=self.tools,
                tool_choice=(
                    "none"
                    if update.kind == "output_repair" or not self.tools
                    else "auto"
                ),
                response_format=self.response_format,
                provider_options=(
                    previous_request.provider_options
                    if previous_request is not None
                    else update.provider_options
                ),
            ),
            mode=previous.mode if previous is not None else update.mode,
        )

    def build_output_repair(
        self,
        result: OutputValidationResult,
    ) -> ConversationUpdate:
        return ConversationUpdate(
            kind="output_repair",
            messages=(
                result.response.message,
                LLMMessage(
                    role="user",
                    content=(
                        "Your previous response did not match the required output "
                        "schema. Return only a corrected structured response. "
                        f"Validation error: {result.error}"
                    ),
                ),
            ),
        )
