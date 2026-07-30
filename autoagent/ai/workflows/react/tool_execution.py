from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import TypeAdapter

from autoagent.ai.models.llm import LLMMessage, LLMResponse
from autoagent.ai.models.react import (
    ConversationUpdate,
    InvalidToolCall,
    ParsedToolCall,
    ToolCallBatch,
    ToolExecutionError,
    ToolExecutionResult,
    ToolInvocationOutcome,
)
from autoagent.ai.tools import ToolDefinition


class ToolArgumentsRepairExhausted(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ToolExecutionPlan:
    definitions: tuple[ToolDefinition, ...]
    max_parse_retries: int

    def validate_calls(
        self,
        response: LLMResponse,
        previous_failures: int,
    ) -> ToolCallBatch:
        tool_by_name = {item.name: item for item in self.definitions}
        valid: list[ParsedToolCall] = []
        invalid: list[InvalidToolCall] = []
        normalized_calls = []
        for call_index, original_call in enumerate(response.message.tool_calls):
            empty_id = not original_call.id.strip()
            empty_name = not original_call.name.strip()
            call = original_call.model_copy(
                update={
                    "id": (
                        f"invalid_tool_call_{call_index}"
                        if empty_id
                        else original_call.id
                    ),
                    "name": (
                        "__invalid_tool__"
                        if empty_name
                        else original_call.name
                    ),
                }
            )
            normalized_calls.append(call)
            if empty_id:
                invalid.append(
                    InvalidToolCall(
                        call_index=call_index,
                        call=call,
                        error="Tool call id cannot be empty.",
                    )
                )
                continue
            if empty_name:
                invalid.append(
                    InvalidToolCall(
                        call_index=call_index,
                        call=call,
                        error="Tool name cannot be empty.",
                    )
                )
                continue
            definition = tool_by_name.get(call.name)
            if definition is None:
                invalid.append(
                    InvalidToolCall(
                        call_index=call_index,
                        call=call,
                        error=f"Unknown tool: {call.name}",
                    )
                )
                continue
            try:
                decoded = json.loads(call.raw_arguments)
                arguments = definition.contract.input.validate(decoded)
            except Exception as exc:
                invalid.append(
                    InvalidToolCall(
                        call_index=call_index,
                        call=call,
                        error=str(exc),
                    )
                )
                continue
            valid.append(
                ParsedToolCall(
                    call_index=call_index,
                    call=call,
                    tool_id=definition.id,
                    arguments=arguments,
                )
            )
        if invalid and previous_failures >= self.max_parse_retries:
            raise ToolArgumentsRepairExhausted(
                "Tool arguments remained invalid after "
                f"{self.max_parse_retries} repair attempt(s): "
                + "; ".join(item.error for item in invalid)
            )
        normalized_response = response.model_copy(
            update={
                "message": response.message.model_copy(
                    update={"tool_calls": tuple(normalized_calls)}
                )
            }
        )
        return ToolCallBatch(
            response=normalized_response,
            valid_calls=tuple(valid),
            invalid_calls=tuple(invalid),
        )

    def collect_results(
        self,
        batch: ToolCallBatch,
        executions: tuple[ToolExecutionResult, ...],
    ) -> ConversationUpdate:
        results = {item.call_index: item for item in executions}
        invalid = {item.call_index: item for item in batch.invalid_calls}
        messages: list[LLMMessage] = [batch.response.message]
        for call_index, call in enumerate(batch.response.message.tool_calls):
            failed = invalid.get(call_index)
            if failed is not None:
                content = json.dumps(
                    {
                        "error": {
                            "type": "tool_arguments_validation_error",
                            "tool": call.name,
                            "message": failed.error,
                            "raw_arguments": call.raw_arguments,
                        }
                    },
                    separators=(",", ":"),
                )
            else:
                executed = results[call_index]
                if executed.error is None:
                    content = tool_output_json(executed.output)
                else:
                    content = json.dumps(
                        {
                            "error": {
                                "type": "tool_execution_error",
                                "tool": call.name,
                                "exception_type": executed.error.type,
                                "message": executed.error.message,
                            }
                        },
                        separators=(",", ":"),
                    )
            messages.append(
                LLMMessage(
                    role="tool",
                    name=call.name,
                    tool_call_id=call.id,
                    content=content,
                )
            )
        return ConversationUpdate(kind="tool_results", messages=tuple(messages))


def recoverable_tool_handler(
    handler: Callable[..., Any],
) -> Callable[[dict[str, Any]], ToolInvocationOutcome]:
    """Turn user Tool exceptions into observations the LLM can repair."""

    if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None)
    ):

        async def invoke(arguments: dict[str, Any]) -> ToolInvocationOutcome:
            try:
                return ToolInvocationOutcome(output=await handler(**arguments))
            except Exception as exc:
                return ToolInvocationOutcome(
                    error=ToolExecutionError(
                        type=type(exc).__name__,
                        message=str(exc),
                    )
                )

        return invoke

    def invoke(arguments: dict[str, Any]) -> ToolInvocationOutcome:
        try:
            return ToolInvocationOutcome(output=handler(**arguments))
        except Exception as exc:
            return ToolInvocationOutcome(
                error=ToolExecutionError(
                    type=type(exc).__name__,
                    message=str(exc),
                )
            )

    return invoke


def validate_tools(definitions: tuple[ToolDefinition, ...]) -> None:
    ids = [item.id for item in definitions]
    names = [item.name for item in definitions]
    if len(ids) != len(set(ids)):
        raise ValueError("ReAct Tool ids must be unique.")
    if len(names) != len(set(names)):
        raise ValueError("ReAct Tool names must be unique.")


def tool_output_json(value: Any) -> str:
    adapter = TypeAdapter(type(value))
    return adapter.dump_json(value).decode("utf-8")
