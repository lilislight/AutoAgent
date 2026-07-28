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
        for call in response.message.tool_calls:
            definition = tool_by_name.get(call.name)
            if definition is None:
                invalid.append(
                    InvalidToolCall(
                        call=call,
                        error=f"Unknown tool: {call.name}",
                    )
                )
                continue
            try:
                decoded = json.loads(call.raw_arguments)
                arguments = definition.contract.input.validate(decoded)
            except Exception as exc:
                invalid.append(InvalidToolCall(call=call, error=str(exc)))
                continue
            valid.append(
                ParsedToolCall(
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
        return ToolCallBatch(
            response=response,
            valid_calls=tuple(valid),
            invalid_calls=tuple(invalid),
        )

    def collect_results(
        self,
        batch: ToolCallBatch,
        executions: tuple[ToolExecutionResult, ...],
    ) -> ConversationUpdate:
        results = {item.tool_call_id: item for item in executions}
        invalid = {item.call.id: item for item in batch.invalid_calls}
        messages: list[LLMMessage] = [batch.response.message]
        for call in batch.response.message.tool_calls:
            failed = invalid.get(call.id)
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
                executed = results[call.id]
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
