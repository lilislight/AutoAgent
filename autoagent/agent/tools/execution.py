"""One validated Tool call, without Workflow scheduling or retry policy."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import json
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pydantic import ValidationError
from autoagent.core.operators import run_sync

from ..models import (
    AudioContent, FileContent, ImageContent, LLMToolCall, LLMToolDefinition,
    LLMToolResult, RefusalContent, TextContent, ToolOutput, VideoContent,
)
from ..models._base import _check_finite
from .definition import (
    TOOL_DEFINITION_ATTRIBUTE, ToolDefinition, get_tool_definition,
)
from .context import ToolContext, _current_context, resolve_context

if TYPE_CHECKING:
    from autoagent.core import Operator


class ToolError(RuntimeError):
    """An unexpected Tool execution or output failure for Core to observe."""

    def __init__(self, message: str, *, tool_name: str, call_id: str) -> None:
        super().__init__(message)
        self.tool_name = tool_name
        self.call_id = call_id


class ToolExecutionError(ToolError):
    """User code failed unexpectedly; the original exception remains the cause."""


class ToolOutputError(ToolError):
    """A Tool returned a value outside its declared or portable output contract."""


class ToolBusinessError(Exception):
    """An expected domain failure that should be reported to the model."""

    def __init__(self, message: str, *, code: str = "tool_error") -> None:
        if not isinstance(message, str) or not isinstance(code, str) or not code.strip():
            raise TypeError("Tool business errors require a message and nonempty string code.")
        super().__init__(message)
        self.code = code


@runtime_checkable
class ExecutableTool(Protocol):
    """Shared boundary for local Tools and future remote Tool adapters."""

    @property
    def definition(self) -> LLMToolDefinition: ...

    async def ainvoke(self, call: LLMToolCall) -> LLMToolResult: ...


def error_result(call: LLMToolCall, kind: str, message: str) -> LLMToolResult:
    return LLMToolResult(
        call_id=call.call_id, is_error=True,
        structured_content={"error": {"kind": kind, "message": message}},
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"Duplicate JSON property: {name!r}.")
        result[name] = value
    return result


class Tool:
    """An executable typed function, separate from its advertised definition.

    ainvoke uses Core's executor and cancellation accounting inside Operators;
    standalone synchronous handlers use the calling loop's default executor.
    Argument and declared business failures become observations; unexpected
    exceptions and invalid outputs raise ToolError without automatic retries.
    """

    def __init__(
        self, handler: Callable[..., object], *, name: str | None = None,
        description: str | None = None, strict: bool | None = None,
    ) -> None:
        if hasattr(handler, TOOL_DEFINITION_ATTRIBUTE):
            if name is not None or description is not None or strict is not None:
                raise ValueError("Set decorated Tool metadata on @tool, not on its executor.")
            self.metadata = get_tool_definition(handler)
        else:
            self.metadata = ToolDefinition.from_callable(
                handler, name=name, description=description, strict=strict,
            )
        self._handler = handler

    @property
    def definition(self) -> LLMToolDefinition:
        return self.metadata.llm_definition()

    @property
    def is_async(self) -> bool:
        return self.metadata.is_async

    def _prepare(self, call: LLMToolCall) -> tuple[dict, dict] | LLMToolResult:
        if call.name != self.metadata.name:
            return error_result(call, "unknown_tool", f"This Tool is named {self.metadata.name!r}, not {call.name!r}.")
        try:
            decoded = json.loads(call.arguments, object_pairs_hook=_unique_object)
            _check_finite(decoded)
            if not isinstance(decoded, dict):
                raise ValueError("Tool arguments must be a JSON object.")
            value = self.metadata._input_model.model_validate_json(call.arguments, strict=True)
        except ValidationError as error:
            details = error.errors(include_url=False, include_input=False, include_context=False)
            message = "; ".join(
                f"{'.'.join(str(part) for part in item['loc']) or 'arguments'}: {item['msg']}"
                for item in details
            )
            return error_result(call, "invalid_arguments", message)
        except (TypeError, ValueError) as error:
            return error_result(call, "invalid_arguments", str(error))
        arguments = {
            parameter: getattr(value, internal)
            for internal, parameter in self.metadata._bindings
        }
        return arguments, value.model_dump(mode="json", by_alias=True, warnings="error")

    def _result(self, call: LLMToolCall, value: object) -> LLMToolResult:
        try:
            adapter = self.metadata._output_adapter
            validated = adapter.validate_python(value, strict=True)
            record = adapter.dump_python(validated, mode="json", by_alias=True, warnings="error")
            encoded = json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
            # Revalidate JSON as well: a mutated BaseModel must not bypass its
            # constraints merely because its default instance policy trusts it.
            validated = adapter.validate_json(encoded, strict=True)
            # Validators may normalize values during JSON validation. Emit that
            # validated representation, including aliases and custom serializers.
            record = adapter.dump_python(validated, mode="json", by_alias=True, warnings="error")
            encoded = json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
            if isinstance(validated, ToolOutput):
                output = validated
            elif isinstance(validated, (TextContent, ImageContent, AudioContent, VideoContent, FileContent, RefusalContent)):
                output = ToolOutput(content=(validated,))
            elif isinstance(record, dict):
                output = ToolOutput(structured_content=record)
            else:
                output = ToolOutput(content=(TextContent(text=record if isinstance(record, str) else encoded),))
            return LLMToolResult(
                call_id=call.call_id, content=output.content,
                structured_content=output.structured_content, is_error=output.is_error,
                structured_content_present=output.structured_content_present,
                tool_metadata=output.tool_metadata,
            )
        except Exception as error:
            raise ToolOutputError(
                f"Tool {call.name!r} returned an invalid output: {error}",
                tool_name=call.name, call_id=call.call_id,
            ) from error

    def _failure(self, call: LLMToolCall, error: BaseException) -> ToolExecutionError:
        return ToolExecutionError(
            f"Tool {call.name!r} raised {type(error).__name__}: {error}",
            tool_name=call.name, call_id=call.call_id,
        )

    def invoke(self, call: LLMToolCall, *, context: ToolContext | None = None) -> LLMToolResult:
        """Execute a synchronous Tool on the caller's thread."""
        if self.is_async:
            raise TypeError("Use ainvoke for an asynchronous Tool.")
        call = LLMToolCall.model_validate(call)
        prepared = self._prepare(call)
        if isinstance(prepared, LLMToolResult):
            return prepared
        arguments, record = prepared
        context = resolve_context(call, context)
        if context.allowed_tools is not None and call.name not in context.allowed_tools:
            return error_result(call, "tool_access_denied", "Tool execution is not permitted.")
        return self._execute(call, arguments, context.for_call(call, record))

    def _execute(self, call, arguments, context):
        arguments = dict(arguments)
        if self.metadata.context_parameter is not None:
            arguments[self.metadata.context_parameter] = context
        token = _current_context.set(context)
        try:
            try:
                value = self._handler(**arguments)
            finally:
                _current_context.reset(token)
        except ToolBusinessError as error:
            return error_result(call, error.code, str(error))
        except (Exception, asyncio.CancelledError) as error:
            raise self._failure(call, error) from error
        return self._result(call, value)

    async def ainvoke(self, call: LLMToolCall, *, context: ToolContext | None = None) -> LLMToolResult:
        """Execute one Tool; externally requested cancellation stays cancellation."""
        if not self.is_async:
            return await run_sync(self.invoke, call, context=context)
        call = LLMToolCall.model_validate(call)
        prepared = self._prepare(call)
        if isinstance(prepared, LLMToolResult):
            return prepared
        arguments, record = prepared
        context = resolve_context(call, context)
        if context.allowed_tools is not None and call.name not in context.allowed_tools:
            return error_result(call, "tool_access_denied", "Tool execution is not permitted.")
        return await self._aexecute(call, arguments, context.for_call(call, record))

    async def _aexecute(self, call, arguments, context):
        if not self.is_async:
            return await run_sync(self._execute, call, arguments, context)
        arguments = dict(arguments)
        if self.metadata.context_parameter is not None:
            arguments[self.metadata.context_parameter] = context
        token = _current_context.set(context)
        try:
            try:
                value = await self._handler(**arguments)
            finally:
                _current_context.reset(token)
        except ToolBusinessError as error:
            return error_result(call, error.code, str(error))
        except asyncio.CancelledError as error:
            if asyncio.current_task().cancelling():
                raise
            raise self._failure(call, error) from error
        except Exception as error:
            raise self._failure(call, error) from error
        return self._result(call, value)

    def as_operator(self, *, id: str | None = None) -> Operator:
        from ..integration.tools import tool_operator

        return tool_operator(self, id=id)


__all__ = [
    "ExecutableTool", "Tool", "ToolBusinessError", "ToolError",
    "ToolExecutionError", "ToolOutputError",
]
