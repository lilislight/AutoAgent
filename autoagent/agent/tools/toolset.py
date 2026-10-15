"""A fixed, named Tool collection with a shared local/remote boundary."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable
from types import MappingProxyType
import asyncio
import inspect
from typing import TYPE_CHECKING

from ..models import LLMToolCall, LLMToolDefinition, LLMToolResult
from .definition import get_tool_definition
from .execution import ExecutableTool, Tool, ToolBusinessError, ToolOutputError, error_result
from .context import ToolContext, ToolHooks, _current_context, resolve_context

if TYPE_CHECKING:
    from autoagent.core import Operator


class ToolSet:
    """Validate unique names once; do not execute, schedule or reconnect at setup."""

    def __init__(self, tools: Iterable[ExecutableTool | Callable[..., object]] = (), *,
                 context: ToolContext | None = None, hooks: ToolHooks | None = None,
                 policy: Callable | None = None) -> None:
        self.context = context
        self.hooks = hooks or ToolHooks()
        self.policy = policy
        if context is not None and not isinstance(context, ToolContext):
            raise TypeError("ToolSet context must be a ToolContext.")
        if not isinstance(self.hooks, ToolHooks):
            raise TypeError("ToolSet hooks must be ToolHooks.")
        if any(callback is not None and not callable(callback) for callback in
               (self.policy, self.hooks.before, self.hooks.after, self.hooks.on_error)):
            raise TypeError("Tool policies and hooks must be callable.")
        registered = {}
        definitions = []
        for candidate in tools:
            if isinstance(candidate, ExecutableTool):
                executable = candidate
            elif callable(candidate):
                get_tool_definition(candidate)
                executable = Tool(candidate)
            else:
                raise TypeError("ToolSet accepts decorated functions or ExecutableTool objects.")
            definition = LLMToolDefinition.model_validate(executable.definition)
            if definition.name in registered:
                raise ValueError(f"Duplicate Tool name: {definition.name!r}.")
            registered[definition.name] = executable
            definitions.append(definition)
        self._tools = MappingProxyType(registered)
        self._definitions = tuple(definitions)

    @property
    def definitions(self) -> tuple[LLMToolDefinition, ...]:
        return self.definitions_for()

    def definitions_for(self, context: ToolContext | None = None) -> tuple[LLMToolDefinition, ...]:
        """Advertise only names permitted by explicit and configured restrictions."""
        if context is not None and not isinstance(context, ToolContext):
            raise TypeError("Tool advertisements require a ToolContext.")
        allowed = None if context is None else context.allowed_tools
        if self.context is not None and self.context.allowed_tools is not None:
            allowed = self.context.allowed_tools if allowed is None else allowed & self.context.allowed_tools
        return tuple(
            LLMToolDefinition.model_validate(value.model_dump(mode="python"))
            for value in self._definitions if allowed is None or value.name in allowed
        )

    def _check_core_hooks(self):
        if any(isinstance(value, Tool) and not value.is_async for value in self._tools.values()):
            for callback in (self.policy, self.hooks.before, self.hooks.after, self.hooks.on_error):
                if inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(getattr(callback, "__call__", None)):
                    raise TypeError("Core sync Tools require sync policies/hooks; use async Tools with async hooks.")

    def __len__(self) -> int:
        return len(self._tools)

    def __getitem__(self, name: str) -> ExecutableTool:
        return self._tools[name]

    def select(
        self, *, include: Collection[str] | None = None, exclude: Collection[str] = (),
    ) -> ToolSet:
        """Create an independent snapshot using advertised names."""
        allowed = None if include is None else frozenset(include)
        denied = frozenset(exclude)
        if allowed is not None and (missing := allowed - self._tools.keys()):
            raise ValueError(f"Unknown included Tools: {sorted(missing)!r}.")
        return ToolSet((value for name, value in self._tools.items()
                        if (allowed is None or name in allowed) and name not in denied),
                       context=self.context, hooks=self.hooks, policy=self.policy)

    def invoke(self, call: LLMToolCall, *, context: ToolContext | None = None) -> LLMToolResult:
        """Dispatch local synchronous Tools on the caller/Core worker thread.

        Synchronous dispatch requires synchronous policies/hooks. Use ainvoke
        for async hooks, and async Tools for that combination inside Core.
        """
        call = LLMToolCall.model_validate(call)
        if call.name not in self._tools:
            return error_result(call, "unknown_tool", f"Unknown Tool: {call.name!r}.")
        selected = self._tools[call.name]
        if not isinstance(selected, Tool) or selected.is_async:
            raise TypeError("Use ainvoke for async or remote Tools.")
        current = resolve_context(call, context, self.context)
        if current.allowed_tools is not None and call.name not in current.allowed_tools:
            return error_result(call, "tool_access_denied", "Tool execution is not permitted.")
        token = _current_context.set(current)
        try:
            try:
                self._sync_hook(self.policy, call, current)
            except ToolBusinessError as error:
                return error_result(call, error.code, str(error))
            self._sync_hook(self.hooks.before, call, current)
            result = self._validate_result(call, selected.invoke(call))
            self._sync_hook(self.hooks.after, call, result, current)
            return result
        except Exception as error:
            try:
                self._sync_hook(self.hooks.on_error, call, error, current)
            except Exception as hook_error:
                raise hook_error from error
            raise
        finally:
            _current_context.reset(token)

    @staticmethod
    def _sync_hook(callback, *arguments):
        if callback is not None:
            value = callback(*arguments)
            if inspect.isawaitable(value):
                if inspect.iscoroutine(value):
                    value.close()
                raise TypeError("Synchronous Tool dispatch requires synchronous policies/hooks.")
            if value is not None:
                raise TypeError("Tool policies and hooks must return None.")

    async def ainvoke(self, call: LLMToolCall, *, context: ToolContext | None = None) -> LLMToolResult:
        """Dispatch one call, preserving its identity even for remote adapters."""
        call = LLMToolCall.model_validate(call)
        if call.name not in self._tools:
            return error_result(call, "unknown_tool", f"Unknown Tool: {call.name!r}.")
        current = resolve_context(call, context, self.context)
        if current.allowed_tools is not None and call.name not in current.allowed_tools:
            return error_result(call, "tool_access_denied", "Tool execution is not permitted.")
        token = _current_context.set(current)
        try:
            try:
                await self._hook(self.policy, call, current)
            except ToolBusinessError as error:
                return error_result(call, error.code, str(error))
            await self._hook(self.hooks.before, call, current)
            value = await self._tools[call.name].ainvoke(call)
            result = self._validate_result(call, value)
            await self._hook(self.hooks.after, call, result, current)
            return result
        except asyncio.CancelledError:
            raise
        except Exception as error:
            try:
                await self._hook(self.hooks.on_error, call, error, current)
            except Exception as hook_error:
                raise hook_error from error
            raise
        finally:
            _current_context.reset(token)

    @staticmethod
    async def _hook(callback, *arguments):
        if callback is not None:
            value = callback(*arguments)
            if inspect.isawaitable(value):
                value = await value
            if value is not None:
                raise TypeError("Tool policies and hooks must return None; raise ToolBusinessError to deny execution.")

    @staticmethod
    def _validate_result(call, value):
        try:
            result = LLMToolResult.model_validate(value)
            if result.call_id != call.call_id:
                raise ValueError("Tool result call_id does not match the incoming call.")
        except Exception as error:
            raise ToolOutputError(
                f"Tool {call.name!r} returned an invalid observation: {error}",
                tool_name=call.name, call_id=call.call_id,
            ) from error
        return result

    def as_operator(self, *, id: str = "tools") -> Operator:
        from ..integration.tools import tool_operator

        return tool_operator(self, id=id)


__all__ = ["ToolSet"]
