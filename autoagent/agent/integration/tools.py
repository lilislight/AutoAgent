"""Adapt Tool execution to Core's own sync/async execution machinery."""

from __future__ import annotations

import inspect

from autoagent.core import Operator

from ..models import LLMToolCall, LLMToolResult
from ..tools import Tool, ToolSet
from ..tools.execution import error_result


def tool_operator(tool: Tool | ToolSet, *, id: str | None = None) -> Operator:
    """Keep synchronous work on Core's pool; await remote/async work on its loop.

    Core explicitly awaits awaitables returned by sync handlers. The annotation
    declares the final business output, rather than an intermediate coroutine.
    Concurrency, cancellation and Recovery remain Core concerns. Any retry or
    timeout policy belongs to the caller rather than this execution adapter.
    """
    async_hooks = isinstance(tool, ToolSet) and any(
        inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(getattr(callback, "__call__", None))
        for callback in (tool.policy, tool.hooks.before, tool.hooks.after, tool.hooks.on_error)
    )

    def invoke(call: LLMToolCall) -> LLMToolResult:
        if isinstance(tool, Tool):
            return tool.ainvoke(call) if tool.is_async else tool.invoke(call)  # type: ignore[return-value]
        call = LLMToolCall.model_validate(call)
        try:
            selected = tool[call.name]
        except KeyError:
            return error_result(call, "unknown_tool", f"Unknown Tool: {call.name!r}.")
        if isinstance(selected, Tool) and not selected.is_async and not async_hooks:
            return tool.invoke(call)
        return tool.ainvoke(call)  # type: ignore[return-value]

    return Operator(invoke, id=id or (tool.definition.name if isinstance(tool, Tool) else "tools"))


__all__ = ["tool_operator"]
