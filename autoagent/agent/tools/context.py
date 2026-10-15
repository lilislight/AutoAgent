"""Application-owned Tool context and execution hooks, never model arguments."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True, slots=True)
class ToolContext:
    """Per-call runtime dependencies; excluded from schemas and checkpoints.

    Dependencies are borrowed from the application. allowed_tools restricts
    advertised names; it never expands an existing application's permissions.
    """

    services: Mapping[str, Any] = field(default_factory=dict, repr=False)
    user_id: str | None = None
    session_id: str | None = None
    allowed_tools: frozenset[str] | None = None
    call_id: str | None = None
    tool_name: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "services", MappingProxyType(dict(self.services)))
        if self.allowed_tools is not None:
            if isinstance(self.allowed_tools, str) or any(not isinstance(name, str) or not name for name in self.allowed_tools):
                raise TypeError("allowed_tools must be a collection of nonempty names.")
            object.__setattr__(self, "allowed_tools", frozenset(self.allowed_tools))

    def for_call(self, call) -> ToolContext:
        return replace(self, call_id=call.call_id, tool_name=call.name)

    def restrict(self, names: Collection[str]) -> ToolContext:
        """Intersect a Skill/application restriction with the current boundary."""
        if isinstance(names, str):
            raise TypeError("Tool restrictions require a collection of names.")
        allowed = frozenset(names)
        if any(not isinstance(name, str) or not name for name in allowed):
            raise TypeError("Tool restrictions require nonempty string names.")
        return replace(self, allowed_tools=allowed if self.allowed_tools is None else self.allowed_tools & allowed)

    @classmethod
    def current(cls) -> ToolContext | None:
        """Read context inside any executable, including remote adapters."""
        return _current_context.get()


def resolve_context(call, context=None, configured=None):
    inherited = ToolContext.current()
    current = context or inherited or configured or ToolContext()
    if not isinstance(current, ToolContext):
        raise TypeError("Tool execution context must be a ToolContext.")
    for boundary in (inherited, configured):
        if boundary is not None and boundary.allowed_tools is not None:
            current = current.restrict(boundary.allowed_tools)
    return current.for_call(call)


_current_context: ContextVar[ToolContext | None] = ContextVar("autoagent_tool_context", default=None)


@dataclass(frozen=True, slots=True)
class ToolHooks:
    """Sync or async callbacks; return values do not replace calls or results.

    before(call, context), after(call, result, context), and
    on_error(call, exception, context) execute in that order around dispatch.
    Callback failures propagate; error-hook failures retain the original cause.
    External cancellation propagates without calling application error hooks.
    """

    before: Callable | None = None
    after: Callable | None = None
    on_error: Callable | None = None


__all__ = ["ToolContext", "ToolHooks"]
