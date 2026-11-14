"""Caller-owned identity and permission facts for SDK Agent Tool dispatch."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, replace
import inspect
from typing import Literal, Protocol, runtime_checkable

from autoagent.core.operators import run_sync

from ..models import LLMToolCall
from .context import ToolContext


@dataclass(frozen=True, slots=True)
class ToolContextRequest:
    """Trusted execution identity plus the proposal; not a model input schema."""

    call: LLMToolCall
    context: ToolContext
    phase: Literal["preflight", "approval", "execution"]
    workflow_id: str | None = None
    workflow_revision_id: str | None = None
    invocation_id: str | None = None

    @property
    def session_id(self):
        return self.context.session_id


@runtime_checkable
class ToolContextProvider(Protocol):
    """Implement get_context with a regular or async method; SDK owns no users."""

    def get_context(self, request: ToolContextRequest) -> ToolContext | Awaitable[ToolContext]: ...


async def provide_tool_context(source: ToolContext | ToolContextProvider | None,
                               request: ToolContextRequest) -> ToolContext:
    """Borrow fresh facts while preserving SDK dependencies and call boundaries."""
    base = request.context.for_call(request.call)
    if source is None:
        return base
    if isinstance(source, ToolContext):
        supplied = source
    else:
        if not isinstance(source, ToolContextProvider) or not callable(source.get_context):
            raise TypeError("Expected a ToolContext or ToolContextProvider.")
        callback = source.get_context
        supplied = callback(request) if inspect.iscoroutinefunction(callback) else await run_sync(callback, request)
        if inspect.isawaitable(supplied):
            supplied = await supplied
    if not isinstance(supplied, ToolContext):
        raise TypeError("ToolContextProvider must return a ToolContext.")
    if base.session_id is not None and supplied.session_id not in (None, base.session_id):
        raise ValueError("ToolContextProvider cannot change the execution Session.")
    allowed = base.allowed_tools
    if supplied.allowed_tools is not None:
        allowed = supplied.allowed_tools if allowed is None else allowed & supplied.allowed_tools
    return replace(base, user_id=supplied.user_id if supplied.user_id is not None else base.user_id,
                   session_id=base.session_id or supplied.session_id,
                   attributes={**base.attributes, **supplied.attributes},
                   services={**supplied.services, **base.services}, allowed_tools=allowed)


__all__ = ["ToolContextRequest", "ToolContextProvider", "provide_tool_context"]
