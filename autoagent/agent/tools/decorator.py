"""Attach Tool metadata without wrapping or replacing an ordinary function."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar, overload

from .definition import TOOL_DEFINITION_ATTRIBUTE, ToolDefinition


F = TypeVar("F", bound=Callable[..., object])


@overload
def tool(handler: F, /) -> F: ...


@overload
def tool(
    handler: None = None, /, *, name: str | None = None,
    description: str | None = None, strict: bool | None = None,
) -> Callable[[F], F]: ...


def tool(
    handler: F | None = None, /, *, name: str | None = None,
    description: str | None = None, strict: bool | None = None,
) -> F | Callable[[F], F]:
    """Advertise a typed function while retaining its normal Python call API."""
    def decorate(function: F) -> F:
        if hasattr(function, TOOL_DEFINITION_ATTRIBUTE):
            raise ValueError("Function is already an AutoAgent Tool.")
        definition = ToolDefinition.from_callable(
            function, name=name, description=description, strict=strict,
        )
        setattr(function, TOOL_DEFINITION_ATTRIBUTE, definition)
        return function

    return decorate(handler) if handler is not None else decorate


__all__ = ["tool"]
