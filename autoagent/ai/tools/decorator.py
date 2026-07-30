from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, TypeVar, overload

from autoagent.ai.tools.definition import (
    TOOL_DEFINITION_ATTRIBUTE,
    ToolDefinition,
)
from autoagent.core.operators.contract import callable_contract


F = TypeVar("F", bound=Callable[..., Any])


@overload
def tool(handler: F, /) -> F: ...


@overload
def tool(
    handler: None = None,
    /,
    *,
    id: str | None = None,
    name: str | None = None,
    description: str | None = None,
) -> Callable[[F], F]: ...


def tool(
    handler: F | None = None,
    /,
    *,
    id: str | None = None,
    name: str | None = None,
    description: str | None = None,
) -> F | Callable[[F], F]:
    """Mark an ordinary typed function as a ReAct Tool."""

    def decorate(function: F) -> F:
        if hasattr(function, TOOL_DEFINITION_ATTRIBUTE):
            raise ValueError(f"Function is already an AutoAgent Tool: {function}")
        contract, issues = callable_contract(function)
        errors = [issue.message for issue in issues if issue.severity == "error"]
        if errors:
            raise ValueError(" ".join(errors))
        missing_types = any(
            "no concrete type annotation" in issue.message
            for issue in issues
        )
        if (
            missing_types
            or not contract.input.known
            or not contract.output.known
            or not contract.input.portable
            or not contract.output.portable
        ):
            raise ValueError(
                "Tool parameters and return value require type annotations."
            )

        resolved_name = (name or getattr(function, "__name__", "")).strip()
        if not resolved_name:
            raise ValueError("Tool name cannot be inferred; provide name explicitly.")
        resolved_id = (id or resolved_name).strip()
        if not resolved_id:
            raise ValueError("Tool id cannot be empty.")
        resolved_description = (
            description
            if description is not None
            else (inspect.getdoc(function) or "")
        ).strip()
        if not resolved_description:
            resolved_description = resolved_name.replace("_", " ")

        definition = ToolDefinition(
            id=resolved_id,
            name=resolved_name,
            description=resolved_description.split("\n\n", 1)[0],
            contract=contract,
        )
        setattr(function, TOOL_DEFINITION_ATTRIBUTE, definition)
        return function

    if handler is not None:
        return decorate(handler)
    return decorate
