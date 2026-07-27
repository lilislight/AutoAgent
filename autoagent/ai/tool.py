from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar, overload

from autoagent.ai.llm import LLMToolDefinition
from autoagent.core.operators.contract import OperatorContract, callable_contract


F = TypeVar("F", bound=Callable[..., Any])
_TOOL_ATTRIBUTE = "__autoagent_tool_definition__"


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    """Framework metadata attached to one ordinary Python Tool function."""

    id: str
    name: str
    description: str
    contract: OperatorContract

    def llm_definition(self) -> LLMToolDefinition:
        return LLMToolDefinition(
            id=self.id,
            name=self.name,
            description=self.description,
            input_schema=self.contract.input.json_schema,
            output_schema=self.contract.output.json_schema,
        )


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
    """Mark an ordinary typed function as a ReAct Tool.

    The original function is returned unchanged. Types define schemas; the
    docstring is used only as the default human-readable description.
    """

    def decorate(function: F) -> F:
        if hasattr(function, _TOOL_ATTRIBUTE):
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
            raise ValueError("Tool parameters and return value require type annotations.")

        resolved_name = (name or getattr(function, "__name__", "")).strip()
        if not resolved_name:
            raise ValueError("Tool name cannot be inferred; provide name explicitly.")
        resolved_id = (
            id
            or f"{getattr(function, '__module__', '__main__')}."
            f"{getattr(function, '__qualname__', resolved_name)}"
        ).strip()
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
        setattr(function, _TOOL_ATTRIBUTE, definition)
        return function

    if handler is not None:
        return decorate(handler)
    return decorate


def get_tool_definition(handler: Callable[..., Any]) -> ToolDefinition:
    definition = getattr(handler, _TOOL_ATTRIBUTE, None)
    if not isinstance(definition, ToolDefinition):
        raise TypeError(
            f"ReAct tools must be functions decorated with @tool: {handler!r}"
        )
    return definition
