from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from autoagent.ai.models.llm import LLMToolDefinition
from autoagent.core.operators.contract import OperatorContract


TOOL_DEFINITION_ATTRIBUTE = "__autoagent_tool_definition__"


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


def get_tool_definition(handler: Callable[..., Any]) -> ToolDefinition:
    definition = getattr(handler, TOOL_DEFINITION_ATTRIBUTE, None)
    if not isinstance(definition, ToolDefinition):
        raise TypeError(
            f"ReAct tools must be functions decorated with @tool: {handler!r}"
        )
    return definition
