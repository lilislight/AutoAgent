"""Parameter-aware permissions shared by file, command and application Tools."""

from collections.abc import Callable
import inspect

from pydantic import Field, JsonValue

from ..models._base import Model, NonEmpty
from .permissions import ToolDecision


class ToolOperation(Model):
    """One Tool-resolved action/target; never accepted from model arguments.

    Action names are extensible (conventionally domain.verb). The Tool defines
    resource normalization and its JSON attributes; SDK imposes no risk category.
    A missing resource does not assert that the action has no side effects.
    """

    action: NonEmpty
    resource: NonEmpty | None = None
    attributes: dict[str, JsonValue] = Field(default_factory=dict)


def combine_decisions(decisions):
    decisions = tuple(ToolDecision.model_validate(value) for value in decisions)
    for action in ("deny", "ask"):
        matches = [value for value in decisions if value.action == action]
        if matches:
            return ToolDecision(action=action, reason="; ".join(dict.fromkeys(
                value.reason for value in matches if value.reason)) or None)
    return ToolDecision(action="allow")


class ToolOperationPolicy:
    """Adapt an operation evaluator to the existing ToolSet policy signature."""

    def __init__(self, evaluate: Callable):
        self.evaluate = evaluate
        self.version = getattr(getattr(evaluate, "__self__", None), "version", None)

    def __call__(self, call, context):
        decisions = []
        for operation in context.operations:
            value = self.evaluate(operation, context)
            if inspect.isawaitable(value):
                async def finish(first=value, remaining=context.operations[len(decisions) + 1:]):
                    completed = [*decisions, await first]
                    for item in remaining:
                        result = self.evaluate(item, context)
                        completed.append(await result if inspect.isawaitable(result) else result)
                    return combine_decisions(completed)
                return finish()
            decisions.append(value)
        return combine_decisions(decisions)


__all__ = ["ToolOperation", "ToolOperationPolicy", "combine_decisions"]
