"""Parameter-aware permissions shared by file, command and application Tools."""

from collections.abc import Callable
from fnmatch import fnmatchcase
import inspect
from typing import Literal

from pydantic import Field

from ..models._base import Model, NonEmpty
from .permissions import ToolDecision


class ToolOperation(Model):
    """An application-resolved operation; never accepted from model arguments."""

    action: NonEmpty
    resource: NonEmpty
    attributes: dict[str, str] = Field(default_factory=dict)


class ToolOperationRule(Model):
    """Last matching rule wins within a policy; separate boundaries intersect."""

    action: str = "*"
    resource: str = "*"
    attributes: dict[str, str] = Field(default_factory=dict)
    decision: Literal["allow", "ask", "deny"] = "ask"
    reason: str | None = None

    def matches(self, operation):
        return (fnmatchcase(operation.action, self.action) and fnmatchcase(operation.resource, self.resource)
            and all(key in operation.attributes and fnmatchcase(operation.attributes[key], pattern)
                for key, pattern in self.attributes.items()))

    def evaluate(self, operation):
        return ToolDecision(action=self.decision, reason=self.reason)


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


__all__ = ["ToolOperation", "ToolOperationRule", "ToolOperationPolicy", "combine_decisions"]
