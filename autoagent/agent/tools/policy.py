"""Serializable parameter and operation rules for any ToolPolicy consumer."""

import hashlib
import json
from fnmatch import fnmatchcase
from collections.abc import Mapping
from typing import Literal

from pydantic import Field, JsonValue, field_validator, model_validator

from ..models._base import Model
from .operations import combine_decisions
from .permissions import ToolDecision

_MISSING = object()


class PolicyCondition(Model):
    source: Literal["arguments", "operation", "call"] = "arguments"
    path: str = ""
    operator: Literal["equals", "not_equals", "in", "not_in", "glob", "exists", "prefix", "contains", "lt", "lte", "gt", "gte", "regex"] = "equals"
    value: JsonValue = None

    @model_validator(mode="after")
    def _valid(self):
        if self.path and not self.path.startswith("/"):
            raise ValueError("Policy field paths use JSON Pointer syntax.")
        try:
            json.dumps(self.value, allow_nan=False)
        except (TypeError, ValueError):
            raise ValueError("Policy values must be finite JSON data.") from None
        if self.operator in {"glob", "regex"} and not isinstance(self.value, str):
            raise ValueError("Pattern conditions require a string value.")
        if self.operator in {"in", "not_in"} and not isinstance(self.value, list):
            raise ValueError("Membership conditions require an array value.")
        if self.operator == "exists" and self.value is not None and not isinstance(self.value, bool):
            raise ValueError("Existence conditions require a boolean value.")
        if self.operator == "regex":
            import regex
            try:
                regex.compile(self.value)
            except regex.error:
                raise ValueError("Invalid policy regular expression.") from None
        return self

    def matches(self, arguments, operation, call):
        roots = {"arguments": arguments, "operation": operation.model_dump(mode="json") if operation is not None else _MISSING,
                 "call": call.model_dump(mode="json")}
        actual = roots[self.source]
        for token in self.path.split("/")[1:] if self.path else ():
            token = token.replace("~1", "/").replace("~0", "~")
            if isinstance(actual, Mapping):
                actual = actual.get(token, _MISSING)
            elif isinstance(actual, (list, tuple)) and token.isdecimal() and int(token) < len(actual):
                actual = actual[int(token)]
            else:
                actual = _MISSING
        if self.operator == "exists":
            return (actual is not _MISSING) == (True if self.value is None else self.value)
        if actual is _MISSING:
            return False
        expected = self.value
        def same(left, right):
            def plain(value):
                if isinstance(value, Mapping):
                    return {key:plain(item) for key, item in value.items()}
                if isinstance(value, (tuple, list)):
                    return [plain(item) for item in value]
                return value
            return json.dumps(plain(left), sort_keys=True, separators=(",", ":")) == json.dumps(plain(right), sort_keys=True, separators=(",", ":"))
        if self.operator == "equals":
            return same(actual, expected)
        if self.operator == "not_equals":
            return not same(actual, expected)
        if self.operator in {"in", "not_in"}:
            found = isinstance(expected, (tuple, list)) and any(same(actual, value) for value in expected)
            return found if self.operator == "in" else not found
        if self.operator == "glob":
            return isinstance(actual, str) and fnmatchcase(actual, expected)
        if self.operator == "regex":
            import regex
            return isinstance(actual, str) and regex.search(expected, actual, timeout=.025) is not None
        if self.operator in {"prefix", "contains"}:
            if isinstance(expected, list) and isinstance(actual, str):
                try:
                    actual = json.loads(actual)
                except ValueError:
                    return False
            if isinstance(actual, str) and isinstance(expected, str):
                return actual.startswith(expected) if self.operator == "prefix" else expected in actual
            if isinstance(actual, (tuple, list)):
                return (isinstance(expected, list) and list(actual[:len(expected)]) == expected) if self.operator == "prefix" else any(same(value, expected) for value in actual)
            return False
        if isinstance(actual, bool) or isinstance(expected, bool) or not isinstance(actual, (int, float)) or not isinstance(expected, (int, float)):
            return False
        return {"lt": actual < expected, "lte": actual <= expected, "gt": actual > expected, "gte": actual >= expected}[self.operator]


class PolicyRule(Model):
    conditions: tuple[PolicyCondition, ...] = ()
    match: Literal["all", "any"] = "all"
    decision: Literal["allow", "ask", "deny"]
    reason: str | None = None

    @field_validator("conditions", mode="before")
    @classmethod
    def _conditions(cls, value):
        return tuple(value) if isinstance(value, list) else value

    def matches(self, arguments, operation, call):
        values = (condition.matches(arguments, operation, call) for condition in self.conditions)
        return all(values) if self.match == "all" else any(values)


class DeclarativeToolPolicy(Model):
    """Last matching rule wins for each operation; every operation must be permitted."""

    default: Literal["allow", "ask", "deny"] = "ask"
    rules: tuple[PolicyRule, ...] = Field(default=(), max_length=256)

    @field_validator("rules", mode="before")
    @classmethod
    def _rules(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @property
    def version(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    def evaluate_operation(self, call, context, operation=None):
        decision = ToolDecision(action=self.default)
        try:
            for rule in self.rules:
                if rule.matches(context.arguments, operation, call):
                    decision = ToolDecision(action=rule.decision, reason=rule.reason)
        except TimeoutError:
            return ToolDecision(action="deny", reason="Policy pattern evaluation timed out.")
        return decision

    def __call__(self, call, context):
        return combine_decisions(self.evaluate_operation(call, context, operation) for operation in (context.operations or (None,)))


__all__ = ["PolicyCondition", "PolicyRule", "DeclarativeToolPolicy"]
