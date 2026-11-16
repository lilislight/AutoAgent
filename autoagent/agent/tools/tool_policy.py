"""Portable Tool permissions over validated arguments and caller-owned facts.

No policy, an empty policy, and an unmatched policy default to Allow. Matching
rules and independent operations combine as Deny > Ask > Allow, never by order.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import re
from typing import Literal

from pydantic import Field, JsonValue, PrivateAttr, field_validator, model_serializer, model_validator

from ..models._base import Model, NonEmpty
from .operations import combine_decisions
from .permissions import ToolDecision


_MISSING = object()
_ROOTS = {"args", "operation", "context"}
_MAX_DEPTH = 32
_MAX_NODES = 1024
_MAX_TEXT = 1_048_576


def _path_tokens(path: str) -> tuple[str | int, ...]:
    """Parse names, array indexes and JSON-quoted keys without evaluating code."""
    if path == "$":
        return ()
    tokens, position = [], 0
    decoder = json.JSONDecoder()
    while position < len(path):
        if path[position] == "[":
            start = position + 1
            if start < len(path) and path[start] == '"':
                try:
                    token, end = decoder.raw_decode(path, start)
                except ValueError:
                    raise ValueError("Invalid quoted field key.") from None
                if not isinstance(token, str) or end >= len(path) or path[end] != "]":
                    raise ValueError("Quoted field keys must end with ].")
                position = end + 1
            else:
                match = re.match(r"\[(0|[1-9][0-9]*)\]", path[position:])
                if match is None:
                    raise ValueError("Field indexes must be nonnegative integers.")
                token = int(match[1])
                position += match.end()
            tokens.append(token)
        else:
            match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", path[position:])
            if match is None:
                raise ValueError("Use dotted names or brackets with JSON-quoted keys.")
            tokens.append(match[0])
            position += match.end()
        if position < len(path):
            if path[position] == ".":
                position += 1
                if position == len(path) or path[position] == "[":
                    raise ValueError("A dot must be followed by a field name.")
            elif path[position] != "[":
                raise ValueError("Invalid field path separator.")
    if not tokens:
        raise ValueError("A field path cannot be empty.")
    return tuple(tokens)


def _read(root, tokens):
    for token in tokens:
        if isinstance(token, str) and isinstance(root, Mapping):
            root = root.get(token, _MISSING)
        elif isinstance(token, int) and isinstance(root, (tuple, list)) and token < len(root):
            root = root[token]
        else:
            return _MISSING
    return root


def _plain(value):
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _same(left, right):
    return json.dumps(_plain(left), sort_keys=True, allow_nan=False) == json.dumps(
        _plain(right), sort_keys=True, allow_nan=False)


def _glob(pattern):
    """Whole-path glob: * and ? exclude /; ** crosses it; **/ may be empty."""
    parts, index = [], 0
    while index < len(pattern):
        if pattern.startswith("**/", index):
            parts.append("(?:.*/)?")
            index += 3
        elif pattern.startswith("**", index):
            parts.append(".*")
            index += 2
        else:
            char = pattern[index]
            parts.append("[^/]*" if char == "*" else "[^/]" if char == "?" else re.escape(char))
            index += 1
    return "\\A" + "".join(parts) + "\\Z"


class FieldCondition(Model):
    """Compare a field; regex searches, while glob matches the entire path."""

    field: NonEmpty = Field(max_length=2048)
    op: Literal["equals", "not_equals", "in", "not_in", "exists", "prefix", "suffix",
                "contains", "glob", "regex", "lt", "lte", "gt", "gte"]
    value: JsonValue
    flags: str = ""
    _tokens: tuple = PrivateAttr(default=())
    _pattern: object = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _compile(self):
        self._tokens = _path_tokens(self.field)
        if self.flags and self.op != "regex":
            raise ValueError("flags are only supported for regex conditions.")
        if any(flag not in "ims" for flag in self.flags) or len(set(self.flags)) != len(self.flags):
            raise ValueError("Regex flags may contain i, m and s once each.")
        if self.op in {"in", "not_in"} and not isinstance(self.value, list):
            raise ValueError("in and not_in require an array value.")
        if self.op == "exists" and type(self.value) is not bool:
            raise ValueError("exists requires a boolean value.")
        if self.op in {"lt", "lte", "gt", "gte"} and type(self.value) not in (int, float):
            raise ValueError("Numeric conditions require a number.")
        if self.op in {"prefix", "suffix"} and not isinstance(self.value, (str, list)):
            raise ValueError("prefix and suffix require a string or array value.")
        if self.op in {"regex", "glob"}:
            if not isinstance(self.value, str) or len(self.value) > 4096:
                raise ValueError("Patterns must be strings of at most 4096 characters.")
            import regex
            flags = regex.DOTALL if self.op == "glob" else 0
            for flag in self.flags:
                flags |= {"i": regex.IGNORECASE, "m": regex.MULTILINE, "s": regex.DOTALL}[flag]
            try:
                self._pattern = regex.compile(_glob(self.value) if self.op == "glob" else self.value, flags)
            except regex.error:
                raise ValueError("Invalid policy pattern.") from None
        return self

    def matches(self, facts):
        actual, expected = _read(facts, self._tokens), self.value
        if self.op == "exists":
            return (actual is not _MISSING) == expected
        if actual is _MISSING:
            return False
        if self.op in {"equals", "not_equals"}:
            same = _same(actual, expected)
            return same if self.op == "equals" else not same
        if self.op in {"in", "not_in"}:
            found = any(_same(actual, item) for item in expected)
            return found if self.op == "in" else not found
        if self.op in {"glob", "regex"}:
            return isinstance(actual, str) and self._pattern.search(actual, timeout=.025) is not None
        if self.op in {"prefix", "suffix"}:
            if isinstance(actual, str) and isinstance(expected, str):
                return actual.startswith(expected) if self.op == "prefix" else actual.endswith(expected)
            if isinstance(actual, (tuple, list)) and isinstance(expected, list):
                part = actual[:len(expected)] if self.op == "prefix" else actual[len(actual)-len(expected):]
                return _same(part, expected)
            return False
        if self.op == "contains":
            if isinstance(actual, str) and isinstance(expected, str):
                return expected in actual
            return isinstance(actual, (tuple, list)) and any(_same(item, expected) for item in actual)
        if type(actual) not in (int, float):
            return False
        if self.op == "lt":
            return actual < expected
        if self.op == "lte":
            return actual <= expected
        if self.op == "gt":
            return actual > expected
        return actual >= expected


class AllCondition(Model):
    all: tuple[Condition, ...] = Field(min_length=1, max_length=256)

    @field_validator("all", mode="before")
    @classmethod
    def _sequence(cls, value):
        return tuple(value) if isinstance(value, list) else value

    def matches(self, facts):
        return all(item.matches(facts) for item in self.all)


class AnyCondition(Model):
    any: tuple[Condition, ...] = Field(min_length=1, max_length=256)

    @field_validator("any", mode="before")
    @classmethod
    def _sequence(cls, value):
        return tuple(value) if isinstance(value, list) else value

    def matches(self, facts):
        return any(item.matches(facts) for item in self.any)


class NotCondition(Model):
    model_config = {**Model.model_config, "validate_by_name": True}
    not_: Condition = Field(alias="not")

    def matches(self, facts):
        return not self.not_.matches(facts)

    @model_serializer(mode="wrap")
    def _serialize(self, handler):
        result = handler(self)
        if "not_" in result:
            result["not"] = result.pop("not_")
        return result


class ItemCondition(Model):
    """Array predicates evaluate their nested condition relative to each item."""

    field: NonEmpty = Field(max_length=2048)
    op: Literal["any_item", "all_items"]
    value: Condition
    _tokens: tuple = PrivateAttr(default=())

    @model_validator(mode="after")
    def _compile(self):
        self._tokens = _path_tokens(self.field)
        return self

    def matches(self, facts):
        actual = _read(facts, self._tokens)
        if not isinstance(actual, (tuple, list)):
            return False
        values = (self.value.matches(item) for item in actual)
        return any(values) if self.op == "any_item" else all(values)


Condition = FieldCondition | AllCondition | AnyCondition | NotCondition | ItemCondition
for _model in (AllCondition, AnyCondition, NotCondition, ItemCondition):
    _model.model_rebuild()


class ToolPolicyRule(Model):
    id: NonEmpty | None = None
    when: Condition
    decision: Literal["allow", "ask", "deny"]
    reason: str | None = Field(default=None, max_length=2000)


class ToolPolicy(Model):
    """One SDK rule language for Python, JSON and safe YAML configuration.

    Tool implementations may supply operations, but ordinary and MCP Tools need
    only validated arguments. Context exposes identity and explicit attributes;
    borrowed services and authorization internals are never field roots.
    """

    default: Literal["allow", "ask", "deny"] = "allow"
    rules: tuple[ToolPolicyRule, ...] = Field(default=(), max_length=256)

    @field_validator("rules", mode="before")
    @classmethod
    def _sequence(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def _validate_conditions(self):
        ids, count = set(), 0

        def visit(condition, *, relative=False, depth=0):
            nonlocal count
            count += 1
            if depth > _MAX_DEPTH or count > _MAX_NODES:
                raise ValueError("Policy condition nesting or size exceeds the limit.")
            if isinstance(condition, (FieldCondition, ItemCondition)):
                if not relative and (not condition._tokens or condition._tokens[0] not in _ROOTS):
                    raise ValueError("Policy fields must start with args, operation or context.")
                if not relative and condition._tokens[0] == "context":
                    if len(condition._tokens) < 2 or condition._tokens[1] not in {
                        "user_id", "session_id", "tool_name", "call_id", "attributes",
                    }:
                        raise ValueError("Context fields expose identity and attributes only.")
                if isinstance(condition, ItemCondition):
                    visit(condition.value, relative=True, depth=depth+1)
            elif isinstance(condition, NotCondition):
                visit(condition.not_, relative=relative, depth=depth+1)
            else:
                for child in condition.all if isinstance(condition, AllCondition) else condition.any:
                    visit(child, relative=relative, depth=depth+1)

        for rule in self.rules:
            if rule.id is not None:
                if rule.id in ids:
                    raise ValueError("Policy rule IDs must be unique.")
                ids.add(rule.id)
            visit(rule.when)
        return self

    @classmethod
    def from_dict(cls, value):
        return cls.model_validate(value)

    @classmethod
    def from_json(cls, text):
        if len(text) > _MAX_TEXT:
            raise ValueError("Policy configuration exceeds the size limit.")
        from .execution import _unique_object
        return cls.model_validate(json.loads(text, object_pairs_hook=_unique_object))

    @classmethod
    def from_yaml(cls, text):
        if len(text) > _MAX_TEXT:
            raise ValueError("Policy configuration exceeds the size limit.")
        import yaml

        class Loader(yaml.SafeLoader):
            def compose_node(self, parent, index):
                if self.check_event(yaml.AliasEvent):
                    raise ValueError("YAML policy aliases are not supported; use explicit conditions.")
                return super().compose_node(parent, index)

        def mapping(loader, node):
            loader.flatten_mapping(node)
            result = {}
            for key_node, value_node in node.value:
                key = loader.construct_object(key_node)
                if not isinstance(key, str) or key in result:
                    raise ValueError("YAML policy keys must be unique strings.")
                result[key] = loader.construct_object(value_node)
            return result

        Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
        try:
            value = yaml.load(text, Loader=Loader)
        except yaml.YAMLError as error:
            mark = getattr(error, "problem_mark", None)
            position = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
            raise ValueError("Invalid YAML policy" + position + ".") from None
        # Reject dates, sets, arbitrary objects and recursive YAML aliases.
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError, RecursionError):
            raise ValueError("YAML policies must contain finite JSON data.") from None
        return cls.model_validate(value)

    def to_dict(self):
        return self.model_dump(mode="json", by_alias=True)

    def to_json(self):
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def to_yaml(self):
        import yaml
        return yaml.safe_dump(self.to_dict(), sort_keys=False, allow_unicode=True)

    @property
    def version(self):
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True).encode()).hexdigest()

    def _evaluate_operation(self, call, context, operation=None):
        facts = {
            "args": context.arguments,
            "operation": operation.policy_facts() if operation is not None else _MISSING,
            "context": {
                "user_id": context.user_id, "session_id": context.session_id,
                "tool_name": context.tool_name, "call_id": context.call_id,
                "attributes": context.attributes,
            },
        }
        try:
            matched = [dict(index=index, id=rule.id, decision=rule.decision, reason=rule.reason)
                       for index, rule in enumerate(self.rules) if rule.when.matches(facts)]
        except TimeoutError:
            return ToolDecision(action="deny", reason="Policy pattern evaluation timed out."), [], False
        decision = combine_decisions(ToolDecision(action=item["decision"], reason=item["reason"]) for item in matched) if matched else ToolDecision(action=self.default)
        return decision, matched, not matched

    def evaluate_operation(self, call, context, operation=None):
        return self._evaluate_operation(call, context, operation)[0]

    def explain(self, call, context):
        """Use the actual evaluator to explain a preview without publishing private Context."""
        decisions, details = [], []
        for operation in context.operations or (None,):
            decision, matched, fallback = self._evaluate_operation(call, context, operation)
            decisions.append(decision)
            details.append(dict(operation=operation.policy_facts() if operation is not None else None,
                decision=decision.model_dump(mode="json"), matched_rules=matched, default_used=fallback))
        return dict(decision=combine_decisions(decisions).model_dump(mode="json"), operations=details)

    def __call__(self, call, context):
        return combine_decisions(self.evaluate_operation(call, context, operation)
                                 for operation in (context.operations or (None,)))


__all__ = ["ToolPolicy", "ToolPolicyRule", "FieldCondition", "AllCondition", "AnyCondition",
           "NotCondition", "ItemCondition", "Condition"]
