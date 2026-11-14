"""The SDK permission language has one portable, order-independent contract."""

import itertools
import json
import unittest
from unittest.mock import Mock

from pydantic import ValidationError
import yaml

from autoagent.agent import (
    AllCondition, FieldCondition, LLMToolCall, Tool, ToolContext, ToolOperation,
    ToolPolicy, ToolPolicyRule, ToolSet,
)


def condition(field="args.value", op="equals", value=True, **options):
    return dict(field=field, op=op, value=value, **options)


def rule(when=None, decision="ask", **options):
    return dict(when=when or condition(), decision=decision, **options)


def evaluate(policy, arguments=None, *, operations=(), attributes=None):
    call = LLMToolCall(name="test", call_id="call", arguments=json.dumps(arguments or {}))
    context = ToolContext(user_id="alice", session_id="session", operations=operations,
                          attributes=attributes or {}).for_call(call, arguments or {})
    return policy(call, context)


class ToolPolicyTests(unittest.TestCase):
    def test_empty_policy_and_missing_default_allow(self):
        """Omitted policies and unmatched rules use SDK Allow unless explicitly changed."""
        def echo(value: str = "ok") -> str:
            return value
        call = LLMToolCall(name="echo", call_id="one", arguments="{}")
        for policy in (None, ToolPolicy(), ToolPolicy(rules=[rule()])):
            with self.subTest(policy=policy):
                self.assertEqual(ToolSet([Tool(echo)], policy=policy).check(call).action, "allow")
        self.assertEqual(evaluate(ToolPolicy(default="deny")).action, "deny")

    def test_all_matching_rules_combine_without_default_or_order(self):
        """A matching Allow overrides default Deny, but a matching Deny beats every rule."""
        for default in ("allow", "ask", "deny"):
            self.assertEqual(evaluate(ToolPolicy(default=default, rules=[rule(decision="allow")]), {"value": True}).action, "allow")
            for decisions in itertools.permutations(("allow", "ask", "deny")):
                policy = ToolPolicy(default=default, rules=[rule(decision=item) for item in decisions])
                self.assertEqual(evaluate(policy, {"value": True}).action, "deny")
        self.assertEqual(evaluate(ToolPolicy(rules=[rule(decision="allow"), rule()]), {"value": True}).action, "ask")

    def test_operation_decisions_are_independent_and_merge(self):
        """A rule cannot combine one operation's action with another operation's scope."""
        read = ToolOperation(action="file.read", resource="/outside/a", attributes={"scope": "external"})
        write = ToolOperation(action="file.write", resource="/inside/a", attributes={"scope": "workspace"})
        policy = ToolPolicy(rules=[rule({"all": [condition("operation.action", value="file.write"),
            condition("operation.attributes.scope", value="external")]}, "deny")])
        self.assertEqual(evaluate(policy, operations=(read, write)).action, "allow")
        outside = write.model_copy(update={"attributes": {"scope": "external"}})
        self.assertEqual(evaluate(policy, operations=(read, outside)).action, "deny")

    def test_nested_boolean_conditions_and_strict_types(self):
        """Nested all/any/not conditions do not coerce numeric, string or boolean values."""
        policy = ToolPolicy(rules=[rule({"all": [
            {"any": [condition(value=True), condition(value="yes")]},
            {"not": condition("args.exempt", value=True)},
        ]})])
        for value, expected in ((True, "ask"), ("yes", "ask"), (1, "allow"), ("true", "allow")):
            self.assertEqual(evaluate(policy, {"value": value}).action, expected)
        self.assertEqual(evaluate(policy, {"value": True, "exempt": True}).action, "allow")
        configured = ToolPolicy(rules=[ToolPolicyRule(when=AllCondition(all=(
            FieldCondition(field="args.value", op="equals", value=True),)), decision="ask")])
        self.assertEqual(evaluate(configured, {"value": True}).action, "ask")

    def test_missing_fields_and_explicit_null_are_distinct(self):
        """Missing values never match negative comparisons and null remains a present value."""
        for op, value in (("equals", None), ("not_equals", "x"), ("in", [None]), ("not_in", ["x"])):
            self.assertEqual(evaluate(ToolPolicy(rules=[rule(condition(op=op, value=value))])).action, "allow")
        self.assertEqual(evaluate(ToolPolicy(rules=[rule(condition(value=None))]), {"value": None}).action, "ask")
        for exists, args, expected in ((False, {}, "ask"), (True, {}, "allow"), (True, {"value": None}, "ask")):
            self.assertEqual(evaluate(ToolPolicy(rules=[rule(condition(op="exists", value=exists))]), args).action, expected)

    def test_paths_cover_indexes_and_quoted_json_keys_without_code(self):
        """Field lookup handles arbitrary JSON keys while rejecting expression syntax."""
        policy = ToolPolicy(rules=[rule(condition('args.headers["X.Key"][0]["a\\\"b"]', value="found"))])
        self.assertEqual(evaluate(policy, {"headers": {"X.Key": [{'a"b': "found"}]}}).action, "ask")
        for path in ("args.path.resolve()", "args..path", "args[-1]", "args[01]", "args.[0]", "args."):
            with self.subTest(path=path), self.assertRaises(ValidationError):
                ToolPolicy(rules=[rule(condition(path))])

    def test_string_array_and_numeric_operators(self):
        """Operators preserve JSON types and support argv prefixes without parsing strings."""
        cases = [
            ("prefix", ["status"], ["status", "--short"], True),
            ("prefix", ["status"], '["status"]', False),
            ("suffix", ["--short"], ["status", "--short"], True),
            ("suffix", [], ["status"], True),
            ("contains", "--force", ["push", "--force"], True),
            ("contains", True, [1], False),
            ("prefix", "src/", "src/a", True), ("suffix", ".py", "a.py", True),
            ("contains", "main", "domain", True),
            ("in", ["a", "b"], "b", True), ("not_in", ["a"], "b", True),
            ("lt", 3, 2, True), ("lte", 2, 2, True),
            ("gt", 1, 2, True), ("gte", 2, 2, True), ("gt", 0, True, False),
            ("equals", {"a": 1, "b": 2}, {"b": 2, "a": 1}, True),
        ]
        for op, expected, actual, matches in cases:
            with self.subTest(op=op, actual=actual):
                policy = ToolPolicy(rules=[rule(condition(op=op, value=expected))])
                self.assertEqual(evaluate(policy, {"value": actual}).action, "ask" if matches else "allow")

    def test_glob_respects_directory_boundaries_and_hidden_files(self):
        """Single-star globs stay in a directory and double-star also matches the root."""
        for pattern, path, expected in (
            ("*.py", "a.py", True), ("*.py", "src/a.py", False),
            ("**/.env", ".env", True), ("**/.env", "a/b/.env", True),
            ("src/**", "src/a/b", True), ("src/?.py", "src/a.py", True),
            ("*.py", "x.py\nother", False),
        ):
            with self.subTest(pattern=pattern, path=path):
                policy = ToolPolicy(rules=[rule(condition(op="glob", value=pattern))])
                self.assertEqual(evaluate(policy, {"value": path}).action, "ask" if expected else "allow")

    def test_regex_search_flags_and_timeout(self):
        """Regex searches support explicit flags and evaluation timeout cannot fall back to Allow."""
        policy = ToolPolicy(rules=[rule(condition(op="regex", value="secret", flags="i"))])
        self.assertEqual(evaluate(policy, {"value": "my SECRET value"}).action, "ask")
        policy.rules[0].when._pattern = Mock(search=Mock(side_effect=TimeoutError))
        self.assertEqual(evaluate(policy, {"value": "text"}).action, "deny")

    def test_item_conditions_use_relative_paths_and_nested_arrays(self):
        """Array predicates handle object fields, scalar roots, nested groups and empty arrays."""
        item = {"all": [condition("kind", value="file"), condition("tags", "any_item",
            condition("$", value="secret"))]}
        policy = ToolPolicy(rules=[rule(condition("args.items", "any_item", item))])
        self.assertEqual(evaluate(policy, {"items": [{"kind": "file", "tags": ["secret"]}]}).action, "ask")
        self.assertEqual(evaluate(policy, {"items": [{"kind": "file"}]}).action, "allow")
        for op, expected in (("any_item", "allow"), ("all_items", "ask")):
            self.assertEqual(evaluate(ToolPolicy(rules=[rule(condition("args.items", op,
                condition("$", value=True)))]), {"items": []}).action, expected)

    def test_no_operation_still_checks_arguments_and_context(self):
        """Black-box Tools use args/context and cannot forge trusted identity through args."""
        policy = ToolPolicy(rules=[rule(condition("context.attributes.role", value="reader"))])
        self.assertEqual(evaluate(policy, {"context": {"attributes": {"role": "reader"}}}).action, "allow")
        self.assertEqual(evaluate(policy, attributes={"role": "reader"}).action, "ask")
        absent = ToolPolicy(rules=[rule(condition("operation.action", value="file.read"))])
        self.assertEqual(evaluate(absent).action, "allow")
        for path in ("context.services", "context.authorization", "context", "os.environ", "$"):
            with self.subTest(path=path), self.assertRaises(ValidationError):
                ToolPolicy(rules=[rule(condition(path))])

    def test_json_yaml_python_roundtrip_and_schema(self):
        """All configuration formats preserve nested rules and alias keys without changing meaning."""
        config = {"rules": [rule({"not": condition("args.exempt", "exists", True)}, id="one")]}
        original = ToolPolicy.from_dict(config)
        variants = (ToolPolicy.from_json(original.to_json()), ToolPolicy.from_yaml(original.to_yaml()),
                    ToolPolicy.model_validate_json(original.model_dump_json(by_alias=True)))
        for policy in variants:
            self.assertEqual(policy.to_dict(), original.to_dict())
            self.assertEqual(policy.version, original.version)
            self.assertEqual(evaluate(policy).action, "ask")
        config["rules"][0]["decision"] = "deny"
        self.assertEqual(evaluate(original).action, "ask")
        self.assertIn("FieldCondition", ToolPolicy.model_json_schema()["$defs"])

    def test_invalid_configuration_never_becomes_an_empty_allow_policy(self):
        """Unknown fields, malformed patterns, duplicate IDs and unsafe YAML fail on load."""
        invalid = [
            {"default": "maybe"}, {"rule": []},
            {"rules": [rule({"all": []})]}, {"rules": [rule({"any": []})]},
            {"rules": [rule(condition(op="regex", value="["))]},
            {"rules": [rule(condition(op="regex", value="x", flags="z"))]},
            {"rules": [rule(condition(op="equals", value="x", flags="i"))]},
            {"rules": [rule(condition(op="exists", value="true"))]},
            {"rules": [rule(condition(op="in", value="one"))]},
            {"rules": [rule(condition(op="gt", value=True))]},
            {"rules": [rule(condition(value=float("nan")))]},
            {"rules": [rule(id="same"), rule(id="same")]},
        ]
        for config in invalid:
            with self.subTest(config=config), self.assertRaises(ValidationError):
                ToolPolicy.from_dict(config)
        for text in ('default: allow\ndefault: deny', '!!python/object/apply:os.system [echo nope]',
                     'default: 2026-01-01', 'rules: &x [*x]', 'rules: !!set {x: null}'):
            with self.subTest(text=text), self.assertRaises((ValueError, yaml.YAMLError)):
                ToolPolicy.from_yaml(text)
        with self.assertRaises(ValueError):
            ToolPolicy.from_json('{"default":"allow","default":"deny"}')

    def test_operation_attributes_support_json_and_context_is_detached(self):
        """Operation facts keep arrays and booleans while Context attributes are immutable JSON."""
        operation = ToolOperation(action="custom.action", attributes={"args": ["status"], "complete": True})
        self.assertIsNone(operation.resource)
        policy = ToolPolicy(rules=[rule(condition("operation.attributes.args", "prefix", ["status"]))])
        self.assertEqual(evaluate(policy, operations=(operation,)).action, "ask")
        original = {"roles": ["reader"]}
        context = ToolContext(attributes=original)
        original["roles"].append("admin")
        self.assertEqual(context.attributes["roles"], ("reader",))
        with self.assertRaises(TypeError):
            context.attributes["roles"] = ()
        for attributes in ({"secret": object()}, {"n": float("inf")}, {1: "x"}):
            with self.subTest(attributes=attributes), self.assertRaises((ValueError, TypeError)):
                ToolContext(attributes=attributes)
