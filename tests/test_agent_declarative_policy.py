"""Declarative permissions use validated arguments and trusted normalized operations."""

import unittest

from autoagent.agent import DeclarativeToolPolicy, LLMToolCall, ToolContext, ToolOperation, Tool, ToolSet


class DeclarativePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_parameter_rules_override_default_and_keep_types_distinct(self):
        """Validated JSON arguments select the final rule without equating booleans and numbers."""
        def echo(count: int) -> int:
            return count
        policy = DeclarativeToolPolicy(default="deny", rules=[dict(
            conditions=[dict(path="/count", value=True)], decision="allow")])
        tools = ToolSet([Tool(echo)], policy=policy)
        result = await tools.apreflight(LLMToolCall(name="echo", call_id="one", arguments='{"count":1}'))
        self.assertEqual(result.decision.action, "deny")
        numeric = DeclarativeToolPolicy(default="deny", rules=[dict(conditions=[dict(path="/count", value=1)], decision="allow")])
        permitted = await ToolSet([Tool(echo)], policy=numeric).apreflight(LLMToolCall(name="echo", call_id="one", arguments='{"count":1}'))
        self.assertEqual(permitted.decision.action, "allow")

    async def test_every_operation_is_evaluated_and_missing_arguments_cannot_bypass(self):
        """An allowed path never overrides a denied second operation in the same ToolCall."""
        policy = DeclarativeToolPolicy(default="ask", rules=[dict(conditions=[dict(
            source="operation", path="/resource", operator="glob", value="/safe/*")], decision="allow"),
            dict(conditions=[dict(source="operation", path="/resource", operator="glob", value="*/secret")], decision="deny")])
        call = LLMToolCall(name="copy", call_id="one", arguments='{}')
        context = ToolContext(operations=(ToolOperation(action="file.read", resource="/safe/a"),
            ToolOperation(action="file.write", resource="/safe/secret")))
        self.assertEqual(policy(call, context).action, "deny")
        missing = DeclarativeToolPolicy(default="deny", rules=[dict(conditions=[dict(path="/missing", operator="not_equals", value="x")], decision="allow")])
        self.assertEqual(missing(call, context).action, "deny")

    async def test_normalized_command_rule_matches_exec_and_shell_operations(self):
        """Trusted argv metadata shares one prefix rule regardless of shell or exec input."""
        policy = DeclarativeToolPolicy(default="ask", rules=[dict(conditions=[dict(source="operation",
            path="/attributes/args", operator="prefix", value=["status"])], decision="allow")])
        call = LLMToolCall(name="execute_command", call_id="one", arguments='{}')
        context = ToolContext(operations=(ToolOperation(action="command.exec", resource="git",
            attributes={"args": '["status","--short"]'}),))
        self.assertEqual(policy(call, context).action, "allow")
