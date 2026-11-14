"""Product permissions remain outside reusable Agent Tool implementations."""

import unittest

from autoagent.agent import CommandRule, ExecutionToolsConfig, FileToolsConfig, ToolContext, ToolOperation, agent_workflow
from autoagent.agent_app import builtin_agent_workflow
from autoagent.agent_app.tool_defaults import execution_defaults, file_defaults


class ToolDefaultsTests(unittest.TestCase):
    def test_sdk_builds_have_neutral_permissions_and_product_builds_apply_product_defaults(self):
        """Compiling the product never installs its external/secret/command rules into SDK builds."""
        def declarations(workflow):
            return next(node for node in workflow.nodes if node.id == "start").executable.handler.__autoagent_resources__
        sdk = declarations(agent_workflow())
        self.assertFalse(sdk.entries("tool"))
        self.assertFalse(hasattr(sdk, "files"))
        self.assertFalse(hasattr(sdk, "execution"))
        self.assertEqual(FileToolsConfig().policy.default, "allow")
        self.assertEqual(ExecutionToolsConfig().default_decision, "allow")
        self.assertEqual(file_defaults().policy.rules[0].decision, "ask")
        self.assertTrue(any(rule.id == "sensitive" and rule.decision == "ask" for rule in file_defaults().policy.rules))
        for action, expected in (("file.read", "allow"), ("file.write", "allow"), ("file.stat", "ask")):
            operation = ToolOperation(action=action, resource="/external/.env.example",
                attributes={"scope": "external", "path": "/external/.env.example"})
            self.assertEqual(file_defaults().policy.evaluate_operation(None, ToolContext(), operation).action, expected)
        handler = next(node for node in builtin_agent_workflow().nodes if node.id == "start").executable.handler
        self.assertEqual(handler.__agent_app_execution__.default_decision, "ask")

    def test_product_execution_defaults_preserve_explicit_custom_rules_and_decisions(self):
        """Code-supplied execution permissions override product defaults without changing generic defaults."""
        custom = ExecutionToolsConfig(default_decision="deny", rules=(CommandRule(program="git", decision="allow"),))
        selected = execution_defaults(custom)
        self.assertEqual(selected.default_decision, "deny")
        self.assertEqual(selected.rules, custom.rules)
        self.assertEqual(selected.file_policy, file_defaults())
        self.assertEqual(custom.file_policy, FileToolsConfig())
        explicit = ExecutionToolsConfig(file_policy=FileToolsConfig(policy={"default": "deny"}), process_write_decision="deny")
        self.assertEqual(execution_defaults(explicit).file_policy.policy.default, "deny")
        self.assertEqual(execution_defaults(explicit).process_write_decision, "deny")
