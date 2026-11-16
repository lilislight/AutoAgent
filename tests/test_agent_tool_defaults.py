"""Product permissions remain outside reusable Agent Tool implementations."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from autoagent.agent import (
    ExecutionToolsConfig, FILE_OPERATION_SCHEMA, FileTools, FileToolsConfig,
    LLMToolCall, ToolContext, ToolOperation, ToolPolicy, agent_workflow,
)
from autoagent.agent_app import builtin_agent_workflow
from autoagent.agent_app.tool_defaults import execution_policy, file_defaults


class ToolDefaultsTests(unittest.TestCase):
    def test_sdk_builds_have_neutral_permissions_and_product_builds_apply_product_defaults(self):
        """Compiling the product never installs its external and command rules into SDK builds."""
        def declarations(workflow):
            return next(node for node in workflow.nodes if node.id == "start").executable.handler.__autoagent_resources__
        sdk = declarations(agent_workflow())
        self.assertFalse(sdk.entries("tool"))
        self.assertFalse(hasattr(sdk, "files"))
        self.assertFalse(hasattr(sdk, "execution"))
        self.assertEqual(FileToolsConfig().policy.default, "allow")
        handler = next(node for node in builtin_agent_workflow().nodes if node.id == "start").executable.handler
        self.assertEqual(handler.__agent_app_command_policy__, execution_policy())

    def test_file_policy_checks_only_workspace_scope(self):
        """All workspace paths are allowed and all external paths require approval."""
        policy = file_defaults().policy
        for name in (".env", ".env.example", "secret.pem", "README.md"):
            for action in FILE_OPERATION_SCHEMA["properties"]["action"]["enum"]:
                for scope, directory, expected in (("workspace", "/workspace", "allow"),
                                                    ("external", "/external", "ask")):
                    with self.subTest(name=name, action=action, scope=scope):
                        resource = f"{directory}/{name}"
                        operation = ToolOperation(action=action, resource=resource,
                            attributes={"scope": scope, "path": name if scope == "workspace" else resource})
                        decision = policy.evaluate_operation(None, ToolContext(), operation)
                        self.assertEqual(decision.action, expected)

    def test_default_file_policy_allows_local_env_transfers_and_checks_external_destination(self):
        """The bound tools apply scope to both endpoints, including protected-looking filenames."""
        with TemporaryDirectory() as directory:
            root = Path(directory) / "workspace"
            root.mkdir()
            (root / "config").mkdir()
            (root / ".env").write_text("CONFIG_VALUE", encoding="utf-8")
            tools = FileTools(root, config=file_defaults())._tools

            def transfer(operation, path, destination):
                return LLMToolCall(name="manage_files", call_id=operation, arguments=json.dumps({
                    "request": {"operation": operation, "path": path, "destination": destination},
                }))

            self.assertFalse(tools.invoke(transfer("copy", ".env", "config/.env")).is_error)
            self.assertFalse(tools.invoke(transfer("move", "config/.env", "config/renamed")).is_error)
            self.assertEqual((root / "config/renamed").read_text(encoding="utf-8"), "CONFIG_VALUE")
            decision = tools.check(transfer("copy", ".env", str(root.parent / ".env")))
            self.assertEqual(decision.action, "ask")
            self.assertEqual([item["attributes"]["scope"] for item in decision.approval_request.operations],
                ["workspace", "external"])

    def test_command_policy_uses_tool_name_and_process_request(self):
        """App defaults ask before starting a command or writing stdin."""
        policy = execution_policy()
        for name, args, expected in (
            ("execute_command", {"mode": "exec", "program": "git"}, "ask"),
            ("manage_process", {"request": {"action": "write"}}, "ask"),
            ("manage_process", {"request": {"action": "read"}}, "allow"),
        ):
            call = LLMToolCall(name=name, call_id=name, arguments=json.dumps(args))
            self.assertEqual(policy(call, ToolContext().for_call(call, args)).action, expected)
