"""Offline CLI contracts, including real subprocess output and resource boundaries."""

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

try:
    import dotenv  # noqa: F401
    import sqlalchemy  # noqa: F401
except ModuleNotFoundError as error:
    raise unittest.SkipTest("Install autoagent[cli] to run CLI integration tests.") from error

from autoagent import Diagnostic
from autoagent.cli.main import main
from autoagent.host import AutoAgentHost


SOURCE = '''
import os
from contextlib import asynccontextmanager
from typing_extensions import TypedDict
from autoagent import Edge, Node, Workflow, TimeoutPolicy
from autoagent.host import workflow

print("project import log")
class Value(TypedDict):
    text: str
def operator(value: Value) -> Value:
    raise AssertionError("CLI must never execute an Operator")
@workflow()
def review():
    print("project factory log")
    return Workflow("review", version=os.environ.get("CLI_TEST_VERSION", "1"),
        nodes=[Node("prepare", operator, timeout=TimeoutPolicy(seconds=2)), Node("finish", operator)],
        edges=[Edge("prepare", "finish", id="route")])
@asynccontextmanager
async def lifespan(setup):
    raise AssertionError("CLI must never start project resources")
    yield
'''


class CliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.module = "cli_fixture_" + uuid4().hex
        (self.root / (self.module + ".py")).write_text(SOURCE)

    def tearDown(self):
        for name in tuple(sys.modules):
            if name == self.module or name.startswith(self.module + "."):
                del sys.modules[name]
        self.temp.cleanup()

    def invoke(self, *arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["--project", str(self.root), *arguments])
        return code, stdout.getvalue(), stderr.getvalue()

    def test_project_check_never_opens_runtime_persistence_or_lifespan(self):
        """Checking imports and constructs definitions without entering a Host or opening storage."""
        (self.root / "auto-agent.toml").write_text(f'''schema_version=1
[project]
name="cli-example"
lifespan="{self.module}:lifespan"
[[workflows]]
entrypoint="{self.module}:review"
''')
        with patch.object(AutoAgentHost, "__aenter__", side_effect=AssertionError("Host opened")), \
                patch("autoagent.host.persistence.DatabaseSink.open", side_effect=AssertionError("Database opened")):
            code, stdout, stderr = self.invoke("project", "check", "--format", "json")
        report = json.loads(stdout)
        self.assertEqual(code, 0, stderr)
        self.assertTrue(report["ok"])
        self.assertEqual(report["source"]["name"], "cli-example")
        self.assertEqual(report["data"]["summary"]["workflows"], 2)
        self.assertIn("project import log", stderr)
        self.assertIn("project factory log", stderr)
        self.assertFalse((self.root / ".autoagent").exists())

    def test_compile_exposes_contracts_mappings_and_recovery_without_python_repr(self):
        """The machine plan is the canonical compiler snapshot, with stable IDs and executable contracts."""
        code, stdout, stderr = self.invoke("workflow", "compile", "review", "--format", "json")
        document = json.loads(stdout)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(document["schema_version"], 1)
        self.assertEqual(document["command"], "workflow.compile")
        self.assertTrue(document["data"]["workflow_revision_id"].startswith("review:"))
        node = document["data"]["plan"]["nodes"][0]
        self.assertEqual(node["recovery"]["mode"], "never")
        self.assertEqual(node["timeout"]["seconds"], 2)
        self.assertEqual(node["input_contract"]["schema"]["required"], ["text"])
        self.assertNotIn("<function", stdout)
        self.assertNotIn("0x", stdout)

    def test_node_and_edge_selection_have_scoped_details_and_graph_context(self):
        """Single-object plan queries retain incident routes instead of dumping unrelated Nodes."""
        for flag, identity, field in (("--node-id", "prepare", "node"), ("--edge-id", "route", "edge")):
            with self.subTest(flag=flag):
                code, stdout, stderr = self.invoke("workflow", "compile", "review", flag, identity, "--format", "json")
                data = json.loads(stdout)["data"]
                self.assertEqual(code, 0, stderr)
                self.assertEqual(data["plan"][field]["id"], identity)
                self.assertNotIn("nodes", data["plan"])
                if field == "node":
                    self.assertEqual(data["plan"]["outgoing_edges"][0]["id"], "route")

    def test_text_plan_keeps_hooks_compact_and_command_names_readable(self):
        """The default plan identifies hooks and Commands without repeating their full JSON contracts."""
        source = SOURCE.replace('from autoagent import Edge,', 'from autoagent import Wait, InputMappingContext, Edge,')
        source = source.replace('@workflow()\ndef review():', 'def map_input(context: InputMappingContext) -> Value:\n    return {"text": "sample"}\n@workflow()\ndef review():')
        source = source.replace('Node("finish", operator)', 'Node("finish", Wait(Value, Value), input_mapping=map_input)')
        (self.root / (self.module + ".py")).write_text(source)
        code, stdout, stderr = self.invoke("workflow", "compile", "review")
        self.assertEqual(code, 0, stderr)
        self.assertIn("system_command:wait", stdout)
        self.assertNotIn("system_command:system_command:", stdout)
        self.assertIn("INPUT_MAPPING map_input", stdout)
        self.assertNotIn('"parameters"', stdout)

    def test_unexpected_failures_and_interrupts_have_explicit_exit_codes(self):
        """Command crashes and interrupts stay structured, and tracebacks require --debug."""
        for error, expected, status in ((RuntimeError("factory failed"), "COMMAND_FAILED", 3),
                                        (KeyboardInterrupt(), "INTERRUPTED", 130)):
            with self.subTest(error=type(error).__name__), patch("autoagent.cli.main.inspect_project", side_effect=error):
                code, stdout, stderr = self.invoke("project", "check", "--format", "json")
                self.assertEqual(code, status)
                self.assertEqual(json.loads(stdout)["error"]["code"], expected)
                self.assertNotIn("Traceback", stderr)
        with patch("autoagent.cli.main.inspect_project", side_effect=RuntimeError("factory failed")):
            code, stdout, stderr = self.invoke("project", "check", "--format", "json", "--debug")
        self.assertEqual(code, 3)
        self.assertFalse(json.loads(stdout)["ok"])
        self.assertIn("Traceback", stderr)

    def test_common_options_work_at_root_group_and_command_levels(self):
        """Common flags retain their values across subparser boundaries and later values win."""
        for arguments in (
            ("--format", "json", "workflow", "check", "review"),
            ("workflow", "--format", "json", "check", "review"),
            ("workflow", "check", "review", "--format", "json"),
            ("--format", "text", "workflow", "check", "review", "--format", "json"),
        ):
            with self.subTest(arguments=arguments):
                code, stdout, stderr = self.invoke(*arguments)
                self.assertEqual(code, 0, stderr)
                self.assertEqual(json.loads(stdout)["data"]["workflow_id"], "review")

    def test_env_precedence_explicit_file_and_no_env_file_restore_process(self):
        """Host .env semantics are preserved, including process precedence and cleanup after checking."""
        (self.root / ".env").write_text("CLI_TEST_VERSION=dotenv\nAUTOAGENT_DATABASE_URL=not-a-database-url\n")
        (self.root / "alternative.env").write_text("CLI_TEST_VERSION=alternative\n")
        before = dict(os.environ)
        for flags, expected in (((), "dotenv"), (("--env-file", "alternative.env"), "alternative"), (("--no-env-file",), "1")):
            with self.subTest(flags=flags), patch.dict(os.environ):
                os.environ.pop("CLI_TEST_VERSION", None)
                code, stdout, stderr = self.invoke("workflow", "compile", "review", *flags, "--format", "json")
                self.assertEqual(code, 0, stderr)
                self.assertEqual(json.loads(stdout)["data"]["workflow_version"], expected)
                self.assertNotIn("CLI_TEST_VERSION", os.environ)
        with patch.dict(os.environ, {"CLI_TEST_VERSION": "process"}):
            code, stdout, _ = self.invoke("workflow", "compile", "review", "--format", "json")
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(stdout)["data"]["workflow_version"], "process")
        self.assertEqual(dict(os.environ), before)

    def test_compile_failures_have_diagnostics_and_do_not_prevent_listing(self):
        """Invalid graph definitions return exit 1 and structured diagnostics while remaining discoverable."""
        path = self.root / (self.module + ".py")
        path.write_text(SOURCE.replace('Edge("prepare", "finish", id="route")', 'Edge("prepare", "missing", id="route")'))
        code, stdout, _ = self.invoke("workflow", "compile", "review", "--format", "json")
        document = json.loads(stdout)
        self.assertEqual(code, 1)
        self.assertFalse(document["ok"])
        self.assertIsNone(document["data"]["plan"])
        self.assertTrue(document["data"]["diagnostics"][0]["code"])
        code, stdout, _ = self.invoke("project", "check", "--format", "json")
        self.assertEqual(code, 1)
        self.assertFalse(json.loads(stdout)["data"]["workflows"][0]["ok"])

    def test_selected_workflow_check_does_not_fail_for_a_sibling_compile_error(self):
        """A selected Workflow's check status is independent of other discovered compile diagnostics."""
        with (self.root / (self.module + ".py")).open("a") as stream:
            stream.write('\n@workflow()\ndef bad(): return Workflow("bad", nodes=[])\n')
        code, stdout, _ = self.invoke("workflow", "check", "review", "--format", "json")
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(stdout)["ok"])
        code, stdout, _ = self.invoke("project", "check", "--format", "json")
        self.assertEqual(code, 1)
        self.assertFalse(json.loads(stdout)["ok"])

    def test_argument_and_selector_errors_remain_machine_readable(self):
        """Malformed commands and missing objects use exit 2 with one JSON error document."""
        cases = (
            (("workflow", "unknown"), "USAGE_ERROR"),
            (("workflow", "compile", "absent"), "WORKFLOW_NOT_FOUND"),
            (("workflow", "compile", "review", "--node-id", "missing"), "NODE_NOT_FOUND"),
            (("workflow", "compile", "review", "--edge-id", "missing"), "EDGE_NOT_FOUND"),
            (("workflow", "compile", "review", "--edge-id", "route", "--node-id", "prepare"), "USAGE_ERROR"),
            (("--env-file", "x", "project", "check", "--no-env-file"), "USAGE_ERROR"),
        )
        for arguments, expected in cases:
            with self.subTest(arguments=arguments):
                code, stdout, _ = self.invoke(*arguments, "--format", "json")
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(stdout)["error"]["code"], expected)

    def test_project_errors_and_missing_env_files_do_not_create_storage(self):
        """Discovery failures are explicit configuration errors without incidental database creation."""
        code, stdout, _ = self.invoke("project", "check", "--env-file", "missing.env", "--format", "json")
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stdout)["error"]["code"], "PROJECT_INVALID")
        self.assertFalse((self.root / ".autoagent").exists())

    def test_output_paths_use_project_root_and_atomic_replacement(self):
        """Reports written to files use the project root and leave JSON stdout clean."""
        destination = self.root / "reports" / "plan.json"
        code, stdout, stderr = self.invoke("workflow", "compile", "review", "--format", "json", "--output", "reports/plan.json")
        self.assertEqual(code, 0, stderr)
        self.assertEqual(stdout, "")
        self.assertTrue(json.loads(destination.read_text())["ok"])
        self.assertEqual(list(destination.parent.iterdir()), [destination])
        code, stdout, _ = self.invoke("workflow", "compile", "absent", "--format", "json", "--output", "reports/plan.json")
        self.assertEqual(code, 2)
        self.assertEqual(stdout, '')
        self.assertIn("error", json.loads(destination.read_text()))

    def test_output_io_errors_use_exit_three_and_keep_structured_output(self):
        """An unwritable output destination reports an I/O failure without partial result output."""
        code, stdout, _ = self.invoke("project", "check", "--format", "json", "--output", str(self.root))
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(stdout)["error"]["code"], "OUTPUT_WRITE_FAILED")

    def test_warnings_as_errors_preserves_compiler_diagnostics_and_explains_policy(self):
        """Strict checking fails on warnings without relabelling them as compiler errors."""
        with redirect_stdout(io.StringIO()):
            report = AutoAgentHost.check_project(self.root)
        result = replace(report.workflows[0], diagnostics=(Diagnostic("EXAMPLE_WARNING", "warning", "Check this definition."),))
        with patch.object(AutoAgentHost, "check_project", return_value=replace(report, workflows=(result,))):
            code, stdout, _ = self.invoke("workflow", "check", "review", "--warnings-as-errors", "--format", "json")
        value = json.loads(stdout)
        self.assertEqual(code, 1)
        self.assertTrue(value["policy"]["warnings_as_errors"])
        self.assertEqual(value["data"]["diagnostics"][0]["severity"], "warning")

    def test_help_version_and_invalid_arguments_do_not_import_project_code(self):
        """Help and usage errors run before discovery and work even for a missing project."""
        for arguments in (("--help",), ("--version",), ("workflow", "compile", "--help")):
            with self.subTest(arguments=arguments):
                code, stdout, stderr = self.invoke(*arguments)
                self.assertEqual(code, 0, stderr)
                self.assertTrue(stdout)
                self.assertNotIn("project import log", stderr)
                self.assertNotIn(self.module, sys.modules)

    def test_project_system_exit_cannot_report_false_success(self):
        """A factory calling sys.exit(0) is a command failure, unlike argparse help or version."""
        (self.root / (self.module + ".py")).write_text(SOURCE.replace('print("project factory log")', 'raise SystemExit(0)'))
        code, stdout, _ = self.invoke("project", "check", "--format", "json")
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(stdout)["error"]["code"], "COMMAND_EXITED")

    def test_subprocess_module_entrypoint_has_clean_json_and_real_exit_codes(self):
        """The actual Python entry point sends project logs to stderr and preserves exit status."""
        process = subprocess.run([sys.executable, "-m", "autoagent", "--project", str(self.root), "workflow", "compile", "review", "--format", "json"],
            capture_output=True, text=True, timeout=15)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(process.stdout)["command"], "workflow.compile")
        self.assertIn("project factory log", process.stderr)
        process = subprocess.run([sys.executable, "-m", "autoagent.cli", "--project", str(self.root), "workflow", "check", "absent", "--format", "json"],
            capture_output=True, text=True, timeout=15)
        self.assertEqual(process.returncode, 2, process.stderr)
        self.assertEqual(json.loads(process.stdout)["error"]["code"], "WORKFLOW_NOT_FOUND")

    def test_console_script_can_import_workspace_siblings_like_module_entrypoint(self):
        """Console launchers retain workspace imports when a selected project is in a subdirectory."""
        project = self.root / "project"
        project.mkdir()
        (self.root / (self.module + ".py")).unlink()
        shared = "shared_" + uuid4().hex
        (self.root / (shared + ".py")).write_text('VERSION = "workspace"\n')
        (project / (self.module + ".py")).write_text(
            SOURCE.replace('import os\n', f'import os\nfrom {shared} import VERSION\n')
            .replace('os.environ.get("CLI_TEST_VERSION", "1")', 'VERSION')
        )
        script = self.root / "bin" / "autoagent"
        script.parent.mkdir()
        script.write_text('from autoagent.cli.main import main\nraise SystemExit(main())\n')
        process = subprocess.run([sys.executable, str(script), "--project", str(project), "workflow", "check", "review", "--format", "json"],
            cwd=self.root, capture_output=True, text=True, timeout=15)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(process.stdout)["data"]["workflow_version"], "workspace")
