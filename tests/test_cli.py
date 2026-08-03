from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import textwrap
import unittest
from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from autoagent.cli import build_parser, main
from autoagent.cli.main import _server
from autoagent.cli.server_client import ServerClientError
from autoagent.cli.settings import app_settings_from_arguments
from autoagent.core.server import ServerSettings
from autoagent.project import load_project_environment


class AutoAgentCliTests(unittest.TestCase):
    def test_server_closes_the_complete_project_host(self) -> None:
        closed = False

        class Host:
            app = object()

            async def close(self) -> None:
                nonlocal closed
                closed = True

        host = Host()
        server = Mock()
        with (
            patch("autoagent.cli.main.ProjectHost", return_value=host),
            patch(
                "autoagent.cli.main.server_settings_from_arguments",
                return_value=SimpleNamespace(
                    ui_directory=None,
                    execution_enabled=True,
                    access_token=None,
                    secure_cookies=False,
                    trace_cache_size=128,
                    host="127.0.0.1",
                    port=8765,
                ),
            ),
            patch(
                "autoagent.cli.main.AutoAgentServer",
                return_value=server,
            ) as server_type,
        ):
            code = _server(
                SimpleNamespace(root=Path(".")),
                {},
                object(),
                SimpleNamespace(reload=False),
            )

        self.assertEqual(0, code)
        self.assertTrue(closed)
        server_type.assert_called_once_with(
            host.app,
            execution_enabled=True,
            access_token=None,
            secure_cookies=False,
            ui_directory=None,
            trace_cache_size=128,
            shutdown_callback=host.close,
        )
        server.run.assert_called_once_with(
            host="127.0.0.1",
            port=8765,
            reload=False,
        )

    def test_parser_has_one_canonical_output_and_response_names(self) -> None:
        parser = build_parser()

        arguments = parser.parse_args(
            [
                "invocation",
                "resume",
                "weather",
                "--session",
                "session-a",
                "--wait-key",
                "approval",
                "--response-json",
                '{"approved": true}',
            ]
        )

        self.assertEqual('{"approved": true}', arguments.response_json)
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["--format", "json", "workflow", "list"])
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["serve"])
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["server", "--file", "workflow.py"])

    def test_parser_exposes_eval_list_check_and_run(self) -> None:
        parser = build_parser()

        listed = parser.parse_args(["eval", "list"])
        checked = parser.parse_args(["eval", "check", "regression"])
        run = parser.parse_args(
            [
                "eval",
                "run",
                "regression",
                "--case",
                "eval_happy_path",
                "--max-concurrency",
                "2",
                "--store",
                "memory",
            ]
        )

        self.assertEqual("list", listed.command)
        self.assertEqual("regression", checked.suite_id)
        self.assertEqual(["eval_happy_path"], run.cases)
        self.assertEqual(2, run.max_concurrency)

    def test_eval_list_is_lazy_and_check_validates_cases(self) -> None:
        evaluation_source = """
            from autoagent.evaluation import EvalCase, Evaluation

            class EchoEvaluation(Evaluation):
                async def eval_happy_path(self, case: EvalCase) -> None:
                    pass
        """
        with self.project(
            module_name="cli_eval_workflow",
            source="""
                from autoagent import Workflow

                def echo(value: str) -> str:
                    return value

                workflow = Workflow(id="echo")
                workflow.add_node(echo, node_id="echo")
            """,
            eval_module_name="cli_echo_evaluation",
            eval_source=evaluation_source,
            eval_suites=(
                (
                    "echo_regression",
                    "echo",
                    "cli_echo_evaluation:EchoEvaluation",
                ),
            ),
        ) as root:
            list_code, listed = self.run_cli(
                "--project",
                str(root),
                "eval",
                "list",
            )
            check_code, checked = self.run_cli(
                "--project",
                str(root),
                "eval",
                "check",
                "echo_regression",
            )

        self.assertEqual(0, list_code, listed)
        self.assertIn("EVAL echo_regression", listed)
        self.assertIn("ENTRYPOINT cli_echo_evaluation:EchoEvaluation", listed)
        self.assertEqual(0, check_code, checked)
        self.assertIn("CASE eval_happy_path", checked)
        self.assertIn("EVAL_RESULT valid", checked)

    def test_eval_list_does_not_import_broken_evaluation(self) -> None:
        with self.project(
            module_name="cli_lazy_eval_workflow",
            source="""
                from autoagent import Workflow
                workflow = Workflow(id="echo")
            """,
            eval_module_name="cli_lazy_broken_evaluation",
            eval_source="raise RuntimeError('must stay lazy')",
            eval_suites=(
                (
                    "lazy",
                    "echo",
                    "cli_lazy_broken_evaluation:Evaluation",
                ),
            ),
        ) as root:
            list_code, listed = self.run_cli(
                "--project",
                str(root),
                "eval",
                "list",
            )
            check_code, checked = self.run_cli(
                "--project",
                str(root),
                "eval",
                "check",
                "lazy",
            )

        self.assertEqual(0, list_code, listed)
        self.assertEqual(2, check_code, checked)
        self.assertIn("EVAL_MODULE_IMPORT_FAILED", checked)

    def test_eval_run_executes_full_mode_and_renders_results(self) -> None:
        with self.project(
            module_name="cli_eval_run_workflow",
            source="""
                from autoagent import Workflow

                def echo(value: str) -> str:
                    return value

                workflow = Workflow(id="echo")
                workflow.add_node(echo, node_id="echo")
            """,
            eval_module_name="cli_eval_run_evaluation",
            eval_source="""
                from autoagent.evaluation import EvalCase, Evaluation, evaluators

                class EchoEvaluation(Evaluation):
                    async def eval_happy_path(self, case: EvalCase) -> None:
                        await case.invoke(
                            {"value": "hello"},
                            evaluators=(
                                evaluators.InvocationState(expected="completed"),
                                evaluators.InvocationResult(
                                    expected={"output": "hello"}
                                ),
                            ),
                        )

                    async def eval_not_selected(self, case: EvalCase) -> None:
                        await case.invoke(
                            {"value": "unused"},
                            evaluators=(
                                evaluators.InvocationState(expected="failed"),
                            ),
                        )
            """,
            eval_suites=(
                (
                    "echo_regression",
                    "echo",
                    "cli_eval_run_evaluation:EchoEvaluation",
                ),
            ),
        ) as root:
            report_path = root / "reports" / "eval.txt"
            code, output = self.run_cli(
                "--project",
                str(root),
                "--no-env-file",
                "eval",
                "run",
                "echo_regression",
                "--case",
                "eval_happy_path",
                "--store",
                "memory",
                "--report-file",
                str(report_path),
            )
            report = report_path.read_text(encoding="utf-8")

        self.assertEqual(0, code, output)
        self.assertEqual(output.strip(), report.strip())
        self.assertIn("STATUS passed", output)
        self.assertIn("CASE eval_happy_path", output)
        self.assertNotIn("CASE eval_not_selected", output)
        self.assertIn("EVALUATOR invocation_result passed", output)
        self.assertRegex(output, r"THROUGH_SEQUENCE [1-9][0-9]*")

    def test_eval_business_failure_and_evaluator_error_have_distinct_exit_codes(
        self,
    ) -> None:
        with self.project(
            module_name="cli_eval_exit_workflow",
            source="""
                from autoagent import Workflow
                workflow = Workflow(id="echo")
                workflow.add_node(lambda: "ok", node_id="echo")
            """,
            eval_module_name="cli_eval_exit_evaluation",
            eval_source="""
                from autoagent.evaluation import (
                    EvalCase,
                    Evaluation,
                    EvaluatorResult,
                    evaluators,
                )

                class BrokenEvaluator:
                    async def evaluate(self, context):
                        raise RuntimeError("judge unavailable")

                class EchoEvaluation(Evaluation):
                    async def eval_business_failure(self, case: EvalCase) -> None:
                        await case.invoke(
                            evaluators=(
                                evaluators.InvocationResult(
                                    expected={"output": "wrong"}
                                ),
                            ),
                        )

                    async def eval_evaluator_error(self, case: EvalCase) -> None:
                        await case.invoke(evaluators=(BrokenEvaluator(),))
            """,
            eval_suites=(
                (
                    "echo_regression",
                    "echo",
                    "cli_eval_exit_evaluation:EchoEvaluation",
                ),
            ),
        ) as root:
            failed_code, failed = self.run_cli(
                "--project",
                str(root),
                "--no-env-file",
                "eval",
                "run",
                "echo_regression",
                "--case",
                "eval_business_failure",
                "--store",
                "memory",
            )
            error_code, errored = self.run_cli(
                "--project",
                str(root),
                "--no-env-file",
                "eval",
                "run",
                "echo_regression",
                "--case",
                "eval_evaluator_error",
                "--store",
                "memory",
            )

        self.assertEqual(1, failed_code, failed)
        self.assertIn("RESULT failed", failed)
        self.assertEqual(2, error_code, errored)
        self.assertIn("EVALUATOR_EXECUTION_ERROR", errored)
        self.assertIn("RESULT error", errored)

    def test_cli_overrides_process_environment_and_project_env(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env").write_text(
                "\n".join(
                    (
                        "AUTOAGENT_DATABASE_URL=sqlite+aiosqlite:///from-file.db",
                        "AUTOAGENT_EXECUTOR_MAX_PARALLEL_UNITS=3",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            environment = load_project_environment(
                root,
                environ={
                    "AUTOAGENT_EXECUTOR_MAX_PARALLEL_UNITS": "4",
                },
            )
            arguments = build_parser().parse_args(
                [
                    "invocation",
                    "run",
                    "echo",
                    "--store",
                    "memory",
                    "--max-parallel-units",
                    "5",
                ]
            )
            settings = app_settings_from_arguments(arguments, environment)

        self.assertIsNone(settings.database_url)
        self.assertEqual(5, settings.executor_max_parallel_units)

    def test_project_check_does_not_require_llm_provider_secret(self) -> None:
        with self.project(
            module_name="cli_llm_workflow",
            source="""
                from autoagent import CapabilityRef, Workflow
                from autoagent.ai import LLM_CALL_CAPABILITY_ID

                workflow = Workflow(id="llm")
                workflow.add_node(
                    CapabilityRef(id=LLM_CALL_CAPABILITY_ID),
                    node_id="llm_call",
                )
            """,
        ) as root:
            code, output = self.run_cli(
                "--project",
                str(root),
                "project",
                "check",
            )

        self.assertEqual(0, code, output)
        self.assertIn("WORKFLOW llm", output)
        self.assertIn("RESULT valid", output)

    def test_workflow_list_and_check_use_manifest_ids(self) -> None:
        with self.project(
            module_name="cli_list_workflow",
            source="""
                from autoagent import Workflow

                def echo(value: str) -> str:
                    return value

                workflow = Workflow(id="echo", version="2")
                workflow.add_node(echo, node_id="echo")
            """,
        ) as root:
            list_code, listed = self.run_cli(
                "--project",
                str(root),
                "workflow",
                "list",
            )
            check_code, checked = self.run_cli(
                "--project",
                str(root),
                "workflow",
                "check",
                "echo",
            )

        self.assertEqual(0, list_code, listed)
        self.assertIn("ENTRYPOINT cli_list_workflow:workflow", listed)
        self.assertEqual(0, check_code, checked)
        self.assertIn("ENTRIES echo", checked)

    def test_workflow_preview_defaults_to_terminal_and_shares_diagnostics(
        self,
    ) -> None:
        with self.project(
            module_name="cli_preview_invalid",
            source="""
                from autoagent import Workflow

                def source() -> str:
                    return "value"

                def target(value: str) -> str:
                    return value

                workflow = Workflow(id="preview_invalid")
                workflow.add_node(source, node_id="source")
                workflow.add_node(target, node_id="target")
                workflow.add_edge(
                    "source",
                    "target",
                    condition="output.accepted == true",
                )
            """,
        ) as root:
            check_code, checked = self.run_cli(
                "--project",
                str(root),
                "workflow",
                "check",
                "preview_invalid",
            )
            preview_code, previewed = self.run_cli(
                "--project",
                str(root),
                "workflow",
                "preview",
                "preview_invalid",
            )

        self.assertEqual(1, check_code, checked)
        self.assertEqual(0, preview_code, previewed)
        self.assertIn("STATUS invalid", previewed)
        self.assertIn("FLOW", previewed)
        self.assertIn("STRING_CONDITION_UNSUPPORTED", checked)
        self.assertIn("STRING_CONDITION_UNSUPPORTED", previewed)

    def test_workflow_preview_supports_mermaid_and_json(self) -> None:
        with self.project(
            module_name="cli_preview_formats",
            source="""
                from autoagent import Workflow

                def source() -> str:
                    return "value"

                def target(value: str) -> str:
                    return value

                workflow = Workflow(id="preview_formats")
                workflow.add_node(source, node_id="source")
                workflow.add_node(target, node_id="target")
                workflow.add_edge("source", "target")
            """,
        ) as root:
            mermaid_code, mermaid = self.run_cli(
                "--project",
                str(root),
                "workflow",
                "preview",
                "preview_formats",
                "--format",
                "mermaid",
            )
            json_code, json_output = self.run_cli(
                "--project",
                str(root),
                "workflow",
                "preview",
                "preview_formats",
                "--format",
                "json",
            )

        self.assertEqual(0, mermaid_code, mermaid)
        self.assertIn("flowchart LR", mermaid)
        self.assertIn("edge_source_target", mermaid)
        self.assertEqual(0, json_code, json_output)
        document = json.loads(json_output)
        self.assertTrue(document["valid"])
        self.assertEqual("preview_formats", document["analysis"]["workflow_id"])

    def test_workflow_preview_writes_relative_output_under_project_root(self) -> None:
        with self.project(
            module_name="cli_preview_output",
            source="""
                from autoagent import Workflow

                def task() -> str:
                    return "done"

                workflow = Workflow(id="preview_output")
                workflow.add_node(task, node_id="task")
            """,
        ) as root:
            code, output = self.run_cli(
                "--project",
                str(root),
                "workflow",
                "preview",
                "preview_output",
                "--format",
                "mermaid",
                "--output",
                "generated/preview.mmd",
            )
            target = root / "generated" / "preview.mmd"
            content = target.read_text(encoding="utf-8")

        self.assertEqual(0, code, output)
        self.assertIn(f"OUTPUT {target}", output)
        self.assertIn("RESULT previewed", output)
        self.assertIn("flowchart LR", content)

    def test_workflow_check_and_preview_load_standalone_python_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow_file = root / "draft-workflow.py"
            workflow_file.write_text(
                textwrap.dedent(
                    """
                    from autoagent import Workflow

                    def task() -> str:
                        return "done"

                    class Exports:
                        pass

                    exports = Exports()
                    exports.draft = Workflow(id="standalone_preview")
                    exports.draft.add_node(task, node_id="task")
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            check_code, checked = self.run_cli(
                "workflow",
                "check",
                "--file",
                str(workflow_file),
                "--object",
                "exports.draft",
            )
            preview_code, previewed = self.run_cli(
                "workflow",
                "preview",
                "--file",
                str(workflow_file),
                "--object",
                "exports.draft",
            )

        self.assertEqual(0, check_code, checked)
        self.assertIn("WORKFLOW standalone_preview", checked)
        self.assertEqual(0, preview_code, previewed)
        self.assertIn("STATUS valid", previewed)
        self.assertIn("task [ENTRY, EXIT]", previewed)

    def test_invocation_run_executes_standalone_workflow_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow_file = root / "run-workflow.py"
            workflow_file.write_text(
                textwrap.dedent(
                    """
                    from autoagent import Workflow

                    def echo(value: str) -> str:
                        return value

                    workflow = Workflow(id="standalone_run")
                    workflow.add_node(echo, node_id="echo")
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            code, output = self.run_cli(
                "--no-env-file",
                "invocation",
                "run",
                "--file",
                str(workflow_file),
                "--input-json",
                '{"value":"hello"}',
                "--store",
                "memory",
            )

        self.assertEqual(0, code, output)
        self.assertIn("WORKFLOW standalone_run", output)
        self.assertIn("STATE completed", output)
        self.assertIn('"output": "hello"', output)

    def test_invocation_resume_executes_standalone_workflow_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_path = root / "runtime.db"
            (root / ".env").write_text(
                "AUTOAGENT_DATABASE_URL="
                f"sqlite+aiosqlite:///{database_path}\n",
                encoding="utf-8",
            )
            workflow_file = root / "wait-workflow.py"
            workflow_file.write_text(
                textwrap.dedent(
                    """
                    from autoagent import SystemCommand, Workflow

                    workflow = Workflow(id="standalone_wait")
                    workflow.add_node(SystemCommand(id="wait"), node_id="wait")
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            run_code, run_output = self.run_cli(
                "invocation",
                "run",
                "--file",
                str(workflow_file),
                "--session",
                "standalone-session",
                "--input-json",
                '{"wait_key":"approval"}',
            )
            resume_code, resume_output = self.run_cli(
                "invocation",
                "resume",
                "--file",
                str(workflow_file),
                "--session",
                "standalone-session",
                "--wait-key",
                "approval",
                "--response-json",
                '{"approved":true}',
            )

        self.assertEqual(0, run_code, run_output)
        self.assertIn("STATE waiting", run_output)
        self.assertEqual(0, resume_code, resume_output)
        self.assertIn("STATE completed", resume_output)

    def test_invocation_run_server_uses_remote_client_without_local_fallback(
        self,
    ) -> None:
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=None)
        client.submit = AsyncMock(
            return_value={
                "workflow_id": "echo",
                "workflow_revision_id": "echo:revision",
                "session_id": "session-id",
                "session_key": "session-key",
                "invocation_id": "invocation-id",
                "state": "created",
            }
        )
        client.wait_for_invocation = AsyncMock(
            return_value={
                "id": "invocation-id",
                "workflow_id": "echo",
                "state": "completed",
                "event_mode": "standard",
                "entry_node_id": "echo",
                "created_at_ms": 10,
                "updated_at_ms": 20,
                "result": {"output": "hello"},
                "error": None,
            }
        )
        client.events = AsyncMock(return_value=[])
        with self.project(
            module_name="cli_remote_workflow",
            source="""
                from autoagent import Workflow
                workflow = Workflow(id="echo")
            """,
        ) as root:
            (root / ".env").write_text(
                "AUTOAGENT_SERVER_URL=http://127.0.0.1:9999\n",
                encoding="utf-8",
            )
            with patch(
                "autoagent.cli.main.AutoAgentServerClient",
                return_value=client,
            ) as client_type:
                code, output = self.run_cli(
                    "--project",
                    str(root),
                    "invocation",
                    "run",
                    "echo",
                    "--server",
                    "--input-json",
                    '{"value":"hello"}',
                )

        self.assertEqual(0, code, output)
        self.assertIn("STATE completed", output)
        client_type.assert_called_once_with(
            "http://127.0.0.1:9999",
            access_token=None,
        )
        client.submit.assert_awaited_once()
        client.wait_for_invocation.assert_awaited_once_with(
            "invocation-id",
            timeout=None,
        )

    def test_invocation_submit_is_remote_and_returns_immediately(self) -> None:
        submitted = {
            "workflow_id": "echo",
            "workflow_revision_id": "echo:revision",
            "session_id": "session-id",
            "session_key": "session-key",
            "invocation_id": "invocation-id",
            "state": "created",
        }
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=None)
        client.submit = AsyncMock(return_value=submitted)
        with self.project(
            module_name="cli_submit_workflow",
            source="""
                from autoagent import Workflow
                workflow = Workflow(id="echo")
            """,
        ) as root, patch(
            "autoagent.cli.main.AutoAgentServerClient",
            return_value=client,
        ):
            code, output = self.run_cli(
                "--project",
                str(root),
                "invocation",
                "submit",
                "echo",
            )

        self.assertEqual(0, code, output)
        self.assertIn("RESULT submitted", output)
        self.assertIn("INVOCATION invocation-id", output)
        self.assertFalse(client.wait_for_invocation.called)

    def test_server_connection_failure_does_not_fall_back_to_local(self) -> None:
        client = MagicMock()
        client.__aenter__ = AsyncMock(
            side_effect=ServerClientError(
                "Cannot connect to AutoAgent Server at http://127.0.0.1:8765."
            )
        )
        client.__aexit__ = AsyncMock(return_value=None)
        with self.project(
            module_name="cli_missing_server_workflow",
            source="""
                from autoagent import Workflow
                workflow = Workflow(id="echo")
            """,
        ) as root, patch(
            "autoagent.cli.main.AutoAgentServerClient",
            return_value=client,
        ), patch("autoagent.cli.main.ProjectHost") as project_host:
            code, output = self.run_cli(
                "--project",
                str(root),
                "invocation",
                "run",
                "echo",
                "--server",
            )

        self.assertEqual(2, code, output)
        self.assertIn("SERVER ERROR", output)
        self.assertIn("Cannot connect", output)
        project_host.assert_not_called()

    def test_standalone_file_selector_rejects_ambiguous_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workflow_file = Path(directory) / "workflow.py"
            workflow_file.write_text(
                "from autoagent import Workflow\nworkflow = Workflow(id='draft')\n",
                encoding="utf-8",
            )
            code, output = self.run_cli(
                "workflow",
                "check",
                "manifest_id",
                "--file",
                str(workflow_file),
            )
            object_code, object_output = self.run_cli(
                "workflow",
                "check",
                "--object",
                "workflow",
            )

        self.assertEqual(2, code)
        self.assertIn("either workflow_id or --file", output)
        self.assertEqual(2, object_code)
        self.assertIn("--object requires --file", object_output)

    def test_invocation_run_uses_memory_and_tees_report(self) -> None:
        with self.project(
            module_name="cli_run_workflow",
            source="""
                from autoagent import Workflow

                def echo(value: str) -> str:
                    return value

                workflow = Workflow(id="echo")
                workflow.add_node(echo, node_id="echo")
            """,
        ) as root:
            report_path = root / "run-report.txt"
            code, output = self.run_cli(
                "--project",
                str(root),
                "--no-env-file",
                "invocation",
                "run",
                "echo",
                "--input-json",
                '{"value":"hello"}',
                "--trace",
                "--report-file",
                str(report_path),
            )

            persisted_report = report_path.read_text(encoding="utf-8")

        self.assertEqual(0, code, output)
        self.assertIn("STATE completed", output)
        self.assertIn('"output": "hello"', output)
        self.assertIn("TRACE", output)
        self.assertEqual(f"{output.strip()}\n", persisted_report)

    def test_server_url_environment_does_not_change_local_default(self) -> None:
        with self.project(
            module_name="cli_local_default_workflow",
            source="""
                from autoagent import Workflow

                def echo(value: str) -> str:
                    return value

                workflow = Workflow(id="echo")
                workflow.add_node(echo, node_id="echo")
            """,
        ) as root:
            (root / ".env").write_text(
                "AUTOAGENT_SERVER_URL=http://127.0.0.1:1\n",
                encoding="utf-8",
            )
            with patch("autoagent.cli.main.AutoAgentServerClient") as client:
                code, output = self.run_cli(
                    "--project",
                    str(root),
                    "invocation",
                    "run",
                    "echo",
                    "--store",
                    "memory",
                    "--input-json",
                    '{"value":"local"}',
                )

        self.assertEqual(0, code, output)
        self.assertIn('"output": "local"', output)
        client.assert_not_called()

    def test_invocation_run_rejects_non_object_input(self) -> None:
        with self.project(
            module_name="cli_input_workflow",
            source="""
                from autoagent import Workflow

                def echo(value: str) -> str:
                    return value

                workflow = Workflow(id="echo")
                workflow.add_node(echo, node_id="echo")
            """,
        ) as root:
            code, output = self.run_cli(
                "--project",
                str(root),
                "invocation",
                "run",
                "echo",
                "--input-json",
                '["not", "an", "object"]',
            )

        self.assertEqual(2, code)
        self.assertIn(
            "Invocation input must be a JSON object or null.",
            output,
        )

    def test_database_wait_can_resume_in_a_new_cli_process(self) -> None:
        with self.project(
            module_name="cli_wait_workflow",
            source="""
                from autoagent import SystemCommand, Workflow

                workflow = Workflow(id="approval")
                workflow.add_node(SystemCommand(id="wait"), node_id="approval")
            """,
        ) as root:
            database_path = root / "runtime.db"
            env_file = root / ".env"
            env_file.write_text(
                "AUTOAGENT_DATABASE_URL="
                f"sqlite+aiosqlite:///{database_path}\n",
                encoding="utf-8",
            )

            run_code, run_output = self.run_cli(
                "--project",
                str(root),
                "invocation",
                "run",
                "approval",
                "--session",
                "session-a",
                "--input-json",
                '{"wait_key":"approval"}',
            )
            resume_code, resume_output = self.run_cli(
                "--project",
                str(root),
                "invocation",
                "resume",
                "approval",
                "--session",
                "session-a",
                "--wait-key",
                "approval",
                "--response-json",
                '{"approved":true}',
            )

        self.assertEqual(0, run_code, run_output)
        self.assertIn("STATE waiting", run_output)
        self.assertEqual(0, resume_code, resume_output)
        self.assertIn("STATE completed", resume_output)

    def test_llm_runtime_requires_provider_environment(self) -> None:
        with self.project(
            module_name="cli_missing_provider",
            source="""
                from autoagent import CapabilityRef, Workflow
                from autoagent.ai import LLM_CALL_CAPABILITY_ID

                workflow = Workflow(id="llm")
                workflow.add_node(
                    CapabilityRef(id=LLM_CALL_CAPABILITY_ID),
                    node_id="llm_call",
                )
            """,
        ) as root:
            code, output = self.run_cli(
                "--project",
                str(root),
                "--no-env-file",
                "invocation",
                "run",
                "llm",
            )

        self.assertEqual(2, code)
        self.assertIn("AUTOAGENT_LLM_API_KEY is required", output)

    def test_non_llm_invocation_ignores_unselected_llm_provider(self) -> None:
        with self.project(
            module_name="cli_mixed_workflows",
            source="""
                from autoagent import CapabilityRef, Workflow
                from autoagent.ai import LLM_CALL_CAPABILITY_ID

                def echo(value: str) -> str:
                    return value

                workflow = Workflow(id="echo")
                workflow.add_node(echo, node_id="echo")

                llm_workflow = Workflow(id="llm")
                llm_workflow.add_node(
                    CapabilityRef(id=LLM_CALL_CAPABILITY_ID),
                    node_id="llm_call",
                )
            """,
            entrypoints=("workflow", "llm_workflow"),
        ) as root:
            code, output = self.run_cli(
                "--project",
                str(root),
                "--no-env-file",
                "invocation",
                "run",
                "echo",
                "--input-json",
                '{"value":"hello"}',
            )

        self.assertEqual(0, code, output)
        self.assertIn("STATE completed", output)

    def test_server_settings_validate_all_environment_values(self) -> None:
        settings = ServerSettings.from_env(
            env_file=None,
            environ={
                "AUTOAGENT_SERVER_HOST": "127.0.0.1",
                "AUTOAGENT_SERVER_PORT": "9000",
                "AUTOAGENT_SERVER_ACCESS_TOKEN": "secret",
                "AUTOAGENT_SERVER_SECURE_COOKIES": "true",
                "AUTOAGENT_SERVER_EXECUTION_ENABLED": "false",
                "AUTOAGENT_SERVER_UI_DIRECTORY": "./custom-ui",
                "AUTOAGENT_SERVER_TRACE_CACHE_SIZE": "64",
            },
        )

        self.assertEqual("127.0.0.1", settings.host)
        self.assertEqual(9000, settings.port)
        self.assertEqual("secret", settings.access_token)
        self.assertTrue(settings.secure_cookies)
        self.assertFalse(settings.execution_enabled)
        self.assertEqual(Path("./custom-ui"), settings.ui_directory)
        self.assertEqual(64, settings.trace_cache_size)

    def run_cli(self, *arguments: str) -> tuple[int, str]:
        output = StringIO()
        clean_environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("AUTOAGENT_")
        }
        with patch.dict(
            os.environ,
            clean_environment,
            clear=True,
        ), redirect_stdout(output):
            code = main(arguments)
        return code, output.getvalue()

    @contextmanager
    def project(
        self,
        *,
        module_name: str,
        source: str,
        entrypoints: tuple[str, ...] = ("workflow",),
        eval_module_name: str | None = None,
        eval_source: str | None = None,
        eval_suites: tuple[tuple[str, str, str], ...] = (),
    ) -> Iterator[Path]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow_entries = "\n\n".join(
                "[[workflows]]\n"
                f'entrypoint = "{module_name}:{entrypoint}"'
                for entrypoint in entrypoints
            )
            evaluation_entries = "\n\n".join(
                "[[eval_suites]]\n"
                f'id = "{suite_id}"\n'
                f'workflow_id = "{workflow_id}"\n'
                f'entrypoint = "{entrypoint}"'
                for suite_id, workflow_id, entrypoint in eval_suites
            )
            (root / "auto-agent.toml").write_text(
                textwrap.dedent(
                    f"""
                    schema_version = 1

                    [project]
                    name = "cli-test"
                    version = "1"

                    {workflow_entries}

                    {evaluation_entries}
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            (root / f"{module_name}.py").write_text(
                textwrap.dedent(source).strip() + "\n",
                encoding="utf-8",
            )
            if eval_module_name is not None and eval_source is not None:
                (root / f"{eval_module_name}.py").write_text(
                    textwrap.dedent(eval_source).strip() + "\n",
                    encoding="utf-8",
                )
            original_cwd = Path.cwd()
            try:
                os.chdir(root)
                yield root
            finally:
                os.chdir(original_cwd)
                sys.modules.pop(module_name, None)
                if eval_module_name is not None:
                    sys.modules.pop(eval_module_name, None)


if __name__ == "__main__":
    unittest.main()
