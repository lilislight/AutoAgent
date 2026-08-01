from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from io import StringIO
import os
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import textwrap
import unittest
from collections.abc import Iterator
from unittest.mock import Mock, patch

from autoagent.cli import build_parser, main
from autoagent.cli.main import _serve
from autoagent.cli.settings import app_settings_from_arguments
from autoagent.core.server import ServerSettings
from autoagent.project import load_project_environment


class AutoAgentCliTests(unittest.TestCase):
    def test_serve_closes_the_complete_project_host(self) -> None:
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
            code = _serve(
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
    ) -> Iterator[Path]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow_entries = "\n\n".join(
                "[[workflows]]\n"
                f'entrypoint = "{module_name}:{entrypoint}"'
                for entrypoint in entrypoints
            )
            (root / "auto-agent.toml").write_text(
                textwrap.dedent(
                    f"""
                    schema_version = 1

                    [project]
                    name = "cli-test"
                    version = "1"

                    {workflow_entries}
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            (root / f"{module_name}.py").write_text(
                textwrap.dedent(source).strip() + "\n",
                encoding="utf-8",
            )
            original_cwd = Path.cwd()
            try:
                os.chdir(root)
                yield root
            finally:
                os.chdir(original_cwd)
                sys.modules.pop(module_name, None)


if __name__ == "__main__":
    unittest.main()
