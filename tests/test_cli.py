from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import textwrap
import time
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

from autoagent.cli import main
from autoagent.core.app import CheckpointLoadResult, InvocationRef
from autoagent.hosting import RuntimeEventStoreError, SQLiteRuntimeStore
from autoagent.tracing import TracingDependencyError


class CliTests(unittest.TestCase):
    def test_compile_reports_revision_without_creating_runtime_storage(self) -> None:
        """Compile every project Workflow without starting a Host or database."""

        module = "cli_compile_workflow"
        with self.project(module) as root:
            output = io.StringIO()
            with redirect_stdout(output):
                code = main(["compile", "--project", str(root)])
            document = json.loads(output.getvalue())
            self.assertEqual(code, 0)
            self.assertTrue(document["ok"])
            self.assertEqual(document["workflows"][0]["workflow_id"], "cli-flow")
            self.assertIn("workflow_revision_id", document["workflows"][0])
            self.assertFalse((root / ".autoagent").exists())
        sys.modules.pop(module, None)

    def test_compile_structures_system_exit_from_user_import(self) -> None:
        """Prevent user module SystemExit from choosing the CLI process exit code."""

        module = "cli_system_exit_workflow"
        with self.project(module) as root:
            (root / f"{module}.py").write_text(
                "raise SystemExit(7)\n",
                encoding="utf-8",
            )
            error = io.StringIO()
            with redirect_stderr(error):
                code = main(["compile", "--project", str(root)])
        document = json.loads(error.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(document["error"]["code"], "HOST_CONFIGURATION_INVALID")
        self.assertEqual(
            document["error"]["diagnostics"][0]["code"],
            "WORKFLOW_MODULE_IMPORT_FAILED",
        )
        sys.modules.pop(module, None)

    def test_invoke_prints_one_stable_machine_result(self) -> None:
        """Invoke through Host and print one stable execution result."""

        module = "cli_invoke_workflow"
        with self.project(module) as root:
            output = io.StringIO()
            with patch.dict(
                os.environ,
                {"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
            ), redirect_stdout(output):
                code = main(
                    [
                        "invoke",
                        "cli-flow",
                        "--project",
                        str(root),
                        "--input",
                        '{"value":7}',
                        "--session-id",
                        "cli-session",
                    ]
                )
            document = json.loads(output.getvalue())
            self.assertEqual(code, 0)
            self.assertEqual(document["status"], "completed")
            self.assertEqual(document["session_id"], "cli-session")
            self.assertEqual(document["output"], {"value": 7})
            self.assertNotIn("checkpoint", document)
        sys.modules.pop(module, None)

    def test_invalid_json_uses_stderr_and_a_nonzero_exit(self) -> None:
        """Reject malformed command input without loading or executing a project."""

        error = io.StringIO()
        with redirect_stderr(error):
            code = main(
                [
                    "invoke",
                    "missing",
                    "--project",
                    "/does/not/matter",
                    "--input",
                    "not-json",
                ]
            )
        document = json.loads(error.getvalue())
        self.assertEqual(code, 1)
        self.assertFalse(document["ok"])
        self.assertEqual(document["error"]["code"], "VALUEERROR")

    def test_input_rejects_non_finite_json_constants(self) -> None:
        """Reject JSON NaN and Infinity extensions at the command boundary."""

        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                error = io.StringIO()
                with redirect_stderr(error):
                    code = main(
                        [
                            "invoke",
                            "missing",
                            "--project",
                            "/does/not/matter",
                            f"--input={value}",
                        ]
                    )
                self.assertEqual(code, 1)
                self.assertEqual(
                    json.loads(error.getvalue())["error"]["code"],
                    "VALUEERROR",
                )

    def test_user_stdout_is_redirected_away_from_machine_json(self) -> None:
        """Keep import and Operator prints off the command stdout channel."""

        module = "cli_printing_workflow"
        with self.project(module) as root:
            (root / f"{module}.py").write_text(
                textwrap.dedent(
                    """
                    from typing_extensions import TypedDict
                    import ctypes
                    import os
                    import subprocess
                    import sys
                    from autoagent import Node, Workflow

                    print("module-output")
                    os.write(1, b"module-fd-output\\n")

                    class Value(TypedDict):
                        value: int

                    def identity(value: Value) -> Value:
                        print("operator-output")
                        os.write(1, b"operator-fd-output\\n")
                        subprocess.run(
                            [sys.executable, "-c", "print('subprocess-output')"],
                            check=True,
                        )
                        ctypes.CDLL(None).printf(b"native-buffered-output")
                        return value

                    workflow = Workflow("cli-flow", nodes=[Node("work", identity)])
                    """
                ),
                encoding="utf-8",
            )
            output = io.StringIO()
            error = io.StringIO()
            with patch.dict(
                os.environ,
                {"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
            ), redirect_stdout(output), redirect_stderr(error):
                code = main(
                    [
                        "invoke",
                        "cli-flow",
                        "--project",
                        str(root),
                        "--input",
                        '{"value":1}',
                    ]
                )
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue())["output"], {"value": 1})
            self.assertIn("module-output", error.getvalue())
            self.assertIn("operator-output", error.getvalue())
            self.assertIn("module-fd-output", error.getvalue())
            self.assertIn("operator-fd-output", error.getvalue())
            self.assertIn("subprocess-output", error.getvalue())
            self.assertIn("native-buffered-output", error.getvalue())
        sys.modules.pop(module, None)

    def test_inherited_stdout_from_a_live_child_cannot_delay_json(self) -> None:
        """Do not wait for a subprocess that retains the redirected stdout fd."""

        module = "cli_live_child_workflow"
        with self.project(module) as root:
            (root / f"{module}.py").write_text(
                textwrap.dedent(
                    """
                    import subprocess
                    import sys
                    from typing_extensions import TypedDict
                    from autoagent import Node, Workflow

                    child = None

                    class Value(TypedDict):
                        value: int

                    def launch(value: Value) -> Value:
                        global child
                        child = subprocess.Popen(
                            [sys.executable, "-c", "import time; time.sleep(5)"]
                        )
                        return value

                    workflow = Workflow("cli-flow", nodes=[Node("work", launch)])
                    """
                ),
                encoding="utf-8",
            )
            output = io.StringIO()
            error = io.StringIO()
            started = time.monotonic()
            try:
                with patch.dict(
                    os.environ,
                    {"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
                ), redirect_stdout(output), redirect_stderr(error):
                    code = main(
                        [
                            "invoke",
                            "cli-flow",
                            "--project",
                            str(root),
                            "--input",
                            '{"value":1}',
                        ]
                    )
                elapsed = time.monotonic() - started
                self.assertEqual(code, 0)
                self.assertLess(elapsed, 1)
                self.assertEqual(
                    json.loads(output.getvalue())["output"],
                    {"value": 1},
                )
            finally:
                loaded = sys.modules.get(module)
                child = None if loaded is None else getattr(loaded, "child", None)
                if child is not None:
                    child.terminate()
                    child.wait(timeout=1)
                sys.modules.pop(module, None)

    def test_unexpected_close_failure_is_structured_command_json(self) -> None:
        """Convert an unexpected Host close failure into a stable CLI error."""

        class Result:
            session_id = "session"
            invocation_id = "invocation"
            status = "completed"
            output = None
            error = None
            waits = ()
            checkpoint = type(
                "Checkpoint",
                (),
                {
                    "id": "checkpoint",
                    "root_session_id": "session",
                    "captured_at_ns": 1,
                    "digest": "digest",
                },
            )()

        class FailingHost:
            def invoke(self, *_args: object, **_kwargs: object) -> object:
                return Result()

            def close(self) -> None:
                raise RuntimeError("close failed")

        output = io.StringIO()
        error = io.StringIO()
        with patch(
            "autoagent.cli.main.AutoAgentHost.from_project",
            return_value=FailingHost(),
        ), redirect_stdout(output), redirect_stderr(error):
            code = main(
                [
                    "invoke",
                    "workflow",
                    "--project",
                    ".",
                    "--input",
                    "null",
                ]
            )
        self.assertEqual(code, 1)
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"],
            "INTERNAL_ERROR",
        )

    def test_non_finite_operator_output_cannot_corrupt_machine_stdout(self) -> None:
        """Never emit a non-standard JSON result for a non-finite output."""

        module = "cli_non_finite_workflow"
        with self.project(module) as root:
            (root / f"{module}.py").write_text(
                textwrap.dedent(
                    """
                    from typing_extensions import TypedDict
                    from autoagent import Node, Workflow

                    class Input(TypedDict):
                        value: int

                    class Output(TypedDict):
                        value: float

                    def non_finite(value: Input) -> Output:
                        return {"value": float("nan")}

                    workflow = Workflow(
                        "cli-flow",
                        nodes=[Node("work", non_finite)],
                    )
                    """
                ),
                encoding="utf-8",
            )
            output = io.StringIO()
            error = io.StringIO()
            with patch.dict(
                os.environ,
                {"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
            ), redirect_stdout(output), redirect_stderr(error):
                code = main(
                    [
                        "invoke",
                        "cli-flow",
                        "--project",
                        str(root),
                        "--input",
                        '{"value":1}',
                    ]
                )
            self.assertEqual(code, 1)
            document = json.loads(output.getvalue())
            self.assertEqual(document["status"], "failed")
            self.assertIsNone(document["output"])
            self.assertEqual(error.getvalue(), "")
        sys.modules.pop(module, None)

    def test_unknown_workflow_preserves_the_domain_error_code(self) -> None:
        """Expose a stable Core error code instead of a class-name fallback."""

        module = "cli_unknown_workflow"
        with self.project(module) as root:
            output = io.StringIO()
            error = io.StringIO()
            with patch.dict(
                os.environ,
                {"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
            ), redirect_stdout(output), redirect_stderr(error):
                code = main(
                    [
                        "invoke",
                        "not-registered",
                        "--project",
                        str(root),
                        "--input",
                        '{"value":1}',
                    ]
                )
            self.assertEqual(code, 1)
            self.assertEqual(output.getvalue(), "")
            self.assertEqual(
                json.loads(error.getvalue())["error"]["code"],
                "WORKFLOW_NOT_REGISTERED",
            )
        sys.modules.pop(module, None)

    def test_invoke_cleanup_cannot_replace_the_execution_failure(self) -> None:
        """Preserve the primary invoke error when Host shutdown also fails."""

        class FailingHost:
            def invoke(self, *_args: object, **_kwargs: object) -> object:
                raise ValueError("invoke failed")

            def close(self) -> None:
                raise RuntimeError("close failed")

        output = io.StringIO()
        error = io.StringIO()
        with patch(
            "autoagent.cli.main.AutoAgentHost.from_project",
            return_value=FailingHost(),
        ), redirect_stdout(output), redirect_stderr(error):
            code = main(
                [
                    "invoke",
                    "workflow",
                    "--project",
                    ".",
                    "--input",
                    "null",
                ]
            )
        self.assertEqual(code, 1)
        self.assertEqual(output.getvalue(), "")
        document = json.loads(error.getvalue())
        self.assertEqual(document["error"]["code"], "VALUEERROR")
        self.assertEqual(document["error"]["message"], "invoke failed")

    def test_waiting_invocation_can_resume_in_a_later_cli_process(self) -> None:
        """Persist a Wait, restore its Root Session, and answer it in a second call."""

        module = "cli_wait_resume_workflow"
        with self.project(module) as root:
            self._write_wait_workflow(root, module)
            (root / ".env").write_text(
                "AUTOAGENT_RUNTIME_EVENT_SINK=sqlite\n"
                "AUTOAGENT_SQLITE_PATH=runtime.db\n",
                encoding="utf-8",
            )
            first_output = io.StringIO()
            second_output = io.StringIO()
            with patch.dict(os.environ, {}, clear=True):
                with redirect_stdout(first_output):
                    first_code = main(
                        [
                            "invoke",
                            "cli-flow",
                            "--project",
                            str(root),
                            "--input",
                            '{"value":1}',
                            "--session-id",
                            "cli-wait-session",
                        ]
                    )
                first = json.loads(first_output.getvalue())
                with redirect_stdout(second_output):
                    second_code = main(
                        [
                            "resume",
                            "cli-wait-session",
                            first["waits"][0]["id"],
                            "--project",
                            str(root),
                            "--response",
                            '{"value":9}',
                        ]
                    )
            second = json.loads(second_output.getvalue())
            self.assertEqual(first_code, 0)
            self.assertEqual(first["status"], "waiting")
            self.assertEqual(second_code, 0)
            self.assertEqual(second["status"], "completed")
            self.assertEqual(second["session_id"], "cli-wait-session")
            self.assertEqual(second["output"], {"value": 9})
        sys.modules.pop(module, None)

    def test_recover_restores_an_unfinished_root_session(self) -> None:
        """Restore a durable waiting Root and recover it to the same boundary."""

        module = "cli_recover_workflow"
        with self.project(module) as root:
            self._write_wait_workflow(root, module)
            (root / ".env").write_text(
                "AUTOAGENT_RUNTIME_EVENT_SINK=sqlite\n"
                "AUTOAGENT_SQLITE_PATH=runtime.db\n",
                encoding="utf-8",
            )
            invoked_output = io.StringIO()
            recovered_output = io.StringIO()
            with patch.dict(os.environ, {}, clear=True):
                with redirect_stdout(invoked_output):
                    invoked_code = main(
                        [
                            "invoke",
                            "cli-flow",
                            "--project",
                            str(root),
                            "--input",
                            '{"value":3}',
                            "--session-id",
                            "cli-recover-session",
                        ]
                    )
                with redirect_stdout(recovered_output):
                    recovered_code = main(
                        [
                            "recover",
                            "cli-recover-session",
                            "--project",
                            str(root),
                        ]
                    )
            self.assertEqual(invoked_code, 0)
            self.assertEqual(recovered_code, 0)
            recovered = json.loads(recovered_output.getvalue())
            self.assertEqual(recovered["status"], "waiting")
            self.assertEqual(recovered["session_id"], "cli-recover-session")
            self.assertEqual(len(recovered["waits"]), 1)
        sys.modules.pop(module, None)

    def test_recover_reports_a_missing_session_with_a_stable_code(self) -> None:
        """Preserve Host's not-found identity for a nonexistent Root Session."""

        module = "cli_missing_recovery_workflow"
        with self.project(module) as root:
            (root / ".env").write_text(
                "AUTOAGENT_RUNTIME_EVENT_SINK=sqlite\n"
                "AUTOAGENT_SQLITE_PATH=runtime.db\n",
                encoding="utf-8",
            )
            error = io.StringIO()
            with patch.dict(os.environ, {}, clear=True), redirect_stderr(error):
                code = main(
                    ["recover", "missing-session", "--project", str(root)]
                )
            self.assertEqual(code, 1)
            self.assertEqual(
                json.loads(error.getvalue())["error"]["code"],
                "HOST_SESSION_NOT_FOUND",
            )
        sys.modules.pop(module, None)

    def test_recover_accepts_an_independently_stored_child_session(self) -> None:
        """Restore a Child Runtime Session through the same Session API."""

        module = "cli_child_recovery_workflow"
        with self.project(module) as root:
            (root / f"{module}.py").write_text(
                textwrap.dedent(
                    """
                    from typing_extensions import TypedDict
                    from autoagent import Node, Wait, Workflow

                    class Value(TypedDict):
                        value: int

                    child = Workflow(
                        "cli-child",
                        nodes=[Node("approval", Wait(Value, Value))],
                    )
                    workflow = Workflow(
                        "cli-flow",
                        nodes=[Node("spawn", child, execution_mode="spawn")],
                    )
                    """
                ),
                encoding="utf-8",
            )
            (root / ".env").write_text(
                "AUTOAGENT_RUNTIME_EVENT_SINK=sqlite\n"
                "AUTOAGENT_SQLITE_PATH=runtime.db\n",
                encoding="utf-8",
            )
            output = io.StringIO()
            error = io.StringIO()
            with patch.dict(os.environ, {}, clear=True):
                with redirect_stdout(output):
                    invoked_code = main(
                        [
                            "invoke",
                            "cli-flow",
                            "--project",
                            str(root),
                            "--input",
                            '{"value":1}',
                            "--session-id",
                            "cli-parent-session",
                        ]
                    )
                child_session_id = json.loads(output.getvalue())["output"][
                    "session_id"
                ]
                with redirect_stderr(error):
                    recovered_code = main(
                        ["recover", child_session_id, "--project", str(root)]
                    )
            self.assertEqual(invoked_code, 0)
            self.assertEqual(recovered_code, 0)
            self.assertEqual(error.getvalue(), "")
        sys.modules.pop(module, None)

    def test_recover_rejects_a_noncurrent_restored_root(self) -> None:
        """Reject a restore result that does not identify the requested current Root."""

        class Host:
            environment: dict[str, str] = {}

            def restore_session(self, _session_id: str) -> CheckpointLoadResult:
                return CheckpointLoadResult(
                    (
                        InvocationRef(
                            session_id="other-session",
                            invocation_id="invocation",
                            workflow_id="workflow",
                            workflow_revision_id="revision",
                        ),
                    )
                )

            def close(self) -> None:
                return None

        error = io.StringIO()
        with patch(
            "autoagent.cli.main.AutoAgentHost.from_project",
            return_value=Host(),
        ), redirect_stderr(error):
            code = main(["recover", "requested-session", "--project", "."])
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"],
            "HOST_SESSION_NOT_CURRENT",
        )

    def test_compile_custom_env_file_is_visible_during_import(self) -> None:
        """Apply a selected environment file before importing Workflow code."""

        module = "cli_compile_environment_workflow"
        with self.project(module) as root:
            (root / "custom.env").write_text(
                "CLI_IMPORT_VALUE=available\n",
                encoding="utf-8",
            )
            source = (root / f"{module}.py").read_text(encoding="utf-8")
            (root / f"{module}.py").write_text(
                "import os\n"
                "if os.environ.get('CLI_IMPORT_VALUE') != 'available':\n"
                "    raise RuntimeError('missing import environment')\n"
                + source,
                encoding="utf-8",
            )
            output = io.StringIO()
            with patch.dict(os.environ, {}, clear=True), redirect_stdout(output):
                code = main(
                    [
                        "compile",
                        "--project",
                        str(root),
                        "--env-file",
                        "custom.env",
                    ]
                )
            self.assertEqual(code, 0)
            self.assertTrue(json.loads(output.getvalue())["ok"])
        sys.modules.pop(module, None)

    def test_invoke_env_file_is_visible_at_runtime_and_then_restored(self) -> None:
        """Keep the Project snapshot active for Operators and restore os.environ."""

        module = "cli_runtime_environment_workflow"
        with self.project(module) as root:
            self._write_environment_workflow(root, module)
            (root / ".env").write_text(
                "AUTOAGENT_RUNTIME_EVENT_SINK=none\nCLI_RUNTIME_VALUE=7\n",
                encoding="utf-8",
            )
            output = io.StringIO()
            with patch.dict(os.environ, {}, clear=True):
                with redirect_stdout(output):
                    code = main(
                        [
                            "invoke",
                            "cli-flow",
                            "--project",
                            str(root),
                            "--input",
                            '{"value":1}',
                        ]
                    )
                self.assertNotIn("CLI_RUNTIME_VALUE", os.environ)
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue())["output"], {"value": 7})
        sys.modules.pop(module, None)

    def test_process_environment_overrides_the_project_env_file(self) -> None:
        """Give process values precedence over matching .env assignments."""

        module = "cli_environment_precedence_workflow"
        with self.project(module) as root:
            self._write_environment_workflow(root, module)
            (root / ".env").write_text(
                "AUTOAGENT_RUNTIME_EVENT_SINK=none\nCLI_RUNTIME_VALUE=7\n",
                encoding="utf-8",
            )
            output = io.StringIO()
            with patch.dict(
                os.environ,
                {
                    "AUTOAGENT_RUNTIME_EVENT_SINK": "none",
                    "CLI_RUNTIME_VALUE": "9",
                },
                clear=True,
            ), redirect_stdout(output):
                code = main(
                    [
                        "invoke",
                        "cli-flow",
                        "--project",
                        str(root),
                        "--input",
                        '{"value":1}',
                    ]
                )
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue())["output"], {"value": 9})
        sys.modules.pop(module, None)

    def test_no_env_file_disables_project_environment_loading(self) -> None:
        """Ignore .env values when the command explicitly disables the file."""

        module = "cli_environment_disabled_workflow"
        with self.project(module) as root:
            self._write_environment_workflow(root, module)
            (root / ".env").write_text(
                "CLI_RUNTIME_VALUE=7\n",
                encoding="utf-8",
            )
            output = io.StringIO()
            with patch.dict(
                os.environ,
                {"AUTOAGENT_RUNTIME_EVENT_SINK": "none"},
                clear=True,
            ), redirect_stdout(output):
                code = main(
                    [
                        "invoke",
                        "cli-flow",
                        "--project",
                        str(root),
                        "--input",
                        '{"value":1}',
                        "--no-env-file",
                    ]
                )
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue())["output"], {"value": 11})
        sys.modules.pop(module, None)

    def test_failed_import_restores_the_original_process_environment(self) -> None:
        """Restore os.environ even when Workflow import fails inside the scope."""

        module = "cli_environment_failure_workflow"
        with self.project(module) as root:
            (root / ".env").write_text(
                "CLI_TRANSIENT_VALUE=temporary\n",
                encoding="utf-8",
            )
            (root / f"{module}.py").write_text(
                "raise RuntimeError('import failed')\n",
                encoding="utf-8",
            )
            error = io.StringIO()
            with patch.dict(os.environ, {}, clear=True):
                with redirect_stderr(error):
                    code = main(["compile", "--project", str(root)])
                self.assertNotIn("CLI_TRANSIENT_VALUE", os.environ)
            self.assertEqual(code, 1)
            self.assertEqual(
                json.loads(error.getvalue())["error"]["code"],
                "HOST_CONFIGURATION_INVALID",
            )
        sys.modules.pop(module, None)

    def test_trace_assembles_read_only_server_with_overrides(self) -> None:
        """Open the selected SQLite Store and pass explicit listen values to uvicorn."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "trace.db"
            store = SQLiteRuntimeStore(database)
            store.start()
            store.close()
            application = object()
            with patch(
                "autoagent.tracing.create_tracing_app",
                return_value=application,
            ) as create, patch("uvicorn.run") as run:
                code = main(
                    [
                        "trace",
                        "--project",
                        str(root),
                        "--database",
                        str(database),
                        "--host",
                        "127.0.0.2",
                        "--port",
                        "9876",
                    ]
                )
            self.assertEqual(code, 0)
            self.assertTrue(database.is_file())
            create.assert_called_once()
            self.assertEqual(
                create.call_args.kwargs["allowed_hosts"],
                ("127.0.0.2",),
            )
            run.assert_called_once_with(application, host="127.0.0.2", port=9876)

    def test_trace_uses_the_selected_environment_file(self) -> None:
        """Apply shared CLI environment controls to local Tracing settings."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "trace.db"
            store = SQLiteRuntimeStore(database)
            store.start()
            store.close()
            (root / "trace.env").write_text(
                "AUTOAGENT_TRACE_HOST=127.0.0.2\nAUTOAGENT_TRACE_PORT=9988\n",
                encoding="utf-8",
            )
            application = object()
            with patch(
                "autoagent.tracing.create_tracing_app",
                return_value=application,
            ), patch("uvicorn.run") as run:
                code = main(
                    [
                        "trace",
                        "--project",
                        str(root),
                        "--database",
                        str(database),
                        "--env-file",
                        "trace.env",
                    ]
                )
            self.assertEqual(code, 0)
            run.assert_called_once_with(application, host="127.0.0.2", port=9988)

    def test_trace_structures_uvicorn_startup_exit_and_closes_store(self) -> None:
        """Translate uvicorn startup exit without leaking the read-only Store."""

        class TrackingStore:
            started = False
            closed = False

            def start(self) -> None:
                self.started = True

            def close(self) -> None:
                self.closed = True

        store = TrackingStore()
        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, patch.object(
            SQLiteRuntimeStore,
            "open_read_only",
            return_value=store,
        ), patch(
            "autoagent.tracing.create_tracing_app",
            return_value=object(),
        ), patch(
            "uvicorn.run",
            side_effect=SystemExit(3),
        ), redirect_stderr(error):
            code = main(["trace", "--project", directory])
        self.assertEqual(code, 1)
        self.assertTrue(store.started)
        self.assertTrue(store.closed)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"],
            "TRACING_SERVER_FAILED",
        )

    def test_trace_does_not_create_a_missing_database(self) -> None:
        """Require an existing Store instead of creating runtime schema or data."""

        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, redirect_stderr(error):
            database = Path(directory) / "missing.db"
            code = main(
                [
                    "trace",
                    "--project",
                    directory,
                    "--database",
                    str(database),
                ]
            )
            self.assertFalse(database.exists())
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"],
            "RUNTIMEEVENTSTOREERROR",
        )

    def test_trace_rejects_explicit_zero_port(self) -> None:
        """Reject an invalid explicit port instead of replacing it with the default."""

        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, redirect_stderr(error):
            code = main(
                [
                    "trace",
                    "--project",
                    directory,
                    "--port",
                    "0",
                ]
            )
        document = json.loads(error.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(document["error"]["code"], "VALUEERROR")

    def test_trace_rejects_an_empty_explicit_host(self) -> None:
        """Treat a blank host override as invalid instead of using the default."""

        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, redirect_stderr(error):
            code = main(
                ["trace", "--project", directory, "--host", "   "]
            )
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"], "VALUEERROR"
        )

    def test_trace_rejects_a_missing_project_root(self) -> None:
        """Reject a nonexistent project instead of reading its parent settings."""

        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, redirect_stderr(error):
            missing = Path(directory) / "missing"
            code = main(["trace", "--project", str(missing)])
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"], "VALUEERROR"
        )

    def test_trace_rejects_an_empty_custom_ui_directory(self) -> None:
        """Never resolve an empty UI override to the current project directory."""

        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, redirect_stderr(error):
            code = main(
                [
                    "trace",
                    "--project",
                    directory,
                    "--ui-directory",
                    "   ",
                ]
            )
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"], "VALUEERROR"
        )

    def test_trace_requires_explicit_unauthenticated_remote_exposure(self) -> None:
        """Keep full Runtime context on loopback unless the caller opts in."""

        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, redirect_stderr(error):
            code = main(
                [
                    "trace",
                    "--project",
                    directory,
                    "--host",
                    "0.0.0.0",
                ]
            )
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"],
            "TRACING_REMOTE_UNAUTHENTICATED",
        )

    def test_trace_dependency_failure_is_structured_command_json(self) -> None:
        """Translate optional Tracing dependency failures into a CLI error."""

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "trace.db"
            store = SQLiteRuntimeStore(database)
            store.start()
            store.close()
            error = io.StringIO()
            with patch(
                "autoagent.tracing.create_tracing_app",
                side_effect=TracingDependencyError("server dependency missing"),
            ), redirect_stderr(error):
                code = main(
                    [
                        "trace",
                        "--project",
                        directory,
                        "--database",
                        str(database),
                    ]
                )
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"],
            "CLI_DEPENDENCY_MISSING",
        )

    def test_trace_closes_a_store_whose_start_fails(self) -> None:
        """Release read resources even when Store validation cannot start."""

        class FailingStore:
            closed = False

            def start(self) -> None:
                raise RuntimeEventStoreError("start failed")

            def close(self) -> None:
                self.closed = True

        store = FailingStore()
        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, patch.object(
            SQLiteRuntimeStore,
            "open_read_only",
            return_value=store,
        ), redirect_stderr(error):
            code = main(["trace", "--project", directory])
        self.assertEqual(code, 1)
        self.assertTrue(store.closed)
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"],
            "RUNTIMEEVENTSTOREERROR",
        )

    @staticmethod
    def _write_wait_workflow(root: Path, module: str) -> None:
        (root / f"{module}.py").write_text(
            textwrap.dedent(
                """
                from typing_extensions import TypedDict
                from autoagent import Node, Wait, Workflow

                class Value(TypedDict):
                    value: int

                workflow = Workflow(
                    "cli-flow",
                    nodes=[Node("approval", Wait(Value, Value))],
                )
                """
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _write_environment_workflow(root: Path, module: str) -> None:
        (root / f"{module}.py").write_text(
            textwrap.dedent(
                """
                import os
                from typing_extensions import TypedDict
                from autoagent import Node, Workflow

                class Value(TypedDict):
                    value: int

                def read_environment(_value: Value) -> Value:
                    return {"value": int(os.environ.get("CLI_RUNTIME_VALUE", "11"))}

                workflow = Workflow(
                    "cli-flow",
                    nodes=[Node("work", read_environment)],
                )
                """
            ),
            encoding="utf-8",
        )

    @staticmethod
    @contextmanager
    def project(module: str) -> Iterator[Path]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "autoagent.toml").write_text(
                textwrap.dedent(
                    f"""
                    schema_version = 1

                    [project]
                    name = "cli-test"
                    version = "1"

                    [[workflows]]
                    entrypoint = "{module}:workflow"
                    """
                ),
                encoding="utf-8",
            )
            (root / f"{module}.py").write_text(
                textwrap.dedent(
                    """
                    from typing_extensions import TypedDict
                    from autoagent import Node, Workflow

                    class Value(TypedDict):
                        value: int

                    def identity(value: Value) -> Value:
                        return value

                    workflow = Workflow("cli-flow", nodes=[Node("work", identity)])
                    """
                ),
                encoding="utf-8",
            )
            yield root


if __name__ == "__main__":
    unittest.main()
