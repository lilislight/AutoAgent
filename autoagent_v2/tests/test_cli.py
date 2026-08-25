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
        """Invoke through Host and print JSON identity, status, output, and checkpoint."""

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
            self.assertEqual(document["checkpoint"]["root_session_id"], "cli-session")
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
