"""Command help, logging isolation and startup error presentation."""

from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from autoagent.cli.main import main
from autoagent.cli.logging import command_logging


class CliUsabilityTests(unittest.TestCase):
    def invoke(self, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(args)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_bare_root_and_groups_show_help_without_opening_a_project(self):
        """Missing subcommands display useful group help and exit successfully, including nested groups."""
        for args in ((), ("workflow",), ("invocation",), ("session",), ("project",)):
            with self.subTest(args=args):
                code, stdout, stderr = self.invoke(*args)
                self.assertEqual(code, 0, stderr)
                self.assertIn("usage: autoagent", stdout)
                self.assertNotIn("required: group", stderr)

    def test_server_help_only_advertises_supported_options(self):
        """Server documents logging and examples while omitting irrelevant remote and file-output flags."""
        code, stdout, stderr = self.invoke("server", "--help")
        self.assertEqual(code, 0, stderr)
        for flag in ("--access-log", "--log-level", "--port", "Examples:"):
            self.assertIn(flag, stdout)
        for flag in ("--server", "--request-timeout", "--output"):
            self.assertNotIn(flag, stdout)

    def test_usage_errors_are_actionable_and_never_write_an_output_file(self):
        """Invalid Server options cannot create success/error report files before validation."""
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "must-not-exist.json"
            for args in (("server", "--output", str(output)), ("--output", str(output), "server")):
                with self.subTest(args=args):
                    code, stdout, stderr = self.invoke(*args)
                    self.assertEqual(code, 2)
                    self.assertEqual(json.loads(stdout)["error"]["code"], "USAGE_ERROR")
                    self.assertFalse(output.exists())
            code, stdout, stderr = self.invoke("workflow", "unknown")
            self.assertEqual(code, 2)
            self.assertIn("autoagent workflow --help", stderr)
            self.assertIn("usage:", stderr)
            code, stdout, stderr = self.invoke("--request-timeout", "3", "server")
            self.assertEqual(code, 2)
            self.assertIn("requires --server", stderr)

    def test_cli_logging_honors_env_and_restores_caller_configuration(self):
        """Only explicit command execution installs handlers; default SDK INFO noise stays suppressed."""
        root = logging.getLogger()
        handlers, level = root.handlers[:], root.level
        with tempfile.TemporaryDirectory() as directory:
            options = Namespace(project=directory, group="project")
            stream = io.StringIO()
            with patch.dict(os.environ, {}, clear=True), command_logging(options, stream):
                logging.getLogger("httpx").info("HTTP Request: hidden")
                logging.getLogger("openai").warning("visible warning")
            self.assertNotIn("HTTP Request", stream.getvalue())
            self.assertIn("visible warning", stream.getvalue())
            (Path(directory) / ".env").write_text("AUTOAGENT_LOG_LEVEL=info\n")
            stream = io.StringIO()
            with patch.dict(os.environ, {}, clear=True), command_logging(Namespace(project=directory, group="project"), stream):
                logging.getLogger("autoagent").info("application info")
                logging.getLogger("httpx").info("hidden SDK info")
            self.assertIn("application info", stream.getvalue())
            self.assertNotIn("hidden SDK info", stream.getvalue())
        self.assertEqual(root.handlers, handlers)
        self.assertEqual(root.level, level)

    def test_server_defaults_show_api_requests_without_sdk_info_noise(self):
        """Server starts at INFO, SDKs stay at WARNING, and explicit quiet/debug modes override thresholds."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            stream = io.StringIO()
            with command_logging(Namespace(project=directory, group="server"), stream):
                logging.getLogger("uvicorn.error").info("server started")
                logging.getLogger("uvicorn.access").info("GET /api/v2/workflows 200")
                logging.getLogger("httpx").info("SDK request hidden")
                logging.getLogger("openai").warning("SDK warning visible")
            self.assertIn("server started", stream.getvalue())
            self.assertRegex(stream.getvalue(),
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4} INFO uvicorn\.error: server started")
            self.assertIn("GET /api/v2/workflows 200", stream.getvalue())
            self.assertIn("SDK warning visible", stream.getvalue())
            self.assertNotIn("SDK request hidden", stream.getvalue())
            stream = io.StringIO()
            with command_logging(Namespace(project=directory, group="server", log_level="error"), stream):
                logging.getLogger("uvicorn.access").info("quiet API request")
                logging.getLogger("httpx").warning("quiet SDK warning")
            self.assertEqual(stream.getvalue(), "")
            stream = io.StringIO()
            with command_logging(Namespace(project=directory, group="server", debug=True), stream):
                logging.getLogger("openai").debug("SDK debug details")
            self.assertIn("SDK debug details", stream.getvalue())

    def test_missing_host_provider_key_reports_original_error_once(self):
        """Server startup errors explain the missing setting without a Uvicorn traceback or false ready report."""
        project = Path(__file__).resolve().parents[1] / "examples/deepseek_weather_assistant"
        environment = {key: value for key, value in os.environ.items() if not key.startswith("AUTOAGENT_")}
        environment.update(AUTOAGENT_LLM_MODEL="test", AUTOAGENT_LLM_PROVIDER="deepseek")
        with tempfile.TemporaryDirectory() as directory:
            environment["AUTOAGENT_DATABASE_URL"] = "sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db")
            result = subprocess.run([sys.executable, "-m", "autoagent", "--project", str(project),
                "--no-env-file", "server", "--no-ui", "--format", "json"], env=environment,
                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("AUTOAGENT_LLM_API_KEY", json.loads(result.stdout)["error"]["message"])
        self.assertNotIn("Traceback", result.stderr)
        self.assertNotIn('"status": "ready"', result.stdout)
