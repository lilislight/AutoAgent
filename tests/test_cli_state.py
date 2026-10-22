"""Local LLM-facing historical state inspection, without executing project code."""

import asyncio
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from unittest import TestCase
from unittest.mock import patch

from autoagent.cli.main import main
from autoagent.host import AutoAgentHost
from tests.test_cli_execution import ProjectFixture


SOURCE = '''
from autoagent import ContextOperation, ContextPatch, OutputBindingContext
def remember(ctx: OutputBindingContext) -> ContextPatch:
    return ContextPatch(session=(ContextOperation.set('last', ctx.output),),
        invocation=(ContextOperation.set('nullable', None),))
@workflow
def remembered():
    return Workflow('cli-state', nodes=[Node('entry',echo,output_binding=remember)])
'''


class CliStateTests(ProjectFixture, TestCase):
    def setUp(self):
        self.prepare_project()
        source = self.root / (self.module + ".py")
        source.write_text(source.read_text() + SOURCE)
        self.env = patch.dict(os.environ, self.environment(), clear=True)
        self.env.start()
        async def seed():
            async with AutoAgentHost.from_project(self.root) as host:
                self.ref = await host.execute("cli-state", {"text": "historical"}, entry_node_id="entry")
                await host.join(self.ref)
                await host._persistence.flush()
                self.sequence = (await host.list_execution_events(self.ref.invocation_id, limit=100)).items[-1]["sequence"]
        with redirect_stdout(io.StringIO()):
            asyncio.run(seed())

    def tearDown(self):
        self.env.stop()
        self.cleanup_project()

    def query(self, *arguments):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main(["--project", str(self.root), "invocation", "state", self.ref.invocation_id,
                "--sequence", str(self.sequence), *arguments, "--format", "json"])
        document = out.getvalue()
        if not document and "--output" in arguments:
            document = (self.root / arguments[arguments.index("--output") + 1]).read_text()
        return code, json.loads(document), err.getvalue()

    def test_summary_and_context_read_with_broken_project_are_read_only(self):
        """The CLI reports the exact historical position and Context without importing even broken source."""
        (self.root / (self.module + ".py")).write_text('raise AssertionError("Must not import")')
        before = self.database.read_bytes()
        with patch("autoagent.host.host.Runtime", side_effect=AssertionError("No Runtime")):
            code, report, err = self.query()
            self.assertEqual(code, 0, (report, err))
            self.assertEqual(report["view"], "durable")
            self.assertEqual(report["data"]["position"]["sequence"], self.sequence)
            self.assertEqual(report["data"]["position"]["boundary"], "after")
            self.assertEqual(report["data"]["state"]["status"], "completed")
            code, report, _ = self.query("--context", "session", "--path", "/last/text")
            self.assertEqual(code, 0, report)
            self.assertEqual(report["data"]["value"], "historical")
            _, report, _ = self.query("--context", "invocation", "--path", "/nullable")
            self.assertTrue(report["data"]["available"])
            self.assertIsNone(report["data"]["value"])
            _, report, _ = self.query("--context", "invocation", "--path", "/missing")
            self.assertFalse(report["data"]["available"])
        self.assertEqual(self.database.read_bytes(), before)

    def test_output_is_full_json_and_invalid_positions_are_clear_errors(self):
        """Output files preserve the normal report and invalid sequence/path combinations fail explicitly."""
        code, report, _ = self.query("--context", "session", "--output", "state.json")
        self.assertEqual(code, 0, report)
        exported = json.loads((self.root / "state.json").read_text())
        self.assertEqual(exported["data"]["value"], {"last": {"text": "historical"}})
        for arguments in (("--sequence", "0"), ("--sequence", "9007199254740992"),
            ("--path", "/last"), ("--context", "session", "--path", "/bad~2escape")):
            code, report, _ = self.query(*arguments)
            self.assertEqual(code, 2, report)
            self.assertEqual(report["error"]["code"], "USAGE_ERROR")
        code, report, _ = self.query("--sequence", "1")
        self.assertEqual(code, 2, report)
        self.assertIn("Session", report["error"]["message"])

    def test_remote_option_is_rejected_before_any_http_request(self):
        """This first local-only query rejects --server with an actionable message rather than a fake route."""
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()), \
             patch("autoagent.cli.remote.ServerClient", side_effect=AssertionError("No HTTP")):
            code = main(["--server", "http://invalid/api/v2", "invocation", "state", self.ref.invocation_id,
                "--sequence", str(self.sequence), "--format", "json"])
        self.assertEqual(code, 2)
        report = json.loads(out.getvalue())
        self.assertEqual(report["error"]["code"], "USAGE_ERROR")
        self.assertIn("local read-only", report["error"]["message"])
