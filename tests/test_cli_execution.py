"""CLI execution contracts, HTTP transport, and real process lifecycle."""

import asyncio
from contextlib import AsyncExitStack, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

import httpx

from autoagent.cli import CliError
from autoagent.cli.execution import validate_options
from autoagent.cli.main import build_parser, main
from autoagent.cli.remote import ServerClient, run_remote
from autoagent.host import AutoAgentHost
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


SOURCE = '''
import asyncio
import os
from pathlib import Path
from contextlib import asynccontextmanager
from typing_extensions import TypedDict
from autoagent import Node, Spawn, Wait, Workflow
from autoagent.host import workflow
class Value(TypedDict):
    text: str
def echo(value: Value) -> Value:
    print("operator log")
    return value
async def slow(value: Value) -> Value:
    Path(os.environ["CLI_STARTED"]).touch()
    await asyncio.sleep(10)
    return value
def fail_once(value: Value) -> Value:
    marker = Path(os.environ["CLI_MARKER"])
    if not marker.exists():
        marker.touch()
        raise RuntimeError("first attempt failed")
    return value
@workflow
def simple():
    return Workflow("cli-echo", nodes=[Node("entry", echo)])
@workflow
def approval():
    return Workflow("cli-approval", nodes=[Node("ask", Wait(Value, Value))])
@workflow
def delayed():
    return Workflow("cli-slow", nodes=[Node("entry", slow)])
@workflow
def recoverable():
    return Workflow("cli-recoverable", nodes=[Node("entry", fail_once)])
@workflow
def parent():
    child = Workflow("cli-child", nodes=[Node("ask", Wait(Value, Value))])
    return Workflow("cli-parent", nodes=[Node("spawn", Spawn(child, "ask"))])
@asynccontextmanager
async def lifespan(setup):
    print("lifespan opened")
    try:
        yield
    finally:
        print("lifespan closed")
'''


class ProjectFixture:
    def prepare_project(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.module = "cli_execution_" + uuid4().hex
        self.database = self.root / "runtime.db"
        self.started = self.root / "started"
        (self.root / (self.module + ".py")).write_text(SOURCE)
        (self.root / "auto-agent.toml").write_text(f'''schema_version=1
[project]
name="cli-execution"
lifespan="{self.module}:lifespan"
''')
        (self.root / ".env").write_text(f"AUTOAGENT_DATABASE_URL=sqlite+aiosqlite:///{self.database}\nCLI_MARKER={self.root / 'attempted'}\nCLI_STARTED={self.started}\n")

    def cleanup_project(self):
        for name in tuple(sys.modules):
            if name == self.module or name.startswith(self.module + "."):
                del sys.modules[name]
        self.temp.cleanup()

    def environment(self):
        return {key: value for key, value in os.environ.items() if not key.startswith(("AUTOAGENT_", "CLI_MARKER", "CLI_STARTED"))}


class CliExecutionTests(ProjectFixture, unittest.TestCase):
    def setUp(self):
        self.prepare_project()

    def tearDown(self):
        self.cleanup_project()

    def invoke(self, *arguments, stdin=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, self.environment(), clear=True), redirect_stdout(stdout), redirect_stderr(stderr):
            if stdin is None:
                status = main(["--project", str(self.root), *arguments, "--format", "json"])
            else:
                with patch("sys.stdin", io.StringIO(stdin)):
                    status = main(["--project", str(self.root), *arguments, "--format", "json"])
        return status, json.loads(stdout.getvalue()), stderr.getvalue()

    def run_workflow(self, workflow="cli-echo", entry="entry", *arguments):
        return self.invoke("invoke", workflow, "--entry-node", entry, "--input-json", '{"text":"hello"}', *arguments)

    def test_local_invoke_executes_lifespan_and_keeps_json_stdout_clean(self):
        """A local invocation executes the project, persists its result, and closes resources before returning."""
        code, report, stderr = self.run_workflow()
        self.assertEqual(code, 0, stderr)
        self.assertEqual(report["command"], "invoke")
        self.assertEqual(report["data"]["status"], "completed")
        self.assertEqual(report["data"]["host_state"], "closed")
        self.assertFalse(report["data"]["execution_continues"])
        self.assertNotIn("output", report["data"])
        for log in ("operator log", "lifespan opened", "lifespan closed"):
            self.assertIn(log, stderr)
        with sqlite3.connect(self.database) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM checkpoints").fetchone()[0], 1)

    def test_session_creation_reuse_and_duplicate_ids_are_explicit(self):
        """Explicit new IDs cannot silently reuse Sessions, while existing mode appends a new Invocation."""
        code, first, _ = self.run_workflow("cli-echo", "entry", "--session-id", "conversation")
        self.assertEqual(code, 0)
        code, duplicate, _ = self.run_workflow("cli-echo", "entry", "--session-id", "conversation")
        self.assertEqual(code, 2)
        self.assertEqual(duplicate["error"]["code"], "session_exists")
        code, second, _ = self.run_workflow("cli-echo", "entry", "--session-mode", "existing", "--session-id", "conversation")
        self.assertEqual(code, 0)
        self.assertEqual(second["data"]["session_id"], "conversation")
        self.assertNotEqual(first["data"]["invocation_id"], second["data"]["invocation_id"])

    def test_wait_can_be_resumed_by_a_later_local_process_command(self):
        """A Wait survives Host shutdown and respond resumes the same Invocation rather than starting another."""
        code, first, _ = self.run_workflow("cli-approval", "ask")
        self.assertEqual(code, 0)
        state = first["data"]
        code, result, stderr = self.invoke("invocation", "respond", state["invocation_id"],
            "--wait-id", state["waits"][0]["wait_id"], "--input-file", "-", stdin='{"text":"approved"}')
        self.assertEqual(code, 0, stderr)
        self.assertEqual(result["data"]["invocation_id"], state["invocation_id"])
        self.assertEqual(result["data"]["status"], "completed")

    def test_child_wait_response_uses_its_exact_target_and_continues_root(self):
        """A Root-scoped CLI response can address a Child approval without confusing its Invocation identity."""
        code, first, _ = self.run_workflow("cli-parent", "spawn")
        self.assertEqual(code, 0)
        root = first["data"]
        wait = next(item for item in root["waits"] if item["kind"] == "external")
        code, result, stderr = self.invoke("invocation", "respond", root["invocation_id"],
            "--target-invocation-id", wait["invocation_id"], "--wait-id", wait["wait_id"], "--input-json", '{"text":"done"}')
        self.assertEqual(code, 0, stderr)
        self.assertEqual(result["data"]["status"], "completed")

    def test_pause_recover_and_cancel_preserve_original_invocation(self):
        """Local control operations share Host semantics and cancellation records the supplied reason."""
        _, first, _ = self.run_workflow("cli-approval", "ask")
        iid = first["data"]["invocation_id"]
        code, paused, stderr = self.invoke("invocation", "pause", iid)
        # Stored executions are interrupted, not running. The same Host
        # eligibility checks as the UI must reject a meaningless pause.
        self.assertEqual(code, 2, (paused, stderr))
        self.assertEqual(paused["error"]["code"], "action_unavailable")
        async def prepare_paused():
            async with AutoAgentHost.from_project(self.root) as host:
                await host.recover(iid)
                await host.pause(iid)
        with patch.dict(os.environ, self.environment(), clear=True):
            asyncio.run(prepare_paused())
        code, recovered, stderr = self.invoke("invocation", "recover", iid, "--request-id", "continue-approval")
        self.assertEqual(code, 0, stderr)
        self.assertEqual(recovered["data"]["control_state"], "active")
        code, cancelled, _ = self.invoke("invocation", "cancel", iid, "--reason", "CLI review stopped")
        self.assertEqual(code, 1)
        self.assertEqual(cancelled["data"]["status"], "cancelled")
        with sqlite3.connect(self.database) as db:
            self.assertIn("CLI review stopped", " ".join(row[0] for row in db.execute("SELECT payload_text FROM runtime_events")))

    def test_recover_failed_and_cancelled_executions_stays_on_original_id(self):
        """Recovery retries failed/cancelled work using the registered code and the original execution history."""
        code, first, _ = self.run_workflow("cli-recoverable")
        self.assertEqual(code, 1)
        self.assertEqual(first["data"]["error_message"], "first attempt failed")
        self.assertEqual(first["data"]["failed_nodes"][0]["node_id"], "entry")
        iid = first["data"]["invocation_id"]
        code, result, stderr = self.invoke("invocation", "recover", iid)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(result["data"]["invocation_id"], iid)
        self.assertEqual(result["data"]["status"], "completed")
        _, approval, _ = self.run_workflow("cli-approval", "ask")
        iid = approval["data"]["invocation_id"]
        self.invoke("invocation", "cancel", iid)
        code, result, stderr = self.invoke("invocation", "recover", iid)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(result["data"]["status"], "waiting")
        self.assertEqual(result["data"]["invocation_id"], iid)

    def test_bad_options_and_json_are_rejected_before_opening_database(self):
        """Invalid input sources, unsupported detached local work and invalid wait budgets cannot execute code."""
        cases = (
            ("invoke", "cli-echo", "--entry-node", "entry", "--input-json", "NaN"),
            ("invoke", "cli-echo", "--entry-node", "entry", "--input-json", "{}", "--wait", "none"),
            ("invoke", "cli-echo", "--entry-node", "entry", "--input-json", "{}", "--wait-timeout", "nan"),
            ("invoke", "cli-echo", "--entry-node", "entry", "--input-json", "{}", "--session-mode", "existing"),
            ("invoke", "cli-echo", "--input-json", "{}"),
            ("invocation", "recover", "missing", "--request-id", "a/b"),
            ("invoke", "cli-echo", "--entry-node", "entry", "--input-json", "{}", "--session-id", ""),
            ("invoke", "cli-echo", "--entry-node", "", "--input-json", "{}"),
            ("--server", "", "invoke", "cli-echo", "--entry-node", "entry", "--input-json", "{}"),
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                code, _, _ = self.invoke(*arguments)
                self.assertEqual(code, 2)
                self.assertFalse(self.database.exists())

    def test_schema_errors_include_field_paths_and_do_not_create_invocation(self):
        """Run input uses the declared Entry contract and reports field-level validation failures."""
        code, report, _ = self.invoke("invoke", "cli-echo", "--entry-node", "entry", "--input-json", '{"text":12}')
        self.assertEqual(code, 2)
        self.assertEqual(report["error"]["code"], "INVALID_INPUT")
        self.assertEqual(report["error"]["details"]["errors"][0]["path"], ["text"])
        with sqlite3.connect(self.database) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM invocations").fetchone()[0], 0)

    def test_local_timeout_persists_interruption_without_business_cancellation(self):
        """A local wait deadline closes Host and stores a recoverable graph rather than cancelling the task."""
        code, report, stderr = self.run_workflow("cli-slow", "entry", "--wait-timeout", "0.03")
        self.assertEqual(code, 4, stderr)
        self.assertEqual(report["data"]["observation"], "timeout")
        self.assertEqual(report["data"]["host_state"], "closed")
        with sqlite3.connect(self.database) as db:
            self.assertNotEqual(db.execute("SELECT status FROM invocations").fetchone()[0], "cancelled")
            self.assertGreater(db.execute("SELECT count(*) FROM checkpoints").fetchone()[0], 0)

    def test_live_writer_conflict_points_to_server_without_falling_back(self):
        """Opening another Host's store is an explicit conflict and never changes transport automatically."""
        async def check():
            async with AutoAgentHost.from_project(self.root):
                return await asyncio.to_thread(self.run_workflow)
        with patch.dict(os.environ, self.environment(), clear=True):
            code, report, _ = asyncio.run(check())
        self.assertEqual(code, 3)
        self.assertEqual(report["error"]["code"], "STORE_BUSY")
        self.assertIn("--server", report["error"]["message"])

    def test_real_local_sigint_saves_checkpoint_and_reports_known_identity(self):
        """Interrupting a real CLI process preserves its graph and reports IDs without a Cancel event."""
        with (self.root / "cli.log").open("w+") as log:
            process = subprocess.Popen([sys.executable, "-m", "autoagent", "--project", str(self.root), "invoke", "cli-slow",
                "--entry-node", "entry", "--input-json", '{"text":"hello"}', "--wait", "terminal", "--format", "json"],
                env=self.environment(), stdout=subprocess.PIPE, stderr=log, text=True)
            try:
                for _ in range(300):
                    if self.started.exists():
                        break
                    if process.poll() is not None:
                        self.fail("CLI exited before starting work")
                    time.sleep(0.01)
                self.assertTrue(self.started.exists())
                process.send_signal(signal.SIGINT)
                stdout, _ = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 130, stdout)
                error = json.loads(stdout)["error"]
                self.assertEqual(error["code"], "INTERRUPTED")
                self.assertTrue(error["details"]["invocation_id"])
                with sqlite3.connect(self.database) as db:
                    self.assertGreater(db.execute("SELECT count(*) FROM checkpoints").fetchone()[0], 0)
                    self.assertEqual(db.execute("SELECT count(*) FROM runtime_events WHERE event_name='invocation.cancelled'").fetchone()[0], 0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()


class CliRemoteTests(ProjectFixture, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.prepare_project()
        self.environment_patch = patch.dict(os.environ, self.environment(), clear=True)
        self.environment_patch.start()
        self.stack = AsyncExitStack()
        self.host = await self.stack.enter_async_context(AutoAgentHost.from_project(self.root, environment=self.environment()))
        self.app = create_app(self.host, ui=False)
        await self.stack.enter_async_context(self.app.router.lifespan_context(self.app))
        self.transport = httpx.ASGITransport(app=self.app)

    async def asyncTearDown(self):
        await self.stack.aclose()
        self.environment_patch.stop()
        self.cleanup_project()

    async def remote(self, *arguments, value=None):
        options = build_parser().parse_args(["--server", "http://test/api/v2", *arguments])
        validate_options(options)
        options.request_timeout = getattr(options, "request_timeout", 30)
        with patch("autoagent.cli.remote.ServerClient", side_effect=lambda url, timeout: ServerClient(url, timeout=timeout, transport=self.transport)):
            return await run_remote(options, value)

    async def test_remote_submit_and_wait_survive_client_exit_and_match_host_state(self):
        """Detached HTTP submission returns IDs while the Server-owned invocation remains resumable."""
        _, report, code = await self.remote("invoke", "cli-approval", "--entry-node", "ask", "--input-json", "{}", "--wait", "none", value={"text": "approval"})
        self.assertEqual(code, 0)
        iid = report["data"]["invocation_id"]
        state = await self.host.wait_execution(iid, timeout=3)
        wait = state["waits"][0]
        _, report, code = await self.remote("invocation", "respond", iid, "--wait-id", wait["wait_id"], "--input-json", "{}", value={"text": "approved"})
        self.assertEqual(code, 0, report)
        self.assertEqual(report["data"]["status"], "completed")
        self.assertEqual(report["data"]["host_state"], "server")
        self.assertEqual(report["data"]["invocation_id"], iid)

    async def test_remote_pause_recover_cancel_and_version_conflict(self):
        """Remote controls use existing receipts, exact recovery versions and the original Root ID."""
        ref = await self.host.execute("cli-approval", {"text": "approval"}, entry_node_id="ask")
        await self.host.join(ref)
        iid = ref.invocation_id
        _, paused, code = await self.remote("invocation", "pause", iid)
        self.assertEqual(code, 0, paused)
        self.assertEqual(paused["data"]["control_state"], "paused")
        with self.assertRaises(CliError) as conflict:
            await self.remote("invocation", "recover", iid, "--expected-version", "0")
        self.assertEqual(conflict.exception.code, "recovery_version_conflict")
        _, recovered, code = await self.remote("invocation", "recover", iid, "--request-id", "remote-continue")
        self.assertEqual(code, 0, recovered)
        _, duplicate, code = await self.remote("invocation", "recover", iid, "--request-id", "remote-continue")
        self.assertEqual(code, 0, duplicate)
        self.assertEqual(duplicate["data"]["submitted_expected_version"], recovered["data"]["submitted_expected_version"])
        _, cancelled, code = await self.remote("invocation", "cancel", iid, "--reason", "stop")
        self.assertEqual(code, 1, cancelled)
        self.assertEqual(cancelled["data"]["status"], "cancelled")

    async def test_remote_observation_timeout_does_not_cancel_wait(self):
        """A timed-out HTTP observer leaves the Server's business Wait and Host lifecycle intact."""
        _, report, code = await self.remote("invoke", "cli-approval", "--entry-node", "ask", "--input-json", "{}",
            "--wait", "terminal", "--wait-timeout", "0.02", value={"text": "waiting"})
        self.assertEqual(code, 4)
        self.assertEqual(report["data"]["observation"], "timeout")
        state = await self.host.wait_execution(report["data"]["invocation_id"], timeout=3)
        self.assertEqual(state["status"], "waiting")

    async def test_mutation_connection_loss_is_unknown_and_never_retried(self):
        """An ambiguous POST response reports exit 4 and performs exactly one network submission."""
        requests = []
        async def broken(request):
            requests.append(request)
            raise httpx.ReadTimeout("response lost", request=request)
        async with ServerClient("http://test/api/v2", transport=httpx.MockTransport(broken)) as client:
            with self.assertRaises(CliError) as error:
                await client.request("POST", "/executions", body={})
        self.assertEqual(error.exception.code, "OUTCOME_UNKNOWN")
        self.assertEqual(error.exception.exit_code, 4)
        self.assertEqual(len(requests), 1)

    async def test_bad_mutation_responses_are_unknown_and_are_not_retried(self):
        """Server errors and corrupt mutation responses cannot be treated as definite execution rejection."""
        for status, content in ((503, b'{}'), (200, b'not json'), (200, b'[]')):
            with self.subTest(status=status, content=content):
                requests = []
                def response(request):
                    requests.append(request)
                    return httpx.Response(status, content=content)
                async with ServerClient("http://test/api/v2", transport=httpx.MockTransport(response)) as client:
                    with self.assertRaises(CliError) as error:
                        await client.request("POST", "/executions", body={})
                self.assertEqual(error.exception.exit_code, 4)
                self.assertEqual(len(requests), 1)

    async def test_missing_operation_receipt_is_unknown_not_a_second_command(self):
        """Receipt expiry returns an unknown outcome and cannot trigger automatic action resubmission."""
        ref = await self.host.execute("cli-approval", {"text": "wait"}, entry_node_id="ask")
        await self.host.join(ref)
        calls = []
        transport = self.transport
        class LostReceipt(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                calls.append((request.method, request.url.path))
                if "/execution-operations/" in request.url.path:
                    return httpx.Response(404, json={"detail": {"code": "operation_unavailable", "message": "expired"}})
                return await transport.handle_async_request(request)
        with patch("autoagent.cli.remote.ServerClient", side_effect=lambda url, timeout: ServerClient(url, timeout=timeout, transport=LostReceipt())):
            options = build_parser().parse_args(["--server", "http://test/api/v2", "invocation", "pause", ref.invocation_id])
            options.request_timeout = 30
            with self.assertRaises(CliError) as error:
                await run_remote(options, None)
        self.assertEqual(error.exception.code, "OUTCOME_UNKNOWN")
        self.assertEqual(sum(method == "POST" for method, _ in calls), 1)

    async def test_remote_mode_does_not_import_project_or_open_local_database(self):
        """HTTP execution does not use offline discovery or create another local Host."""
        with patch("autoagent.cli.commands.inspect_project", side_effect=AssertionError("No local inspection")), \
             patch.object(AutoAgentHost, "from_project", side_effect=AssertionError("No local Host")):
            _, report, code = await self.remote("invoke", "cli-echo", "--entry-node", "entry", "--input-json", "{}", value={"text": "remote"})
        self.assertEqual(code, 0, report)


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test URL is required.")
class PostgreSQLCliRemoteTests(CliRemoteTests):
    def prepare_project(self):
        super().prepare_project()
        path = self.root / ".env"
        path.write_text(path.read_text().replace("sqlite+aiosqlite:///" + str(self.database), self.postgres_url))

    async def asyncSetUp(self):
        self.postgres = PostgreSQLDatabase()
        self.postgres_url = await self.postgres.create()
        await super().asyncSetUp()

    async def asyncTearDown(self):
        await super().asyncTearDown()
        await self.postgres.close()


class CliServerProcessTests(ProjectFixture, unittest.TestCase):
    def setUp(self):
        self.prepare_project()

    def tearDown(self):
        self.cleanup_project()

    def test_failed_bind_never_emits_ready_and_has_nonzero_status(self):
        """A conflicting listening socket cannot produce a false ready report or a successful exit."""
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            reserved.listen()
            port = reserved.getsockname()[1]
            process = subprocess.run([sys.executable, "-m", "autoagent", "--project", str(self.root), "server",
                "--port", str(port), "--no-ui", "--format", "json"], env=self.environment(), capture_output=True, text=True, timeout=15)
        self.assertEqual(process.returncode, 3, process.stdout + process.stderr)
        report = json.loads(process.stdout)
        self.assertFalse(report["ok"])
        self.assertNotIn("data", report)

    def test_real_server_ready_remote_invoke_and_one_signal_shutdown(self):
        """The foreground CLI serves real HTTP, emits ready JSON, and saves a pending Wait on shutdown."""
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        (self.root / "server.env").write_text((self.root / ".env").read_text() + "AUTOAGENT_SERVER_API_PREFIX=/cli/api\nAUTOAGENT_SERVER_PORT=1\n")
        with (self.root / "server.stdout").open("w+") as stdout, (self.root / "server.stderr").open("w+") as stderr:
            process = subprocess.Popen([sys.executable, "-m", "autoagent", "--project", str(self.root), "server", "--env-file", "server.env", "--port", str(port), "--no-ui", "--format", "json"],
                env=self.environment(), stdout=stdout, stderr=stderr)
            try:
                url = f"http://127.0.0.1:{port}/cli/api"
                for _ in range(300):
                    if process.poll() is not None:
                        stderr.seek(0)
                        self.fail(stderr.read())
                    stdout.seek(0)
                    try:
                        ready = json.load(stdout)
                        break
                    except ValueError:
                        time.sleep(0.02)
                else:
                    self.fail("Server did not report readiness")
                self.assertEqual(ready["data"]["api_url"], url)
                self.assertIsNone(ready["data"]["ui_url"])
                result = subprocess.run([sys.executable, "-m", "autoagent", "--server", url, "invoke", "cli-approval",
                    "--entry-node", "ask", "--input-json", '{"text":"review"}', "--format", "json"],
                    capture_output=True, text=True, timeout=15, env=self.environment())
                self.assertEqual(result.returncode, 0, result.stderr)
                invocation = json.loads(result.stdout)["data"]
                self.assertEqual(invocation["status"], "waiting")
                for source in (("--project", str(self.root)), ("--server", url)):
                    result = subprocess.run([sys.executable, "-m", "autoagent", *source, "invocation", "show",
                        invocation["invocation_id"], "--format", "json"],
                        capture_output=True, text=True, timeout=15, env=self.environment())
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    history = json.loads(result.stdout)
                    self.assertEqual(history["data"]["status"], "waiting")
                    self.assertEqual(history["view"], "durable")
                    self.assertEqual(history["data"]["invocation_id"], invocation["invocation_id"])
                queries = [
                    ['invocation', 'actions', invocation['invocation_id']],
                    ['checkpoint', 'create', invocation['session_id'], '--pin'],
                    ['invocation', 'pause', invocation['invocation_id'], '--request-id', 'process-pause', '--wait', 'none'],
                    ['operation', 'show', 'process-pause'],
                ]
                for query in queries:
                    result = subprocess.run([sys.executable, '-m', 'autoagent', '--server', url, *query, '--format', 'json'],
                        capture_output=True, text=True, timeout=15, env=self.environment())
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertTrue(json.loads(result.stdout)['ok'])
                process.send_signal(signal.SIGINT)
                self.assertEqual(process.wait(timeout=10), 0)
                stderr.seek(0)
                logs = stderr.read()
                self.assertIn('POST /cli/api/executions HTTP/1.1" 202', logs)
                self.assertIn('Started server process', logs)
                self.assertNotIn('HTTP Request:', logs)
                with sqlite3.connect(self.database) as db:
                    self.assertGreater(db.execute("SELECT count(*) FROM checkpoints").fetchone()[0], 0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()

    def test_access_logging_can_be_disabled_by_flag_or_environment(self):
        """A CLI flag overrides enabled access logging, and an environment override disables the default."""
        for arguments, configured in ((["--no-access-log"], "true"), ([], "false")):
            with self.subTest(arguments=arguments), socket.socket() as reserved:
                reserved.bind(("127.0.0.1", 0))
                port = reserved.getsockname()[1]
            environment = dict(self.environment(), AUTOAGENT_SERVER_ACCESS_LOG=configured)
            with (self.root / "quiet.stdout").open("w+") as stdout, (self.root / "quiet.stderr").open("w+") as stderr:
                process = subprocess.Popen([sys.executable, "-m", "autoagent", "--project", str(self.root), "server",
                    "--port", str(port), "--no-ui", *arguments], env=environment, stdout=stdout, stderr=stderr)
                try:
                    with httpx.Client(timeout=2) as client:
                        for _ in range(200):
                            try:
                                response = client.get(f"http://127.0.0.1:{port}/api/v2/workflows")
                                if response.status_code == 200:
                                    break
                            except httpx.TransportError:
                                pass
                            time.sleep(.02)
                        else:
                            self.fail("Server did not start")
                    process.send_signal(signal.SIGINT)
                    self.assertEqual(process.wait(timeout=10), 0)
                    stderr.seek(0)
                    logs = stderr.read()
                    self.assertIn("Started server process", logs)
                    self.assertNotIn("GET /api/v2/workflows", logs)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait()
