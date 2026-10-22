"""Read-only history, scoped pagination and explicit value access on SQLite/PostgreSQL."""

import asyncio
from contextlib import AsyncExitStack, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

import httpx
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from autoagent.cli.history import run_query
from autoagent.cli.main import build_parser, main
from autoagent.cli.remote import ServerClient
from autoagent.host import AutoAgentHost, HostRunConflict
from autoagent.host.persistence.reading import SourceReader
from autoagent.host.persistence.storage import Storage
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_cli_execution import ProjectFixture


EXTRA = '''
from autoagent import ConditionContext, Edge, InputMappingContext
def from_route(ctx: InputMappingContext) -> Value:
    return ctx.incoming['route']
def from_raw(ctx: InputMappingContext) -> Value:
    return {'text':ctx.invocation_input['text']}
def route_condition(ctx: ConditionContext) -> bool:
    return True
@workflow
def chain():
    return Workflow('cli-chain', nodes=[Node('entry',echo),Node('finish',echo,input_mapping=from_route)],
        edges=[Edge('entry','finish',id='route',condition=route_condition)])
@workflow
def raw_json():
    return Workflow('cli-json', nodes=[Node('entry',echo,input_mapping=from_raw)])
'''


class HistoryFixture(ProjectFixture):
    def prepare_project(self):
        super().prepare_project()
        path = self.root / (self.module + ".py")
        path.write_text(path.read_text() + EXTRA)

    async def seed(self, host, *, large_value=True):
        self.refs = []
        for workflow, entry in (("cli-chain", "entry"), ("cli-echo", "entry"), ("cli-chain", "entry")):
            ref = await host.execute(workflow, {"text": "history"}, session_id="history-conversation", entry_node_id=entry)
            await host.join(ref)
            self.refs.append(ref)
        self.current = self.refs[-1]
        self.failed = await host.execute("cli-recoverable", {"text": "fail"}, entry_node_id="entry")
        await host.join(self.failed)
        self.waiting = await host.execute("cli-parent", {"text": "approval"}, entry_node_id="spawn")
        await host.join(self.waiting)
        if large_value:
            self.raw = await host.execute("cli-json", {"text": "small", "nested": [{"a/b": "selected", "~key": "tilde", "0": "zero"}],
                "unused": "x" * (2 * 1024 * 1024)}, entry_node_id="entry")
            await host.join(self.raw)


class CliHistoryTests(HistoryFixture, unittest.TestCase):
    def setUp(self):
        self.prepare_project()
        self.environment_patch = patch.dict(os.environ, self.environment(), clear=True)
        self.environment_patch.start()
        async def prepare():
            async with AutoAgentHost.from_project(self.root) as host:
                await self.seed(host)
        with redirect_stdout(io.StringIO()):
            asyncio.run(prepare())

    def tearDown(self):
        self.environment_patch.stop()
        self.cleanup_project()

    def query(self, *arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = main(["--project", str(self.root), *arguments, "--format", "json"])
        return status, json.loads(stdout.getvalue()), stderr.getvalue()

    def test_queries_never_import_project_open_runtime_or_change_history(self):
        """Broken source code does not prevent inspection or cause new registration/checkpoint writes."""
        before = self.database.read_bytes()
        (self.root / (self.module + ".py")).write_text('raise AssertionError("Project code must not execute")\n')
        with patch("autoagent.host.host.Runtime", side_effect=AssertionError("No Runtime")), \
             patch.object(SourceReader, "read", side_effect=AssertionError("Metadata only")):
            for arguments in (("session", "list"), ("session", "show", "history-conversation"),
                              ("invocation", "show", self.current.invocation_id),
                              ("invocation", "nodes", self.current.invocation_id),
                              ("invocation", "events", self.current.invocation_id)):
                with self.subTest(arguments=arguments):
                    status, report, stderr = self.query(*arguments)
                    self.assertEqual(status, 0, (report, stderr))
                    self.assertEqual(report["view"], "durable")
        self.assertEqual(self.database.read_bytes(), before)

    def test_session_invocation_pagination_preserves_filters_and_rejects_cross_scope_cursors(self):
        """Seek cursors visit each Invocation once and cannot be reused for another filter or collection."""
        ids, cursor = [], None
        for _ in range(10):
            arguments = ["session", "invocations", "history-conversation", "--limit", "1"]
            if cursor:
                arguments += ["--cursor", cursor]
            status, report, _ = self.query(*arguments)
            self.assertEqual(status, 0, report)
            ids += [item["invocation_id"] for item in report["data"]["items"]]
            cursor = report["data"]["next_cursor"]
            if cursor is None:
                break
        self.assertEqual(ids, [ref.invocation_id for ref in self.refs])
        _, first, _ = self.query("session", "invocations", "history-conversation", "--limit", "1")
        cursor = first["data"]["next_cursor"]
        for arguments in (("session", "invocations", "history-conversation", "--workflow-id", "cli-chain"),
                          ("invocation", "list")):
            status, report, _ = self.query(*arguments, "--cursor", cursor)
            self.assertEqual(status, 2)
            self.assertEqual(report["error"]["code"], "INVALID_CURSOR")
        status, report, _ = self.query("invocation", "list", "--workflow-id", "cli-chain", "--session-id", "history-conversation")
        self.assertEqual(status, 0, report)
        self.assertEqual(len(report["data"]["items"]), 2)

    def test_occurrence_call_edge_stage_and_explicit_values_are_scoped(self):
        """Metadata IDs lead to exact records, mapped input and values without confusing Node definitions and occurrences."""
        iid = self.current.invocation_id
        status, report, _ = self.query("invocation", "nodes", iid, "--node-id", "finish")
        self.assertEqual(status, 0, report)
        occurrence = report["data"]["items"][0]["occurrence_id"]
        status, report, _ = self.query("invocation", "node", iid, occurrence)
        self.assertEqual(status, 0, report)
        self.assertEqual(report["data"]["node_id"], "finish")
        self.assertNotIn("value_keys", report["data"])
        _, report, _ = self.query("invocation", "calls", iid, "--occurrence-id", occurrence)
        call = report["data"]["items"][0]["call_id"]
        _, report, _ = self.query("invocation", "call", iid, call)
        self.assertEqual(report["data"]["occurrence_id"], occurrence)
        _, report, _ = self.query("invocation", "value", iid, "--kind", "call", "--identity", call, "--field", "output", "--path", "/text")
        self.assertEqual(report["data"], {"available": True, "value": "history"})
        _, report, _ = self.query("invocation", "stages", iid, occurrence)
        mapping = next(item for item in report["data"]["items"] if item["event_name"] == "input.mapped")
        status, report, _ = self.query("invocation", "stage-value", iid, occurrence, mapping["event_id"], "--field", "mapped_input", "--path", "/text")
        self.assertEqual(status, 0, report)
        self.assertEqual(report["data"]["value"], "history")
        _, report, _ = self.query("invocation", "edges", iid)
        edge = report["data"]["items"][0]
        _, report, _ = self.query("invocation", "edge", iid, f"{edge['event_id']}:{edge['result_index']}")
        self.assertEqual(report["data"]["edge_id"], "route")
        status, _, _ = self.query("invocation", "call", self.failed.invocation_id, call)
        self.assertEqual(status, 2)

    def test_pointer_selection_does_not_load_unrelated_artifacts(self):
        """A small nested value remains readable while loading the unrelated MiB Artifact is forbidden."""
        original = Storage.row
        async def reject_artifacts(storage, conn, name, **identity):
            if name == "artifacts":
                raise AssertionError("Unrelated Artifact loaded")
            return await original(storage, conn, name, **identity)
        for pointer, value in (("/nested/0/a~1b", "selected"), ("/nested/0/~0key", "tilde"), ("/nested/0/0", "zero")):
            with self.subTest(pointer=pointer), patch.object(Storage, "row", reject_artifacts):
                status, report, _ = self.query("invocation", "value", self.raw.invocation_id, "--field", "input", "--path", pointer)
                self.assertEqual(status, 0, report)
                self.assertEqual(report["data"]["value"], value)
        _, report, _ = self.query("invocation", "value", self.raw.invocation_id, "--field", "input", "--path", "/missing")
        self.assertFalse(report["data"]["available"])

    def test_event_headers_are_paged_by_session_sequence_and_bodies_are_explicit(self):
        """Invocation Events retain their Session sequence, including gaps caused by earlier Invocations."""
        iid = self.current.invocation_id
        sequences, cursor = [], None
        while True:
            arguments = ["invocation", "events", iid, "--limit", "5"]
            if cursor:
                arguments += ["--cursor", cursor]
            status, report, _ = self.query(*arguments)
            self.assertEqual(status, 0, report)
            rows = report["data"]["items"]
            self.assertTrue(all("payload" not in item and item["sequence_scope"] == "session" for item in rows))
            sequences += [item["sequence"] for item in rows]
            cursor = report["data"]["next_cursor"]
            if cursor is None:
                break
        self.assertGreater(sequences[0], 1)
        self.assertEqual(sequences, sorted(set(sequences)))
        _, report, _ = self.query("invocation", "events", iid, "--event-name", "invocation.started")
        event = report["data"]["items"][0]
        _, report, _ = self.query("invocation", "event", iid, event["event_id"])
        self.assertNotIn("payload", report["data"])
        _, report, _ = self.query("invocation", "event", iid, event["event_id"], "--field", "payload")
        self.assertTrue(report["data"]["available"])
        status, _, _ = self.query("invocation", "event", self.failed.invocation_id, event["event_id"], "--field", "payload")
        self.assertEqual(status, 2)

    def test_failed_invocation_queries_succeed_and_error_values_are_explicit(self):
        """Query success is independent of business failure, and fault detail is loaded only when requested."""
        iid = self.failed.invocation_id
        status, report, _ = self.query("invocation", "show", iid)
        self.assertEqual(status, 0, report)
        self.assertEqual(report["data"]["status"], "failed")
        self.assertEqual(report["data"]["counts"]["nodes"]["failed"], 1)
        _, report, _ = self.query("invocation", "nodes", iid, "--status", "failed")
        self.assertEqual(len(report["data"]["items"]), 1)
        _, report, _ = self.query("invocation", "value", iid, "--field", "error")
        self.assertTrue(report["data"]["available"])
        self.assertIn("first attempt failed", str(report["data"]["value"]))

    def test_family_waits_historical_definitions_and_checkpoints_are_readable(self):
        """Saved revisions and parent/child evidence remain navigable without registered project code."""
        _, report, _ = self.query("invocation", "children", self.waiting.invocation_id)
        child = report["data"]["items"][0]["invocation_id"]
        _, report, _ = self.query("invocation", "family", child)
        self.assertEqual(report["data"]["root"]["invocation_id"], self.waiting.invocation_id)
        _, report, _ = self.query("invocation", "waits", child)
        self.assertEqual(report["data"]["items"][0]["kind"], "external")
        _, report, _ = self.query("workflow", "revisions", "cli-chain")
        revision = report["data"]["items"][0]["revision_id"]
        _, report, _ = self.query("workflow", "show", "cli-chain", "--revision-id", revision, "--node-id", "finish")
        self.assertEqual(report["data"]["id"], "finish")
        _, report, _ = self.query("session", "checkpoints", "history-conversation")
        checkpoint = report["data"]["items"][0]["checkpoint_id"]
        status, report, _ = self.query("checkpoint", "show", checkpoint)
        self.assertEqual(status, 0, report)
        self.assertNotIn("checkpoint_text", report["data"])

    def test_missing_database_and_invalid_query_arguments_never_create_storage(self):
        """A missing store or malformed filters/cursors are explicit failures, never an empty initialized database."""
        for arguments in (("session", "list", "--limit", "0"), ("invocation", "list", "--started-after", "2026-10-18"),
                          ("invocation", "value", self.raw.invocation_id, "--kind", "node", "--field", "input"),
                          ("invocation", "value", self.raw.invocation_id, "--field", "input", "--path", "wrong")):
            status, _, _ = self.query(*arguments)
            self.assertEqual(status, 2)
        self.database.unlink()
        for suffix in ("-wal", "-shm", ".writer.lock"):
            Path(str(self.database) + suffix).unlink(missing_ok=True)
        status, report, _ = self.query("session", "list")
        self.assertEqual(status, 2, report)
        self.assertEqual(report["error"]["code"], "DATABASE_NOT_FOUND")
        self.assertFalse(self.database.exists())

    def test_empty_database_is_not_initialized_and_text_reports_keep_drilldown_ids(self):
        """Inspection refuses an empty schema and text pages expose exact Edge identities and available values."""
        _, report, _ = self.query("invocation", "edges", self.current.invocation_id)
        edge = report["data"]["items"][0]
        stdout = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(io.StringIO()):
            status = main(["--project", str(self.root), "invocation", "edges", self.current.invocation_id])
        self.assertEqual(status, 0)
        self.assertIn(f"{edge['event_id']}:{edge['result_index']}", stdout.getvalue())
        empty = self.root / "empty.db"
        sqlite3.connect(empty).close()
        with patch.dict(os.environ, {"AUTOAGENT_DATABASE_URL": "sqlite+aiosqlite:///" + str(empty)}):
            status, report, _ = self.query("session", "list")
        self.assertEqual(status, 3, report)
        self.assertIn("schema", report["error"]["message"])
        with sqlite3.connect(empty) as db:
            self.assertEqual(db.execute("SELECT name FROM sqlite_master").fetchall(), [])

    def test_cursor_cannot_be_reused_after_database_replacement(self):
        """Cursor source identity prevents a new store at the same path from returning unrelated continuation data."""
        _, first, _ = self.query("session", "invocations", "history-conversation", "--limit", "1")
        with sqlite3.connect(self.database) as db:
            db.execute("UPDATE persistence_meta SET store_id='replacement-store'")
        status, report, _ = self.query("session", "invocations", "history-conversation", "--cursor", first["data"]["next_cursor"])
        self.assertEqual(status, 2, report)
        self.assertEqual(report["error"]["code"], "INVALID_CURSOR")


class HostReadOnlyTests(HistoryFixture, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.prepare_project()
        self.environment_patch = patch.dict(os.environ, self.environment(), clear=True)
        self.environment_patch.start()
        self.stack = AsyncExitStack()
        self.host = await self.stack.enter_async_context(AutoAgentHost.from_project(self.root))
        await self.seed(self.host)

    async def asyncTearDown(self):
        await self.stack.aclose()
        self.environment_patch.stop()
        self.cleanup_project()

    async def test_reader_coexists_with_writer_and_enforces_read_only_at_host_and_driver(self):
        """A reader holds no writer lease, starts no Core, and cannot mutate through Host or raw SQL."""
        settings = self.host.settings
        async with AutoAgentHost(settings=settings, environment={}, root=self.root, read_only=True) as reader:
            self.assertIsNone(reader._runtime)
            self.assertIsNone(reader._persistence._writer.task)
            self.assertEqual((await reader.get_invocation_summary(self.current.invocation_id))["status"], "completed")
            with self.assertRaises(HostRunConflict):
                await reader.execute("cli-echo", {"text": "forbidden"}, entry_node_id="entry")
            with self.assertRaises(HostRunConflict):
                await reader.delete_checkpoint("missing")
            async def forbidden():
                async with reader._persistence._writer.storage.engine.begin() as conn:
                    await conn.execute(text("UPDATE persistence_meta SET last_store_order=0"))
            with self.assertRaises(DBAPIError):
                await reader._persistence._writer.run(forbidden)
            ref = await self.host.execute("cli-echo", {"text": "writer still works"}, entry_node_id="entry")
            await self.host.join(ref)
            self.assertEqual((await reader.get_invocation_summary(ref.invocation_id))["status"], "completed")

    async def query_remote(self, *arguments):
        app = create_app(self.host, ui=False)
        options = build_parser().parse_args(["--server", "http://test/api/v2", *arguments])
        options.request_timeout = 30
        with patch("autoagent.cli.remote.ServerClient", side_effect=lambda url, timeout: ServerClient(url, timeout=timeout, transport=httpx.ASGITransport(app=app))):
            return await run_query(options)

    async def test_remote_query_chain_has_metadata_parity_and_scoped_pagination(self):
        """HTTP queries reuse Host semantics and never need local project import or another writer."""
        _, report, status = await self.query_remote("session", "invocations", "history-conversation", "--limit", "1")
        self.assertEqual(status, 0, report)
        cursor = report["data"]["next_cursor"]
        _, second, _ = await self.query_remote("session", "invocations", "history-conversation", "--limit", "1", "--cursor", cursor)
        self.assertNotEqual(report["data"]["items"][0]["invocation_id"], second["data"]["items"][0]["invocation_id"])
        for arguments in (("session", "list"), ("session", "show", "history-conversation"),
            ("session", "workflows", "history-conversation"), ("invocation", "list", "--workflow-id", "cli-chain"),
            ("invocation", "show", self.current.invocation_id), ("invocation", "nodes", self.current.invocation_id),
            ("invocation", "calls", self.current.invocation_id), ("invocation", "edges", self.current.invocation_id),
            ("invocation", "events", self.current.invocation_id), ("invocation", "children", self.waiting.invocation_id),
            ("workflow", "revisions", "cli-chain")):
            with self.subTest(arguments=arguments), patch.object(SourceReader, "read", side_effect=AssertionError("Metadata only")):
                _, value, status = await self.query_remote(*arguments)
                self.assertEqual(status, 0, value)
                self.assertEqual(value["view"], "durable")
        _, value, _ = await self.query_remote("invocation", "value", self.raw.invocation_id, "--field", "input", "--path", "/nested/0/a~1b")
        self.assertEqual(value["data"]["value"], "selected")
        iid = self.current.invocation_id
        _, value, _ = await self.query_remote("invocation", "nodes", iid, "--node-id", "finish")
        occurrence = value["data"]["items"][0]["occurrence_id"]
        _, value, _ = await self.query_remote("invocation", "stages", iid, occurrence)
        stage = next(item for item in value["data"]["items"] if item["event_name"] == "input.mapped")
        _, value, _ = await self.query_remote("invocation", "stage-value", iid, occurrence, stage["event_id"], "--field", "mapped_input", "--path", "/text")
        self.assertEqual(value["data"]["value"], "history")
        _, value, _ = await self.query_remote("invocation", "events", iid, "--event-name", "invocation.started")
        event = value["data"]["items"][0]["event_id"]
        _, value, _ = await self.query_remote("invocation", "event", iid, event, "--field", "payload")
        self.assertTrue(value["data"]["available"])
        _, value, _ = await self.query_remote("workflow", "revisions", "cli-chain")
        revision = value["data"]["items"][0]["revision_id"]
        _, value, _ = await self.query_remote("workflow", "show", "cli-chain", "--revision-id", revision, "--definition")
        self.assertEqual(value["data"]["workflow_id"], "cli-chain")


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test URL is required.")
class PostgreSQLHostReadOnlyTests(HostReadOnlyTests):
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
