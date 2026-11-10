"""Durable chat FIFO, exact-target messages and interrupted admission."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import httpx

from autoagent.agent import LLMMessage
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings
from tests.test_agent_subagents import call, response


class QueueContracts:
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "auto-agent.toml").write_text("schema_version=1\nworkflows=[]\n")
        self.url = await self.database_url()

    @asynccontextmanager
    async def host(self, provider):
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.resources.register_provider("offline", provider)
            setup.agent_context.set_defaults(settings())
            yield
        async with AutoAgentHost.from_project(self.root, use_env_file=False, environment={}, lifespan=lifespan,
            settings=HostSettings(database_url=self.url)) as host:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host)), base_url="http://test") as client:
                yield host, client

    async def until(self, condition):
        for _ in range(500):
            value = await condition()
            if value:
                return value
            await asyncio.sleep(.02)
        self.fail("Queue did not reach the expected state")

    def gate(self):
        release = threading.Event()
        self.addCleanup(release.set)
        async def step(request):
            while not release.is_set():
                await asyncio.sleep(.01)
            return answer("First done")
        return release, step

    async def item(self, host, item_id):
        return (await host._persistence.agent_queue_operation("rows", "chat", item_id=item_id))[0]

    async def test_fifo_runs_without_a_browser_and_cancel_removes_only_pending_item(self):
        """The Host starts queued turns in order after completion without client polling."""
        release, first = self.gate()
        provider = ScriptedProvider(first, answer("Second"), answer("Third"))
        async with self.host(provider) as (host, client):
            first = await host.agent_context.messages.submit("chat", "First", "one")
            for identity in ("two", "cancelled", "three"):
                response = await client.post("/api/v2/agent/queue", json={"session_id": "chat", "message": identity, "request_id": identity})
                self.assertEqual(response.status_code, 202, response.text)
                self.assertEqual(response.json()["state"], "queued")
            response = await client.post("/api/v2/agent/queue/actions", json={"session_id": "chat", "item_id": "cancelled", "action": "cancel"})
            self.assertEqual(response.json()["state"], "cancelled")
            release.set()
            await host.join(first["invocation_id"])
            async def done():
                item = await self.item(host, "three")
                return item if item["state"] == "started" else None
            last = await self.until(done)
            self.assertEqual((await host.join(last["invocation_id"])).status, "completed")
            self.assertEqual(len(provider.requests), 3)
            self.assertEqual((await host.agent_context.messages.view("chat"))["items"], [])

    async def test_duplicate_submission_is_idempotent_and_changed_content_conflicts(self):
        """Network retries return the original Invocation and cannot reuse IDs for new text."""
        provider = ScriptedProvider(answer(), answer())
        async with self.host(provider) as (host, client):
            body = dict(session_id="chat", message="Hello", request_id="same")
            first = (await client.post("/api/v2/agent/queue", json=body)).json()
            await host.join(first["invocation_id"])
            next_item = await host.agent_context.messages.submit("chat", "Later task", "later")
            await host.join(next_item["invocation_id"])
            again = await client.post("/api/v2/agent/queue", json=body)
            self.assertEqual(again.json()["invocation_id"], first["invocation_id"])
            changed = await client.post("/api/v2/agent/queue", json={**body, "message": "Different"})
            self.assertEqual(changed.status_code, 409)
            self.assertEqual(len(provider.requests), 2)

    async def test_external_wait_does_not_release_the_next_queued_turn(self):
        """Waiting for a real answer keeps the current turn active and the next messages queued."""
        provider = ScriptedProvider(response(call("ask_user", {"questions": [
            {"id": "answer", "type": "text", "text": "Which option should I use?"}]})))
        async with self.host(provider) as (host, _):
            current = await host.agent_context.messages.submit("chat", "Ask me first", "one")
            self.assertEqual((await host.join(current["invocation_id"])).status, "waiting")
            for identity in ("two", "three"):
                item = await host.agent_context.messages.submit("chat", "Next task", identity)
                self.assertEqual(item["state"], "queued")
            self.assertEqual(len(provider.requests), 1)

    async def test_ambiguous_signal_delivery_retries_the_same_receipt_without_duplicates(self):
        """Losing a Signal acknowledgement retains its exact target and deduplicates retries."""
        release, first = self.gate()
        provider = ScriptedProvider(first, answer())
        async with self.host(provider) as (host, _):
            current = await host.agent_context.messages.submit("chat", "First", "one")
            await host.agent_context.messages.submit("chat", "Unique update", "update")
            original = host.signal
            async def ambiguous(*args, **kwargs):
                await original(*args, **kwargs)
                raise ConnectionError("Receipt lost after admission")
            with patch.object(host, "signal", ambiguous):
                item = await host.agent_context.messages.act("chat", "update", "send_now", current["invocation_id"])
            self.assertEqual(item["state"], "sending")
            item = await host.agent_context.messages.act("chat", "update", "send_now", current["invocation_id"])
            self.assertEqual(item["state"], "sent")
            release.set()
            self.assertEqual((await host.join(current["invocation_id"])).status, "completed")
            messages = [item for item in provider.requests[-1].input if isinstance(item, LLMMessage) and item.role == "user"]
            self.assertEqual(sum("Unique update" in str(item) for item in messages), 1)

    async def test_send_now_is_an_idempotent_signal_not_a_new_invocation(self):
        """A queued item can become one exact-target Message without running again later."""
        release, first = self.gate()
        provider = ScriptedProvider(first, answer("Updated"))
        async with self.host(provider) as (host, client):
            current = await host.agent_context.messages.submit("chat", "First", "one")
            await host.agent_context.messages.submit("chat", "Please focus on safety", "update")
            body = dict(session_id="chat", item_id="update", action="send_now", invocation_id=current["invocation_id"])
            for _ in range(2):
                response = await client.post("/api/v2/agent/queue/actions", json=body)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["state"], "sent")
            release.set()
            result = await host.join(current["invocation_id"])
            self.assertEqual(result.status, "completed", result.error)
            rows = await host.list_session_invocations("chat")
            self.assertEqual(len(rows.items), 1)
            messages = [item for item in provider.requests[-1].input if isinstance(item, LLMMessage) and item.role == "user"]
            self.assertEqual(sum("Please focus on safety" in str(item) for item in messages), 1)

    async def test_failed_turn_blocks_queue_until_explicit_continue(self):
        """An unsuccessful previous turn cannot silently trigger dependent queued work."""
        release, _ = self.gate()
        async def failure(request):
            while not release.is_set():
                await asyncio.sleep(.01)
            raise RuntimeError("Deliberate failure")
        async with self.host(ScriptedProvider(failure, answer())) as (host, client):
            current = await host.agent_context.messages.submit("chat", "First", "one")
            await host.agent_context.messages.submit("chat", "Next", "two")
            release.set()
            self.assertEqual((await host.join(current["invocation_id"])).status, "failed")
            async def blocked():
                return (await self.item(host, "two"))["state"] == "blocked"
            await self.until(blocked)
            response = await client.post("/api/v2/agent/queue/actions", json=dict(session_id="chat", item_id="two", action="continue"))
            self.assertEqual(response.status_code, 200, response.text)
            async def started():
                item = await self.item(host, "two")
                return item if item["state"] == "started" else None
            item = await self.until(started)
            self.assertEqual((await host.join(item["invocation_id"])).status, "completed")

    async def test_restart_reconciles_accepted_admission_without_duplicate_execution(self):
        """A crash between Invocation admission and queue confirmation does not resubmit it."""
        async with self.host(ScriptedProvider(answer())) as (host, _):
            await host.agent_context.messages.close()
            await host._persistence.agent_queue_operation("add", "chat", "one", "First")
            await host._persistence.agent_queue_operation("change", "chat", "one", ("queued",), state="starting")
            ref = await host.agent_context.start_conversation("chat", "First", request_id="one")
            await host.join(ref)
            await host._persistence.flush()
        provider = ScriptedProvider()
        async with self.host(provider) as (host, _):
            async def reconciled():
                item = await self.item(host, "one")
                return item if item["state"] == "started" else None
            item = await self.until(reconciled)
            self.assertEqual(item["invocation_id"], ref.invocation_id)
            self.assertEqual(provider.requests, [])


class SQLiteQueueTests(QueueContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(self.root / "runtime.db")


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test database is not configured")
class PostgreSQLQueueTests(QueueContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
