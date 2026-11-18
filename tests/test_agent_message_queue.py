"""Durable chat FIFO, exact-target messages and interrupted admission."""

import asyncio
from base64 import b64decode, b64encode
from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import httpx

from autoagent.agent import AgentInput, LLMMessage
from autoagent.agent_app import AgentApplication
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings
from tests.test_agent_subagents import call, response

PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jVn8AAAAASUVORK5CYII="


class LegacyAgentInput(AgentInput):
    request_id: str | None = None


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
            setup.providers.register("offline", provider)
            setup.applications["agent"].context.set_defaults(settings())
            yield
        async with AutoAgentHost.from_project(self.root, use_env_file=False, environment={}, lifespan=lifespan, applications=(AgentApplication,),
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
        return (await host.applications["agent"].storage.queue("rows", "chat", item_id=item_id))[0]

    async def test_fifo_runs_without_a_browser_and_cancel_removes_only_pending_item(self):
        """The Host starts queued turns in order after completion without client polling."""
        release, first = self.gate()
        provider = ScriptedProvider(first, answer("Second"), answer("Third"))
        async with self.host(provider) as (host, client):
            first = await host.applications["agent"].context.messages.submit("chat", "First", "one")
            for identity in ("two", "cancelled", "three"):
                response = await client.post("/api/v2/agent/queue", json={"session_id": "chat", "message": identity, "item_id": identity})
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
            self.assertEqual((await host.applications["agent"].context.messages.view("chat"))["items"], [])

    async def test_duplicate_submission_is_idempotent_and_changed_content_conflicts(self):
        """Network retries return the original Invocation and cannot reuse IDs for new text."""
        provider = ScriptedProvider(answer(), answer())
        async with self.host(provider) as (host, client):
            body = dict(session_id="chat", message="Hello", item_id="same")
            first = (await client.post("/api/v2/agent/queue", json=body)).json()
            await host.join(first["invocation_id"])
            next_item = await host.applications["agent"].context.messages.submit("chat", "Later task", "later")
            await host.join(next_item["invocation_id"])
            again = await client.post("/api/v2/agent/queue", json=body)
            self.assertEqual(again.json()["invocation_id"], first["invocation_id"])
            changed = await client.post("/api/v2/agent/queue", json={**body, "message": "Different"})
            self.assertEqual(changed.status_code, 409)
            self.assertEqual(len(provider.requests), 2)

    async def test_queued_image_is_durable_and_delivered_to_the_model(self):
        """An image-only queued turn retains its media on retry and across Host restart."""
        image = dict(mime_type="image/png", data=PNG)
        release, blocked = self.gate()
        provider = ScriptedProvider(blocked, answer("Image received"))
        async with self.host(provider) as (host, client):
            first = await host.applications["agent"].queue_submit("chat", "Start first", "first")
            body = dict(session_id="chat", message="", item_id="image", images=[image])
            accepted = await client.post("/api/v2/agent/queue", json=body)
            self.assertEqual(accepted.status_code, 202, accepted.text)
            item = accepted.json()
            self.assertEqual(item["state"], "queued")
            self.assertEqual(item["images"][0]["source"]["type"], "asset")
            key = item["images"][0]["source"]["uri"].rsplit("/", 1)[1]
            retry = await client.post("/api/v2/agent/queue", json=body)
            self.assertEqual(retry.json(), item)
            other_image = {**image, "data": b64encode(b64decode(image["data"]) + b"x").decode("ascii")}
            conflict = await client.post("/api/v2/agent/queue", json={**body, "images": [other_image]})
            self.assertEqual(conflict.status_code, 409)
            invalid = await client.post("/api/v2/agent/queue", json={**body, "images": [{**image, "mime_type": "image/jpeg"}]})
            self.assertEqual(invalid.status_code, 400)
            release.set()
            await host.join(first["invocation_id"])
            async def started():
                current = await self.item(host, "image")
                return current if current["state"] == "started" else None
            final = await self.until(started)
            self.assertEqual((await host.join(final["invocation_id"])).status, "completed")
            received = [part.source for message in provider.requests[-1].input if isinstance(message, LLMMessage)
                for part in message.content if part.type == "image"]
            self.assertEqual([source.data for source in received], [image["data"]])
        async with self.host(ScriptedProvider()) as (_, client):
            fetched = await client.get(f"/api/v2/agent/images/{key}")
            self.assertEqual(fetched.headers["content-type"], "image/png")
            self.assertEqual(fetched.content, b64decode(image["data"]))

    async def test_rejected_first_submission_does_not_bind_workspace_or_title(self):
        """Validation and rejected queue inserts leave a draft free to choose a Workspace."""
        async with self.host(ScriptedProvider(answer(), answer())) as (host, client):
            first = dict(session_id="draft", message="Rejected title", item_id="retry")
            (self.root / 'providers.json').write_text(json.dumps({
                'providers': {'catalog': {'models': [{'id': 'available'}]}}}))
            await host.providers.reload()
            rejected = await client.post('/api/v2/agent/queue', json={**first,
                'model': {'provider': 'catalog', 'model': 'missing'}})
            self.assertEqual(rejected.status_code, 400, rejected.text)
            used_id = dict(session_id="other", message="Reserve this queue ID", item_id="taken",
                model={"provider": "offline", "model": "offline"})
            accepted = await client.post('/api/v2/agent/queue', json=used_id)
            self.assertEqual(accepted.status_code, 202, accepted.text)
            await host.join(accepted.json()['invocation_id'])
            conflict = await client.post('/api/v2/agent/queue', json={**used_id, 'session_id': 'draft'})
            self.assertEqual(conflict.status_code, 409, conflict.text)
            info = await client.get('/api/v2/agent/settings', params={'session_id': 'draft'})
            self.assertEqual(info.json()['version'], 0, info.text)
            self.assertIsNone(info.json()['title'])
            retry = await client.post('/api/v2/agent/queue', json={**first, 'workspace_id': None,
                'message': 'Accepted title', 'model': used_id['model']})
            self.assertEqual(retry.status_code, 202, retry.text)
            await host.join(retry.json()['invocation_id'])
            info = (await client.get('/api/v2/agent/settings', params={'session_id': 'draft'})).json()
            self.assertIsNone(info['workspace'])
            self.assertEqual(info['title'], 'Accepted title')

    async def test_external_wait_does_not_release_the_next_queued_turn(self):
        """Waiting for a real answer keeps the current turn active and the next messages queued."""
        provider = ScriptedProvider(response(call("ask_user", {"questions": [
            {"id": "answer", "type": "text", "text": "Which option should I use?"}]})))
        async with self.host(provider) as (host, _):
            current = await host.applications["agent"].context.messages.submit("chat", "Ask me first", "one")
            self.assertEqual((await host.join(current["invocation_id"])).status, "waiting")
            for identity in ("two", "three"):
                item = await host.applications["agent"].context.messages.submit("chat", "Next task", identity)
                self.assertEqual(item["state"], "queued")
            self.assertEqual(len(provider.requests), 1)

    async def test_ambiguous_signal_delivery_retries_the_same_receipt_without_duplicates(self):
        """Losing a Signal acknowledgement retains its exact target and deduplicates retries."""
        release, first = self.gate()
        provider = ScriptedProvider(first, answer())
        async with self.host(provider) as (host, _):
            current = await host.applications["agent"].context.messages.submit("chat", "First", "one")
            await host.applications["agent"].context.messages.submit("chat", "Unique update", "update")
            original = host.signal
            async def ambiguous(*args, **kwargs):
                await original(*args, **kwargs)
                raise ConnectionError("Receipt lost after admission")
            with patch.object(host, "signal", ambiguous):
                item = await host.applications["agent"].context.messages.act("chat", "update", "send_now", current["invocation_id"])
            self.assertEqual(item["state"], "sending")
            item = await host.applications["agent"].context.messages.act("chat", "update", "send_now", current["invocation_id"])
            self.assertEqual(item["state"], "sent")
            release.set()
            self.assertEqual((await host.join(current["invocation_id"])).status, "completed")
            messages = [item for item in provider.requests[-1].input if isinstance(item, LLMMessage) and item.role == "user"]
            self.assertEqual(sum("Unique update" in str(item) for item in messages), 1)

    async def test_send_now_is_an_idempotent_signal_not_a_new_invocation(self):
        """A queued item with an image becomes one exact-target Message."""
        release, first = self.gate()
        provider = ScriptedProvider(first, answer("Updated"))
        async with self.host(provider) as (host, client):
            current = await host.applications["agent"].context.messages.submit("chat", "First", "one")
            queued = await client.post("/api/v2/agent/queue", json=dict(session_id="chat", message="Please focus on safety",
                item_id="update", images=[dict(mime_type="image/png", data=PNG)]))
            self.assertEqual(queued.status_code, 202, queued.text)
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
            self.assertEqual([part.source.data for item in messages for part in item.content if part.type == "image"], [PNG])

    async def test_send_now_request_cannot_move_to_another_invocation(self):
        """Exact retries return the same receipt while another target conflicts on both send paths."""
        release_first, first = self.gate()
        release_second, second = self.gate()
        provider = ScriptedProvider(first, answer(), second, answer())
        async with self.host(provider) as (host, client):
            queue = host.applications["agent"].context.messages
            current = await queue.submit("chat", "First", "one")
            update = dict(session_id="chat", message="Direct update", item_id="direct", send_to=current["invocation_id"])
            sent = await client.post("/api/v2/agent/queue", json=update)
            self.assertEqual(sent.status_code, 202, sent.text)
            self.assertEqual(sent.json()["state"], "sent")
            await queue.submit("chat", "Queued update", "queued-update")
            await queue.act("chat", "queued-update", "send_now", current["invocation_id"])
            release_first.set()
            self.assertEqual((await host.join(current["invocation_id"])).status, "completed")
            following = await queue.submit("chat", "Second", "two")
            try:
                retried = await client.post("/api/v2/agent/queue", json=update)
                self.assertEqual(retried.status_code, 202, retried.text)
                self.assertEqual(retried.json()["invocation_id"], current["invocation_id"])
                target = following["invocation_id"]
                wrong = await client.post("/api/v2/agent/queue", json={**update, "send_to": target})
                self.assertEqual(wrong.status_code, 409, wrong.text)
                action = await client.post("/api/v2/agent/queue/actions", json={
                    "session_id": "chat", "item_id": "queued-update", "action": "send_now", "invocation_id": target})
                self.assertEqual(action.status_code, 409, action.text)
            finally:
                release_second.set()

    async def test_existing_queue_recovers_immediate_target_during_startup(self):
        """Startup backfills the target of a previously persisted direct Message receipt."""
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

        release, first = self.gate()
        async with self.host(ScriptedProvider(first, answer())) as (host, _):
            queue = host.applications["agent"].context.messages
            running = await queue.submit("chat", "First", "first")
            target = running["invocation_id"]
            try:
                sent = await queue.submit("chat", "Update", "update", send_to=target)
                self.assertEqual(sent["state"], "sent")
            finally:
                release.set()
            self.assertEqual((await host.join(target)).status, "completed")
        engine = create_async_engine(self.url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("ALTER TABLE agent_app_message_queue DROP COLUMN send_to"))
                await conn.execute(text("ALTER TABLE agent_app_message_queue DROP COLUMN images"))
        finally:
            await engine.dispose()
        async with self.host(ScriptedProvider()) as (_, client):
            sent = await client.post("/api/v2/agent/queue", json={
                "session_id": "chat", "message": "Update", "item_id": "update", "send_to": target})
            self.assertEqual(sent.status_code, 202, sent.text)
            self.assertEqual(sent.json()["state"], "sent")
            self.assertEqual(sent.json()["invocation_id"], target)

    async def test_failed_turn_blocks_queue_until_explicit_continue(self):
        """An unsuccessful previous turn cannot silently trigger dependent queued work."""
        release, _ = self.gate()
        async def failure(request):
            while not release.is_set():
                await asyncio.sleep(.01)
            raise RuntimeError("Deliberate failure")
        async with self.host(ScriptedProvider(failure, answer())) as (host, client):
            current = await host.applications["agent"].context.messages.submit("chat", "First", "one")
            await host.applications["agent"].context.messages.submit("chat", "Next", "two")
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
            await host.applications["agent"].context.messages.close()
            await host.applications["agent"].storage.queue("add", "chat", "one", "First")
            await host.applications["agent"].storage.queue("change", "chat", "one", ("queued",), state="starting")
            app = host.applications["agent"]
            item = await self.item(host, "one")
            ref = await app.context.messages._submit_invocation(item)
            pending = await self.item(host, "one")
            self.assertEqual(pending["invocation_id"], ref.invocation_id)
            self.assertEqual(pending["state"], "starting")
            invocation = await host._persistence.get_invocation(ref.invocation_id, view="accepted")
            recorded = (await host._persistence.resolve(invocation["input_ref"]))["value"]
            self.assertEqual(set(recorded), {"message", "model", "images"})
            self.assertEqual(recorded["images"], [])
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

    async def test_restart_retries_reservation_without_a_start_event(self):
        """A reserved Invocation ID alone does not mean the queued turn was admitted."""
        async with self.host(ScriptedProvider()) as (host, _):
            app = host.applications["agent"]
            await app.context.messages.close()
            await app.storage.queue("add", "chat", "one", "First")
            await app.storage.queue("change", "chat", "one", ("queued",), state="starting",
                invocation_id="uncommitted-reservation")
        provider = ScriptedProvider(answer())
        async with self.host(provider) as (host, _):
            async def started():
                item = await self.item(host, "one")
                return item if item["state"] == "started" else None
            item = await self.until(started)
            self.assertNotEqual(item["invocation_id"], "uncommitted-reservation")
            self.assertEqual((await host.join(item["invocation_id"])).status, "completed")
            self.assertEqual(len(provider.requests), 1)

    async def test_startup_reconciles_an_admission_recorded_by_the_old_input(self):
        """Upgrade accepted queue entries and discard stale steering targets from older rows."""
        with patch("autoagent.agent.workflow.builder.AgentInput", LegacyAgentInput):
            async with self.host(ScriptedProvider(answer(), answer())) as (host, _):
                app = host.applications["agent"]
                await app.context.messages.close()
                await app.storage.queue("add", "chat", "one", "First")
                await app.storage.queue("change", "chat", "one", ("queued",), state="starting")
                ref = await host.execute("autoagent.agent", LegacyAgentInput(message="First", request_id="one"),
                    session_id="chat")
                self.assertEqual((await host.join(ref)).status, "completed")
                prior = await host.execute("autoagent.agent", LegacyAgentInput(message="Previous turn"),
                    session_id="other")
                self.assertEqual((await host.join(prior)).status, "completed")
                await app.storage.queue("add", "other", "steered", "Retry as a new turn")
                await app.storage.queue("change", "other", "steered", ("queued",), state="starting",
                    invocation_id=prior.invocation_id, previous_invocation_id=prior.invocation_id)
        provider = ScriptedProvider(answer())
        async with self.host(provider) as (host, _):
            async def reconciled():
                item = await self.item(host, "one")
                return item if item["state"] == "started" else None
            item = await self.until(reconciled)
            self.assertEqual(item["invocation_id"], ref.invocation_id)
            async def started_retry():
                rows = await host.applications["agent"].storage.queue("rows", "other", item_id="steered")
                return rows[0] if rows[0]["state"] == "started" else None
            retry = await self.until(started_retry)
            self.assertNotEqual(retry["invocation_id"], prior.invocation_id)
            self.assertEqual((await host.join(retry["invocation_id"])).status, "completed")
            self.assertEqual(len(provider.requests), 1)


class SQLiteQueueTests(QueueContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(self.root / "runtime.db")


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test database is not configured")
class PostgreSQLQueueTests(QueueContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
