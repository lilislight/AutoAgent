"""Live-only observations and complete persistent history across Host restarts."""

import asyncio
from pathlib import Path
import tempfile
import unittest

from autoagent import Map, Node, Stream, UserEventContext, UserEventSpec, Wait, Workflow
from autoagent.core.runtime import UserEvent
from autoagent.host import AutoAgentHost, HostSettings, UserEventStreamOverflow
from autoagent.host.persistence import DatabaseSink
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_user_event_lifecycle import Value, Sum, identity, mapped, pieces


def live_then_complete(context: UserEventContext) -> UserEventSpec:
    return UserEventSpec(kind=context.phase, payload=context.value, persist=context.phase != "chunk")


def interactions(context: UserEventContext) -> UserEventSpec:
    return UserEventSpec(kind=context.phase, payload=context.value, persist=context.phase != "waiting")


class HostUserEventContracts:
    async def test_wait_can_be_answered_from_live_event_without_joining_first(self):
        """Every published Wait ID is actionable even during partial Map registration."""
        workflow = Workflow("live-map-replies", nodes=[Node("ask", Wait(Value, Value),
            input_mapping=mapped, map=Map(max_parallelism=1), user_events=interactions)])
        async with AutoAgentHost(workflows=[workflow], settings=self.settings) as host:
            async with host.subscribe_user_events() as stream:
                execution = await host.execute(workflow.id, {"value": 3})
                answered = set()
                while len(answered) < 3:
                    event = await asyncio.wait_for(anext(stream), 3)
                    if event.kind == "waiting":
                        wait_id = event.payload["wait_id"]
                        await host.respond(execution, wait_id, {"value": event.payload["request"]["value"] + 10})
                        answered.add(wait_id)
                result = await host.join(execution)
                self.assertEqual(result.output, [{"value": 10}, {"value": 11}, {"value": 12}])
                saved = (await host.get_user_events(execution.invocation_id)).items
                self.assertEqual([e["sequence"] for e in saved], [1, 2, 3, 4])
                self.assertEqual([e["kind"] for e in saved], ["wait_resolved"] * 3 + ["output"])

    async def test_live_filter_and_subscription_close_do_not_leak_other_invocations(self):
        """An Invocation subscription only receives its own future events and closes promptly."""
        ask = Workflow("filtered-ask", nodes=[Node("ask", Wait(Value, Value), user_events=interactions)])
        work = Workflow("unrelated-work", nodes=[Node("work", identity, user_events=interactions)])
        async with AutoAgentHost(workflows=[ask, work], settings=self.settings) as host:
            execution = await host.execute("filtered-ask", {"value": 1})
            waiting = await host.join(execution)
            async with host.subscribe_user_events(execution.invocation_id) as stream:
                unrelated = await host.execute("unrelated-work", {"value": 3})
                await host.join(unrelated)
                await host.respond(execution, waiting.waits[0].id, {"value": 2})
                await host.join(execution)
                events = [await asyncio.wait_for(anext(stream), 2) for _ in range(2)]
                self.assertTrue(all(e.invocation_id == execution.invocation_id for e in events))
                self.assertEqual([e.kind for e in events], ["wait_resolved", "output"])
            with self.assertRaises(StopAsyncIteration):
                await anext(stream)

    async def test_live_delivery_has_full_chunks_but_history_has_only_complete_output(self):
        """Live readers see progress; SQLite/PostgreSQL retain one complete event."""
        workflow = Workflow("live-history", nodes=[Node("sum", pieces, stream=Stream(Sum()), user_events=live_then_complete)])
        async with AutoAgentHost(workflows=[workflow], settings=self.settings) as host:
            async with host.subscribe_user_events(capacity=64) as stream:
                execution = await host.execute("live-history", {"value": 20})
                await host.join(execution)
                observed = [await asyncio.wait_for(anext(stream), 2) for _ in range(21)]
                self.assertEqual([event.sequence for event in observed], [None] * 20 + [1])
                page = await host.get_user_events(execution.invocation_id)
                self.assertEqual(len(page.items), 1)
                self.assertEqual(page.items[0]["payload"], {"value": 190})
                self.assertEqual(page.items[0]["id"], observed[-1].id)
            self.assertFalse(host._user_events.subscriptions)

    async def test_overflow_disconnects_live_reader_without_affecting_history(self):
        """A slow browser cannot block execution or silently lose an interior delta."""
        workflow = Workflow("overflow", nodes=[Node("sum", pieces, stream=Stream(Sum()), user_events=live_then_complete)])
        async with AutoAgentHost(workflows=[workflow], settings=self.settings) as host:
            async with host.subscribe_user_events(capacity=1) as stream:
                execution = await host.execute("overflow", {"value": 10})
                result = await host.join(execution)
                self.assertEqual(result.status, "completed")
                with self.assertRaises(UserEventStreamOverflow):
                    await anext(stream)
                page = await host.get_user_events(execution.invocation_id)
                self.assertEqual(len(page.items), 1)
                self.assertEqual(page.items[0]["sequence"], 1)

    async def test_wait_restart_restores_live_controls_and_continues_history(self):
        """A transient Wait request stays resumable while saved resolution sequences continue."""
        workflow = Workflow("wait-history", nodes=[Node("ask", Wait(Value, Value), user_events=interactions)])
        async with AutoAgentHost(workflows=[workflow], settings=self.settings) as host:
            async with host.subscribe_user_events() as stream:
                execution = await host.execute("wait-history", {"value": 1})
                result = await host.join(execution)
                event = await asyncio.wait_for(anext(stream), 2)
                self.assertFalse(event.persist)
                self.assertEqual((await host.get_user_events(execution.invocation_id)).items, ())
                wait_id = result.waits[0].id
        async with AutoAgentHost(workflows=[workflow], settings=self.settings) as host:
            async with host.subscribe_user_events(execution.invocation_id) as stream:
                current = await host.get_execution(execution)
                self.assertEqual(current.waits[0].id, wait_id)
                await host.respond(execution, wait_id, {"value": 2})
                await host.join(execution)
                observed = [await asyncio.wait_for(anext(stream), 2) for _ in range(2)]
                self.assertEqual([event.kind for event in observed], ["wait_resolved", "output"])
                self.assertEqual([event.sequence for event in observed], [1, 2])
                self.assertEqual([row["sequence"] for row in (await host.get_user_events(execution.invocation_id)).items], [1, 2])

    async def test_database_refuses_transient_event_even_when_called_directly(self):
        """The persistent sink cannot accidentally store a sequence-less observation."""
        async with await DatabaseSink.open(self.settings.database_url) as sink:
            with self.assertRaisesRegex(ValueError, "Transient"):
                await sink.append_user_event(UserEvent("session", "invocation", None, "progress", {}, persist=False))
            await sink.append_user_event(UserEvent("session", "invocation", 1, "completed", {"value": 1}))
            self.assertEqual(len((await sink.user_events("invocation")).items), 1)


class SQLiteHostUserEventTests(HostUserEventContracts, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.settings = HostSettings(database_url="sqlite+aiosqlite:///" + str(Path(self.temp.name) / "events.db"))

    async def asyncTearDown(self):
        self.temp.cleanup()


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLHostUserEventTests(HostUserEventContracts, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.database = PostgreSQLDatabase()
        self.settings = HostSettings(database_url=await self.database.create())

    async def asyncTearDown(self):
        await self.database.close()
