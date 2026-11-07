"""Local queue handoff, slow admission fences and bounded encoding reuse."""

import asyncio
import json
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from autoagent.core.runtime.events import RuntimeEvent, SessionOpened, InvocationStarted
from autoagent.core.runtime.operations import StateDelta, StateOperation
from autoagent.core.runtime.user_events import UserEvent
from autoagent.core.runtime.values import freeze, record_values, thaw
from autoagent.host.persistence.codec import EncodingMemo, encode, assign, canonical
from autoagent.host.persistence.storage import prepare
from autoagent.host.persistence import DatabaseSink, PersistenceConfig, ConflictError, ClosedError
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_host_persistence import PersistenceContracts


class LocalAdmissionContracts:
    asyncSetUp = PersistenceContracts.asyncSetUp
    asyncTearDown = PersistenceContracts.asyncTearDown
    sink = PersistenceContracts.sink
    workflow = PersistenceContracts.workflow
    registered = PersistenceContracts.registered
    database_url = PersistenceContracts.database_url
    remove_database = PersistenceContracts.remove_database

    async def test_local_runtime_handoff_does_not_wait_for_writer_loop(self):
        """Publish a validated Event and its identical retry while the SQL loop is busy."""
        sink = await self.sink(mode='buffered', batch_delay_ms=60000)
        app, workflow, _ = await self.registered(sink)
        writer = sink._writer
        first = RuntimeEvent('local', 1, SessionOpened({}))
        await sink.append(first)
        revision = app.workflow_definition_snapshot(workflow.id).workflow_revision_id
        # Warm the immutable header as a cold admission would.
        async def warm():
            async with writer.storage.engine.connect() as conn:
                await writer.storage.definition_header(conn, 'workflow_revision', revision)
        await writer.run(warm)
        second = RuntimeEvent('local', 2, InvocationStarted(workflow.id, revision, 'a', {}), 'inv')
        entered, release = threading.Event(), threading.Event()

        def busy():
            entered.set()
            release.wait(5)

        writer.loop.call_soon_threadsafe(busy)
        await asyncio.to_thread(entered.wait, 2)
        try:
            self.assertTrue(entered.is_set())
            await asyncio.wait_for(sink.append(second), .5)
            await asyncio.wait_for(sink.append(second), .5)
            with writer.queue_lock:
                self.assertEqual(writer.pending_count, 2)
                self.assertIs(writer.pending_tails[('runtime_event', 'local')].value, second)
        finally:
            release.set()
        await sink.flush()
        self.assertEqual((await sink.get_runtime_event(second.id)).to_record(), second.to_record())

    async def test_slow_admission_excludes_local_overtaking(self):
        """A pending maintenance/admission fence prevents a local producer overtaking it."""
        sink = await self.sink(mode='buffered', batch_delay_ms=60000)
        first = UserEvent('s', 'i', 1, 'progress', {})
        await sink.append_user_event(first)
        writer = sink._writer
        async def hold():
            async with writer.admission:
                entered.set()
                await release.wait()
        async def scenario():
            nonlocal entered, release
            entered, release = asyncio.Event(), asyncio.Event()
            task = asyncio.create_task(hold())
            await entered.wait()
            return task
        entered = release = None
        held = await writer.run(scenario)
        append = asyncio.create_task(sink.append_user_event(UserEvent('s', 'i', 2, 'progress', {})))
        try:
            await asyncio.sleep(.02)
            self.assertFalse(append.done())
        finally:
            async def unlock():
                release.set()
                await held
            await writer.run(unlock)
        await asyncio.wait_for(append, 2)
        await sink.flush()

    async def test_local_producers_share_capacity_and_preserve_all_streams(self):
        """Multiple caller loops use one bounded queue and one durable frontier."""
        sink = await self.sink(mode='buffered', batch_count=8, queue_count=16, batch_delay_ms=0)
        async def produce(stream):
            for sequence in range(1, 33):
                await sink.append_user_event(UserEvent(stream, stream, sequence, 'progress', {'n': sequence}))
        def run(stream):
            asyncio.run(produce(stream))
        await asyncio.gather(*(asyncio.to_thread(run, f'stream-{i}') for i in range(4)))
        await sink.flush()
        health = await sink.health()
        self.assertEqual(health.accepted_frontier, health.durable_frontier)
        self.assertEqual((health.queued_count, health.retained_bytes), (0, 0))
        for index in range(4):
            self.assertEqual([e['sequence'] for e in (await sink.user_events(f'stream-{index}')).items], list(range(1, 33)))

    async def test_local_conflicts_and_closed_sink_do_not_advance_frontier(self):
        """Sequence gaps, changed duplicate content and closing still reject admission."""
        sink = await self.sink(mode='buffered', batch_delay_ms=60000)
        first = UserEvent('s', 'i', 1, 'progress', {})
        await sink.append_user_event(first)
        with self.assertRaises(ConflictError):
            await sink.append_user_event(UserEvent('s', 'i', 3, 'progress', {}))
        with self.assertRaises(ConflictError):
            await sink.append_user_event(UserEvent('s', 'i', 1, 'progress', {'changed': True}, id=first.id, occurred_at_us=first.occurred_at_us))
        self.assertEqual((await sink.health()).accepted_frontier, 1)
        await sink.close()
        with self.assertRaises(ClosedError):
            await sink.append_user_event(UserEvent('s', 'i', 2, 'progress', {}))


class SQLiteLocalAdmissionTests(LocalAdmissionContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLLocalAdmissionTests(LocalAdmissionContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        self.database = PostgreSQLDatabase()
        return await self.database.create()

    async def remove_database(self):
        await self.database.close()


class EncodingReuseTests(unittest.TestCase):
    def test_owned_values_reuse_conversions_and_restore_all_artifact_dependencies(self):
        """Repeated immutable payload/delta roots carry complete dependencies in every Event."""
        value = freeze({'rows': [{'text': '中文' * 2048, 'number': i} for i in range(80)]})
        events = [RuntimeEvent('s', i, SessionOpened(value), delta=StateDelta((StateOperation('replace', ('session', 'context'), value),))) for i in (1, 2)]
        entries = [SimpleNamespace(kind='runtime_event', identity=e.id, value=e, binding=None, anchor=False) for e in events]
        prepared = prepare(entries, PersistenceConfig(artifact_threshold_bytes=1024))
        self.assertEqual(len(prepared), 2)
        for event, entry in zip(events, prepared):
            self.assertTrue(any(artifact['deps'] for artifact in entry.encoded.artifacts.values()))
            record = {**entry.encoded.record, 'payload': json.loads(entry.texts['payload_text']), 'delta': json.loads(entry.texts['delta_text'])}
            def load(digest):
                artifact = entry.encoded.artifacts[digest]
                result = json.loads(artifact['body'])
                for path, child in artifact['deps']:
                    result = assign(result, path, load(child))
                return result
            for path, digest in entry.encoded.refs:
                record = assign(record, path, load(digest))
            self.assertEqual(canonical(record), canonical(event.to_record()))
        detached = events[0].to_record()
        detached['payload']['context']['rows'].clear()
        self.assertEqual(len(events[0].to_record()['payload']['context']['rows']), 80)

    def test_batch_memo_is_bounded_and_does_not_cache_mutable_external_json(self):
        """Eviction and mutable caller changes cannot produce stale encoding results."""
        memo = EncodingMemo(8192)
        for index in range(30):
            with record_values(memo.record_value):
                value = thaw(freeze({'value': str(index) * 100}))
            encode({'payload': value}, 'user_event', 64, memo)
            self.assertLessEqual(memo.converted_bytes + memo.packed_bytes + memo.used + memo.key_bytes, memo.budget)
        external = {'value': 'a' * 100}
        first = encode({'payload': external}, 'user_event', 64, memo)
        external['value'] = 'b' * 100
        second = encode({'payload': external}, 'user_event', 64, memo)
        self.assertNotEqual(set(first.artifacts), set(second.artifacts))

    def test_reused_root_respects_threshold_and_core_conversion_scopes(self):
        """Cached roots preserve threshold changes, and the scope resets after errors."""
        memo = EncodingMemo(65536)
        original = freeze({'value': 'x' * 200})
        try:
            with record_values(memo.record_value):
                record = thaw(original)
                self.assertIs(thaw(original), record)
                low = encode({'payload': record}, 'user_event', 64, memo)
                high = encode({'payload': record}, 'user_event', 1024, memo)
                self.assertTrue(low.artifacts)
                self.assertFalse(high.artifacts)
                raise ValueError('stop')
        except ValueError:
            pass
        self.assertIsNot(thaw(original), record)

    def test_large_strings_can_use_budget_reserved_from_container_reuse(self):
        """Container reuse must not disable caching strings that fit the total budget."""
        memo = EncodingMemo(16384)
        with record_values(memo.record_value):
            record = thaw(freeze({'value': 'small' * 100}))
        encode({'payload': record}, 'user_event', 128, memo)
        text = 'x' * 12000
        self.assertIs(memo.string(text), memo.string(text))
        with record_values(memo.record_value):
            for index in range(20):
                record = thaw(freeze({'n': index, 'value': 'z' * 300}))
                encode({'payload': record}, 'user_event', 128, memo)
                self.assertLessEqual(memo.converted_bytes + memo.packed_bytes + memo.used + memo.key_bytes, memo.budget)


class StartupFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_probe_keeps_startup_failure_cleanup(self):
        """A failed startup still stops its I/O thread when local admission is attempted."""
        from autoagent.host.persistence.storage import Storage
        async def fail(storage):
            raise RuntimeError('startup failed')
        with patch.object(Storage, 'open', fail):
            sink = DatabaseSink('sqlite+aiosqlite:///:memory:', config=PersistenceConfig(mode='buffered'))
            await asyncio.to_thread(sink._writer._startup.exception)
            with self.assertRaisesRegex(RuntimeError, 'startup failed'):
                await sink.append_user_event(UserEvent('s', 'i', 1, 'progress', {}))
            self.assertFalse(sink._writer.thread.is_alive())
