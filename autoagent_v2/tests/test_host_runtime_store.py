from __future__ import annotations

import asyncio
import base64
from dataclasses import replace
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import stat
from tempfile import TemporaryDirectory
import threading
import time
import unittest
from unittest.mock import patch
from typing_extensions import TypedDict

import autoagent.hosting.sqlite as sqlite_hosting
from autoagent import AutoAgentApp, Edge, InputMappingContext, Map, Node, Wait, Workflow
from autoagent.core.runtime import (
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    InMemoryEventJournal,
    InvocationOpened,
    RuntimeEvent,
    RuntimeState,
    StateReducer,
)
from autoagent.hosting import (
    HttpRuntimeEventSink,
    RuntimeEventConflictError,
    RuntimeEventSequenceError,
    RuntimeEventStoreError,
    SQLITE_STORE_SCHEMA_VERSION,
    SQLiteRuntimeStore,
)
from autoagent.hosting._worker import ConcurrentWorker, SerialWorker


class Value(TypedDict):
    value: int


class Batch(TypedDict):
    items: list[Value]


def identity(value: Value) -> Value:
    return value


def map_items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


class _Collector:
    def __init__(self) -> None:
        self.events = []

    async def append(self, event) -> None:
        self.events.append(event)


class _Response:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


class _Client:
    def __init__(self, status_code: int = 202) -> None:
        self.status_code = status_code
        self.calls: list[tuple[str, object, dict[str, str]]] = []
        self.closed = False

    def post(self, url: str, *, json: object, headers: dict[str, str]):
        self.calls.append((url, json, headers))
        return _Response(self.status_code, "rejected")

    def close(self) -> None:
        self.closed = True


def _capture_events():
    collector = _Collector()
    app = AutoAgentApp(runtime_event_sink=collector)
    try:
        workflow = Workflow("captured", nodes=[Node("work", identity)])
        result = app.invoke(workflow, {"value": 1}, session_id="captured-session")
        assert result.status == "completed"
    finally:
        app.close()
    return tuple(collector.events)


def _persist_events(path: Path, events: tuple[RuntimeEvent, ...]) -> None:
    store = SQLiteRuntimeStore(path)
    try:
        for event in events:
            asyncio.run(store.append(event))
    finally:
        store.close()


def _start_store_in_process(path: str, ready, release, result) -> None:
    """Start one Store after every test process reaches the same barrier."""

    store: SQLiteRuntimeStore | None = None
    error: BaseException | None = None
    try:
        ready.put(None)
        if not release.wait(10):
            raise TimeoutError("SQLite initialization barrier timed out.")
        store = SQLiteRuntimeStore(path)
        store.start()
    except BaseException as caught:
        error = caught
    finally:
        if store is not None:
            try:
                store.close()
            except BaseException as cleanup_error:
                if error is None:
                    error = cleanup_error
                else:
                    error.add_note(f"Store cleanup failed: {cleanup_error}")
        result.put(None if error is None else repr(error))


class WorkerBridgeTests(unittest.TestCase):
    def test_cancelled_serial_work_is_not_executed_after_leaving_the_queue(
        self,
    ) -> None:
        """Skip a queued SQLite-style operation whose Future was cancelled."""

        worker = SerialWorker("cancelled-serial-work")
        started = threading.Event()
        release = threading.Event()
        executed = False

        def block() -> None:
            started.set()
            release.wait(1)

        def mark() -> None:
            nonlocal executed
            executed = True

        async def run() -> None:
            first = asyncio.create_task(worker.call_async(block))
            while not started.is_set():
                await asyncio.sleep(0)
            cancelled = asyncio.create_task(worker.call_async(mark))
            await asyncio.sleep(0)
            cancelled.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await cancelled
            release.set()
            await first

        try:
            asyncio.run(run())
            self.assertFalse(executed)
        finally:
            release.set()
            worker.close()

    def test_cancelled_waiter_cannot_notify_a_reused_file_descriptor(self) -> None:
        """Unregister a thread completion notifier before closing its pipe."""

        worker = ConcurrentWorker("cancelled-notifier", max_workers=1)
        started = threading.Event()
        release = threading.Event()

        def block() -> None:
            started.set()
            release.wait(1)

        async def run() -> bytes:
            waiting = asyncio.create_task(worker.call_async(block))
            while not started.is_set():
                await asyncio.sleep(0)
            waiting.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiting
            reader, writer = os.pipe()
            os.set_blocking(reader, False)
            try:
                release.set()
                for _ in range(20):
                    await asyncio.sleep(0)
                try:
                    return os.read(reader, 1)
                except BlockingIOError:
                    return b""
            finally:
                os.close(reader)
                os.close(writer)

        try:
            self.assertEqual(asyncio.run(run()), b"")
        finally:
            release.set()
            worker.close()


class SQLiteRuntimeStoreTests(unittest.TestCase):
    @unittest.skipIf(os.name == "nt", "POSIX permissions only")
    def test_new_store_resources_use_private_permissions(self) -> None:
        """Protect a newly created Store directory, database, WAL, and SHM."""

        with TemporaryDirectory() as directory:
            project = Path(directory) / "project"
            project.mkdir(mode=0o755)
            os.chmod(project, 0o755)
            path = project / ".autoagent" / "runtime.db"
            previous_umask = os.umask(0o002)
            store = SQLiteRuntimeStore(path)
            try:
                store.start()
            finally:
                os.umask(previous_umask)
            try:
                self.assertEqual(stat.S_IMODE(project.stat().st_mode), 0o755)
                self.assertEqual(
                    stat.S_IMODE(path.parent.stat().st_mode),
                    0o700,
                )
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                for suffix in ("-wal", "-shm"):
                    sidecar = path.with_name(path.name + suffix)
                    self.assertTrue(sidecar.is_file())
                    self.assertEqual(stat.S_IMODE(sidecar.stat().st_mode), 0o600)
            finally:
                store.close()

    @unittest.skipIf(os.name == "nt", "POSIX permissions only")
    def test_existing_store_permissions_are_not_changed(self) -> None:
        """Leave user-selected modes unchanged when opening existing resources."""

        with TemporaryDirectory() as directory:
            parent = Path(directory) / "custom"
            parent.mkdir(mode=0o775)
            os.chmod(parent, 0o775)
            path = parent / "runtime.db"
            path.touch(mode=0o644)
            os.chmod(path, 0o644)
            store = SQLiteRuntimeStore(path)
            try:
                store.start()
                self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o775)
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o644)
            finally:
                store.close()

    @unittest.skipIf(os.name == "nt", "POSIX permissions only")
    def test_failed_first_initialization_leaves_private_retryable_file(self) -> None:
        """Close the creation descriptor and safely retry a failed first open."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "runtime.db"
            failed = SQLiteRuntimeStore(path)
            try:
                with patch.object(
                    sqlite_hosting.sqlite3,
                    "connect",
                    side_effect=sqlite3.OperationalError("injected open failure"),
                ):
                    with self.assertRaises(RuntimeEventStoreError):
                        failed.start()
            finally:
                failed.close()
            self.assertTrue(path.is_file())
            self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

            retried = SQLiteRuntimeStore(path)
            try:
                retried.start()
                self.assertEqual(asyncio.run(retried.list_workflows()).items, ())
            finally:
                retried.close()

    def test_failed_schema_transaction_leaves_no_partial_tables(self) -> None:
        """Roll back every DDL statement when first-time schema creation fails."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            failed = SQLiteRuntimeStore(path)
            try:
                with patch.object(
                    sqlite_hosting,
                    "_schema_statements",
                    return_value=(
                        "CREATE TABLE partial_runtime_state(value TEXT);",
                        "THIS IS NOT VALID SQL;",
                    ),
                ):
                    with self.assertRaises(RuntimeEventStoreError):
                        failed.start()
            finally:
                failed.close()
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(sqlite_hosting._sqlite_tables(connection), set())
            finally:
                connection.close()

            retried = SQLiteRuntimeStore(path)
            try:
                retried.start()
                self.assertEqual(asyncio.run(retried.list_workflows()).items, ())
            finally:
                retried.close()

    def test_numeric_query_boundaries_reject_nonfinite_and_oversized_values(
        self,
    ) -> None:
        """Validate SDK Store numbers before they reach SQLite or timers."""

        for refresh in (True, float("nan"), float("inf"), 0, 10**1000):
            with self.subTest(refresh=refresh), self.assertRaises(ValueError):
                SQLiteRuntimeStore("runtime.db", refresh_seconds=refresh)

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                for sequence in (True, -1, 10**100):
                    with self.subTest(after_sequence=sequence), self.assertRaises(
                        ValueError
                    ):
                        asyncio.run(
                            store.list_trace_events(
                                "invocation",
                                after_sequence=sequence,
                            )
                        )
                for sequence in (True, 0, 10**100):
                    with self.subTest(through_sequence=sequence), self.assertRaises(
                        ValueError
                    ):
                        asyncio.run(
                            store.rebuild_state(
                                "session",
                                through_sequence=sequence,
                            )
                        )
                for timeout in (True, -1, float("nan"), float("inf"), 10**1000):
                    with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                        asyncio.run(
                            store.wait_for_trace(
                                "invocation",
                                after_sequence=0,
                                timeout=timeout,
                            )
                        )
            finally:
                store.close()

    def test_concurrent_store_start_initializes_one_schema(self) -> None:
        """Initialize one new database safely from concurrent Host starters."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            stores = [SQLiteRuntimeStore(path) for _ in range(8)]
            barrier = threading.Barrier(len(stores))
            errors: list[BaseException] = []

            def start(store: SQLiteRuntimeStore) -> None:
                try:
                    barrier.wait()
                    store.start()
                except BaseException as error:
                    errors.append(error)

            threads = [threading.Thread(target=start, args=(store,)) for store in stores]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            for store in stores:
                store.close()
            self.assertEqual(errors, [])

    def test_concurrent_processes_initialize_one_schema(self) -> None:
        """Recheck schema emptiness under SQLite's cross-process write lock."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "runtime.db"
            context = multiprocessing.get_context("spawn")
            ready = context.Queue()
            release = context.Event()
            result = context.Queue()
            processes = [
                context.Process(
                    target=_start_store_in_process,
                    args=(str(path), ready, release, result),
                )
                for _ in range(4)
            ]
            try:
                for process in processes:
                    process.start()
                for _ in processes:
                    ready.get(timeout=10)
                release.set()
                for process in processes:
                    process.join(10)
                self.assertTrue(
                    all(not process.is_alive() for process in processes),
                    "SQLite initializer process did not terminate.",
                )
                self.assertEqual(
                    [result.get(timeout=2) for _ in processes],
                    [None] * len(processes),
                )
                self.assertTrue(all(process.exitcode == 0 for process in processes))
                connection = sqlite3.connect(path)
                try:
                    self.assertEqual(
                        connection.execute("PRAGMA integrity_check").fetchone()[0],
                        "ok",
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT value FROM schema_metadata WHERE key = ?",
                            ("schema_version",),
                        ).fetchone()[0],
                        str(SQLITE_STORE_SCHEMA_VERSION),
                    )
                finally:
                    connection.close()
            finally:
                release.set()
                for process in processes:
                    if process.is_alive():
                        process.terminate()
                    process.join(2)
                ready.close()
                result.close()

    def test_read_only_store_handles_uri_characters_without_wrong_files(self) -> None:
        """Percent-encode SQLite file URIs while retaining live read access."""

        with TemporaryDirectory() as directory:
            root = Path(directory)
            for name, wrong in (
                ("trace?name.db", "trace"),
                ("trace#name.db", "trace"),
                ("trace%20name.db", "trace name.db"),
            ):
                with self.subTest(name=name):
                    path = root / name
                    writer = SQLiteRuntimeStore(path)
                    writer.start()
                    writer.close()
                    reader = SQLiteRuntimeStore.open_read_only(path)
                    try:
                        reader.start()
                        page = asyncio.run(reader.list_workflows())
                        self.assertEqual(page.items, ())
                    finally:
                        reader.close()
                    self.assertTrue(path.is_file())
                    self.assertFalse((root / wrong).exists())

    def test_read_only_store_rejects_an_incomplete_claimed_schema(self) -> None:
        """Fail at startup when a version marker lacks required query tables."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "incomplete.db"
            connection = sqlite3.connect(path)
            try:
                connection.executescript(
                    """
                    CREATE TABLE schema_metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    """
                )
                connection.execute(
                    "INSERT INTO schema_metadata(key, value) VALUES (?, ?)",
                    ("schema_version", str(SQLITE_STORE_SCHEMA_VERSION)),
                )
                connection.commit()
            finally:
                connection.close()
            store = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "missing required tables",
                ):
                    store.start()
            finally:
                store.close()

    def test_start_does_not_modify_an_unsupported_existing_database(self) -> None:
        """Reject another schema version before running any Runtime DDL."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "unsupported.db"
            connection = sqlite3.connect(path)
            try:
                connection.executescript(
                    """
                    CREATE TABLE schema_metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    INSERT INTO schema_metadata(key, value)
                    VALUES ('schema_version', '999');
                    CREATE TABLE application_data(value TEXT);
                    """
                )
            finally:
                connection.close()
            before = path.read_bytes()
            store = SQLiteRuntimeStore(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Unsupported SQLite Store schema",
                ):
                    store.start()
            finally:
                store.close()
            self.assertEqual(path.read_bytes(), before)
            connection = sqlite3.connect(path)
            try:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
            finally:
                connection.close()
            self.assertEqual(tables, {"schema_metadata", "application_data"})

    def test_writable_start_wraps_a_malformed_database(self) -> None:
        """Expose invalid SQLite files through the stable Store error boundary."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "not-sqlite.db"
            path.write_bytes(b"not a sqlite database")
            store = SQLiteRuntimeStore(path)
            try:
                with self.assertRaises(RuntimeEventStoreError):
                    store.start()
            finally:
                store.close()

    def test_start_rejects_a_versioned_schema_with_missing_columns(self) -> None:
        """Validate the exact query columns before accepting an existing Store."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "damaged.db"
            writer = SQLiteRuntimeStore(path)
            writer.start()
            writer.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "ALTER TABLE workflow_definitions DROP COLUMN definition_hash"
                )
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "missing columns: definition_hash",
                ):
                    reader.start()
            finally:
                reader.close()

    def test_start_rejects_an_index_with_the_right_name_but_wrong_shape(self) -> None:
        """Validate index columns and constraints, not only object names."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "damaged-index.db"
            writer = SQLiteRuntimeStore(path)
            writer.start()
            writer.close()
            connection = sqlite3.connect(path)
            try:
                connection.executescript(
                    """
                    DROP INDEX runtime_events_invocation;
                    CREATE INDEX runtime_events_invocation
                    ON runtime_events(session_id);
                    """
                )
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "incompatible schema objects: runtime_events_invocation",
                ):
                    reader.start()
            finally:
                reader.close()

    def test_start_rejects_triggers_on_managed_runtime_tables(self) -> None:
        """Prevent an unknown trigger from corrupting a durability acknowledgement."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "triggered.db"
            writer = SQLiteRuntimeStore(path)
            writer.start()
            writer.close()
            connection = sqlite3.connect(path)
            try:
                connection.executescript(
                    """
                    CREATE TRIGGER corrupt_session_head
                    AFTER UPDATE OF last_event_digest ON sessions
                    BEGIN
                        UPDATE sessions SET last_event_digest = 'bad'
                        WHERE session_id = NEW.session_id;
                    END;
                    """
                )
            finally:
                connection.close()
            reopened = SQLiteRuntimeStore(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "incompatible schema objects: corrupt_session_head",
                ):
                    reopened.start()
            finally:
                reopened.close()

    def test_async_close_keeps_the_caller_event_loop_responsive(self) -> None:
        """Drain a blocked SQLite owner thread without blocking async peers."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            store.start()
            original = store._close_connection
            release = threading.Event()

            def delayed_close() -> None:
                release.wait(1)
                original()

            store._close_connection = delayed_close  # type: ignore[method-assign]

            async def run() -> bool:
                progressed = False

                async def peer() -> None:
                    nonlocal progressed
                    await asyncio.sleep(0)
                    progressed = True

                peer_task = asyncio.create_task(peer())
                timer = threading.Timer(0.05, release.set)
                timer.start()
                try:
                    await store.aclose()
                    return progressed
                finally:
                    await peer_task
                    timer.cancel()

            self.assertTrue(asyncio.run(run()))

    def test_cancelled_async_close_preserves_the_shared_failure(self) -> None:
        """Let a later closer observe cleanup failure after waiter cancellation."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            store.start()
            original = store._close_connection
            entered = threading.Event()
            release = threading.Event()
            failure = RuntimeError("sqlite close failed")

            def fail_close() -> None:
                entered.set()
                release.wait(1)
                original()
                raise failure

            store._close_connection = fail_close  # type: ignore[method-assign]

            async def run() -> None:
                closing = asyncio.create_task(store.aclose())
                while not entered.is_set():
                    await asyncio.sleep(0)
                closing.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await closing
                release.set()
                with self.assertRaises(RuntimeError) as raised:
                    await store.aclose()
                self.assertIs(raised.exception, failure)

            try:
                asyncio.run(run())
                with self.assertRaises(RuntimeError) as raised:
                    store.close()
                self.assertIs(raised.exception, failure)
            finally:
                release.set()

    def test_store_indexes_trace_and_rebuilds_state(self) -> None:
        """Persist one Invocation and rebuild its exact current State."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            app = AutoAgentApp(runtime_event_sink=store)
            try:
                workflow = Workflow("stored", nodes=[Node("work", identity)])
                compiled = app.register_workflow(workflow)
                store.save_workflow(
                    app.workflow_definition_snapshot(compiled.workflow_revision_id)
                )
                result = app.invoke(
                    "stored",
                    {"value": 3},
                    session_id="stored-session",
                )

                state = asyncio.run(store.rebuild_state("stored-session"))
                self.assertEqual(state.invocation.status, "completed")
                self.assertEqual(state.invocation.output, {"value": 3})

                invocation = asyncio.run(
                    store.get_invocation(result.invocation_id)
                )
                self.assertEqual(invocation["status"], "completed")
                traces = asyncio.run(
                    store.list_trace_events(result.invocation_id)
                )
                self.assertGreater(len(traces), 3)
                self.assertEqual(traces[-1].status, "completed")

                workflows = asyncio.run(store.list_workflows())
                self.assertEqual(workflows.items[0]["workflow_id"], "stored")
            finally:
                app.close()
                store.close()

    def test_rebuild_uses_one_typed_state_decode_for_a_sealed_prefix(self) -> None:
        """Replay accepted Event operations once without decoding every prefix."""

        events = _capture_events()
        strict = RuntimeState()
        for event in events:
            strict = StateReducer().apply(strict, event)

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with patch.object(
                    RuntimeState,
                    "from_record",
                    wraps=RuntimeState.from_record,
                ) as decode:
                    rebuilt = asyncio.run(
                        reader.rebuild_state("captured-session")
                    )
                self.assertEqual(rebuilt, strict)
                self.assertEqual(decode.call_count, 1)
            finally:
                reader.close()

    def test_revision_filtered_sessions_use_that_revision_latest_invocation(
        self,
    ) -> None:
        """Keep a revision-filtered Session summary internally consistent."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            app = AutoAgentApp(runtime_event_sink=store)
            try:
                first = app.register_workflow(
                    Workflow("first-workflow", nodes=[Node("work", identity)])
                )
                second = app.register_workflow(
                    Workflow("second-workflow", nodes=[Node("work", identity)])
                )
                first_result = app.invoke(
                    first.workflow_revision_id,
                    {"value": 1},
                    session_id="shared-session",
                )
                second_result = app.invoke(
                    second.workflow_revision_id,
                    {"value": 2},
                    session_id="shared-session",
                )
                app.invoke(
                    first.workflow_revision_id,
                    {"value": 3},
                    session_id="first-only-session",
                )

                first_page = asyncio.run(
                    store.list_sessions(
                        workflow_revision_id=first.workflow_revision_id
                    )
                )
                second_page = asyncio.run(
                    store.list_sessions(
                        workflow_revision_id=second.workflow_revision_id
                    )
                )
                unfiltered = asyncio.run(store.list_sessions())
                first_sessions = {
                    item["session_id"]: item for item in first_page.items
                }
                unfiltered_sessions = {
                    item["session_id"]: item for item in unfiltered.items
                }

                self.assertEqual(
                    first_sessions["shared-session"]["current_invocation_id"],
                    first_result.invocation_id,
                )
                self.assertEqual(
                    first_sessions["shared-session"]["workflow_revision_id"],
                    first.workflow_revision_id,
                )
                self.assertEqual(
                    first_sessions["shared-session"]["invocation_count"], 1
                )
                self.assertEqual(
                    second_page.items[0]["current_invocation_id"],
                    second_result.invocation_id,
                )
                self.assertEqual(
                    unfiltered_sessions["shared-session"]["current_invocation_id"],
                    second_result.invocation_id,
                )
                self.assertEqual(
                    unfiltered_sessions["shared-session"]["invocation_count"], 2
                )
                first_cursor_page = asyncio.run(
                    store.list_sessions(
                        workflow_revision_id=first.workflow_revision_id,
                        limit=1,
                    )
                )
                self.assertIsNotNone(first_cursor_page.next_cursor)
                with self.assertRaises(ValueError):
                    asyncio.run(
                        store.list_sessions(
                            workflow_revision_id=second.workflow_revision_id,
                            cursor=first_cursor_page.next_cursor,
                        )
                    )
            finally:
                app.close()
                store.close()

    def test_summary_queries_use_one_lightweight_canonical_session_pass(self) -> None:
        """Keep summary reads off the full Reducer and batch Invocation pages."""

        collector = _Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            workflow = app.register_workflow(
                Workflow("summary-batch", nodes=[Node("work", identity)])
            )
            first = app.invoke(
                workflow.workflow_revision_id,
                {"value": 1},
                session_id="summary-batch-session",
            )
            second = app.invoke(
                workflow.workflow_revision_id,
                {"value": 2},
                session_id="summary-batch-session",
            )
        finally:
            app.close()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, tuple(collector.events))
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with patch.object(
                    SQLiteRuntimeStore,
                    "_rebuild_state_with_connection",
                    side_effect=AssertionError("summary query rebuilt Runtime State"),
                ):
                    asyncio.run(reader.get_session(first.session_id))
                    asyncio.run(reader.list_sessions())
                    asyncio.run(reader.get_invocation(second.invocation_id))
                    with patch.object(
                        SQLiteRuntimeStore,
                        "_canonical_session_projection",
                        wraps=SQLiteRuntimeStore._canonical_session_projection,
                    ) as canonical_scan:
                        page = asyncio.run(
                            reader.list_invocations(first.session_id, limit=50)
                        )
                    self.assertEqual(len(page.items), 2)
                    self.assertEqual(canonical_scan.call_count, 1)
            finally:
                reader.close()

    def test_summary_queries_bind_counts_and_times_to_canonical_logs(self) -> None:
        """Reject coordinated summary metadata that canonical Logs do not derive."""

        events = _capture_events()
        invocation_id = next(
            event.invocation_id for event in events if event.invocation_id is not None
        )
        session_id = events[0].session_id
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE sessions SET invocation_count = invocation_count + 1, "
                    "created_at_ns = created_at_ns + 1, updated_at_ns = updated_at_ns + 1 "
                    "WHERE session_id = ?",
                    (session_id,),
                )
                connection.execute(
                    "UPDATE invocations SET created_at_ns = created_at_ns + 1, "
                    "updated_at_ns = updated_at_ns + 1, ended_at_ns = ended_at_ns + 1 "
                    "WHERE invocation_id = ?",
                    (invocation_id,),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(reader.get_session(session_id))
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(reader.get_invocation(invocation_id))
            finally:
                reader.close()

    def test_append_is_idempotent_and_rejects_identity_conflicts(self) -> None:
        """Accept exact retry once while rejecting reused Event identity."""

        event = _capture_events()[0]
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                asyncio.run(store.append(event))
                asyncio.run(store.append(event))
                traces = asyncio.run(
                    store.list_trace_events(event.invocation_id)
                )
                self.assertEqual(
                    len(traces),
                    sum(
                        log.invocation_id == event.invocation_id
                        for log in event.logs
                    ),
                )

                with self.assertRaises(RuntimeEventConflictError):
                    asyncio.run(store.append(replace(event, sequence=2)))
            finally:
                store.close()

    def test_restart_rejects_corrupt_trace_projection_before_append(self) -> None:
        """Validate existing Trace projections before a restarted writer appends."""

        events = _capture_events()
        self.assertGreaterEqual(len(events), 2)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            first = SQLiteRuntimeStore(path)
            try:
                asyncio.run(first.append(events[0]))
            finally:
                first.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE sessions SET trace_count = trace_count + 7 "
                    "WHERE session_id = ?",
                    (events[0].session_id,),
                )
                connection.execute(
                    "UPDATE invocations SET trace_count = trace_count + 7 "
                    "WHERE invocation_id = ?",
                    (events[0].invocation_id,),
                )
                connection.commit()
            finally:
                connection.close()

            restarted = SQLiteRuntimeStore(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Trace count",
                ):
                    asyncio.run(restarted.append(events[1]))
            finally:
                restarted.close()
            connection = sqlite3.connect(path)
            try:
                count = connection.execute(
                    "SELECT COUNT(*) FROM runtime_events"
                ).fetchone()[0]
                self.assertEqual(count, 1)
            finally:
                connection.close()

    def test_exact_retry_rejects_a_missing_historical_invocation(self) -> None:
        """Do not acknowledge a historical Event whose projection was deleted."""

        collector = _Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            workflow = app.register_workflow(
                Workflow("retry-history", nodes=[Node("work", identity)])
            )
            first = app.invoke(
                workflow.workflow_revision_id,
                {"value": 1},
                session_id="retry-history-session",
            )
            first_event_count = len(collector.events)
            app.invoke(
                workflow.workflow_revision_id,
                {"value": 2},
                session_id="retry-history-session",
            )
        finally:
            app.close()
        retried = next(
            event
            for event in collector.events[:first_event_count]
            if event.invocation_id == first.invocation_id
        )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, tuple(collector.events))
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "DELETE FROM invocations WHERE invocation_id = ?",
                    (first.invocation_id,),
                )
                connection.commit()
            finally:
                connection.close()
            store = SQLiteRuntimeStore(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "no materialized projection",
                ):
                    asyncio.run(store.append(retried))
            finally:
                store.close()

    def test_checkpoint_rejects_consistently_forged_child_roots(self) -> None:
        """Anchor a Child root to its parent plan, not three agreeing projections."""

        collector = _Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            child = Workflow("root-child", nodes=[Node("work", identity)])
            parent = Workflow("root-parent", nodes=[Node("child", child)])
            app.invoke(parent, {"value": 1}, session_id="root-parent-session")
        finally:
            app.close()
        plan = next(
            log.payload
            for event in collector.events
            for log in event.logs
            if isinstance(log.payload, ChildInvocationPlanned)
        )
        child_session_id = plan.units[0].child_session_id

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, tuple(collector.events))
            connection = sqlite3.connect(path)
            try:
                for table in ("sessions", "invocations", "session_ownership"):
                    connection.execute(
                        f"UPDATE {table} SET root_session_id = ? WHERE session_id = ?",
                        ("forged-root", child_session_id),
                    )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Child ownership projection",
                ):
                    asyncio.run(reader.rebuild_checkpoint("root-parent-session"))
            finally:
                reader.close()

    def test_child_first_event_rejects_reparented_ownership(self) -> None:
        """Bind a planned Child to the exact canonical parent plan before admission."""

        collector = _Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            child = Workflow("owned-child", nodes=[Node("work", identity)])
            parent = Workflow("owned-parent", nodes=[Node("child", child)])
            app.invoke(parent, {"value": 1}, session_id="original-root")
            app.invoke(
                Workflow("other-parent", nodes=[Node("work", identity)]),
                {"value": 2},
                session_id="other-root",
            )
        finally:
            app.close()
        planned_event = next(
            event
            for event in collector.events
            if event.session_id == "original-root"
            and any(
                isinstance(log.payload, ChildInvocationPlanned)
                for log in event.logs
            )
        )
        plan = next(
            log.payload
            for log in planned_event.logs
            if isinstance(log.payload, ChildInvocationPlanned)
        )
        child_session_id = plan.units[0].child_session_id
        child_first = next(
            event
            for event in collector.events
            if event.session_id == child_session_id and event.sequence == 1
        )
        admitted = tuple(
            event
            for event in collector.events
            if (
                event.session_id == "other-root"
                or (
                    event.session_id == "original-root"
                    and event.sequence <= planned_event.sequence
                )
            )
        )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, admitted)
            connection = sqlite3.connect(path)
            try:
                other_invocation_id = connection.execute(
                    "SELECT current_invocation_id FROM sessions "
                    "WHERE session_id = 'other-root'"
                ).fetchone()[0]
                connection.execute(
                    """
                    UPDATE session_ownership
                    SET root_session_id = 'other-root',
                        parent_session_id = 'other-root',
                        parent_invocation_id = ?
                    WHERE session_id = ?
                    """,
                    (other_invocation_id, child_session_id),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "parent plan",
                ):
                    asyncio.run(
                        reader.list_child_sessions(other_invocation_id)
                    )
            finally:
                reader.close()
            store = SQLiteRuntimeStore(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "parent plan",
                ):
                    asyncio.run(store.append(child_first))
            finally:
                store.close()

    def test_root_ownership_rejects_child_only_fields(self) -> None:
        """Reject a structural Root carrying fields reserved for Child ownership."""

        events = _capture_events()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE session_ownership SET phase = 'planned' "
                    "WHERE session_id = ?",
                    (events[0].session_id,),
                )
                connection.commit()
            finally:
                connection.close()
            store = SQLiteRuntimeStore(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Root Session ownership",
                ):
                    asyncio.run(store.append(events[-1]))
            finally:
                store.close()

    def test_child_ownership_phase_is_derived_from_parent_events(self) -> None:
        """Reject a valid-looking Child phase that disagrees with parent Events."""

        collector = _Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            child = Workflow("phase-child", nodes=[Node("work", identity)])
            parent = Workflow("phase-parent", nodes=[Node("child", child)])
            app.invoke(parent, {"value": 1}, session_id="phase-root")
        finally:
            app.close()
        plan = next(
            log.payload
            for event in collector.events
            for log in event.logs
            if isinstance(log.payload, ChildInvocationPlanned)
        )
        child_session_id = plan.units[0].child_session_id
        child_tail = next(
            event
            for event in reversed(collector.events)
            if event.session_id == child_session_id
        )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, tuple(collector.events))
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE session_ownership SET phase = 'accepted' "
                    "WHERE session_id = ?",
                    (child_session_id,),
                )
                connection.commit()
            finally:
                connection.close()
            store = SQLiteRuntimeStore(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "phase does not match canonical Events",
                ):
                    asyncio.run(store.append(child_tail))
            finally:
                store.close()

    def test_child_append_requires_the_complete_parent_plan_prefix(self) -> None:
        """Reject Child progress when an Event before its parent plan is missing."""

        collector = _Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            child = Workflow("prefix-child", nodes=[Node("work", identity)])
            parent = Workflow("prefix-parent", nodes=[Node("child", child)])
            app.invoke(parent, {"value": 1}, session_id="prefix-root")
        finally:
            app.close()
        planned_event = next(
            event
            for event in collector.events
            if event.session_id == "prefix-root"
            and any(
                isinstance(log.payload, ChildInvocationPlanned)
                for log in event.logs
            )
        )
        self.assertGreater(planned_event.sequence, 1)
        plan = next(
            log.payload
            for log in planned_event.logs
            if isinstance(log.payload, ChildInvocationPlanned)
        )
        child_session_id = plan.units[0].child_session_id
        child_tail = next(
            event
            for event in reversed(collector.events)
            if event.session_id == child_session_id
        )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, tuple(collector.events))
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "DELETE FROM runtime_events "
                    "WHERE session_id = ? AND sequence = ?",
                    ("prefix-root", planned_event.sequence - 1),
                )
                connection.commit()
            finally:
                connection.close()
            store = SQLiteRuntimeStore(path)
            try:
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(store.append(child_tail))
            finally:
                store.close()

    def test_trace_completeness_is_anchored_to_runtime_logs(self) -> None:
        """Reject a removed terminal Trace even when every SQL head is forged."""

        events = _capture_events()
        invocation_id = next(
            event.invocation_id for event in events if event.invocation_id is not None
        )
        session_id = events[0].session_id
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            connection.row_factory = sqlite3.Row
            try:
                connection.execute(
                    "DELETE FROM trace_events WHERE invocation_id = ? "
                    "AND kind = 'invocation.completed'",
                    (invocation_id,),
                )
                session_traces = connection.execute(
                    "SELECT id, trace_sequence, record_json FROM trace_events "
                    "WHERE session_id = ? ORDER BY trace_sequence",
                    (session_id,),
                ).fetchall()
                invocation_traces = connection.execute(
                    "SELECT trace_sequence FROM trace_events "
                    "WHERE invocation_id = ? ORDER BY trace_sequence",
                    (invocation_id,),
                ).fetchall()
                tail = session_traces[-1]
                tail_digest = hashlib.sha256(
                    tail["record_json"].encode("utf-8")
                ).hexdigest()
                connection.execute(
                    """
                    UPDATE sessions
                    SET trace_count = ?, last_trace_sequence = ?,
                        last_trace_id = ?, last_trace_digest = ?
                    WHERE session_id = ?
                    """,
                    (
                        len(session_traces),
                        tail["trace_sequence"],
                        tail["id"],
                        tail_digest,
                        session_id,
                    ),
                )
                connection.execute(
                    """
                    UPDATE invocations
                    SET trace_count = ?, last_trace_sequence = ?, status = 'running'
                    WHERE invocation_id = ?
                    """,
                    (
                        len(invocation_traces),
                        invocation_traces[-1]["trace_sequence"],
                        invocation_id,
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "canonical Runtime logs",
                ):
                    asyncio.run(
                        reader.list_trace_events(
                            invocation_id,
                            after_sequence=0,
                            limit=1_000,
                        )
                    )
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(
                        reader.terminal_trace_status(
                            invocation_id,
                            through_sequence=tail["trace_sequence"],
                        )
                    )
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(reader.latest_trace_sequence(invocation_id))
            finally:
                reader.close()

    def test_trace_page_decodes_each_batched_source_event_once(self) -> None:
        """Avoid per-Trace source decoding when one Event carries many Logs."""

        events = _capture_events()
        first = events[0]
        last = events[-1]
        batched = replace(
            last,
            id="batched-runtime-event",
            sequence=1,
            previous_event_id=None,
            previous_event_digest=None,
            from_state_version=first.from_state_version,
            operation_batches=tuple(
                batch for event in events for batch in event.operation_batches
            ),
            logs=tuple(log for event in events for log in event.logs),
        )
        assert last.invocation_id is not None

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                asyncio.run(store.append(batched))
                with patch.object(
                    sqlite_hosting,
                    "_decode_verified_runtime_event",
                    wraps=sqlite_hosting._decode_verified_runtime_event,
                ) as source_decode:
                    traces = asyncio.run(
                        store.list_trace_events(last.invocation_id, limit=100)
                    )
                expected = sum(
                    log.invocation_id == last.invocation_id
                    for log in batched.logs
                )
                self.assertEqual(len(traces), expected)
                self.assertLess(source_decode.call_count, len(traces))
            finally:
                store.close()

    def test_forward_trace_list_rejects_a_deleted_middle_row(self) -> None:
        """Reject a forward Trace page that would silently skip a missing row."""

        events = _capture_events()
        invocation_id = next(
            event.invocation_id for event in events if event.invocation_id is not None
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            try:
                sequences = [
                    row[0]
                    for row in connection.execute(
                        "SELECT trace_sequence FROM trace_events "
                        "WHERE invocation_id = ? ORDER BY trace_sequence",
                        (invocation_id,),
                    ).fetchall()
                ]
                self.assertGreater(len(sequences), 2)
                connection.execute(
                    "DELETE FROM trace_events "
                    "WHERE invocation_id = ? AND trace_sequence = ?",
                    (invocation_id, sequences[len(sequences) // 2]),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                missing = sequences[len(sequences) // 2]
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Trace sequence is incomplete",
                ):
                    asyncio.run(
                        reader.list_trace_events(
                            invocation_id,
                            after_sequence=missing - 1,
                            limit=1,
                        )
                    )
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Trace sequence is incomplete",
                ):
                    asyncio.run(
                        reader.list_trace_events(invocation_id, limit=1_000)
                    )
            finally:
                reader.close()

    def test_append_rolls_back_canonical_event_when_projection_fails(self) -> None:
        """Roll back the canonical row when any transactional projection fails."""

        event = _capture_events()[0]
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            original = store._index_event

            def fail_projection(*_args: object) -> None:
                raise RuntimeError("projection failed")

            store._index_event = fail_projection  # type: ignore[method-assign]
            try:
                with self.assertRaisesRegex(RuntimeError, "projection failed"):
                    asyncio.run(store.append(event))
                with self.assertRaises(KeyError):
                    asyncio.run(store.rebuild_state(event.session_id))
                store._index_event = original  # type: ignore[method-assign]
                asyncio.run(store.append(event))
                state = asyncio.run(store.rebuild_state(event.session_id))
                self.assertEqual(state.sequence, event.sequence)
                traces = asyncio.run(
                    store.list_trace_events(event.invocation_id)
                )
                self.assertEqual(
                    len(traces),
                    sum(
                        log.invocation_id == event.invocation_id
                        for log in event.logs
                    ),
                )
            finally:
                store.close()

    def test_append_rejects_an_event_that_cannot_rebuild_runtime_state(self) -> None:
        """Reject a hash-valid Event whose operations violate reducer invariants."""

        events = _capture_events()
        record = events[1].to_record()
        batches = record["operation_batches"]
        assert isinstance(batches, list)
        operations = batches[0]["operations"]
        assert isinstance(operations, list)
        operations[0]["value"] = events[1].occurred_at_ns + 1
        forged = RuntimeEvent.from_record(record)
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                asyncio.run(store.append(events[0]))
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "cannot be replayed",
                ):
                    asyncio.run(store.append(forged))
                asyncio.run(store.append(events[1]))
                state = asyncio.run(store.rebuild_state(events[1].session_id))
                self.assertEqual(state.sequence, events[1].sequence)
            finally:
                store.close()

    def test_append_rejects_operations_that_disagree_with_runtime_logs(self) -> None:
        """Keep canonical State and log-derived projections semantically identical."""

        events = _capture_events()
        record = events[1].to_record()
        batches = record["operation_batches"]
        assert isinstance(batches, list)
        operations = batches[0]["operations"]
        assert isinstance(operations, list)
        batches[0]["operations"] = operations[:1]
        forged = RuntimeEvent.from_record(record)
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                asyncio.run(store.append(events[0]))
                before = asyncio.run(
                    store.list_trace_events(events[0].invocation_id)
                )
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "cannot be replayed",
                ):
                    asyncio.run(store.append(forged))
                state = asyncio.run(store.rebuild_state(events[0].session_id))
                after = asyncio.run(
                    store.list_trace_events(events[0].invocation_id)
                )
                self.assertEqual(state.sequence, events[0].sequence)
                self.assertEqual(after, before)
            finally:
                store.close()

    def test_corrupt_projection_values_are_store_errors(self) -> None:
        """Classify malformed persisted projection values as Store corruption."""

        event = _capture_events()[0]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            try:
                asyncio.run(store.append(event))
            finally:
                store.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE sessions SET invocation_count = 'oops'"
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(reader.get_session(event.session_id))
            finally:
                reader.close()

    def test_missing_materialized_identity_is_store_corruption(self) -> None:
        """Distinguish deleted projections from genuinely unknown identities."""

        events = _capture_events()
        invocation_id = next(
            event.invocation_id for event in events if event.invocation_id is not None
        )
        session_id = events[0].session_id
        for table, operation, message in (
            (
                "invocations",
                lambda store: store.get_invocation(invocation_id),
                "no Invocation projection",
            ),
            (
                "sessions",
                lambda store: store.get_session(session_id),
                "no Session projection",
            ),
        ):
            with self.subTest(table=table), TemporaryDirectory() as directory:
                path = Path(directory) / "runtime.db"
                _persist_events(path, events)
                connection = sqlite3.connect(path)
                try:
                    identity_column = (
                        "invocation_id" if table == "invocations" else "session_id"
                    )
                    identity = invocation_id if table == "invocations" else session_id
                    connection.execute(
                        f"DELETE FROM {table} WHERE {identity_column} = ?",
                        (identity,),
                    )
                    connection.commit()
                finally:
                    connection.close()
                reader = SQLiteRuntimeStore.open_read_only(path)
                try:
                    with self.assertRaisesRegex(RuntimeEventStoreError, message):
                        asyncio.run(operation(reader))
                finally:
                    reader.close()

    def test_summary_queries_reject_self_consistent_forged_projections(self) -> None:
        """Bind Session and Invocation summaries back to canonical Runtime Events."""

        events = _capture_events()
        invocation_id = next(
            event.invocation_id for event in events if event.invocation_id is not None
        )
        session_id = events[0].session_id
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE invocations SET workflow_id = 'forged-workflow', "
                    "status = 'running' WHERE invocation_id = ?",
                    (invocation_id,),
                )
                connection.execute(
                    "UPDATE sessions SET root_session_id = 'forged-root' "
                    "WHERE session_id = ?",
                    (session_id,),
                )
                connection.execute(
                    "UPDATE session_ownership SET root_session_id = 'forged-root' "
                    "WHERE session_id = ?",
                    (session_id,),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                operations = (
                    lambda: reader.get_session(session_id),
                    lambda: reader.list_sessions(),
                    lambda: reader.get_invocation(invocation_id),
                    lambda: reader.list_invocations(session_id),
                )
                for operation in operations:
                    with self.subTest(operation=operation), self.assertRaises(
                        RuntimeEventStoreError
                    ):
                        asyncio.run(operation())
            finally:
                reader.close()

    def test_corrupt_projection_text_is_a_store_error(self) -> None:
        """Reject BLOB values before they escape a typed Store projection."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            app = AutoAgentApp()
            try:
                compiled = app.register_workflow(
                    Workflow("projection-text", nodes=[Node("work", identity)])
                )
                store.save_workflow(
                    app.workflow_definition_snapshot(
                        compiled.workflow_revision_id
                    )
                )
            finally:
                app.close()
                store.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE workflow_definitions SET workflow_id = ?",
                    (sqlite3.Binary(b"invalid"),),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(reader.list_workflows())
            finally:
                reader.close()

    def test_last_runtime_event_has_an_independent_integrity_digest(self) -> None:
        """Detect canonical corruption even when no later chain link exists."""

        event = _capture_events()[0]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            try:
                asyncio.run(store.append(event))
            finally:
                store.close()
            connection = sqlite3.connect(path)
            try:
                record = json.loads(
                    connection.execute(
                        "SELECT record_json FROM runtime_events WHERE id = ?",
                        (event.id,),
                    ).fetchone()[0]
                )
                record["occurred_at_ns"] += 1
                connection.execute(
                    "UPDATE runtime_events SET record_json = ? WHERE id = ?",
                    (
                        json.dumps(
                            record,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        event.id,
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "digest does not match",
                ):
                    asyncio.run(reader.rebuild_state(event.session_id))
            finally:
                reader.close()

    def test_session_head_detects_a_deleted_or_relocated_final_event(self) -> None:
        """Never recover an older State when the durable Session tail disappears."""

        events = _capture_events()
        final = events[-1]
        for mutation in ("delete", "relocate"):
            with self.subTest(mutation=mutation), TemporaryDirectory() as directory:
                path = Path(directory) / "runtime.db"
                _persist_events(path, events)
                connection = sqlite3.connect(path)
                try:
                    if mutation == "delete":
                        connection.execute(
                            "DELETE FROM runtime_events WHERE id = ?",
                            (final.id,),
                        )
                    else:
                        connection.execute(
                            "UPDATE runtime_events SET session_id = ? WHERE id = ?",
                            ("relocated-session", final.id),
                        )
                    connection.commit()
                finally:
                    connection.close()
                store = SQLiteRuntimeStore(path)
                try:
                    with self.assertRaisesRegex(
                        RuntimeEventStoreError,
                        "Session head",
                    ):
                        asyncio.run(store.rebuild_state(final.session_id))
                    with self.assertRaises(RuntimeEventStoreError):
                        asyncio.run(store.append(final))
                finally:
                    store.close()

    def test_external_middle_chain_damage_invalidates_the_writer_cache(self) -> None:
        """Never acknowledge a new Event from State cached before external damage."""

        events = _capture_events()
        self.assertGreaterEqual(len(events), 5)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            try:
                for event in events[:4]:
                    asyncio.run(store.append(event))
                connection = sqlite3.connect(path)
                try:
                    connection.execute(
                        "DELETE FROM runtime_events WHERE id = ?",
                        (events[1].id,),
                    )
                    connection.commit()
                finally:
                    connection.close()
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(store.append(events[4]))
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(store.append(events[3]))
                connection = sqlite3.connect(path)
                try:
                    head = connection.execute(
                        "SELECT last_event_sequence FROM sessions WHERE session_id = ?",
                        (events[0].session_id,),
                    ).fetchone()[0]
                    inserted = connection.execute(
                        "SELECT COUNT(*) FROM runtime_events WHERE id = ?",
                        (events[4].id,),
                    ).fetchone()[0]
                finally:
                    connection.close()
                self.assertEqual(head, events[3].sequence)
                self.assertEqual(inserted, 0)
            finally:
                store.close()

    def test_external_valid_append_forces_replay_then_continues(self) -> None:
        """Accept another Store writer without trusting this process's stale cache."""

        events = _capture_events()
        self.assertGreaterEqual(len(events), 4)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            first = SQLiteRuntimeStore(path)
            second = SQLiteRuntimeStore(path)
            try:
                asyncio.run(first.append(events[0]))
                asyncio.run(first.append(events[1]))
                asyncio.run(second.append(events[2]))
                asyncio.run(first.append(events[3]))
                state = asyncio.run(first.rebuild_state(events[0].session_id))
                self.assertEqual(state.sequence, events[3].sequence)
            finally:
                second.close()
                first.close()

    def test_runtime_event_sql_envelope_is_bound_to_canonical_json(self) -> None:
        """Reject indexed Event identity that disagrees with its sealed record."""

        events = _capture_events()
        final = events[-1]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE runtime_events SET event_name = ? WHERE id = ?",
                    ("forged.event", final.id),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "SQL envelope",
                ):
                    asyncio.run(reader.rebuild_state(final.session_id))
            finally:
                reader.close()

    def test_workflow_definition_sql_envelope_is_bound_to_snapshot(self) -> None:
        """Reject Workflow list and detail identities forged outside the snapshot."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            app = AutoAgentApp()
            try:
                compiled = app.register_workflow(
                    Workflow("workflow-envelope", nodes=[Node("work", identity)])
                )
                store.save_workflow(
                    app.workflow_definition_snapshot(compiled.workflow_revision_id)
                )
            finally:
                app.close()
                store.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE workflow_definitions SET revision_id = ?, workflow_id = ?",
                    ("forged-revision", "forged-workflow"),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Workflow Definition SQL envelope",
                ):
                    asyncio.run(reader.list_workflows())
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Workflow Definition SQL envelope",
                ):
                    asyncio.run(reader.get_workflow("forged-revision"))
            finally:
                reader.close()

    def test_historical_prefix_is_exact_and_independent_of_later_damage(self) -> None:
        """Replay exactly N Events without requiring a healthy Event after N."""

        events = _capture_events()
        self.assertGreaterEqual(len(events), 3)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE runtime_events SET event_name = ? WHERE id = ?",
                    ("damaged.future", events[-1].id),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                prefix = asyncio.run(
                    reader.rebuild_state(
                        events[0].session_id,
                        through_sequence=events[-2].sequence,
                    )
                )
                self.assertEqual(prefix.sequence, events[-2].sequence)
            finally:
                reader.close()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "missing-prefix.db"
            _persist_events(path, events)
            missing = events[1]
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "DELETE FROM runtime_events WHERE id = ?",
                    (missing.id,),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(
                        reader.rebuild_state(
                            missing.session_id,
                            through_sequence=missing.sequence,
                        )
                    )
            finally:
                reader.close()

    def test_invocation_projection_cannot_truncate_canonical_state(self) -> None:
        """Reject a projected Invocation head older than its verified Events."""

        events = _capture_events()
        invocation_id = next(
            event.invocation_id for event in events if event.invocation_id is not None
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE invocations SET last_event_sequence = first_event_sequence "
                    "WHERE invocation_id = ?",
                    (invocation_id,),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Invocation range",
                ):
                    asyncio.run(reader.rebuild_invocation_state(invocation_id))
            finally:
                reader.close()

    def test_spawn_parent_cache_survives_late_child_terminal_events(self) -> None:
        """Avoid replaying the whole parent history for every detached Child update."""

        async def delayed(value: Value) -> Value:
            await asyncio.sleep(0.02)
            return value

        collector = _Collector()
        app = AutoAgentApp(
            runtime_event_sink=collector,
            runtime_journal=InMemoryEventJournal(max_batches_per_event=1),
        )
        try:
            child = Workflow("cache-child", nodes=[Node("work", delayed)])
            parent = Workflow(
                "cache-parent",
                nodes=[
                    Node(
                        "spawn",
                        child,
                        input_mapping=map_items,
                        map=Map(max_parallelism=4),
                        execution_mode="spawn",
                    )
                ],
            )
            result = app.invoke(
                parent,
                {"items": [{"value": index} for index in range(4)]},
                session_id="cache-parent-session",
            )
            for handle in app.child_handles(result.ref):
                app.wait_child(handle, timeout=2)
        finally:
            app.close()
        parent_events = tuple(
            event
            for event in collector.events
            if event.session_id == "cache-parent-session"
        )
        terminal_updates = [
            event
            for event in parent_events
            if any(
                isinstance(log.payload, ChildInvocationPhaseChanged)
                and log.payload.phase == "terminal"
                for log in event.logs
            )
        ]
        self.assertGreaterEqual(len(terminal_updates), 2)

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            original = store._rebuild_state_with_connection
            rebuilds = 0

            def counted_rebuild(*args, **kwargs):
                nonlocal rebuilds
                rebuilds += 1
                return original(*args, **kwargs)

            store._rebuild_state_with_connection = counted_rebuild  # type: ignore[method-assign]
            try:
                for event in parent_events:
                    asyncio.run(store.append(event))
                self.assertEqual(rebuilds, 0)
                self.assertNotIn("cache-parent-session", store._validated_states)
                self.assertNotIn(
                    "cache-parent-session",
                    store._canonical_ownership,
                )
            finally:
                store.close()

    def test_trace_sequence_sql_envelope_rejects_numeric_corruption(self) -> None:
        """Reject a REAL or forged Trace cursor instead of truncating or skipping it."""

        events = _capture_events()
        invocation_id = next(
            event.invocation_id for event in events if event.invocation_id is not None
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            _persist_events(path, events)
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    """
                    UPDATE trace_events SET trace_sequence = 999.5
                    WHERE row_id = (
                        SELECT MAX(row_id) FROM trace_events
                        WHERE invocation_id = ?
                    )
                    """,
                    (invocation_id,),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(reader.latest_trace_sequence(invocation_id))
            finally:
                reader.close()

    def test_terminal_status_rejects_an_unknown_child_phase(self) -> None:
        """Turn corrupt Child activity into a Store error instead of an endless SSE."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            app = AutoAgentApp(runtime_event_sink=store)
            try:
                child = Workflow("phase-child", nodes=[Node("work", identity)])
                parent = Workflow(
                    "phase-parent",
                    nodes=[Node("spawn", child, execution_mode="spawn")],
                )
                result = app.invoke(
                    parent,
                    {"value": 1},
                    session_id="phase-parent-session",
                )
                for handle in app.child_handles(result.ref):
                    app.wait_child(handle, timeout=2)
            finally:
                app.close()
                store.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE session_ownership SET phase = 'bogus' "
                    "WHERE parent_invocation_id = ?",
                    (result.invocation_id,),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "phase",
                ):
                    asyncio.run(
                        reader.terminal_trace_status(
                            result.invocation_id,
                            through_sequence=10**6,
                        )
                    )
            finally:
                reader.close()

    @unittest.skipIf(os.name == "nt", "POSIX pipe notifier only")
    def test_change_notification_holds_listener_ownership_during_write(self) -> None:
        """Prevent waiter teardown from closing and reusing an announced pipe fd."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            reader, writer = os.pipe()
            entered = threading.Event()
            release = threading.Event()
            removed = threading.Event()

            def blocked_write(_fd: int, _value: bytes) -> int:
                entered.set()
                release.wait(1)
                return 1

            def remove_listener() -> None:
                with store._listener_lock:
                    store._listener_writers.discard(writer)
                removed.set()

            with store._listener_lock:
                store._listener_writers.add(writer)
            try:
                with patch("autoagent.hosting.sqlite.os.write", blocked_write):
                    announcer = threading.Thread(target=store._announce_change)
                    announcer.start()
                    self.assertTrue(entered.wait(1))
                    remover = threading.Thread(target=remove_listener)
                    remover.start()
                    self.assertFalse(removed.wait(0.05))
                    release.set()
                    announcer.join(1)
                    remover.join(1)
                    self.assertFalse(announcer.is_alive())
                    self.assertFalse(remover.is_alive())
                    self.assertTrue(removed.is_set())
            finally:
                release.set()
                with store._listener_lock:
                    store._listener_writers.discard(writer)
                os.close(reader)
                os.close(writer)
                store.close()

    def test_terminal_sessions_do_not_accumulate_in_validation_cache(self) -> None:
        """Bound semantic validation memory independently of Session history."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            app = AutoAgentApp(runtime_event_sink=store)
            try:
                workflow = Workflow("cache-bound", nodes=[Node("work", identity)])
                for index in range(20):
                    result = app.invoke(
                        workflow,
                        {"value": index},
                        session_id=f"cache-session-{index}",
                    )
                    self.assertEqual(result.status, "completed")
                self.assertEqual(store._validated_states, {})
            finally:
                app.close()
                store.close()

    def test_wait_resume_projection_matches_canonical_running_state(self) -> None:
        """Project status from validated State for transitions such as WaitResumed."""

        slow_started = threading.Event()
        release_slow = threading.Event()

        def slow(value: Value) -> Value:
            slow_started.set()
            release_slow.wait(2)
            return value

        workflow = Workflow(
            "stored-resume",
            nodes=[
                Node("start", identity),
                Node("slow", slow),
                Node("approval", Wait(Value, Value)),
            ],
            edges=[Edge("start", "slow"), Edge("start", "approval")],
        )
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            app = AutoAgentApp(runtime_event_sink=store)
            try:
                submitted = app.submit_invoke(
                    workflow,
                    {"value": 1},
                    session_id="stored-resume-session",
                )
                self.assertTrue(slow_started.wait(1))
                wait_id = None
                for _ in range(100):
                    state = asyncio.run(store.rebuild_state(submitted.session_id))
                    waits = tuple(state.invocation.scheduler.waits.values())
                    if waits:
                        wait_id = waits[0].id
                        break
                    time.sleep(0.005)
                self.assertIsNotNone(wait_id)
                resumed = threading.Thread(
                    target=lambda: app.resume(
                        submitted.ref,
                        wait_id,  # type: ignore[arg-type]
                        {"value": 2},
                    )
                )
                resumed.start()
                canonical = None
                for _ in range(100):
                    canonical = asyncio.run(
                        store.rebuild_state(submitted.session_id)
                    )
                    if canonical.invocation.status == "running":
                        break
                    time.sleep(0.005)
                assert canonical is not None
                projected = asyncio.run(
                    store.get_invocation(submitted.invocation_id)
                )
                self.assertEqual(canonical.invocation.status, "running")
                self.assertEqual(projected["status"], "running")
                release_slow.set()
                resumed.join(2)
                self.assertFalse(resumed.is_alive())
            finally:
                release_slow.set()
                app.close()
                store.close()

    def test_close_cannot_overtake_admitted_reads_or_writes(self) -> None:
        """Atomically admit Store work before a concurrent close drains workers."""

        event = _capture_events()[0]
        for operation in ("write", "read"):
            with self.subTest(operation=operation), TemporaryDirectory() as directory:
                store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
                store.start()
                if operation == "read":
                    asyncio.run(store.append(event))
                    worker = store._reader_worker
                    call = lambda: asyncio.run(store.list_sessions())
                else:
                    assert store._writer is not None
                    worker = store._writer
                    call = lambda: asyncio.run(store.append(event))
                original = worker.call_async
                entered = threading.Event()
                release = threading.Event()
                errors: list[BaseException] = []

                def paused(function, *arguments):
                    entered.set()
                    release.wait(1)
                    return original(function, *arguments)

                worker.call_async = paused  # type: ignore[method-assign]

                def execute() -> None:
                    try:
                        call()
                    except BaseException as error:
                        errors.append(error)

                completed_close = threading.Event()
                operation_thread = threading.Thread(target=execute)
                operation_thread.start()
                self.assertTrue(entered.wait(1))
                close_thread = threading.Thread(
                    target=lambda: (store.close(), completed_close.set())
                )
                close_thread.start()
                self.assertFalse(completed_close.wait(0.05))
                release.set()
                operation_thread.join(1)
                close_thread.join(1)
                self.assertFalse(operation_thread.is_alive())
                self.assertFalse(close_thread.is_alive())
                self.assertEqual(errors, [])

    def test_wait_for_trace_observes_another_store_writer(self) -> None:
        """Discover another process-equivalent Store write by bounded refresh."""

        event = _capture_events()[0]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            writer = SQLiteRuntimeStore(path, refresh_seconds=0.02)
            observer = SQLiteRuntimeStore(path, refresh_seconds=0.02)
            writer.start()
            observer.start()

            async def observe() -> bool:
                waiting = asyncio.create_task(
                    observer.wait_for_trace(
                        event.invocation_id,
                        after_sequence=0,
                        timeout=1,
                    )
                )
                await asyncio.sleep(0.05)
                await writer.append(event)
                return await waiting

            try:
                self.assertTrue(asyncio.run(observe()))
            finally:
                observer.close()
                writer.close()

    def test_store_rejects_event_gaps_and_broken_hash_links(self) -> None:
        """Reject sequence, hash, and state-version discontinuities."""

        events = _capture_events()
        self.assertGreaterEqual(len(events), 2)
        with TemporaryDirectory() as directory:
            gap_store = SQLiteRuntimeStore(Path(directory) / "gap.db")
            try:
                with self.assertRaises(RuntimeEventSequenceError):
                    asyncio.run(gap_store.append(events[1]))
            finally:
                gap_store.close()

            chain_store = SQLiteRuntimeStore(Path(directory) / "chain.db")
            try:
                asyncio.run(chain_store.append(events[0]))
                forged = replace(events[1], previous_event_digest="forged")
                with self.assertRaises(RuntimeEventSequenceError):
                    asyncio.run(chain_store.append(forged))
            finally:
                chain_store.close()

            version_store = SQLiteRuntimeStore(Path(directory) / "version.db")
            try:
                asyncio.run(version_store.append(events[0]))
                assert events[0].to_state_version is not None
                jumped_version = events[0].to_state_version + 100
                jumped = replace(
                    events[1],
                    from_state_version=jumped_version,
                    to_state_version=jumped_version,
                    operation_batches=(),
                    logs=tuple(
                        replace(log, state_version=jumped_version)
                        for log in events[1].logs
                    ),
                )
                with self.assertRaises(RuntimeEventSequenceError):
                    asyncio.run(version_store.append(jumped))
            finally:
                version_store.close()

    def test_store_rebuilds_parent_child_checkpoint(self) -> None:
        """Reconstruct a complete Root and Child Runtime graph from Events."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            app = AutoAgentApp(runtime_event_sink=store)
            try:
                child = Workflow("stored-child", nodes=[Node("work", identity)])
                parent = Workflow("stored-parent", nodes=[Node("child", child)])
                result = app.invoke(
                    parent,
                    {"value": 7},
                    session_id="stored-parent-session",
                )
                self.assertEqual(result.status, "completed")

                rebuilt = asyncio.run(
                    store.rebuild_checkpoint("stored-parent-session")
                )
                self.assertEqual(rebuilt.root_session_id, result.session_id)
                self.assertEqual(len(rebuilt.states), 2)
                self.assertEqual(
                    rebuilt.state(result.session_id).invocation.output,
                    {"value": 7},
                )
            finally:
                app.close()
                store.close()

    def test_checkpoint_rejects_a_child_head_whose_event_tail_was_deleted(
        self,
    ) -> None:
        """Do not mistake an opened but corrupt Child for an unopened plan."""

        collector = _Collector()
        app = AutoAgentApp(
            runtime_event_sink=collector,
            runtime_journal=InMemoryEventJournal(max_batches_per_event=1),
        )
        try:
            child = Workflow("orphan-child", nodes=[Node("work", identity)])
            parent = Workflow("orphan-parent", nodes=[Node("child", child)])
            app.invoke(parent, {"value": 1}, session_id="orphan-root")
        finally:
            app.close()
        planned_event = next(
            event
            for event in collector.events
            if event.session_id == "orphan-root"
            and any(
                isinstance(log.payload, ChildInvocationPlanned)
                for log in event.logs
            )
        )
        plan = next(
            log.payload
            for log in planned_event.logs
            if isinstance(log.payload, ChildInvocationPlanned)
        )
        child_session_id = plan.units[0].child_session_id
        child_first = next(
            event
            for event in collector.events
            if event.session_id == child_session_id and event.sequence == 1
        )
        parent_prefix = tuple(
            event
            for event in collector.events
            if event.session_id == "orphan-root"
            and event.sequence <= planned_event.sequence
        )

        with TemporaryDirectory() as directory:
            unopened = SQLiteRuntimeStore(Path(directory) / "unopened.db")
            try:
                for event in parent_prefix:
                    asyncio.run(unopened.append(event))
                checkpoint = asyncio.run(unopened.rebuild_checkpoint("orphan-root"))
                self.assertEqual(set(checkpoint.states), {"orphan-root"})
            finally:
                unopened.close()

            path = Path(directory) / "orphaned.db"
            opened = SQLiteRuntimeStore(path)
            try:
                for event in (*parent_prefix, child_first):
                    asyncio.run(opened.append(event))
            finally:
                opened.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "DELETE FROM runtime_events WHERE session_id = ?",
                    (child_session_id,),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "Session head",
                ):
                    asyncio.run(reader.rebuild_checkpoint("orphan-root"))
            finally:
                reader.close()

    def test_checkpoint_rebuild_uses_one_parent_child_database_snapshot(self) -> None:
        """Never combine parent and Child States from different WAL moments."""

        collector = _Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            child = Workflow("snapshot-child", nodes=[Node("work", identity)])
            parent = Workflow("snapshot-parent", nodes=[Node("child", child)])
            app.invoke(parent, {"value": 1}, session_id="snapshot-root")
        finally:
            app.close()
        plan = next(
            log.payload
            for event in collector.events
            for log in event.logs
            if isinstance(log.payload, ChildInvocationPlanned)
        )
        child_session_id = plan.units[0].child_session_id
        child_events = [
            event
            for event in collector.events
            if event.session_id == child_session_id
        ]
        self.assertGreaterEqual(len(child_events), 2)
        initial = [
            event
            for event in collector.events
            if event.session_id == "snapshot-root" and event.sequence <= 5
        ] + [child_events[0]]

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            for event in initial:
                asyncio.run(store.append(event))
            original = store._rebuild_state_with_connection
            root_read = threading.Event()
            release = threading.Event()

            def pause_after_root(connection, session_id, through_sequence):
                state = original(connection, session_id, through_sequence)
                if session_id == "snapshot-root":
                    root_read.set()
                    release.wait(1)
                return state

            store._rebuild_state_with_connection = pause_after_root  # type: ignore[method-assign]
            result: list[object] = []
            errors: list[BaseException] = []

            def rebuild() -> None:
                try:
                    result.append(
                        asyncio.run(store.rebuild_checkpoint("snapshot-root"))
                    )
                except BaseException as error:
                    errors.append(error)

            thread = threading.Thread(target=rebuild)
            thread.start()
            self.assertTrue(root_read.wait(1))
            asyncio.run(store.append(child_events[1]))
            release.set()
            thread.join(1)
            try:
                self.assertEqual(errors, [])
                self.assertEqual(len(result), 1)
                checkpoint = result[0]
                self.assertEqual(
                    checkpoint.state(child_session_id).sequence,  # type: ignore[union-attr]
                    child_events[0].sequence,
                )
            finally:
                release.set()
                store.close()

    def test_child_session_index_preserves_plan_metadata_and_cursor_scope(
        self,
    ) -> None:
        """Page Child tasks in plan order and bind cursors to their parent."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            app = AutoAgentApp(runtime_event_sink=store)
            try:
                child = Workflow("indexed-child", nodes=[Node("work", identity)])
                parent = Workflow(
                    "indexed-parent",
                    nodes=[
                        Node(
                            "children",
                            child,
                            input_mapping=map_items,
                            map=Map(max_parallelism=2),
                            execution_mode="spawn",
                        )
                    ],
                )
                first = app.invoke(
                    parent,
                    {"items": [{"value": 1}, {"value": 2}, {"value": 3}]},
                    session_id="first-parent",
                )
                second = app.invoke(
                    parent,
                    {"items": [{"value": 4}]},
                    session_id="second-parent",
                )
                first_handles = app.child_handles(first.ref)
                second_handles = app.child_handles(second.ref)
                for handle in (*first_handles, *second_handles):
                    app.wait_child(handle, timeout=1)

                with patch.object(
                    store,
                    "_read_parent_child_ownership",
                    wraps=store._read_parent_child_ownership,
                ) as ownership_scan:
                    page_one = asyncio.run(
                        store.list_child_sessions(first.invocation_id, limit=2)
                    )
                self.assertEqual(ownership_scan.call_count, 1)
                self.assertEqual(
                    [item["unit_index"] for item in page_one.items],
                    [0, 1],
                )
                self.assertIsNotNone(page_one.next_cursor)
                assert page_one.next_cursor is not None
                self.assertLessEqual(len(page_one.next_cursor), 512)
                page_two = asyncio.run(
                    store.list_child_sessions(
                        first.invocation_id,
                        limit=2,
                        cursor=page_one.next_cursor,
                    )
                )
                self.assertEqual(
                    [item["unit_index"] for item in page_two.items],
                    [2],
                )
                first_child = page_one.items[0]
                self.assertEqual(first_child["mode"], "spawn")
                self.assertEqual(first_child["planned_workflow_id"], "indexed-child")
                self.assertEqual(first_child["phase"], "terminal")
                self.assertEqual(
                    first_child["planned_invocation_id"],
                    first_child["current_invocation_id"],
                )
                self.assertTrue(first_child["parent_occurrence_id"])
                self.assertGreater(first_child["planned_event_sequence"], 0)

                with patch.object(
                    store,
                    "_read_parent_child_ownership",
                    wraps=store._read_parent_child_ownership,
                ) as checkpoint_scan:
                    rebuilt = asyncio.run(
                        store.rebuild_checkpoint(first.session_id)
                    )
                self.assertEqual(len(rebuilt.states), 4)
                self.assertEqual(checkpoint_scan.call_count, 1)

                with self.assertRaises(ValueError):
                    asyncio.run(
                        store.list_child_sessions(
                            second.invocation_id,
                            cursor=page_one.next_cursor,
                        )
                    )
                with self.assertRaises(ValueError):
                    asyncio.run(
                        store.list_child_sessions(
                            first.invocation_id,
                            cursor="W10",
                        )
                    )
            finally:
                app.close()
                store.close()

    def test_child_session_index_rejects_missing_opened_runtime_rows(self) -> None:
        """Do not downgrade an opened or terminal Child to a planned summary."""

        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            app = AutoAgentApp(runtime_event_sink=store)
            try:
                child = Workflow("missing-child", nodes=[Node("work", identity)])
                parent = Workflow(
                    "missing-parent",
                    nodes=[Node("child", child, execution_mode="spawn")],
                )
                result = app.invoke(
                    parent,
                    {"value": 1},
                    session_id="missing-parent-session",
                )
                handle = app.child_handles(result.ref)[0]
                app.wait_child(handle, timeout=1)
                child_session_id = handle["session_id"]
            finally:
                app.close()
                store.close()
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "DELETE FROM sessions WHERE session_id = ?",
                    (child_session_id,),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaisesRegex(
                    RuntimeEventStoreError,
                    "no Session projection",
                ):
                    asyncio.run(
                        reader.list_child_sessions(result.invocation_id)
                    )
            finally:
                reader.close()

    def test_child_open_projection_must_match_the_durable_plan(self) -> None:
        """Roll back a Child open whose Workflow contradicts its parent plan."""

        collector = _Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            child = Workflow("planned-child", nodes=[Node("work", identity)])
            parent = Workflow("planned-parent", nodes=[Node("child", child)])
            result = app.invoke(
                parent,
                {"value": 1},
                session_id="planned-parent-session",
            )
            self.assertEqual(result.status, "completed")
        finally:
            app.close()

        plan = next(
            log.payload
            for event in collector.events
            for log in event.logs
            if isinstance(log.payload, ChildInvocationPlanned)
        )
        opened_event = next(
            event
            for event in collector.events
            if event.session_id == plan.units[0].child_session_id
            and any(isinstance(log.payload, InvocationOpened) for log in event.logs)
        )
        forged_record = opened_event.to_record()
        logs = forged_record["logs"]
        assert isinstance(logs, list)
        for log in logs:
            if log["event_name"] == "invocation.opened":
                log["payload"]["workflow_id"] = "different-child"
        batches = forged_record["operation_batches"]
        assert isinstance(batches, list)
        for batch in batches:
            for operation in batch["operations"]:
                if operation["path"] == ["invocation"]:
                    operation["value"]["workflow_id"] = "different-child"
        forged_event = RuntimeEvent.from_record(forged_record)

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                for event in collector.events:
                    if event is opened_event:
                        with self.assertRaises(RuntimeEventConflictError):
                            asyncio.run(store.append(forged_event))
                        asyncio.run(store.append(opened_event))
                        break
                    asyncio.run(store.append(event))
                child_record = asyncio.run(
                    store.get_invocation(plan.units[0].child_invocation_id)
                )
                self.assertEqual(child_record["workflow_id"], "planned-child")
            finally:
                store.close()

    def test_cursor_pages_are_stable_and_opaque(self) -> None:
        """Traverse Workflow rows through an opaque keyset cursor."""

        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            app = AutoAgentApp()
            try:
                for index in range(3):
                    workflow = Workflow(
                        f"workflow-{index}", nodes=[Node("work", identity)]
                    )
                    compiled = app.register_workflow(workflow)
                    store.save_workflow(
                        app.workflow_definition_snapshot(
                            compiled.workflow_revision_id
                        )
                    )
                first = asyncio.run(store.list_workflows(limit=2))
                second = asyncio.run(
                    store.list_workflows(limit=2, cursor=first.next_cursor)
                )
                self.assertTrue(first.has_more)
                self.assertEqual(len(first.items), 2)
                self.assertEqual(len(second.items), 1)
                self.assertFalse(second.has_more)
                with self.assertRaises(ValueError):
                    asyncio.run(
                        store.list_sessions(cursor=first.next_cursor)
                    )
                scope = hashlib.sha256(
                    json.dumps(
                        {"identities": []},
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                ).hexdigest()
                oversized = base64.urlsafe_b64encode(
                    json.dumps(
                        {
                            "collection": "workflows",
                            "row_id": 10**100,
                            "scope": scope,
                            "version": 1,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                ).decode().rstrip("=")
                with self.assertRaises(ValueError):
                    asyncio.run(store.list_workflows(cursor=oversized))
                with self.assertRaises(ValueError):
                    asyncio.run(store.list_workflows(cursor="W10"))
            finally:
                app.close()
                store.close()


class HttpRuntimeEventSinkTests(unittest.TestCase):
    def test_constructor_rejects_nonfinite_or_ambiguous_limits(self) -> None:
        """Validate HTTP timeout and worker limits before allocating threads."""

        for timeout in (True, 0, float("nan"), float("inf"), 10**1000):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                HttpRuntimeEventSink(
                    "https://events.example.test/v1/runtime-events",
                    timeout_seconds=timeout,
                )
        for concurrency in (True, 0, float("nan"), float("inf")):
            with self.subTest(concurrency=concurrency), self.assertRaises(ValueError):
                HttpRuntimeEventSink(
                    "https://events.example.test/v1/runtime-events",
                    max_concurrency=concurrency,  # type: ignore[arg-type]
                )

    def test_concurrent_closers_share_completion_and_failure(self) -> None:
        """Make every concurrent close wait for and observe one shutdown result."""

        client = _Client()
        entered = threading.Event()
        release = threading.Event()
        failure = RuntimeError("shared close failed")

        def fail_close() -> None:
            entered.set()
            release.wait(1)
            raise failure

        client.close = fail_close  # type: ignore[method-assign]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        sink._owns_client = True
        errors: list[BaseException] = []

        def close() -> None:
            try:
                sink.close()
            except BaseException as error:
                errors.append(error)

        first = threading.Thread(target=close)
        second = threading.Thread(target=close)
        first.start()
        self.assertTrue(entered.wait(1))
        second.start()
        self.assertTrue(second.is_alive())
        release.set()
        first.join(1)
        second.join(1)
        self.assertEqual(errors, [failure, failure])

    def test_close_failure_still_terminates_worker(self) -> None:
        """Shut down every HTTP worker even when client cleanup raises."""

        client = _Client()

        def fail_close() -> None:
            raise RuntimeError("close failed")

        client.close = fail_close  # type: ignore[method-assign]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        sink._owns_client = True
        with self.assertRaisesRegex(RuntimeError, "close failed"):
            sink.close()
        with self.assertRaises(Exception):
            asyncio.run(sink.append(_capture_events()[0]))
        self.assertTrue(
            all(not thread.is_alive() for thread in sink._worker._executor._threads)
        )

    def test_concurrent_close_does_not_mask_an_admitted_request_failure(self) -> None:
        """Keep the remote response error after close starts draining the pool."""

        entered = threading.Event()
        release = threading.Event()
        client = _Client()

        def rejected_post(
            url: str,
            *,
            json: object,
            headers: dict[str, str],
        ) -> _Response:
            entered.set()
            release.wait(1)
            return _Response(503, "unavailable")

        client.post = rejected_post  # type: ignore[method-assign]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        closer: threading.Thread | None = None

        async def run() -> None:
            nonlocal closer
            sending = asyncio.create_task(sink.append(_capture_events()[0]))
            while not entered.is_set():
                await asyncio.sleep(0)
            closer = threading.Thread(target=sink.close)
            closer.start()
            while not sink._closed:
                await asyncio.sleep(0)
            release.set()
            with self.assertRaisesRegex(
                RuntimeEventStoreError,
                "returned 503: unavailable",
            ):
                await sending

        try:
            asyncio.run(run())
        finally:
            release.set()
            if closer is not None:
                closer.join(1)
            sink.close()

    def test_async_close_keeps_the_caller_event_loop_responsive(self) -> None:
        """Close a blocked HTTP pool without blocking unrelated async work."""

        client = _Client()
        release = threading.Event()

        def delayed_close() -> None:
            release.wait(1)
            client.closed = True

        client.close = delayed_close  # type: ignore[method-assign]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        sink._owns_client = True

        async def run() -> bool:
            progressed = False

            async def peer() -> None:
                nonlocal progressed
                await asyncio.sleep(0)
                progressed = True

            peer_task = asyncio.create_task(peer())
            timer = threading.Timer(0.05, release.set)
            timer.start()
            try:
                await sink.aclose()
                return progressed
            finally:
                await peer_task
                timer.cancel()

        self.assertTrue(asyncio.run(run()))
        self.assertTrue(client.closed)

    def test_cancelled_queued_append_never_sends_a_request(self) -> None:
        """Cancel queued HTTP work before a worker can start its request."""

        events = _capture_events()
        first = events[0]
        second = replace(first, id="cancelled-http-event")
        entered = threading.Event()
        release = threading.Event()
        sent: list[str] = []
        client = _Client()

        def blocked_post(
            url: str,
            *,
            json: object,
            headers: dict[str, str],
        ) -> _Response:
            sent.append(headers["Idempotency-Key"])
            entered.set()
            release.wait(1)
            return _Response(202)

        client.post = blocked_post  # type: ignore[method-assign]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
            max_concurrency=1,
        )

        async def run() -> None:
            sending = asyncio.create_task(sink.append(first))
            while not entered.is_set():
                await asyncio.sleep(0)
            cancelled = asyncio.create_task(sink.append(second))
            await asyncio.sleep(0)
            cancelled.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await cancelled
            release.set()
            await sending

        try:
            asyncio.run(run())
            self.assertEqual(sent, [first.id])
        finally:
            release.set()
            sink.close()

    def test_cancelled_async_close_does_not_cancel_shared_shutdown(self) -> None:
        """Let a later closer join shutdown after the first waiter is cancelled."""

        client = _Client()
        entered = threading.Event()
        release = threading.Event()

        def blocked_close() -> None:
            entered.set()
            release.wait(1)
            client.closed = True

        client.close = blocked_close  # type: ignore[method-assign]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        sink._owns_client = True

        async def run() -> None:
            closing = asyncio.create_task(sink.aclose())
            while not entered.is_set():
                await asyncio.sleep(0)
            closing.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await closing
            release.set()
            await sink.aclose()

        try:
            asyncio.run(run())
            self.assertTrue(client.closed)
        finally:
            release.set()
            sink.close()

    def test_http_sink_sends_canonical_event_with_idempotency_headers(self) -> None:
        """Send the exact Event record and stable remote idempotency key."""

        event = _capture_events()[0]
        client = _Client()
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            token="secret",
            client=client,
        )
        try:
            asyncio.run(sink.append(event))
            url, payload, headers = client.calls[0]
            self.assertEqual(url, "https://events.example.test/v1/runtime-events")
            self.assertEqual(payload, event.to_record())
            self.assertEqual(headers["Idempotency-Key"], event.id)
            self.assertEqual(headers["Authorization"], "Bearer secret")
            self.assertEqual(headers["X-AutoAgent-Record-Type"], "runtime_event")
            self.assertNotIn("X-AutoAgent-Session-Id", headers)
        finally:
            sink.close()

    def test_http_sink_sends_portable_workflow_definition(self) -> None:
        """Publish the graph snapshot needed by a remote Tracing backend."""

        app = AutoAgentApp()
        client = _Client()
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        try:
            compiled = app.register_workflow(
                Workflow("remote-definition", nodes=[Node("work", identity)])
            )
            snapshot = app.workflow_definition_snapshot(
                compiled.workflow_revision_id
            )
            sink.save_workflow(snapshot)
            _, payload, headers = client.calls[0]
            self.assertEqual(payload, snapshot.to_record())
            self.assertEqual(
                headers["Idempotency-Key"], snapshot.workflow_revision_id
            )
            self.assertEqual(
                headers["X-AutoAgent-Record-Type"], "workflow_definition"
            )
        finally:
            sink.close()
            app.close()

    def test_http_sink_accepts_unicode_session_identity_in_json(self) -> None:
        """Keep unrestricted Session ids out of ASCII-only HTTP headers."""

        event = replace(_capture_events()[0], session_id="会话")
        client = _Client()
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        try:
            asyncio.run(sink.append(event))
            self.assertEqual(client.calls[0][1]["session_id"], "会话")
            self.assertNotIn("X-AutoAgent-Session-Id", client.calls[0][2])
        finally:
            sink.close()

    def test_http_sink_executes_independent_sessions_concurrently(self) -> None:
        """Avoid globally serializing remote Events from different Sessions."""

        first = _capture_events()[0]
        second = replace(first, id="second-event", session_id="second-session")
        lock = threading.Lock()
        release = threading.Event()
        active = 0
        maximum = 0
        client = _Client()

        def concurrent_post(
            url: str,
            *,
            json: object,
            headers: dict[str, str],
        ) -> _Response:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                if active == 2:
                    release.set()
            release.wait(1)
            with lock:
                active -= 1
            return _Response(202)

        client.post = concurrent_post  # type: ignore[method-assign]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        try:
            async def run() -> None:
                await asyncio.gather(sink.append(first), sink.append(second))

            asyncio.run(run())
            self.assertEqual(maximum, 2)
        finally:
            sink.close()

    def test_http_sink_propagates_non_success_response(self) -> None:
        """Reject a remote response that did not durably accept the Event."""

        event = _capture_events()[0]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=_Client(status_code=503),
        )
        try:
            with self.assertRaises(RuntimeEventStoreError):
                asyncio.run(sink.append(event))
        finally:
            sink.close()

    def test_http_sink_rejects_malformed_response_status(self) -> None:
        """Wrap a custom client's non-integer or invalid HTTP status."""

        event = _capture_events()[0]
        for status_code in ("202", 202.0, True, 99, 600):
            with self.subTest(status_code=status_code):
                client = _Client()
                client.status_code = status_code  # type: ignore[assignment]
                sink = HttpRuntimeEventSink(
                    "https://events.example.test/v1/runtime-events",
                    client=client,
                )
                try:
                    with self.assertRaises(RuntimeEventStoreError):
                        asyncio.run(sink.append(event))
                finally:
                    sink.close()

    def test_http_sink_wraps_transport_failure(self) -> None:
        """Expose a stable Store error when the remote transport times out."""

        event = _capture_events()[0]
        client = _Client()

        def fail_request(*_args: object, **_kwargs: object) -> _Response:
            raise TimeoutError("remote timeout")

        client.post = fail_request  # type: ignore[method-assign]
        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events",
            client=client,
        )
        try:
            with self.assertRaises(RuntimeEventStoreError) as raised:
                asyncio.run(sink.append(event))
            self.assertIsInstance(raised.exception.__cause__, TimeoutError)
        finally:
            sink.close()

    def test_http_sink_wraps_lazy_client_construction_failure(self) -> None:
        """Keep optional HTTP client initialization inside the Store boundary."""

        sink = HttpRuntimeEventSink(
            "https://events.example.test/v1/runtime-events"
        )
        try:
            with patch("httpx.Client", side_effect=OSError("socket unavailable")):
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(sink.append(_capture_events()[0]))
        finally:
            sink.close()


if __name__ == "__main__":
    unittest.main()
