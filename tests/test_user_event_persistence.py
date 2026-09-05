from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
import warnings
from collections.abc import Iterator

from typing_extensions import TypedDict

from autoagent import AutoAgentApp, Node, Stream, StreamContext, Workflow
from autoagent.core.runtime import RuntimeEvent, UserEvent
from autoagent.hosting import (
    RuntimeEventConflictError,
    RuntimeEventSequenceError,
    RuntimeEventStoreError,
    SQLiteRuntimeStore,
)


class Value(TypedDict):
    value: int


class Chunk(TypedDict):
    value: int


class Total(TypedDict):
    total: int


def identity(value: Value) -> Value:
    return value


class _RuntimeCollector:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def append(self, event: RuntimeEvent) -> None:
        self.events.append(event)


def _captured_invocation() -> tuple[tuple[RuntimeEvent, ...], str, str]:
    collector = _RuntimeCollector()
    app = AutoAgentApp(runtime_event_sink=collector)
    try:
        result = app.invoke(
            Workflow("user-events", nodes=[Node("work", identity)]),
            {"value": 1},
            session_id="user-event-session",
        )
    finally:
        app.close()
    return tuple(collector.events), result.session_id, result.invocation_id


def _persist_runtime(store: SQLiteRuntimeStore, events: tuple[RuntimeEvent, ...]) -> None:
    for event in events:
        asyncio.run(store.append(event))


class SQLiteUserEventTests(unittest.TestCase):
    def test_user_events_round_trip_independently_from_runtime_state(self) -> None:
        """Persist, page, tail, and retry an independent User Event stream."""

        runtime, session_id, invocation_id = _captured_invocation()
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                _persist_runtime(store, runtime)
                events = (
                    UserEvent(
                        id="user-1",
                        session_id=session_id,
                        invocation_id=invocation_id,
                        sequence=1,
                        kind="message.delta",
                        payload={"text": "a"},
                        occurred_at_ns=10,
                    ),
                    UserEvent(
                        id="user-2",
                        session_id=session_id,
                        invocation_id=invocation_id,
                        sequence=2,
                        kind="message.delta",
                        payload={"text": "b"},
                        occurred_at_ns=11,
                    ),
                )
                asyncio.run(store.append_user_event(events[0]))
                asyncio.run(store.append_user_event(events[0]))
                asyncio.run(store.append_user_event(events[1]))

                self.assertEqual(
                    asyncio.run(store.list_user_events(invocation_id)),
                    events,
                )
                self.assertEqual(
                    asyncio.run(
                        store.list_user_events(invocation_id, after_sequence=1)
                    ),
                    (events[1],),
                )
                self.assertEqual(
                    asyncio.run(store.tail_user_events(invocation_id, limit=1)),
                    (events[1],),
                )
                self.assertEqual(
                    asyncio.run(store.latest_user_event_sequence(invocation_id)),
                    2,
                )
                rebuilt = asyncio.run(store.rebuild_invocation_state(invocation_id))
                self.assertEqual(rebuilt.invocation.output, {"value": 1})
            finally:
                store.close()

    def test_user_event_identity_and_sequence_conflicts_are_rejected(self) -> None:
        """Reject missing prefixes, duplicate positions, and conflicting ids."""

        runtime, session_id, invocation_id = _captured_invocation()
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                _persist_runtime(store, runtime)
                first = UserEvent(
                    id="user-1",
                    session_id=session_id,
                    invocation_id=invocation_id,
                    sequence=1,
                    kind="progress",
                    payload={"value": 1},
                    occurred_at_ns=10,
                )
                with self.assertRaises(RuntimeEventSequenceError):
                    asyncio.run(
                        store.append_user_event(replace(first, id="gap", sequence=2))
                    )
                asyncio.run(store.append_user_event(first))
                with self.assertRaises(RuntimeEventConflictError):
                    asyncio.run(
                        store.append_user_event(replace(first, payload={"value": 2}))
                    )
                with self.assertRaises(RuntimeEventConflictError):
                    asyncio.run(
                        store.append_user_event(replace(first, id="another"))
                    )
            finally:
                store.close()

    def test_user_event_queries_detect_projection_and_record_tampering(self) -> None:
        """Detect missing rows, forged stream heads, and altered Event records."""

        runtime, session_id, invocation_id = _captured_invocation()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = SQLiteRuntimeStore(path)
            _persist_runtime(store, runtime)
            event = UserEvent(
                id="user-1",
                session_id=session_id,
                invocation_id=invocation_id,
                sequence=1,
                kind="progress",
                payload={"value": 1},
                occurred_at_ns=10,
            )
            asyncio.run(store.append_user_event(event))
            store.close()

            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "UPDATE user_events SET record_json = '{}' WHERE id = 'user-1'"
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteRuntimeStore.open_read_only(path)
            try:
                with self.assertRaises(RuntimeEventStoreError):
                    asyncio.run(reader.list_user_events(invocation_id))
            finally:
                reader.close()

    def test_user_event_wait_uses_store_notifications(self) -> None:
        """Wake a subscriber after a newer User Event is committed."""

        runtime, session_id, invocation_id = _captured_invocation()
        with TemporaryDirectory() as directory:
            store = SQLiteRuntimeStore(Path(directory) / "runtime.db")
            try:
                _persist_runtime(store, runtime)

                async def exercise() -> bool:
                    waiting = asyncio.create_task(
                        store.wait_for_user_event(
                            invocation_id,
                            after_sequence=0,
                            timeout=1,
                        )
                    )
                    await asyncio.sleep(0)
                    await store.append_user_event(
                        UserEvent(
                            id="user-1",
                            session_id=session_id,
                            invocation_id=invocation_id,
                            sequence=1,
                            kind="progress",
                            payload={"value": 1},
                            occurred_at_ns=10,
                        )
                    )
                    return await waiting

                self.assertTrue(asyncio.run(exercise()))
            finally:
                store.close()


class CoreUserEventSinkTests(unittest.TestCase):
    def test_user_event_sink_failure_does_not_change_workflow_result(self) -> None:
        """Keep canonical execution successful when observation delivery fails."""

        class FailingSink:
            def __init__(self) -> None:
                self.invocation_ids: list[str] = []

            async def append_user_event(self, _event: UserEvent) -> None:
                self.invocation_ids.append(_event.invocation_id)
                raise RuntimeError("sink failed")

        def stream(value: Value) -> Iterator[Chunk]:
            yield value

        class Reducer:
            def initial(self, _context: StreamContext) -> Total:
                return {"total": 0}

            def add(
                self,
                _context: StreamContext,
                state: Total,
                chunk: Chunk,
            ) -> Total:
                return {"total": state["total"] + chunk["value"]}

            def finish(self, _context: StreamContext, state: Total) -> Total:
                return state

        sink = FailingSink()
        app = AutoAgentApp(user_event_sink=sink)
        try:
            workflow = Workflow(
                "sink-failure",
                nodes=[
                    Node(
                        "work",
                        stream,
                        stream=Stream(Reducer()),
                    )
                ],
            )
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = app.invoke(workflow, {"value": 1})
                second = app.invoke(workflow, {"value": 2})
            self.assertEqual(result.status, "completed")
            self.assertEqual(second.status, "completed")
            self.assertIsInstance(app.user_event_sink_error, RuntimeError)
            self.assertEqual(len(app.user_event_sink_errors), 2)
            self.assertEqual(len(set(sink.invocation_ids)), 2)
            self.assertEqual(len(caught), 2)
            self.assertEqual(len(result.user_events), 1)
            self.assertEqual(len(second.user_events), 1)
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
