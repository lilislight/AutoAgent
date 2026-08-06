from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import Future
import os
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel
from sqlalchemy import inspect, select, text
from sqlalchemy.exc import OperationalError

from autoagent import (
    AutoAgentApp,
    DatabaseBackend,
    FailurePolicy,
    JsonRuntimeSerializer,
    MapPolicy,
    NodePolicy,
    PersistenceAdmissionError,
    PersistencePolicy,
    RecoveryPolicy,
    RuntimeRetentionPolicy,
    SystemCommand,
    Workflow,
    WorkflowPolicy,
)
from autoagent.core.runtime import (
    ArtifactPolicy,
    Invocation,
    PersistenceEnvelope,
    RuntimeEvent,
    RuntimeStore,
    SessionBusyError,
    build_state_operations,
    capture_execution_state,
)
from autoagent.core.compiler import workflow_revision_id
from autoagent.core.runtime.time import utc_timestamp_ms
from autoagent.core.runtime.persistence import UserEventPersistenceError
from autoagent.core.server.trace import TraceProjectionReducer


class ApprovalSeed(BaseModel):
    message: str
from tests.helpers import dynamic_json_callable, isolated_app, started_app
from autoagent.core.runtime.backends.database import (
    _PersistenceItem,
    _is_retryable_database_error,
)
from autoagent.core.runtime.backends.models import (
    ArtifactRow,
    InvocationRow,
    RecoveryStateRow,
    RuntimeEventRow,
    SessionRow,
    UserEventRow,
)
from autoagent.core.runtime.user_event import UserEventSpec


class DatabaseBackendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "runtime.db"
        self.backend = DatabaseBackend.from_path(self.path)
        self.store = RuntimeStore(backend=self.backend)

    async def asyncTearDown(self) -> None:
        await self.store.aclose()
        self.directory.cleanup()

    async def test_cross_thread_database_operation_uses_scoped_fast_pulse(
        self,
    ) -> None:
        async def polling_waits() -> int:
            return self.backend._database_loop._polling_waits

        active = await self.backend._arun_database_operation(
            polling_waits()
        )

        self.assertEqual(1, active)
        self.assertEqual(0, self.backend._database_loop._polling_waits)

    async def test_sqlite_initialization_creates_missing_parent_directory(
        self,
    ) -> None:
        nested_path = (
            Path(self.directory.name)
            / ".autoagent"
            / "nested"
            / "runtime.db"
        )
        backend = DatabaseBackend.from_path(nested_path)
        store = RuntimeStore(backend=backend)
        try:
            self.assertFalse(nested_path.parent.exists())
            await store.ainitialize()
            self.assertTrue(nested_path.parent.is_dir())
            self.assertTrue(nested_path.is_file())
        finally:
            await store.aclose()

    async def test_v1_schema_has_events_artifacts_and_invocation_checkpoints(
        self,
    ) -> None:
        await self.store.ainitialize()

        async def table_names() -> set[str]:
            async with self.backend.engine.connect() as connection:
                rows = await connection.execute(
                    text("SELECT name FROM sqlite_master WHERE type = 'table'")
                )
                return set(rows.scalars())

        names = await self.backend._database_loop.arun(table_names())
        self.assertEqual(
            {
                "workflow_versions",
                "sessions",
                "invocations",
                "runtime_events",
                "user_events",
                "runtime_recovery_states",
                "artifacts",
            },
            names,
        )

        async def table_columns(table_name: str) -> set[str]:
            async with self.backend.engine.connect() as connection:
                return set(
                    await connection.run_sync(
                        lambda sync_connection: {
                            column["name"]
                            for column in inspect(sync_connection).get_columns(
                                table_name
                            )
                        }
                    )
                )

        invocation_columns = await self.backend._database_loop.arun(
            table_columns("invocations")
        )
        workflow_columns = await self.backend._database_loop.arun(
            table_columns("workflow_versions")
        )
        session_columns = await self.backend._database_loop.arun(
            table_columns("sessions")
        )
        user_event_columns = await self.backend._database_loop.arun(
            table_columns("user_events")
        )
        runtime_event_columns = await self.backend._database_loop.arun(
            table_columns("runtime_events")
        )
        artifact_columns = await self.backend._database_loop.arun(
            table_columns("artifacts")
        )
        self.assertIn("genesis_state_json", invocation_columns)
        self.assertIn("event_mode", invocation_columns)
        self.assertIn("input_json", invocation_columns)
        self.assertIn("result_json", invocation_columns)
        self.assertNotIn("recovery_state_json", invocation_columns)
        self.assertNotIn("recovery_sequence", invocation_columns)
        self.assertNotIn("state_json", invocation_columns)
        self.assertNotIn("snapshot_json", workflow_columns)
        self.assertIn("definition_json", workflow_columns)
        self.assertIn("workflow_revision_id", session_columns)
        self.assertNotIn("operator_manifest_hash", workflow_columns)
        self.assertNotIn("operator_manifests_json", workflow_columns)
        self.assertNotIn("namespace", workflow_columns)
        self.assertNotIn("namespace", session_columns)
        self.assertNotIn("namespace", artifact_columns)
        self.assertIn("workflow_path_json", user_event_columns)
        self.assertIn("node_execution_id", runtime_event_columns)

    async def test_close_abandons_flush_after_shutdown_deadline(self) -> None:
        self.backend.shutdown_timeout_ms = 10
        await self.store.ainitialize()
        original_aflush = self.backend.aflush

        async def blocked_flush() -> None:
            await asyncio.Event().wait()

        self.backend.aflush = blocked_flush  # type: ignore[method-assign]
        try:
            await asyncio.wait_for(self.store.aclose(), timeout=0.5)
        finally:
            self.backend.aflush = original_aflush  # type: ignore[method-assign]

        self.assertFalse(self.backend._initialized)

    async def test_sqlite_uses_wal_with_full_durability_by_default(
        self,
    ) -> None:
        await self.store.ainitialize()

        async def pragmas() -> tuple[str, int]:
            async with self.backend.engine.connect() as connection:
                journal = await connection.execute(text("PRAGMA journal_mode"))
                synchronous = await connection.execute(
                    text("PRAGMA synchronous")
                )
                return str(journal.scalar_one()), int(synchronous.scalar_one())

        journal, synchronous = await self.backend._database_loop.arun(pragmas())
        self.assertEqual("wal", journal.lower())
        self.assertEqual(2, synchronous)

    async def test_sqlite_normal_durability_is_explicit_opt_in(self) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(
            self.path,
            sqlite_synchronous="NORMAL",
        )
        self.store = RuntimeStore(backend=self.backend)
        await self.store.ainitialize()

        async def synchronous_pragma() -> int:
            async with self.backend.engine.connect() as connection:
                result = await connection.execute(text("PRAGMA synchronous"))
                return int(result.scalar_one())

        synchronous = await self.backend._database_loop.arun(
            synchronous_pragma()
        )
        self.assertEqual(1, synchronous)

    def test_sqlite_durability_rejects_unknown_profile(self) -> None:
        with self.assertRaisesRegex(ValueError, "FULL or NORMAL"):
            DatabaseBackend.from_path(
                self.path,
                sqlite_synchronous="OFF",
            )

    async def test_large_admission_value_is_externalized_and_recoverable(
        self,
    ) -> None:
        await self.store.aclose()
        serializer = JsonRuntimeSerializer(max_inline_bytes=2_048)
        self.backend = DatabaseBackend.from_path(self.path)
        self.store = RuntimeStore(
            backend=self.backend,
            serializer=serializer,
        )
        workflow = Workflow(id="atomic_admission")
        workflow.add_node(dynamic_json_callable(lambda value: len(value)), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        invocation = await app.ainvoke(
            workflow,
            input={"value": "x" * 10_000},
            session_id="same",
        )
        await self.store.aflush()

        session = self.store.find_session(
            workflow_revision_id=invocation.workflow_revision_id,
            session_key="same",
        )
        self.assertIsNotNone(session)
        assert session is not None
        self.assertEqual(invocation.id, session.get_current_invocation().id)
        self.assertEqual({"output": 10_000}, invocation.result)
        self.assertEqual({}, self.store._pending_admissions)

        async def artifact_count() -> int:
            async with self.backend._database_sessions() as database:
                return len((await database.scalars(select(ArtifactRow))).all())

        self.assertGreater(
            await self.backend._database_loop.arun(artifact_count()),
            0,
        )
        await app.aclose()

    async def test_event_serialization_failure_is_invocation_scoped(
        self,
    ) -> None:
        await self.store.aclose()
        serializer = JsonRuntimeSerializer(max_inline_bytes=10_000)
        self.backend = DatabaseBackend.from_path(
            self.path,
            artifact_policy=ArtifactPolicy(enabled=False),
        )
        self.store = RuntimeStore(
            backend=self.backend,
            serializer=serializer,
        )
        workflow = Workflow(id="atomic_event")
        workflow.add_node(dynamic_json_callable(lambda: "x" * 20_000), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        invocation = await app.ainvoke(
            workflow,
            session_id="same",
            event_mode="full",
        )

        session = self.store.find_session(
            workflow_revision_id=invocation.workflow_revision_id,
            session_key="same",
        )
        assert session is not None
        current = session.get_current_invocation()
        assert current is not None
        self.assertEqual(invocation.id, current.id)
        self.assertEqual("completed", invocation.state)
        self.assertEqual(
            capture_execution_state(session, invocation),
            self.store.reduced_state(invocation.id),
        )
        self.assertEqual(
            invocation.event_sequence,
            len(self.store.runtime_events[invocation.id]),
        )
        while self.store.persistence_status(invocation.id) == "pending":
            await asyncio.sleep(0.001)
        self.assertEqual(
            "unserializable",
            self.store.persistence_status(invocation.id),
        )
        with self.assertRaisesRegex(RuntimeError, "sequence"):
            await self.store.aflush()

        small = Workflow(id="serialization_isolation")
        small.add_node(dynamic_json_callable(lambda: "ok"), node_id="node")
        other = await app.ainvoke(small, session_id="other")
        while self.store.persistence_status(other.id) == "pending":
            await asyncio.sleep(0.001)
        self.assertEqual("completed", other.state)
        self.assertEqual("durable", self.store.persistence_status(other.id))
        with self.assertRaisesRegex(RuntimeError, "sequence"):
            await app.aclose()

    async def test_event_preparation_is_submitted_without_waiting(self) -> None:
        release = threading.Event()

        class SlowAcceptanceBackend(DatabaseBackend):
            def _prepare_event_item(self, envelope):
                while not release.is_set():
                    release.wait(0.001)
                return super()._prepare_event_item(envelope)

        await self.store.aclose()
        self.backend = SlowAcceptanceBackend.from_path(self.path)
        self.store = RuntimeStore(backend=self.backend)
        workflow = Workflow(id="one_way_event_submission")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        invocation = await asyncio.wait_for(app.ainvoke(workflow), timeout=1)

        self.assertEqual("completed", invocation.state)
        self.assertEqual(
            invocation.event_sequence,
            len(self.store.runtime_events[invocation.id]),
        )
        self.assertGreater(self.store.pending_persistence_count, 0)
        self.assertGreater(self.store.pending_persistence_bytes, 0)
        release.set()
        await self.store.aflush()
        self.assertEqual("durable", self.store.persistence_status(invocation.id))
        await app.aclose()

    async def test_unavailable_backend_does_not_interrupt_execution(
        self,
    ) -> None:
        workflow = Workflow(id="unavailable_event_acceptance")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        entry = app.register_workflow(workflow)
        await self.store.asave_workflow_snapshot(
            entry.workflow_snapshot,
        )
        revision_id = workflow_revision_id(
            entry.workflow_snapshot.workflow_id,
            entry.workflow_snapshot.definition_hash,
        )
        session = await self.store.aget_or_create_session(
            workflow_id=workflow.id,
            workflow_revision_id=revision_id,
            session_key="same",
        )
        invocation = Invocation(
            workflow_id=workflow.id,
            workflow_revision_id=revision_id,
            workflow_version=entry.workflow_ir.workflow_version,
            workflow_definition_hash=entry.workflow_ir.definition_hash,
            entry_node_id="node",
        )
        session = await self.store.aadmit_invocation(
            session.id,
            invocation,
        )
        await self.store.aflush()
        assert self.store.persistence is not None
        self.store.persistence.mark_unavailable(RuntimeError("offline"))

        completed = await app.workflow_executor.ainvoke(
            workflow_ir=entry.workflow_ir,
            session=session,
            invocation=invocation,
        )

        self.assertEqual("completed", completed.state)
        self.assertGreater(completed.event_sequence, 0)
        self.assertEqual(
            "degraded",
            self.store.persistence_status(invocation.id),
        )
        another = await app.ainvoke(workflow, session_id="new")
        self.assertEqual("completed", another.state)
        self.assertEqual(
            "degraded",
            self.store.persistence_status(another.id),
        )
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            await app.aclose()

    async def test_permanent_write_failure_releases_prepared_items(
        self,
    ) -> None:
        class FailingEventBackend(DatabaseBackend):
            async def _persist_batch(self, batch):
                if any(item.kind == "event" for item in batch):
                    raise RuntimeError("permanent event write failure")
                await super()._persist_batch(batch)

        await self.store.aclose()
        backend = FailingEventBackend.from_path(self.path)
        store = RuntimeStore(backend=backend)
        self.backend = backend
        self.store = store
        workflow = Workflow(id="permanent_write_failure")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=store)
        await app.astart()

        with self.assertLogs(
            "autoagent.core.runtime.backends.database",
            level="ERROR",
        ):
            invocation = await app.ainvoke(workflow)
            for _ in range(200):
                assert store.persistence is not None
                if store.persistence.health.state == "unavailable":
                    break
                await asyncio.sleep(0.005)

        assert store.persistence is not None
        self.assertEqual("completed", invocation.state)
        self.assertEqual("unavailable", store.persistence.health.state)
        self.assertEqual({}, backend._queues)
        self.assertEqual(0, backend._pending_count)
        self.assertEqual(0, backend._pending_bytes)
        self.assertEqual(0, backend._inflight_count)
        self.assertEqual(0, backend._inflight_bytes)
        self.assertFalse(hasattr(backend, "_halted_items"))
        self.assertGreater(store.persistence.pending_count, 0)
        self.assertEqual(
            store.persistence.pending_count,
            store.pending_persistence_count,
        )

        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            await app.aclose()

    async def test_batch_byte_limit_finishes_current_batch(self) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(
            self.path,
            batch_max_bytes=1_000,
            batch_max_delay_ms=0,
        )
        self.store = RuntimeStore(backend=self.backend)
        session_id = Invocation(
            workflow_id="batch",
            workflow_revision_id="batch-revision",
            workflow_version=1,
            entry_node_id="node",
        ).id
        key = str(session_id)
        self.backend._queues[key] = deque(
            [
                _PersistenceItem(
                    kind="event",
                    session_id=session_id,
                    invocation_id=None,
                    record={},
                    encoded=b"x",
                    artifacts=(),
                    size_bytes=700,
                ),
                _PersistenceItem(
                    kind="event",
                    session_id=session_id,
                    invocation_id=None,
                    record={},
                    encoded=b"x",
                    artifacts=(),
                    size_bytes=700,
                ),
            ]
        )
        self.backend._ready_sessions.append(key)
        self.backend._ready_set.add(key)

        first = await asyncio.wait_for(
            self.backend._take_batch(),
            timeout=0.1,
        )
        second = await asyncio.wait_for(
            self.backend._take_batch(),
            timeout=0.1,
        )

        self.assertEqual([700], [item.size_bytes for item in first])
        self.assertEqual([700], [item.size_bytes for item in second])

    async def test_session_row_timestamp_tracks_committed_events(self) -> None:
        workflow = Workflow(id="session_timestamp")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(workflow, session_id="same")
        await self.store.aflush()
        session = self.store.sessions[
            self.store.invocation_sessions[invocation.id]
        ]

        async def load_updated_at() -> int:
            async with self.backend._database_sessions() as database:
                row = await database.scalar(
                    select(SessionRow).where(
                        SessionRow.id == str(session.id)
                    )
                )
                assert row is not None
                return row.updated_at_ms

        persisted = await self.backend._database_loop.arun(
            load_updated_at()
        )
        self.assertEqual(session.updated_at_ms, persisted)
        await app.aclose()

    def test_recovery_event_interval_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "recovery_event_interval"):
            DatabaseBackend.from_path(self.path, recovery_event_interval=0)

    async def test_user_events_persist_for_every_mode_without_builtin_deltas(
        self,
    ) -> None:
        workflow = Workflow(id="durable_user_events")
        workflow.add_node(dynamic_json_callable(lambda value: value), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocations = [
            await app.ainvoke(
                workflow,
                input={"value": mode},
                session_id=mode,
                event_mode=mode,
            )
            for mode in ("minimal", "standard", "full")
        ]
        for invocation in invocations:
            for event_type in (
                "message_delta",
                "reasoning_delta",
                "tool_call_delta",
            ):
                self.store.record_user_event(
                    invocation_id=invocation.id,
                    spec=UserEventSpec(
                        type=event_type,
                        data={"delta": "transient"},
                        node_id="node",
                        node_execution_id=uuid4(),
                        workflow_path=("agent",),
                    ),
                )
            self.store.record_user_event(
                invocation_id=invocation.id,
                spec=UserEventSpec(
                    type="tool_call_requested",
                    data={
                        "call_id": "call-1",
                        "reasoning_content": "authoritative reasoning",
                        "arguments": {"city": "Paris"},
                    },
                    node_id="node",
                    node_execution_id=uuid4(),
                    workflow_path=("agent",),
                ),
            )
            self.store.record_user_event(
                invocation_id=invocation.id,
                spec=UserEventSpec(
                    type="custom_stream_chunk",
                    data={"text": "x" * 20_000},
                    node_id="node",
                    node_execution_id=uuid4(),
                    workflow_path=("agent",),
                ),
            )
        await self.store.aflush()

        async def load_rows() -> tuple[list[UserEventRow], list[ArtifactRow]]:
            async with self.backend._database_sessions() as database:
                return (
                    list(
                        (
                            await database.scalars(
                                select(UserEventRow).order_by(
                                    UserEventRow.invocation_id,
                                    UserEventRow.sequence,
                                )
                            )
                        ).all()
                    ),
                    list(
                        (await database.scalars(select(ArtifactRow))).all()
                    ),
                )

        rows, artifacts = await self.backend._database_loop.arun(load_rows())
        self.assertEqual(6, len(rows))
        self.assertEqual(
            {"tool_call_requested", "custom_stream_chunk"},
            {row.type for row in rows},
        )
        self.assertEqual(
            {str(invocation.id) for invocation in invocations},
            {row.invocation_id for row in rows},
        )
        self.assertEqual(
            {str(self.store.invocation_sessions[value.id]) for value in invocations},
            {row.session_id for row in rows},
        )
        self.assertEqual(
            {'["agent"]'},
            {row.workflow_path_json for row in rows},
        )
        self.assertEqual({2}, {row.schema_version for row in rows})
        self.assertTrue(artifacts)

        first_id = invocations[0].id
        await app.aclose()
        reopened_backend = DatabaseBackend.from_path(self.path)
        reopened = RuntimeStore(backend=reopened_backend)
        self.backend = reopened_backend
        self.store = reopened
        persisted = await reopened.alist_user_events(
            invocation_id=first_id,
            limit=20,
        )
        self.assertEqual(
            ["tool_call_requested", "custom_stream_chunk"],
            [event.type for event in persisted],
        )
        self.assertEqual(
            "authoritative reasoning",
            persisted[0].data["reasoning_content"],
        )
        self.assertEqual("x" * 20_000, persisted[1].data["text"])
        self.assertEqual(("agent",), persisted[0].workflow_path)
        self.assertEqual(2, persisted[0].schema_version)
        self.assertEqual(
            persisted[-1].sequence,
            await reopened.alatest_user_event_sequence(first_id),
        )

    async def test_user_event_write_failure_does_not_poison_runtime_journal(
        self,
    ) -> None:
        class InvalidUserEventBackend(DatabaseBackend):
            async def _persist_user_events(self, database, items) -> None:
                raise RuntimeError("invalid user event")

        await self.store.aclose()
        self.backend = InvalidUserEventBackend.from_path(self.path)
        self.store = RuntimeStore(backend=self.backend)
        workflow = Workflow(id="isolated_user_event_failure")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(workflow, event_mode="full")
        await self.store.aflush()

        self.store.record_user_event(
            invocation_id=invocation.id,
            spec=UserEventSpec(
                type="agent_output",
                data={"output": "done"},
                node_id="node",
                node_execution_id=uuid4(),
            ),
        )
        with self.assertRaises(UserEventPersistenceError):
            await self.store.aflush()

        self.assertEqual("healthy", self.store.persistence.health.state)
        self.assertEqual(
            "durable",
            self.store.persistence_status(invocation.id),
        )
        self.assertEqual(
            "degraded",
            self.store.user_event_persistence_status(invocation.id),
        )
        with self.assertRaises(UserEventPersistenceError):
            await app.aclose()

    async def test_trace_invocations_support_bidirectional_anchor_queries(
        self,
    ) -> None:
        workflow = Workflow(id="bidirectional_trace_invocations")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        created = [
            await app.ainvoke(workflow, session_id="neighbors")
            for _ in range(5)
        ]
        await self.store.aflush()
        ordered = sorted(
            created,
            key=lambda value: (value.created_at_ms, str(value.id)),
        )
        anchor = ordered[2]
        session_id = self.store.invocation_sessions[anchor.id]
        anchor_key = (anchor.created_at_ms, str(anchor.id))

        older = await self.backend.alist_trace_invocations(
            session_id=session_id,
            before=anchor_key,
            limit=1,
        )
        newer = await self.backend.alist_trace_invocations(
            session_id=session_id,
            after=anchor_key,
            limit=1,
        )

        self.assertEqual(str(ordered[1].id), older[0]["id"])
        self.assertEqual(str(ordered[3].id), newer[0]["id"])
        await app.aclose()

    def test_admission_timeout_must_be_non_negative(self) -> None:
        with self.assertRaisesRegex(ValueError, "admission_timeout_ms"):
            PersistencePolicy(admission_timeout_ms=-1)

    def test_retryable_database_errors_are_classified_explicitly(self) -> None:
        locked = OperationalError(
            "INSERT",
            {},
            OSError("database is locked"),
            connection_invalidated=False,
        )
        invalid_query = OperationalError(
            "INSERT",
            {},
            OSError("no such table"),
            connection_invalidated=False,
        )
        self.assertTrue(_is_retryable_database_error(locked))
        self.assertFalse(_is_retryable_database_error(invalid_query))

    async def test_sqlite_round_trip_rebuilds_from_genesis_and_runtime_events(self) -> None:
        workflow = Workflow(id="database_round_trip")
        workflow.add_node(dynamic_json_callable(lambda value: value + 1), node_id="increment")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(
            workflow,
            input={"value": 1},
            session_id="session",
            event_mode="full",
        )
        await app.aclose()

        self.backend = DatabaseBackend.from_path(self.path)
        reopened = RuntimeStore(backend=self.backend)
        self.store = reopened
        events = await reopened.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )
        _, rebuilt = await reopened.arebuild_execution(invocation.id)

        self.assertEqual("completed", rebuilt.state)
        self.assertEqual(invocation.result, rebuilt.result)
        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )
        self.assertTrue(all(event.operations is not None for event in events))

    async def test_parallel_internal_event_sequence_survives_sqlite_round_trip(
        self,
    ) -> None:
        selector_completed = False

        def select_items(ctx):
            nonlocal selector_completed
            selector_completed = True
            return [{"value": value} for value in ctx.input]

        async def finish_after_selector(value: list[int]) -> list[int]:
            while not selector_completed:
                await asyncio.sleep(0)
            return value

        async def slow_map_item(value: int) -> int:
            await asyncio.sleep(0.03)
            return value

        workflow = Workflow(id="database_parallel_event_sequence")
        workflow.add_node(dynamic_json_callable(lambda: [1]), node_id="start")
        workflow.add_node(
            finish_after_selector,
            node_id="fast",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_node(
            slow_map_item,
            node_id="mapped",
            policy=NodePolicy(map=MapPolicy(item_selector=select_items)),
        )
        workflow.add_edge("start", "fast")
        workflow.add_edge(
            "start",
            "mapped",
        )

        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(
            workflow,
            event_mode="full",
        )
        await app.aclose()

        self.backend = DatabaseBackend.from_path(self.path)
        reopened = RuntimeStore(backend=self.backend)
        self.store = reopened
        events = await reopened.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )

        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )

        def sequence(event_name: str, node_id: str) -> int:
            return next(
                event.sequence
                for event in events
                if event.event_name == event_name
                and event.payload.get("node_id") == node_id
            )

        self.assertLess(
            sequence("item_selection.completed", "mapped"),
            sequence("node.completed", "fast"),
        )
        self.assertLess(
            sequence("node.completed", "fast"),
            sequence("operator_call.completed", "mapped"),
        )

    async def test_large_runtime_value_is_deduplicated_and_hydrated(self) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(
            self.path,
            recovery_event_interval=2,
        )
        self.store = RuntimeStore(backend=self.backend)
        workflow = Workflow(id="artifact_round_trip")
        workflow.add_node(dynamic_json_callable(lambda: "x" * 100_000), node_id="source")
        workflow.add_node(
            dynamic_json_callable(lambda value: value),
            node_id="target",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_edge("source", "target")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(
            workflow,
            session_id="artifact",
            event_mode="full",
        )
        await self.store.aflush()

        async def persisted_artifacts() -> tuple[int, int, bool]:
            async with self.backend._database_sessions() as database:
                artifacts = (
                    await database.scalars(select(ArtifactRow))
                ).all()
                events = await database.execute(
                    text(
                        "SELECT COALESCE(payload_json, '') || "
                        "COALESCE(input_json, '') || "
                        "COALESCE(output_json, '') || "
                        "COALESCE(operations_json, '') FROM runtime_events"
                    )
                )
                return (
                    len(artifacts),
                    sum(row.size_bytes for row in artifacts),
                    any(
                        '"__autoagent_type__":"artifact"' in payload
                        for payload in events.scalars()
                    ),
                )

        count, payload_bytes, event_has_ref = (
            await self.backend._database_loop.arun(persisted_artifacts())
        )
        self.assertEqual(1, count)
        self.assertGreaterEqual(payload_bytes, 100_000)
        self.assertTrue(event_has_ref)
        await app.aclose()

        self.backend = DatabaseBackend.from_path(
            self.path,
            recovery_event_interval=2,
        )
        reopened = RuntimeStore(backend=self.backend)
        self.store = reopened
        _, rebuilt = await reopened.arebuild_execution(invocation.id)
        events = await reopened.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )

        self.assertEqual(invocation.result, rebuilt.result)
        self.assertEqual(100_000, len(rebuilt.result["output"]))
        output_event = next(
            event
            for event in events
            if event.event_name == "operator_call.completed"
            and event.payload["node_id"] == "source"
        )
        self.assertEqual("x" * 100_000, output_event.output)

    async def test_terminal_standard_invocation_keeps_compact_recovery_state(
        self,
    ) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(
            self.path,
            recovery_event_interval=1_000,
        )
        self.store = RuntimeStore(backend=self.backend)
        workflow = Workflow(id="terminal_recovery_state")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(workflow)
        await self.store.aflush()

        async def load_row() -> tuple[InvocationRow, RecoveryStateRow | None]:
            async with self.backend._database_sessions() as database:
                row = await database.get(InvocationRow, str(invocation.id))
                assert row is not None
                recovery = await database.get(
                    RecoveryStateRow,
                    str(invocation.id),
                )
                return row, recovery

        row, recovery = await self.backend._database_loop.arun(load_row())
        self.assertEqual(invocation.event_sequence, row.durable_sequence)
        assert recovery is not None
        state = self.store.serializer.loads(recovery.state_json)
        self.assertLessEqual(recovery.event_sequence, invocation.event_sequence)
        self.assertEqual("standard", state["invocation"]["event_mode"])
        self.assertTrue(state["node_executions"])
        self.assertIsNone(state["node_executions"][0]["input"])
        self.assertEqual(
            1,
            state["node_executions"][0]["operator_summary"]["attempt_count"],
        )
        await app.aclose()

    async def test_event_modes_share_schema_but_persist_distinct_detail(self) -> None:
        workflow = Workflow(id="event_mode_storage")
        workflow.add_node(dynamic_json_callable(lambda value: value + 1), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        invocations = {
            mode: await app.ainvoke(
                workflow,
                input={"value": index},
                session_id=mode,
                event_mode=mode,
            )
            for index, mode in enumerate(("minimal", "standard", "full"))
        }
        await self.store.aflush()

        async def load_rows():
            async with self.backend._database_sessions() as database:
                invocation_rows = {
                    row.event_mode: row
                    for row in (await database.scalars(select(InvocationRow))).all()
                }
                event_rows = (
                    await database.scalars(select(RuntimeEventRow))
                ).all()
                recovery_rows = (
                    await database.scalars(select(RecoveryStateRow))
                ).all()
                return invocation_rows, event_rows, recovery_rows

        rows, events, recovery = await self.backend._database_loop.arun(load_rows())
        by_invocation: dict[str, list[RuntimeEventRow]] = {}
        for event in events:
            by_invocation.setdefault(event.invocation_id, []).append(event)

        minimal = rows["minimal"]
        self.assertEqual([], by_invocation.get(minimal.id, []))
        self.assertIsNone(minimal.genesis_state_json)
        self.assertEqual({"value": 0}, self.store.serializer.loads(minimal.input_json))
        self.assertEqual(
            {"output": 1},
            self.store.serializer.loads(minimal.result_json),
        )

        standard = rows["standard"]
        standard_events = by_invocation[standard.id]
        self.assertTrue(standard_events)
        self.assertTrue(all(row.operations_json is None for row in standard_events))
        self.assertNotIn(
            "input_mapping.completed",
            {row.event_name for row in standard_events},
        )
        self.assertIsNotNone(standard.genesis_state_json)
        standard_genesis = self.store.serializer.loads(
            standard.genesis_state_json
        )
        self.assertEqual("created", standard_genesis["invocation"]["state"])
        self.assertEqual([], standard_genesis["node_executions"])

        full = rows["full"]
        full_events = by_invocation[full.id]
        self.assertTrue(all(row.operations_json is not None for row in full_events))
        self.assertIn(
            "input_mapping.completed",
            {row.event_name for row in full_events},
        )
        self.assertIsNotNone(full.genesis_state_json)
        self.assertEqual(
            {
                str(invocations["standard"].id),
                str(invocations["full"].id),
            },
            {row.invocation_id for row in recovery},
        )
        await app.aclose()

    async def test_database_pages_every_map_call_by_node_execution(
        self,
    ) -> None:
        workflow = Workflow(id="persisted_actual_map_calls")
        workflow.add_node(
            dynamic_json_callable(lambda: list(range(60))),
            node_id="source",
        )
        workflow.add_node(
            dynamic_json_callable(lambda value: value * 2),
            node_id="mapped",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": value} for value in ctx.input
                    ],
                    max_parallelism=10,
                )
            ),
        )
        workflow.add_edge("source", "mapped")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(workflow, event_mode="full")
        await self.store.aflush()

        execution = invocation.latest_node_execution("mapped")
        events = await self.backend.alist_trace_runtime_events(
            invocation_id=invocation.id,
            after_sequence=0,
            limit=100,
            event_names=("operator_call.completed",),
            node_execution_id=execution.id,
        )

        self.assertEqual(60, len(events))
        self.assertEqual(list(range(60)), sorted(
            event.payload["unit_index"] for event in events
        ))
        self.assertTrue(all(event.input is not None for event in events))
        self.assertTrue(all(event.output is not None for event in events))
        await app.aclose()

    async def test_minimal_wait_resumes_only_while_runtime_memory_survives(
        self,
    ) -> None:
        workflow = Workflow(id="minimal_wait")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        waiting = await app.ainvoke(
            workflow,
            input={"wait_key": "same-process"},
            session_id="same-process",
            event_mode="minimal",
        )
        waiting_state = waiting.state
        resumed = await app.aresume(
            workflow,
            session_id="same-process",
            wait_key="same-process",
            output="ok",
        )
        self.assertEqual("waiting", waiting_state)
        self.assertEqual("completed", resumed.state)

        await app.ainvoke(
            workflow,
            input={"wait_key": "after-restart"},
            session_id="restart",
            event_mode="minimal",
        )
        await self.store.aflush()
        await app.aclose()

        self.backend = DatabaseBackend.from_path(self.path)
        self.store = RuntimeStore(backend=self.backend)
        reopened = AutoAgentApp(runtime_store=self.store)
        await reopened.astart()
        with self.assertRaisesRegex(ValueError, "current Invocation"):
            await reopened.aresume(
                workflow,
                session_id="restart",
                wait_key="after-restart",
                output="unavailable",
            )
        await reopened.aclose()

    async def test_wait_event_requests_recovery_state_without_waiting(self) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(
            self.path,
            recovery_event_interval=1_000,
        )
        self.store = RuntimeStore(backend=self.backend)
        workflow = Workflow(id="wait_recovery_state")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(
            workflow,
            input={"wait_key": "approval"},
            session_id="wait",
        )
        self.assertEqual("waiting", invocation.state)
        await self.store.aflush()

        async def load_row() -> RecoveryStateRow | None:
            async with self.backend._database_sessions() as database:
                return await database.get(
                    RecoveryStateRow,
                    str(invocation.id),
                )

        row = await self.backend._database_loop.arun(load_row())
        assert row is not None
        self.assertEqual(invocation.event_sequence, row.event_sequence)
        self.assertIsNotNone(row.state_json)
        await app.aclose()

    async def test_retention_can_evict_only_durable_terminal_invocations(
        self,
    ) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(self.path)
        self.store = RuntimeStore(
            backend=self.backend,
            retention_policy=RuntimeRetentionPolicy(
                mode="evict_durable_terminal",
            ),
        )
        workflow = Workflow(id="durable_retention")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(
            workflow,
            session_id="retention",
            event_mode="full",
        )
        await self.store.aflush()

        self.assertNotIn(invocation.id, self.store.invocations)
        self.assertNotIn(invocation.id, self.store.runtime_events)
        self.assertFalse(
            any(
                session.session_key == "retention"
                for session in self.store.sessions.values()
            )
        )
        self.assertNotIn(
            (invocation.workflow_revision_id, "retention"),
            self.store.session_keys,
        )
        self.assertEqual(
            "durable",
            self.store.persistence_status(invocation.id),
        )
        events = await self.store.alist_runtime_events(
            invocation_id=invocation.id,
            limit=100,
        )
        _, rebuilt = await self.store.arebuild_execution(invocation.id)
        self.assertEqual(invocation.event_sequence, len(events))
        self.assertEqual(invocation.result, rebuilt.result)
        await app.aclose()

    async def test_minimal_retention_waits_for_terminal_state_persistence(
        self,
    ) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(self.path)
        self.store = RuntimeStore(
            backend=self.backend,
            retention_policy=RuntimeRetentionPolicy(
                mode="evict_durable_terminal",
            ),
        )
        workflow = Workflow(id="minimal_retention")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(
            workflow,
            session_id="minimal",
            event_mode="minimal",
        )
        await self.store.aflush()

        self.assertNotIn(invocation.id, self.store.invocations)
        self.assertEqual("durable", self.store.persistence_status(invocation.id))
        self.assertEqual(
            (),
            await self.store.alist_runtime_events(invocation_id=invocation.id),
        )
        await app.aclose()

    async def test_rebuild_from_recovery_state_caches_complete_event_history(
        self,
    ) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(
            self.path,
            recovery_event_interval=2,
        )
        self.store = RuntimeStore(backend=self.backend)
        workflow = Workflow(id="snapshot_event_cache")
        workflow.add_node(dynamic_json_callable(lambda value: value + 1), node_id="increment")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        invocation = await app.ainvoke(
            workflow,
            input={"value": 1},
            event_mode="full",
        )
        await self.store.aflush()

        async def recovery_cursor() -> tuple[int | None, str | None]:
            async with self.backend._database_sessions() as database:
                row = await database.get(
                    RecoveryStateRow,
                    str(invocation.id),
                )
                if row is None:
                    return None, None
                return row.event_sequence, row.state_json

        cursor, recovery_state = await self.backend._database_loop.arun(
            recovery_cursor()
        )
        self.assertIsNotNone(recovery_state)
        self.assertIsNotNone(cursor)
        assert cursor is not None
        self.assertLess(
            invocation.event_sequence - cursor,
            self.backend.recovery_event_interval,
        )
        await app.aclose()

        self.backend = DatabaseBackend.from_path(
            self.path,
            recovery_event_interval=2,
        )
        reopened = RuntimeStore(backend=self.backend)
        self.store = reopened
        _, rebuilt = await reopened.arebuild_execution(invocation.id)
        cached = await reopened.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )

        self.assertEqual("completed", rebuilt.state)
        self.assertEqual(
            list(range(1, rebuilt.event_sequence + 1)),
            [event.sequence for event in cached],
        )

    async def test_sequences_restart_at_one_for_each_invocation(self) -> None:
        workflow = Workflow(id="per_invocation_sequence")
        workflow.add_node(dynamic_json_callable(lambda: "ok"), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        self.addAsyncCleanup(app.aclose)
        first = await app.ainvoke(workflow, session_id="same")
        second = await app.ainvoke(workflow, session_id="same")
        first_events = await self.store.alist_runtime_events(
            invocation_id=first.id,
            limit=10_000,
        )
        second_events = await self.store.alist_runtime_events(
            invocation_id=second.id,
            limit=10_000,
        )
        self.assertEqual(1, first_events[0].sequence)
        self.assertEqual(1, second_events[0].sequence)

    async def test_database_app_can_mix_sync_and_async_entrypoints(self) -> None:
        workflow = Workflow(id="database_mixed_api")
        workflow.add_node(dynamic_json_callable(lambda value: value), node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        synchronous_future: Future = Future()

        def invoke_synchronously() -> None:
            try:
                synchronous_future.set_result(
                    app.invoke(
                        workflow,
                        {"value": "sync"},
                        session_id="sync",
                    )
                )
            except BaseException as exc:
                synchronous_future.set_exception(exc)

        thread = threading.Thread(target=invoke_synchronously)
        thread.start()
        asynchronous = await app.ainvoke(
            workflow,
            input={"value": "async"},
            session_id="async",
        )
        while thread.is_alive():
            await asyncio.sleep(0.001)
        thread.join()
        synchronous = synchronous_future.result()

        self.assertEqual({"output": "sync"}, synchronous.result)
        self.assertEqual({"output": "async"}, asynchronous.result)
        await app.aclose()

    async def test_postgresql_url_uses_generic_async_dialect(self) -> None:
        backend = DatabaseBackend("postgresql://user:pass@localhost/runtime")
        try:
            self.assertEqual("postgresql", backend.engine.dialect.name)
            self.assertTrue(backend.database_url.startswith("postgresql+asyncpg://"))
        finally:
            await backend.aclose()

    async def test_database_string_requires_explicit_url_scheme(self) -> None:
        with self.assertRaisesRegex(ValueError, "explicit scheme"):
            DatabaseBackend("runtime.db")

    async def test_runtime_serialization_runs_on_persistence_thread(self) -> None:
        class RecordingSerializer(JsonRuntimeSerializer):
            def __init__(self) -> None:
                super().__init__()
                self.dump_threads: list[str] = []

            def dumps(self, value) -> bytes:
                self.dump_threads.append(threading.current_thread().name)
                return super().dumps(value)

        await self.store.aclose()
        serializer = RecordingSerializer()
        self.backend = DatabaseBackend.from_path(
            self.path,
            recovery_event_interval=1,
        )
        self.store = RuntimeStore(
            backend=self.backend,
            serializer=serializer,
        )
        execution_threads: list[str] = []

        async def run() -> str:
            execution_threads.append(threading.current_thread().name)
            return "done"

        workflow = Workflow(id="persistence_thread_serialization")
        workflow.add_node(run, node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        invocation = await app.ainvoke(workflow)

        self.assertEqual("completed", invocation.state)
        self.assertEqual(["autoagent-runtime"], execution_threads)
        await self.store.aflush()
        self.assertTrue(serializer.dump_threads)
        self.assertEqual(
            {"autoagent-persistence-runtime"},
            set(serializer.dump_threads),
        )
        await app.aclose()

    async def test_wait_and_resume_can_recover_after_an_explicit_flush(self) -> None:
        workflow = Workflow(id="database_wait_resume")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        waiting = await app.ainvoke(
            workflow,
            input={"wait_key": "approval"},
            session_id="session",
        )
        self.assertEqual("waiting", waiting.state)
        await app.aclose()

        self.backend = DatabaseBackend.from_path(self.path)
        reopened = RuntimeStore(backend=self.backend)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)
        restarted.register_workflow(workflow)
        await restarted.astart()
        self.addAsyncCleanup(restarted.aclose)
        resumed = await restarted.aresume(
            workflow,
            session_id="session",
            wait_key="approval",
            output={"approved": True},
        )
        self.assertEqual("completed", resumed.state)
        self.assertEqual({"output": {"approved": True}}, resumed.result)

    async def test_resume_restores_prior_outputs_from_exact_workflow_contracts(
        self,
    ) -> None:
        def prepare() -> ApprovalSeed:
            return ApprovalSeed(message="review")

        def map_wait(_ctx: Any) -> dict[str, Any]:
            return {"wait_key": "typed-approval"}

        def finish(seed: ApprovalSeed, approved: bool) -> str:
            return f"{seed.message}:{approved}"

        def map_finish(ctx: Any) -> dict[str, Any]:
            return {
                "seed": ctx.outputs.latest("prepare"),
                "approved": ctx.incoming[0].value["approved"],
            }

        workflow = Workflow(id="database_typed_resume")
        workflow.add_node(prepare, node_id="prepare")
        workflow.add_node(
            SystemCommand(id="wait"),
            node_id="wait",
            input_mapping=map_wait,
        )
        workflow.add_node(finish, node_id="finish", input_mapping=map_finish)
        workflow.add_edge("prepare", "wait")
        workflow.add_edge("wait", "finish")

        first = AutoAgentApp(runtime_store=self.store)
        await first.astart()
        waiting = await first.ainvoke(workflow, session_id="typed-resume")
        await self.store.aflush()
        await first.aclose()

        self.backend = DatabaseBackend.from_path(self.path)
        reopened = RuntimeStore(backend=self.backend)
        self.store = reopened
        _, observed = await reopened.arebuild_execution(waiting.id)
        self.assertIsInstance(observed.latest_node_execution("prepare").output, dict)

        restarted = AutoAgentApp(runtime_store=reopened)
        restarted.register_workflow(workflow)
        await restarted.astart()
        resumed = await restarted.aresume(
            workflow,
            session_id="typed-resume",
            wait_key="typed-approval",
            output={"approved": True},
        )

        self.assertEqual("completed", resumed.state)
        self.assertEqual({"output": "review:True"}, resumed.result)
        self.assertIsInstance(
            resumed.latest_node_execution("prepare").output,
            ApprovalSeed,
        )
        await restarted.aclose()

    async def test_invoke_does_not_lazily_recover_unregistered_workflow(
        self,
    ) -> None:
        workflow = Workflow(id="startup_only_recovery")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        first = AutoAgentApp(runtime_store=self.store)
        first.register_workflow(workflow)
        await first.astart()
        waiting = await first.ainvoke(
            workflow,
            input={"wait_key": "approval"},
            session_id="session",
        )
        self.assertEqual("waiting", waiting.state)
        await first.aclose()

        self.backend = DatabaseBackend.from_path(self.path)
        reopened_store = RuntimeStore(backend=self.backend)
        self.store = reopened_store
        reopened = AutoAgentApp(runtime_store=reopened_store)
        await reopened.astart()

        with self.assertRaisesRegex(
            RuntimeError,
            "Register the Workflow before starting",
        ):
            await reopened.ainvoke(
                workflow,
                session_id="session",
            )
        self.assertEqual({}, reopened_store.invocations)
        await reopened.aclose()

    async def test_start_ignores_active_unregistered_workflow_revision(
        self,
    ) -> None:
        historical = Workflow(id="historical_active_revision")
        historical.add_node(SystemCommand(id="wait"), node_id="wait")
        first = AutoAgentApp(runtime_store=self.store)
        first.register_workflow(historical)
        await first.astart()
        waiting = await first.ainvoke(
            historical,
            input={"wait_key": "approval"},
            session_id="historical-session",
        )
        self.assertEqual("waiting", waiting.state)
        await self.store.aflush()
        invocation_id = waiting.id
        await first.aclose()

        self.backend = DatabaseBackend.from_path(self.path)
        reopened_store = RuntimeStore(backend=self.backend)
        self.store = reopened_store
        reopened = AutoAgentApp(runtime_store=reopened_store)
        registered = Workflow(id="registered_other_revision")
        registered.add_node(dynamic_json_callable(lambda: "ok"), node_id="done")
        reopened.register_workflow(registered)
        await reopened.astart()
        try:
            self.assertNotIn(invocation_id, reopened_store.invocations)
            persisted = await self.backend.aload_trace_invocation(
                invocation_id
            )
            self.assertIsNotNone(persisted)
            assert persisted is not None
            self.assertEqual("waiting", persisted["state"])
        finally:
            await reopened.aclose()

    async def test_transient_database_failure_retries_without_losing_events(self) -> None:
        class FlakyBackend(DatabaseBackend):
            failures_remaining = 1

            async def _persist_batch(self, batch) -> None:
                if (
                    any(item.kind == "event" for item in batch)
                    and self.failures_remaining
                ):
                    self.failures_remaining -= 1
                    raise OperationalError(
                        "INSERT",
                        {},
                        OSError("database temporarily unavailable"),
                        connection_invalidated=True,
                    )
                await super()._persist_batch(batch)

        await self.store.aclose()
        flaky_backend = FlakyBackend.from_path(self.path)
        flaky = RuntimeStore(backend=flaky_backend)
        self.backend = flaky_backend
        self.store = flaky
        workflow = Workflow(id="database_retry_queue")
        workflow.add_node(dynamic_json_callable(lambda: "ok"), node_id="node")
        app = AutoAgentApp(runtime_store=flaky)
        await app.astart()
        invocation = await app.ainvoke(workflow)
        await flaky.aflush()
        await app.aclose()
        self.backend = DatabaseBackend.from_path(self.path)
        reopened = RuntimeStore(backend=self.backend)
        self.store = reopened
        events = await reopened.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )

        self.assertEqual(0, flaky_backend.failures_remaining)
        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )

    async def test_terminal_return_does_not_wait_for_event_persistence(self) -> None:
        release = threading.Event()

        class SlowBackend(DatabaseBackend):
            async def _persist_batch(self, batch) -> None:
                if any(item.kind == "event" for item in batch):
                    while not release.is_set():
                        await asyncio.sleep(0.001)
                await super()._persist_batch(batch)

        await self.store.aclose()
        slow_backend = SlowBackend.from_path(self.path)
        slow = RuntimeStore(backend=slow_backend)
        self.backend = slow_backend
        self.store = slow
        workflow = Workflow(id="nonblocking_terminal")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=slow)
        await app.astart()

        invocation = await asyncio.wait_for(app.ainvoke(workflow), timeout=1)

        self.assertEqual("completed", invocation.state)
        self.assertEqual("pending", slow.persistence_status(invocation.id))
        self.assertGreater(slow.pending_persistence_bytes, 0)
        release.set()
        await slow.aflush()
        self.assertEqual("durable", slow.persistence_status(invocation.id))
        await app.aclose()

    async def test_wait_and_resume_do_not_wait_for_persistence(self) -> None:
        release = threading.Event()

        class BlockedBackend(DatabaseBackend):
            async def _persist_batch(self, batch) -> None:
                while not release.is_set():
                    await asyncio.sleep(0.001)
                await super()._persist_batch(batch)

        await self.store.aclose()
        blocked_backend = BlockedBackend.from_path(self.path)
        blocked = RuntimeStore(backend=blocked_backend)
        self.backend = blocked_backend
        self.store = blocked
        workflow = Workflow(id="nonblocking_wait_resume")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=blocked)
        await app.astart()

        waiting = await asyncio.wait_for(
            app.ainvoke(
                workflow,
                input={"wait_key": "approval"},
                session_id="session",
            ),
            timeout=1,
        )
        self.assertEqual("waiting", waiting.state)
        self.assertEqual("pending", blocked.persistence_status(waiting.id))

        resumed = await asyncio.wait_for(
            app.aresume(
                workflow,
                session_id="session",
                wait_key="approval",
                output={"approved": True},
            ),
            timeout=1,
        )
        self.assertEqual("completed", resumed.state)
        self.assertGreater(blocked.pending_persistence_count, 0)

        release.set()
        await blocked.aflush()
        self.assertEqual("durable", blocked.persistence_status(resumed.id))
        await app.aclose()

    async def test_full_wait_resume_applies_persisted_operations_once(self) -> None:
        workflow = Workflow(id="full_wait_resume_operations")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        waiting = await app.ainvoke(
            workflow,
            input={"wait_key": "approval"},
            session_id="full-resume",
            event_mode="full",
        )
        self.assertEqual("waiting", waiting.state)
        await self.store.aflush()

        resumed = await app.aresume(
            workflow,
            session_id="full-resume",
            wait_key="approval",
            output={"approved": True},
        )
        self.assertEqual("completed", resumed.state)
        await self.store.aflush()

        assert self.store.persistence is not None
        self.assertEqual("healthy", self.store.persistence.health.state)
        self.assertEqual("durable", self.store.persistence_status(resumed.id))
        await app.aclose()

    async def test_unavailable_database_rejects_only_after_backlog_limit(
        self,
    ) -> None:
        await self.store.aclose()
        unavailable_backend = DatabaseBackend.from_path(self.path)
        unavailable = RuntimeStore(
            backend=unavailable_backend,
            persistence_policy=PersistencePolicy(
                queue_low_watermark_bytes=8 * 1024,
                queue_high_watermark_bytes=16 * 1024,
                queue_hard_watermark_bytes=1024 * 1024,
                admission_timeout_ms=50,
            ),
        )
        self.backend = unavailable_backend
        self.store = unavailable
        workflow = Workflow(id="unavailable_backlog")
        workflow.add_node(dynamic_json_callable(lambda: "x" * (32 * 1024)), node_id="node")
        app = AutoAgentApp(runtime_store=unavailable)
        await app.astart()
        assert unavailable.persistence is not None
        unavailable.persistence.mark_unavailable(RuntimeError("database offline"))

        first = await asyncio.wait_for(
            app.ainvoke(workflow, session_id="first"),
            timeout=1,
        )
        self.assertEqual("completed", first.state)
        self.assertTrue(unavailable.admission_paused)

        with self.assertRaisesRegex(
            PersistenceAdmissionError,
            "database persistence is unavailable.*database offline",
        ):
            await app.ainvoke(workflow, session_id="second")

        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            await app.aclose()

    async def test_hard_queue_limit_degrades_persistence_not_execution(
        self,
    ) -> None:
        await self.store.aclose()
        bounded_backend = DatabaseBackend.from_path(self.path)
        bounded = RuntimeStore(
            backend=bounded_backend,
            persistence_policy=PersistencePolicy(
                queue_low_watermark_bytes=32 * 1024,
                queue_high_watermark_bytes=64 * 1024,
                queue_hard_watermark_bytes=80 * 1024,
                admission_timeout_ms=0,
            ),
        )
        self.backend = bounded_backend
        self.store = bounded
        workflow = Workflow(id="hard_queue_execution_priority")
        workflow.add_node(dynamic_json_callable(lambda: "x" * (128 * 1024)), node_id="node")
        app = AutoAgentApp(runtime_store=bounded)
        await app.astart()

        invocation = await asyncio.wait_for(app.ainvoke(workflow), timeout=1)

        self.assertEqual("completed", invocation.state)
        self.assertEqual("degraded", bounded.persistence_status(invocation.id))
        self.assertLessEqual(
            bounded.pending_persistence_bytes,
            80 * 1024,
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "hard memory limit",
        ):
            await app.aclose()

    async def test_database_initialization_failure_rejects_invocation(
        self,
    ) -> None:
        class UnavailableAtStartBackend(DatabaseBackend):
            async def ainitialize(self) -> None:
                raise OperationalError(
                    "CONNECT",
                    {},
                    OSError("database offline at startup"),
                    connection_invalidated=True,
                )

        await self.store.aclose()
        unavailable_backend = UnavailableAtStartBackend.from_path(self.path)
        unavailable = RuntimeStore(backend=unavailable_backend)
        self.backend = unavailable_backend
        self.store = unavailable
        workflow = Workflow(id="startup_database_failure")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=unavailable)

        with self.assertRaisesRegex(
            OperationalError,
            "database offline at startup",
        ):
            await app.astart()
        with self.assertRaisesRegex(
            RuntimeError,
            "not started",
        ):
            await app.ainvoke(workflow)

        self.assertEqual({}, unavailable.invocations)
        self.assertEqual({}, unavailable.runtime_events)
        await app.aclose()

    async def test_byte_backpressure_rejects_new_admission_until_queue_drains(
        self,
    ) -> None:
        release = threading.Event()

        class SlowBackend(DatabaseBackend):
            async def _persist_batch(self, batch) -> None:
                if any(item.kind == "event" for item in batch):
                    while not release.is_set():
                        await asyncio.sleep(0.001)
                await super()._persist_batch(batch)

        await self.store.aclose()
        slow_backend = SlowBackend.from_path(self.path)
        slow = RuntimeStore(
            backend=slow_backend,
            persistence_policy=PersistencePolicy(
                queue_low_watermark_bytes=8 * 1024,
                queue_high_watermark_bytes=16 * 1024,
                queue_hard_watermark_bytes=1024 * 1024,
                admission_timeout_ms=0,
            ),
        )
        self.backend = slow_backend
        self.store = slow
        workflow = Workflow(id="byte_backpressure")
        workflow.add_node(dynamic_json_callable(lambda: "x" * (32 * 1024)), node_id="node")
        app = AutoAgentApp(runtime_store=slow)
        await app.astart()
        first = await app.ainvoke(workflow, session_id="first")

        self.assertEqual("completed", first.state)
        self.assertTrue(slow.admission_paused)
        with self.assertRaisesRegex(RuntimeError, "backlog"):
            await app.ainvoke(workflow, session_id="second")

        release.set()
        await slow.aflush()
        second = await app.ainvoke(workflow, session_id="second")
        self.assertEqual("completed", second.state)
        await app.aclose()

    async def test_admission_timeout_zero_rejects_immediately(self) -> None:
        """A zero admission timeout restores immediate rejection."""

        release = threading.Event()

        class SlowBackend(DatabaseBackend):
            async def _persist_batch(self, batch) -> None:
                if any(item.kind == "event" for item in batch):
                    while not release.is_set():
                        await asyncio.sleep(0.001)
                await super()._persist_batch(batch)

        await self.store.aclose()
        slow_backend = SlowBackend.from_path(self.path)
        slow = RuntimeStore(
            backend=slow_backend,
            persistence_policy=PersistencePolicy(
                queue_low_watermark_bytes=8 * 1024,
                queue_high_watermark_bytes=16 * 1024,
                queue_hard_watermark_bytes=1024 * 1024,
                admission_timeout_ms=0,
            ),
        )
        self.backend = slow_backend
        self.store = slow
        workflow = Workflow(id="timeout_zero")
        workflow.add_node(dynamic_json_callable(lambda: "x" * (32 * 1024)), node_id="node")
        app = AutoAgentApp(runtime_store=slow)
        await app.astart()
        first = await app.ainvoke(workflow, session_id="first")

        self.assertEqual("completed", first.state)
        self.assertTrue(slow.admission_paused)
        with self.assertRaisesRegex(RuntimeError, "backlog"):
            await app.ainvoke(workflow, session_id="second")

        release.set()
        await slow.aflush()
        await app.aclose()

    async def test_admission_timeout_succeeds_when_queue_drains(self) -> None:
        """The caller waits for admission to resume within the timeout.

        The queue is allowed to drain after a brief block, so the waiting
        accept path should succeed instead of raising TimeoutError.
        """

        release = threading.Event()

        class SlowBackend(DatabaseBackend):
            async def _persist_batch(self, batch) -> None:
                if any(item.kind == "event" for item in batch):
                    while not release.is_set():
                        await asyncio.sleep(0.001)
                await super()._persist_batch(batch)

        await self.store.aclose()
        slow_backend = SlowBackend.from_path(self.path)
        slow = RuntimeStore(
            backend=slow_backend,
            persistence_policy=PersistencePolicy(
                queue_low_watermark_bytes=8 * 1024,
                queue_high_watermark_bytes=16 * 1024,
                queue_hard_watermark_bytes=1024 * 1024,
                admission_timeout_ms=5_000,
            ),
        )
        self.backend = slow_backend
        self.store = slow
        workflow = Workflow(id="timeout_drain")
        workflow.add_node(dynamic_json_callable(lambda: "x" * (32 * 1024)), node_id="node")
        app = AutoAgentApp(runtime_store=slow)
        await app.astart()

        first = await app.ainvoke(workflow, session_id="first")
        self.assertEqual("completed", first.state)
        self.assertTrue(slow.admission_paused)

        # Release the writer in the background so the queue drains.
        release.set()

        second = await app.ainvoke(workflow, session_id="second")
        self.assertEqual("completed", second.state)
        await slow.aflush()
        self.assertFalse(slow.admission_paused)
        await app.aclose()

    async def test_admission_timeout_raises_when_queue_stalls(self) -> None:
        """Admission blocks until its configured persistence deadline."""

        release = threading.Event()

        class BlockedBackend(DatabaseBackend):
            async def _persist_batch(self, batch) -> None:
                if any(item.kind == "event" for item in batch):
                    while not release.is_set():
                        await asyncio.sleep(0.001)
                await super()._persist_batch(batch)

        await self.store.aclose()
        blocked = BlockedBackend.from_path(self.path)
        slow = RuntimeStore(
            backend=blocked,
            persistence_policy=PersistencePolicy(
                queue_low_watermark_bytes=8 * 1024,
                queue_high_watermark_bytes=16 * 1024,
                queue_hard_watermark_bytes=1024 * 1024,
                admission_timeout_ms=200,
            ),
        )
        self.backend = blocked
        self.store = slow
        workflow = Workflow(id="admission_timeout")
        workflow.add_node(dynamic_json_callable(lambda: "x" * (32 * 1024)), node_id="node")
        app = AutoAgentApp(runtime_store=slow)
        await app.astart()

        first = await app.ainvoke(workflow, session_id="first")
        self.assertEqual("completed", first.state)
        self.assertTrue(slow.admission_paused)

        with self.assertRaisesRegex(
            RuntimeError,
            "Cannot submit.*persistence backlog",
        ):
            await app.ainvoke(workflow, session_id="stalled")

        release.set()
        await slow.aflush()
        await app.aclose()

    async def test_default_admission_timeout_is_nonzero(self) -> None:
        """The default timeout allows waiting, not immediate rejection."""

        assert self.store.persistence is not None
        self.assertEqual(
            5_000,
            self.store.persistence.policy.admission_timeout_ms,
        )

    async def test_waiting_session_rejects_new_invoke_with_database(self) -> None:
        """Persistent session with a waiting invocation rejects new invoke."""

        workflow = Workflow(id="persistent_waiting_guard")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()

        waiting = await app.ainvoke(
            workflow,
            input={"wait_key": "approval"},
            session_id="shared-session",
        )
        self.assertEqual("waiting", waiting.state)

        with self.assertRaises(SessionBusyError):
            await app.ainvoke(
                workflow,
                session_id="shared-session",
            )

        # After resume, new invoke on same session should succeed.
        resumed = await app.aresume(
            workflow,
            session_id="shared-session",
            wait_key="approval",
            output={"approved": True},
        )
        self.assertEqual("completed", resumed.state)

        second = await app.ainvoke(
            workflow,
            input={"wait_key": "second"},
            session_id="shared-session",
        )
        self.assertEqual("waiting", second.state)
        self.assertNotEqual(second.id, waiting.id)
        await app.aclose()

    async def test_session_busy_wins_before_persistence_backpressure(self) -> None:
        await self.store.aclose()
        self.backend = DatabaseBackend.from_path(self.path)
        self.store = RuntimeStore(
            backend=self.backend,
            persistence_policy=PersistencePolicy(
                queue_low_watermark_bytes=10 * 1024,
                queue_high_watermark_bytes=20 * 1024,
                queue_hard_watermark_bytes=100 * 1024,
                admission_timeout_ms=5_000,
            ),
        )
        workflow = Workflow(id="busy_before_backpressure")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=self.store)
        await app.astart()
        waiting = await app.ainvoke(
            workflow,
            input={"wait_key": "approval"},
            session_id="shared",
        )
        assert self.store.persistence is not None
        envelope = PersistenceEnvelope(
            kind="event",
            session_id=self.store.invocation_sessions[waiting.id],
            session_updated_at_ms=0,
            invocation_id=waiting.id,
            invocation_state="waiting",
            execution_mode="normal",
            invocation_updated_at_ms=0,
            event=RuntimeEvent(
                invocation_id=waiting.id,
                sequence=waiting.event_sequence + 1,
                event_type="state_change",
                event_name="invocation.running",
                subject_type="invocation",
                subject_id=str(waiting.id),
                occurred_at_ms=0,
            ),
            estimated_bytes=30 * 1024,
            force_recovery_checkpoint=False,
        )
        reservation = self.store.persistence.try_reserve(envelope)
        assert reservation is not None
        self.assertTrue(self.store.admission_paused)

        try:
            with self.assertRaises(SessionBusyError):
                await asyncio.wait_for(
                    app.ainvoke(workflow, session_id="shared"),
                    timeout=0.1,
                )
        finally:
            self.store.persistence.cancel(reservation)
        await app.aclose()

    async def test_runtime_events_are_coalesced_into_database_batches(self) -> None:
        class RecordingBackend(DatabaseBackend):
            event_batch_sizes: list[int]

            def __init__(self, *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self.event_batch_sizes = []

            async def _persist_batch(self, batch) -> None:
                event_count = sum(item.kind == "event" for item in batch)
                if event_count:
                    self.event_batch_sizes.append(event_count)
                await super()._persist_batch(batch)

        await self.store.aclose()
        recording_backend = RecordingBackend.from_path(
            self.path,
            batch_max_delay_ms=10,
        )
        recording = RuntimeStore(backend=recording_backend)
        self.backend = recording_backend
        self.store = recording
        workflow = Workflow(id="batch_events")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = AutoAgentApp(runtime_store=recording)
        await app.astart()

        await app.ainvoke(workflow)
        await recording.aflush()

        self.assertGreater(max(recording_backend.event_batch_sizes), 1)
        await app.aclose()


class RuntimeEventTests(unittest.TestCase):
    def _started_app(self, **kwargs: Any) -> AutoAgentApp:
        app = started_app(**kwargs)
        self.addCleanup(app.close)
        return app

    def test_invalid_event_mode_is_rejected_before_admission(self) -> None:
        workflow = Workflow(id="invalid_event_mode")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="node")
        app = self._started_app()

        with self.assertRaisesRegex(ValueError, "Invalid event_mode"):
            app.invoke(workflow, event_mode="verbose")  # type: ignore[arg-type]
        self.assertEqual({}, app.runtime_store.invocations)

    def test_standard_records_graph_events_without_operations(self) -> None:
        store = RuntimeStore()
        app = self._started_app(runtime_store=store)
        workflow = Workflow(id="standard_events")
        workflow.add_node(dynamic_json_callable(lambda value: value), node_id="node")
        invocation = app.invoke(
            workflow,
            input={"value": 1},
            event_mode="standard",
        )
        events = asyncio.run(
            store.alist_runtime_events(invocation_id=invocation.id, limit=10_000)
        )
        self.assertEqual(
            [
                "invocation.running",
                "node.running",
                "operator_call.completed",
                "node.completed",
                "invocation.completed",
            ],
            [event.event_name for event in events],
        )
        self.assertTrue(all(event.operations is None for event in events))
        self.assertTrue(all(event.occurred_at_ms > 0 for event in events))

    def test_standard_records_scheduler_skips_as_node_state_events(self) -> None:
        store = RuntimeStore()
        app = self._started_app(runtime_store=store)
        workflow = Workflow(id="standard_skip_events")
        workflow.add_node(dynamic_json_callable(lambda: "source"), node_id="source")
        workflow.add_node(
            dynamic_json_callable(lambda: "target"),
            node_id="target",
            input_mapping=lambda _ctx: {},
        )
        workflow.add_edge(
            "source",
            "target",
            condition=lambda _ctx: False,
        )
        invocation = app.invoke(workflow, event_mode="standard")
        events = asyncio.run(
            store.alist_runtime_events(invocation_id=invocation.id, limit=10_000)
        )

        skipped = [
            event for event in events if event.event_name == "node.skipped"
        ]
        self.assertEqual(1, len(skipped))
        self.assertEqual("target", skipped[0].subject_id)
        self.assertEqual("skipped", skipped[0].status)

    def test_full_records_internal_phases_and_rebuilds_each_event(self) -> None:
        store = RuntimeStore()
        app = self._started_app(runtime_store=store)
        workflow = Workflow(id="full_events")
        workflow.add_node(dynamic_json_callable(lambda value: value.upper()), node_id="node")
        invocation = app.invoke(
            workflow,
            input={"value": "hello"},
            event_mode="full",
        )
        events = asyncio.run(
            store.alist_runtime_events(invocation_id=invocation.id, limit=10_000)
        )
        cursors = {
            event.event_name: event.sequence
            for event in events
        }

        _, mapped = asyncio.run(
            store.arebuild_execution(
                invocation.id,
                through_sequence=cursors["input_mapping.completed"],
            )
        )
        _, called = asyncio.run(
            store.arebuild_execution(
                invocation.id,
                through_sequence=cursors["operator_call.completed"],
            )
        )

        self.assertEqual("running", mapped.node_executions[0].state)
        self.assertEqual({"value": "hello"}, mapped.node_executions[0].input)
        self.assertEqual(0, mapped.node_executions[0].operator_summary.attempt_count)
        self.assertEqual(1, called.node_executions[0].operator_summary.attempt_count)
        self.assertTrue(all(event.operations is not None for event in events))

    def test_full_freezes_each_parallel_edge_decision_independently(self) -> None:
        store = RuntimeStore()
        app = self._started_app(runtime_store=store)
        workflow = Workflow(id="atomic_edge_events")
        workflow.add_node(dynamic_json_callable(lambda: "source"), node_id="source")
        workflow.add_node(
            dynamic_json_callable(lambda: "left"),
            node_id="left",
            input_mapping=lambda _ctx: {},
        )
        workflow.add_node(
            dynamic_json_callable(lambda: "right"),
            node_id="right",
            input_mapping=lambda _ctx: {},
        )
        workflow.add_edge("source", "left", edge_id="left-edge")
        workflow.add_edge("source", "right", edge_id="right-edge")
        invocation = app.invoke(workflow, event_mode="full")
        events = asyncio.run(
            store.alist_runtime_events(invocation_id=invocation.id, limit=10_000)
        )
        edge_events = [
            event for event in events if event.event_name == "edge.evaluated"
        ]

        _, after_first = asyncio.run(
            store.arebuild_execution(
                invocation.id,
                through_sequence=edge_events[0].sequence,
            )
        )
        _, after_second = asyncio.run(
            store.arebuild_execution(
                invocation.id,
                through_sequence=edge_events[1].sequence,
            )
        )

        source_first = after_first.latest_node_execution("source")
        source_second = after_second.latest_node_execution("source")
        self.assertEqual(["left-edge"], [
            item.edge_id for item in source_first.edge_evaluations
        ])
        self.assertEqual(["left-edge", "right-edge"], [
            item.edge_id for item in source_second.edge_evaluations
        ])
        self.assertEqual(
            {"left-edge", "right-edge"},
            {event.subject_id for event in edge_events},
        )

    def test_event_timing_uses_wall_clock_occurrence_and_monotonic_duration(
        self,
    ) -> None:
        store = RuntimeStore()
        app = self._started_app(runtime_store=store)
        workflow = Workflow(id="event_timing")
        workflow.add_node(dynamic_json_callable(lambda: "source"), node_id="source")
        workflow.add_node(
            dynamic_json_callable(lambda: "target"),
            node_id="target",
            input_mapping=lambda _ctx: {},
        )
        workflow.add_edge(
            "source",
            "target",
            condition=lambda _ctx: True,
        )
        invocation = app.invoke(workflow, event_mode="full")
        events = asyncio.run(
            store.alist_runtime_events(invocation_id=invocation.id, limit=10_000)
        )

        self.assertTrue(all(event.occurred_at_ms > 0 for event in events))
        timed = [
            event
            for event in events
            if event.event_name in {
                "input_mapping.completed",
                "operator_call.completed",
                "node.completed",
                "edge.evaluated",
            }
        ]
        self.assertTrue(timed)
        self.assertTrue(all(event.elapsed_ns is not None for event in timed))
        self.assertTrue(all(event.elapsed_ns >= 0 for event in timed))

    def test_map_units_emit_actual_calls_without_growing_runtime_state(self) -> None:
        store = RuntimeStore()
        app = self._started_app(runtime_store=store)
        workflow = Workflow(id="bounded_map_history")
        workflow.add_node(dynamic_json_callable(lambda: list(range(100))), node_id="source")
        workflow.add_node(
            dynamic_json_callable(lambda value: value * 2),
            node_id="target",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": value} for value in ctx.input
                    ],
                    max_parallelism=20,
                )
            ),
        )
        workflow.add_edge(
            "source",
            "target",
        )

        invocation = app.invoke(workflow, event_mode="full")
        events = asyncio.run(
            store.alist_runtime_events(
                invocation_id=invocation.id,
                limit=10_000,
            )
        )
        target = invocation.latest_node_execution("target")
        operator_events = [
            event
            for event in events
            if event.event_name == "operator_call.completed"
            and event.payload.get("node_id") == "target"
        ]
        self.assertEqual(100, len(operator_events))
        self.assertEqual(100, target.operator_summary.attempt_count)
        logical = target.parallel_summary
        assert logical is not None
        self.assertEqual("map", logical.kind)
        self.assertEqual(100, logical.call_count)
        self.assertEqual(100, logical.attempt_count)
        operations = [
            operation
            for event in operator_events
            for operation in (event.operations or ())
        ]
        self.assertFalse(
            any("operator_calls" in operation.path for operation in operations)
        )
        self.assertTrue(all(event.input is not None for event in operator_events))
        self.assertTrue(all(event.output is not None for event in operator_events))
        selection = next(
            event
            for event in events
            if event.event_name == "item_selection.completed"
            and event.payload.get("node_id") == "target"
        )
        self.assertIsNone(selection.input)
        self.assertIsNone(selection.output)
        aggregation = next(
            event
            for event in events
            if event.event_name == "aggregation.completed"
            and event.payload.get("node_id") == "target"
        )
        self.assertIsNone(aggregation.input)
        self.assertEqual(list(range(0, 200, 2)), aggregation.output)
        projection = TraceProjectionReducer.initial(invocation.id)
        for event in events:
            projection = TraceProjectionReducer.apply(projection, event)
        projected = projection["node_executions"][str(target.id)]
        self.assertEqual(100, projected["operator_call_count"])
        self.assertEqual(50, len(projected["operator_calls"]))


class RecoveryExecutionModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_idempotent_policy_requires_explicit_operator_key_contract(self) -> None:
        invalid = Workflow(id="invalid_idempotency")
        invalid.add_node(
            dynamic_json_callable(lambda value: value),
            node_id="node",
            policy=NodePolicy(recovery=RecoveryPolicy(mode="idempotent")),
        )
        result = isolated_app().compiler.compile(invalid)
        self.assertFalse(result.ok)
        self.assertIn(
            "POLICY_RECOVERY_IDEMPOTENCY_KEY_REQUIRED",
            {diagnostic.code for diagnostic in result.diagnostics},
        )

    async def test_recovery_follows_selected_path_and_stops_at_first_forbidden_node(
        self,
    ) -> None:
        calls: list[str] = []

        def safe() -> str:
            calls.append("safe")
            return "safe"

        def forbidden(value: str) -> str:
            calls.append("forbidden")
            return value

        workflow = Workflow(id="recovery_gate")
        workflow.add_node(
            safe,
            node_id="safe",
            policy=NodePolicy(
                recovery=RecoveryPolicy(mode="replay_safe", max_attempts=1)
            ),
        )
        workflow.add_node(forbidden, node_id="forbidden")
        workflow.add_edge("safe", "forbidden")
        store = RuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        entry = app.register_workflow(workflow)
        revision_id = workflow_revision_id(
            entry.workflow_snapshot.workflow_id,
            entry.workflow_snapshot.definition_hash,
        )
        session = await store.aget_or_create_session(
            workflow_id=workflow.id,
            workflow_revision_id=revision_id,
            session_key="recovery",
        )
        invocation = Invocation(
            workflow_id=workflow.id,
            workflow_revision_id=revision_id,
            workflow_version=entry.workflow_ir.workflow_version,
            workflow_definition_hash=entry.workflow_ir.definition_hash,
            entry_node_id="safe",
            event_mode="full",
        )
        session = await store.aadmit_invocation(session.id, invocation)
        # Persist a realistic crash point: the safe node has started, but its
        # worker result has not committed. Recovery must replay this whole node.
        previous = capture_execution_state(session, invocation)
        invocation.scheduler.drain_ready()
        invocation.mark_running()
        execution = invocation.create_node_execution("safe")
        invocation.mark_node_running(execution.id)
        sequence = invocation.next_event_sequence()
        operations = build_state_operations(
            previous,
            session,
            invocation,
            node_execution_ids=(execution.id,),
        )
        await store.arecord_event(
            session,
            invocation,
            RuntimeEvent(
                invocation_id=invocation.id,
                sequence=sequence,
                event_type="state_change",
                event_name="node.running",
                subject_type="node",
                subject_id=str(execution.id),
                occurred_at_ms=utc_timestamp_ms(),
                operations=operations,
            ),
        )

        recovered = await app.workflow_executor.arecover(
            workflow_ir=entry.workflow_ir,
            workflow_snapshot=entry.workflow_snapshot,
            session=session,
            invocation=invocation,
        )
        events = await store.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )

        self.assertEqual(["safe"], calls)
        self.assertEqual("recovery", recovered.execution_mode)
        self.assertEqual("interrupted", recovered.state)
        self.assertTrue(
            any(
                event.event_name == "node.interrupted"
                and event.subject_id == str(execution.id)
                and event.payload["error"]["code"] == "WORKER_LOST"
                for event in events
            )
        )
        self.assertEqual(
            "recovery.interrupted",
            events[-1].event_name,
        )

    async def test_recovery_policy_can_finish_other_active_branches(self) -> None:
        calls: list[str] = []

        def allowed() -> str:
            calls.append("allowed")
            return "done"

        def must_skip(_value: str) -> str:
            calls.append("must_skip")
            return "unexpected"

        workflow = Workflow(
            id="recovery_continue",
            policy=WorkflowPolicy(
                failure=FailurePolicy(mode="continue_active_branches")
            ),
        )
        workflow.add_node(dynamic_json_callable(lambda: "blocked"), node_id="blocked")
        workflow.add_node(must_skip, node_id="must_skip")
        workflow.add_node(
            allowed,
            node_id="allowed",
            policy=NodePolicy(
                recovery=RecoveryPolicy(mode="replay_safe", max_attempts=1)
            ),
        )
        workflow.add_edge("blocked", "must_skip")
        store = RuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        entry = app.register_workflow(workflow)
        revision_id = workflow_revision_id(
            entry.workflow_snapshot.workflow_id,
            entry.workflow_snapshot.definition_hash,
        )
        session = await store.aget_or_create_session(
            workflow_id=workflow.id,
            workflow_revision_id=revision_id,
            session_key="recovery",
        )
        invocation = Invocation(
            workflow_id=workflow.id,
            workflow_revision_id=revision_id,
            workflow_version=entry.workflow_ir.workflow_version,
            workflow_definition_hash=entry.workflow_ir.definition_hash,
            entry_node_id="blocked",
            event_mode="full",
        )
        session = await store.aadmit_invocation(session.id, invocation)
        previous = capture_execution_state(session, invocation)
        invocation.scheduler.drain_ready()
        invocation.scheduler.enqueue_ready("blocked")
        invocation.scheduler.enqueue_ready("allowed")
        invocation.mark_running()
        sequence = invocation.next_event_sequence()
        operations = build_state_operations(
            previous,
            session,
            invocation,
        )
        await store.arecord_event(
            session,
            invocation,
            RuntimeEvent(
                invocation_id=invocation.id,
                sequence=sequence,
                event_type="routing",
                event_name="routing.evaluated",
                subject_type="invocation",
                subject_id=str(invocation.id),
                occurred_at_ms=utc_timestamp_ms(),
                operations=operations,
            ),
        )

        recovered = await app.workflow_executor.arecover(
            workflow_ir=entry.workflow_ir,
            workflow_snapshot=entry.workflow_snapshot,
            session=session,
            invocation=invocation,
        )

        self.assertEqual(["allowed"], calls)
        self.assertEqual("interrupted", recovered.state)
        self.assertEqual(
            "completed",
            recovered.latest_node_execution("allowed").state,
        )
        self.assertIsNone(recovered.latest_node_execution("blocked"))
        self.assertIsNone(recovered.latest_node_execution("must_skip"))


@unittest.skipUnless(
    os.environ.get("AUTOAGENT_TEST_POSTGRES_URL"),
    "AUTOAGENT_TEST_POSTGRES_URL is not configured",
)
class PostgreSQLDatabaseBackendIntegrationTests(
    unittest.IsolatedAsyncioTestCase
):
    async def test_postgresql_round_trip(self) -> None:
        database_url = os.environ["AUTOAGENT_TEST_POSTGRES_URL"]
        workflow = Workflow(id=f"postgres_{os.getpid()}_{id(self)}")
        workflow.add_node(dynamic_json_callable(lambda value: value + 1), node_id="node")
        backend = DatabaseBackend(database_url)
        store = RuntimeStore(backend=backend)
        app = AutoAgentApp(runtime_store=store)
        await app.astart()
        invocation = await app.ainvoke(
            workflow,
            input={"value": 1},
            session_id=f"session_{id(self)}",
            event_mode="full",
        )
        await store.aflush()
        await app.aclose()

        reopened_backend = DatabaseBackend(database_url)
        reopened = RuntimeStore(backend=reopened_backend)
        try:
            _, rebuilt = await reopened.arebuild_execution(invocation.id)
            self.assertEqual(invocation.result, rebuilt.result)
        finally:
            await reopened.aclose()
