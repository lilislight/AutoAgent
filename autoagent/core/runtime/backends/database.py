from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, tuple_
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from autoagent.core.compiler import WorkflowVersionSnapshot
from autoagent.core.runtime.backends.models import (
    InvocationRow,
    RuntimeDatabaseBase,
    RuntimeEventRow,
    RuntimeSnapshotRow,
    SessionRow,
    WorkflowVersionRow,
)
from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.runtime.hooks import RuntimeEventLoop
from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.snapshot import ExecutionSnapshot
from autoagent.core.runtime.store import RuntimeStore
from autoagent.core.runtime.time import utc_timestamp_ms


@dataclass
class _PersistenceItem:
    kind: str
    session_id: UUID | None
    invocation_id: UUID | None
    value: Any
    encoded: bytes | None
    size_bytes: int
    done: asyncio.Future[None] | None = None


class DatabaseBackend:
    """SQLite/PostgreSQL durability for one authoritative ``RuntimeStore``."""

    def __init__(
        self,
        database_url: str | Path,
        *,
        echo: bool = False,
        queue_high_watermark_bytes: int = 64 * 1024 * 1024,
        queue_low_watermark_bytes: int | None = None,
        queue_hard_watermark_bytes: int | None = None,
        batch_max_items: int = 256,
        batch_max_bytes: int = 4 * 1024 * 1024,
        batch_max_delay_ms: int = 5,
        snapshot_interval: int = 50,
    ) -> None:
        if queue_high_watermark_bytes < 1:
            raise ValueError("queue_high_watermark_bytes must be positive.")
        low = (
            queue_high_watermark_bytes // 2
            if queue_low_watermark_bytes is None
            else queue_low_watermark_bytes
        )
        hard = (
            queue_high_watermark_bytes * 2
            if queue_hard_watermark_bytes is None
            else queue_hard_watermark_bytes
        )
        if not 0 <= low < queue_high_watermark_bytes < hard:
            raise ValueError("Expected low < high < hard byte watermarks.")
        if batch_max_items < 1 or batch_max_bytes < 1 or batch_max_delay_ms < 0:
            raise ValueError("Invalid persistence batch limits.")
        if snapshot_interval < 1:
            raise ValueError("snapshot_interval must be positive.")

        self.database_url = _resolve_database_url(database_url)
        self.engine: AsyncEngine = create_async_engine(
            self.database_url,
            echo=echo,
        )
        self._database_sessions = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        self.queue_high_watermark_bytes = queue_high_watermark_bytes
        self.queue_low_watermark_bytes = low
        self.queue_hard_watermark_bytes = hard
        self.batch_max_items = batch_max_items
        self.batch_max_bytes = batch_max_bytes
        self.batch_max_delay_ms = batch_max_delay_ms
        self.snapshot_interval = snapshot_interval

        self._database_loop = RuntimeEventLoop(
            name="autoagent-persistence-runtime"
        )
        self._queues: dict[str, deque[_PersistenceItem]] = {}
        self._ready_sessions: deque[str] = deque()
        self._ready_set: set[str] = set()
        self._queue_event: asyncio.Event | None = None
        self._initialize_lock: asyncio.Lock | None = None
        self._worker: asyncio.Task[None] | None = None
        self._initialized = False
        self._closing = False
        self._pressure_lock = RLock()
        self._pending_bytes = 0
        self._inflight_bytes = 0
        self._pending_count = 0
        self._inflight_count = 0
        self._admission_pressure = False
        self._persistence_error: BaseException | None = None
        self._fatal_persistence_error: BaseException | None = None
        self._workflow_version_ids: dict[tuple[str, str, str, str], UUID] = {}
        self._durable_sequences: dict[UUID, int] = {}
        self._store: RuntimeStore | None = None

    @classmethod
    def from_path(cls, path: str | Path, **kwargs: Any) -> DatabaseBackend:
        return cls(Path(path), **kwargs)

    def bind(self, store: RuntimeStore) -> None:
        if self._store is not None and self._store is not store:
            raise RuntimeError("A DatabaseBackend can belong to only one RuntimeStore.")
        self._store = store

    @property
    def store(self) -> RuntimeStore:
        if self._store is None:
            raise RuntimeError("DatabaseBackend is not attached to a RuntimeStore.")
        return self._store

    @property
    def serializer(self):
        return self.store.serializer

    @property
    def pending_persistence_bytes(self) -> int:
        with self._pressure_lock:
            return self._pending_bytes + self._inflight_bytes

    @property
    def pending_persistence_count(self) -> int:
        with self._pressure_lock:
            return self._pending_count + self._inflight_count

    @property
    def admission_paused(self) -> bool:
        pending = self.pending_persistence_bytes
        with self._pressure_lock:
            if pending >= self.queue_high_watermark_bytes:
                self._admission_pressure = True
            elif (
                self._admission_pressure
                and pending <= self.queue_low_watermark_bytes
            ):
                self._admission_pressure = False
            pressure = self._admission_pressure
        return (
            pressure
            or self._fatal_persistence_error is not None
        )

    async def ainitialize(self) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(self.ainitialize())
            return
        if self._initialized:
            self._ensure_worker()
            return
        if self._initialize_lock is None:
            self._initialize_lock = asyncio.Lock()
        async with self._initialize_lock:
            if not self._initialized:
                self._queue_event = asyncio.Event()
                async with self.engine.begin() as connection:
                    await connection.run_sync(
                        RuntimeDatabaseBase.metadata.create_all
                    )
                self._initialized = True
        self._ensure_worker()

    async def aclose(self) -> None:
        if not self._database_loop.is_current():
            if not self._initialized:
                await self.engine.dispose()
                self._database_loop.stop()
                return
            try:
                await self._database_loop.arun(self.aclose())
            finally:
                self._database_loop.stop()
            return
        if self._closing:
            return
        self._closing = True
        try:
            await self.aflush()
        finally:
            if self._worker is not None:
                assert self._queue_event is not None
                self._queue_event.set()
                await self._worker
                self._worker = None
            await self.engine.dispose()
            self._initialized = False

    async def aflush(self) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(self.aflush())
            return
        await self.ainitialize()
        while self.pending_persistence_count:
            await asyncio.sleep(0.001)
        if self._fatal_persistence_error is not None:
            raise RuntimeError("Runtime persistence failed permanently.") from (
                self._fatal_persistence_error
            )

    async def await_capacity(self) -> None:
        if self._fatal_persistence_error is not None:
            raise RuntimeError("Runtime persistence is unavailable.") from (
                self._fatal_persistence_error
            )
        while self.pending_persistence_bytes >= self.queue_hard_watermark_bytes:
            if self._fatal_persistence_error is not None:
                raise RuntimeError("Runtime persistence is unavailable.") from (
                    self._fatal_persistence_error
                )
            await asyncio.sleep(0.005)

    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        key = (
            namespace,
            snapshot.workflow_id,
            snapshot.definition_hash,
            snapshot.operator_manifest_hash,
        )
        if key in self._workflow_version_ids:
            return
        version_id = uuid4()
        self._workflow_version_ids[key] = version_id
        record = {
            "id": version_id,
            "namespace": namespace,
            "workflow_id": snapshot.workflow_id,
            "workflow_version": (
                None
                if snapshot.workflow_version is None
                else str(snapshot.workflow_version)
            ),
            "ir_version": snapshot.ir_version,
            "compiler_version": snapshot.compiler_version,
            "definition_hash": snapshot.definition_hash,
            "operator_manifest_hash": snapshot.operator_manifest_hash,
            "created_at_ms": utc_timestamp_ms(),
        }
        await self._enqueue(
            _PersistenceItem(
                kind="workflow_version",
                session_id=None,
                invocation_id=None,
                value=record,
                encoded=None,
                size_bytes=512,
            ),
            barrier=True,
        )

    async def afind_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.afind_session(
                    namespace=namespace,
                    workflow_id=workflow_id,
                    session_key=session_key,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            row = await database.scalar(
                select(SessionRow).where(
                    SessionRow.namespace == namespace,
                    SessionRow.workflow_id == workflow_id,
                    SessionRow.session_key == session_key,
                )
            )
        if row is None:
            return None
        if row.current_invocation_id is not None:
            session, _ = await self.store.arebuild_execution(
                UUID(row.current_invocation_id)
            )
            return session
        session = Session(
            id=UUID(row.id),
            namespace=row.namespace,
            workflow_id=row.workflow_id,
            session_key=row.session_key,
            created_at_ms=row.created_at_ms,
            updated_at_ms=row.updated_at_ms,
        )
        return session

    async def aadmit_invocation(
        self,
        session: Session,
        invocation: Invocation,
        snapshot: ExecutionSnapshot,
    ) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(
                self.aadmit_invocation(session, invocation, snapshot)
            )
            return
        if self.admission_paused:
            raise RuntimeError(
                "Runtime persistence backlog is above the admission watermark."
            )
        key = (
            session.namespace,
            session.workflow_id,
            invocation.workflow_definition_hash or "",
            invocation.workflow_operator_manifest_hash or "",
        )
        version_id = self._workflow_version_ids.get(key)
        if version_id is None:
            version_id = await self._load_workflow_version_id(key)
        if version_id is None:
            raise RuntimeError("Workflow version metadata is not durable.")
        encoded_snapshot = self.serializer.dumps(snapshot.state)
        admission = {
            "session": deepcopy(snapshot.state["session"]),
            "invocation": deepcopy(snapshot.state["invocation"]),
            "workflow_version_id": version_id,
            "snapshot": snapshot,
        }
        await self._enqueue(
            _PersistenceItem(
                kind="admission",
                session_id=session.id,
                invocation_id=invocation.id,
                value=admission,
                encoded=encoded_snapshot,
                size_bytes=len(encoded_snapshot) + 1024,
            ),
            barrier=True,
        )
    async def aappend_event(
        self,
        session: Session,
        invocation: Invocation,
        event: RuntimeEvent,
        *,
        durability_barrier: bool = False,
    ) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(
                self.aappend_event(
                    session,
                    invocation,
                    event,
                    durability_barrier=durability_barrier,
                )
            )
            return
        encoded_payload = self.serializer.dumps(event.payload)
        await self._enqueue(
            _PersistenceItem(
                kind="event",
                session_id=session.id,
                invocation_id=invocation.id,
                value={
                    "event": event,
                    "invocation_state": invocation.state,
                    "execution_mode": invocation.execution_mode,
                    "updated_at_ms": invocation.updated_at_ms,
                    "session_updated_at_ms": session.updated_at_ms,
                },
                encoded=encoded_payload,
                size_bytes=len(encoded_payload) + 256,
            ),
            barrier=durability_barrier,
        )

    async def asave_execution_snapshot(
        self,
        snapshot: ExecutionSnapshot,
        *,
        session_id: UUID | None,
        durability_barrier: bool = False,
    ) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(
                self.asave_execution_snapshot(
                    snapshot,
                    session_id=session_id,
                    durability_barrier=durability_barrier,
                )
            )
            return
        encoded = self.serializer.dumps(snapshot.state)
        await self._enqueue(
            _PersistenceItem(
                kind="snapshot",
                session_id=session_id,
                invocation_id=snapshot.invocation_id,
                value=snapshot,
                encoded=encoded,
                size_bytes=len(encoded) + 128,
            ),
            barrier=durability_barrier,
        )

    async def aload_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None = None,
    ) -> ExecutionSnapshot | None:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.aload_execution_snapshot(
                    invocation_id,
                    at_or_before_sequence=at_or_before_sequence,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = select(RuntimeSnapshotRow).where(
                RuntimeSnapshotRow.invocation_id == str(invocation_id)
            )
            if at_or_before_sequence is not None:
                statement = statement.where(
                    RuntimeSnapshotRow.through_sequence <= at_or_before_sequence
                )
            row = await database.scalar(
                statement.order_by(
                    RuntimeSnapshotRow.through_sequence.desc()
                ).limit(1)
            )
        if row is None:
            return None
        return ExecutionSnapshot(
            id=UUID(row.id),
            invocation_id=invocation_id,
            through_sequence=row.through_sequence,
            state=self.serializer.loads(row.state_json),
            created_at_ms=row.created_at_ms,
        )

    async def alist_runtime_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
    ) -> tuple[RuntimeEvent, ...]:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_runtime_events(
                    invocation_id=invocation_id,
                    after_sequence=after_sequence,
                    before_sequence=before_sequence,
                    limit=limit,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = select(RuntimeEventRow).where(
                RuntimeEventRow.invocation_id == str(invocation_id),
                RuntimeEventRow.sequence > after_sequence,
            )
            if before_sequence is not None:
                statement = statement.where(
                    RuntimeEventRow.sequence < before_sequence
                ).order_by(RuntimeEventRow.sequence.desc())
            else:
                statement = statement.order_by(RuntimeEventRow.sequence)
            rows = (
                await database.scalars(statement.limit(limit))
            ).all()
        if before_sequence is not None:
            rows.reverse()
        events = tuple(
            RuntimeEvent(
                id=UUID(row.id),
                invocation_id=invocation_id,
                sequence=row.sequence,
                schema_version=row.schema_version,
                type=row.type,
                occurred_at_ms=row.occurred_at_ms,
                payload=self.serializer.loads(row.payload_json),
            )
            for row in rows
        )
        if events:
            self._durable_sequences[invocation_id] = max(
                self._durable_sequences.get(invocation_id, 0),
                events[-1].sequence,
            )
        return events

    def persistence_status(self, invocation_id: UUID) -> str:
        if self._fatal_persistence_error is not None:
            return "error"
        invocation = self.store.invocations.get(invocation_id)
        if invocation is None:
            raise KeyError(f"Unknown Invocation: {invocation_id}")
        return (
            "durable"
            if self.durable_sequence(invocation_id) >= invocation.event_sequence
            else "pending"
        )

    def durable_sequence(self, invocation_id: UUID) -> int:
        return self._durable_sequences.get(invocation_id, 0)

    async def _enqueue(
        self,
        item: _PersistenceItem,
        *,
        barrier: bool = False,
    ) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(
                self._enqueue(item, barrier=barrier)
            )
            return
        await self.ainitialize()
        if self._fatal_persistence_error is not None:
            raise RuntimeError("Runtime persistence failed permanently.") from (
                self._fatal_persistence_error
            )
        if barrier:
            item.done = asyncio.get_running_loop().create_future()
        key = str(item.session_id) if item.session_id is not None else "__control__"
        queue = self._queues.setdefault(key, deque())
        queue.append(item)
        if key not in self._ready_set:
            self._ready_set.add(key)
            self._ready_sessions.append(key)
        with self._pressure_lock:
            self._pending_bytes += item.size_bytes
            self._pending_count += 1
        assert self._queue_event is not None
        self._queue_event.set()
        if item.done is not None:
            await item.done

    def _ensure_worker(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(
                self._persistence_loop(),
                name="autoagent-persistence-writer",
            )

    async def _persistence_loop(self) -> None:
        assert self._queue_event is not None
        while True:
            if not self._ready_sessions:
                if self._closing:
                    return
                self._queue_event.clear()
                await self._queue_event.wait()
                continue
            batch = await self._take_batch()
            if not batch:
                continue
            batch_bytes = sum(item.size_bytes for item in batch)
            with self._pressure_lock:
                self._pending_bytes -= batch_bytes
                self._pending_count -= len(batch)
                self._inflight_bytes += batch_bytes
                self._inflight_count += len(batch)
            retry_delay = 0.05
            while True:
                try:
                    await self._persist_batch(batch)
                except asyncio.CancelledError:
                    raise
                except (OperationalError, DBAPIError) as exc:
                    if not _is_retryable_database_error(exc):
                        self._fail_batch_permanently(batch, exc)
                        break
                    self._persistence_error = exc
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(5.0, retry_delay * 2)
                    continue
                except BaseException as exc:
                    self._fail_batch_permanently(batch, exc)
                    break
                else:
                    self._persistence_error = None
                    for item in batch:
                        if (
                            item.invocation_id is not None
                            and item.kind == "event"
                        ):
                            event: RuntimeEvent = item.value["event"]
                            self._durable_sequences[item.invocation_id] = max(
                                self._durable_sequences.get(item.invocation_id, 0),
                                event.sequence,
                            )
                        if item.done is not None and not item.done.done():
                            item.done.set_result(None)
                    break
            with self._pressure_lock:
                self._inflight_bytes -= batch_bytes
                self._inflight_count -= len(batch)

    async def _take_batch(self) -> list[_PersistenceItem]:
        batch: list[_PersistenceItem] = []
        total_bytes = 0
        first_key = self._ready_sessions[0]
        first_item = self._queues[first_key][0]
        if first_item.done is None and self.batch_max_delay_ms:
            # Give concurrent sessions a short coalescing window. This wait is
            # owned by the persistence loop and never delays WorkflowExecutor.
            await asyncio.sleep(self.batch_max_delay_ms / 1000)
        while self._ready_sessions and len(batch) < self.batch_max_items:
            key = self._ready_sessions.popleft()
            self._ready_set.discard(key)
            queue = self._queues[key]
            item = queue.popleft()
            deferred_for_size = False
            if (
                batch
                and total_bytes + item.size_bytes > self.batch_max_bytes
            ):
                queue.appendleft(item)
                deferred_for_size = True
            else:
                batch.append(item)
                total_bytes += item.size_bytes
            if queue:
                self._ready_sessions.append(key)
                self._ready_set.add(key)
            else:
                del self._queues[key]
            if deferred_for_size:
                break
            if any(value.done is not None for value in batch):
                break
            if total_bytes >= self.batch_max_bytes:
                break
        return batch

    async def _persist_batch(self, batch: list[_PersistenceItem]) -> None:
        async with self._database_sessions.begin() as database:
            workflow_items = [
                item for item in batch if item.kind == "workflow_version"
            ]
            admission_items = [
                item for item in batch if item.kind == "admission"
            ]
            event_items = [item for item in batch if item.kind == "event"]
            snapshot_items = [
                item for item in batch if item.kind == "snapshot"
            ]
            for item in workflow_items:
                await self._persist_workflow_version(database, item.value)
            for item in admission_items:
                await self._persist_admission(database, item)
            if event_items:
                await self._persist_events(database, event_items)
            if snapshot_items:
                await self._persist_snapshots(database, snapshot_items)

    async def _persist_workflow_version(self, database, record: dict[str, Any]) -> None:
        row = await database.scalar(
            select(WorkflowVersionRow).where(
                WorkflowVersionRow.namespace == record["namespace"],
                WorkflowVersionRow.workflow_id == record["workflow_id"],
                WorkflowVersionRow.definition_hash == record["definition_hash"],
                WorkflowVersionRow.operator_manifest_hash
                == record["operator_manifest_hash"],
            )
        )
        if row is None:
            database.add(
                WorkflowVersionRow(
                    **{
                        key: str(value) if key == "id" else value
                        for key, value in record.items()
                    }
                )
            )
        else:
            key = (
                row.namespace,
                row.workflow_id,
                row.definition_hash,
                row.operator_manifest_hash,
            )
            self._workflow_version_ids[key] = UUID(row.id)

    async def _persist_admission(
        self,
        database,
        item: _PersistenceItem,
    ) -> None:
        value = item.value
        session = value["session"]
        invocation = value["invocation"]
        version_id: UUID = value["workflow_version_id"]
        snapshot: ExecutionSnapshot = value["snapshot"]
        session_row = await database.get(SessionRow, session["id"])
        session_values = {
            "namespace": session["namespace"],
            "workflow_id": session["workflow_id"],
            "session_key": session["session_key"],
            "current_invocation_id": invocation["id"],
            "created_at_ms": session["created_at_ms"],
            "updated_at_ms": session["updated_at_ms"],
        }
        if session_row is None:
            database.add(SessionRow(id=session["id"], **session_values))
        else:
            for key, data in session_values.items():
                setattr(session_row, key, data)
        invocation_row = await database.get(InvocationRow, invocation["id"])
        invocation_values = {
            "session_id": session["id"],
            "workflow_version_id": str(version_id),
            "entry_node_id": invocation["entry_node_id"],
            "state": invocation["state"],
            "execution_mode": invocation["execution_mode"],
            "durable_sequence": 0,
            "created_at_ms": invocation["created_at_ms"],
            "updated_at_ms": invocation["updated_at_ms"],
        }
        if invocation_row is None:
            database.add(InvocationRow(id=invocation["id"], **invocation_values))
        snapshot_row = await database.scalar(
            select(RuntimeSnapshotRow).where(
                RuntimeSnapshotRow.invocation_id == invocation["id"],
                RuntimeSnapshotRow.through_sequence == 0,
            )
        )
        if snapshot_row is None:
            database.add(
                RuntimeSnapshotRow(
                    id=str(snapshot.id),
                    invocation_id=invocation["id"],
                    through_sequence=0,
                    state_json=_text(item.encoded),
                    created_at_ms=snapshot.created_at_ms,
                )
            )

    async def _persist_events(
        self,
        database,
        items: list[_PersistenceItem],
    ) -> None:
        events = [item.value["event"] for item in items]
        identities = [
            (str(event.invocation_id), event.sequence)
            for event in events
        ]
        existing_rows = (
            await database.scalars(
                select(RuntimeEventRow).where(
                    tuple_(
                        RuntimeEventRow.invocation_id,
                        RuntimeEventRow.sequence,
                    ).in_(identities)
                )
            )
        ).all()
        existing = {
            (row.invocation_id, row.sequence): row
            for row in existing_rows
        }
        for item, event in zip(items, events, strict=True):
            row = existing.get((str(event.invocation_id), event.sequence))
            if row is None:
                continue
            if (
                row.id != str(event.id)
                or row.schema_version != event.schema_version
                or row.type != event.type
                or row.occurred_at_ms != event.occurred_at_ms
                or row.payload_json != _text(item.encoded)
            ):
                raise RuntimeError(
                    "RuntimeEvent sequence already contains different data: "
                    f"invocation_id={event.invocation_id}, "
                    f"sequence={event.sequence}."
                )
        database.add_all(
            [
                RuntimeEventRow(
                    id=str(event.id),
                    invocation_id=str(event.invocation_id),
                    sequence=event.sequence,
                    schema_version=event.schema_version,
                    type=event.type,
                    occurred_at_ms=event.occurred_at_ms,
                    payload_json=_text(item.encoded),
                )
                for item, event in zip(items, events, strict=True)
                if (str(event.invocation_id), event.sequence) not in existing
            ]
        )

        final_by_invocation: dict[str, _PersistenceItem] = {}
        for item, event in zip(items, events, strict=True):
            key = str(event.invocation_id)
            previous = final_by_invocation.get(key)
            if (
                previous is None
                or previous.value["event"].sequence < event.sequence
            ):
                final_by_invocation[key] = item
        rows = (
            await database.scalars(
                select(InvocationRow).where(
                    InvocationRow.id.in_(tuple(final_by_invocation))
                )
            )
        ).all()
        row_by_id = {row.id: row for row in rows}
        missing = set(final_by_invocation) - set(row_by_id)
        if missing:
            raise RuntimeError(
                "Events reference unknown durable Invocations: "
                + ", ".join(sorted(missing))
            )
        for invocation_id, item in final_by_invocation.items():
            event = item.value["event"]
            row = row_by_id[invocation_id]
            row.state = item.value["invocation_state"]
            row.execution_mode = item.value["execution_mode"]
            row.durable_sequence = max(row.durable_sequence, event.sequence)
            row.updated_at_ms = item.value["updated_at_ms"]

        final_by_session: dict[str, _PersistenceItem] = {}
        for item in items:
            if item.session_id is None:
                continue
            session_id = str(item.session_id)
            previous = final_by_session.get(session_id)
            if (
                previous is None
                or previous.value["session_updated_at_ms"]
                < item.value["session_updated_at_ms"]
            ):
                final_by_session[session_id] = item
        if final_by_session:
            session_rows = (
                await database.scalars(
                    select(SessionRow).where(
                        SessionRow.id.in_(tuple(final_by_session))
                    )
                )
            ).all()
            for row in session_rows:
                row.updated_at_ms = max(
                    row.updated_at_ms,
                    final_by_session[row.id].value[
                        "session_updated_at_ms"
                    ],
                )

    async def _persist_snapshots(
        self,
        database,
        items: list[_PersistenceItem],
    ) -> None:
        snapshots = [item.value for item in items]
        identities = [
            (str(snapshot.invocation_id), snapshot.through_sequence)
            for snapshot in snapshots
        ]
        existing = set(
            (
                await database.execute(
                    select(
                        RuntimeSnapshotRow.invocation_id,
                        RuntimeSnapshotRow.through_sequence,
                    ).where(
                        tuple_(
                            RuntimeSnapshotRow.invocation_id,
                            RuntimeSnapshotRow.through_sequence,
                        ).in_(identities)
                    )
                )
            ).all()
        )
        database.add_all(
            [
                RuntimeSnapshotRow(
                    id=str(snapshot.id),
                    invocation_id=str(snapshot.invocation_id),
                    through_sequence=snapshot.through_sequence,
                    state_json=_text(item.encoded),
                    created_at_ms=snapshot.created_at_ms,
                )
                for item, snapshot in zip(items, snapshots, strict=True)
                if (
                    str(snapshot.invocation_id),
                    snapshot.through_sequence,
                )
                not in existing
            ]
        )

    async def _load_workflow_version_id(
        self,
        key: tuple[str, str, str, str],
    ) -> UUID | None:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self._load_workflow_version_id(key)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            row = await database.scalar(
                select(WorkflowVersionRow).where(
                    WorkflowVersionRow.namespace == key[0],
                    WorkflowVersionRow.workflow_id == key[1],
                    WorkflowVersionRow.definition_hash == key[2],
                    WorkflowVersionRow.operator_manifest_hash == key[3],
                )
            )
        if row is None:
            return None
        version_id = UUID(row.id)
        self._workflow_version_ids[key] = version_id
        return version_id

    def _fail_batch_permanently(
        self,
        batch: list[_PersistenceItem],
        error: BaseException,
    ) -> None:
        self._fatal_persistence_error = error
        for item in batch:
            if item.done is not None and not item.done.done():
                item.done.set_exception(
                    RuntimeError("Runtime persistence failed permanently.")
                )


def _text(payload: bytes | None) -> str:
    if payload is None:
        raise ValueError("Persistence payload was not serialized.")
    return payload.decode("utf-8")


def _is_retryable_database_error(error: DBAPIError) -> bool:
    if error.connection_invalidated:
        return True
    original = error.orig
    code = (
        getattr(original, "sqlstate", None)
        or getattr(original, "pgcode", None)
    )
    if isinstance(code, str) and (
        code.startswith("08")
        or code in {
            "40001",  # serialization_failure
            "40P01",  # deadlock_detected
            "55P03",  # lock_not_available
            "53300",  # too_many_connections
            "57P01",  # admin_shutdown
        }
    ):
        return True
    message = str(original).lower()
    return any(
        marker in message
        for marker in (
            "database is locked",
            "database is busy",
            "temporarily unavailable",
            "connection reset",
            "connection refused",
            "connection closed",
        )
    )


def _resolve_database_url(value: str | Path) -> str:
    if isinstance(value, Path):
        return f"sqlite+aiosqlite:///{value.expanduser().resolve()}"
    text = str(value)
    if "://" not in text:
        return f"sqlite+aiosqlite:///{Path(text).expanduser().resolve()}"
    if text.startswith("sqlite:///"):
        return "sqlite+aiosqlite:///" + text.removeprefix("sqlite:///")
    if text.startswith("postgresql://"):
        return "postgresql+asyncpg://" + text.removeprefix("postgresql://")
    if text.startswith("postgres://"):
        return "postgresql+asyncpg://" + text.removeprefix("postgres://")
    if not (
        text.startswith("sqlite+aiosqlite://")
        or text.startswith("postgresql+asyncpg://")
    ):
        raise ValueError(
            "V1 DatabaseBackend supports SQLite and PostgreSQL only."
        )
    return text
