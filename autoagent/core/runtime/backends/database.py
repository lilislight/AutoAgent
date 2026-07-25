from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import logging
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import event, select, text, tuple_
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from autoagent.core.runtime.artifact import (
    ArtifactPolicy,
    EncodedArtifact,
    RuntimeArtifactEncoder,
)
from autoagent.core.runtime.backends.models import (
    ArtifactRow,
    InvocationRow,
    RuntimeDatabaseBase,
    RuntimeEventRow,
    SessionRow,
    WorkflowVersionRow,
)
from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.runtime.hooks import RuntimeEventLoop
from autoagent.core.runtime.persistence import PersistenceEnvelope
from autoagent.core.runtime.serialization import ArtifactRef
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.snapshot import (
    ExecutionSnapshot,
    StateOperation,
    apply_state_operations,
)
from autoagent.core.runtime.store import RuntimeStore
from autoagent.core.runtime.time import utc_timestamp_ms


logger = logging.getLogger(__name__)


@dataclass
class _PersistenceItem:
    kind: str
    session_id: UUID | None
    invocation_id: UUID | None
    record: dict[str, Any]
    encoded: bytes | None
    artifacts: tuple[EncodedArtifact, ...]
    size_bytes: int
    done: asyncio.Future[None] | None = None
    coordinator_id: UUID | None = None


class DatabaseBackend:
    """SQLite/PostgreSQL durability for one authoritative ``RuntimeStore``."""

    def __init__(
        self,
        database_url: str | Path,
        *,
        echo: bool = False,
        batch_max_items: int = 256,
        batch_max_bytes: int = 4 * 1024 * 1024,
        batch_max_delay_ms: int = 5,
        recovery_event_interval: int = 200,
        artifact_policy: ArtifactPolicy | None = None,
        sqlite_synchronous: str = "FULL",
    ) -> None:
        if batch_max_items < 1 or batch_max_bytes < 1 or batch_max_delay_ms < 0:
            raise ValueError("Invalid persistence batch limits.")
        if recovery_event_interval < 1:
            raise ValueError("recovery_event_interval must be positive.")
        normalized_synchronous = sqlite_synchronous.upper()
        if normalized_synchronous not in {"FULL", "NORMAL"}:
            raise ValueError("sqlite_synchronous must be FULL or NORMAL.")

        self.database_url = _resolve_database_url(database_url)
        self.engine: AsyncEngine = create_async_engine(
            self.database_url,
            echo=echo,
        )
        self._database_sessions = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        self.batch_max_items = batch_max_items
        self.batch_max_bytes = batch_max_bytes
        self.batch_max_delay_ms = batch_max_delay_ms
        self.recovery_event_interval = recovery_event_interval
        self.artifact_policy = artifact_policy or ArtifactPolicy()
        self.sqlite_synchronous = normalized_synchronous

        self._database_loop = RuntimeEventLoop(
            name="autoagent-persistence-runtime"
        )
        self._queues: dict[str, deque[_PersistenceItem]] = {}
        self._halted_items: deque[_PersistenceItem] = deque()
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
        self._workflow_version_ids: dict[tuple[str, str, str, str], UUID] = {}
        self._recovery_sequences: dict[UUID, int] = {}
        self._durable_states: dict[UUID, dict[str, Any]] = {}
        self._projection_sequences: dict[UUID, int] = {}
        self._artifact_encoder: RuntimeArtifactEncoder | None = None
        self._store: RuntimeStore | None = None

    @classmethod
    def from_path(cls, path: str | Path, **kwargs: Any) -> DatabaseBackend:
        return cls(Path(path), **kwargs)

    def bind(self, store: RuntimeStore) -> None:
        if self._store is not None and self._store is not store:
            raise RuntimeError("A DatabaseBackend can belong to only one RuntimeStore.")
        self._store = store
        if store.persistence is None:
            raise RuntimeError("DatabaseBackend requires a PersistenceCoordinator.")
        store.persistence.bind_consumer(self._wake_persistence)

    @property
    def store(self) -> RuntimeStore:
        if self._store is None:
            raise RuntimeError("DatabaseBackend is not attached to a RuntimeStore.")
        return self._store

    @property
    def serializer(self):
        return self.store.serializer

    @property
    def coordinator(self):
        value = self.store.persistence
        if value is None:
            raise RuntimeError("DatabaseBackend has no PersistenceCoordinator.")
        return value

    @property
    def artifact_encoder(self) -> RuntimeArtifactEncoder:
        if self._artifact_encoder is None:
            self._artifact_encoder = RuntimeArtifactEncoder(
                self.serializer,
                self.artifact_policy,
            )
        return self._artifact_encoder

    def _total_pending_count(self) -> int:
        with self._pressure_lock:
            control = self._pending_count + self._inflight_count
        return control + self.coordinator.pending_count

    def _wake_persistence(self) -> None:
        self._database_loop.call_soon(self._signal_persistence)

    def _signal_persistence(self) -> None:
        if self._queue_event is not None:
            self._queue_event.set()

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
                # WAL is shared by every SQLite profile. FULL remains the
                # durable default; NORMAL is an explicit performance choice.
                @event.listens_for(self.engine.sync_engine, "connect")
                def _set_sqlite_pragma(
                    dbapi_connection: Any,
                    connection_record: Any,
                ) -> None:
                    if self.database_url.startswith("sqlite"):
                        cursor = dbapi_connection.cursor()
                        cursor.execute("PRAGMA journal_mode=WAL")
                        cursor.execute(
                            f"PRAGMA synchronous={self.sqlite_synchronous}"
                        )
                        cursor.close()

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
        while self._total_pending_count():
            health = self.coordinator.health
            if health.state == "unavailable":
                raise RuntimeError(
                    health.last_error or "Persistence backend is unavailable."
                )
            await asyncio.sleep(0.001)
        await self.coordinator.flush()

    def _prepare_workflow_item(
        self,
        envelope: PersistenceEnvelope,
    ) -> _PersistenceItem:
        snapshot = envelope.workflow_snapshot
        if snapshot is None:
            raise ValueError("Workflow persistence envelope has no snapshot.")
        key = (
            envelope.namespace,
            snapshot.workflow_id,
            snapshot.definition_hash,
            snapshot.operator_manifest_hash,
        )
        version_id = self._workflow_version_ids.setdefault(key, uuid4())
        record = {
            "id": version_id,
            "namespace": envelope.namespace,
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
        return _PersistenceItem(
            kind="workflow_version",
            session_id=None,
            invocation_id=None,
            record=record,
            encoded=None,
            artifacts=(),
            size_bytes=envelope.estimated_bytes,
            coordinator_id=envelope.id,
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
        if self.coordinator.health.state == "unavailable":
            return None
        await self.ainitialize()
        try:
            async with self._database_sessions() as database:
                row = await database.scalar(
                    select(SessionRow).where(
                        SessionRow.namespace == namespace,
                        SessionRow.workflow_id == workflow_id,
                        SessionRow.session_key == session_key,
                    )
                )
        except (OperationalError, DBAPIError) as exc:
            self.coordinator.mark_retrying(exc)
            logger.warning(
                "Historical Session lookup is unavailable; execution may "
                "create a process-local Session instead: workflow_id=%s "
                "session_key=%s error=%s",
                workflow_id,
                session_key,
                exc,
            )
            return None
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

    def _prepare_admission_item(
        self,
        envelope: PersistenceEnvelope,
    ) -> _PersistenceItem:
        snapshot = envelope.execution_snapshot
        if (
            snapshot is None
            or envelope.session_id is None
            or envelope.invocation_id is None
            or envelope.workflow_key is None
        ):
            raise ValueError("Admission persistence envelope is incomplete.")
        persisted_state, artifacts = self._externalize_admission_state(
            snapshot.state,
            namespace=envelope.namespace,
            invocation_id=envelope.invocation_id,
        )
        encoded_genesis = self.serializer.dumps(persisted_state)
        session_record = snapshot.state["session"]
        invocation_record = snapshot.state["invocation"]
        admission = {
            "session": {
                key: session_record[key]
                for key in (
                    "id",
                    "namespace",
                    "workflow_id",
                    "session_key",
                    "current_invocation_id",
                    "created_at_ms",
                    "updated_at_ms",
                )
            },
            "invocation": {
                key: invocation_record[key]
                for key in (
                    "id",
                    "entry_node_id",
                    "state",
                    "execution_mode",
                    "created_at_ms",
                    "updated_at_ms",
                )
            },
            "workflow_key": envelope.workflow_key,
            "genesis_created_at_ms": snapshot.created_at_ms,
        }
        return _PersistenceItem(
            kind="admission",
            session_id=envelope.session_id,
            invocation_id=envelope.invocation_id,
            record=admission,
            encoded=encoded_genesis,
            artifacts=artifacts,
            size_bytes=(
                len(encoded_genesis)
                + sum(artifact.size_bytes for artifact in artifacts)
                + 1024
            ),
            coordinator_id=envelope.id,
        )

    def _prepare_event_item(
        self,
        envelope: PersistenceEnvelope,
    ) -> _PersistenceItem:
        event = envelope.event
        if (
            event is None
            or envelope.session_id is None
            or envelope.invocation_id is None
            or envelope.session_updated_at_ms is None
            or envelope.invocation_state is None
            or envelope.execution_mode is None
            or envelope.invocation_updated_at_ms is None
        ):
            raise ValueError("Event persistence envelope is incomplete.")
        encoded_payload = self.serializer.dumps_unchecked(event.payload)
        if (
            self.artifact_policy.enabled
            and len(encoded_payload) > self.artifact_policy.inline_max_bytes
        ):
            persisted_payload, artifacts = self._externalize_event_payload(
                event.payload,
                namespace=envelope.namespace,
                invocation_id=envelope.invocation_id,
            )
            encoded_payload = self.serializer.dumps(persisted_payload)
        else:
            artifacts = ()
            if self.serializer.max_inline_bytes is not None:
                encoded_payload = self.serializer.dumps(event.payload)
        return _PersistenceItem(
            kind="event",
            session_id=envelope.session_id,
            invocation_id=envelope.invocation_id,
            record={
                "event_id": event.id,
                "sequence": event.sequence,
                "schema_version": event.schema_version,
                "event_type": event.type,
                "occurred_at_ms": event.occurred_at_ms,
                "invocation_state": envelope.invocation_state,
                "execution_mode": envelope.execution_mode,
                "updated_at_ms": envelope.invocation_updated_at_ms,
                "session_updated_at_ms": envelope.session_updated_at_ms,
                "force_recovery_state": envelope.force_recovery_checkpoint,
            },
            encoded=encoded_payload,
            artifacts=artifacts,
            size_bytes=(
                len(encoded_payload)
                + sum(artifact.size_bytes for artifact in artifacts)
                + 256
            ),
            coordinator_id=envelope.id,
        )

    def _externalize_admission_state(
        self,
        state: dict[str, Any],
        *,
        namespace: str,
        invocation_id: UUID,
    ) -> tuple[dict[str, Any], tuple[EncodedArtifact, ...]]:
        """Externalize mutable values without replacing restart structures."""

        result = dict(state)
        artifacts: list[EncodedArtifact] = []
        fields = {
            "session": ("context",),
            "invocation": (
                "input",
                "context",
                "result",
                "error",
                "deferred_error",
            ),
        }
        for section, section_fields in fields.items():
            record = dict(result[section])
            result[section] = record
            for field in section_fields:
                if field not in record:
                    continue
                persisted, encoded = self.artifact_encoder.externalize(
                    record[field],
                    namespace=namespace,
                    invocation_id=invocation_id,
                )
                record[field] = persisted
                artifacts.extend(encoded)
        return result, tuple(artifacts)

    def _externalize_event_payload(
        self,
        payload: dict[str, Any],
        *,
        namespace: str,
        invocation_id: UUID,
    ) -> tuple[dict[str, Any], tuple[EncodedArtifact, ...]]:
        """Keep the reducer envelope inline and externalize operation values."""

        result = dict(payload)
        artifacts: list[EncodedArtifact] = []
        operations: list[dict[str, Any]] = []
        for raw_operation in payload.get("operations", ()):
            operation = dict(raw_operation)
            if "value" in operation:
                persisted, encoded = self.artifact_encoder.externalize(
                    operation["value"],
                    namespace=namespace,
                    invocation_id=invocation_id,
                )
                operation["value"] = persisted
                artifacts.extend(encoded)
            operations.append(operation)
        result["operations"] = operations
        return result, tuple(artifacts)

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
            row = await database.get(InvocationRow, str(invocation_id))
        if row is None:
            return None
        use_recovery = (
            at_or_before_sequence is None
            and row.recovery_state_json is not None
            and row.recovery_sequence is not None
        )
        encoded_state = (
            row.recovery_state_json
            if use_recovery
            else row.genesis_state_json
        )
        sequence = int(row.recovery_sequence or 0) if use_recovery else 0
        state = await self._hydrate_runtime_values(
            self.serializer.loads(encoded_state)
        )
        self._durable_states[invocation_id] = self.serializer.loads(encoded_state)
        self._recovery_sequences[invocation_id] = sequence
        self.coordinator.remember_durable(
            invocation_id,
            row.durable_sequence,
        )
        self.coordinator.remember_admission_durable(invocation_id)
        return ExecutionSnapshot(
            invocation_id=invocation_id,
            through_sequence=sequence,
            state=state,
            created_at_ms=row.recovery_updated_at_ms or row.created_at_ms,
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
        payloads = await self._hydrate_runtime_values(
            [self.serializer.loads(row.payload_json) for row in rows]
        )
        events = tuple(
            RuntimeEvent(
                id=UUID(row.id),
                invocation_id=invocation_id,
                sequence=row.sequence,
                schema_version=row.schema_version,
                type=row.type,
                occurred_at_ms=row.occurred_at_ms,
                payload=payload,
            )
            for row, payload in zip(rows, payloads, strict=True)
        )
        if events:
            self.coordinator.remember_durable(
                invocation_id,
                events[-1].sequence,
            )
        return events

    def _enqueue_nowait(self, item: _PersistenceItem) -> None:
        if not self._database_loop.is_current():
            raise RuntimeError("Persistence items belong to the database loop.")
        if not self._initialized or self._queue_event is None:
            raise RuntimeError("DatabaseBackend is not initialized.")
        key = str(item.session_id) if item.session_id is not None else "__control__"
        queue = self._queues.setdefault(key, deque())
        queue.append(item)
        if key not in self._ready_set:
            self._ready_set.add(key)
            self._ready_sessions.append(key)
        if item.coordinator_id is None:
            with self._pressure_lock:
                self._pending_bytes += item.size_bytes
                self._pending_count += 1
        self._queue_event.set()

    def _ensure_worker(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(
                self._persistence_loop(),
                name="autoagent-persistence-writer",
            )

    async def _persistence_loop(self) -> None:
        assert self._queue_event is not None
        while True:
            if self.coordinator.health.state == "unavailable":
                return
            self._import_incoming()
            if not self._ready_sessions:
                if self._closing:
                    return
                self._queue_event.clear()
                self._import_incoming()
                if self._ready_sessions:
                    continue
                await self._queue_event.wait()
                continue
            batch = await self._take_batch()
            if not batch:
                continue
            control_items = [
                item for item in batch if item.coordinator_id is None
            ]
            batch_bytes = sum(item.size_bytes for item in control_items)
            with self._pressure_lock:
                self._pending_bytes -= batch_bytes
                self._pending_count -= len(control_items)
                self._inflight_bytes += batch_bytes
                self._inflight_count += len(control_items)
            retry_delay = 0.05
            while True:
                try:
                    await self._persist_batch(batch)
                except asyncio.CancelledError:
                    raise
                except (OperationalError, DBAPIError) as exc:
                    if not _is_retryable_database_error(exc):
                        self._halt_unavailable_persistence(batch, exc)
                        return
                    self.coordinator.mark_retrying(exc)
                    logger.warning(
                        "Database persistence is retrying; Workflow execution "
                        "continues in memory: %s",
                        exc,
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(5.0, retry_delay * 2)
                    continue
                except Exception as exc:
                    self._halt_unavailable_persistence(batch, exc)
                    return
                else:
                    self.coordinator.mark_healthy()
                    advanced: set[UUID] = set()
                    for item in batch:
                        if item.kind == "event":
                            assert item.invocation_id is not None
                            advanced.add(item.invocation_id)
                            assert item.coordinator_id is not None
                            self.coordinator.mark_durable(
                                item.coordinator_id,
                                item.invocation_id,
                                int(item.record["sequence"]),
                            )
                        elif item.kind == "admission":
                            assert item.invocation_id is not None
                            assert item.coordinator_id is not None
                            self.coordinator.mark_admission_durable(
                                item.coordinator_id,
                                item.invocation_id,
                            )
                        else:
                            assert item.coordinator_id is not None
                            self.coordinator.discard(item.coordinator_id)
                        if item.done is not None and not item.done.done():
                            item.done.set_result(None)
                    for invocation_id in advanced:
                        self.store._persistence_advanced(invocation_id)
                    break
            with self._pressure_lock:
                self._inflight_bytes -= batch_bytes
                self._inflight_count -= len(control_items)

    def _import_incoming(self) -> None:
        for envelope in self.coordinator.take(self.batch_max_items):
            if (
                envelope.invocation_id is not None
                and self.coordinator.invocation_error(envelope.invocation_id)
                is not None
            ):
                self.coordinator.discard(envelope.id)
                continue
            try:
                if envelope.kind == "workflow_version":
                    item = self._prepare_workflow_item(envelope)
                elif envelope.kind == "admission":
                    item = self._prepare_admission_item(envelope)
                else:
                    item = self._prepare_event_item(envelope)
            except Exception as exc:
                self.coordinator.discard(envelope.id)
                if envelope.invocation_id is not None:
                    sequence = (
                        envelope.event.sequence
                        if envelope.event is not None
                        else 0
                    )
                    self.coordinator.fail_invocation(
                        envelope.invocation_id,
                        sequence,
                        exc,
                    )
                else:
                    self.coordinator.mark_unavailable(exc)
                logger.exception(
                    "Persistence record preparation failed; Workflow "
                    "execution remains available: kind=%s invocation_id=%s",
                    envelope.kind,
                    envelope.invocation_id,
                )
                continue
            self.coordinator.adjust_size(envelope.id, item.size_bytes)
            self._enqueue_nowait(item)

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
            for item in workflow_items:
                await self._persist_workflow_version(database, item.record)
            if workflow_items:
                await database.flush()
            for item in admission_items:
                await self._persist_admission(database, item)
            artifacts = tuple(
                artifact
                for item in batch
                for artifact in item.artifacts
            )
            if artifacts:
                await self._persist_artifacts(database, artifacts)
            if event_items:
                await self._persist_events(database, event_items)

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
        value = item.record
        session = value["session"]
        invocation = value["invocation"]
        workflow_key = value["workflow_key"]
        version_row = await database.scalar(
            select(WorkflowVersionRow).where(
                WorkflowVersionRow.namespace == workflow_key[0],
                WorkflowVersionRow.workflow_id == workflow_key[1],
                WorkflowVersionRow.definition_hash == workflow_key[2],
                WorkflowVersionRow.operator_manifest_hash == workflow_key[3],
            )
        )
        if version_row is None:
            raise RuntimeError(
                "Invocation genesis references workflow metadata that is not "
                "durable yet."
            )
        version_id = UUID(version_row.id)
        genesis_state = self.serializer.loads(_text(item.encoded))
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
            "genesis_state_json": _text(item.encoded),
            "recovery_state_json": None,
            "recovery_sequence": None,
            "recovery_updated_at_ms": None,
            "created_at_ms": invocation["created_at_ms"],
            "updated_at_ms": invocation["updated_at_ms"],
        }
        if invocation_row is None:
            database.add(InvocationRow(id=invocation["id"], **invocation_values))
        invocation_id = UUID(str(invocation["id"]))
        self._durable_states[invocation_id] = genesis_state
        self._projection_sequences[invocation_id] = 0
        self._recovery_sequences[invocation_id] = 0

    async def _persist_events(
        self,
        database,
        items: list[_PersistenceItem],
    ) -> None:
        identities = [
            (str(item.invocation_id), int(item.record["sequence"]))
            for item in items
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
        for item in items:
            invocation_id = str(item.invocation_id)
            sequence = int(item.record["sequence"])
            row = existing.get((invocation_id, sequence))
            if row is None:
                continue
            if (
                row.id != str(item.record["event_id"])
                or row.schema_version != item.record["schema_version"]
                or row.type != item.record["event_type"]
                or row.occurred_at_ms != item.record["occurred_at_ms"]
                or row.payload_json != _text(item.encoded)
            ):
                raise RuntimeError(
                    "RuntimeEvent sequence already contains different data: "
                    f"invocation_id={invocation_id}, "
                    f"sequence={sequence}."
                )
        database.add_all(
            [
                RuntimeEventRow(
                    id=str(item.record["event_id"]),
                    invocation_id=str(item.invocation_id),
                    sequence=int(item.record["sequence"]),
                    schema_version=int(item.record["schema_version"]),
                    type=str(item.record["event_type"]),
                    occurred_at_ms=int(item.record["occurred_at_ms"]),
                    payload_json=_text(item.encoded),
                )
                for item in items
                if (
                    str(item.invocation_id),
                    int(item.record["sequence"]),
                )
                not in existing
            ]
        )

        final_by_invocation: dict[str, _PersistenceItem] = {}
        grouped: dict[str, list[_PersistenceItem]] = {}
        for item in items:
            key = str(item.invocation_id)
            grouped.setdefault(key, []).append(item)
            previous = final_by_invocation.get(key)
            if (
                previous is None
                or int(previous.record["sequence"])
                < int(item.record["sequence"])
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
            row = row_by_id[invocation_id]
            ordered = sorted(
                grouped[invocation_id],
                key=lambda value: int(value.record["sequence"]),
            )
            state = await self._load_projection_state(database, row)
            projection_sequence = self._projection_sequences.get(
                UUID(invocation_id),
                row.recovery_sequence or 0,
            )
            for event_item in ordered:
                sequence = int(event_item.record["sequence"])
                if sequence <= projection_sequence:
                    continue
                payload = self.serializer.loads(_text(event_item.encoded))
                operations = tuple(
                    StateOperation.model_validate(operation)
                    for operation in payload["operations"]
                )
                state = apply_state_operations(state, operations)
                projection_sequence = sequence

            final_sequence = int(item.record["sequence"])
            row.state = str(item.record["invocation_state"])
            row.execution_mode = str(item.record["execution_mode"])
            row.durable_sequence = max(row.durable_sequence, final_sequence)
            row.updated_at_ms = int(item.record["updated_at_ms"])

            recovery_sequence = int(row.recovery_sequence or 0)
            force_recovery = any(
                bool(value.record["force_recovery_state"])
                for value in ordered
            )
            if (
                force_recovery
                or final_sequence - recovery_sequence
                >= self.recovery_event_interval
            ):
                row.recovery_state_json = _text(
                    self.serializer.dumps_unchecked(state)
                )
                row.recovery_sequence = final_sequence
                row.recovery_updated_at_ms = int(
                    item.record["updated_at_ms"]
                )
                self._recovery_sequences[UUID(invocation_id)] = final_sequence

            self._durable_states[UUID(invocation_id)] = state
            self._projection_sequences[UUID(invocation_id)] = final_sequence

        final_by_session: dict[str, _PersistenceItem] = {}
        for item in items:
            if item.session_id is None:
                continue
            session_id = str(item.session_id)
            previous = final_by_session.get(session_id)
            if (
                previous is None
                or previous.record["session_updated_at_ms"]
                < item.record["session_updated_at_ms"]
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
                    final_by_session[row.id].record[
                        "session_updated_at_ms"
                    ],
                )

    async def _persist_artifacts(
        self,
        database,
        artifacts: tuple[EncodedArtifact, ...],
    ) -> None:
        unique = {str(artifact.id): artifact for artifact in artifacts}
        if not unique:
            return
        existing_ids = set(
            await database.scalars(
                select(ArtifactRow.id).where(
                    ArtifactRow.id.in_(tuple(unique))
                )
            )
        )
        database.add_all(
            [
                ArtifactRow(
                    id=artifact_id,
                    namespace=artifact.namespace,
                    owner_invocation_id=str(artifact.owner_invocation_id),
                    kind=artifact.kind,
                    storage=artifact.storage,
                    uri=artifact.uri,
                    media_type=artifact.media_type,
                    encoding=artifact.encoding,
                    size_bytes=artifact.size_bytes,
                    sha256=artifact.sha256,
                    payload_blob=artifact.payload,
                    metadata_json=_text(
                        self.serializer.dumps_unchecked(artifact.metadata)
                    ),
                    created_at_ms=artifact.created_at_ms,
                )
                for artifact_id, artifact in unique.items()
                if artifact_id not in existing_ids
            ]
        )

    async def _load_projection_state(
        self,
        database,
        row: InvocationRow,
    ) -> dict[str, Any]:
        invocation_id = UUID(row.id)
        cached = self._durable_states.get(invocation_id)
        if (
            cached is not None
            and self._projection_sequences.get(invocation_id)
            == row.durable_sequence
        ):
            return cached

        if (
            row.recovery_state_json is not None
            and row.recovery_sequence is not None
        ):
            state = self.serializer.loads(row.recovery_state_json)
            cursor = row.recovery_sequence
        else:
            state = self.serializer.loads(row.genesis_state_json)
            cursor = 0
        if cursor < row.durable_sequence:
            rows = (
                await database.scalars(
                    select(RuntimeEventRow)
                    .where(
                        RuntimeEventRow.invocation_id == row.id,
                        RuntimeEventRow.sequence > cursor,
                        RuntimeEventRow.sequence <= row.durable_sequence,
                    )
                    .order_by(RuntimeEventRow.sequence)
                )
            ).all()
            for event_row in rows:
                payload = self.serializer.loads(event_row.payload_json)
                operations = tuple(
                    StateOperation.model_validate(operation)
                    for operation in payload["operations"]
                )
                state = apply_state_operations(state, operations)
                cursor = event_row.sequence
        self._durable_states[invocation_id] = state
        self._projection_sequences[invocation_id] = cursor
        self._recovery_sequences[invocation_id] = int(
            row.recovery_sequence or 0
        )
        return state

    async def _hydrate_runtime_values(self, value: Any) -> Any:
        current = value
        while True:
            refs: dict[UUID, ArtifactRef] = {}
            _collect_runtime_artifact_refs(current, refs)
            if not refs:
                return current
            async with self._database_sessions() as database:
                rows = (
                    await database.scalars(
                        select(ArtifactRow).where(
                            ArtifactRow.id.in_(
                                tuple(str(value) for value in refs)
                            )
                        )
                    )
                ).all()
            row_by_id = {UUID(row.id): row for row in rows}
            missing = set(refs) - set(row_by_id)
            if missing:
                raise RuntimeError(
                    "Runtime values reference missing Artifacts: "
                    + ", ".join(sorted(str(value) for value in missing))
                )
            replacements: dict[UUID, Any] = {}
            for artifact_id, row in row_by_id.items():
                if row.payload_blob is None:
                    raise RuntimeError(
                        f"Runtime value Artifact has no database payload: {artifact_id}"
                    )
                ref = refs[artifact_id]
                self.artifact_encoder.remember(
                    ref,
                    invocation_id=UUID(str(row.owner_invocation_id)),
                )
                replacements[artifact_id] = self.serializer.loads(
                    row.payload_blob
                )
            current = _replace_runtime_artifact_refs(current, replacements)

    def _halt_unavailable_persistence(
        self,
        batch: list[_PersistenceItem],
        error: BaseException,
    ) -> None:
        self.coordinator.mark_unavailable(error)
        self._halted_items.extend(batch)
        for queue in self._queues.values():
            self._halted_items.extend(queue)
        logger.error(
            "Database persistence is unavailable; Workflow execution remains "
            "available in memory and new submission will be limited by "
            "persistence backlog: %s",
            error,
        )
        for item in batch:
            if item.done is not None and not item.done.done():
                item.done.set_exception(
                    RuntimeError("Runtime persistence backend is unavailable.")
                )
        for queue in self._queues.values():
            for item in queue:
                if item.done is not None and not item.done.done():
                    item.done.set_exception(
                        RuntimeError("Runtime persistence backend is unavailable.")
                    )
        self._queues.clear()
        self._ready_sessions.clear()
        self._ready_set.clear()
        with self._pressure_lock:
            self._pending_bytes = 0
            self._pending_count = 0

    def release_invocation_cache(self, invocation_id: UUID) -> None:
        self._durable_states.pop(invocation_id, None)
        self._projection_sequences.pop(invocation_id, None)
        self._recovery_sequences.pop(invocation_id, None)
        if self._artifact_encoder is not None:
            self._artifact_encoder.forget_invocation(invocation_id)


def _text(payload: bytes | None) -> str:
    if payload is None:
        raise ValueError("Persistence payload was not serialized.")
    return payload.decode("utf-8")


def _collect_runtime_artifact_refs(
    value: Any,
    refs: dict[UUID, ArtifactRef],
) -> None:
    if isinstance(value, ArtifactRef):
        if value.kind == "runtime_value":
            refs[value.id] = value
        return
    if isinstance(value, dict):
        for item in value.values():
            _collect_runtime_artifact_refs(item, refs)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_runtime_artifact_refs(item, refs)


def _replace_runtime_artifact_refs(
    value: Any,
    replacements: dict[UUID, Any],
) -> Any:
    if isinstance(value, ArtifactRef):
        return replacements.get(value.id, value)
    if isinstance(value, dict):
        return {
            key: _replace_runtime_artifact_refs(item, replacements)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _replace_runtime_artifact_refs(item, replacements)
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            _replace_runtime_artifact_refs(item, replacements)
            for item in value
        )
    if isinstance(value, set):
        return {
            _replace_runtime_artifact_refs(item, replacements)
            for item in value
        }
    return value


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
