from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import logging
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import event, func, select, text, tuple_
from sqlalchemy.engine import make_url
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
    RecoveryStateRow,
    RuntimeDatabaseBase,
    RuntimeEventRow,
    SessionRow,
    UserEventRow,
    WorkflowVersionRow,
)
from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.compiler import WorkflowVersionSnapshot, workflow_revision_id
from autoagent.core.runtime.context import SessionContext
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
from autoagent.core.runtime.user_event import UserEvent


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
        shutdown_timeout_ms: int = 5_000,
    ) -> None:
        if batch_max_items < 1 or batch_max_bytes < 1 or batch_max_delay_ms < 0:
            raise ValueError("Invalid persistence batch limits.")
        if recovery_event_interval < 1:
            raise ValueError("recovery_event_interval must be positive.")
        if shutdown_timeout_ms < 0:
            raise ValueError("shutdown_timeout_ms cannot be negative.")
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
        self.shutdown_timeout_ms = shutdown_timeout_ms

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
        self._workflow_version_ids: dict[tuple[str, str], UUID] = {}
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
    def persistence_worker_state(self) -> str:
        if self._closing:
            return "stopping"
        if not self._initialized:
            return "starting"
        if self._worker is None or self._worker.done():
            return "stopped"
        return "running"

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

    def _total_pending_bytes(self) -> int:
        with self._pressure_lock:
            control = self._pending_bytes + self._inflight_bytes
        return control + self.coordinator.pending_bytes

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
                _ensure_sqlite_parent_directory(self.database_url)
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
                self._database_loop.stop(
                    timeout_s=self.shutdown_timeout_ms / 1000
                )
                return
            try:
                await self._database_loop.arun(self.aclose())
            finally:
                self._database_loop.stop(
                    timeout_s=self.shutdown_timeout_ms / 1000
                )
            return
        if self._closing:
            return
        self._closing = True
        flush_error: Exception | None = None
        try:
            try:
                async with asyncio.timeout(self.shutdown_timeout_ms / 1000):
                    await self.aflush()
            except TimeoutError:
                logger.error(
                    "Persistence shutdown exceeded %d ms; abandoning "
                    "undurable in-memory backlog: pending_count=%d "
                    "pending_bytes=%d",
                    self.shutdown_timeout_ms,
                    self._total_pending_count(),
                    self._total_pending_bytes(),
                )
            except Exception as exc:
                logger.exception(
                    "Persistence could not flush during shutdown; undurable "
                    "in-memory records are being abandoned."
                )
                flush_error = exc
        finally:
            if self._worker is not None:
                assert self._queue_event is not None
                self._queue_event.set()
                if not self._worker.done():
                    self._worker.cancel()
                await asyncio.gather(self._worker, return_exceptions=True)
                self._worker = None
            await self.engine.dispose()
            self._initialized = False
        if flush_error is not None:
            raise flush_error

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
            snapshot.workflow_id,
            snapshot.definition_hash,
        )
        version_id = self._workflow_version_ids.setdefault(
            key,
            UUID(
                workflow_revision_id(
                    snapshot.workflow_id,
                    snapshot.definition_hash,
                )
            ),
        )
        record = {
            "id": version_id,
            "workflow_id": snapshot.workflow_id,
            "workflow_version": (
                None
                if snapshot.workflow_version is None
                else str(snapshot.workflow_version)
            ),
            "ir_version": snapshot.ir_version,
            "compiler_version": snapshot.compiler_version,
            "definition_hash": snapshot.definition_hash,
            "definition_json": _text(
                self.serializer.dumps_unchecked(snapshot.definition)
            ),
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
        workflow_revision_id: str,
        session_key: str,
    ) -> Session | None:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.afind_session(
                    workflow_revision_id=workflow_revision_id,
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
                        SessionRow.workflow_revision_id
                        == workflow_revision_id,
                        SessionRow.session_key == session_key,
                    )
                )
        except (OperationalError, DBAPIError) as exc:
            self.coordinator.mark_retrying(exc)
            logger.warning(
                "Historical Session lookup is unavailable; execution may "
                "create a process-local Session instead: workflow_revision_id=%s "
                "session_key=%s error=%s",
                workflow_revision_id,
                session_key,
                exc,
            )
            return None
        if row is None:
            return None
        if row.current_invocation_id is not None:
            async with self._database_sessions() as database:
                invocation_row = await database.get(
                    InvocationRow,
                    row.current_invocation_id,
                )
            if (
                invocation_row is not None
                and invocation_row.event_mode != "minimal"
                and invocation_row.state in {"created", "running", "waiting"}
            ):
                raise RuntimeError(
                    "Persisted active Invocation was not loaded during "
                    "AutoAgentApp.start(). Register the Workflow before starting "
                    "the App so startup recovery can claim it: "
                    f"workflow_revision_id={workflow_revision_id}, "
                    f"invocation_id={invocation_row.id}"
                )
        context = await self._hydrate_runtime_values(
            self.serializer.loads(row.context_json)
        )
        session = Session(
            id=UUID(row.id),
            workflow_id=row.workflow_id,
            workflow_revision_id=row.workflow_revision_id,
            session_key=row.session_key,
            context=SessionContext.from_record(context),
            created_at_ms=row.created_at_ms,
            updated_at_ms=row.updated_at_ms,
        )
        return session

    async def alist_recoverable_invocation_ids(
        self,
        *,
        workflow_revision_ids: tuple[str, ...],
    ) -> tuple[UUID, ...]:
        if not workflow_revision_ids:
            return ()
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_recoverable_invocation_ids(
                    workflow_revision_ids=workflow_revision_ids,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            rows = (
                await database.scalars(
                    select(InvocationRow.id)
                    .join(
                        SessionRow,
                        SessionRow.id == InvocationRow.session_id,
                    )
                    .where(
                        SessionRow.workflow_revision_id.in_(
                            workflow_revision_ids
                        ),
                        SessionRow.current_invocation_id == InvocationRow.id,
                        InvocationRow.event_mode != "minimal",
                        InvocationRow.state.in_(
                            ("created", "running", "waiting")
                        ),
                    )
                    .order_by(
                        InvocationRow.created_at_ms,
                        InvocationRow.id,
                    )
                )
            ).all()
        return tuple(UUID(value) for value in rows)

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
        session_record = snapshot.state["session"]
        invocation_record = snapshot.state["invocation"]
        event_mode = invocation_record["event_mode"]
        persisted_input, input_artifacts = self.artifact_encoder.externalize(
            invocation_record.get("input"),
            invocation_id=envelope.invocation_id,
        )
        artifacts: tuple[EncodedArtifact, ...] = input_artifacts
        persisted_state: dict[str, Any] | None = None
        if event_mode != "minimal":
            persisted_state, state_artifacts = self._externalize_admission_state(
                snapshot.state,
                invocation_id=envelope.invocation_id,
            )
            artifacts = (*artifacts, *state_artifacts)
        encoded_bundle = self.serializer.dumps_unchecked(
            {
                "input": persisted_input,
                "session_context": (
                    persisted_state["session"].get("context", {})
                    if persisted_state is not None
                    else {"data": {}}
                ),
                "state": persisted_state,
            }
        )
        admission = {
            "session": {
                key: session_record[key]
                for key in (
                    "id",
                    "workflow_id",
                    "workflow_revision_id",
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
                    "workflow_revision_id",
                    "entry_node_id",
                    "state",
                    "execution_mode",
                    "event_mode",
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
            encoded=encoded_bundle,
            artifacts=artifacts,
            size_bytes=(
                len(encoded_bundle)
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
        persisted_event, artifacts = self._externalize_event_values(
            event,
            invocation_id=envelope.invocation_id,
        )
        encoded_payload = self.serializer.dumps(persisted_event["payload"])
        input_json = (
            None
            if persisted_event["input"] is None
            else _text(self.serializer.dumps(persisted_event["input"]))
        )
        output_json = (
            None
            if persisted_event["output"] is None
            else _text(self.serializer.dumps(persisted_event["output"]))
        )
        operations_json = (
            None
            if persisted_event["operations"] is None
            else _text(self.serializer.dumps(persisted_event["operations"]))
        )
        persisted_result, result_artifacts = self.artifact_encoder.externalize(
            envelope.invocation_result,
            invocation_id=envelope.invocation_id,
        )
        artifacts = (*artifacts, *result_artifacts)
        recovery_state_json = None
        recovery_sequence = None
        if envelope.recovery_snapshot is not None:
            persisted_recovery, recovery_artifacts = (
                self._externalize_admission_state(
                    envelope.recovery_snapshot.state,
                    invocation_id=envelope.invocation_id,
                )
            )
            artifacts = (*artifacts, *recovery_artifacts)
            recovery_state_json = _text(
                self.serializer.dumps_unchecked(persisted_recovery)
            )
            recovery_sequence = envelope.recovery_snapshot.through_sequence
        return _PersistenceItem(
            kind="event",
            session_id=envelope.session_id,
            invocation_id=envelope.invocation_id,
            record={
                "event_id": event.id,
                "sequence": event.sequence,
                "schema_version": event.schema_version,
                "event_type": event.event_type,
                "event_name": event.event_name,
                "subject_type": event.subject_type,
                "subject_id": event.subject_id,
                "occurred_at_ms": event.occurred_at_ms,
                "elapsed_ns": event.elapsed_ns,
                "status": event.status,
                "timing_json": _text(
                    self.serializer.dumps_unchecked(event.timing)
                ),
                "input_json": input_json,
                "output_json": output_json,
                "operations_json": operations_json,
                "recovery_state_json": recovery_state_json,
                "recovery_sequence": recovery_sequence,
                "invocation_state": envelope.invocation_state,
                "execution_mode": envelope.execution_mode,
                "updated_at_ms": envelope.invocation_updated_at_ms,
                "invocation_result_json": (
                    None
                    if persisted_result is None
                    else _text(self.serializer.dumps(persisted_result))
                ),
                "invocation_error_json": (
                    None
                    if envelope.invocation_error is None
                    else _text(self.serializer.dumps(envelope.invocation_error))
                ),
                "session_updated_at_ms": envelope.session_updated_at_ms,
                "force_recovery_state": envelope.force_recovery_checkpoint,
            },
            encoded=encoded_payload,
            artifacts=artifacts,
            size_bytes=(
                len(encoded_payload)
                + sum(
                    len(value.encode("utf-8"))
                    for value in (input_json, output_json, operations_json)
                    if value is not None
                )
                + (
                    len(recovery_state_json.encode("utf-8"))
                    if recovery_state_json is not None
                    else 0
                )
                + sum(artifact.size_bytes for artifact in artifacts)
                + 256
            ),
            coordinator_id=envelope.id,
        )

    def _prepare_invocation_state_item(
        self,
        envelope: PersistenceEnvelope,
    ) -> _PersistenceItem:
        if (
            envelope.session_id is None
            or envelope.invocation_id is None
            or envelope.session_record is None
            or envelope.invocation_record is None
        ):
            raise ValueError("Invocation state persistence envelope is incomplete.")
        persisted_input, input_artifacts = self.artifact_encoder.externalize(
            envelope.invocation_record.get("input"),
            invocation_id=envelope.invocation_id,
        )
        persisted_result, result_artifacts = self.artifact_encoder.externalize(
            envelope.invocation_record.get("result"),
            invocation_id=envelope.invocation_id,
        )
        record = {
            "session": envelope.session_record,
            "invocation": {
                **envelope.invocation_record,
                "input_json": _text(self.serializer.dumps(persisted_input)),
                "result_json": (
                    None
                    if persisted_result is None
                    else _text(self.serializer.dumps(persisted_result))
                ),
                "error_json": (
                    None
                    if envelope.invocation_record.get("error") is None
                    else _text(
                        self.serializer.dumps(
                            envelope.invocation_record["error"]
                        )
                    )
                ),
            },
        }
        return _PersistenceItem(
            kind="invocation_state",
            session_id=envelope.session_id,
            invocation_id=envelope.invocation_id,
            record=record,
            encoded=None,
            artifacts=(*input_artifacts, *result_artifacts),
            size_bytes=(
                envelope.estimated_bytes
                + sum(
                    value.size_bytes
                    for value in (*input_artifacts, *result_artifacts)
                )
            ),
            coordinator_id=envelope.id,
        )

    def _prepare_user_event_batch_item(
        self,
        envelope: PersistenceEnvelope,
    ) -> _PersistenceItem:
        if (
            envelope.session_id is None
            or envelope.invocation_id is None
            or not envelope.user_events
        ):
            raise ValueError("UserEvent persistence envelope is incomplete.")
        records: list[dict[str, Any]] = []
        artifacts: list[EncodedArtifact] = []
        encoded_bytes = 0
        for event_value in envelope.user_events:
            persisted_data, event_artifacts = self.artifact_encoder.externalize(
                event_value.data,
                invocation_id=envelope.invocation_id,
            )
            data_json = _text(self.serializer.dumps(persisted_data))
            encoded_bytes += len(data_json.encode("utf-8"))
            artifacts.extend(event_artifacts)
            records.append(
                {
                    "id": str(event_value.id),
                    "session_id": str(envelope.session_id),
                    "invocation_id": str(envelope.invocation_id),
                    "sequence": event_value.sequence,
                    "schema_version": event_value.schema_version,
                    "type": event_value.type,
                    "data_json": data_json,
                    "node_id": event_value.node_id,
                    "workflow_path_json": _text(
                        self.serializer.dumps_unchecked(
                            list(event_value.workflow_path)
                        )
                    ),
                    "node_execution_id": str(
                        event_value.node_execution_id
                    ),
                    "operator_call_id": (
                        None
                        if event_value.operator_call_id is None
                        else str(event_value.operator_call_id)
                    ),
                    "occurred_at_ms": event_value.occurred_at_ms,
                }
            )
        return _PersistenceItem(
            kind="user_event_batch",
            session_id=envelope.session_id,
            invocation_id=envelope.invocation_id,
            record={"events": records},
            encoded=None,
            artifacts=tuple(artifacts),
            size_bytes=(
                encoded_bytes
                + sum(artifact.size_bytes for artifact in artifacts)
                + 256 * len(records)
            ),
            coordinator_id=envelope.id,
        )

    def _externalize_admission_state(
        self,
        state: dict[str, Any],
        *,
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
                    invocation_id=invocation_id,
                )
                record[field] = persisted
                artifacts.extend(encoded)
        return result, tuple(artifacts)

    def _externalize_event_values(
        self,
        event: RuntimeEvent,
        *,
        invocation_id: UUID,
    ) -> tuple[dict[str, Any], tuple[EncodedArtifact, ...]]:
        """Externalize large Event values while preserving its typed envelope."""

        artifacts: list[EncodedArtifact] = []
        persisted_payload, encoded = self.artifact_encoder.externalize(
            event.payload,
            invocation_id=invocation_id,
        )
        artifacts.extend(encoded)
        persisted_input, encoded = self.artifact_encoder.externalize(
            event.input,
            invocation_id=invocation_id,
        )
        artifacts.extend(encoded)
        persisted_output, encoded = self.artifact_encoder.externalize(
            event.output,
            invocation_id=invocation_id,
        )
        artifacts.extend(encoded)
        operations: list[dict[str, Any]] | None = None
        if event.operations is not None:
            operations = []
            for raw_operation in event.operations:
                # Do not use model_dump() here. StateOperation.value is typed
                # as Any, so Pydantic would recursively turn nested Runtime
                # models into plain dictionaries before the Runtime serializer
                # has a chance to attach their trusted type identifiers.
                operation = {
                    "op": raw_operation.op,
                    "path": raw_operation.path,
                    "value": raw_operation.value,
                }
                if raw_operation.op != "remove":
                    persisted, encoded = self.artifact_encoder.externalize(
                        raw_operation.value,
                        invocation_id=invocation_id,
                    )
                    operation["value"] = persisted
                    artifacts.extend(encoded)
                operations.append(operation)
        return {
            "payload": persisted_payload,
            "input": persisted_input,
            "output": persisted_output,
            "operations": operations,
        }, tuple(artifacts)

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
            recovery_row = await database.get(
                RecoveryStateRow,
                str(invocation_id),
            )
        if row is None:
            return None
        use_recovery = at_or_before_sequence is None and recovery_row is not None
        encoded_state = (
            recovery_row.state_json
            if use_recovery
            else row.genesis_state_json
        )
        if encoded_state is None:
            return None
        sequence = recovery_row.event_sequence if use_recovery else 0
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
            created_at_ms=(
                recovery_row.updated_at_ms
                if use_recovery and recovery_row is not None
                else row.created_at_ms
            ),
        )

    async def aload_trace_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None = None,
    ) -> ExecutionSnapshot | None:
        """Load a type-neutral snapshot for observation and UI replay."""

        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.aload_trace_execution_snapshot(
                    invocation_id,
                    at_or_before_sequence=at_or_before_sequence,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            row = await database.get(InvocationRow, str(invocation_id))
            recovery_row = await database.get(
                RecoveryStateRow,
                str(invocation_id),
            )
        if row is None:
            return None
        use_recovery = (
            recovery_row is not None
            and (
                at_or_before_sequence is None
                or recovery_row.event_sequence <= at_or_before_sequence
            )
        )
        encoded_state = (
            recovery_row.state_json
            if use_recovery and recovery_row is not None
            else row.genesis_state_json
        )
        if encoded_state is None:
            return None
        return ExecutionSnapshot(
            invocation_id=invocation_id,
            through_sequence=(
                recovery_row.event_sequence
                if use_recovery and recovery_row is not None
                else 0
            ),
            state=self.serializer.json_view(encoded_state),
            created_at_ms=(
                recovery_row.updated_at_ms
                if use_recovery and recovery_row is not None
                else row.created_at_ms
            ),
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
        encoded_values = [
            {
                "payload": self.serializer.loads(row.payload_json),
                "input": (
                    None
                    if row.input_json is None
                    else self.serializer.loads(row.input_json)
                ),
                "output": (
                    None
                    if row.output_json is None
                    else self.serializer.loads(row.output_json)
                ),
                "operations": (
                    None
                    if row.operations_json is None
                    else self.serializer.loads(row.operations_json)
                ),
            }
            for row in rows
        ]
        values = await self._hydrate_runtime_values(encoded_values)
        events = tuple(
            RuntimeEvent(
                id=UUID(row.id),
                invocation_id=invocation_id,
                sequence=row.sequence,
                schema_version=row.schema_version,
                event_type=row.event_type,
                event_name=row.event_name,
                subject_type=row.subject_type,
                subject_id=row.subject_id,
                occurred_at_ms=row.occurred_at_ms,
                elapsed_ns=row.elapsed_ns,
                status=row.status,
                timing=self.serializer.loads(row.timing_json),
                payload=value["payload"],
                input=value["input"],
                output=value["output"],
                operations=value["operations"],
            )
            for row, value in zip(rows, values, strict=True)
        )
        if events:
            self.coordinator.remember_durable(
                invocation_id,
                events[-1].sequence,
            )
        return events

    async def alist_trace_runtime_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
    ) -> tuple[RuntimeEvent, ...]:
        """Load type-neutral Events for tracing without runtime registrations."""

        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_trace_runtime_events(
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
        return tuple(
            RuntimeEvent(
                id=UUID(row.id),
                invocation_id=invocation_id,
                sequence=row.sequence,
                schema_version=row.schema_version,
                event_type=row.event_type,
                event_name=row.event_name,
                subject_type=row.subject_type,
                subject_id=row.subject_id,
                occurred_at_ms=row.occurred_at_ms,
                elapsed_ns=row.elapsed_ns,
                status=row.status,
                timing=self.serializer.json_view(row.timing_json),
                payload=self.serializer.json_view(row.payload_json),
                input=(
                    None
                    if row.input_json is None
                    else self.serializer.json_view(row.input_json)
                ),
                output=(
                    None
                    if row.output_json is None
                    else self.serializer.json_view(row.output_json)
                ),
                operations=(
                    None
                    if row.operations_json is None
                    else self.serializer.json_view(row.operations_json)
                ),
            )
            for row in rows
        )

    async def alist_user_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int = 0,
        limit: int = 200,
    ) -> tuple[UserEvent, ...]:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_user_events(
                    invocation_id=invocation_id,
                    after_sequence=after_sequence,
                    limit=limit,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            rows = (
                await database.scalars(
                    select(UserEventRow)
                    .where(
                        UserEventRow.invocation_id == str(invocation_id),
                        UserEventRow.sequence > after_sequence,
                    )
                    .order_by(UserEventRow.sequence)
                    .limit(limit)
                )
            ).all()
        data_values = await self._hydrate_runtime_values(
            [self.serializer.loads(row.data_json) for row in rows]
        )
        events = tuple(
            UserEvent(
                id=UUID(row.id),
                invocation_id=invocation_id,
                sequence=row.sequence,
                schema_version=row.schema_version,
                type=row.type,
                data=data,
                node_id=row.node_id,
                workflow_path=tuple(
                    str(value)
                    for value in self.serializer.json_view(
                        row.workflow_path_json
                    )
                ),
                node_execution_id=UUID(row.node_execution_id),
                operator_call_id=(
                    None
                    if row.operator_call_id is None
                    else UUID(row.operator_call_id)
                ),
                occurred_at_ms=row.occurred_at_ms,
            )
            for row, data in zip(rows, data_values, strict=True)
        )
        if events:
            self.coordinator.remember_user_events_durable(
                invocation_id,
                events[-1].sequence,
            )
        return events

    async def alatest_user_event_sequence(
        self,
        invocation_id: UUID,
    ) -> int:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alatest_user_event_sequence(invocation_id)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            value = await database.scalar(
                select(func.max(UserEventRow.sequence)).where(
                    UserEventRow.invocation_id == str(invocation_id)
                )
            )
        sequence = int(value or 0)
        self.coordinator.remember_user_events_durable(
            invocation_id,
            sequence,
        )
        return sequence

    async def alist_trace_workflow_versions(
        self,
        *,
        limit: int = 500,
        before: tuple[int, str] | None = None,
        workflow_id: str | None = None,
    ) -> tuple[tuple[str, WorkflowVersionSnapshot, int], ...]:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_trace_workflow_versions(
                    limit=limit,
                    before=before,
                    workflow_id=workflow_id,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = select(WorkflowVersionRow)
            if workflow_id is not None:
                statement = statement.where(
                    WorkflowVersionRow.workflow_id == workflow_id,
                )
            if before is not None:
                statement = statement.where(
                    tuple_(
                        WorkflowVersionRow.created_at_ms,
                        WorkflowVersionRow.id,
                    )
                    < before
                )
            rows = (
                await database.scalars(
                    statement.order_by(
                        WorkflowVersionRow.created_at_ms.desc(),
                        WorkflowVersionRow.id.desc(),
                    )
                    .limit(limit)
                )
            ).all()
        return tuple(
            (
                row.id,
                WorkflowVersionSnapshot(
                    workflow_id=row.workflow_id,
                    workflow_version=row.workflow_version,
                    ir_version=row.ir_version,
                    compiler_version=row.compiler_version,
                    definition_hash=row.definition_hash,
                    definition=self.serializer.json_view(
                        row.definition_json
                    ),
                ),
                row.created_at_ms,
            )
            for row in rows
        )

    async def aload_trace_workflow_version(
        self,
        revision_id: str,
    ) -> tuple[str, WorkflowVersionSnapshot, int] | None:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.aload_trace_workflow_version(revision_id)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            row = await database.get(WorkflowVersionRow, revision_id)
        if row is None:
            return None
        return (
            row.id,
            WorkflowVersionSnapshot(
                workflow_id=row.workflow_id,
                workflow_version=row.workflow_version,
                ir_version=row.ir_version,
                compiler_version=row.compiler_version,
                definition_hash=row.definition_hash,
                definition=self.serializer.json_view(row.definition_json),
            ),
            row.created_at_ms,
        )

    async def aload_trace_workflow_versions(
        self,
        revision_ids: tuple[str, ...],
    ) -> tuple[tuple[str, WorkflowVersionSnapshot, int], ...]:
        if not revision_ids:
            return ()
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.aload_trace_workflow_versions(revision_ids)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            rows = (
                await database.scalars(
                    select(WorkflowVersionRow).where(
                        WorkflowVersionRow.id.in_(revision_ids)
                    )
                )
            ).all()
        return tuple(
            (
                row.id,
                WorkflowVersionSnapshot(
                    workflow_id=row.workflow_id,
                    workflow_version=row.workflow_version,
                    ir_version=row.ir_version,
                    compiler_version=row.compiler_version,
                    definition_hash=row.definition_hash,
                    definition=self.serializer.json_view(
                        row.definition_json
                    ),
                ),
                row.created_at_ms,
            )
            for row in rows
        )

    async def alist_trace_sessions(
        self,
        *,
        workflow_revision_id: str,
        limit: int = 500,
        before: tuple[int, str] | None = None,
    ) -> tuple[dict[str, Any], ...]:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_trace_sessions(
                    workflow_revision_id=workflow_revision_id,
                    limit=limit,
                    before=before,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = select(SessionRow).where(
                SessionRow.workflow_revision_id == workflow_revision_id,
            )
            if before is not None:
                statement = statement.where(
                    tuple_(SessionRow.updated_at_ms, SessionRow.id) < before
                )
            rows = (
                await database.scalars(
                    statement.order_by(
                        SessionRow.updated_at_ms.desc(),
                        SessionRow.id.desc(),
                    )
                    .limit(limit)
                )
            ).all()
            current_ids = tuple(
                row.current_invocation_id
                for row in rows
                if row.current_invocation_id is not None
            )
            current_rows = (
                (
                    await database.scalars(
                        select(InvocationRow).where(
                            InvocationRow.id.in_(current_ids)
                        )
                    )
                ).all()
                if current_ids
                else ()
            )
            count_rows = (
                (
                    await database.execute(
                        select(
                            InvocationRow.session_id,
                            func.count(InvocationRow.id),
                        )
                        .where(
                            InvocationRow.session_id.in_(
                                tuple(row.id for row in rows)
                            )
                        )
                        .group_by(InvocationRow.session_id)
                    )
                ).all()
                if rows
                else ()
            )
        states = {row.id: row.state for row in current_rows}
        invocation_counts = {
            str(session_id): int(count)
            for session_id, count in count_rows
        }
        return tuple(
            {
                "id": row.id,
                "workflow_id": row.workflow_id,
                "workflow_revision_id": row.workflow_revision_id,
                "session_key": row.session_key,
                "current_invocation_id": row.current_invocation_id,
                "current_invocation_state": (
                    states.get(row.current_invocation_id)
                    if row.current_invocation_id is not None
                    else None
                ),
                "invocation_count": invocation_counts.get(row.id, 0),
                "created_at_ms": row.created_at_ms,
                "updated_at_ms": row.updated_at_ms,
            }
            for row in rows
        )

    async def alist_trace_invocations(
        self,
        *,
        session_id: UUID,
        limit: int = 500,
        before: tuple[int, str] | None = None,
        after: tuple[int, str] | None = None,
    ) -> tuple[dict[str, Any], ...]:
        if before is not None and after is not None:
            raise ValueError("Invocation lookup accepts before or after, not both.")
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_trace_invocations(
                    session_id=session_id,
                    limit=limit,
                    before=before,
                    after=after,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = (
                select(InvocationRow, WorkflowVersionRow)
                .join(
                    WorkflowVersionRow,
                    WorkflowVersionRow.id
                    == InvocationRow.workflow_version_id,
                )
                .where(InvocationRow.session_id == str(session_id))
            )
            if before is not None:
                statement = statement.where(
                    tuple_(
                        InvocationRow.created_at_ms,
                        InvocationRow.id,
                    ) < before
                )
            if after is not None:
                statement = statement.where(
                    tuple_(
                        InvocationRow.created_at_ms,
                        InvocationRow.id,
                    ) > after
                )
            order = (
                (
                    InvocationRow.created_at_ms.asc(),
                    InvocationRow.id.asc(),
                )
                if after is not None
                else (
                    InvocationRow.created_at_ms.desc(),
                    InvocationRow.id.desc(),
                )
            )
            rows = (
                await database.execute(
                    statement.order_by(*order).limit(limit)
                )
            ).all()
        return tuple(
            self._trace_invocation_record(
                invocation,
                workflow,
                include_values=False,
            )
            for invocation, workflow in rows
        )

    async def aload_trace_invocation(
        self,
        invocation_id: UUID,
    ) -> dict[str, Any] | None:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.aload_trace_invocation(invocation_id)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            row = (
                await database.execute(
                    select(InvocationRow, WorkflowVersionRow, SessionRow)
                    .join(
                        WorkflowVersionRow,
                        WorkflowVersionRow.id
                        == InvocationRow.workflow_version_id,
                    )
                    .join(SessionRow, SessionRow.id == InvocationRow.session_id)
                    .where(InvocationRow.id == str(invocation_id))
                )
            ).first()
        if row is None:
            return None
        invocation, workflow, session = row
        return {
            **self._trace_invocation_record(
                invocation,
                workflow,
                include_values=True,
            ),
            "session": {
                "id": session.id,
                "workflow_id": session.workflow_id,
                "workflow_revision_id": session.workflow_revision_id,
                "session_key": session.session_key,
                "current_invocation_id": session.current_invocation_id,
                "current_invocation_state": invocation.state,
                "created_at_ms": session.created_at_ms,
                "updated_at_ms": session.updated_at_ms,
            },
        }

    def _trace_invocation_record(
        self,
        invocation: InvocationRow,
        workflow: WorkflowVersionRow,
        *,
        include_values: bool,
    ) -> dict[str, Any]:
        record = {
            "id": invocation.id,
            "session_id": invocation.session_id,
            "workflow_id": workflow.workflow_id,
            "workflow_revision_id": workflow.id,
            "workflow_version": workflow.workflow_version,
            "definition_hash": workflow.definition_hash,
            "entry_node_id": invocation.entry_node_id,
            "state": invocation.state,
            "execution_mode": invocation.execution_mode,
            "event_mode": invocation.event_mode,
            "live_sequence": invocation.durable_sequence,
            "durable_sequence": invocation.durable_sequence,
            "persistence_status": "durable",
            "created_at_ms": invocation.created_at_ms,
            "updated_at_ms": invocation.updated_at_ms,
        }
        if include_values:
            record.update(
                {
                    "input": self.serializer.json_view(
                        invocation.input_json
                    ),
                    "result": (
                        None
                        if invocation.result_json is None
                        else self.serializer.json_view(
                            invocation.result_json
                        )
                    ),
                    "error": (
                        None
                        if invocation.error_json is None
                        else self.serializer.json_view(
                            invocation.error_json
                        )
                    ),
                }
            )
        return record

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
            self._database_loop.begin_compatibility_wait()
            try:
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
                            elif item.kind == "user_event_batch":
                                assert item.invocation_id is not None
                                assert item.coordinator_id is not None
                                if item.record.get("persistence_failed"):
                                    self.coordinator.discard(item.coordinator_id)
                                else:
                                    advanced.add(item.invocation_id)
                                    self.coordinator.mark_user_events_durable(
                                        item.coordinator_id,
                                        item.invocation_id,
                                        max(
                                            int(value["sequence"])
                                            for value in item.record["events"]
                                        ),
                                    )
                            elif item.kind == "admission":
                                assert item.invocation_id is not None
                                assert item.coordinator_id is not None
                                self.coordinator.mark_admission_durable(
                                    item.coordinator_id,
                                    item.invocation_id,
                                )
                            elif item.kind == "invocation_state":
                                assert item.invocation_id is not None
                                advanced.add(item.invocation_id)
                                assert item.coordinator_id is not None
                                self.coordinator.discard(item.coordinator_id)
                            else:
                                assert item.coordinator_id is not None
                                self.coordinator.discard(item.coordinator_id)
                            if item.done is not None and not item.done.done():
                                item.done.set_result(None)
                        for invocation_id in advanced:
                            self.store._persistence_advanced(invocation_id)
                        break
            finally:
                self._database_loop.end_compatibility_wait()
                with self._pressure_lock:
                    self._inflight_bytes -= batch_bytes
                    self._inflight_count -= len(control_items)

    def _import_incoming(self) -> None:
        for envelope in self.coordinator.take(self.batch_max_items):
            if (
                envelope.kind != "user_event_batch"
                and envelope.invocation_id is not None
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
                elif envelope.kind == "invocation_state":
                    item = self._prepare_invocation_state_item(envelope)
                elif envelope.kind == "user_event_batch":
                    item = self._prepare_user_event_batch_item(envelope)
                else:
                    item = self._prepare_event_item(envelope)
            except Exception as exc:
                self.coordinator.discard(envelope.id)
                if envelope.invocation_id is not None:
                    if envelope.kind == "user_event_batch":
                        sequence = (
                            envelope.user_events[0].sequence
                            if envelope.user_events
                            else 0
                        )
                        self.coordinator.degrade_user_events(
                            envelope.invocation_id,
                            sequence,
                            str(exc),
                        )
                    else:
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
                        self.store._notify_runtime_change(
                            envelope.invocation_id
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
        workflow_items = [
            item for item in batch if item.kind == "workflow_version"
        ]
        admission_items = [
            item for item in batch if item.kind == "admission"
        ]
        invocation_state_items = [
            item for item in batch if item.kind == "invocation_state"
        ]
        event_items = [item for item in batch if item.kind == "event"]
        user_event_items = [
            item for item in batch if item.kind == "user_event_batch"
        ]
        runtime_items = [
            item for item in batch if item.kind != "user_event_batch"
        ]
        if runtime_items:
            async with self._database_sessions.begin() as database:
                for item in workflow_items:
                    await self._persist_workflow_version(database, item.record)
                if workflow_items:
                    await database.flush()
                for item in admission_items:
                    await self._persist_admission(database, item)
                for item in invocation_state_items:
                    await self._persist_invocation_state(database, item)
                artifacts = tuple(
                    artifact
                    for item in runtime_items
                    for artifact in item.artifacts
                )
                if artifacts:
                    await self._persist_artifacts(database, artifacts)
                if event_items:
                    await self._persist_events(database, event_items)
        if user_event_items:
            try:
                async with self._database_sessions.begin() as database:
                    artifacts = tuple(
                        artifact
                        for item in user_event_items
                        for artifact in item.artifacts
                    )
                    if artifacts:
                        await self._persist_artifacts(database, artifacts)
                    await self._persist_user_events(database, user_event_items)
            except (OperationalError, DBAPIError):
                raise
            except Exception as exc:
                for item in user_event_items:
                    assert item.invocation_id is not None
                    item.record["persistence_failed"] = True
                    self.coordinator.degrade_user_events(
                        item.invocation_id,
                        min(
                            int(value["sequence"])
                            for value in item.record["events"]
                        ),
                        str(exc),
                    )
                logger.exception(
                    "UserEvent persistence failed; RuntimeEvent durability and "
                    "Workflow execution remain available."
                )

    async def _persist_workflow_version(self, database, record: dict[str, Any]) -> None:
        row = await database.scalar(
            select(WorkflowVersionRow).where(
                WorkflowVersionRow.workflow_id == record["workflow_id"],
                WorkflowVersionRow.definition_hash == record["definition_hash"],
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
            row.workflow_version = record["workflow_version"]
            row.ir_version = record["ir_version"]
            row.compiler_version = record["compiler_version"]
            row.definition_json = record["definition_json"]
            key = (
                row.workflow_id,
                row.definition_hash,
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
                WorkflowVersionRow.workflow_id == workflow_key[0],
                WorkflowVersionRow.definition_hash == workflow_key[1],
            )
        )
        if version_row is None:
            raise RuntimeError(
                "Invocation genesis references workflow metadata that is not "
                "durable yet."
            )
        version_id = UUID(version_row.id)
        if (
            str(version_id) != session["workflow_revision_id"]
            or str(version_id) != invocation["workflow_revision_id"]
        ):
            raise RuntimeError(
                "Session and Invocation workflow revision identities do not "
                "match the durable Workflow revision: "
                f"durable={version_id}, "
                f"session={session['workflow_revision_id']}, "
                f"invocation={invocation['workflow_revision_id']}."
            )
        bundle = self.serializer.loads(_text(item.encoded))
        state = bundle["state"]
        event_mode = invocation["event_mode"]
        session_row = await database.get(SessionRow, session["id"])
        session_values = {
            "workflow_id": session["workflow_id"],
            "workflow_revision_id": session["workflow_revision_id"],
            "session_key": session["session_key"],
            "current_invocation_id": invocation["id"],
            "context_json": _text(
                self.serializer.dumps_unchecked(bundle["session_context"])
            ),
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
            "event_mode": event_mode,
            "durable_sequence": 0,
            "input_json": _text(
                self.serializer.dumps_unchecked(bundle["input"])
            ),
            "result_json": None,
            "error_json": None,
            "genesis_state_json": (
                _text(self.serializer.dumps_unchecked(state))
                if event_mode == "full"
                else None
            ),
            "created_at_ms": invocation["created_at_ms"],
            "updated_at_ms": invocation["updated_at_ms"],
        }
        if invocation_row is None:
            database.add(InvocationRow(id=invocation["id"], **invocation_values))
        invocation_id = UUID(str(invocation["id"]))
        if event_mode != "minimal":
            if state is None:
                raise RuntimeError("Recoverable Invocation admission has no state.")
            recovery_row = await database.get(
                RecoveryStateRow,
                invocation["id"],
            )
            if recovery_row is None:
                database.add(RecoveryStateRow(
                    invocation_id=invocation["id"],
                    event_sequence=0,
                    state_json=_text(
                        self.serializer.dumps_unchecked(state)
                    ),
                    updated_at_ms=invocation["updated_at_ms"],
                ))
            self._durable_states[invocation_id] = state
            self._projection_sequences[invocation_id] = 0
            self._recovery_sequences[invocation_id] = 0

    async def _persist_invocation_state(
        self,
        database,
        item: _PersistenceItem,
    ) -> None:
        session = item.record["session"]
        invocation = item.record["invocation"]
        session_row = await database.get(SessionRow, str(item.session_id))
        invocation_row = await database.get(
            InvocationRow,
            str(item.invocation_id),
        )
        if session_row is None or invocation_row is None:
            raise RuntimeError("Invocation state references a missing admission.")
        session_row.current_invocation_id = session.get("current_invocation_id")
        session_row.updated_at_ms = int(session["updated_at_ms"])
        invocation_row.state = str(invocation["state"])
        invocation_row.input_json = str(invocation["input_json"])
        invocation_row.result_json = invocation["result_json"]
        invocation_row.error_json = invocation["error_json"]
        invocation_row.updated_at_ms = int(invocation["updated_at_ms"])

    async def _persist_user_events(
        self,
        database,
        items: list[_PersistenceItem],
    ) -> None:
        records = [
            record
            for item in items
            for record in item.record["events"]
        ]
        identities = [
            (record["invocation_id"], int(record["sequence"]))
            for record in records
        ]
        existing_rows = (
            await database.scalars(
                select(UserEventRow).where(
                    tuple_(
                        UserEventRow.invocation_id,
                        UserEventRow.sequence,
                    ).in_(identities)
                )
            )
        ).all()
        existing = {
            (row.invocation_id, row.sequence): row
            for row in existing_rows
        }
        fields = (
            "id",
            "session_id",
            "schema_version",
            "type",
            "data_json",
            "node_id",
            "workflow_path_json",
            "node_execution_id",
            "operator_call_id",
            "occurred_at_ms",
        )
        for record in records:
            identity = (
                record["invocation_id"],
                int(record["sequence"]),
            )
            row = existing.get(identity)
            if row is None:
                continue
            if any(getattr(row, field) != record[field] for field in fields):
                raise RuntimeError(
                    "UserEvent sequence already contains different data: "
                    f"invocation_id={identity[0]}, sequence={identity[1]}."
                )
        database.add_all(
            [
                UserEventRow(**record)
                for record in records
                if (
                    record["invocation_id"],
                    int(record["sequence"]),
                )
                not in existing
            ]
        )

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
                or row.event_type != item.record["event_type"]
                or row.event_name != item.record["event_name"]
                or row.subject_type != item.record["subject_type"]
                or row.subject_id != item.record["subject_id"]
                or row.occurred_at_ms != item.record["occurred_at_ms"]
                or row.elapsed_ns != item.record["elapsed_ns"]
                or row.status != item.record["status"]
                or row.timing_json != item.record["timing_json"]
                or row.payload_json != _text(item.encoded)
                or row.input_json != item.record["input_json"]
                or row.output_json != item.record["output_json"]
                or row.operations_json != item.record["operations_json"]
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
                    event_type=str(item.record["event_type"]),
                    event_name=str(item.record["event_name"]),
                    subject_type=str(item.record["subject_type"]),
                    subject_id=str(item.record["subject_id"]),
                    occurred_at_ms=int(item.record["occurred_at_ms"]),
                    elapsed_ns=item.record["elapsed_ns"],
                    status=item.record["status"],
                    timing_json=str(item.record["timing_json"]),
                    payload_json=_text(item.encoded),
                    input_json=item.record["input_json"],
                    output_json=item.record["output_json"],
                    operations_json=item.record["operations_json"],
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
            final_sequence = int(item.record["sequence"])
            previous_durable_sequence = row.durable_sequence
            row.state = str(item.record["invocation_state"])
            row.execution_mode = str(item.record["execution_mode"])
            row.result_json = item.record["invocation_result_json"]
            row.error_json = item.record["invocation_error_json"]
            row.updated_at_ms = int(item.record["updated_at_ms"])

            recovery_row = await database.get(
                RecoveryStateRow,
                invocation_id,
            )
            state: dict[str, Any] | None = None
            projection_sequence = (
                self._projection_sequences.get(UUID(invocation_id), 0)
            )
            if row.event_mode == "full":
                # Load only the state that was durable before this transaction.
                # New RuntimeEvent rows in ``ordered`` may already be visible
                # through the transaction's autoflush; advancing the row cursor
                # before loading would reduce those Events here and then apply
                # the same Operations again below.
                row.durable_sequence = previous_durable_sequence
                state = await self._load_projection_state(
                    database,
                    row,
                    recovery_row,
                )
                projection_sequence = self._projection_sequences.get(
                    UUID(invocation_id),
                    previous_durable_sequence,
                )
                for event_item in ordered:
                    sequence = int(event_item.record["sequence"])
                    if sequence <= projection_sequence:
                        continue
                    raw_operations = event_item.record["operations_json"]
                    if raw_operations is None:
                        raise RuntimeError(
                            "Full RuntimeEvent has no persisted StateOperations."
                        )
                    operations = tuple(
                        StateOperation.model_validate(operation)
                        for operation in self.serializer.loads(raw_operations)
                    )
                    state = apply_state_operations(state, operations)
                    projection_sequence = sequence
            row.durable_sequence = max(
                previous_durable_sequence,
                final_sequence,
            )

            checkpoint_item = max(
                (
                    value
                    for value in ordered
                    if value.record["recovery_state_json"] is not None
                ),
                key=lambda value: int(value.record["recovery_sequence"]),
                default=None,
            )
            recovery_sequence = (
                recovery_row.event_sequence
                if recovery_row is not None
                else 0
            )
            should_checkpoint_full = (
                row.event_mode == "full"
                and state is not None
                and final_sequence - recovery_sequence
                >= self.recovery_event_interval
            )
            if checkpoint_item is not None:
                checkpoint_sequence = int(
                    checkpoint_item.record["recovery_sequence"]
                )
                checkpoint_state_json = str(
                    checkpoint_item.record["recovery_state_json"]
                )
            elif should_checkpoint_full:
                checkpoint_sequence = final_sequence
                checkpoint_state_json = _text(
                    self.serializer.dumps_unchecked(state)
                )
            else:
                checkpoint_sequence = None
                checkpoint_state_json = None

            context_state = (
                self.serializer.loads(checkpoint_state_json)
                if checkpoint_state_json is not None
                else state
            )
            if context_state is not None:
                item.record["session_context_json"] = _text(
                    self.serializer.dumps_unchecked(
                        context_state["session"]["context"]
                    )
                )

            if checkpoint_sequence is not None and checkpoint_state_json is not None:
                if recovery_row is None:
                    recovery_row = RecoveryStateRow(
                        invocation_id=invocation_id,
                        event_sequence=checkpoint_sequence,
                        state_json=checkpoint_state_json,
                        updated_at_ms=int(item.record["updated_at_ms"]),
                    )
                    database.add(recovery_row)
                else:
                    recovery_row.event_sequence = checkpoint_sequence
                    recovery_row.state_json = checkpoint_state_json
                    recovery_row.updated_at_ms = int(item.record["updated_at_ms"])
                self._recovery_sequences[UUID(invocation_id)] = checkpoint_sequence

            if state is not None:
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
                context_json = final_by_session[row.id].record.get(
                    "session_context_json"
                )
                if context_json is not None:
                    row.context_json = str(context_json)

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
        recovery_row: RecoveryStateRow | None = None,
    ) -> dict[str, Any]:
        invocation_id = UUID(row.id)
        cached = self._durable_states.get(invocation_id)
        if (
            cached is not None
            and self._projection_sequences.get(invocation_id)
            == row.durable_sequence
        ):
            return cached

        if recovery_row is None:
            recovery_row = await database.get(RecoveryStateRow, row.id)
        if recovery_row is not None:
            state = self.serializer.loads(recovery_row.state_json)
            cursor = recovery_row.event_sequence
        else:
            if row.genesis_state_json is None:
                raise RuntimeError(
                    "Invocation cannot rebuild Runtime State in this Event mode."
                )
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
                if event_row.operations_json is None:
                    raise RuntimeError(
                        "RuntimeEvent tail cannot rebuild Runtime State."
                    )
                operations = tuple(
                    StateOperation.model_validate(operation)
                    for operation in self.serializer.loads(
                        event_row.operations_json
                    )
                )
                state = apply_state_operations(state, operations)
                cursor = event_row.sequence
        self._durable_states[invocation_id] = state
        self._projection_sequences[invocation_id] = cursor
        self._recovery_sequences[invocation_id] = int(
            recovery_row.event_sequence if recovery_row is not None else 0
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
        logger.error(
            "Database persistence is unavailable; Workflow execution remains "
            "available in memory and new submission will be limited by "
            "persistence backlog: error_type=%s error=%r",
            type(error).__name__,
            error,
            exc_info=True,
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
        raise ValueError(
            "Database URL strings require an explicit scheme. Use "
            "'sqlite:///path/to/runtime.db', a PostgreSQL URL, or pass a "
            "path through DatabaseBackend.from_path()."
        )
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


def _ensure_sqlite_parent_directory(database_url: str) -> None:
    """Create the parent of a regular SQLite file before opening it."""

    url = make_url(database_url)
    if url.get_backend_name() != "sqlite":
        return
    database = url.database
    if (
        database is None
        or database == ":memory:"
        or database.startswith("file:")
    ):
        return
    Path(database).expanduser().parent.mkdir(parents=True, exist_ok=True)
