from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from autoagent.core.compiler import WorkflowVersionSnapshot
from autoagent.core.runtime.database_models import (
    InvocationRow,
    RuntimeDatabaseBase,
    RuntimeEventRow,
    RuntimeProjectionCheckpointRow,
    RuntimeSnapshotRow,
    SessionRow,
    WorkflowVersionRow,
)
from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.hooks import RuntimeEventLoop
from autoagent.core.runtime.serialization import JsonRuntimeSerializer
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.snapshot import ExecutionSnapshot
from autoagent.core.runtime.store import InMemoryRuntimeStore
from autoagent.core.runtime.time import utc_timestamp_ms


@dataclass
class _PersistenceCommand:
    kind: str
    payload: Any
    done: asyncio.Future[None] | None = None


class DatabaseRuntimeStore(InMemoryRuntimeStore):
    """In-memory Runtime center with an asynchronous SQL durable backend.

    Executor-facing calls first update the inherited in-memory journal and then
    enqueue ordered persistence commands. SQLite and PostgreSQL use the same
    SQLAlchemy schema and behavior; only their async drivers differ. Ordinary
    boundaries never wait for database latency. Explicit durability barriers
    wait until the queue cursor reaches the submitted command.
    """

    def __init__(
        self,
        database_url: str | Path,
        *,
        serializer: JsonRuntimeSerializer | None = None,
        event_sinks=(),
        echo: bool = False,
        queue_high_watermark: int = 10_000,
        queue_low_watermark: int | None = None,
        snapshot_interval: int = 50,
    ) -> None:
        super().__init__(serializer=serializer, event_sinks=event_sinks)
        if queue_high_watermark < 1:
            raise ValueError("queue_high_watermark must be positive.")
        self.database_url = _resolve_database_url(database_url)
        # NullPool keeps read-side queries usable from a server loop while the
        # App-owned Runtime loop owns persistence. No async DB connection is
        # ever reused across event loops.
        self.engine: AsyncEngine = create_async_engine(
            self.database_url,
            echo=echo,
            poolclass=NullPool,
        )
        self._database_sessions = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        self.queue_high_watermark = queue_high_watermark
        self.queue_low_watermark = (
            max(0, queue_high_watermark // 2)
            if queue_low_watermark is None
            else queue_low_watermark
        )
        if self.queue_low_watermark >= self.queue_high_watermark:
            raise ValueError("queue_low_watermark must be below the high watermark.")
        self.snapshot_interval = max(1, snapshot_interval)
        self._persistence_queue: asyncio.Queue[_PersistenceCommand | None] = (
            asyncio.Queue()
        )
        self._worker: asyncio.Task[None] | None = None
        self._heartbeat: asyncio.Task[None] | None = None
        self._initialize_lock = asyncio.Lock()
        self._initialized = False
        self._closing = False
        self._admission_paused = False
        self._persistence_error: BaseException | None = None
        self._inflight_persistence = 0
        self._database_loop = RuntimeEventLoop(name="autoagent-database-runtime")

    @classmethod
    def from_path(cls, path: str | Path, **kwargs: Any) -> "DatabaseRuntimeStore":
        return cls(Path(path), **kwargs)

    @property
    def pending_persistence_count(self) -> int:
        return self._persistence_queue.qsize() + self._inflight_persistence

    async def ainitialize(self) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(self.ainitialize())
            return
        if self._initialized:
            self._ensure_worker()
            return
        async with self._initialize_lock:
            if not self._initialized:
                self._ensure_worker()
                async with self.engine.begin() as connection:
                    await connection.run_sync(RuntimeDatabaseBase.metadata.create_all)
                self._initialized = True
            self._ensure_worker()

    async def initialize(self) -> None:
        await self.ainitialize()

    async def aclose(self) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(self.aclose())
            self._database_loop.stop()
            return
        if self._closing:
            return
        self._closing = True
        if self._worker is not None:
            await self.aflush()
            await self._persistence_queue.put(None)
            await self._worker
            self._worker = None
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            await asyncio.gather(self._heartbeat, return_exceptions=True)
            self._heartbeat = None
        await self.engine.dispose()
        self._initialized = False

    async def close(self) -> None:
        await self.aclose()

    async def aflush(self) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(self.aflush())
            return
        await self.ainitialize()
        await self._persistence_queue.join()
        if self._persistence_error is not None:
            raise RuntimeError("Runtime persistence worker failed.") from self._persistence_error

    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        await self.ainitialize()
        await super().asave_workflow_snapshot(namespace, snapshot)
        await self._enqueue("workflow", (namespace, snapshot))

    async def asave_execution_snapshot(
        self,
        snapshot: ExecutionSnapshot,
        *,
        durability_barrier: bool = False,
    ) -> None:
        await self.ainitialize()
        await super().asave_execution_snapshot(snapshot)
        await self._enqueue("snapshot", snapshot, barrier=durability_barrier)

    async def aadmit_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> Session:
        await self.ainitialize()
        self._refresh_backpressure()
        if self._admission_paused:
            raise RuntimeError(
                "Runtime persistence backlog reached its high watermark; "
                "new Invocations are temporarily unavailable."
            )
        before = len(self.invocation_runtime_events.get(invocation.id, ()))
        session = await super().aadmit_invocation(session_id, invocation)
        await self._enqueue_new_events(invocation.id, before)
        await self._enqueue_state(session, invocation, barrier=True)
        return session

    async def asave_session(self, session: Session) -> None:
        await self.ainitialize()
        await super().asave_session(session)
        await self._enqueue("session", session.to_record())
        for invocation in session.invocations:
            await self._enqueue_state(session, invocation)

    async def asave_session_context(self, session: Session) -> None:
        await self.ainitialize()
        await super().asave_session_context(session)
        await self._enqueue("session", session.to_record())

    async def asave_invocation(self, session_id: UUID, invocation: Invocation) -> None:
        await self.ainitialize()
        before = len(self.invocation_runtime_events.get(invocation.id, ()))
        await super().asave_invocation(session_id, invocation)
        await self._enqueue_new_events(invocation.id, before)
        session = await super().aload_session(session_id)
        if session is None:
            raise KeyError(f"Unknown session: {session_id}")
        await self._enqueue_state(session, invocation)

    async def aapply_event(
        self,
        session: Session,
        invocation: Invocation,
        event: RuntimeEvent,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
        durability_barrier: bool = False,
    ) -> RuntimeEvent:
        before = len(self.invocation_runtime_events.get(invocation.id, ()))
        applied = await super().aapply_event(
            session,
            invocation,
            event,
            node_execution_ids=node_execution_ids,
        )
        await self._enqueue_new_events(invocation.id, before)
        await self._enqueue_state(session, invocation)
        if durability_barrier:
            await self.aflush()
        return applied

    async def aload_invocation(self, invocation_id: UUID) -> Invocation | None:
        value = await super().aload_invocation(invocation_id)
        if value is not None:
            return value
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.aload_invocation(invocation_id)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            row = await database.get(InvocationRow, str(invocation_id))
            if row is None:
                return None
            state = self._load(row.state_json)
        session, invocation = _restore_materialized_state(state)
        self._hydrate_state(session, invocation)
        await self._load_events_into_memory(invocation.id)
        return invocation

    async def aload_session(self, session_id: UUID) -> Session | None:
        value = await super().aload_session(session_id)
        if value is not None:
            return value
        if not self._database_loop.is_current():
            return await self._database_loop.arun(self.aload_session(session_id))
        await self.ainitialize()
        async with self._database_sessions() as database:
            row = await database.get(SessionRow, str(session_id))
            if row is None:
                return None
            invocation_rows = (
                await database.scalars(
                    select(InvocationRow)
                    .where(InvocationRow.session_id == str(session_id))
                    .order_by(InvocationRow.created_at_ms)
                )
            ).all()
        invocations: list[Invocation] = []
        session: Session | None = None
        for invocation_row in invocation_rows:
            restored_session, invocation = _restore_materialized_state(
                self._load(invocation_row.state_json)
            )
            session = restored_session
            invocations.append(invocation)
        if session is None:
            session = Session.from_record(self._load(row.state_json))
        session.invocations = invocations
        with self._lock:
            self.sessions[session.id] = session.to_record()
            self.session_keys[(session.namespace, session.workflow_id, session.session_key)] = session.id
            self.session_invocations[session.id] = []
            for invocation in invocations:
                self._hydrate_state(session, invocation)
        return session

    async def afind_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        value = await super().afind_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
        )
        if value is not None:
            return value
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
        return await self.aload_session(UUID(row.id)) if row is not None else None

    async def aget_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        if session_key is not None:
            existing = await self.afind_session(
                namespace=namespace,
                workflow_id=workflow_id,
                session_key=session_key,
            )
            if existing is not None:
                return existing
        session = await super().aget_or_create_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
        )
        await self._enqueue("session", session.to_record(), barrier=True)
        return session

    async def aclaim_waiting_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
        wait_key: str,
        workflow_definition_hash: str | None = None,
        workflow_operator_manifest_hash: str | None = None,
    ) -> Session:
        session = await self.afind_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
        )
        if session is None:
            raise KeyError(f"Unknown session: {session_key}")
        invocation = session.get_current_invocation()
        if invocation is not None:
            await self._load_events_into_memory(invocation.id)
        before = (
            len(self.invocation_runtime_events.get(invocation.id, ()))
            if invocation is not None
            else 0
        )
        claimed = await super().aclaim_waiting_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
            wait_key=wait_key,
            workflow_definition_hash=workflow_definition_hash,
            workflow_operator_manifest_hash=workflow_operator_manifest_hash,
        )
        invocation = claimed.get_current_invocation()
        if invocation is None:
            raise RuntimeError("Claimed Session has no current Invocation.")
        await self._enqueue_new_events(invocation.id, before)
        await self._enqueue_state(claimed, invocation, barrier=True)
        return claimed

    async def aload_workflow_snapshot(self, **kwargs: Any) -> WorkflowVersionSnapshot | None:
        value = await super().aload_workflow_snapshot(**kwargs)
        if value is not None:
            return value
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.aload_workflow_snapshot(**kwargs)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = select(WorkflowVersionRow).where(
                WorkflowVersionRow.namespace == kwargs["namespace"],
                WorkflowVersionRow.workflow_id == kwargs["workflow_id"],
                WorkflowVersionRow.definition_hash == kwargs["definition_hash"],
            )
            operator_hash = kwargs.get("operator_manifest_hash")
            if operator_hash is not None:
                statement = statement.where(
                    WorkflowVersionRow.operator_manifest_hash == operator_hash
                )
            rows = (await database.scalars(statement)).all()
        if len(rows) > 1:
            raise ValueError("operator_manifest_hash is required for this Workflow version.")
        if not rows:
            return None
        snapshot = WorkflowVersionSnapshot.model_validate(self._load(rows[0].snapshot_json))
        self.save_workflow_snapshot(kwargs["namespace"], snapshot)
        return snapshot

    async def alist_runtime_events(self, **kwargs: Any) -> tuple[RuntimeEvent, ...]:
        invocation_id = kwargs.get("invocation_id")
        if invocation_id is not None and invocation_id not in self.invocation_runtime_events:
            await self._load_events_into_memory(invocation_id)
        return await super().alist_runtime_events(**kwargs)

    async def alist_workflow_snapshots(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[WorkflowVersionSnapshot, ...]:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_workflow_snapshots(
                    namespace=namespace,
                    workflow_id=workflow_id,
                )
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = select(WorkflowVersionRow)
            if namespace is not None:
                statement = statement.where(WorkflowVersionRow.namespace == namespace)
            if workflow_id is not None:
                statement = statement.where(WorkflowVersionRow.workflow_id == workflow_id)
            rows = (await database.scalars(statement)).all()
        for row in rows:
            snapshot = WorkflowVersionSnapshot.model_validate(
                self._load(row.snapshot_json)
            )
            self.save_workflow_snapshot(row.namespace, snapshot)
        return await super().alist_workflow_snapshots(
            namespace=namespace,
            workflow_id=workflow_id,
        )

    async def alist_sessions(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[Session, ...]:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_sessions(namespace=namespace, workflow_id=workflow_id)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = select(SessionRow)
            if namespace is not None:
                statement = statement.where(SessionRow.namespace == namespace)
            if workflow_id is not None:
                statement = statement.where(SessionRow.workflow_id == workflow_id)
            rows = (await database.scalars(statement)).all()
        values = [
            session
            for row in rows
            if (session := await self.aload_session(UUID(row.id))) is not None
        ]
        values.sort(key=lambda item: (item.created_at_ms, str(item.id)))
        return tuple(values)

    async def alist_session_invocations(
        self,
        session_id: UUID,
    ) -> tuple[Invocation, ...]:
        session = await self.aload_session(session_id)
        return session.list_invocations() if session is not None else ()

    async def alist_active_invocations(self) -> tuple[Invocation, ...]:
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.alist_active_invocations()
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            rows = (
                await database.scalars(
                    select(InvocationRow).where(
                        InvocationRow.state.in_(("created", "running", "waiting"))
                    )
                )
            ).all()
        values = [
            invocation
            for row in rows
            if (invocation := await self.aload_invocation(UUID(row.id))) is not None
        ]
        return tuple(values)

    async def aload_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None = None,
    ) -> ExecutionSnapshot | None:
        value = await super().aload_execution_snapshot(
            invocation_id,
            at_or_before_sequence=at_or_before_sequence,
        )
        if value is not None:
            return value
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
                statement.order_by(RuntimeSnapshotRow.through_sequence.desc()).limit(1)
            )
        if row is None:
            return None
        snapshot = ExecutionSnapshot.model_validate(self._load(row.snapshot_json))
        await super().asave_execution_snapshot(snapshot)
        return snapshot

    async def asave_projection_checkpoint(self, **kwargs: Any) -> None:
        await super().asave_projection_checkpoint(**kwargs)
        await self._enqueue("projection", kwargs)

    async def aload_projection_checkpoint(self, **kwargs: Any):
        value = await super().aload_projection_checkpoint(**kwargs)
        if value is not None:
            return value
        if not self._database_loop.is_current():
            return await self._database_loop.arun(
                self.aload_projection_checkpoint(**kwargs)
            )
        await self.ainitialize()
        async with self._database_sessions() as database:
            statement = select(RuntimeProjectionCheckpointRow).where(
                RuntimeProjectionCheckpointRow.invocation_id
                == str(kwargs["invocation_id"])
            )
            cursor = kwargs.get("at_or_before_sequence")
            if cursor is not None:
                statement = statement.where(
                    RuntimeProjectionCheckpointRow.through_sequence <= cursor
                )
            row = await database.scalar(
                statement.order_by(
                    RuntimeProjectionCheckpointRow.through_sequence.desc()
                ).limit(1)
            )
        if row is None:
            return None
        return row.through_sequence, self._load(row.projection_json)

    async def _enqueue_state(
        self,
        session: Session,
        invocation: Invocation,
        *,
        barrier: bool = False,
    ) -> None:
        from autoagent.core.runtime.snapshot import capture_execution_state

        await self._enqueue(
            "state",
            capture_execution_state(session, invocation),
            barrier=barrier,
        )

    async def _enqueue_new_events(self, invocation_id: UUID, start: int) -> None:
        with self._lock:
            event_ids = tuple(
                self.invocation_runtime_events.get(invocation_id, ())[start:]
            )
            events = tuple(
                RuntimeEvent.model_validate(self.runtime_events[event_id])
                for event_id in event_ids
            )
        for event in events:
            await self._enqueue("event", event)

    async def _enqueue(self, kind: str, payload: Any, *, barrier: bool = False) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(
                self._enqueue(kind, payload, barrier=barrier)
            )
            return
        await self.ainitialize()
        done = asyncio.get_running_loop().create_future() if barrier else None
        await self._persistence_queue.put(_PersistenceCommand(kind, payload, done))
        self._refresh_backpressure()
        if done is not None:
            await done

    def _ensure_worker(self) -> None:
        if self._heartbeat is None or self._heartbeat.done():
            self._heartbeat = asyncio.create_task(
                self._poll_cross_thread_completions(),
                name="autoagent-database-heartbeat",
            )
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(
                self._persistence_loop(),
                name="autoagent-runtime-persistence",
            )

    async def _poll_cross_thread_completions(self) -> None:
        # aiosqlite and some async PostgreSQL transports complete work from a
        # helper thread. Restricted hosts may not wake asyncio's self-pipe, so
        # keep the selector timeout bounded without coupling SQL to execution.
        while True:
            await asyncio.sleep(0.001)

    async def _persistence_loop(self) -> None:
        while True:
            command = await self._persistence_queue.get()
            if command is None:
                self._persistence_queue.task_done()
                return
            self._inflight_persistence = 1
            retry_delay = 0.05
            while True:
                try:
                    await self._persist(command)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    # Preserve the head command and ordering during a backend
                    # outage. Queue growth drives admission backpressure while
                    # reads continue from the memory center.
                    self._persistence_error = exc
                    self._admission_paused = True
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(5.0, retry_delay * 2)
                    continue
                self._persistence_error = None
                if command.done is not None and not command.done.done():
                    command.done.set_result(None)
                self._inflight_persistence = 0
                self._persistence_queue.task_done()
                self._refresh_backpressure()
                break

    async def _persist(self, command: _PersistenceCommand) -> None:
        async with self._database_sessions.begin() as database:
            if command.kind == "workflow":
                namespace, snapshot = command.payload
                key = (namespace, snapshot.workflow_id, snapshot.definition_hash, snapshot.operator_manifest_hash)
                row = await database.scalar(select(WorkflowVersionRow).where(
                    WorkflowVersionRow.namespace == key[0],
                    WorkflowVersionRow.workflow_id == key[1],
                    WorkflowVersionRow.definition_hash == key[2],
                    WorkflowVersionRow.operator_manifest_hash == key[3],
                ))
                if row is None:
                    database.add(WorkflowVersionRow(
                        id=str(uuid4()), namespace=namespace,
                        workflow_id=snapshot.workflow_id,
                        definition_hash=snapshot.definition_hash,
                        operator_manifest_hash=snapshot.operator_manifest_hash,
                        snapshot_json=self._dump(snapshot.model_dump(mode="python")),
                        created_at_ms=utc_timestamp_ms(),
                    ))
            elif command.kind == "session":
                await self._persist_session(database, command.payload)
            elif command.kind == "state":
                state = command.payload
                await self._persist_session(database, state["session"])
                await self._persist_invocation(database, state)
            elif command.kind == "event":
                event: RuntimeEvent = command.payload
                if await database.get(RuntimeEventRow, str(event.id)) is None:
                    database.add(RuntimeEventRow(
                        id=str(event.id), namespace=event.namespace,
                        workflow_id=event.workflow_id, session_id=str(event.session_id),
                        invocation_id=str(event.invocation_id), sequence=event.sequence,
                        schema_version=event.schema_version, role=event.role,
                        boundary=event.boundary, commit_id=str(event.commit_id),
                        type=event.type, entity_type=event.entity_type,
                        occurred_at_ms=event.occurred_at_ms,
                        event_json=self._dump(event.model_dump(mode="python")),
                    ))
            elif command.kind == "snapshot":
                snapshot: ExecutionSnapshot = command.payload
                row = await database.scalar(select(RuntimeSnapshotRow).where(
                    RuntimeSnapshotRow.invocation_id == str(snapshot.invocation_id),
                    RuntimeSnapshotRow.through_sequence == snapshot.through_sequence,
                ))
                if row is None:
                    database.add(RuntimeSnapshotRow(
                        id=str(snapshot.id), namespace=snapshot.namespace,
                        workflow_id=snapshot.workflow_id,
                        session_id=str(snapshot.session_id),
                        invocation_id=str(snapshot.invocation_id),
                        through_sequence=snapshot.through_sequence,
                        snapshot_json=self._dump(snapshot.model_dump(mode="python")),
                        created_at_ms=snapshot.created_at_ms,
                    ))
            elif command.kind == "projection":
                payload = command.payload
                invocation_id = str(payload["invocation_id"])
                sequence = payload["through_sequence"]
                row = await database.scalar(select(RuntimeProjectionCheckpointRow).where(
                    RuntimeProjectionCheckpointRow.invocation_id == invocation_id,
                    RuntimeProjectionCheckpointRow.through_sequence == sequence,
                ))
                if row is None:
                    database.add(RuntimeProjectionCheckpointRow(
                        id=str(uuid4()), invocation_id=invocation_id,
                        through_sequence=sequence,
                        projection_json=self._dump(payload["projection"]),
                        created_at_ms=utc_timestamp_ms(),
                    ))
                else:
                    row.projection_json = self._dump(payload["projection"])

    async def _persist_session(self, database, record: dict[str, Any]) -> None:
        row = await database.get(SessionRow, str(record["id"]))
        values = dict(
            namespace=record["namespace"], workflow_id=record["workflow_id"],
            session_key=record.get("session_key"), state_json=self._dump(record),
            current_invocation_id=record.get("current_invocation_id"),
            created_at_ms=record["created_at_ms"], updated_at_ms=record["updated_at_ms"],
        )
        if row is None:
            database.add(SessionRow(id=str(record["id"]), **values))
        else:
            for name, value in values.items():
                setattr(row, name, value)

    async def _persist_invocation(self, database, state: dict[str, Any]) -> None:
        record = state["invocation"]
        row = await database.get(InvocationRow, str(record["id"]))
        values = dict(
            session_id=record["session_id"], workflow_id=record["workflow_id"],
            state=record["state"], execution_mode=record.get("execution_mode", "normal"),
            head_sequence=record.get("event_sequence", 0),
            durable_sequence=record.get("event_sequence", 0),
            state_json=self._dump(state), created_at_ms=record["created_at_ms"],
            updated_at_ms=record["updated_at_ms"],
        )
        if row is None:
            database.add(InvocationRow(id=str(record["id"]), **values))
        else:
            for name, value in values.items():
                setattr(row, name, value)

    async def _load_events_into_memory(self, invocation_id: UUID) -> None:
        if not self._database_loop.is_current():
            await self._database_loop.arun(
                self._load_events_into_memory(invocation_id)
            )
            return
        await self.ainitialize()
        async with self._database_sessions() as database:
            rows = (await database.scalars(
                select(RuntimeEventRow)
                .where(RuntimeEventRow.invocation_id == str(invocation_id))
                .order_by(RuntimeEventRow.sequence)
            )).all()
        with self._lock:
            ids = self.invocation_runtime_events.setdefault(invocation_id, [])
            for row in rows:
                event = RuntimeEvent.model_validate(self._load(row.event_json))
                self.runtime_events[event.id] = event.model_dump(mode="python")
                if event.id not in ids:
                    ids.append(event.id)

    def _hydrate_state(self, session: Session, invocation: Invocation) -> None:
        """Populate the memory center without synthesizing new RuntimeEvents."""

        with self._lock:
            self.sessions[session.id] = session.to_record()
            self.session_keys[
                (session.namespace, session.workflow_id, session.session_key)
            ] = session.id
            invocation_ids = self.session_invocations.setdefault(session.id, [])
            if invocation.id not in invocation_ids:
                invocation_ids.append(invocation.id)
            self.invocations[invocation.id] = invocation.to_record(session.id)
            self.invocation_node_executions[invocation.id] = []
            for execution in invocation.node_executions:
                self._save_node_execution(invocation.id, execution)

    def _refresh_backpressure(self) -> None:
        size = self.pending_persistence_count
        if size >= self.queue_high_watermark:
            self._admission_paused = True
        elif size <= self.queue_low_watermark and self._persistence_error is None:
            self._admission_paused = False

    def _dump(self, value: Any) -> str:
        return self.serializer.dumps(value)

    def _load(self, value: str) -> Any:
        return self.serializer.loads(value)


def _restore_materialized_state(state: dict[str, Any]) -> tuple[Session, Invocation]:
    from autoagent.core.runtime.snapshot import restore_execution_state

    return restore_execution_state(state)


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
        raise ValueError("V1 DatabaseRuntimeStore supports SQLite and PostgreSQL only.")
    return text
