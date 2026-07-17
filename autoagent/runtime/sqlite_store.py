from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, event, func, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from autoagent.compiler import WorkflowVersionSnapshot
from autoagent.runtime.database_models import (
    InvocationRow,
    NodeExecutionRow,
    OperatorCallRow,
    RuntimeProjectionCheckpointRow,
    RuntimeEventRow,
    RuntimeDatabaseBase,
    SessionRow,
    WorkflowVersionRow,
)
from autoagent.runtime.event import (
    RuntimeEvent,
    RuntimeEventDraft,
    invocation_checkpoint_events,
    operator_call_checkpoint_events,
    session_context_event,
    sort_runtime_event_drafts,
)
from autoagent.runtime.execution import NodeExecution, OperatorCall
from autoagent.runtime.invocation import Invocation
from autoagent.runtime.serialization import JsonRuntimeSerializer
from autoagent.runtime.session import Session
from autoagent.runtime.store import RuntimeStore, SessionBusyError
from autoagent.runtime.time import utc_timestamp_ms


RUNTIME_SCHEMA_REVISION = "0003_projection_checkpoints"


class SQLiteRuntimeStore(RuntimeStore):
    """Async SQLAlchemy RuntimeStore backed by SQLite.

    Each public operation opens a short AsyncSession and commits one transaction.
    Execution checkpoints update the Invocation control row, SessionContext,
    explicitly changed NodeExecutions, and generated Runtime Events in one
    transaction. Historical NodeExecutions are not rewritten on every loop turn.

    ``initialize`` creates and stamps a new embedded database at the current
    Alembic revision. Existing databases at an older revision are rejected so
    applications must run ``alembic upgrade head`` instead of silently operating
    against a partially compatible schema.
    """

    def __init__(
        self,
        database_url: str | Path,
        *,
        serializer: JsonRuntimeSerializer | None = None,
        echo: bool = False,
    ) -> None:
        resolved_url = _resolve_database_url(database_url)
        self.engine: AsyncEngine = create_async_engine(resolved_url, echo=echo)
        self._sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.serializer = serializer or JsonRuntimeSerializer()
        self._initialized = False
        self._initialize_lock = asyncio.Lock()
        # Session admission and wait claiming are compare-and-change operations.
        # Serialize them within one Store instance; the database's unique active
        # Invocation index remains the final integrity guard.
        self._admission_lock = asyncio.Lock()
        # Session event sequences are allocated transactionally after materialized
        # state changes. One Store instance serializes those allocations; the
        # database uniqueness constraint remains the cross-process guard.
        self._event_lock = asyncio.Lock()
        event.listen(self.engine.sync_engine, "connect", _enable_sqlite_foreign_keys)

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        *,
        serializer: JsonRuntimeSerializer | None = None,
        echo: bool = False,
    ) -> SQLiteRuntimeStore:
        resolved = Path(path).expanduser().resolve()
        return cls(
            f"sqlite+aiosqlite:///{resolved}",
            serializer=serializer,
            echo=echo,
        )

    async def initialize(self) -> None:
        """Create a missing schema once; safe to call before every App startup."""

        if self._initialized:
            return
        async with self._initialize_lock:
            if self._initialized:
                return
            async with self.engine.begin() as connection:
                await connection.execute(
                    text(
                        "CREATE TABLE IF NOT EXISTS alembic_version "
                        "(version_num VARCHAR(32) NOT NULL PRIMARY KEY)"
                    )
                )
                revision = await connection.scalar(
                    text("SELECT version_num FROM alembic_version LIMIT 1")
                )
                if revision is not None and revision != RUNTIME_SCHEMA_REVISION:
                    raise RuntimeError(
                        "Runtime database schema is not current: "
                        f"found {revision}, expected {RUNTIME_SCHEMA_REVISION}. "
                        "Run 'uv run alembic upgrade head'."
                    )
                await connection.run_sync(RuntimeDatabaseBase.metadata.create_all)
                if revision is None:
                    await connection.execute(
                        text(
                            "INSERT INTO alembic_version (version_num) VALUES (:revision)"
                        ),
                        {"revision": RUNTIME_SCHEMA_REVISION},
                    )
            self._initialized = True

    async def ainitialize(self) -> None:
        """RuntimeStore lifecycle alias used by AutoAgentApp."""

        await self.initialize()

    async def close(self) -> None:
        """Release pooled connections after an application or test shuts down."""

        await self.engine.dispose()
        self._initialized = False

    async def aclose(self) -> None:
        """RuntimeStore lifecycle alias used by AutoAgentApp."""

        await self.close()

    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        await self.initialize()
        async with (
            self._admission_lock,
            self._event_lock,
            self._sessions.begin() as database,
        ):
            existing = await database.scalar(
                select(WorkflowVersionRow).where(
                    WorkflowVersionRow.namespace == namespace,
                    WorkflowVersionRow.workflow_id == snapshot.workflow_id,
                    WorkflowVersionRow.definition_hash == snapshot.definition_hash,
                    WorkflowVersionRow.operator_manifest_hash
                    == snapshot.operator_manifest_hash,
                )
            )
            snapshot_json = self._dump(snapshot.model_dump(mode="python"))
            if existing is not None:
                if existing.snapshot_json != snapshot_json:
                    raise ValueError(
                        "Workflow snapshot identity collision: "
                        f"{snapshot.workflow_id}/{snapshot.definition_hash}"
                    )
                return
            database.add(
                WorkflowVersionRow(
                    id=str(uuid4()),
                    namespace=namespace,
                    workflow_id=snapshot.workflow_id,
                    workflow_version_json=self._dump(snapshot.workflow_version),
                    definition_hash=snapshot.definition_hash,
                    operator_manifest_hash=snapshot.operator_manifest_hash,
                    ir_version=snapshot.ir_version,
                    compiler_version=snapshot.compiler_version,
                    snapshot_json=snapshot_json,
                    created_at_ms=utc_timestamp_ms(),
                )
            )

    async def aload_workflow_snapshot(
        self,
        *,
        namespace: str,
        workflow_id: str,
        definition_hash: str,
        operator_manifest_hash: str | None = None,
    ) -> WorkflowVersionSnapshot | None:
        await self.initialize()
        async with self._sessions() as database:
            statement = select(WorkflowVersionRow).where(
                WorkflowVersionRow.namespace == namespace,
                WorkflowVersionRow.workflow_id == workflow_id,
                WorkflowVersionRow.definition_hash == definition_hash,
            )
            if operator_manifest_hash is not None:
                statement = statement.where(
                    WorkflowVersionRow.operator_manifest_hash
                    == operator_manifest_hash
                )
            rows = (await database.scalars(statement)).all()
            if len(rows) > 1:
                raise ValueError(
                    "operator_manifest_hash is required when a Workflow "
                    "definition has multiple Operator environments."
                )
            row = rows[0] if rows else None
            return (
                WorkflowVersionSnapshot.model_validate(self._load(row.snapshot_json))
                if row is not None
                else None
            )

    async def asave_session(self, session: Session) -> None:
        await self.initialize()
        async with self._event_lock, self._sessions.begin() as database:
            await self._upsert_session(database, session)
            for invocation in session.invocations:
                await self._upsert_invocation(database, session.id, invocation)

    async def asave_session_context(self, session: Session) -> None:
        await self.initialize()
        async with self._event_lock, self._sessions.begin() as database:
            row = await database.get(SessionRow, str(session.id))
            if row is None:
                raise KeyError(f"Unknown session: {session.id}")
            previous_context = self._load(row.context_json)
            row.context_json = self._dump(session.context.to_record())
            row.current_invocation_id = (
                str(session.current_invocation_id)
                if session.current_invocation_id is not None
                else None
            )
            row.updated_at_ms = session.updated_at_ms
            if (
                previous_context != session.context.to_record()
                and session.current_invocation_id is not None
            ):
                await self._append_event_drafts(
                    database,
                    session_id=session.id,
                    invocation_id=session.current_invocation_id,
                    drafts=(
                        session_context_event(
                            session_id=session.id,
                            invocation_id=session.current_invocation_id,
                            context=session.context.to_record(),
                            occurred_at_ms=session.updated_at_ms,
                        ),
                    ),
                )

    async def aload_session(self, session_id: UUID) -> Session | None:
        await self.initialize()
        async with self._sessions() as database:
            return await self._load_session(database, session_id)

    async def aget_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        await self.initialize()
        async with (
            self._admission_lock,
            self._event_lock,
            self._sessions.begin() as database,
        ):
            statement = select(SessionRow).where(
                SessionRow.namespace == namespace,
                SessionRow.workflow_id == workflow_id,
            )
            statement = (
                statement.where(SessionRow.session_key.is_(None))
                if session_key is None
                else statement.where(SessionRow.session_key == session_key)
            )
            row = await database.scalar(statement)
            if row is None:
                created = Session(
                    namespace=namespace,
                    workflow_id=workflow_id,
                    session_key=session_key,
                )
                await self._upsert_session(database, created)
                session_id = created.id
            else:
                session_id = UUID(row.id)
        loaded = await self.aload_session(session_id)
        if loaded is None:  # pragma: no cover - transaction/database corruption guard.
            raise RuntimeError("Session disappeared after creation.")
        return loaded

    async def afind_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        await self.initialize()
        async with self._sessions() as database:
            session_id = await database.scalar(
                select(SessionRow.id).where(
                    SessionRow.namespace == namespace,
                    SessionRow.workflow_id == workflow_id,
                    SessionRow.session_key == session_key,
                )
            )
        return await self.aload_session(UUID(session_id)) if session_id else None

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
        """Atomically change waiting to running so only one resume can win."""

        await self.initialize()
        async with self._admission_lock, self._sessions.begin() as database:
            session_row = await database.scalar(
                select(SessionRow).where(
                    SessionRow.namespace == namespace,
                    SessionRow.workflow_id == workflow_id,
                    SessionRow.session_key == session_key,
                )
            )
            if session_row is None:
                raise KeyError(f"Unknown session: {session_key}")
            if session_row.current_invocation_id is None:
                raise ValueError("Session does not have a current Invocation.")
            invocation_row = await database.get(
                InvocationRow,
                session_row.current_invocation_id,
            )
            if invocation_row is None:
                raise ValueError("Session current Invocation does not exist.")
            if invocation_row.state != "waiting":
                invocation = await self._load_invocation(database, UUID(invocation_row.id))
                if invocation is None:
                    raise ValueError("Session current Invocation does not exist.")
                session = await self._load_session(database, UUID(session_row.id))
                assert session is not None
                if invocation_row.state in {"created", "running"}:
                    raise SessionBusyError(session, invocation)
                raise ValueError("Session does not have a waiting Invocation.")
            if (
                workflow_definition_hash is not None
                and invocation_row.definition_hash != workflow_definition_hash
            ):
                raise ValueError(
                    "Waiting Invocation belongs to a different Workflow definition."
                )
            if (
                workflow_operator_manifest_hash is not None
                and invocation_row.operator_manifest_hash
                != workflow_operator_manifest_hash
            ):
                raise ValueError(
                    "Waiting Invocation belongs to a different Operator manifest "
                    "environment."
                )
            scheduler = self._load(invocation_row.scheduler_json)
            if wait_key not in scheduler.get("waiting_executions", {}):
                raise KeyError(f"Unknown wait key: {wait_key}")
            session_id = UUID(session_row.id)
            invocation = await self._load_invocation(database, UUID(invocation_row.id))
            if invocation is None:  # pragma: no cover - guarded by row lookup.
                raise ValueError("Session current Invocation does not exist.")
            invocation.mark_running()
            await self._upsert_invocation(database, session_id, invocation)
        claimed = await self.aload_session(session_id)
        if claimed is None:  # pragma: no cover
            raise RuntimeError("Claimed Session disappeared.")
        return claimed

    async def asave_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> None:
        await self.initialize()
        async with self._event_lock, self._sessions.begin() as database:
            if await database.get(SessionRow, str(session_id)) is None:
                raise KeyError(f"Unknown session: {session_id}")
            await self._upsert_invocation(database, session_id, invocation)

    async def acheckpoint_invocation(
        self,
        session: Session,
        invocation: Invocation,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
    ) -> None:
        """Persist one execution delta without rewriting historical children."""

        await self.initialize()
        async with self._event_lock, self._sessions.begin() as database:
            session_row = await database.get(SessionRow, str(session.id))
            if session_row is None:
                raise KeyError(f"Unknown session: {session.id}")
            invocation_row = await database.get(InvocationRow, str(invocation.id))
            if invocation_row is None:
                raise KeyError(f"Unknown invocation: {invocation.id}")
            if invocation_row.session_id != str(session.id):
                raise ValueError("Invocation does not belong to the supplied session.")

            previous = await self._load_invocation(database, invocation.id)
            if previous is None:  # pragma: no cover - guarded by row lookup.
                raise KeyError(f"Unknown invocation: {invocation.id}")
            previous_session_context = self._load(session_row.context_json)

            record = invocation.to_record(session.id)
            _assign(
                invocation_row,
                {
                    "state": invocation.state,
                    "context_json": self._dump(record["context"]),
                    "result_json": self._dump(record["result"]),
                    "scheduler_json": self._dump(record["scheduler"]),
                    "error_json": self._dump(record["error"]),
                    "updated_at_ms": invocation.updated_at_ms,
                },
            )

            changed_ids = tuple(dict.fromkeys(node_execution_ids))
            for execution_id in changed_ids:
                execution = invocation.get_node_execution(execution_id)
                if execution is None:
                    raise KeyError(f"Unknown NodeExecution: {execution_id}")
                await self._upsert_node_execution(database, invocation.id, execution)

            session_row.context_json = self._dump(session.context.to_record())
            session_row.current_invocation_id = (
                str(session.current_invocation_id)
                if session.current_invocation_id is not None
                else None
            )
            session_row.updated_at_ms = session.updated_at_ms

            drafts = invocation_checkpoint_events(
                previous,
                invocation,
                changed_node_execution_ids=frozenset(map(str, changed_ids)),
            )
            if previous_session_context != session.context.to_record():
                drafts.append(
                    session_context_event(
                        session_id=session.id,
                        invocation_id=invocation.id,
                        context=session.context.to_record(),
                        occurred_at_ms=session.updated_at_ms,
                    )
                )
            sort_runtime_event_drafts(drafts)
            await self._append_event_drafts(
                database,
                session_id=session.id,
                invocation_id=invocation.id,
                drafts=tuple(drafts),
            )

    async def acheckpoint_operator_call(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        node_execution_id: UUID,
        node_id: str,
        call: OperatorCall,
    ) -> None:
        """Persist call start/completion before returning to execution code."""

        await self.initialize()
        async with self._event_lock, self._sessions.begin() as database:
            invocation_row = await database.get(InvocationRow, str(invocation_id))
            if invocation_row is None:
                raise KeyError(f"Unknown invocation: {invocation_id}")
            if invocation_row.session_id != str(session_id):
                raise ValueError("Invocation does not belong to the supplied session.")
            execution_row = await database.get(
                NodeExecutionRow,
                str(node_execution_id),
            )
            if execution_row is None:
                raise KeyError(f"Unknown NodeExecution: {node_execution_id}")
            if execution_row.invocation_id != str(invocation_id):
                raise ValueError("NodeExecution does not belong to the Invocation.")

            previous_row = await database.get(OperatorCallRow, str(call.id))
            previous = (
                self._operator_call_from_row(previous_row)
                if previous_row is not None
                else None
            )
            await self._upsert_operator_call(database, node_execution_id, call)
            await self._append_event_drafts(
                database,
                session_id=session_id,
                invocation_id=invocation_id,
                drafts=tuple(
                    operator_call_checkpoint_events(
                        previous,
                        call,
                        node_id=node_id,
                    )
                ),
            )

    async def aadmit_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> Session:
        await self.initialize()
        async with (
            self._admission_lock,
            self._event_lock,
            self._sessions.begin() as database,
        ):
            session_row = await database.get(SessionRow, str(session_id))
            if session_row is None:
                raise KeyError(f"Unknown session: {session_id}")
            if session_row.current_invocation_id is not None:
                current_row = await database.get(
                    InvocationRow,
                    session_row.current_invocation_id,
                )
                if current_row is not None and current_row.state in {
                    "created",
                    "running",
                    "waiting",
                }:
                    current = await self._load_invocation(database, UUID(current_row.id))
                    session = await self._load_session(database, session_id)
                    assert current is not None and session is not None
                    raise SessionBusyError(session, current)
            await self._upsert_invocation(database, session_id, invocation)
            session_row.current_invocation_id = str(invocation.id)
            session_row.updated_at_ms = utc_timestamp_ms()
        admitted = await self.aload_session(session_id)
        if admitted is None:  # pragma: no cover
            raise RuntimeError("Admitted Session disappeared.")
        return admitted

    async def aload_invocation(self, invocation_id: UUID) -> Invocation | None:
        await self.initialize()
        async with self._sessions() as database:
            return await self._load_invocation(database, invocation_id)

    async def alist_active_invocations(self) -> tuple[Invocation, ...]:
        await self.initialize()
        async with self._sessions() as database:
            ids = (
                await database.scalars(
                    select(InvocationRow.id)
                    .where(InvocationRow.state.in_(("created", "running", "waiting")))
                    .order_by(InvocationRow.created_at_ms)
                )
            ).all()
            values = [
                invocation
                for raw_id in ids
                if (invocation := await self._load_invocation(database, UUID(raw_id)))
                is not None
            ]
            return tuple(values)

    async def arecover_interrupted_invocations(self) -> tuple[Invocation, ...]:
        """Force persisted created/running work to terminal interrupted.

        RuntimeStore cannot decide replay compatibility because it does not own
        the current Workflow and Operator registries. Normal applications do not
        call this method: AutoAgentApp performs lazy compatibility checks and
        delegates recoverable work to WorkflowExecutor. Waiting invocations are
        intentionally left resumable.

        TODO: move this operation out of the public RuntimeStore contract when a
        dedicated maintenance API is introduced.
        """

        await self.initialize()
        recovered: list[Invocation] = []
        async with self._event_lock, self._sessions.begin() as database:
            ids = (
                await database.scalars(
                    select(InvocationRow.id).where(
                        InvocationRow.state.in_(("created", "running"))
                    )
                )
            ).all()
            for raw_id in ids:
                invocation = await self._load_invocation(database, UUID(raw_id))
                if invocation is None:
                    continue
                invocation.recover_interrupted_executions()
                if invocation.state != "interrupted":
                    invocation.mark_interrupted()
                session_id = UUID(
                    (await database.get(InvocationRow, raw_id)).session_id  # type: ignore[union-attr]
                )
                await self._upsert_invocation(database, session_id, invocation)
                recovered.append(invocation)
        return tuple(recovered)

    async def alist_workflow_snapshots(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[WorkflowVersionSnapshot, ...]:
        await self.initialize()
        async with self._sessions() as database:
            statement = select(WorkflowVersionRow).order_by(
                WorkflowVersionRow.workflow_id,
                WorkflowVersionRow.created_at_ms,
            )
            if namespace is not None:
                statement = statement.where(WorkflowVersionRow.namespace == namespace)
            if workflow_id is not None:
                statement = statement.where(
                    WorkflowVersionRow.workflow_id == workflow_id
                )
            rows = (await database.scalars(statement)).all()
            return tuple(
                WorkflowVersionSnapshot.model_validate(
                    self.serializer.json_view(row.snapshot_json)
                )
                for row in rows
            )

    async def alist_sessions(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[Session, ...]:
        await self.initialize()
        async with self._sessions() as database:
            statement = select(SessionRow.id).order_by(
                SessionRow.created_at_ms,
                SessionRow.id,
            )
            if namespace is not None:
                statement = statement.where(SessionRow.namespace == namespace)
            if workflow_id is not None:
                statement = statement.where(SessionRow.workflow_id == workflow_id)
            ids = (await database.scalars(statement)).all()
            values = [
                session
                for raw_id in ids
                if (session := await self._load_session(database, UUID(raw_id)))
                is not None
            ]
            return tuple(values)

    async def alist_session_invocations(
        self,
        session_id: UUID,
    ) -> tuple[Invocation, ...]:
        await self.initialize()
        async with self._sessions() as database:
            ids = (
                await database.scalars(
                    select(InvocationRow.id)
                    .where(InvocationRow.session_id == str(session_id))
                    .order_by(InvocationRow.created_at_ms, InvocationRow.id)
                )
            ).all()
            values = [
                invocation
                for raw_id in ids
                if (
                    invocation := await self._load_invocation(database, UUID(raw_id))
                )
                is not None
            ]
            return tuple(values)

    async def alist_runtime_events(
        self,
        *,
        session_id: UUID | None = None,
        invocation_id: UUID | None = None,
        after_sequence: int = 0,
        limit: int = 1000,
        visibility: str | None = None,
    ) -> tuple[RuntimeEvent, ...]:
        _validate_event_query(session_id, invocation_id, after_sequence, limit)
        await self.initialize()
        async with self._sessions() as database:
            statement = (
                select(RuntimeEventRow)
                .where(RuntimeEventRow.sequence > after_sequence)
                .order_by(RuntimeEventRow.sequence)
                .limit(limit)
            )
            if session_id is not None:
                statement = statement.where(
                    RuntimeEventRow.session_id == str(session_id)
                )
            if invocation_id is not None:
                statement = statement.where(
                    RuntimeEventRow.invocation_id == str(invocation_id)
                )
            if visibility is not None:
                statement = statement.where(RuntimeEventRow.visibility == visibility)
            rows = (await database.scalars(statement)).all()
            return tuple(self._runtime_event_from_row(row) for row in rows)

    async def aappend_runtime_events(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        drafts: tuple[RuntimeEventDraft, ...],
    ) -> tuple[RuntimeEvent, ...]:
        await self.initialize()
        async with self._event_lock, self._sessions.begin() as database:
            return await self._append_event_drafts(
                database,
                session_id=session_id,
                invocation_id=invocation_id,
                drafts=drafts,
            )

    async def asave_projection_checkpoint(
        self,
        *,
        invocation_id: UUID,
        through_sequence: int,
        projection: dict[str, Any],
    ) -> None:
        if through_sequence < 0:
            raise ValueError("through_sequence cannot be negative.")
        await self.initialize()
        async with self._sessions.begin() as database:
            if await database.get(InvocationRow, str(invocation_id)) is None:
                raise KeyError(f"Unknown invocation: {invocation_id}")
            existing = await database.scalar(
                select(RuntimeProjectionCheckpointRow).where(
                    RuntimeProjectionCheckpointRow.invocation_id
                    == str(invocation_id),
                    RuntimeProjectionCheckpointRow.through_sequence
                    == through_sequence,
                )
            )
            payload = self._dump(projection)
            if existing is None:
                database.add(
                    RuntimeProjectionCheckpointRow(
                        id=str(uuid4()),
                        invocation_id=str(invocation_id),
                        through_sequence=through_sequence,
                        projection_json=payload,
                        created_at_ms=utc_timestamp_ms(),
                    )
                )
            else:
                existing.projection_json = payload

    async def aload_projection_checkpoint(
        self,
        *,
        invocation_id: UUID,
        at_or_before_sequence: int | None = None,
    ) -> tuple[int, dict[str, Any]] | None:
        await self.initialize()
        async with self._sessions() as database:
            statement = (
                select(RuntimeProjectionCheckpointRow)
                .where(
                    RuntimeProjectionCheckpointRow.invocation_id
                    == str(invocation_id)
                )
                .order_by(RuntimeProjectionCheckpointRow.through_sequence.desc())
                .limit(1)
            )
            if at_or_before_sequence is not None:
                statement = statement.where(
                    RuntimeProjectionCheckpointRow.through_sequence
                    <= at_or_before_sequence
                )
            row = await database.scalar(statement)
            if row is None:
                return None
            return row.through_sequence, self._load(row.projection_json)

    async def _upsert_session(
        self,
        database: AsyncSession,
        session: Session,
    ) -> None:
        row = await database.get(SessionRow, str(session.id))
        values = {
            "namespace": session.namespace,
            "workflow_id": session.workflow_id,
            "session_key": session.session_key,
            "context_json": self._dump(session.context.to_record()),
            "current_invocation_id": (
                str(session.current_invocation_id)
                if session.current_invocation_id is not None
                else None
            ),
            "created_at_ms": session.created_at_ms,
            "updated_at_ms": session.updated_at_ms,
        }
        if row is None:
            database.add(SessionRow(id=str(session.id), **values))
            return
        _assign(row, values)

    async def _upsert_invocation(
        self,
        database: AsyncSession,
        session_id: UUID,
        invocation: Invocation,
    ) -> None:
        if not invocation.workflow_definition_hash:
            raise ValueError(
                "Durable Invocation requires workflow_definition_hash."
            )
        if not invocation.workflow_operator_manifest_hash:
            raise ValueError(
                "Durable Invocation requires workflow_operator_manifest_hash."
            )
        session_row = await database.get(SessionRow, str(session_id))
        if session_row is None:
            raise KeyError(f"Unknown session: {session_id}")
        row = await database.get(InvocationRow, str(invocation.id))
        previous = (
            await self._load_invocation(database, invocation.id)
            if row is not None
            else None
        )
        workflow_version_id = await database.scalar(
            select(WorkflowVersionRow.id).where(
                WorkflowVersionRow.namespace == session_row.namespace,
                WorkflowVersionRow.workflow_id == invocation.workflow_id,
                WorkflowVersionRow.definition_hash
                == invocation.workflow_definition_hash,
                WorkflowVersionRow.operator_manifest_hash
                == invocation.workflow_operator_manifest_hash,
            )
        )
        if workflow_version_id is None:
            raise ValueError(
                "WorkflowVersionSnapshot must be stored before its Invocation."
            )

        record = invocation.to_record(session_id)
        values = {
            "session_id": str(session_id),
            "workflow_version_id": workflow_version_id,
            "workflow_id": invocation.workflow_id,
            "workflow_version_json": self._dump(invocation.workflow_version),
            "definition_hash": invocation.workflow_definition_hash,
            "operator_manifest_hash": invocation.workflow_operator_manifest_hash,
            "entry_node_id": invocation.entry_node_id,
            "state": invocation.state,
            "input_json": self._dump(record["input"]),
            "context_json": self._dump(record["context"]),
            "result_json": self._dump(record["result"]),
            "scheduler_json": self._dump(record["scheduler"]),
            "error_json": self._dump(record["error"]),
            "created_at_ms": invocation.created_at_ms,
            "updated_at_ms": invocation.updated_at_ms,
        }
        if row is None:
            database.add(InvocationRow(id=str(invocation.id), **values))
            await database.flush()
        else:
            _assign(row, values)

        persisted_execution_ids: set[str] = set()
        for execution in invocation.node_executions:
            persisted_execution_ids.add(str(execution.id))
            await self._upsert_node_execution(database, invocation.id, execution)
        await self._delete_missing_children(
            database,
            NodeExecutionRow,
            NodeExecutionRow.invocation_id,
            str(invocation.id),
            persisted_execution_ids,
        )
        await self._append_event_drafts(
            database,
            session_id=session_id,
            invocation_id=invocation.id,
            drafts=tuple(invocation_checkpoint_events(previous, invocation)),
        )

    async def _upsert_node_execution(
        self,
        database: AsyncSession,
        invocation_id: UUID,
        execution: NodeExecution,
    ) -> None:
        record = execution.to_record(invocation_id)
        row = await database.get(NodeExecutionRow, str(execution.id))
        values = {
            "invocation_id": str(invocation_id),
            "node_id": execution.node_id,
            "sequence": execution.sequence,
            "state": execution.state,
            "input_json": self._dump(record["input"]),
            "output_json": self._dump(record["output"]),
            "error_json": self._dump(record["error"]),
            "idempotency_key": execution.idempotency_key,
            "recovery_of_execution_id": (
                str(execution.recovery_of_execution_id)
                if execution.recovery_of_execution_id is not None
                else None
            ),
            "recovery_attempt": execution.recovery_attempt,
            "incoming_activations_json": self._dump(record["incoming_activations"]),
            "edge_evaluations_json": self._dump(record["edge_evaluations"]),
            "resource_usage_json": self._dump(record["resource_usage"]),
            "started_at_ms": execution.started_at_ms,
            "ended_at_ms": execution.ended_at_ms,
            "created_at_ms": execution.created_at_ms,
            "updated_at_ms": execution.updated_at_ms,
        }
        if row is None:
            database.add(NodeExecutionRow(id=str(execution.id), **values))
            await database.flush()
        else:
            _assign(row, values)

        persisted_call_ids: set[str] = set()
        for call in execution.operator_calls:
            persisted_call_ids.add(str(call.id))
            await self._upsert_operator_call(database, execution.id, call)
        await self._delete_missing_children(
            database,
            OperatorCallRow,
            OperatorCallRow.node_execution_id,
            str(execution.id),
            persisted_call_ids,
        )

    async def _upsert_operator_call(
        self,
        database: AsyncSession,
        node_execution_id: UUID,
        call: OperatorCall,
    ) -> None:
        record = call.to_record(node_execution_id)
        manifest = call.operator_manifest
        row = await database.get(OperatorCallRow, str(call.id))
        values = {
            "node_execution_id": str(node_execution_id),
            "operator_id": call.operator_id,
            "operator_version_json": self._dump(
                manifest.version if manifest is not None else None
            ),
            "manifest_hash": manifest.manifest_hash if manifest is not None else None,
            "operator_manifest_json": self._dump(record["operator_manifest"]),
            "call_no": call.call_no,
            "kind": call.kind,
            "item_index": call.item_index,
            "replica_index": call.replica_index,
            "state": call.state,
            "input_json": self._dump(record["input"]),
            "output_json": self._dump(record["output"]),
            "error_json": self._dump(record["error"]),
            "resource_usage_json": self._dump(record["resource_usage"]),
            "started_at_ms": call.started_at_ms,
            "ended_at_ms": call.ended_at_ms,
            "created_at_ms": call.created_at_ms,
            "updated_at_ms": call.updated_at_ms,
        }
        if row is None:
            database.add(OperatorCallRow(id=str(call.id), **values))
        else:
            _assign(row, values)

    async def _load_session(
        self,
        database: AsyncSession,
        session_id: UUID,
    ) -> Session | None:
        row = await database.get(SessionRow, str(session_id))
        if row is None:
            return None
        invocation_ids = (
            await database.scalars(
                select(InvocationRow.id)
                .where(InvocationRow.session_id == row.id)
                .order_by(InvocationRow.created_at_ms, InvocationRow.id)
            )
        ).all()
        invocations = [
            invocation
            for raw_id in invocation_ids
            if (invocation := await self._load_invocation(database, UUID(raw_id)))
            is not None
        ]
        return Session.from_record(
            {
                "id": row.id,
                "namespace": row.namespace,
                "workflow_id": row.workflow_id,
                "session_key": row.session_key,
                "context": self._load(row.context_json),
                "current_invocation_id": row.current_invocation_id,
                "created_at_ms": row.created_at_ms,
                "updated_at_ms": row.updated_at_ms,
            },
            invocations=invocations,
        )

    async def _load_invocation(
        self,
        database: AsyncSession,
        invocation_id: UUID,
    ) -> Invocation | None:
        row = await database.get(InvocationRow, str(invocation_id))
        if row is None:
            return None
        execution_rows = (
            await database.scalars(
                select(NodeExecutionRow)
                .where(NodeExecutionRow.invocation_id == row.id)
                .order_by(NodeExecutionRow.sequence)
            )
        ).all()
        executions = [
            await self._load_node_execution(database, execution_row)
            for execution_row in execution_rows
        ]
        return Invocation.from_record(
            {
                "id": row.id,
                "workflow_id": row.workflow_id,
                "workflow_version": self._load(row.workflow_version_json),
                "workflow_definition_hash": row.definition_hash,
                "workflow_operator_manifest_hash": row.operator_manifest_hash,
                "entry_node_id": row.entry_node_id,
                "state": row.state,
                "input": self._load(row.input_json),
                "context": self._load(row.context_json),
                "result": self._load(row.result_json),
                "scheduler": self._load(row.scheduler_json),
                "error": self._load(row.error_json),
                "created_at_ms": row.created_at_ms,
                "updated_at_ms": row.updated_at_ms,
            },
            node_executions=executions,
        )

    async def _load_node_execution(
        self,
        database: AsyncSession,
        row: NodeExecutionRow,
    ) -> NodeExecution:
        call_rows = (
            await database.scalars(
                select(OperatorCallRow)
                .where(OperatorCallRow.node_execution_id == row.id)
                .order_by(OperatorCallRow.call_no)
            )
        ).all()
        calls = [self._operator_call_from_row(call_row) for call_row in call_rows]
        return NodeExecution.from_record(
            {
                "id": row.id,
                "node_id": row.node_id,
                "sequence": row.sequence,
                "state": row.state,
                "input": self._load(row.input_json),
                "output": self._load(row.output_json),
                "error": self._load(row.error_json),
                "idempotency_key": row.idempotency_key,
                "recovery_of_execution_id": row.recovery_of_execution_id,
                "recovery_attempt": row.recovery_attempt,
                "incoming_activations": self._load(row.incoming_activations_json),
                "edge_evaluations": self._load(row.edge_evaluations_json),
                "resource_usage": self._load(row.resource_usage_json),
                "started_at_ms": row.started_at_ms,
                "ended_at_ms": row.ended_at_ms,
                "created_at_ms": row.created_at_ms,
                "updated_at_ms": row.updated_at_ms,
            },
            operator_calls=calls,
        )

    def _operator_call_from_row(self, row: OperatorCallRow) -> OperatorCall:
        return OperatorCall.from_record(
            {
                "id": row.id,
                "operator_id": row.operator_id,
                "operator_manifest": self._load(row.operator_manifest_json),
                "call_no": row.call_no,
                "kind": row.kind,
                "item_index": row.item_index,
                "replica_index": row.replica_index,
                "state": row.state,
                "input": self._load(row.input_json),
                "output": self._load(row.output_json),
                "error": self._load(row.error_json),
                "resource_usage": self._load(row.resource_usage_json),
                "started_at_ms": row.started_at_ms,
                "ended_at_ms": row.ended_at_ms,
                "created_at_ms": row.created_at_ms,
                "updated_at_ms": row.updated_at_ms,
            }
        )

    async def _delete_missing_children(
        self,
        database: AsyncSession,
        model: type[Any],
        foreign_key: Any,
        parent_id: str,
        retained_ids: set[str],
    ) -> None:
        statement = delete(model).where(foreign_key == parent_id)
        if retained_ids:
            statement = statement.where(model.id.not_in(retained_ids))
        await database.execute(statement)

    async def _append_event_drafts(
        self,
        database: AsyncSession,
        *,
        session_id: UUID,
        invocation_id: UUID,
        drafts: tuple[RuntimeEventDraft, ...],
    ) -> tuple[RuntimeEvent, ...]:
        if not drafts:
            return ()
        session_row = await database.get(SessionRow, str(session_id))
        invocation_row = await database.get(InvocationRow, str(invocation_id))
        if session_row is None:
            raise KeyError(f"Unknown session: {session_id}")
        if invocation_row is None:
            raise KeyError(f"Unknown invocation: {invocation_id}")
        latest = await database.scalar(
            select(func.max(RuntimeEventRow.sequence)).where(
                RuntimeEventRow.session_id == str(session_id)
            )
        )
        next_sequence = int(latest or 0) + 1
        values: list[RuntimeEvent] = []
        for offset, draft in enumerate(drafts):
            runtime_event = draft.materialize(
                namespace=session_row.namespace,
                workflow_id=session_row.workflow_id,
                session_id=session_id,
                invocation_id=invocation_id,
                sequence=next_sequence + offset,
            )
            database.add(
                RuntimeEventRow(
                    id=str(runtime_event.id),
                    session_id=str(runtime_event.session_id),
                    invocation_id=str(runtime_event.invocation_id),
                    namespace=runtime_event.namespace,
                    workflow_id=runtime_event.workflow_id,
                    sequence=runtime_event.sequence,
                    type=runtime_event.type,
                    entity_type=runtime_event.entity_type,
                    entity_id=runtime_event.entity_id,
                    node_id=runtime_event.node_id,
                    edge_id=runtime_event.edge_id,
                    occurred_at_ms=runtime_event.occurred_at_ms,
                    channel=runtime_event.channel,
                    visibility=runtime_event.visibility,
                    payload_json=self._dump(runtime_event.payload),
                )
            )
            values.append(runtime_event)
        await database.flush()
        return tuple(values)

    def _runtime_event_from_row(self, row: RuntimeEventRow) -> RuntimeEvent:
        return RuntimeEvent(
            id=UUID(row.id),
            namespace=row.namespace,
            workflow_id=row.workflow_id,
            session_id=UUID(row.session_id),
            invocation_id=UUID(row.invocation_id),
            sequence=row.sequence,
            type=row.type,
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            node_id=row.node_id,
            edge_id=row.edge_id,
            occurred_at_ms=row.occurred_at_ms,
            channel=row.channel,
            visibility=row.visibility,
            payload=self.serializer.json_view(row.payload_json),
        )

    def _dump(self, value: Any) -> str:
        return self.serializer.dumps(value).decode("utf-8")

    def _load(self, payload: str) -> Any:
        return self.serializer.loads(payload)


def _assign(row: Any, values: dict[str, Any]) -> None:
    for key, value in values.items():
        setattr(row, key, value)


def _validate_event_query(
    session_id: UUID | None,
    invocation_id: UUID | None,
    after_sequence: int,
    limit: int,
) -> None:
    if session_id is None and invocation_id is None:
        raise ValueError("session_id or invocation_id is required.")
    if after_sequence < 0:
        raise ValueError("after_sequence cannot be negative.")
    if limit <= 0 or limit > 10_000:
        raise ValueError("limit must be between 1 and 10000.")


def _enable_sqlite_foreign_keys(connection: Any, _record: Any) -> None:
    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def _resolve_database_url(value: str | Path) -> str:
    raw = str(value)
    if raw.startswith("sqlite+aiosqlite://"):
        return raw
    if raw == ":memory:":
        return "sqlite+aiosqlite:///:memory:"
    return f"sqlite+aiosqlite:///{Path(raw).expanduser().resolve()}"
