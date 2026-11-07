"""Host-owned Agent configuration; independent of Core projections/checkpoints."""

import time
from uuid import uuid4

from sqlalchemy import CheckConstraint, Index, MetaData, Table, UniqueConstraint, func, insert, select, update

from autoagent.agent.context import (
    AgentExecutionSnapshot, AgentSessionSettings, AgentSettingsRecord, restore,
)
from .models import ConflictError
from .schema import B, J, c, meta


context_meta = MetaData()
session_settings = Table(
    "agent_session_settings", context_meta,
    c("session_id", nullable=False, pk=True), c("version", B, False),
    c("settings", J, False), c("updated_at_us", B, False),
    CheckConstraint("version >= 1"),
)
execution_settings = Table(
    "agent_execution_settings", context_meta,
    c("snapshot_id", nullable=False, pk=True), c("session_id", nullable=False),
    c("invocation_id", nullable=False), c("epoch", B, False),
    c("settings_version", B, False), c("settings", J, False),
    c("bindings", J, False), c("reason", nullable=False),
    c("activation_id", nullable=False), c("created_at_us", B, False),
    CheckConstraint("epoch >= 1"),
)
execution_settings.append_constraint(UniqueConstraint("invocation_id", "epoch"))
execution_settings.append_constraint(UniqueConstraint("invocation_id", "activation_id"))
Index("agent_execution_settings_latest", execution_settings.c.invocation_id, execution_settings.c.epoch)


class AgentContextStorage:
    def __init__(self, writer):
        self.writer = writer
        self._ready = False

    async def _prepare(self, conn):
        if not self._ready:
            from sqlalchemy import inspect
            from .models import FormatError
            await conn.run_sync(context_meta.create_all)
            for table in context_meta.tables.values():
                columns = await conn.run_sync(lambda sync, name=table.name:
                    {item["name"] for item in inspect(sync).get_columns(name)})
                if columns != set(table.columns.keys()):
                    raise FormatError("Agent Context storage does not match the current schema.")

    async def _exists(self, conn, table):
        from sqlalchemy import inspect
        return await conn.run_sync(lambda sync: inspect(sync).has_table(table.name))

    async def settings(self, sid, defaults):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            row = None
            if await self._exists(conn, session_settings):
                row = (await conn.execute(select(session_settings).where(session_settings.c.session_id == sid))).mappings().first()
            return AgentSettingsRecord(session_id=sid, version=row["version"] if row else 0,
                settings=restore(AgentSessionSettings, row["settings"] if row else defaults))

    async def configure(self, sid, settings, expected_version):
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot configure Agent settings.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                row = (await conn.execute(select(session_settings).where(session_settings.c.session_id == sid))).mappings().first()
                version = row["version"] if row else 0
                if expected_version is not None and expected_version != version:
                    raise ConflictError("Agent Session settings version conflict.")
                values = dict(version=version + 1, settings=settings.model_dump(mode="json"),
                              updated_at_us=time.time_ns() // 1000)
                if row:
                    await conn.execute(update(session_settings).where(session_settings.c.session_id == sid).values(**values))
                else:
                    await conn.execute(insert(session_settings).values(session_id=sid, **values))
            self._ready = True
        return AgentSettingsRecord(session_id=sid, version=version + 1, settings=settings)

    async def activate(self, sid, iid, defaults, reason, activation_id, bind):
        """Select desired settings and insert one immutable stage before dispatch."""
        self.writer.check_open()
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                previous = (await conn.execute(select(execution_settings).where(
                    execution_settings.c.invocation_id == iid,
                    execution_settings.c.activation_id == activation_id))).mappings().first()
                if previous:
                    return self._snapshot(previous)
                configured = (await conn.execute(select(session_settings).where(session_settings.c.session_id == sid))).mappings().first()
                settings = restore(AgentSessionSettings, configured["settings"] if configured else defaults)
                bindings = bind(settings)
                epoch = (await conn.scalar(select(func.max(execution_settings.c.epoch)).where(
                    execution_settings.c.invocation_id == iid)) or 0) + 1
                row = dict(snapshot_id=uuid4().hex, session_id=sid, invocation_id=iid, epoch=epoch,
                    settings_version=configured["version"] if configured else 0,
                    settings=settings.model_dump(mode="json"), bindings=bindings, reason=reason,
                    activation_id=activation_id, created_at_us=time.time_ns() // 1000)
                await conn.execute(insert(execution_settings).values(**row))
            self._ready = True
            return self._snapshot(row)

    @staticmethod
    def _snapshot(row):
        return restore(AgentExecutionSnapshot, {key: value for key, value in row.items() if key != "activation_id"})

    async def snapshot(self, iid):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not await self._exists(conn, execution_settings):
                return None
            # A staged snapshot becomes effective only with its accepted Runtime
            # Event. Failed Event admission must not change crash recovery.
            events = meta.tables["runtime_events"]
            row = (await conn.execute(select(execution_settings).join(events,
                events.c.event_id == execution_settings.c.activation_id).where(execution_settings.c.invocation_id == iid)
                .order_by(execution_settings.c.epoch.desc()).limit(1))).mappings().first()
            return self._snapshot(row) if row else None
