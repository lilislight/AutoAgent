"""Durable pending chat submissions, separate from execution history."""

import time

from sqlalchemy import CheckConstraint, Index, MetaData, Table, UniqueConstraint, func, insert, select, update

from autoagent.host.persistence.models import ConflictError
from autoagent.host.persistence.schema import B, J, c

metadata = MetaData()
messages = Table("agent_app_message_queue", metadata,
    c("id", nullable=False, pk=True), c("session_id", nullable=False),
    c("position", B, False), c("message", nullable=False), c("state", nullable=False),
    c("model", J),
    c("invocation_id"), c("previous_invocation_id"), c("continue_after_id"), c("blocked_on_id"), c("error"),
    c("created_at_us", B, False),
    UniqueConstraint("session_id", "position"),
    CheckConstraint("state IN ('queued','starting','started','sending','sent','cancelled','blocked')"))
Index("agent_app_message_queue_pending", messages.c.session_id, messages.c.state, messages.c.position)
PENDING = ("queued", "starting", "sending", "blocked")


class ApplicationQueueStorage:
    def __init__(self, writer, queries):
        self.writer, self.queries = writer, queries
        self.ready = False

    async def _exists(self, conn):
        from sqlalchemy import inspect
        return self.ready or await conn.run_sync(lambda sync: inspect(sync).has_table(messages.name))

    async def rows(self, session_id=None, *, item_id=None):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not await self._exists(conn):
                return []
            query = select(messages).order_by(messages.c.position)
            if session_id is not None:
                query = query.where(messages.c.session_id == session_id)
            if item_id is not None:
                query = query.where(messages.c.id == item_id)
            else:
                query = query.where(messages.c.state.in_(PENDING))
            return [dict(row) for row in (await conn.execute(query)).mappings()]

    async def pending_sessions(self):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not await self._exists(conn):
                return []
            return list((await conn.execute(select(messages.c.session_id).where(
                messages.c.state.in_(PENDING)).distinct())).scalars())

    async def add(self, sid, item_id, message, model=None):
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot enqueue messages.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                if not self.ready:
                    await conn.run_sync(metadata.create_all)
                previous = (await conn.execute(select(messages).where(messages.c.id == item_id))).mappings().first()
                if previous:
                    if previous["session_id"] != sid or previous["message"] != message or previous["model"] != model:
                        raise ConflictError("Message request ID was reused with different content.")
                    return dict(previous)
                count = await conn.scalar(select(func.count()).select_from(messages).where(
                    messages.c.session_id == sid, messages.c.state.in_(PENDING)))
                if count >= 100:
                    raise ConflictError("This conversation already has 100 pending messages.")
                position = (await conn.scalar(select(func.max(messages.c.position)).where(messages.c.session_id == sid)) or 0) + 1
                row = dict(id=item_id, session_id=sid, message=message, model=model, position=position, state="queued",
                    invocation_id=None, previous_invocation_id=None, continue_after_id=None, blocked_on_id=None, error=None,
                    created_at_us=time.time_ns() // 1000)
                await conn.execute(insert(messages).values(**row))
            self.ready = True
            return row

    async def change(self, sid, item_id, expected, **values):
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot change pending messages.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                if not await self._exists(conn):
                    raise ConflictError("Queued message is unavailable.")
                result = await conn.execute(update(messages).where(messages.c.session_id == sid,
                    messages.c.id == item_id, messages.c.state.in_(expected)).values(**values))
                if result.rowcount != 1:
                    raise ConflictError("Message already started or changed; refresh the queue.")

    async def request_for(self, iid):
        """Reconcile admission after a crash without inferring identity from message text."""
        async with self.queries.view("accepted", control=True, invocation_id=iid) as (conn, overlay, pending, _):
            row = await overlay.get("invocations", invocation_id=iid)
            if row is None:
                return None
            value = (await self.queries._resolve(conn, pending, row.get("input_ref")))["value"]
            return value.get("request_id") if isinstance(value, dict) else None
