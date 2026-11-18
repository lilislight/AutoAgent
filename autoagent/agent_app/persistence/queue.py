"""Durable pending chat submissions, separate from execution history."""

import time

from sqlalchemy import CheckConstraint, Index, MetaData, Table, UniqueConstraint, func, insert, inspect, select, text, update

from autoagent.host.persistence.models import ConflictError
from autoagent.host.persistence.schema import B, J, c, meta
from .context import ApplicationContextStorage

metadata = MetaData()
messages = Table("agent_app_message_queue", metadata,
    c("id", nullable=False, pk=True), c("session_id", nullable=False),
    c("position", B, False), c("message", nullable=False), c("state", nullable=False),
    c("model", J), c("send_to"), c("images", J),
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
        return self.ready or await conn.run_sync(lambda sync: inspect(sync).has_table(messages.name))

    async def _prepare(self, conn):
        if not self.ready:
            await conn.run_sync(metadata.create_all)
            columns = await conn.run_sync(lambda sync: {column["name"] for column in inspect(sync).get_columns(messages.name)})
            if "images" not in columns:
                column_type = messages.c.images.type.compile(dialect=conn.dialect)
                await conn.execute(text(f"ALTER TABLE agent_app_message_queue ADD COLUMN images {column_type}"))
            if "send_to" not in columns:
                await conn.execute(text("ALTER TABLE agent_app_message_queue ADD COLUMN send_to TEXT"))
                # Earlier immediate submissions had no model and kept their
                # original target in invocation_id until a separate queue action.
                await conn.execute(update(messages).where(messages.c.model.is_(None),
                    messages.c.invocation_id.is_not(None),
                    messages.c.state.in_(("sending", "sent", "blocked", "cancelled"))
                ).values(send_to=messages.c.invocation_id))

    async def prepare(self):
        self.writer.check_open()
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                await self._restore_legacy_admissions(conn)
            self.ready = True

    async def _restore_legacy_admissions(self, conn):
        """Retain admitted entries from databases that only recorded the ID in the old input."""
        sessions, invocations = (meta.tables[name] for name in ("sessions", "invocations"))
        pending = select(messages.c.id, invocations.c.invocation_id, invocations.c.input_ref).select_from(messages).join(
            sessions, sessions.c.session_id == messages.c.session_id).join(
            invocations, invocations.c.invocation_id == sessions.c.latest_invocation_id).where(
            messages.c.state == "starting")
        for item in (await conn.execute(pending)).mappings():
            reference = item["input_ref"]
            if reference is None:
                continue
            source = {**reference, "path": [*reference["path"], "request_id"]}
            recorded = await self.queries._resolve(conn, (), source)
            if recorded["available"]:
                iid = item["invocation_id"] if recorded["value"] == item["id"] else None
                await conn.execute(update(messages).where(messages.c.id == item["id"]).values(invocation_id=iid))

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

    async def add(self, sid, item_id, message, model=None, *, conversation=None, send_to=None, uploads=()):
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot enqueue messages.")
        image_refs = [image.content().model_dump(mode="json") for image in uploads]
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                previous = (await conn.execute(select(messages).where(messages.c.id == item_id))).mappings().first()
                if previous:
                    if (previous["session_id"] != sid or previous["message"] != message or previous["model"] != model
                            or (previous["images"] or []) != image_refs):
                        raise ConflictError("Queue item ID was reused with different content.")
                    if previous["send_to"] != send_to:
                        raise ConflictError("Queue item ID was reused with a different delivery target.")
                    return dict(previous)
                count = await conn.scalar(select(func.count()).select_from(messages).where(
                    messages.c.session_id == sid, messages.c.state.in_(PENDING)))
                if count >= 100:
                    raise ConflictError("This conversation already has 100 pending messages.")
                if conversation is not None:
                    await ApplicationContextStorage._initialize_settings(
                        conn, sid, conversation.settings, conversation.workspace, message or ("Image" if uploads else ""))
                    if model is not None:
                        await ApplicationContextStorage._remember_model(conn, sid, model)
                position = (await conn.scalar(select(func.max(messages.c.position)).where(messages.c.session_id == sid)) or 0) + 1
                await ApplicationContextStorage._store_images(conn, sid, uploads)
                row = dict(id=item_id, session_id=sid, message=message, model=model, position=position, images=image_refs or None,
                    send_to=send_to, state="sending" if send_to is not None else "queued",
                    invocation_id=send_to, previous_invocation_id=None, continue_after_id=None, blocked_on_id=None, error=None,
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
