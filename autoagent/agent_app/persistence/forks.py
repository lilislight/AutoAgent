"""Immutable Agent conversation ancestry, independent of Workflow replay."""

import time

from sqlalchemy import and_, func, insert, or_, select

from autoagent.host.models import HostNotFoundError, HostRunConflict
from autoagent.host.persistence.models import ConflictError
from autoagent.host.persistence.schema import meta

from .context import sessions
from ..workflow import BUILTIN_AGENT_WORKFLOW_ID


async def history_ranges(conn, session_id):
    """Flatten ancestry and truncate it at the selected physical Invocation."""
    inv = meta.tables["invocations"]
    chain, seen = [], set()
    current = session_id
    while current:
        if current in seen:
            raise ConflictError("Conversation Fork ancestry contains a cycle.")
        seen.add(current)
        row = (await conn.execute(select(sessions).where(sessions.c.session_id == current))).mappings().first()
        chain.append((current, row))
        current = row["fork_source_session_id"] if row else None
    ranges = []
    for sid, row in reversed(chain):
        if row and row["fork_source_session_id"]:
            owner = await conn.scalar(select(inv.c.session_id).where(
                inv.c.invocation_id == row["fork_source_invocation_id"]))
            position = next((i for i, (source, _) in enumerate(ranges) if source == owner), None)
            if position is None:
                raise HostNotFoundError("A Fork's source history is unavailable.")
            maximum = ranges[position][1]
            boundary = row["fork_source_sequence"]
            if maximum is not None and boundary > maximum:
                raise ConflictError("Fork boundary exceeds inherited history.")
            ranges = [*ranges[:position], (owner, boundary)]
        ranges.append((sid, None))
    return ranges


def range_condition(invocations, ranges):
    return or_(*(and_(invocations.c.session_id == sid,
        invocations.c.last_sequence <= boundary) if boundary is not None else
        invocations.c.session_id == sid for sid, boundary in ranges))


class ForkStorage:
    def __init__(self, writer, queries):
        self.writer, self.queries = writer, queries

    async def info(self, session_id):
        async with self.queries.view() as (conn, _, _, _):
            row = (await conn.execute(select(sessions).where(sessions.c.session_id == session_id))).mappings().first()
            return dict(row) if row else None

    async def target(self, session_id, invocation_id):
        inv, definitions, events = (meta.tables[name] for name in ("invocations", "workflow_revisions", "runtime_events"))
        async with self.queries.view() as (conn, _, _, _):
            ranges = await history_ranges(conn, session_id)
            row = (await conn.execute(select(inv).join(definitions).where(
                inv.c.invocation_id == invocation_id, definitions.c.workflow_id == BUILTIN_AGENT_WORKFLOW_ID,
                range_condition(inv, ranges)))).mappings().first()
            if row is None:
                raise HostNotFoundError("This reply is unavailable in the conversation.")
            if row["status"] != "completed":
                raise HostRunConflict("fork_not_completed", "Choose a completed reply to Fork.")
            boundary = await conn.scalar(select(events.c.sequence).where(events.c.invocation_id == invocation_id,
                events.c.event_name == "invocation.completed").order_by(events.c.sequence.desc()).limit(1))
            if boundary is None:
                raise HostRunConflict("fork_not_saved", "The completed reply is still being saved. Try again.")
            return dict(invocation_id=invocation_id, session_id=row["session_id"], sequence=boundary)

    async def create(self, source_session_id, new_session_id, target, record):
        """Validate source and destination under the same writer lock used by purge."""
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot create a Fork.")
        inv, events, core_sessions = (meta.tables[name] for name in ("invocations", "runtime_events", "sessions"))
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                previous = (await conn.execute(select(sessions).where(sessions.c.session_id == new_session_id))).mappings().first()
                origin = dict(fork_source_session_id=source_session_id,
                    fork_source_invocation_id=target["invocation_id"], fork_source_sequence=target["sequence"])
                if previous:
                    if all(previous[key] == value for key, value in origin.items()):
                        return
                    raise ConflictError("The destination conversation already exists.")
                if await conn.scalar(select(core_sessions.c.session_id).where(core_sessions.c.session_id == new_session_id)):
                    raise ConflictError("The destination Session already exists.")
                exists = await conn.scalar(select(events.c.event_id).join(inv,
                    inv.c.invocation_id == events.c.invocation_id).where(
                        inv.c.invocation_id == target["invocation_id"], inv.c.status == "completed",
                        events.c.sequence == target["sequence"], events.c.event_name == "invocation.completed"))
                if not exists:
                    raise ConflictError("The Fork's source history is no longer available.")
                ranges = await history_ranges(conn, source_session_id)
                source = (await conn.execute(select(inv.c.invocation_id).where(
                    inv.c.invocation_id == target["invocation_id"], range_condition(inv, ranges)))).first()
                if source is None:
                    raise ConflictError("The Fork's source is outside the conversation.")
                count = await conn.scalar(select(func.count()).select_from(sessions).where(
                    sessions.c.fork_source_session_id == source_session_id))
                suffix = " · Fork" + (f" {count + 1}" if count else "")
                title = (record.title or "Conversation")[:120 - len(suffix)] + suffix
                await conn.execute(insert(sessions).values(session_id=new_session_id, title=title, metadata_version=1,
                    version=1, settings=record.settings.model_dump(mode="json"),
                    workspace=record.workspace.model_dump(mode="json") if record.workspace else None,
                    updated_at_us=time.time_ns() // 1000, **origin))

    async def image(self, session_id, image_id):
        """Resolve an attachment only through messages inside the inherited ranges."""
        from ..images import IMAGE_URI_PREFIX
        from .conversation import ConversationStorage, _session_invocations
        from .context import images

        async with self.queries.view() as (conn, _, _, _):
            asset = (await conn.execute(select(images).where(images.c.id == image_id))).mappings().first()
            if asset is None:
                return None
            if asset["session_id"] == session_id:
                return dict(asset)
            ranges = await history_ranges(conn, session_id)
            if not any(sid == asset["session_id"] for sid, _ in ranges):
                return None
            statement = _session_invocations(ranges)
            visible = statement.with_only_columns(statement.selected_columns.invocation_id, maintain_column_froms=True)
            events = meta.tables["user_events"]
            projector = ConversationStorage(self.writer, self.queries)
            async for _row, reader in projector._sources(conn, events.c.invocation_id.in_(visible) & (events.c.kind == "agent.messages")):
                messages = await reader.read(("payload", "messages"))
                if any(part.get("source", {}).get("uri") == IMAGE_URI_PREFIX + image_id
                        for message in messages for part in message["content"]):
                    return dict(asset)
        return None
