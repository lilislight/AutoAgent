"""Durable Invocation and Event evidence, without graph or payload hydration."""

from sqlalchemy import func, select

from .models import Page
from .queries import limit_value
from .reading import SourceReader, pointer_tokens
from .schema import meta
from .tracing import summary


class History:
    def __init__(self, queries):
        self.queries = queries

    async def identity(self):
        async with self.queries.view() as (conn, _, _, _):
            return await conn.scalar(select(meta.tables["persistence_meta"].c.store_id))

    async def invocation(self, iid):
        invs, definitions, events = (meta.tables[name] for name in ("invocations", "workflow_revisions", "runtime_events"))
        async with self.queries.view() as (conn, _, _, frontier):
            row = (await conn.execute(select(invs, definitions.c.workflow_id, definitions.c.workflow_version)
                .join(definitions, invs.c.workflow_revision_id == definitions.c.revision_id)
                .where(invs.c.invocation_id == iid))).mappings().first()
            if row is None:
                return None
            result = summary(row)
            from .tracing import fork_metadata
            result['fork'] = await fork_metadata(self.queries.storage,conn,iid)
            result["counts"] = {}
            for label, name in (("nodes", "node_executions"), ("calls", "operator_calls"), ("waits", "waits")):
                table = meta.tables[name]
                result["counts"][label] = dict((await conn.execute(select(table.c.status, func.count())
                    .where(table.c.invocation_id == iid).group_by(table.c.status))).all())
            edges = meta.tables["edge_evaluations"]
            result["counts"]["edge_evaluations"] = await conn.scalar(select(func.count()).select_from(edges).where(edges.c.invocation_id == iid))
            count, first, last = (await conn.execute(select(func.count(), func.min(events.c.sequence), func.max(events.c.sequence))
                .where(events.c.invocation_id == iid))).one()
            result.update(event_count=count, first_sequence=first, last_sequence=last or row["last_sequence"],
                sequence_scope="session", frontier=frontier, view="durable")
            result["duration_us"] = max(0, row["completed_at_us"] - row["started_at_us"]) if row["completed_at_us"] is not None and row["started_at_us"] is not None else None
            return result

    async def events(self, iid, *, after=0, names=(), limit=50, cursor=None):
        limit_value(limit)
        if type(after) is not int or not 0 <= after <= 9007199254740991:
            raise ValueError("after must be a nonnegative Session Event sequence.")
        if len(names) > 64 or any(not isinstance(name, str) or not name or len(name) > 512 for name in names):
            raise ValueError("Invalid Event name filter.")
        if cursor is not None and (not isinstance(cursor, (tuple, list)) or len(cursor) != 2 or type(cursor[0]) is not int or not isinstance(cursor[1], str)):
            raise ValueError("Invalid Event cursor.")
        table = meta.tables["runtime_events"]
        async with self.queries.view() as (conn, _, _, frontier):
            if await self.queries.storage.row(conn, "invocations", invocation_id=iid) is None:
                return None
            where = [table.c.invocation_id == iid, table.c.sequence > max(after, cursor[0] if cursor else 0)]
            if names:
                where.append(table.c.event_name.in_(names))
            rows = (await conn.execute(select(table.c.event_id, table.c.session_id, table.c.invocation_id,
                table.c.sequence, table.c.event_name, table.c.occurred_at_us, table.c.delta_text.is_not(None).label("has_delta"))
                .where(*where).order_by(table.c.sequence).limit(limit + 1))).mappings().all()
            items = [dict(row, sequence_scope="session", values=["payload", "delta"] if row["has_delta"] else ["payload"]) for row in rows[:limit]]
            return Page(tuple(items), (items[-1]["sequence"], items[-1]["event_id"]) if len(rows) > limit else None, frontier)

    async def event(self, iid, event_id, *, field=None, pointer=None):
        pointer_tokens(pointer)
        if field not in (None, "payload", "delta"):
            raise ValueError("Event field must be payload or delta.")
        if pointer is not None and field is None:
            raise ValueError("A JSON Pointer requires an Event field.")
        table = meta.tables["runtime_events"]
        async with self.queries.view() as (conn, _, _, _):
            row = (await conn.execute(select(table.c.event_id, table.c.session_id, table.c.invocation_id,
                table.c.sequence, table.c.event_name, table.c.occurred_at_us, table.c.delta_text.is_not(None).label("has_delta"))
                .where(table.c.event_id == event_id, table.c.invocation_id == iid))).mappings().first()
            if row is None:
                return None
            if field is None:
                return dict(row, sequence_scope="session", values=["payload", "delta"] if row["has_delta"] else ["payload"])
            if field == "delta" and not row["has_delta"]:
                return dict(available=False, value=None)
            reader = await SourceReader.open(self.queries.storage, conn, "runtime_event", event_id)
            try:
                return dict(available=True, value=await reader.read_pointer(pointer, base=(field,)))
            except (KeyError, IndexError, TypeError):
                return dict(available=False, value=None)
