"""Read Agent chat history without hydrating collapsed execution payloads."""

from collections import defaultdict

from sqlalchemy import case, func, select, tuple_

from autoagent.host.models import HostNotFoundError
from autoagent.host.persistence.reading import SourceReader
from autoagent.host.persistence.schema import meta

from ..workflow import BUILTIN_AGENT_WORKFLOW_ID
from .forks import history_ranges, range_condition


_invocations = meta.tables["invocations"]
_revisions = meta.tables["workflow_revisions"]
_events = meta.tables["user_events"]
_refs = meta.tables["artifact_refs"]
_time = func.coalesce(_invocations.c.started_at_us, 0)
_answer_types = {"text", "refusal", "media"}


def _session_invocations(ranges):
    rank = case({sid: i for i, (sid, _) in enumerate(ranges)}, value=_invocations.c.session_id)
    rows = select(
        _invocations.c.invocation_id, _invocations.c.session_id,
        _invocations.c.workflow_revision_id, _invocations.c.status,
        _invocations.c.started_at_us, _invocations.c.completed_at_us,
        _invocations.c.parent_invocation_id, _invocations.c.root_invocation_id,
        _revisions.c.workflow_id,
        func.row_number().over(order_by=(rank, _time, _invocations.c.invocation_id)).label("position_time"),
    ).join(_revisions, _invocations.c.workflow_revision_id == _revisions.c.revision_id).where(
        range_condition(_invocations, ranges),
        _invocations.c.status.is_not(None),
        _revisions.c.workflow_id == BUILTIN_AGENT_WORKFLOW_ID,
    ).subquery()
    return select(rows)


def _excerpt(parts, limit):
    text = " ".join(" ".join(parts).split())
    return text if len(text) <= limit else text[:limit] + "…"


class ConversationStorage:
    def __init__(self, writer, queries):
        self.writer, self.queries = writer, queries

    async def _sources(self, conn, clause):
        rows = (await conn.execute(select(_events).where(clause).order_by(
            _events.c.sequence))).mappings().all()
        if not rows:
            return
        refs = defaultdict(dict)
        for ref in await conn.execute(select(_refs.c.owner_id, _refs.c.path, _refs.c.artifact_hash).where(
                _refs.c.owner_kind == "user_event",
                _refs.c.owner_id.in_(select(_events.c.event_id).where(clause)))):
            refs[ref.owner_id][ref.path] = ref.artifact_hash
        for row in rows:
            reader = await SourceReader.open(self.writer.storage, conn, "user_event", row["event_id"],
                row=row, refs=refs[row["event_id"]])
            yield row, reader

    @staticmethod
    def _event(row, payload):
        return {**{key: row[key] for key in ("session_id", "invocation_id", "sequence",
            "kind", "occurrence_id", "occurred_at_us")}, "id": row["event_id"], "persist": True, "payload": payload}

    async def _project(self, conn, records, scope, *, index=False):
        messages, completed, answers = defaultdict(list), {}, {}
        visible, details = [], {}
        async for row, reader in self._sources(conn, scope & _events.c.kind.in_(("agent.messages", "agent.completed"))):
            iid = row["invocation_id"]
            if row["kind"] == "agent.completed":
                if records[iid]["status"] not in ("failed", "cancelled"):
                    completed[iid] = await reader.read(("payload",))
                    if not index:
                        visible.append(self._event(row, completed[iid]))
                continue
            path = ("payload", "messages")
            users = []
            for i in range(len(await reader.read(path, deep=False))):
                entry = (*path, i)
                message = await reader.read(entry, deep=False)
                if message["source"] != "user":
                    details[iid] = min(details.get(iid, row["occurred_at_us"]), row["occurred_at_us"])
                elif index:
                    parts = await self._preview(reader, (*entry, "content"), {"text"}, 180)
                    messages[iid].append(dict(message_id=message["message_id"], invocation_id=iid,
                        at=row["occurred_at_us"], question=parts or "Message with attachments"))
                else:
                    users.append(await reader.read(entry))
            if users:
                payload = await reader.read(("payload",), deep=False)
                visible.append(self._event(row, {**payload, "messages": users}))

        final = [(iid, value["final_response_id"]) for iid, value in completed.items()]
        # Use the (Invocation, Occurrence, sequence) index and bound SQL parameter counts.
        for start in range(0, len(final), 200):
            batch = final[start:start + 200]
            async for row, reader in self._sources(conn, (_events.c.kind == "agent.response") &
                    tuple_(_events.c.invocation_id, _events.c.occurrence_id).in_(batch)):
                iid = row["invocation_id"]
                path = ("payload", "blocks")
                if index:
                    answers[iid] = await self._preview(reader, path, {"text", "refusal"}, 320,
                        attachment_label="Answer contains attachments", content_type="media")
                else:
                    blocks = []
                    for i in range(len(await reader.read(path, deep=False))):
                        entry = (*path, i)
                        if await reader.read((*entry, "type")) in _answer_types:
                            blocks.append(await reader.read(entry))
                        else:
                            details[iid] = min(details.get(iid, row["occurred_at_us"]), row["occurred_at_us"])
                    payload = await reader.read(("payload",), deep=False)
                    visible.append(self._event(row, {**payload, "blocks": blocks}))
        if index:
            fallback = {"cancelled": "Task stopped · No final answer", "failed": "Task failed · No final answer",
                        "waiting": "Waiting for input…", "completed": "No text answer"}
            items = [{**message, "answer": answers.get(iid) or fallback.get(records[iid]["status"], "Working…")}
                for iid, user_messages in messages.items() for message in user_messages]
            return sorted(items, key=lambda item: (item["at"], item["invocation_id"], item["message_id"]))

        # Select only the first log position. Tool arguments, results, and intermediate
        # responses are hydrated exclusively by the details endpoint.
        for row in await conn.execute(select(_events.c.invocation_id, _events.c.kind,
                func.count().label("count"), func.min(_events.c.occurred_at_us).label("at")).where(
                    scope, _events.c.kind.in_(("agent.response", "agent.tools", "agent.interaction",
                        "agent.compaction", "agent.error"))).group_by(_events.c.invocation_id, _events.c.kind)):
            if row.kind != "agent.response" or row.invocation_id not in completed or row.count > 1:
                details[row.invocation_id] = min(details.get(row.invocation_id, row.at), row.at)
        return dict(events=visible, details=details)

    @staticmethod
    async def _preview(reader, path, text_types, limit, *, attachment_label="", content_type=None):
        parts = []
        remaining = limit + 1
        has_attachment = False
        for i in range(len(await reader.read(path, deep=False))):
            entry = (*path, i)
            kind = await reader.read((*entry, "type"))
            if kind in text_types:
                text = (await reader.read((*entry, "text")))[:remaining * 3]
                segment = " ".join(text.split())
                if segment:
                    parts.append(segment)
                    remaining -= len(segment) + 1
                    if remaining <= 0:
                        break
            elif kind == content_type:
                has_attachment = True
        return _excerpt(parts, limit) or (attachment_label if has_attachment else "")

    async def page(self, session_id, *, limit, cursor=None, anchor=None):
        """Return one window of questions, final replies, and collapsed log positions."""
        async with self.queries.view() as (conn, _, _, _):
            statement = _session_invocations(await history_ranges(conn, session_id))
            columns = statement.selected_columns
            position = tuple_(columns.position_time, columns.invocation_id)
            if cursor is not None:
                statement = statement.where(position < cursor)
            elif anchor is not None:
                target = (await conn.execute(statement.where(columns.invocation_id == anchor))).mappings().first()
                if target is None:
                    raise HostNotFoundError("Turn is unavailable in this conversation.")
                statement = statement.where(position <= (target["position_time"], anchor))
            rows = (await conn.execute(statement.order_by(columns.position_time.desc(), columns.invocation_id.desc())
                .limit(limit + 1))).mappings().all()
            records = {row["invocation_id"]: dict(row) for row in rows[:limit]}
            scope = _events.c.invocation_id.in_(tuple(records))
            projection = await self._project(conn, records, scope)
            next_position = (rows[limit - 1]["position_time"], rows[limit - 1]["invocation_id"]) if len(rows) > limit else None
            for record in records.values():
                record.pop("position_time")
                record["inherited"] = record["session_id"] != session_id
            return dict(items=list(records.values()), next_cursor=next_position, **projection)

    async def index(self, session_id):
        """Return the complete turn index with bounded previews, including steering messages."""
        async with self.queries.view() as (conn, _, _, _):
            statement = _session_invocations(await history_ranges(conn, session_id))
            records = {row.invocation_id: row for row in (await conn.execute(statement)).mappings()}
            scope = _events.c.invocation_id.in_(statement.with_only_columns(statement.selected_columns.invocation_id,
                maintain_column_froms=True))
            return await self._project(conn, records, scope, index=True)
