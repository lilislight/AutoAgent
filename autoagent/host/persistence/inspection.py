"""Read exact historical fields without installing a Runtime or hydrating its graph.

Core commits Context as whole-field replacements. Resolve the last applicable
State Operation (including ancestor replacements), then read only the requested
JSON subtree. Checkpoints are optional bases, never newly persisted query results.
"""

import json

from sqlalchemy import LargeBinary, cast, func, select

from .models import FormatError, InspectionError
from .reading import SourceReader, pointer_tokens
from .schema import meta
from .storage import SOURCES


_MISSING = object()
_SUMMARY_FIELDS = (
    "id", "workflow_id", "workflow_revision_id", "entry_node_id", "status",
    "control_state", "pending_outcome", "recovery_count", "last_recovery_id",
    "created_at_us", "started_at_us", "completed_at_us", "kind",
)


class ReadBudget:
    def __init__(self, config):
        self.remaining = config.inspection_read_bytes
        self.result_bytes = config.inspection_result_bytes
        self.scanned = 0
        self.scan_events = config.inspection_scan_events

    def consume(self, size):
        self.remaining -= size
        if self.remaining < 0:
            raise InspectionError("INSPECTION_LIMIT", "Historical JSON decode budget exceeded. Narrow --path or raise AUTOAGENT_INSPECTION_READ_BYTES.")

    def scan(self):
        self.scanned += 1
        if self.scanned > self.scan_events:
            raise InspectionError("INSPECTION_LIMIT", "Historical Event scan budget exceeded. Raise AUTOAGENT_INSPECTION_SCAN_EVENTS or retain a nearer Checkpoint.")

    def check_result(self, value):
        size = 0
        for part in json.JSONEncoder(ensure_ascii=False, allow_nan=False).iterencode(value):
            size += len(part.encode("utf-8"))
            if size > self.result_bytes:
                raise InspectionError("INSPECTION_LIMIT", "Historical value exceeds the result budget. Narrow --path or raise AUTOAGENT_INSPECTION_RESULT_BYTES; --output does not bypass this limit.")


class BoundedReader(SourceReader):
    """SourceReader with query-local limits checked before fetching JSON bodies."""

    @classmethod
    async def open(cls, storage, conn, kind, identity, budget):
        name, key, body = SOURCES[kind]
        table = meta.tables[name]
        column = table.c.delta_text if kind == "runtime_event" else table.c[body]
        length = func.length(cast(column, LargeBinary)) if storage.sqlite else func.octet_length(column)
        size = await conn.scalar(select(length).where(table.c[key] == identity))
        budget.consume(size or 0)
        text = await conn.scalar(select(column).where(table.c[key] == identity))
        record = {"delta": json.loads(text) if text else None} if kind == "runtime_event" else json.loads(text)
        refs = meta.tables["artifact_refs"]
        where = [refs.c.owner_kind == kind, refs.c.owner_id == identity]
        if kind == "runtime_event":
            where.append((refs.c.path == "/delta") | refs.c.path.startswith("/delta/", autoescape=True))
        references = {r.path: r.artifact_hash for r in (await conn.execute(
            select(refs.c.path, refs.c.artifact_hash).where(*where)))}
        reader = cls(storage, conn, record, references)
        reader.budget = budget
        return reader

    async def _read(self, value, refs, path, deep, loading):
        if "" in refs and refs[""] not in self.cache and deep != "present":
            artifacts = meta.tables["artifacts"]
            row = (await self.conn.execute(select(func.length(artifacts.c.body), artifacts.c.logical_bytes)
                .where(artifacts.c.hash == refs[""]))).first()
            if row is None:
                raise FormatError("Missing Artifact.")
            self.budget.consume(row[0])
            if not path and deep is True and row[1] > self.budget.result_bytes:
                raise InspectionError("INSPECTION_LIMIT", "Selected Artifact exceeds the result budget. Narrow --path or raise AUTOAGENT_INSPECTION_RESULT_BYTES.")
        return await super()._read(value, refs, path, deep, loading)


async def _optional(reader, path, *, pointer=None):
    try:
        return await reader.read_pointer(pointer, base=path)
    except (KeyError, IndexError, TypeError):
        return _MISSING


class StateInspection:
    def __init__(self, queries, config):
        self.queries, self.config = queries, config

    async def read(self, iid, *, sequence, scope=None, pointer=None):
        if not isinstance(iid, str) or not iid.strip() or len(iid) > 4096:
            raise ValueError("Invocation ID must be nonempty and at most 4096 characters.")
        if type(sequence) is not int or not 1 <= sequence <= 9007199254740991:
            raise ValueError("sequence must be a positive Session Event sequence within the supported integer range.")
        if scope not in (None, "session", "invocation"):
            raise ValueError("scope must be session or invocation.")
        if pointer is not None and scope is None:
            raise ValueError("A Context scope is required with a JSON Pointer.")
        pointer_tokens(pointer)
        budget = ReadBudget(self.config)
        q = self.queries
        events, invs, members = (meta.tables[n] for n in ("runtime_events", "invocations", "checkpoint_sessions"))
        async with q.view() as (conn, _, _, frontier):
            inv = (await conn.execute(select(invs.c.session_id, invs.c.root_invocation_id,
                invs.c.parent_invocation_id).where(invs.c.invocation_id == iid))).first()
            if inv is None:
                return None
            target = (await conn.execute(select(events.c.event_id, events.c.event_name, events.c.occurred_at_us)
                .where(events.c.session_id == inv.session_id, events.c.invocation_id == iid,
                    events.c.sequence == sequence))).first()
            if target is None:
                raise ValueError("The requested persisted Event is unavailable in this Invocation; sequence is Session-scoped.")
            position = dict(session_id=inv.session_id, invocation_id=iid, sequence=sequence,
                event_id=target.event_id, event_name=target.event_name, occurred_at_us=target.occurred_at_us,
                sequence_scope="session", boundary="after")
            anchor = (await conn.execute(select(members.c.checkpoint_id, members.c.sequence)
                .where(members.c.session_id == inv.session_id, members.c.sequence <= sequence)
                .order_by(members.c.sequence.desc(), members.c.checkpoint_id).limit(1))).first()
            paths = {"context": (scope, "context")} if scope else {
                key: ("invocation", key) for key in _SUMMARY_FIELDS}
            resolved, sources = await self._resolve(conn, inv.session_id, sequence, anchor, paths, budget, pointer)
            result = dict(position=position, view="durable")
            # The source position is fixed even if this store frontier advances.
            result["read"] = dict(frontier=frontier, scanned_events=budget.scanned)
            if scope:
                value = resolved["context"]
                result.update(scope=scope, pointer=pointer or "", available=value is not _MISSING,
                    value=None if value is _MISSING else value, source=sources["context"])
                if value is _MISSING:
                    result["reason"] = sources["context"].get("reason", "path_not_found")
                budget.check_result(result["value"])
                return result
            state = {k: v for k, v in resolved.items() if v is not _MISSING}
            if state.get("id") != iid:
                raise InspectionError("HISTORY_UNAVAILABLE", "Historical Invocation identity does not match the requested Event.")
            compacted = state.pop("kind", None) == "child_result"
            state["invocation_id"] = state.pop("id")
            state["revision_id"] = state.pop("workflow_revision_id")
            state.setdefault("control_state", "active")
            state.setdefault("pending_outcome", None)
            state.update(root_invocation_id=inv.root_invocation_id, parent_invocation_id=inv.parent_invocation_id,
                compacted=compacted)
            result.update(state=state, contexts={"session": dict(available=True),
                "invocation": dict(available=not compacted, **({"reason": "released"} if compacted else {}))})
            budget.check_result(result)
            return result

    async def _resolve(self, conn, sid, sequence, anchor, paths, budget, pointer):
        events = meta.tables["runtime_events"]
        remaining, values, sources = dict(paths), {}, {}
        after = anchor.sequence if anchor else 0
        before, expected = sequence + 1, sequence
        while remaining and before - 1 > after:
            rows = (await conn.execute(select(events.c.event_id, events.c.sequence)
                .where(events.c.session_id == sid, events.c.sequence > after, events.c.sequence < before)
                .order_by(events.c.sequence.desc()).limit(min(128, budget.scan_events - budget.scanned + 1)))).all()
            if not rows:
                raise InspectionError("HISTORY_UNAVAILABLE", "Event history is incomplete and has no usable Checkpoint at this position.")
            for event in rows:
                if event.sequence != expected:
                    raise InspectionError("HISTORY_UNAVAILABLE", "A gap in the Session Event history prevents historical inspection.")
                expected -= 1
                budget.scan()
                reader = await BoundedReader.open(self.queries.storage, conn, "runtime_event", event.event_id, budget)
                if await reader.read(("delta",), deep=False) is not None:
                    ops = await reader.read(("delta", "operations"), deep=False)
                    for index in range(len(ops) - 1, -1, -1):
                        base = ("delta", "operations", index)
                        path = tuple(await reader.read((*base, "path")))
                        op = await reader.read((*base, "op"))
                        if op not in ("add", "replace", "remove") or not path:
                            raise FormatError("Unsupported historical State Operation.")
                        for key, wanted in tuple(remaining.items()):
                            if path == wanted[:len(path)]:
                                source = dict(kind="runtime_event", id=event.event_id, sequence=event.sequence)
                                value = _MISSING if op == "remove" else await _optional(
                                    reader, (*base, "value", *wanted[len(path):]), pointer=pointer)
                                if key == "context" and path == ("invocation",) and value is _MISSING:
                                    kind = await _optional(reader, (*base, "value", "kind"))
                                    if kind == "child_result":
                                        source["reason"] = "released"
                                values[key], sources[key] = value, source
                                del remaining[key]
                            elif wanted == path[:len(wanted)]:
                                raise FormatError("Granular Context State Operations require an updated inspection reader.")
                # Readers and their decoded Artifact caches live for one Event only.
                del reader
                if not remaining:
                    return values, sources
            before = rows[-1].sequence
        if remaining and anchor:
            reader = await BoundedReader.open(self.queries.storage, conn, "checkpoint", anchor.checkpoint_id, budget)
            sessions = await reader.read(("sessions",), deep=False)
            member = None
            for index in range(len(sessions)):
                if await reader.read(("sessions", index, "session_id")) == sid:
                    member = ("sessions", index, "state")
                    break
            if member is None:
                raise FormatError("Checkpoint metadata references a missing Session member.")
            for key, wanted in remaining.items():
                values[key] = await _optional(reader, (*member, *wanted), pointer=pointer)
                sources[key] = dict(kind="checkpoint", id=anchor.checkpoint_id, sequence=anchor.sequence)
                if key == "context" and wanted[0] == "invocation" and values[key] is _MISSING:
                    kind = await _optional(reader, (*member, "invocation", "kind"))
                    if kind == "child_result":
                        sources[key]["reason"] = "released"
            return values, sources
        if remaining:
            raise InspectionError("HISTORY_UNAVAILABLE", "No historical initialization or usable Checkpoint is available.")
        return values, sources
