"""Metadata queries and stable accepted overlays with on-demand source loading."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import case, func, select, tuple_, or_

from .models import Page
from .projections import ProjectionBatch, event_metadata
from .schema import DETAIL_TABLES, meta
from .operational import OperationalBatch
from .read_models import ProjectionCache, ProjectionConnection
from .storage import SOURCES


@dataclass(frozen=True, slots=True)
class AcceptedRecord:
    """A query owns immutable values, never mutable Writer queue entries."""
    kind: str
    identity: str
    value: object
    binding: str | None
    order: int


def pending_metadata(entries):
    return [event_metadata(entry.value) for entry in entries]


def limit_value(limit):
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000.")
    return limit


class Queries:
    def __init__(self, writer):
        self.writer = writer
        self.storage = writer.storage
        self.projection_cache = ProjectionCache(writer)
        import weakref
        self.storage.projection_cache = weakref.proxy(self.projection_cache)

    @asynccontextmanager
    async def view(self, mode="durable", *, control=False, invocation_id=None, event_records=None):
        if mode not in ("durable", "accepted"):
            raise ValueError("view must be durable or accepted.")
        locked = mode == "accepted" or self.storage.memory
        if locked:
            await self.writer.mutation.acquire()
        try:
            engine = self.storage.engine if control else self.storage.query_engine
            async with engine.connect() as raw:
                if not self.storage.sqlite:
                    raw = await raw.execution_options(isolation_level="REPEATABLE READ")
                async with raw.begin():
                    metadata = (await raw.execute(select(meta.tables['persistence_meta'].c.last_store_order,
                        meta.tables['persistence_meta'].c.store_id))).one()
                    order, store_id = metadata
                    pending=[]
                    if mode == 'accepted':
                        with self.writer.queue_lock:
                            for entry in self.writer.pending:
                                if entry.kind == 'runtime_event':order += 1
                                pending.append(AcceptedRecord(entry.kind,entry.identity,entry.value,entry.binding,order))
                    pending=tuple(pending)
                    # The SQL snapshot and accepted tail now share a boundary.
                    # Disk readers must not hold admission/mutation during UI
                    # reconstruction or a long query.
                    if locked and not self.storage.memory:
                        self.writer.mutation.release()
                        locked = False
                    if pending and invocation_id:
                        pending=await self._scope_pending(raw,pending,invocation_id)
                    conn = raw if control else ProjectionConnection(raw,self.projection_cache,metadata.last_store_order,store_id,scope=invocation_id)
                    overlay = OperationalBatch(conn) if control else ProjectionBatch(conn,self.storage.source,graph_loader=self.storage.workflow_graph)
                    runtime=tuple(entry for entry in pending if entry.kind=='runtime_event')
                    if runtime:
                        records=await self.writer.cpu(pending_metadata,runtime)
                        if event_records is not None:
                            event_records.extend(zip(runtime, records))
                        for entry,light in zip(runtime,records):
                            if control:await overlay.runtime(light,entry.order)
                            else:await overlay.runtime(light,entry.order,entry.binding)
                    yield conn, overlay, pending, order
        finally:
            if locked:
                self.writer.mutation.release()

    async def _scope_pending(self, conn, pending, invocation_id):
        """Select one Root family, including Children not yet stored in SQL."""
        parents={}
        for entry in pending:
            if entry.kind!='runtime_event':continue
            event=entry.value
            if event.event_name=='child_invocation.planned':
                parents.update((unit.child_invocation_id,event.invocation_id) for unit in event.payload.units)
            for operation in event.delta.operations if event.delta else ():
                if operation.op=='remove':continue
                path=operation.path
                plans=(operation.value,) if path[:2]==('invocation','child_plans') and len(path)==3 else (
                    operation.value.child_plans.values() if path==('invocation',) and operation.value else ())
                for plan in plans:
                    parents.update((unit.invocation_id,event.invocation_id) for unit in plan.units)
        ancestors={invocation_id}
        root=invocation_id
        while root in parents:
            root=parents[root]
            if root in ancestors:raise ValueError('Cyclic pending Child ownership.')
            ancestors.add(root)
        table=meta.tables['invocations']
        known=await conn.scalar(select(table.c.root_invocation_id).where(table.c.invocation_id.in_(ancestors)).limit(1))
        root=known or root
        rows=(await conn.execute(select(table.c.invocation_id,table.c.session_id).where(table.c.root_invocation_id==root))).all()
        members={row.invocation_id for row in rows}|{root}
        while True:
            children={child for child,parent in parents.items() if parent in members}
            if children<=members:break
            members.update(children)
        sessions={row.session_id for row in rows}
        sessions.update(entry.value.session_id for entry in pending if entry.kind=='runtime_event' and entry.value.invocation_id in members)
        return tuple(entry for entry in pending if entry.kind!='runtime_event'
            or entry.value.invocation_id in members
            or entry.value.invocation_id is None and entry.value.session_id in sessions)

    async def _rows(
        self,
        conn,
        overlay,
        name,
        filters,
        order,
        limit,
        cursor=None,
        *,
        extra_where=(),
        extra_filter=lambda row: True,
    ):
        table = meta.tables[name]
        keys = list(table.primary_key.columns.keys())
        order = tuple(order)
        predicate = [table.c[k] == v for k, v in filters.items()]
        where = [*predicate, *extra_where]
        if cursor is not None:
            if not isinstance(cursor, (list, tuple)) or len(cursor) != len(order):
                raise ValueError("Invalid cursor.")
            where.append(tuple_(*(table.c[k] for k in order)) > tuple(cursor))
        changed = overlay.rows.get(name, {})
        # Fetch enough candidates to compensate for overlay removals from filters.
        if name in DETAIL_TABLES and not extra_where:
            result=await conn.detail_rows(name,filters,order=order,cursor=cursor,limit=limit+len(changed)+1)
            if cursor is not None:result=[row for row in result if tuple(row[k] for k in order)>tuple(cursor)]
        else:
            result = (await conn.execute(select(table).where(*where)
                .order_by(*(table.c[k] for k in order)).limit(limit + len(changed) + 1))).mappings()
        candidates = {tuple(row[k] for k in keys): dict(row) for row in result}
        for key, row in changed.items():
            candidates.pop(key, None)
            if row is None:
                continue
            if (
                extra_filter(row)
                and all(row.get(k) == v for k, v in filters.items())
                and (cursor is None or tuple(row.get(k) for k in order) > tuple(cursor))
            ):
                candidates[key] = dict(row)
        rows = sorted(
            candidates.values(), key=lambda row: tuple(row.get(k) for k in order)
        )
        page = rows[:limit]
        next_cursor = (
            tuple(page[-1][k] for k in order) if len(rows) > limit and page else None
        )
        return page, next_cursor

    async def source_record(self, kind, identity):
        async with self.view() as (conn, _, _, _):
            return await self.storage.source(conn, kind, identity)

    async def definitions(self, kind, identity_filter, limit, cursor):
        limit_value(limit)
        name, _, _ = SOURCES[kind]
        filters = (
            {
                (
                    "workflow_id" if kind == "workflow_revision" else "operator_id"
                ): identity_filter
            }
            if identity_filter
            else {}
        )
        order = (
            ("registered_at_us", "revision_id")
            if kind == "workflow_revision"
            else ("operator_id", "version", "definition_id")
        )
        async with self.view() as (conn, _overlay, _, frontier):
            table = meta.tables[name]
            columns = [
                c
                for c in table.columns
                if c.name not in ("snapshot_text", "definition_text")
            ]
            where = [table.c[k] == v for k, v in filters.items()]
            if cursor is not None:
                if len(cursor) != len(order):
                    raise ValueError("Invalid cursor.")
                where.append(tuple_(*(table.c[k] for k in order)) > tuple(cursor))
            rows = [
                dict(r)
                for r in (
                    await conn.execute(
                        select(*columns)
                        .where(*where)
                        .order_by(*(table.c[k] for k in order))
                        .limit(limit + 1)
                    )
                ).mappings()
            ]
            return Page(
                tuple(rows[:limit]),
                tuple(rows[limit - 1][k] for k in order) if len(rows) > limit else None,
                frontier,
            )

    async def workflow_catalog(self, registered_ids, search, limit, cursor, registered_only=False):
        """Group metadata before paging, with ID fuzzy matching or hash prefixes."""
        limit_value(limit)
        revisions = meta.tables["workflow_revisions"]
        table = select(
            revisions.c.workflow_id, revisions.c.revision_id, revisions.c.workflow_version,
            revisions.c.definition_hash, revisions.c.registered_at_us,
            func.max(revisions.c.registered_at_us).over(partition_by=revisions.c.workflow_id).label("workflow_registered_at_us"),
        ).subquery()
        # Ordered characters also match abbreviated IDs, such as 'ordproc'.
        escaped = [char.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") for char in search if not char.isspace()]
        id_match = func.lower(table.c.workflow_id).like("%" + "%".join(escaped) + "%", escape="\\")
        prefix = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        revision_match = or_(
            func.lower(table.c.definition_hash).like(prefix + "%", escape="\\"),
            func.lower(table.c.revision_id).like(prefix + "%", escape="\\") & (":" in search),
        ) & bool(search)
        matches = select(
            table.c.workflow_id, table.c.revision_id, table.c.workflow_version,
            table.c.workflow_registered_at_us,
            case((revision_match, table.c.revision_id), else_=None).label("matched_revision_id"),
            func.row_number().over(partition_by=table.c.workflow_id, order_by=(case((revision_match, 0), else_=1), table.c.registered_at_us.desc(), table.c.revision_id.desc())).label("position"),
        ).where(or_(id_match, revision_match)).subquery()
        ranked = case((matches.c.workflow_id.in_(registered_ids), 0), else_=1)
        newest_first = -matches.c.workflow_registered_at_us
        statement = select(
            matches.c.workflow_id, matches.c.revision_id, matches.c.workflow_version,
            matches.c.workflow_registered_at_us.label("registered_at_us"),
            matches.c.matched_revision_id, ranked.label("registration_rank"),
        ).where(matches.c.position == 1)
        if registered_only:
            statement = statement.where(matches.c.workflow_id.in_(registered_ids))
        if cursor is not None:
            if not isinstance(cursor, (tuple, list)) or len(cursor) != 3 or type(cursor[0]) is not int or cursor[0] not in (0, 1) or type(cursor[1]) is not int or not isinstance(cursor[2], str):
                raise ValueError("Invalid Workflow catalog cursor.")
            statement = statement.where(tuple_(ranked, newest_first, matches.c.workflow_id) > tuple(cursor))
        async with self.view() as (conn, _, _, frontier):
            rows = [dict(row) for row in (await conn.execute(statement.order_by(ranked, newest_first, matches.c.workflow_id).limit(limit + 1))).mappings()]
        return Page(tuple(rows[:limit]), (rows[limit - 1]["registration_rank"], -rows[limit - 1]["registered_at_us"], rows[limit - 1]["workflow_id"]) if len(rows) > limit else None, frontier)

    async def workflow_revisions(self, workflow_id, prefix, limit, cursor):
        """Return newest Revision metadata first, without loading definitions."""
        limit_value(limit)
        table = meta.tables["workflow_revisions"]
        statement = select(*(column for column in table.columns if column.name != "snapshot_text")).where(table.c.workflow_id == workflow_id)
        if prefix:
            escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            statement = statement.where(or_(
                func.lower(table.c.definition_hash).like(escaped + "%", escape="\\"),
                func.lower(table.c.revision_id).like(escaped + "%", escape="\\"),
            ))
        if cursor is not None:
            if not isinstance(cursor, (tuple, list)) or len(cursor) != 2 or type(cursor[0]) is not int or cursor[0] < 0 or not isinstance(cursor[1], str):
                raise ValueError("Invalid Workflow Revision cursor.")
            statement = statement.where(tuple_(table.c.registered_at_us, table.c.revision_id) < tuple(cursor))
        async with self.view() as (conn, _, _, frontier):
            rows = [dict(row) for row in (await conn.execute(statement.order_by(table.c.registered_at_us.desc(), table.c.revision_id.desc()).limit(limit + 1))).mappings()]
        return Page(tuple(rows[:limit]), (rows[limit - 1]["registered_at_us"], rows[limit - 1]["revision_id"]) if len(rows) > limit else None, frontier)

    async def events(
        self, kind, stream, after, limit, mode, occurrence_id=None, invocation_id=None
    ):
        limit_value(limit)
        if type(after) is not int or after < 0:
            raise ValueError("after must be nonnegative.")
        name, _, _ = SOURCES[kind]
        field = "session_id" if kind == "runtime_event" else "invocation_id"
        async with self.view(mode, invocation_id=invocation_id) as (conn, _, pending, frontier):
            table = meta.tables[name]
            where = [table.c[field] == stream, table.c.sequence > after]
            if occurrence_id is not None:
                where.append(table.c.occurrence_id == occurrence_id)
            if invocation_id is not None:
                where.append(table.c.invocation_id == invocation_id)
            rows = (
                await conn.execute(
                    select(table.c.event_id, table.c.sequence)
                    .where(*where)
                    .order_by(table.c.sequence)
                    .limit(limit + 1)
                )
            ).all()
            items = [
                await self.storage.source(conn, kind, row.event_id) for row in rows
            ]
            for entry in pending:
                if (
                    entry.kind == kind
                    and getattr(entry.value, field) == stream
                    and entry.value.sequence > after
                    and (
                        invocation_id is None
                        or entry.value.invocation_id == invocation_id
                    )
                    and (
                        occurrence_id is None
                        or entry.value.occurrence_id == occurrence_id
                    )
                ):
                    items.append(await self.writer.cpu(entry.value.to_record))
            items.sort(key=lambda r: r["sequence"])
            return Page(
                tuple(items[:limit]),
                (items[limit - 1]["sequence"],) if len(items) > limit else None,
                frontier,
            )

    async def list_rows(
        self, name, filters, limit, cursor, mode="durable", include_planned=True
    ):
        limit_value(limit)
        orders = {
            "sessions": ("session_id",),
            "invocations": ("invocation_id",),
            "node_executions": ("occurrence_id",),
            "operator_calls": ("call_id",),
            "waits": ("wait_id",),
        }
        async with self.view(mode, control=name in ("sessions","invocations"), invocation_id=filters.get("invocation_id")) as (conn, overlay, _, frontier):
            # Stable identity cursors avoid ambiguous ordering for planned NULL timestamps.
            if name == "invocations" and not include_planned:
                filters = dict(filters)
                table = meta.tables[name]
                rows, _ = await self._rows(
                    conn,
                    overlay,
                    name,
                    filters,
                    orders[name],
                    limit + 1,
                    cursor,
                    extra_where=(table.c.status.is_not(None),),
                    extra_filter=lambda row: row.get("status") is not None,
                )
                rows = [r for r in rows if r.get("status") is not None]
                page = rows[:limit]
                next_cursor = (
                    tuple(page[-1][k] for k in orders[name])
                    if len(rows) > limit
                    else None
                )
            else:
                page, next_cursor = await self._rows(
                    conn, overlay, name, filters, orders[name], limit, cursor
                )
            if name == "sessions" and mode == "accepted":
                for row in page:
                    durable = await self.storage.row(
                        conn, "sessions", session_id=row["session_id"]
                    )
                    row["accepted_sequence"] = row["durable_sequence"]
                    row["durable_sequence"] = (durable or {}).get("durable_sequence", 0)
            return Page(tuple(page), next_cursor, frontier)

    async def get_row(self, name, identity, mode):
        async with self.view(mode, control=name in ("sessions","invocations"), invocation_id=identity.get("invocation_id")) as (_conn, overlay, _, _):
            return await overlay.get(name, **identity)

    async def changes(self, iid, after, limit, root_graph, mode):
        limit_value(limit)
        if type(after) is not int or after < 0:
            raise ValueError("after must be nonnegative.")
        async with self.view(mode, invocation_id=iid) as (conn, overlay, _, frontier):
            graph = meta.tables["graph_changes"]
            invs = meta.tables["invocations"]
            field = "store_order" if root_graph else "sequence"
            ids = {iid}
            if root_graph:
                ids.update(
                    (
                        await conn.execute(
                            select(invs.c.invocation_id).where(
                                invs.c.root_invocation_id == iid
                            )
                        )
                    ).scalars()
                )
                ids.update(
                    r["invocation_id"]
                    for r in overlay.rows["invocations"].values()
                    if r and r["root_invocation_id"] == iid
                )
            # limit counts Event groups, never splits an atomic Event delta.
            groups = (
                await conn.execute(
                    select(graph.c.event_id, graph.c[field])
                    .where(graph.c.invocation_id.in_(ids), graph.c[field] > after)
                    .distinct()
                    .order_by(graph.c[field])
                    .limit(limit + 1)
                )
            ).all()
            event_ids = {row.event_id for row in groups}
            event_ids.update(
                r["event_id"]
                for r in overlay.rows["graph_changes"].values()
                if r and r["invocation_id"] in ids and r[field] > after
            )
            stored = (
                (
                    await conn.execute(
                        select(graph).where(graph.c.event_id.in_(event_ids))
                    )
                ).mappings()
                if event_ids
                else ()
            )
            items = {(r["event_id"], r["change_index"]): dict(r) for r in stored}
            items.update(
                {
                    key: dict(r)
                    for key, r in overlay.rows["graph_changes"].items()
                    if r and r["event_id"] in event_ids
                }
            )
            ranks = sorted({r[field] for r in items.values()})
            chosen = set(ranks[:limit])
            rows = sorted(
                (r for r in items.values() if r[field] in chosen),
                key=lambda r: (r[field], r["change_index"]),
            )
            return Page(
                tuple(rows),
                (ranks[limit - 1],) if len(ranks) > limit else None,
                frontier,
            )

    async def _resolve(self, conn, pending, reference):
        if reference is None:
            return {"available": False, "value": None}
        if (
            set(reference) != {"kind", "id", "path"}
            or reference["kind"] not in SOURCES
            or not isinstance(reference["path"], list)
        ):
            raise ValueError("Invalid SourceRef.")
        if (
            not isinstance(reference["id"], str)
            or not reference["id"]
            or any(
                type(t) not in (str, int) or type(t) is int and t < 0
                for t in reference["path"]
            )
        ):
            raise ValueError("Invalid SourceRef identity or path.")
        record = None
        for entry in pending:
            if entry.kind == reference["kind"] and entry.identity == reference["id"]:
                record = await self.writer.cpu(entry.value.to_record)
                break
        if record is None:
            record = await self.storage.source(conn, reference["kind"], reference["id"])
        if record is None:
            return {"available": False, "value": None}
        try:
            for token in reference["path"]:
                if type(token) not in (str, int):
                    raise ValueError("Invalid path token.")
                record = record[token]
        except (KeyError, IndexError, TypeError):
            return {"available": False, "value": None}
        return {"available": True, "value": record}

    async def resolve(self, reference):
        async with self.view("accepted") as (conn, _, pending, _):
            return await self._resolve(conn, pending, reference)

    async def detail(self, name, identity, mode):
        async with self.view(mode,invocation_id=identity['invocation_id']) as (conn, overlay, pending, _):
            row = await overlay.get(name, **identity)
            if row is None:
                return None
            result = dict(row)
            inv = await overlay.get("invocations", invocation_id=row["invocation_id"])
            revision = await self.storage.source(
                conn, "workflow_revision", inv["workflow_revision_id"]
            )
            definitions = revision["definition"]["nodes"]
            if name == "node_executions":
                definition = next(
                    (node for node in definitions if node["id"] == row["node_id"]), None
                )
            else:
                node = await overlay.get(
                    "node_executions",
                    invocation_id=row["invocation_id"],
                    occurrence_id=row["occurrence_id"],
                )
                definition = next(
                    (n for n in definitions if n["id"] == node["node_id"]), None
                )
                if name == "operator_calls" and row.get("operator_definition_id"):
                    definition = await self.storage.source(
                        conn, "operator_definition", row["operator_definition_id"]
                    )
            result["definition"] = definition
            for key in list(row):
                if key.endswith("_ref"):
                    result[key[:-4]] = await self._resolve(conn, pending, row[key])
            if name == "node_executions" and not row.get("input_ref"):
                stored=await conn.detail_rows('operator_calls',dict(invocation_id=row['invocation_id'],occurrence_id=row['occurrence_id']))
                by_id = {r["call_id"]: dict(r) for r in stored}
                by_id.update(
                    {
                        r["call_id"]: dict(r)
                        for r in overlay.rows["operator_calls"].values()
                        if r
                        and r["invocation_id"] == row["invocation_id"]
                        and r["occurrence_id"] == row["occurrence_id"]
                    }
                )
                ordered = sorted(
                    by_id.values(),
                    key=lambda r: (r["unit_index"], r["started_at_us"], r["call_id"]),
                )
                result["call_inputs"] = [
                    {
                        "call_id": r["call_id"],
                        "unit_index": r["unit_index"],
                        "input_ref": r.get("input_ref"),
                    }
                    for r in ordered
                ]
                if len(ordered) == 1:
                    result["input"] = await self._resolve(
                        conn, pending, ordered[0].get("input_ref")
                    )
            start, end = row.get("started_at_us"), row.get("completed_at_us")
            if start is not None:
                result["elapsed_us"] = end - start if end is not None else None
            return result

    async def edge(
        self, iid, edge_id, limit, mode, evaluations_cursor=None, changes_cursor=None
    ):
        limit_value(limit)
        for cursor in (evaluations_cursor, changes_cursor):
            if cursor is not None and (
                not isinstance(cursor, (tuple, list))
                or len(cursor) != 2
                or any(type(v) is not int or v < 0 for v in cursor)
            ):
                raise ValueError("Invalid Edge detail cursor.")
        async with self.view(mode, invocation_id=iid) as (conn, overlay, pending, _):
            inv = await overlay.get("invocations", invocation_id=iid)
            if inv is None:
                return {
                    "definition": None,
                    "evaluations": (),
                    "changes": (),
                    "evaluations_cursor": None,
                    "changes_cursor": None,
                }
            revision = await self.storage.source(
                conn, "workflow_revision", inv["workflow_revision_id"]
            )
            definition = next(
                (e for e in revision["definition"]["edges"] if e["id"] == edge_id), None
            )
            evaluations, events, graph = (
                meta.tables[n]
                for n in ("edge_evaluations", "runtime_events", "graph_changes")
            )
            where = [
                evaluations.c.invocation_id == iid,
                evaluations.c.edge_id == edge_id,
            ]
            if evaluations_cursor is not None:
                where.append(
                    tuple_(events.c.sequence, evaluations.c.result_index)
                    > tuple(evaluations_cursor)
                )
            stored = (
                await conn.execute(
                    select(evaluations, events.c.sequence)
                    .join(events, evaluations.c.event_id == events.c.event_id)
                    .where(*where)
                    .order_by(events.c.sequence, evaluations.c.result_index)
                    .limit(limit + 1)
                )
            ).mappings()
            evals = {(r["event_id"], r["result_index"]): dict(r) for r in stored}
            sequences = {
                e.identity: e.value.sequence
                for e in pending
                if e.kind == "runtime_event"
            }
            for key, row in overlay.rows["edge_evaluations"].items():
                if row and row["invocation_id"] == iid and row["edge_id"] == edge_id:
                    value = dict(row, sequence=sequences[row["event_id"]])
                    if evaluations_cursor is None or (
                        value["sequence"],
                        value["result_index"],
                    ) > tuple(evaluations_cursor):
                        evals[key] = value
            evals = sorted(
                evals.values(), key=lambda r: (r["sequence"], r["result_index"])
            )
            where = [
                graph.c.invocation_id == iid,
                graph.c.entity_type == "edge",
                graph.c.change["edge_id"].as_string() == edge_id,
            ]
            if changes_cursor is not None:
                where.append(
                    tuple_(graph.c.sequence, graph.c.change_index)
                    > tuple(changes_cursor)
                )
            stored = (
                await conn.execute(
                    select(graph)
                    .where(*where)
                    .order_by(graph.c.sequence, graph.c.change_index)
                    .limit(limit + 1)
                )
            ).mappings()
            changes = {(r["event_id"], r["change_index"]): dict(r) for r in stored}
            for key, row in overlay.rows["graph_changes"].items():
                if (
                    row
                    and row["invocation_id"] == iid
                    and row["entity_type"] == "edge"
                    and row["change"].get("edge_id") == edge_id
                ) and (
                    changes_cursor is None
                    or (
                        row["sequence"],
                        row["change_index"],
                    )
                    > tuple(changes_cursor)
                ):
                    changes[key] = dict(row)
            changes = sorted(
                changes.values(), key=lambda r: (r["sequence"], r["change_index"])
            )
            return {
                "definition": definition,
                "evaluations": tuple(evals[:limit]),
                "changes": tuple(changes[:limit]),
                "evaluations_cursor": (
                    evals[limit - 1]["sequence"],
                    evals[limit - 1]["result_index"],
                )
                if len(evals) > limit
                else None,
                "changes_cursor": (
                    changes[limit - 1]["sequence"],
                    changes[limit - 1]["change_index"],
                )
                if len(changes) > limit
                else None,
            }

    async def execution_snapshot(self, iid, mode):
        """Resolve a Root result and its graph waits in one consistent view."""
        async with self.view(mode, control=True, invocation_id=iid) as (conn, overlay, pending, _):
            row = await overlay.get("invocations", invocation_id=iid)
            if row is None:
                return None
            snapshot = dict(row)
            for field in ("output", "error"):
                snapshot[field] = (await self._resolve(conn, pending, row.get(field + "_ref")))["value"]
            snapshot["waits"] = []
            cursor = None
            while True:
                members, cursor = await self._rows(conn, overlay, "invocations", {"root_invocation_id": iid}, ("invocation_id",), 1000, cursor)
                for member in members:
                    if member.get("status") not in ("running", "waiting", "settling") or member.get("pending_outcome") in ("failed", "cancelled"):
                        continue
                    wait_cursor = None
                    while True:
                        waits, wait_cursor = await self._rows(conn, overlay, "active_waits", {"invocation_id": member["invocation_id"], "status": "waiting"}, ("wait_id",), 1000, wait_cursor)
                        for wait in waits:
                            request = (await self._resolve(conn, pending, wait.get("request_ref")))["value"]
                            snapshot["waits"].append({"id": wait["wait_id"], "kind": wait["kind"], "request": request})
                        if wait_cursor is None:
                            break
                if cursor is None:
                    break
            return snapshot

    async def activation_candidates(self, now_us, limit, cursor=None):
        """Bounded durable Root candidates, including Child Timer deadlines."""
        limit_value(limit)
        inv = meta.tables["invocations"]
        root = inv.alias("root")
        sess = meta.tables["sessions"]
        waits = meta.tables["active_waits"]
        nodes = meta.tables["active_nodes"]
        due = select(waits.c.wait_id).join(inv, waits.c.invocation_id == inv.c.invocation_id).where(
            inv.c.root_invocation_id == root.c.invocation_id,
            waits.c.status == "waiting", waits.c.deadline_at_us <= now_us,
        ).exists()
        work = select(nodes.c.occurrence_id).join(inv, nodes.c.invocation_id == inv.c.invocation_id).where(
            inv.c.root_invocation_id == root.c.invocation_id,
            nodes.c.status.in_(("ready", "running")),
        ).exists()
        query = select(root).join(sess, sess.c.latest_invocation_id == root.c.invocation_id).where(
            root.c.parent_invocation_id.is_(None), root.c.control_state == "active",
            root.c.status.in_(("created", "running", "waiting", "settling")),
            or_(root.c.status.in_(("created", "running")), work, due),
        ).order_by(root.c.invocation_id).limit(limit + 1)
        if cursor is not None:
            query = query.where(root.c.invocation_id > cursor)
        async with self.view(control=True) as (conn, _, _, frontier):
            rows = [dict(row) for row in (await conn.execute(query)).mappings()]
        return Page(tuple(rows[:limit]), rows[limit - 1]["invocation_id"] if len(rows) > limit else None, frontier)

    async def recovery_bundle(self, root_invocation_id, *, full_history=False):
        """Read one durable Root graph and its histories from one MVCC snapshot."""
        from .checkpoints import progress_order
        async with self.view(control=True) as (conn, _, _, frontier):
            invocations = meta.tables["invocations"]
            root = await self.storage.row(
                conn, "invocations", invocation_id=root_invocation_id
            )
            if root is None or root["parent_invocation_id"] is not None:
                raise ValueError("Recovery requires a known Root Invocation.")
            session = await self.storage.row(
                conn, "sessions", session_id=root["session_id"]
            )
            if session["latest_invocation_id"] != root_invocation_id:
                raise ValueError(
                    "An historical Invocation cannot replace its Session's latest Invocation."
                )
            rows = [
                dict(row)
                for row in (
                    await conn.execute(
                        select(invocations).where(
                            invocations.c.root_invocation_id == root_invocation_id
                        )
                    )
                ).mappings()
            ]
            checkpoints = meta.tables["checkpoints"]
            checkpoint_id = await conn.scalar(
                select(checkpoints.c.checkpoint_id)
                .where(checkpoints.c.root_session_id == root["session_id"])
                .order_by(*progress_order())
                .limit(1)
            )
            checkpoint = (
                await self.storage.source(conn, "checkpoint", checkpoint_id)
                if checkpoint_id and not full_history
                else None
            )
            anchors = (
                {
                    item["session_id"]: item["state"]["sequence"]
                    for item in checkpoint["sessions"]
                }
                if checkpoint
                else {}
            )
            events = meta.tables["runtime_events"]
            histories = {}
            for sid in sorted({row["session_id"] for row in rows} | set(anchors)):
                current = await self.storage.row(conn, "sessions", session_id=sid)
                if current is None:
                    raise ValueError("Recovery graph is missing a Session.")
                after = anchors.get(sid, 0)
                if (
                    after > current["durable_sequence"]
                    or after + 1 < current["history_start_sequence"]
                ):
                    raise ValueError(
                        "Recovery history has no usable Checkpoint anchor."
                    )
                ids = (
                    await conn.scalars(
                        select(events.c.event_id)
                        .where(
                            events.c.session_id == sid,
                            events.c.sequence > after,
                            events.c.sequence <= current["durable_sequence"],
                        )
                        .order_by(events.c.sequence)
                    )
                ).all()
                histories[sid] = [
                    await self.storage.source(conn, "runtime_event", identity)
                    for identity in ids
                ]
            users = meta.tables["user_events"]
            ids = [row["invocation_id"] for row in rows]
            user_frontiers = dict(
                (
                    await conn.execute(
                        select(users.c.invocation_id, func.max(users.c.sequence))
                        .where(users.c.invocation_id.in_(ids))
                        .group_by(users.c.invocation_id)
                    )
                ).all()
            )
            # Bindings are immutable CallStarted facts. Core decides which
            # Calls still require an implementation after restoring the graph.
            from sqlalchemy import JSON, cast, type_coerce
            body=type_coerce(events.c.payload_text,JSON) if conn.dialect.name=='sqlite' else cast(events.c.payload_text,JSON)
            definitions=meta.tables['operator_definitions']
            bindings={}
            for item in (await conn.execute(select(events.c.invocation_id,
                body['call_id'].as_string().label('call_id'),definitions.c.operator_id,definitions.c.definition_id)
                .join(definitions,events.c.operator_definition_id==definitions.c.definition_id)
                .where(events.c.invocation_id.in_(ids),events.c.event_name=='operator_call.started')
                .order_by(events.c.store_order))).mappings():
                bindings[(item['invocation_id'],item['call_id'])]=dict(item)
            return {
                "root_session_id": root["session_id"],
                "checkpoint": checkpoint,
                "events": histories,
                "user_frontiers": user_frontiers,
                "operator_bindings": list(bindings.values()),
                "frontier": frontier,
            }

    async def statistics(self, entity, revision):
        choices = {
            "node": ("node_executions", "node_id"),
            "operator": ("operator_calls", "operator_id"),
            "edge": ("edge_evaluations", "edge_id"),
        }
        if entity not in choices:
            raise ValueError("entity must be node, operator or edge.")
        name, field = choices[entity]
        table = meta.tables[name]
        invs = meta.tables["invocations"]
        groups = [invs.c.workflow_revision_id, table.c[field]]
        columns = [*groups, func.count().label("count")]
        if entity == "edge":
            groups.extend([table.c.source_status, table.c.condition_result])
            columns.extend(
                [
                    table.c.source_status,
                    table.c.condition_result,
                    func.avg(table.c.duration_ns).label("mean_duration_ns"),
                ]
            )
        else:
            groups.append(table.c.status)
            columns.append(table.c.status)
            if entity == "operator":
                groups.append(table.c.operator_definition_id)
                columns.append(table.c.operator_definition_id)
                columns.extend(
                    [
                        func.avg(table.c.execution_duration_ns).label(
                            "mean_execution_ns"
                        ),
                        func.avg(table.c.queue_duration_ns).label("mean_queue_ns"),
                    ]
                )
            else:
                columns.append(
                    func.avg(table.c.completed_at_us - table.c.started_at_us).label(
                        "mean_elapsed_us"
                    )
                )
        statement = select(*columns).select_from(
            table.join(invs, table.c.invocation_id == invs.c.invocation_id)
        )
        if revision:
            statement = statement.where(invs.c.workflow_revision_id == revision)
        async with self.view() as (conn, _, _, _):
            return tuple(
                dict(r)
                for r in (
                    await conn.execute(statement.group_by(*groups).order_by(*groups))
                ).mappings()
            )
