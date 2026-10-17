"""Async SQLAlchemy transactions and lossless source-record loading."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import event, insert, inspect, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from .codec import DOMAIN, Encoded, EncodingMemo, assign, canonical, encode
from .models import ConflictError, FormatError
from .projections import ProjectionBatch, light_record
from .schema import meta

FORMAT_VERSION = 4
PROJECTION_VERSION = 4
SOURCES = {
    "workflow_revision": ("workflow_revisions", "revision_id", "snapshot_text"),
    "operator_definition": ("operator_definitions", "definition_id", "definition_text"),
    "runtime_event": ("runtime_events", "event_id", None),
    "user_event": ("user_events", "event_id", None),
    "checkpoint": ("checkpoints", "checkpoint_id", "checkpoint_text"),
}


@dataclass(slots=True)
class Prepared:
    kind: str
    identity: str
    encoded: Encoded
    light: dict
    binding: str | None = None
    anchor: bool = False


def prepare(entries, config):
    memo = EncodingMemo(config.encoding_cache_bytes)
    result = []
    size, seen = 0, set()
    for entry in entries:
        record = entry.value.to_record()
        encoded = encode(record, entry.kind, config.artifact_threshold_bytes, memo)
        addition = encoded.encoded_bytes - sum(
            len(a["body"]) for h, a in encoded.artifacts.items() if h in seen
        )
        if result and size + addition > config.batch_bytes:
            break
        result.append(
            Prepared(
                entry.kind,
                entry.identity,
                encoded,
                light_record(record, availability=entry.kind == "checkpoint"),
                entry.binding,
                entry.anchor,
            )
        )
        size += addition
        seen.update(encoded.artifacts)
    return result


class Storage:
    def __init__(self, url):
        parsed = make_url(url)
        if parsed.drivername not in ("sqlite+aiosqlite", "postgresql+asyncpg"):
            raise ValueError("Use sqlite+aiosqlite or postgresql+asyncpg.")
        self.sqlite = parsed.drivername == "sqlite+aiosqlite"
        self.memory = self.sqlite and parsed.database in (None, "", ":memory:")
        self.url = parsed
        self.engine = None
        self._lock_file = None
        self._pg_lock = None

    async def open(self):
        if self.sqlite and not self.memory:
            import fcntl

            self._lock_file = open(  # noqa: ASYNC230, SIM115 -- writer lifetime lock
                os.path.abspath(self.url.database) + ".writer.lock", "a"
            )
            try:
                fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                self._lock_file.close()
                self._lock_file = None
                raise ConflictError(
                    "Another Persistence writer owns this SQLite database."
                ) from None
        # sqlite3's statement cache can retain bound payloads after COMMIT.
        self.engine = create_async_engine(
            self.url, connect_args={"cached_statements": 0} if self.sqlite else {}
        )
        if self.sqlite:

            @event.listens_for(self.engine.sync_engine, "connect")
            def configure(dbapi_connection, _):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute("PRAGMA synchronous=FULL")
                cursor.execute("PRAGMA busy_timeout=5000")
                cursor.close()
                dbapi_connection.isolation_level = None
        else:
            self._pg_lock = await self.engine.connect()
            if not await self._pg_lock.scalar(
                text(
                    "SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema() || :suffix))"
                ),
                {"suffix": ":autoagent-persistence"},
            ):
                raise ConflictError(
                    "Another Persistence writer owns this PostgreSQL storage."
                )
            await self._pg_lock.commit()
        if self.sqlite:
            async with self.engine.connect() as conn:
                await conn.execute(text("PRAGMA journal_mode=WAL"))
                await conn.commit()

            @event.listens_for(self.engine.sync_engine, "begin")
            def begin(conn):
                conn.exec_driver_sql("BEGIN")

        async with self.engine.begin() as conn:
            names = await conn.run_sync(lambda c: inspect(c).get_table_names())
            if names:
                if not set(meta.tables).issubset(names):
                    raise FormatError("Database is not a complete Persistence schema.")
                info = (
                    (await conn.execute(select(meta.tables["persistence_meta"].c.format_version,
                                               meta.tables["persistence_meta"].c.projection_version)))
                    .mappings()
                    .all()
                )
                if (
                    len(info) != 1
                    or info[0]["format_version"] != FORMAT_VERSION
                    or info[0]["projection_version"] != PROJECTION_VERSION
                ):
                    raise FormatError(
                        "Unsupported Persistence format/projection version."
                    )
                expected = {t.name: set(t.columns.keys()) for t in meta.tables.values()}
                actual = await conn.run_sync(
                    lambda c: {
                        name: {col["name"] for col in inspect(c).get_columns(name)}
                        for name in expected
                    }
                )
                if actual != expected:
                    raise FormatError(
                        "Persistence columns do not match the format version."
                    )
            else:
                await conn.run_sync(meta.create_all)
                await conn.execute(
                    insert(meta.tables["persistence_meta"]).values(
                        id=1,
                        format_version=FORMAT_VERSION,
                        projection_version=PROJECTION_VERSION,
                        last_store_order=0,
                        store_id=uuid4().hex,
                    )
                )

    async def close(self):
        if self._pg_lock is not None:
            await self._pg_lock.close()
            self._pg_lock = None
        if self.engine is not None:
            await self.engine.dispose()
            self.engine = None
        if self._lock_file is not None:
            self._lock_file.close()
            self._lock_file = None

    async def row(self, conn, name, **identity):
        table = meta.tables[name]
        row = (
            (
                await conn.execute(
                    select(table).where(*(table.c[k] == v for k, v in identity.items()))
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row else None

    async def source(self, conn, kind, identity):
        if kind not in SOURCES:
            raise ValueError("Unknown source kind.")
        name, key, body = SOURCES[kind]
        row = await self.row(conn, name, **{key: identity})
        if row is None:
            return None
        if body:
            record = json.loads(row[body])
        elif kind == "runtime_event":
            record = {
                k: row[k]
                for k in (
                    "schema_version",
                    "session_id",
                    "invocation_id",
                    "sequence",
                    "event_name",
                    "occurred_at_us",
                )
            }
            record.update(
                id=row["event_id"],
                payload=json.loads(row["payload_text"]),
                delta=json.loads(row["delta_text"])
                if row["delta_text"] is not None
                else None,
            )
        else:
            record = {
                k: row[k]
                for k in (
                    "session_id",
                    "invocation_id",
                    "sequence",
                    "kind",
                    "occurrence_id",
                    "occurred_at_us",
                )
            }
            record.update(id=row["event_id"], payload=json.loads(row["payload_text"]))
        refs = meta.tables["artifact_refs"]
        deps = meta.tables["artifact_dependencies"]
        cache, loading = {}, set()

        async def artifact(digest):
            if digest in cache:
                return cache[digest]
            if digest in loading:
                raise FormatError("Cyclic Artifact dependencies.")
            loading.add(digest)
            stored = await self.row(conn, "artifacts", hash=digest)
            if stored is None:
                raise FormatError("Missing Artifact.")
            value = json.loads(stored["body"])
            for child in (
                await conn.execute(
                    select(deps)
                    .where(deps.c.parent_hash == digest)
                    .order_by(deps.c.slot)
                )
            ).mappings():
                value = assign(
                    value, child["path"], await artifact(child["child_hash"])
                )
            data = canonical(value)
            if (
                len(data) != stored["logical_bytes"]
                or hashlib.sha256(DOMAIN + data).hexdigest() != digest
            ):
                raise FormatError("Artifact content digest mismatch.")
            loading.remove(digest)
            cache[digest] = value
            return value

        for ref in (
            await conn.execute(
                select(refs)
                .where(refs.c.owner_kind == kind, refs.c.owner_id == identity)
                .order_by(refs.c.slot)
            )
        ).mappings():
            record = assign(record, ref["path"], await artifact(ref["artifact_hash"]))
        return record

    async def upsert(self, conn, name, rows):
        table = meta.tables[name]
        groups = {}
        for row in rows:
            if row is not None:
                groups.setdefault(tuple(sorted(row)), []).append(row)
        for columns, group in groups.items():
            statement = (sqlite_insert if self.sqlite else pg_insert)(table)
            keys = list(table.primary_key.columns.keys())
            changes = {k: statement.excluded[k] for k in columns if k not in keys}
            if changes:
                statement = statement.on_conflict_do_update(
                    index_elements=keys, set_=changes
                )
            else:
                statement = statement.on_conflict_do_nothing(index_elements=keys)
            await conn.execute(statement, group)

    async def projections(self, conn, batch):
        await self.upsert(conn, "sessions", batch.changed("sessions"))
        invs = batch.changed("invocations")
        nodes = batch.changed("node_executions")
        # The Parent occurrence FK introduces a dependency across these tables.
        pending = list(invs)
        while pending:
            ready = []
            for row in pending:
                parent = row.get("parent_invocation_id")
                if parent is None or await self.row(
                    conn,
                    "node_executions",
                    invocation_id=parent,
                    occurrence_id=row["parent_occurrence_id"],
                ):
                    ready.append(row)
            if not ready:
                raise ConflictError(
                    "Child projection references an absent Parent occurrence."
                )
            await self.upsert(conn, "invocations", ready)
            ids = {r["invocation_id"] for r in ready}
            await self.upsert(
                conn, "node_executions", [r for r in nodes if r["invocation_id"] in ids]
            )
            pending = [r for r in pending if r["invocation_id"] not in ids]
        await self.upsert(conn, "node_executions", nodes)
        for name in ("operator_calls", "waits", "edge_evaluations", "graph_changes"):
            await self.upsert(conn, name, batch.changed(name))

    async def write(self, entries):
        async with self.engine.begin() as conn:
            return await self.write_transaction(conn, entries)

    async def write_transaction(self, conn, entries):
        batch = ProjectionBatch(conn, self.source)
        order = await conn.scalar(
            select(meta.tables["persistence_meta"].c.last_store_order)
        )
        for entry in entries:
            name, key, _body_col = SOURCES[entry.kind]
            row = await self.row(conn, name, **{key: entry.identity})
            logical = entry.light
            if row is not None:
                # Admission handles logical idempotency; no second projection.
                continue
            arts = entry.encoded.artifacts
            missing = []
            hashes = list(arts)
            existing = set()
            for start in range(0, len(hashes), 500):
                existing.update(
                    (
                        await conn.execute(
                            select(meta.tables["artifacts"].c.hash).where(
                                meta.tables["artifacts"].c.hash.in_(
                                    hashes[start : start + 500]
                                )
                            )
                        )
                    ).scalars()
                )
            missing = [a for h, a in arts.items() if h not in existing]
            if missing:
                await conn.execute(
                    insert(meta.tables["artifacts"]),
                    [
                        {
                            k: a[k]
                            for k in ("hash", "value_type", "logical_bytes", "body")
                        }
                        for a in missing
                    ],
                )
                dependencies = [
                    {"parent_hash": a["hash"], "slot": i, "path": p, "child_hash": h}
                    for a in missing
                    for i, (p, h) in enumerate(a["deps"])
                ]
                if dependencies:
                    await conn.execute(
                        insert(meta.tables["artifact_dependencies"]), dependencies
                    )
            record = entry.encoded.record
            if entry.kind == "workflow_revision":
                data = {
                    "revision_id": entry.identity,
                    "workflow_id": record["workflow_id"],
                    "workflow_version": record["workflow_version"],
                    "definition_hash": record["definition_hash"],
                    "registered_at_us": time.time_ns() // 1000,
                    "snapshot_text": canonical(record).decode(),
                }
            elif entry.kind == "operator_definition":
                data = {
                    "definition_id": entry.identity,
                    "operator_id": record["operator_id"],
                    "version": record["version"],
                    "definition_text": canonical(record).decode(),
                }
            elif entry.kind == "runtime_event":
                table = meta.tables["runtime_events"]
                if await conn.scalar(
                    select(table.c.event_id).where(
                        table.c.session_id == logical["session_id"],
                        table.c.sequence == logical["sequence"],
                    )
                ):
                    raise ConflictError("Runtime Session sequence already exists.")
                session = await batch.get("sessions", session_id=logical["session_id"])
                expected = (session or {}).get("durable_sequence", 0) + 1
                if logical["sequence"] != expected:
                    raise ConflictError(
                        f"Runtime sequence expected {expected}, got {logical['sequence']}."
                    )
                order += 1
                data = {
                    k: record[k]
                    for k in (
                        "session_id",
                        "invocation_id",
                        "sequence",
                        "event_name",
                        "occurred_at_us",
                        "schema_version",
                    )
                }
                data.update(
                    event_id=entry.identity,
                    store_order=order,
                    operator_definition_id=entry.binding,
                    payload_text=canonical(record["payload"]).decode(),
                    delta_text=canonical(record["delta"]).decode()
                    if record["delta"] is not None
                    else None,
                )
                await batch.runtime(logical, order, entry.binding)
            elif entry.kind == "user_event":
                table = meta.tables["user_events"]
                latest = await conn.scalar(
                    select(table.c.sequence)
                    .where(table.c.invocation_id == logical["invocation_id"])
                    .order_by(table.c.sequence.desc())
                    .limit(1)
                )
                if logical["sequence"] != (latest or 0) + 1:
                    raise ConflictError("User Event sequence is not contiguous.")
                data = {
                    k: record[k]
                    for k in (
                        "session_id",
                        "invocation_id",
                        "sequence",
                        "kind",
                        "occurrence_id",
                        "occurred_at_us",
                    )
                }
                data.update(
                    event_id=entry.identity,
                    payload_text=canonical(record["payload"]).decode(),
                )
            else:
                data = {
                    "checkpoint_id": entry.identity,
                    "root_session_id": record["root_session_id"],
                    "captured_at_us": max(
                        s["captured_at_us"] for s in logical["sessions"]
                    ),
                    "digest": record["digest"],
                    "checkpoint_text": canonical(record).decode(),
                }
                for session in logical["sessions"]:
                    previous = await batch.get(
                        "sessions", session_id=session["session_id"]
                    )
                    if not entry.anchor and (
                        previous is None
                        or previous["durable_sequence"] < session["state"]["sequence"]
                    ):
                        raise ConflictError(
                            "Checkpoint frontier is ahead of durable Events."
                        )
                    if entry.anchor and previous and previous["durable_sequence"] > 0:
                        raise ConflictError(
                            "Checkpoint import requires fresh Session histories."
                        )
                await batch.checkpoint(logical, entry.identity, anchor=entry.anchor)
            await conn.execute(insert(meta.tables[name]), data)
            refs = [
                {
                    "owner_kind": entry.kind,
                    "owner_id": entry.identity,
                    "slot": i,
                    "path": path,
                    "artifact_hash": h,
                }
                for i, (path, h) in enumerate(entry.encoded.refs)
            ]
            if refs:
                await conn.execute(insert(meta.tables["artifact_refs"]), refs)
            if entry.kind == "checkpoint":
                await conn.execute(
                    insert(meta.tables["checkpoint_sessions"]),
                    [
                        {
                            "checkpoint_id": entry.identity,
                            "session_id": s["session_id"],
                            "sequence": s["state"]["sequence"],
                            "invocation_id": (s["state"]["invocation"] or {}).get("id"),
                            "workflow_revision_id": (
                                s["state"]["invocation"] or {}
                            ).get("workflow_revision_id"),
                        }
                        for s in logical["sessions"]
                    ],
                )
        await self.projections(conn, batch)
        if any(entry.kind == 'checkpoint' and entry.anchor for entry in entries):
            await conn.execute(update(meta.tables['persistence_meta']).values(store_id=uuid4().hex))
        await conn.execute(
            update(meta.tables["persistence_meta"]).values(last_store_order=order)
        )
        return order
