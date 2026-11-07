"""Async SQLAlchemy transactions and lossless source-record loading."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from contextlib import asynccontextmanager
from functools import lru_cache
from uuid import uuid4

from sqlalchemy import event, insert, inspect, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from .codec import DOMAIN, Encoded, EncodingMemo, assign, canonical, canonical_text, encode
from .models import ConflictError, FormatError
from .metadata import MetadataCache
from .identities import SourceIdentities
from .graph_metadata import WorkflowGraphCache, outgoing_edges
from .projections import light_record
from .operational import OperationalBatch
from .schema import DETAIL_TABLES, meta, PERSISTED_TABLES
from autoagent.core.runtime.values import record_values

FORMAT_VERSION = 7
PROJECTION_VERSION = 5
SOURCES = {
    "workflow_revision": ("workflow_revisions", "revision_id", "snapshot_text"),
    "operator_definition": ("operator_definitions", "definition_id", "definition_text"),
    "runtime_event": ("runtime_events", "event_id", None),
    "user_event": ("user_events", "event_id", None),
    "checkpoint": ("checkpoints", "checkpoint_id", "checkpoint_text"),
}

REFERENCE_COLUMNS = ('owner_kind', 'owner_id', 'slot', 'path', 'artifact_hash')


@lru_cache(maxsize=16)
def _reference_insert(sqlite, count):
    """Cache placeholder text only; values never enter a statement cache."""
    groups = []
    for row in range(count):
        groups.append('(' + ','.join('?' if sqlite else f'${row * 5 + column + 1}'
                                     for column in range(5)) + ')')
    return 'INSERT INTO artifact_refs (' + ','.join(REFERENCE_COLUMNS) + ') VALUES ' + ','.join(groups)


@dataclass(slots=True)
class Prepared:
    kind: str
    identity: str
    encoded: Encoded
    light: dict
    binding: str | None = None
    anchor: bool = False
    texts: dict | None = None


def source_texts(encoded, kind):
    """Finalize driver text in the codec executor and release encoded containers."""
    record = encoded.record
    if kind in ("runtime_event", "user_event"):
        texts = {"payload_text": canonical_text(record["payload"])}
        if kind == "runtime_event":
            texts["delta_text"] = canonical_text(record["delta"]) if record["delta"] is not None else None
        encoded.record = {key: value for key, value in record.items() if key not in ("payload", "delta")}
    else:
        column = SOURCES[kind][2]
        texts = {column: canonical_text(record)}
        encoded.record = {key: value for key, value in record.items() if key not in ("definition", "sessions")}
    return texts


def prepare(entries, config):
    memo = EncodingMemo(config.encoding_cache_bytes)
    result = []
    size, seen = 0, set()
    for entry in entries:
        with record_values(memo.record_value):
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
                light_record(record, availability=entry.kind == "checkpoint" or record.get('event_name') in ('invocation.forked','fork.state_imported'))
                if entry.kind in ("runtime_event", "checkpoint") else
                {key: value for key, value in record.items() if key != "payload"}
                if entry.kind == "user_event" else {},
                entry.binding,
                entry.anchor,
                source_texts(encoded, entry.kind),
            )
        )
        size += addition
        seen.update(encoded.artifacts)
    return result


class Storage:
    def __init__(self, url, *, read_only=False, graph_cache_bytes=4 * 1024 * 1024,
                 metadata_cache_bytes=4 * 1024 * 1024, config=None):
        parsed = make_url(url)
        if parsed.drivername not in ("sqlite+aiosqlite", "postgresql+asyncpg"):
            raise ValueError("Use sqlite+aiosqlite or postgresql+asyncpg.")
        self.sqlite = parsed.drivername == "sqlite+aiosqlite"
        self.memory = self.sqlite and parsed.database in (None, "", ":memory:")
        if self.memory:
            parsed = parsed.set(database='file:autoagent-' + uuid4().hex,
                                query={'mode': 'memory', 'cache': 'shared', 'uri': 'true'})
        self.url = parsed
        self.config = config
        self.last_write = time.monotonic()
        self.read_only = read_only
        self.engine = None
        self.query_engine = None
        self._lock_file = None
        self._pg_lock = None
        self.graph_cache = WorkflowGraphCache(graph_cache_bytes)
        self.identities = SourceIdentities(min(metadata_cache_bytes // 8, 512 * 1024))
        self.metadata_cache = MetadataCache(metadata_cache_bytes - len(self.identities.bits))
        self._store_order = None
        self._committing_batch = None
        self._committing_artifacts = set()
        self.committed_invocations = set()

    async def open(self):
        if self.read_only:
            return await self._open_reader()
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
                conn.exec_driver_sql('BEGIN IMMEDIATE' if conn.get_execution_options().get('persistence_write') else 'BEGIN')

        async with self.engine.begin() as conn:
            await self._validate_schema(conn, create=True)
        if self.identities.bits:
            async with self.engine.connect() as conn:
                for kind, (name, key, _) in SOURCES.items():
                    table = meta.tables[name]
                    async with conn.stream(select(table.c[key]).execution_options(yield_per=1000)) as rows:
                        async for identity in rows.scalars():
                            self.identities.add(kind, identity)
            self.identities.ready = True
        self.query_engine = self.engine if self.memory else self._create_reader_engine()
        if self.config and self.config.projection_mode == 'transactional':
            from .query_projections import catch_up
            async with self.engine.begin() as conn:
                await catch_up(self, conn, self.config)

    def _create_reader_engine(self):
        """UI reconstruction cannot exhaust the Writer/control connection pool."""
        url, arguments = self.url, {}
        if self.sqlite:
            from pathlib import Path
            from urllib.parse import quote
            path = Path(self.url.database).expanduser().resolve()
            if not path.is_file():
                raise FileNotFoundError(f"Persistence database does not exist: {path}")
            url = self.url.set(database="file:" + quote(str(path), safe="/"),
                query={**self.url.query, "mode": "ro", "uri": "true"})
            arguments = {"cached_statements": 0}
        else:
            arguments = {"server_settings": {"default_transaction_read_only": "on"}}
        engine = create_async_engine(url, connect_args=arguments, pool_size=4, max_overflow=0)
        if self.sqlite:
            @event.listens_for(engine.sync_engine, "connect")
            def configure_reader(dbapi_connection, _):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA query_only=ON")
                cursor.close()
                dbapi_connection.isolation_level = None

            @event.listens_for(engine.sync_engine, "begin")
            def begin_reader(conn):
                conn.exec_driver_sql("BEGIN")
        return engine

    async def _open_reader(self):
        """Open an existing database without writer ownership, DDL or registration."""
        if self.memory:
            raise ValueError("Read-only inspection requires an existing database.")
        self.engine = self.query_engine = self._create_reader_engine()
        async with self.engine.connect() as conn:
            await self._validate_schema(conn, create=False)

    async def _validate_schema(self, conn, *, create):
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
        if names:
            if not {t.name for t in PERSISTED_TABLES}.issubset(names):
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
            expected = {t.name: set(t.columns.keys()) for t in PERSISTED_TABLES}
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
            if not create:
                raise FormatError("Database is not a complete Persistence schema.")
            await conn.run_sync(lambda c: meta.create_all(c, tables=list(PERSISTED_TABLES)))
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
        self.identities.clear()
        self._store_order = None
        self.metadata_cache.clear()
        self.committed_invocations.clear()
        if self.graph_cache is not None:
            self.graph_cache.clear()
        if self._pg_lock is not None:
            await self._pg_lock.close()
            self._pg_lock = None
        if self.query_engine is not None and self.query_engine is not self.engine:
            await self.query_engine.dispose()
        self.query_engine = None
        if self.engine is not None:
            await self.engine.dispose()
            self.engine = None
        if self._lock_file is not None:
            self._lock_file.close()
            self._lock_file = None

    @asynccontextmanager
    async def transaction(self):
        """Reserve SQLite's write slot before reads establish a WAL snapshot.

        Readers keep deferred transactions. An idle projector yields to this
        reservation instead of making a foreground read-to-write upgrade fail.
        """
        async with self.engine.connect() as conn:
            if self.sqlite:
                conn = await conn.execution_options(persistence_write=True)
            async with conn.begin():
                yield conn

    async def row(self, conn, name, *, columns=None, **identity):
        if name in DETAIL_TABLES:
            row=await conn.detail_get(name,identity)
            return {key:row[key] for key in columns} if row and columns else row
        table = meta.tables[name]
        statement = select(*(table.c[k] for k in columns)) if columns else select(table)
        row = (
            (
                await conn.execute(
                    statement.where(*(table.c[k] == v for k, v in identity.items()))
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row else None

    async def has_row(self, conn, name, **identity):
        table = meta.tables[name]
        statement = select(next(iter(table.primary_key.columns))).where(
            *(table.c[k] == v for k, v in identity.items())
        ).limit(1)
        return await conn.scalar(statement) is not None

    async def writer_row(self, conn, name, *, columns=None, **identity):
        """Read committed metadata on the Writer path, never from query snapshots."""
        key = tuple(identity[c.name] for c in meta.tables[name].primary_key)
        row = self.metadata_cache.get((name, key))
        if row is None:
            return await self.row(conn, name, columns=columns, **identity)
        return {k: row.get(k) for k in columns} if columns else row

    async def source_header(self, conn, kind, identity):
        name, key, _ = SOURCES[kind]
        if not self.identities.may_contain(kind, identity):
            return None
        columns = (key, "operator_definition_id") if kind == "runtime_event" else (key,)
        return await self.row(conn, name, columns=columns, **{key: identity})

    async def definition_header(self, conn, kind, identity):
        """Reuse only committed immutable definition metadata, never payloads."""
        cache_key = ('definition_header', (kind, identity))
        row = self.metadata_cache.get(cache_key)
        if row is None:
            name, key, _ = SOURCES[kind]
            columns = (key, 'operator_id') if kind == 'operator_definition' else (key,)
            row = await self.row(conn, name, columns=columns, **{key: identity})
            if row is not None:
                self.metadata_cache.put(cache_key, row)
        return row

    async def workflow_graph(self, conn, revision):
        graph = self.graph_cache.get(revision)
        if graph is None:
            from .reading import MetadataReader
            reader = await MetadataReader.open(self,conn,'workflow_revision',revision)
            packed = await reader.read(('definition','edges'),deep=False)
            edges=[]
            for index in range(len(packed)):
                path=('definition','edges',index)
                edge={key:await reader.read((*path,key),deep=False) for key in ('id','source','target','on')}
                edge['condition']={} if await reader.present((*path,'condition')) else None
                edges.append(edge)
            graph = outgoing_edges({'definition':{'edges':edges}})
            self.graph_cache.put(revision, graph)
        return graph

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
            record.update(id=row["event_id"], payload=json.loads(row["payload_text"]), persist=True)
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
            for start in range(0, len(group), 500):
                await conn.execute(statement, group[start:start + 500])

    async def projections(self, conn, batch):
        """Commit only operational metadata; detailed read models are optional."""
        from sqlalchemy import delete
        for name in ('sessions', 'invocations', 'active_nodes', 'active_waits'):
            rows = batch.changed(name)
            if name == "invocations":
                rows.sort(key=lambda r:r["root_invocation_id"] != r["invocation_id"])
            await self.upsert(conn, name, rows)
        for name in ('active_nodes', 'active_waits'):
            table = meta.tables[name]
            removed = [key for key in batch.dirty[name] if batch.rows[name][key] is None]
            for start in range(0, len(removed), 500):
                await conn.execute(delete(table).where(tuple_(*table.primary_key.columns).in_(removed[start:start+500])))

    async def write_artifacts(self, conn, entries):
        """Prefetch each batch's hashes once; write bodies before dependencies."""
        artifacts = {digest: value for entry in entries
                     for digest, value in entry.encoded.artifacts.items()}
        existing={digest for digest in artifacts if self.metadata_cache.get(('artifact',(digest,))) is True}
        hashes=list(artifacts.keys()-existing)
        table = meta.tables["artifacts"]
        for start in range(0, len(hashes), 500):
            existing.update((await conn.execute(select(table.c.hash).where(
                table.c.hash.in_(hashes[start:start + 500])))).scalars())
        missing = [value for digest, value in artifacts.items() if digest not in existing]
        await self.insert_rows(conn, "artifacts", [
            {key: value[key] for key in ("hash", "value_type", "logical_bytes", "body")}
            for value in missing])
        await self.insert_rows(conn, "artifact_dependencies", [
            {"parent_hash": value["hash"], "slot": index, "path": path, "child_hash": digest}
            for value in missing for index, (path, digest) in enumerate(value["deps"])])
        self._committing_artifacts.update(hashes)

    async def insert_rows(self, conn, name, rows):
        """Bound parameter groups while preserving input and dependency order."""
        if name == 'artifact_refs':
            # References contain only text/integer columns. A bound multi-row
            # INSERT avoids one driver execution per reference while keeping
            # the conservative 999-parameter SQLite limit and current SQL tx.
            for start in range(0, len(rows), 128):
                chunk = rows[start:start + 128]
                parameters = tuple(row[key] for row in chunk for key in REFERENCE_COLUMNS)
                await conn.exec_driver_sql(_reference_insert(self.sqlite, len(chunk)), parameters)
            return
        for start in range(0, len(rows), 500):
            await conn.execute(insert(meta.tables[name]), rows[start:start + 500])

    async def write(self, entries):
        try:
            async with self.transaction() as conn:
                order = await self.write_transaction(conn, entries)
            # The context manager has completed COMMIT before publication.
            self._store_order = order
            for digest in self._committing_artifacts:
                self.metadata_cache.put(('artifact',(digest,)),True)
            if self._committing_batch is not None:
                self._committing_batch.publish()
                self.committed_invocations = {
                    identity for row in self._committing_batch.rows["invocations"].values() if row
                    for identity in (row["invocation_id"], row.get("root_invocation_id")) if identity
                }
            for entry in entries:
                if entry.kind == 'user_event':
                    self.metadata_cache.put(('user_event_tail', (entry.light['invocation_id'],)), entry.light['sequence'])
            return order
        except BaseException:
            # Includes uncertain COMMIT outcomes and metadata read before rollback.
            self.graph_cache.clear()
            self.metadata_cache.clear()
            self.committed_invocations.clear()
            self._store_order = None
            raise
        finally:
            self._committing_batch = None
            self._committing_artifacts.clear()

    async def write_transaction(self, conn, entries):
        batch = OperationalBatch(conn, metadata_cache=self.metadata_cache)
        self._committing_batch = batch
        order = self._store_order
        if order is None:
            order = await conn.scalar(select(meta.tables['persistence_meta'].c.last_store_order))
        initial_order = order
        identities = {}
        for entry in entries:
            identities.setdefault(entry.kind, set()).add(entry.identity)
        existing_sources = {}
        for kind, values in identities.items():
            name, key, _ = SOURCES[kind]
            table = meta.tables[name]
            possible = [identity for identity in values if self.identities.may_contain(kind, identity)]
            # Publish conservative membership before any SQL insert. Rollback
            # leaves extra positives; a committed source can never be missed.
            for identity in values:
                self.identities.add(kind, identity)
            values = possible
            found = existing_sources[kind] = set()
            for start in range(0, len(values), 500):
                found.update((await conn.execute(select(table.c[key]).where(
                    table.c[key].in_(values[start:start + 500])))).scalars())
        await batch.prefetch([entry.light for entry in entries if entry.kind == "runtime_event"])
        await self.write_artifacts(conn, entries)
        sources, references, members = {}, [], []
        imported = False

        async def flush_sources():
            for name, rows in sources.items():
                await self.insert_rows(conn, name, rows)
            await self.insert_rows(conn, "artifact_refs", references)
            await self.insert_rows(conn, "checkpoint_sessions", members)
            sources.clear()
            references.clear()
            members.clear()

        for entry in entries:
            name, key, _body_col = SOURCES[entry.kind]
            row = entry.identity in existing_sources[entry.kind]
            logical = entry.light
            if row:
                # Admission handles logical idempotency; no second projection.
                continue
            existing_sources[entry.kind].add(entry.identity)
            if logical.get("event_name") in ("invocation.forked", "fork.state_imported"):
                # Fork projection reads historical source positions/graph facts.
                # Materialize its dependencies before that SQL snapshot.
                await flush_sources()
                await self.projections(conn, batch)
                for values in batch.dirty.values():
                    values.clear()
            record = entry.encoded.record
            if entry.kind == "workflow_revision":
                data = {
                    "revision_id": entry.identity,
                    "workflow_id": record["workflow_id"],
                    "workflow_version": record["workflow_version"],
                    "definition_hash": record["definition_hash"],
                    "registered_at_us": time.time_ns() // 1000,
                    "snapshot_text": entry.texts["snapshot_text"],
                }
            elif entry.kind == "operator_definition":
                data = {
                    "definition_id": entry.identity,
                    "operator_id": record["operator_id"],
                    "version": record["version"],
                    "definition_text": entry.texts["definition_text"],
                }
            elif entry.kind == "runtime_event":
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
                    payload_text=entry.texts["payload_text"],
                    delta_text=entry.texts["delta_text"],
                )
                await batch.runtime(logical, order)
            elif entry.kind == "user_event":
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
                    payload_text=entry.texts["payload_text"],
                )
            else:
                data = {
                    "checkpoint_id": entry.identity,
                    "root_session_id": record["root_session_id"],
                    "captured_at_us": max(
                        s["captured_at_us"] for s in logical["sessions"]
                    ),
                    "digest": record["digest"],
                    "checkpoint_text": entry.texts["checkpoint_text"],
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
                imported |= entry.anchor
            sources.setdefault(name, []).append(data)
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
            references.extend(refs)
            if entry.kind == "checkpoint":
                members.extend(
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
                    ]
                )
            if entry.kind in ("workflow_revision", "operator_definition"):
                # Later entries may load definitions and their Artifact refs.
                await flush_sources()
        await flush_sources()
        await self.projections(conn, batch)
        if imported:
            from .query_projections import reset
            await reset(conn)
            await conn.execute(update(meta.tables['persistence_meta']).values(store_id=uuid4().hex))
        await conn.execute(
            update(meta.tables["persistence_meta"]).values(last_store_order=order)
        )
        if self.config and self.config.projection_mode == 'transactional' and (order > initial_order or imported):
            from .query_projections import catch_up
            await catch_up(self, conn, self.config)
        self.last_write = time.monotonic()
        return order
