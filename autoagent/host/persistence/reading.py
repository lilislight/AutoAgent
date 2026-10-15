"""Read a stored JSON subtree without hydrating unrelated Artifacts."""

import hashlib
import json

from sqlalchemy import select

from .codec import DOMAIN, assign, canonical, pointer
from .models import FormatError
from .schema import meta
from .storage import SOURCES


class SourceReader:
    def __init__(self, storage, conn, record, refs):
        self.storage, self.conn = storage, conn
        self.record, self.refs = record, refs
        self.cache = {}

    @classmethod
    async def open(cls, storage, conn, kind, identity):
        name, key, body = SOURCES[kind]
        row = await storage.row(conn, name, **{key: identity})
        if row is None:
            return None
        if body:
            record = json.loads(row[body])
        elif kind == "runtime_event":
            record = {"payload": json.loads(row["payload_text"]),
                      "delta": json.loads(row["delta_text"]) if row["delta_text"] else None}
        else:
            record = {"payload": json.loads(row["payload_text"])}
        table = meta.tables["artifact_refs"]
        refs = {r.path: r.artifact_hash for r in (await conn.execute(
            select(table.c.path, table.c.artifact_hash).where(
                table.c.owner_kind == kind, table.c.owner_id == identity))) }
        return cls(storage, conn, record, refs)

    async def read(self, path=(), *, deep=True):
        return await self._read(self.record, self.refs, tuple(path), deep, frozenset())

    async def present(self, path):
        return await self._read(self.record, self.refs, tuple(path), "present", frozenset())

    async def _read(self, value, refs, path, deep, loading):
        if "" in refs:
            if not path and deep == "present":
                return True
            digest = refs[""]
            if digest in loading:
                raise FormatError("Cyclic Artifact dependencies.")
            if digest not in self.cache:
                row = await self.storage.row(self.conn, "artifacts", hash=digest)
                if row is None:
                    raise FormatError("Missing Artifact.")
                deps = meta.tables["artifact_dependencies"]
                children = {r.path: r.child_hash for r in (await self.conn.execute(
                    select(deps.c.path, deps.c.child_hash).where(deps.c.parent_hash == digest)))}
                self.cache[digest] = (json.loads(row["body"]), children, row["logical_bytes"])
            value, refs, logical_bytes = self.cache[digest]
            value = await self._read(value, refs, path, deep, loading | {digest})
            if not path and deep is True:
                data = canonical(value)
                if len(data) != logical_bytes or hashlib.sha256(DOMAIN + data).hexdigest() != digest:
                    raise FormatError("Artifact content digest mismatch.")
            return value
        if path:
            token = path[0]
            prefix = pointer((token,))
            children = {key[len(prefix):]: digest for key, digest in refs.items()
                        if key == prefix or key.startswith(prefix + "/")}
            return await self._read(value[token], children, path[1:], deep, loading)
        if deep == "present":
            return value is not None
        if not deep or not refs:
            return value
        # Copy before filling references: cached packed containers stay immutable.
        value = json.loads(json.dumps(value))
        for key, digest in refs.items():
            child = await self._read(None, {"": digest}, (), True, loading)
            value = assign(value, key, child)
        return value
