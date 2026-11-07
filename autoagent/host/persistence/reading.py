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
    async def open(cls, storage, conn, kind, identity, *, row=None, refs=None):
        """Read one source, reusing rows and references from this SQL snapshot."""
        name, key, body = SOURCES[kind]
        if row is None:
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
        if refs is None:
            table = meta.tables["artifact_refs"]
            refs = {r.path: r.artifact_hash for r in (await conn.execute(
                select(table.c.path, table.c.artifact_hash).where(
                    table.c.owner_kind == kind, table.c.owner_id == identity))) }
        return cls(storage, conn, record, refs)

    async def read(self, path=(), *, deep=True):
        return await self._read(self.record, self.refs, tuple(path), deep, frozenset())

    async def read_pointer(self, pointer=None, *, base=()):
        """Resolve an RFC 6901 path, hydrating only the selected subtree."""
        path = tuple(base)
        for token in pointer_tokens(pointer):
            parent = await self.read(path, deep=False)
            if isinstance(parent, list):
                if not token.isascii() or not token.isdigit() or len(token) > 1 and token.startswith("0"):
                    raise KeyError(token)
                token = int(token)
            elif not isinstance(parent, dict):
                raise KeyError(token)
            path += (token,)
        return await self.read(path)

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


def pointer_tokens(pointer):
    if pointer in (None, ""):
        return ()
    if not isinstance(pointer, str) or not pointer.startswith("/") or len(pointer) > 8192:
        raise ValueError("JSON Pointer must be empty or start with '/', with at most 8192 characters.")
    tokens = pointer[1:].split("/")
    import re
    if any(re.search(r"~(?![01])", token) for token in tokens):
        raise ValueError("Invalid JSON Pointer escape; use ~0 for '~' and ~1 for '/'.")
    return tuple(token.replace("~1", "/").replace("~0", "~") for token in tokens)


class MetadataReader(SourceReader):
    """Read structural fields without permitting business-value traversal."""
    async def read(self, path=(), *, deep=False):
        if not path or deep or any(token in {'input','output','context','mapped_input',
            'aggregate_output','response','metrics','message'} for token in path):
            raise ValueError('MetadataReader requires a shallow metadata field path.')
        return await self._read(self.record,self.refs,tuple(path),False,frozenset())
