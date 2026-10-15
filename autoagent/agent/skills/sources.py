"""Application-owned Skill sources; no Provider, network or SDK dependency."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
import io
import base64
import mimetypes
import os
from pathlib import Path
from typing import BinaryIO, Protocol, runtime_checkable

from pydantic import ValidationError
from ..models import DataSource

from .errors import SkillChangedError, SkillLimitError, SkillResourceError
from .files import identity, open_resource, resolve_resource, resource_path
from .models import SkillLimits, SkillResource


def resource_mime(path: str) -> str:
    if Path(path).suffix.lower() in {".md", ".txt", ".csv", ".yaml", ".yml", ".toml", ".py", ".sh", ".js", ".ts", ".sql"}:
        return "text/plain"
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


def is_text(mime: str) -> bool:
    return mime.startswith("text/") or mime in {"application/json", "application/xml", "image/svg+xml"}


def normalize_prefix(prefix: str | None) -> str | None:
    return None if prefix is None else resource_path(prefix.rstrip("/"))


def matches_prefix(path: str, prefix: str | None) -> bool:
    return prefix is None or path == prefix or path.startswith(prefix + "/")


_IGNORED_DIRECTORIES = frozenset({"__pycache__", ".git", ".pytest_cache", ".venv", "node_modules"})


def ignored_resource(path: str) -> bool:
    return any(part in _IGNORED_DIRECTORIES for part in path.split("/")[:-1])


@runtime_checkable
class SkillSource(Protocol):
    """Synchronous source, suitable for Tool's worker pool.

    Implementations own access to their storage and raise SkillError for expected
    failures. Streams are closed by their contexts. Remote SDK data can be
    prefetched by its owning async client into MemorySkillSource, or served by an
    application adapter. location is a diagnostic identifier, without credentials.
    """

    name: str
    location: str

    def open_binary(self, path: str) -> AbstractContextManager[BinaryIO]: ...
    def resource_info(self, path: str) -> SkillResource: ...
    def iter_resources(self, *, prefix: str | None, limits: SkillLimits) -> Iterator[SkillResource]: ...


class LocalSkillSource:
    def __init__(self, directory: str | Path):
        try:
            self.directory = Path(directory).resolve(strict=True)
            self._identity = identity(self.directory)
        except (OSError, RuntimeError) as error:
            raise SkillResourceError("Skill directory is missing or inaccessible.") from error
        self.name = self.directory.name
        self.location = str(self.directory / "SKILL.md")

    def _check_root(self):
        try:
            if identity(self.directory) != self._identity:
                raise SkillChangedError("Skill root changed; rediscover the Skill.")
        except OSError as error:
            raise SkillResourceError("Skill directory is inaccessible.") from error

    def open_binary(self, path: str):
        return open_resource(self.directory, path, self._identity)

    def resource_info(self, path: str) -> SkillResource:
        with self.open_binary(path) as stream:
            size = os.fstat(stream.fileno()).st_size
        return SkillResource(path=path, size_bytes=size, mime_type=resource_mime(path))

    def iter_resources(self, *, prefix: str | None, limits: SkillLimits) -> Iterator[SkillResource]:
        self._check_root()
        prefix = normalize_prefix(prefix)
        start = self.directory
        if prefix is not None:
            raw = self.directory / prefix
            if not os.path.lexists(raw):
                return
            start = resolve_resource(self.directory, prefix)
            if start.is_file():
                if prefix != "SKILL.md":
                    yield self.resource_info(prefix)
                return
            if raw.is_symlink():
                raise SkillResourceError("Resource directory links are not traversed.")
        count = 0

        def visit(directory: Path, relative: str, depth: int):
            nonlocal count
            if depth > limits.max_resource_depth:
                raise SkillLimitError("Skill resources exceed directory depth limits.")
            with os.scandir(directory) as scan:
                entries = []
                for entry in scan:
                    count += 1
                    if count > limits.max_scan_entries:
                        raise SkillLimitError("Skill resource scan exceeds its entry limit.")
                    entries.append(entry)
            for entry in sorted(entries, key=lambda value: value.name):
                path = f"{relative}/{entry.name}" if relative else entry.name
                resource_path(path)
                if entry.name in _IGNORED_DIRECTORIES or ignored_resource(path):
                    continue
                if entry.is_dir(follow_symlinks=False):
                    yield from visit(Path(entry.path), path, depth + 1)
                elif path != "SKILL.md":
                    # Skip internal directory links; external links still fail
                    # containment checks. Never recursively follow a directory link.
                    if entry.is_symlink() and resolve_resource(self.directory, path).is_dir():
                        continue
                    yield self.resource_info(path)

        try:
            yield from visit(start, prefix or "", len(Path(prefix).parts) if prefix else 0)
        except OSError as error:
            raise SkillResourceError("Skill resource listing is inaccessible.") from error


class MemorySkillSource:
    """Detached bytes from uploads, remote repositories or an async MCP client."""

    def __init__(self, name: str, files: Mapping[str, bytes | str], *, mime_types: Mapping[str, str] | None = None):
        self.name = name
        self.location = f"memory:{name}"
        self._files = {}
        for path, value in files.items():
            resource_path(path)
            if not isinstance(value, (bytes, str)):
                raise TypeError("Memory Skill files must be bytes or UTF-8 strings.")
            self._files[path] = value.encode("utf-8") if isinstance(value, str) else value
        self._mime_types = dict(mime_types or {})
        for path, mime in self._mime_types.items():
            if path not in self._files:
                raise SkillResourceError("MIME metadata must identify an existing Skill file.")
            try:
                DataSource(data="eA==", mime_type=mime)
            except ValidationError as error:
                raise SkillResourceError("Invalid Skill MIME metadata.") from error

    @classmethod
    async def from_mcp(cls, client, name: str, resources: Mapping[str, str], *,
                       limits: SkillLimits | None = None, max_total_bytes: int = 16 * 1024 * 1024):
        """Prefetch mapped MCP resource URIs on the client's owning event loop.

        Each URI must produce one text/blob content item. The resulting source
        is detached and usable after the client closes. No connection is created
        or closed here. Failed/cancelled fetches publish no partial source.
        """
        limits = SkillLimits.model_validate(limits or SkillLimits())
        mapping = dict(resources)
        if type(max_total_bytes) is not int or max_total_bytes <= 0:
            raise ValueError("MCP Skill total byte limit must be positive.")
        if "SKILL.md" not in mapping:
            raise SkillResourceError("MCP Skill mapping must include SKILL.md.")
        if len(mapping) > limits.max_scan_entries:
            raise SkillLimitError("MCP Skill mapping exceeds its file limit.")
        for path in mapping:
            resource_path(path)
        total = 0
        files = {}
        mime_types = {}
        for path, uri in mapping.items():
            result = await client.read_resource(uri)
            if len(result.contents) != 1:
                raise SkillResourceError("Each MCP Skill resource must return one content item.")
            item = result.contents[0]
            maximum = limits.max_document_bytes if path == "SKILL.md" else limits.max_resource_bytes
            text, blob = getattr(item, "text", None), getattr(item, "blob", None)
            if isinstance(text, str):
                if len(text) > maximum:
                    raise SkillLimitError("MCP Skill resource exceeds its byte limit.")
                try:
                    data = text.encode("utf-8")
                except UnicodeError as error:
                    raise SkillResourceError("MCP Skill text must be valid UTF-8.") from error
            elif isinstance(blob, str):
                if len(blob) > 4 * ((maximum + 2) // 3):
                    raise SkillLimitError("MCP Skill resource exceeds its byte limit.")
                try:
                    data = base64.b64decode(blob, validate=True)
                except ValueError as error:
                    raise SkillResourceError("MCP Skill blob must be valid base64.") from error
            else:
                raise SkillResourceError("Unsupported MCP Skill resource contents.")
            total += len(data)
            if len(data) > maximum or total > max_total_bytes:
                raise SkillLimitError("MCP Skill files exceed their configured byte limits.")
            files[path] = data
            mime_types[path] = getattr(item, "mime_type", None) or ("text/plain" if isinstance(text, str) else resource_mime(path))
        return cls(name, files, mime_types=mime_types)

    @contextmanager
    def open_binary(self, path: str):
        resource_path(path)
        if path not in self._files:
            raise SkillResourceError("Skill resource is missing.")
        with io.BytesIO(self._files[path]) as stream:
            yield stream

    def resource_info(self, path: str) -> SkillResource:
        resource_path(path)
        if path not in self._files:
            raise SkillResourceError("Skill resource is missing.")
        return SkillResource(path=path, size_bytes=len(self._files[path]), mime_type=self._mime_types.get(path, resource_mime(path)))

    def iter_resources(self, *, prefix: str | None, limits: SkillLimits) -> Iterator[SkillResource]:
        prefix = normalize_prefix(prefix)
        count = 0
        for path in sorted(self._files):
            if path == "SKILL.md" or ignored_resource(path) or not matches_prefix(path, prefix):
                continue
            count += 1
            if count > limits.max_scan_entries or len(Path(path).parts) - 1 > limits.max_resource_depth:
                raise SkillLimitError("Memory Skill resource scan exceeds its limits.")
            yield self.resource_info(path)


class PackageSkillSource:
    """Read bundled package resources, including packages imported from zip files."""

    def __init__(self, package: str, directory: str):
        from importlib.resources import files

        resource_path(directory)
        self._root = files(package).joinpath(*directory.split("/"))
        if not self._root.is_dir():
            raise SkillResourceError("Package Skill directory is missing.")
        self.name = directory.split("/")[-1]
        self.location = f"package:{package}/{directory}/SKILL.md"
        self._local = LocalSkillSource(self._root) if isinstance(self._root, Path) else None

    def _path(self, path: str):
        resource_path(path)
        result = self._root.joinpath(*path.split("/"))
        if not result.is_file():
            raise SkillResourceError("Package Skill resource must be a regular file.")
        return result

    @contextmanager
    def open_binary(self, path: str):
        if self._local is not None:
            with self._local.open_binary(path) as stream:
                yield stream
        else:
            try:
                with self._path(path).open("rb") as stream:
                    yield stream
            except OSError as error:
                raise SkillResourceError("Package Skill resource is inaccessible.") from error

    def resource_info(self, path: str) -> SkillResource:
        if self._local is not None:
            return self._local.resource_info(path)
        result = self._path(path)
        # Traversable has no general stat contract. zipfile.Path exposes its
        # archive and entry name; other implementations can leave size unknown.
        import zipfile

        size = result.root.getinfo(result.at).file_size if isinstance(result, zipfile.Path) else None
        return SkillResource(path=path, size_bytes=size, mime_type=resource_mime(path))

    def iter_resources(self, *, prefix: str | None, limits: SkillLimits) -> Iterator[SkillResource]:
        if self._local is not None:
            yield from self._local.iter_resources(prefix=prefix, limits=limits)
            return
        prefix = normalize_prefix(prefix)
        start = self._root if prefix is None else self._root.joinpath(*prefix.split("/"))
        if start.is_file():
            if prefix != "SKILL.md":
                yield self.resource_info(prefix)
            return
        if not start.is_dir():
            return
        count = 0

        def visit(directory, relative, depth):
            nonlocal count
            if depth > limits.max_resource_depth:
                raise SkillLimitError("Package Skill resources exceed depth limits.")
            entries = []
            for entry in directory.iterdir():
                count += 1
                if count > limits.max_scan_entries:
                    raise SkillLimitError("Package Skill resource scan exceeds its entry limit.")
                entries.append(entry)
            for entry in sorted(entries, key=lambda value: value.name):
                path = f"{relative}/{entry.name}" if relative else entry.name
                resource_path(path)
                if entry.name in _IGNORED_DIRECTORIES or ignored_resource(path):
                    continue
                if entry.is_dir():
                    yield from visit(entry, path, depth + 1)
                elif path != "SKILL.md":
                    yield self.resource_info(path)

        yield from visit(start, prefix or "", len(Path(prefix).parts) if prefix else 0)


__all__ = ["SkillSource", "LocalSkillSource", "MemorySkillSource", "PackageSkillSource"]
