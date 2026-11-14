"""Portable, verified Skill versions and immutable sources for subsequent reads."""

from __future__ import annotations

import base64
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import io
import json
from types import MappingProxyType

from pydantic import ValidationError, model_validator

from ..models._base import Model, NonEmpty
from ..models import DataSource
from .errors import SkillChangedError, SkillLimitError, SkillResourceError
from .files import resource_path
from .models import Digest, SkillName, SkillResource
from .sources import matches_prefix


class SkillSnapshotFile(Model):
    resource: SkillResource
    data: str
    revision: Digest

    @model_validator(mode="after")
    def _verify(self):
        DataSource(data="YQ==", mime_type=self.resource.mime_type)
        try:
            value = base64.b64decode(self.data, validate=True)
        except ValueError as error:
            raise ValueError("Snapshot files require valid base64.") from error
        if len(value) != self.resource.size_bytes or hashlib.sha256(value).hexdigest() != self.revision:
            raise ValueError("Snapshot file size or content hash does not match its manifest.")
        return self


def _revision(name, files):
    manifest = {"name": name, "files": [{"resource": item.resource.model_dump(mode="json"), "revision": item.revision} for item in files]}
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


class SkillSnapshot(Model):
    """JSON-serializable bytes and a version covering document and every resource.

    source_location is provenance only, excluded from the content identity.
    Snapshot restore verifies bytes; it never returns to the original storage.
    """

    name: SkillName
    source_location: NonEmpty
    revision: Digest
    files: tuple[SkillSnapshotFile, ...]

    @model_validator(mode="after")
    def _manifest(self):
        paths = tuple(item.resource.path for item in self.files)
        if paths != tuple(sorted(set(paths))) or "SKILL.md" not in paths:
            raise ValueError("Snapshots require sorted unique paths including SKILL.md.")
        if _revision(self.name, self.files) != self.revision:
            raise ValueError("Snapshot revision does not match its complete manifest.")
        return self

    def to_skill(self, *, limits=None):
        from .skill import Skill
        from .models import SkillLimits

        limits = SkillLimits.model_validate(limits or SkillLimits())
        total = 0
        if len(self.files) > limits.max_scan_entries:
            raise SkillLimitError("Snapshot contains too many files.")
        for item in self.files:
            size = item.resource.size_bytes
            maximum = limits.max_document_bytes if item.resource.path == "SKILL.md" else limits.max_resource_bytes
            if size > maximum:
                raise SkillLimitError("Snapshot file exceeds its configured byte limit.")
            total += size
        if total > limits.max_snapshot_bytes:
            raise SkillLimitError("Snapshot exceeds its total byte limit.")
        return Skill(SnapshotSkillSource(self), limits=limits)


@dataclass(frozen=True, slots=True, init=False)
class SnapshotSkillSource:
    """Immutable resource metadata and decoded bytes, backed by a verified manifest."""

    snapshot: SkillSnapshot
    name: str
    location: str
    snapshot_revision: str
    _files: object

    def __init__(self, snapshot: SkillSnapshot):
        snapshot = SkillSnapshot.model_validate(snapshot)
        object.__setattr__(self, "snapshot", snapshot)
        object.__setattr__(self, "name", snapshot.name)
        object.__setattr__(self, "location", f"snapshot:{snapshot.name}@{snapshot.revision}")
        object.__setattr__(self, "snapshot_revision", snapshot.revision)
        object.__setattr__(self, "_files", MappingProxyType({item.resource.path: item for item in snapshot.files}))

    def _file(self, path):
        resource_path(path)
        try:
            return self._files[path]
        except KeyError as error:
            raise SkillResourceError("Resource is not included in this Skill snapshot.") from error

    @contextmanager
    def open_binary(self, path):
        with io.BytesIO(base64.b64decode(self._file(path).data, validate=True)) as stream:
            yield stream

    def resource_info(self, path):
        return self._file(path).resource

    def iter_resources(self, *, prefix, limits):
        for path, item in self._files.items():
            if matches_prefix(path, prefix):
                yield item.resource


def _listing(skill):
    result = {}
    for count, candidate in enumerate(skill.source.iter_resources(prefix=None, limits=skill.limits), 1):
        if count > skill.limits.max_scan_entries:
            raise SkillLimitError("Skill snapshot listing exceeds its entry limit.")
        item = SkillResource.model_validate(candidate)
        if item.path in result:
            raise SkillResourceError("Skill snapshot source returned duplicate paths.")
        result[item.path] = item
    return result


def capture_snapshot(skill, *, paths=None, expected_revision=None):
    """Copy bounded bytes, then verify a second read before publishing a version.

    This detects observed source changes; arbitrary live sources do not provide
    a filesystem transaction. Published snapshot reads and execution use only
    the captured bytes, with no subsequent dependence on that live source.
    """
    document, _, _ = skill._document()
    try:
        listing = _listing(skill) if paths is None else None
        if isinstance(paths, str):
            raise TypeError("Snapshot paths require a collection of relative paths.")
        selected = set(listing if listing is not None else (resource_path(path) for path in paths))
        selected.add("SKILL.md")
        if len(selected) > skill.limits.max_scan_entries:
            raise SkillLimitError("Skill snapshot contains too many files.")
        files, originals, total = [], {}, 0
        for path in sorted(selected):
            resource = SkillResource.model_validate(skill.source.resource_info(path))
            if resource.path != path:
                raise SkillResourceError("Snapshot resource identity does not match its path.")
            maximum = skill.limits.max_document_bytes if path == "SKILL.md" else skill.limits.max_resource_bytes
            data = skill._read(path, maximum)
            if path == "SKILL.md" and data != document:
                raise SkillChangedError("Skill document changed during snapshot capture.")
            if resource.size_bytes is not None and resource.size_bytes != len(data):
                raise SkillChangedError("Skill resource changed during snapshot capture.")
            total += len(data)
            if total > skill.limits.max_snapshot_bytes:
                raise SkillLimitError("Skill snapshot exceeds its total byte limit.")
            originals[path] = resource
            files.append(SkillSnapshotFile(resource=SkillResource(path=path, size_bytes=len(data), mime_type=resource.mime_type),
                                           data=base64.b64encode(data).decode("ascii"), revision=hashlib.sha256(data).hexdigest()))
        for item in files:
            path = item.resource.path
            current = SkillResource.model_validate(skill.source.resource_info(path))
            maximum = skill.limits.max_document_bytes if path == "SKILL.md" else skill.limits.max_resource_bytes
            data = skill._read(path, maximum)
            if current != originals[path] or hashlib.sha256(data).hexdigest() != item.revision:
                raise SkillChangedError("Skill resources changed during snapshot capture; retry explicitly.")
        if listing is not None and _listing(skill) != listing:
            raise SkillChangedError("Skill resource listing changed during snapshot capture.")
    except OSError as error:
        raise SkillResourceError("Skill snapshot source is inaccessible.") from error
    except ValidationError as error:
        raise SkillResourceError("Skill snapshot source returned invalid resource metadata.") from error
    revision = _revision(skill.metadata.name, files)
    if expected_revision is not None and revision != expected_revision:
        raise SkillChangedError("Skill snapshot does not match the expected version.")
    origin = getattr(getattr(skill.source, "snapshot", None), "source_location", skill.source.location)
    return SkillSnapshot(name=skill.metadata.name, source_location=origin, revision=revision, files=tuple(files))


__all__ = ["SkillSnapshotFile", "SkillSnapshot", "SnapshotSkillSource"]
