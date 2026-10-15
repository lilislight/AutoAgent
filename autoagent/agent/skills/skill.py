"""Source-independent Skill reads and detached, bounded content snapshots."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import re

from pydantic import ValidationError

from ..models import AudioContent, DataSource, FileContent, ImageContent, TextContent, VideoContent
from .errors import SkillChangedError, SkillLimitError, SkillResourceError
from .files import resource_path
from .models import (
    SkillDocument, SkillLimits, SkillMetadata, SkillResourceContent,
    SkillResourcePage, SkillTextContent, SkillResource,
)
from .parser import parse_skill
from .sources import LocalSkillSource, SkillSource, is_text, normalize_prefix, matches_prefix


class Skill:
    """Metadata is discovered once; each operation checks it against the source.

    Loading instructions never scans resources. Resource pages are independent
    of document loading. Files remain live until read; returned content is a
    detached snapshot. Expected source/content failures raise SkillError.
    """

    def __init__(self, source: SkillSource | str | Path, *, limits: SkillLimits | None = None):
        if isinstance(source, (str, Path)):
            source = LocalSkillSource(source)
        if not isinstance(source, SkillSource):
            raise TypeError("Skill requires a SkillSource or local directory.")
        self.source = source
        self.limits = SkillLimits.model_validate(limits or SkillLimits())
        data = self._read("SKILL.md", self.limits.max_document_bytes)
        self._metadata, _ = parse_skill(data, directory_name=source.name)

    @classmethod
    def from_directory(cls, directory: str | Path, *, limits: SkillLimits | None = None) -> Skill:
        return cls(LocalSkillSource(directory), limits=limits)

    @classmethod
    def from_source(cls, source: SkillSource, *, limits: SkillLimits | None = None) -> Skill:
        return cls(source, limits=limits)

    @property
    def metadata(self) -> SkillMetadata:
        return SkillMetadata.model_validate(self._metadata.model_dump(mode="python"))

    @property
    def snapshot_revision(self) -> str | None:
        return getattr(self.source, "snapshot_revision", None)

    def snapshot(self, *, paths=None, expected_revision: str | None = None):
        """Capture a verified version; selected paths always include SKILL.md."""
        from .snapshot import capture_snapshot

        return capture_snapshot(self, paths=paths, expected_revision=expected_revision)

    def freeze(self, *, paths=None, expected_revision: str | None = None) -> Skill:
        """Use one immutable version for document, resource, text and script reads."""
        return self.snapshot(paths=paths, expected_revision=expected_revision).to_skill(limits=self.limits)

    def _read(self, path: str, maximum: int) -> bytes:
        resource_path(path)
        try:
            with self.source.open_binary(path) as stream:
                data = stream.read(maximum + 1)
        except OSError as error:
            raise SkillResourceError("Skill resource is inaccessible.") from error
        if not isinstance(data, bytes):
            raise TypeError("SkillSource must return binary streams.")
        if len(data) > maximum:
            raise SkillLimitError("Skill file exceeds its configured byte limit.")
        return data

    def _document(self):
        data = self._read("SKILL.md", self.limits.max_document_bytes)
        metadata, instructions = parse_skill(data, directory_name=self.source.name)
        if metadata != self._metadata:
            raise SkillChangedError("Skill metadata changed; rediscover the Skill.")
        return data, metadata, instructions

    def load(self) -> SkillDocument:
        data, metadata, instructions = self._document()
        return SkillDocument(metadata=metadata, instructions=instructions, revision=hashlib.sha256(data).hexdigest(),
                             snapshot_revision=self.snapshot_revision)

    def list_resources(self, *, prefix: str | None = None, cursor: str | None = None, limit: int | None = None) -> SkillResourcePage:
        self._document()
        prefix = normalize_prefix(prefix)
        limit = self.limits.max_resources if limit is None else limit
        if type(limit) is not int or not 1 <= limit <= self.limits.max_resources:
            raise SkillLimitError("Resource page size must be within the configured maximum.")
        resources = []
        seen = set()
        count = 0
        try:
            for value in self.source.iter_resources(prefix=prefix, limits=self.limits):
                count += 1
                if count > self.limits.max_scan_entries:
                    raise SkillLimitError("Skill resource scan exceeds its entry limit.")
                # Revalidate source models at this public boundary.
                value = SkillResource.model_validate(value)
                resource_path(value.path)
                if value.path in seen:
                    raise SkillResourceError("Source returned duplicate resource paths.")
                seen.add(value.path)
                if value.path != "SKILL.md" and matches_prefix(value.path, prefix):
                    resources.append(value)
        except (ValidationError, OSError) as error:
            raise SkillResourceError("Source returned invalid or inaccessible resources.") from error
        resources.sort(key=lambda value: value.path)
        record = {"source": self.source.location, "prefix": prefix,
                  "resources": [value.model_dump(mode="json") for value in resources]}
        revision = hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        offset = 0
        if cursor is not None:
            match = re.fullmatch(r"([a-f0-9]{64}):([0-9]{1,12})", cursor)
            if match is None:
                raise SkillResourceError("Invalid resource page cursor.")
            if match[1] != revision:
                raise SkillChangedError("Resource listing changed; restart pagination.")
            offset = int(match[2])
            if not 0 <= offset <= len(resources):
                raise SkillResourceError("Resource page cursor is out of range.")
        end = min(offset + limit, len(resources))
        return SkillResourcePage(resources=tuple(resources[offset:end]), revision=revision,
                                 snapshot_revision=self.snapshot_revision,
                                 next_cursor=f"{revision}:{end}" if end < len(resources) else None)

    def read_bytes(self, path: str) -> bytes:
        """Bounded raw bytes for explicit application runners and source adapters."""
        document, _, _ = self._document()
        if path == "SKILL.md":
            if len(document) > self.limits.max_resource_bytes:
                raise SkillLimitError("Skill file exceeds the configured resource byte limit.")
            return document
        return self._read(path, self.limits.max_resource_bytes)

    def read_resource(self, path: str) -> SkillResourceContent:
        data = self.read_bytes(path)
        try:
            resource = self.source.resource_info(path)
            resource = SkillResource.model_validate(resource)
            if resource.path != path:
                raise SkillResourceError("Source resource identity does not match its path.")
            mime = resource.mime_type
            if is_text(mime):
                content = TextContent(text=data.decode("utf-8-sig"))
            else:
                if not data:
                    raise SkillResourceError("Binary Skill resources cannot be empty.")
                source = DataSource(data=base64.b64encode(data).decode("ascii"), mime_type=mime)
                if mime.startswith("image/"):
                    content = ImageContent(source=source)
                elif mime.startswith("audio/"):
                    content = AudioContent(source=source)
                elif mime.startswith("video/"):
                    content = VideoContent(source=source)
                else:
                    content = FileContent(source=source, filename=Path(path).name)
            resource = SkillResource(path=path, size_bytes=len(data), mime_type=mime)
            return SkillResourceContent(skill_name=self._metadata.name, resource=resource,
                                        revision=hashlib.sha256(data).hexdigest(), content=(content,), snapshot_revision=self.snapshot_revision)
        except (ValidationError, UnicodeError, OSError) as error:
            raise SkillResourceError("Skill resource content or metadata is invalid.") from error

    def read_text(self, path: str, *, start_line: int = 1, max_lines: int | None = None, max_bytes: int | None = None) -> SkillTextContent:
        """Read complete UTF-8 lines, bounded by scan and returned-content budgets.

        revision hashes this returned range, not the full resource. next_line
        continues against the live source. A single line larger than max_bytes
        fails explicitly instead of returning an empty, nonadvancing page.
        """
        self._document()
        resource_path(path)
        max_lines = self.limits.max_text_lines if max_lines is None else max_lines
        max_bytes = self.limits.max_text_bytes if max_bytes is None else max_bytes
        if type(start_line) is not int or start_line < 1:
            raise SkillResourceError("Text start_line must be positive.")
        if type(max_lines) is not int or not 1 <= max_lines <= self.limits.max_text_lines:
            raise SkillLimitError("Text line count exceeds its configured limit.")
        if type(max_bytes) is not int or not 1 <= max_bytes <= self.limits.max_text_bytes:
            raise SkillLimitError("Text byte count exceeds its configured limit.")
        try:
            resource = SkillResource.model_validate(self.source.resource_info(path))
            if resource.path != path:
                raise SkillResourceError("Source resource identity does not match its path.")
            if not is_text(resource.mime_type):
                raise SkillResourceError("Text ranges require a text resource.")
            scanned = 0
            lines = []
            more = False
            with self.source.open_binary(path) as stream:
                for _ in range(start_line - 1):
                    skipped = stream.readline(self.limits.max_resource_bytes - scanned + 1)
                    scanned += len(skipped)
                    if scanned > self.limits.max_resource_bytes:
                        raise SkillLimitError("Text scan exceeds its configured byte limit.")
                    if not skipped:
                        break
                else:
                    returned = 0
                    for _ in range(max_lines):
                        line = stream.readline(min(max_bytes - returned, self.limits.max_resource_bytes - scanned) + 1)
                        scanned += len(line)
                        if scanned > self.limits.max_resource_bytes:
                            raise SkillLimitError("Text scan exceeds its configured byte limit.")
                        if not line:
                            break
                        if returned + len(line) > max_bytes:
                            if not lines:
                                raise SkillLimitError("A text line exceeds the returned byte limit.")
                            more = True
                            break
                        lines.append(line)
                        returned += len(line)
                    else:
                        more = bool(stream.read(1))
            data = b"".join(lines)
            text = data.decode("utf-8-sig" if start_line == 1 else "utf-8")
            end = start_line + len(lines) - 1
            return SkillTextContent(skill_name=self._metadata.name, resource=resource,
                                    start_line=start_line, end_line=end,
                                    next_line=end + 1 if more else None,
                                    text=text, revision=hashlib.sha256(data).hexdigest(), snapshot_revision=self.snapshot_revision)
        except (ValidationError, UnicodeError, OSError) as error:
            raise SkillResourceError("Skill text resource is invalid or inaccessible.") from error


__all__ = ["Skill"]
