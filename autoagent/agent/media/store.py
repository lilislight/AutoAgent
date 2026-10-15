"""Bounded application-owned media, with memory or persistent local storage."""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from functools import wraps
import hashlib
import json
import mimetypes
import os
from pathlib import Path
import re
import stat
import tempfile
from threading import RLock

from ..models import AssetSource, AudioContent, DataSource, FileContent, ImageContent, VideoContent


class MediaError(ValueError):
    """An unavailable, changed, oversized or invalid application media asset."""


@dataclass(frozen=True, slots=True)
class MediaAsset:
    source: AssetSource
    filename: str
    data: bytes = field(repr=False)

    def inline(self) -> DataSource:
        return DataSource(data=base64.b64encode(self.data).decode("ascii"), mime_type=self.source.mime_type)


def media_content(source, *, filename: str | None = None):
    """Choose the media container by MIME; document bytes are not extracted."""
    mime = (source.mime_type or "application/octet-stream").split(";", 1)[0]
    if mime.startswith("image/"):
        return ImageContent(source=source)
    if mime.startswith("audio/"):
        return AudioContent(source=source)
    if mime.startswith("video/"):
        return VideoContent(source=source)
    return FileContent(source=source, filename=filename)


def _filename(value):
    if not isinstance(value, str) or not value or value in {".", ".."} or any(c in value for c in "/\\\x00\r\n"):
        raise MediaError("Media filenames must be plain nonempty basenames.")
    value.encode("utf-8")
    return value


def _read(path, maximum):
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise MediaError("Media sources must be regular files.")
        value = stream.read(maximum + 1)
    if len(value) > maximum:
        raise MediaError("Media exceeds the configured byte limit.")
    return value


def _digest(data, mime, filename):
    header = json.dumps([mime, filename], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(header + b"\0" + data).hexdigest()


def _synchronized(method):
    @wraps(method)
    def invoke(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return invoke


class MemoryMediaStore:
    """Explicit writes only; content identities include MIME and filename."""

    def __init__(self, *, max_asset_bytes: int = 16 * 1024 * 1024, max_total_bytes: int = 64 * 1024 * 1024):
        if type(max_asset_bytes) is not int or max_asset_bytes <= 0 or type(max_total_bytes) is not int or max_total_bytes <= 0:
            raise ValueError("Media byte limits must be positive integers.")
        self.max_asset_bytes = max_asset_bytes
        self.max_total_bytes = max_total_bytes
        self._assets = {}
        self._total = 0
        self._lock = RLock()

    def _asset(self, data, mime_type, filename):
        if not isinstance(data, bytes) or not data:
            raise MediaError("Media requires nonempty bytes.")
        if len(data) > self.max_asset_bytes:
            raise MediaError("Media exceeds the configured byte limit.")
        filename = _filename(filename)
        # Validate MIME before creating a reference that might reach a Provider.
        DataSource(data="YQ==", mime_type=mime_type)
        source = AssetSource(uri="asset://media/" + _digest(data, mime_type, filename), mime_type=mime_type)
        return MediaAsset(source, filename, data)

    @_synchronized
    def put_bytes(self, data: bytes, *, mime_type: str, filename: str = "asset.bin") -> AssetSource:
        asset = self._asset(data, mime_type, filename)
        if asset.source.uri not in self._assets:
            if self._total + len(data) > self.max_total_bytes:
                raise MediaError("Media store exceeds its total byte limit.")
            self._assets[asset.source.uri] = asset
            self._total += len(data)
        return asset.source

    def put_file(self, path: str | Path, *, mime_type: str | None = None, filename: str | None = None) -> AssetSource:
        path = Path(path)
        try:
            data = _read(path, self.max_asset_bytes)
        except OSError as error:
            raise MediaError("Cannot read the selected media file.") from error
        return self.put_bytes(data, mime_type=mime_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                              filename=filename or path.name)

    @_synchronized
    def get(self, source: AssetSource) -> MediaAsset:
        source = AssetSource.model_validate(source)
        try:
            asset = self._assets[source.uri]
        except KeyError as error:
            raise MediaError("Unknown media asset.") from error
        if source.mime_type is not None and source.mime_type != asset.source.mime_type:
            raise MediaError("Media reference MIME does not match the stored asset.")
        return asset

    @_synchronized
    def delete(self, source: AssetSource) -> None:
        asset = self.get(source)
        del self._assets[asset.source.uri]
        self._total -= len(asset.data)


class LocalMediaStore(MemoryMediaStore):
    """Persistent content-addressed assets in an application-owned directory.

    Regular-file/no-follow reads and digest verification reject replaced assets.
    The application owns directory access and retention. max_asset_bytes bounds
    reads; disk quota and coordination with other processes remain application concerns.
    """

    def __init__(self, directory: str | Path, *, max_asset_bytes: int = 16 * 1024 * 1024):
        super().__init__(max_asset_bytes=max_asset_bytes)
        self.directory = Path(directory).expanduser().resolve()
        self.directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _key(source):
        match = re.fullmatch(r"asset://media/([0-9a-f]{64})", source.uri)
        if match is None:
            raise MediaError("Invalid local media asset reference.")
        return match[1]

    def _write(self, path, data):
        fd, temporary = tempfile.mkstemp(prefix=".media-", dir=self.directory)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            Path(temporary).unlink(missing_ok=True)

    @_synchronized
    def put_bytes(self, data: bytes, *, mime_type: str, filename: str = "asset.bin") -> AssetSource:
        asset = self._asset(data, mime_type, filename)
        key = self._key(asset.source)
        metadata = json.dumps({"mime_type": mime_type, "filename": filename}, ensure_ascii=False).encode("utf-8")
        if len(metadata) > 16 * 1024:
            raise MediaError("Media metadata exceeds its byte limit.")
        try:
            self._write(self.directory / (key + ".bin"), data)
            self._write(self.directory / (key + ".json"), metadata)
        except OSError as error:
            raise MediaError("Cannot store the selected media asset.") from error
        return asset.source

    @_synchronized
    def get(self, source: AssetSource) -> MediaAsset:
        source = AssetSource.model_validate(source)
        key = self._key(source)
        try:
            metadata = json.loads(_read(self.directory / (key + ".json"), 16 * 1024))
            data = _read(self.directory / (key + ".bin"), self.max_asset_bytes)
            asset = self._asset(data, metadata["mime_type"], metadata["filename"])
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise MediaError("Cannot read a valid local media asset.") from error
        if asset.source.uri != source.uri or (source.mime_type is not None and source.mime_type != asset.source.mime_type):
            raise MediaError("Stored media has changed or its MIME does not match.")
        return asset

    @_synchronized
    def delete(self, source: AssetSource) -> None:
        self.get(source)
        key = self._key(source)
        (self.directory / (key + ".json")).unlink()
        (self.directory / (key + ".bin")).unlink(missing_ok=True)


__all__ = ["MediaError", "MediaAsset", "MemoryMediaStore", "LocalMediaStore", "media_content"]
