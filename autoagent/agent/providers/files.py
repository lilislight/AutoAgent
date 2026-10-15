"""OpenAI Files SDK wrapper with explicit ownership, caching and cleanup."""

from __future__ import annotations

import asyncio
import hashlib

from ..models import ProviderSource
from ..media.store import MediaError, _filename
from .base import ProviderError
from ._openai_client import OpenAIClientOwner


class OpenAIFileClient(OpenAIClientOwner):
    """Uploaded files survive close; cleanup explicitly deletes this client's uploads.

    Injected SDK clients remain borrowed. The in-memory cache is scoped to this
    connection and purpose; provider-side deletion/expiry requires invalidate().
    Upload/delete calls have no wrapper retries and require the owning loop.
    """

    protocol = "openai.files"
    error_type = ProviderError

    def __init__(self, config=None, *, client=None, max_file_bytes: int = 16 * 1024 * 1024):
        super().__init__(config, client=client)
        if type(max_file_bytes) is not int or max_file_bytes <= 0:
            raise ValueError("File upload byte limit must be a positive integer.")
        self.max_file_bytes = max_file_bytes
        self._uploads = {}
        self._created = set()
        self._lock = asyncio.Lock()
        self._cleaning = False

    async def aupload(self, data: bytes, *, filename: str, mime_type: str,
                      purpose: str = "user_data", cache: bool = True) -> ProviderSource:
        from ..models import DataSource

        self._bind_loop()
        if self._cleaning:
            raise RuntimeError("Settle file cleanup before uploading more files.")
        if not isinstance(data, bytes) or not data or len(data) > self.max_file_bytes:
            raise MediaError("File upload requires nonempty bytes within the configured limit.")
        _filename(filename)
        DataSource(data="YQ==", mime_type=mime_type)
        if purpose not in {"assistants", "batch", "fine-tune", "vision", "user_data", "evals"}:
            raise ValueError("Unsupported OpenAI file purpose.")
        key = (hashlib.sha256(data).hexdigest(), filename, mime_type, purpose)
        self._active += 1
        try:
            async with self._lock:
                if cache and key in self._uploads:
                    return self._uploads[key]
                raw = await self._get_client().files.create(file=(filename, data, mime_type), purpose=purpose)
                source = ProviderSource(provider_id=self.provider_id, file_id=raw.id, mime_type=mime_type)
                self._created.add(source.file_id)
                if cache:
                    self._uploads[key] = source
                return source
        except (ProviderError, ImportError):
            raise
        except Exception as error:
            raise self._error(error) from error
        finally:
            self._active -= 1

    def _check_source(self, source):
        self._bind_loop()
        source = ProviderSource.model_validate(source)
        if source.provider_id != self.provider_id:
            raise MediaError("File reference belongs to another Provider connection.")
        return source

    def invalidate(self, source: ProviderSource) -> None:
        source = self._check_source(source)
        self._uploads = {key: value for key, value in self._uploads.items() if value.file_id != source.file_id}

    async def aretrieve(self, source: ProviderSource) -> dict:
        source = self._check_source(source)
        self._active += 1
        try:
            value = await self._get_client().files.retrieve(source.file_id)
            if value.id != source.file_id:
                raise ValueError("File retrieval returned a different file identity.")
            return value.model_dump(mode="json")
        except (ProviderError, ImportError):
            raise
        except Exception as error:
            raise self._error(error) from error
        finally:
            self._active -= 1

    async def adelete(self, source: ProviderSource) -> None:
        source = self._check_source(source)
        self._active += 1
        try:
            async with self._lock:
                result = await self._get_client().files.delete(source.file_id)
                if not result.deleted or result.id != source.file_id:
                    raise ValueError("File deletion was not acknowledged for the selected file.")
                self.invalidate(source)
                self._created.discard(source.file_id)
        except (ProviderError, ImportError):
            raise
        except Exception as error:
            raise self._error(error) from error
        finally:
            self._active -= 1

    async def acleanup(self) -> None:
        """Delete every tracked upload; retain failures so cleanup can be retried."""
        self._bind_loop()
        if self._active:
            raise RuntimeError("Settle active file requests before cleanup.")
        failures = []
        self._cleaning = True
        self._active += 1
        try:
            for file_id in tuple(self._created):
                try:
                    await self.adelete(ProviderSource(provider_id=self.provider_id, file_id=file_id))
                except Exception as error:
                    failures.append(error)
        finally:
            self._cleaning = False
            self._active -= 1
        if failures:
            raise ExceptionGroup("Some uploaded files could not be deleted.", failures)


__all__ = ["OpenAIFileClient"]
