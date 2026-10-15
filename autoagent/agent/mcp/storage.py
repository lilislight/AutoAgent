"""Explicit credential persistence outside Workflow and Runtime checkpoints."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.shared.auth import OAuthClientInformationFull, OAuthToken


class MCPFileTokenStorage:
    """SDK TokenStorage using one application-selected file per MCP server.

    Atomic writes use mode 0600. Absolute expiry survives process restarts, so
    loading a token never grants it its original lifetime again. A single
    storage instance serializes access; sharing a file between writers/processes
    requires an application storage implementation with stronger coordination.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = asyncio.Lock()
        self._loop = None

    def _bind_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise RuntimeError("MCPFileTokenStorage must be used on its owning loop.")
        self._loop = loop

    def _read(self) -> dict:
        try:
            record = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"version": 1}
        if not isinstance(record, dict) or record.get("version") != 1:
            raise ValueError("Invalid MCP credential storage format.")
        return record

    def _write(self, record: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, temporary = tempfile.mkstemp(prefix=".mcp-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(record, stream, ensure_ascii=False, allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    async def _io(self, function, *arguments):
        # Thread cancellation cannot stop a filesystem operation. Keep the
        # storage lock until that operation settles, including on cancellation.
        task = asyncio.create_task(asyncio.to_thread(function, *arguments))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not task.cancelled():
                task.exception()
            raise

    async def get_tokens(self) -> OAuthToken | None:
        from mcp.shared.auth import OAuthToken

        self._bind_loop()
        async with self._lock:
            record = await self._io(self._read)
        if record.get("tokens") is None:
            return None
        tokens = OAuthToken.model_validate(record["tokens"])
        expires_at = record.get("expires_at")
        if expires_at is not None:
            tokens.expires_in = max(0, int(expires_at - time.time()))
        return tokens

    async def set_tokens(self, tokens: OAuthToken) -> None:
        self._bind_loop()
        value = tokens.model_dump(mode="json")
        expiry = None if tokens.expires_in is None else time.time() + tokens.expires_in
        async with self._lock:
            record = await self._io(self._read)
            record.update(tokens=value, expires_at=expiry)
            await self._io(self._write, record)

    async def get_client_info(self) -> OAuthClientInformationFull | None:
        from mcp.shared.auth import OAuthClientInformationFull

        self._bind_loop()
        async with self._lock:
            record = await self._io(self._read)
        return None if record.get("client_info") is None else OAuthClientInformationFull.model_validate(record["client_info"])

    async def set_client_info(self, client_info: OAuthClientInformationFull) -> None:
        self._bind_loop()
        value = client_info.model_dump(mode="json")
        async with self._lock:
            record = await self._io(self._read)
            record["client_info"] = value
            await self._io(self._write, record)


__all__ = ["MCPFileTokenStorage"]
