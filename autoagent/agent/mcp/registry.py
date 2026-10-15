"""Explicit, atomic replacement of discovered Tool snapshots."""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING

from ..models import LLMToolCall, LLMToolDefinition, LLMToolResult
from ..tools import ToolSet
from .client import MCPClient
from .client import MCPClientError

if TYPE_CHECKING:
    from autoagent.core import Operator


class MCPToolRegistry:
    """Refresh at application-selected boundaries; failed refresh keeps old Tools.

    Each request captures a single immutable ToolSet. An LLM turn can capture
    snapshot once for both its request definitions and subsequent dispatch.
    Reconnection rediscovery swaps the complete catalog. watch() handles change
    notifications in the consuming task. Tool calls are never replayed.
    """

    def __init__(
        self, client: MCPClient, *, prefix: str = "", names: Mapping[str, str] | None = None,
        include: Collection[str] | None = None, exclude: Collection[str] = (),
    ) -> None:
        self._client = client
        self._options = dict(prefix=prefix, names=dict(names or {}),
                             include=None if include is None else frozenset(include),
                             exclude=frozenset(exclude))
        self._lock = asyncio.Lock()
        self._snapshot = None
        self._revision = 0
        self._generation = None
        client._registries.add(self)

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def snapshot(self) -> ToolSet:
        if self._snapshot is None:
            raise RuntimeError("Refresh MCPToolRegistry before using its snapshot.")
        if self._generation != self._client.generation:
            raise MCPClientError("Tool catalog belongs to a previous connection; refresh before using it.",
                                 server_id=self._client.config.server_id, operation="discovery")
        return self._snapshot

    @property
    def definitions(self) -> tuple[LLMToolDefinition, ...]:
        return self.snapshot.definitions

    async def refresh(self) -> ToolSet:
        self._client._bind_loop()
        await self._client.aprepare()
        async with self._lock:
            sdk, generation = self._client._sdk, self._client.generation
            replacement = await self._client._toolset_on_sdk(sdk, **self._options)
            if sdk is not self._client._sdk:
                raise MCPClientError("Connection changed during Tool refresh.", server_id=self._client.config.server_id,
                                     operation="discovery", connection_lost=True)
            self._snapshot = replacement
            self._generation = generation
            self._revision += 1
            return replacement

    async def _restore(self, sdk, generation):
        if self._snapshot is None:
            return
        # No registry lock here: a user refresh may be waiting for the connection
        # lock. Publication is atomic on the owning loop and checks connection ID.
        replacement = await self._client._toolset_on_sdk(sdk, **self._options)
        if sdk is not self._client._sdk:
            raise MCPClientError("Connection changed during Tool rediscovery.", server_id=self._client.config.server_id,
                                 operation="discovery", connection_lost=True)
        self._snapshot = replacement
        self._generation = generation
        self._revision += 1

    async def aprepare(self) -> None:
        await self._client.aprepare()
        if self._snapshot is None or self._generation != self._client.generation:
            await self.refresh()

    async def ainvoke(self, call: LLMToolCall) -> LLMToolResult:
        await self.aprepare()
        return await self.snapshot.ainvoke(call)

    async def watch(self, *, policy=None):
        """Refresh on subscribed changes and expose resync boundaries to the caller."""
        await self.aprepare()
        async with self._client.listen_resilient(tools_list_changed=True, policy=policy) as subscription:
            async for update in subscription:
                await self.refresh()
                yield update

    def as_operator(self, *, id: str = "mcp_tools") -> Operator:
        from autoagent.core import Operator

        return Operator(self.ainvoke, id=id)


__all__ = ["MCPToolRegistry"]
