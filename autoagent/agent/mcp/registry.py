"""Explicit, atomic replacement of discovered Tool snapshots."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, aclosing
import math
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
        self._observers = set()
        client._registries.add(self)

    def subscribe(self, callback):
        """Observe completed catalog publications on the client's owning loop.

        The callback is synchronous so reconnect cannot finish before dependent
        registries have published the new generation. The returned function
        removes the observer; clients and transports remain application-owned.
        """
        self._observers.add(callback)
        return lambda: self._observers.discard(callback)

    def _notify(self):
        for callback in tuple(self._observers):
            callback(self)

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def server_id(self) -> str:
        return self._client.config.server_id

    @property
    def generation(self) -> int:
        return self._client.generation

    @property
    def metadata(self) -> dict:
        """Full discovery contracts, including output schemas and annotations."""
        return {name: tool.metadata for name, tool in self.snapshot._tools.items()}

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
            self._notify()
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
        self._notify()

    async def aprepare(self) -> None:
        await self._client.aprepare()
        if self._snapshot is None or self._generation != self._client.generation:
            await self.refresh()

    async def ainvoke(self, call: LLMToolCall) -> LLMToolResult:
        await self.aprepare()
        return await self.snapshot.ainvoke(call)

    async def watch(self, *, policy=None, poll_interval_seconds=30.0):
        """Use supported notifications, with catalog polling when unavailable."""
        if (isinstance(poll_interval_seconds, bool) or not isinstance(poll_interval_seconds, (int, float))
                or not math.isfinite(poll_interval_seconds) or poll_interval_seconds <= 0):
            raise ValueError("MCP catalog poll interval must be finite and positive.")
        await self.aprepare()
        notifications = bool(self._client.server_capabilities.get("tools", {}).get("listChanged"))
        if self._client.protocol_version >= "2026-07-28" and notifications:
            from mcp.shared.exceptions import MCPError
            async with AsyncExitStack() as stack:
                try:
                    subscription = await stack.enter_async_context(
                        self._client.listen_resilient(tools_list_changed=True, policy=policy))
                except MCPError as error:
                    # Some peers advertise changes but omit the subscription
                    # method. Do not hide authentication or discovery failures.
                    if error.code != -32601:
                        raise
                else:
                    async for update in subscription:
                        if update.type == "connected" and not update.honored.get("toolsListChanged"):
                            break
                        await self.refresh()
                        yield update
                    else:
                        return
            notifications = False
        async with aclosing(self._watch_messages(policy=policy,
                poll_interval_seconds=None if notifications else poll_interval_seconds)) as updates:
            async for update in updates:
                yield update

    async def _watch_messages(self, *, policy, poll_interval_seconds):
        from .recovery import MCPChangeEvent, MCPSubscriptionUpdate
        changed = asyncio.Event()
        def notice(message):
            if getattr(message, "method", None) == "notifications/tools/list_changed" or (
                isinstance(message, Exception) and self._client._is_connection_error(message)
            ):
                changed.set()
        unsubscribe = self._client.observe_messages(notice)
        try:
            # Observe before rediscovery so an initial change cannot be lost.
            await self.refresh()
            yield MCPSubscriptionUpdate(type="connected", generation=self._client.generation,
                resync_required=True, honored={})
            while True:
                try:
                    await asyncio.wait_for(changed.wait(), timeout=poll_interval_seconds)
                except TimeoutError:
                    pass
                changed.clear()
                previous, metadata = self._generation, self.metadata
                try:
                    await self.refresh()
                except Exception as error:
                    if not self._client._is_connection_error(error):
                        raise
                    await self._client.areconnect(policy=policy, expected_generation=previous)
                    await self.refresh()
                if self._client.generation != previous:
                    yield MCPSubscriptionUpdate(type="connected", generation=self._client.generation,
                        resync_required=True, honored={})
                elif self.metadata != metadata:
                    yield MCPSubscriptionUpdate(type="notification", generation=self._client.generation,
                        event=MCPChangeEvent(type="tools_list_changed"))
        finally:
            unsubscribe()

    def as_operator(self, *, id: str = "mcp_tools") -> Operator:
        from autoagent.core import Operator

        return Operator(self.ainvoke, id=id)


__all__ = ["MCPToolRegistry"]
