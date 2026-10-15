"""Bounded connection recovery and subscriptions that explicitly request resync."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from ..models._base import Model, NonEmpty


class MCPReconnectPolicy(Model):
    """Opt-in connection attempts; dispatched application RPCs are never retried."""

    max_attempts: int = Field(default=3, ge=1, le=100)
    initial_delay_seconds: float = Field(default=0.1, ge=0)
    max_delay_seconds: float = Field(default=2.0, ge=0)
    multiplier: float = Field(default=2.0, ge=1)
    drain_timeout_seconds: float = Field(default=60.0, gt=0)

    @model_validator(mode="after")
    def _delay_range(self):
        if self.initial_delay_seconds > self.max_delay_seconds:
            raise ValueError("Initial reconnect delay must not exceed its maximum.")
        return self


class MCPChangeEvent(Model):
    type: Literal["tools_list_changed", "prompts_list_changed", "resources_list_changed", "resource_updated"]
    uri: NonEmpty | None = None

    @model_validator(mode="after")
    def _uri(self):
        if (self.type == "resource_updated") != (self.uri is not None):
            raise ValueError("Only resource update events require a URI.")
        return self


class MCPSubscriptionUpdate(Model):
    """Portable update; a connected update requires application state refetch.

    Notifications lost while disconnected cannot be replayed. The first update
    and every reopened subscription request resync, including same-session gaps.
    """

    type: Literal["connected", "notification"]
    generation: int = Field(ge=1)
    resync_required: bool = False
    honored: dict[str, JsonValue] | None = None
    event: MCPChangeEvent | None = None

    @model_validator(mode="after")
    def _shape(self):
        if self.type == "connected":
            if not self.resync_required or self.honored is None or self.event is not None:
                raise ValueError("Connection updates require resync and the honored subscription filter.")
        elif self.event is None or self.resync_required or self.honored is not None:
            raise ValueError("Notification updates require one change event.")
        return self


def _change_event(event):
    from mcp.shared.subscriptions import ToolsListChanged, PromptsListChanged, ResourcesListChanged, ResourceUpdated

    for cls, kind in ((ToolsListChanged, "tools_list_changed"), (PromptsListChanged, "prompts_list_changed"),
                      (ResourcesListChanged, "resources_list_changed")):
        if isinstance(event, cls):
            return MCPChangeEvent(type=kind)
    if isinstance(event, ResourceUpdated):
        return MCPChangeEvent(type="resource_updated", uri=event.uri)
    raise ValueError("Unsupported MCP subscription event.")


class RecoveringMCPSubscription:
    """Consumer-driven recovery; holds no background task or unbounded queue."""

    def __init__(self, client, options, policy):
        self.client = client
        self.options = options
        self.policy = policy
        self._context = None
        self._subscription = None
        self._generation = None
        self._closed = False
        self._announce = False
        self._advancing = False

    async def _open(self):
        context = self.client.listen(**self.options)
        subscription = await context.__aenter__()
        self._context, self._subscription = context, subscription
        self._generation = self.client.generation
        self._announce = True

    async def _release(self):
        context, self._context = self._context, None
        self._subscription = None
        if context is not None:
            try:
                await context.__aexit__(None, None, None)
            except Exception as error:
                if not self.client._is_connection_error(error):
                    raise

    async def _recover(self, error):
        if not self.client._is_connection_error(error):
            raise error
        await self._release()
        last = error
        delay = self.policy.initial_delay_seconds
        import asyncio
        for attempt in range(self.policy.max_attempts):
            if delay:
                await asyncio.sleep(delay)
            try:
                once = self.policy.model_copy(update={"max_attempts": 1, "initial_delay_seconds": 0.0})
                await self.client.areconnect(policy=once, expected_generation=self._generation)
                await self._open()
                return
            except Exception as failure:
                if not self.client._is_connection_error(failure):
                    raise
                last = failure
            delay = min(self.policy.max_delay_seconds, delay * self.policy.multiplier)
        raise last

    async def start(self):
        try:
            await self._open()
        except Exception as error:
            await self._recover(error)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._advancing:
            raise RuntimeError("Consume a recovering subscription from one task at a time.")
        self._advancing = True
        try:
            while not self._closed:
                if self._announce:
                    self._announce = False
                    return MCPSubscriptionUpdate(type="connected", generation=self._generation, resync_required=True,
                                                 honored=self._subscription.honored.model_dump(mode="json", by_alias=True, exclude_none=True))
                try:
                    event = await anext(self._subscription)
                    return MCPSubscriptionUpdate(type="notification", generation=self._generation, event=_change_event(event))
                except StopAsyncIteration:
                    await self.aclose()
                    raise
                except Exception as error:
                    await self._recover(error)
            raise StopAsyncIteration
        finally:
            self._advancing = False

    async def aclose(self):
        self._closed = True
        await self._release()


__all__ = ["MCPReconnectPolicy", "MCPSubscriptionUpdate", "MCPChangeEvent"]
