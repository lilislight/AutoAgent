"""Strict caller-driven attached streams."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from .events import Event, RuntimeEvent, UserEvent
from .invocation import Invocation


EventChannel = Literal["user", "runtime", "all"]
_END = object()


class AttachedChannel:
    """A zero-backlog logical rendezvous between execution and one caller."""

    def __init__(self, event_channel: EventChannel) -> None:
        if event_channel not in {"user", "runtime", "all"}:
            raise ValueError(f"Unsupported attached stream channel: {event_channel!r}")
        self.event_channel = event_channel
        self._demand = asyncio.Event()
        self._queue: asyncio.Queue[Event | object] = asyncio.Queue(maxsize=1)
        self._ack: asyncio.Event | None = None
        self._closed = False

    def selects(self, event: Event) -> bool:
        return (
            self.event_channel == "all"
            or self.event_channel == "runtime"
            and isinstance(event, RuntimeEvent)
            or self.event_channel == "user"
            and isinstance(event, UserEvent)
        )

    async def wait_started(self) -> None:
        await self._demand.wait()

    async def publish(self, event: Event) -> None:
        if self._closed or not self.selects(event):
            return
        await self._demand.wait()
        self._demand.clear()
        ack = asyncio.Event()
        self._ack = ack
        await self._queue.put(event)
        await ack.wait()

    async def receive(self) -> Event | object:
        if self._ack is not None:
            self._ack.set()
            self._ack = None
        self._demand.set()
        return await self._queue.get()

    async def finish(self) -> None:
        if self._closed:
            return
        if self._ack is not None:
            await self._ack.wait()
        self._closed = True
        await self._demand.wait()
        self._demand.clear()
        await self._queue.put(_END)

    def abandon(self) -> None:
        self._closed = True
        self._demand.set()
        if self._ack is not None:
            self._ack.set()
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        try:
            self._queue.put_nowait(_END)
        except asyncio.QueueFull:
            pass


class InvocationStream:
    def __init__(
        self,
        *,
        invocation: Invocation,
        receive: Callable[[], Event | object],
        close: Callable[[], None],
    ) -> None:
        self.invocation = invocation
        self._receive = receive
        self._close = close
        self._closed = False

    def __iter__(self) -> "InvocationStream":
        return self

    def __next__(self) -> Event:
        if self._closed:
            raise StopIteration
        item = self._receive()
        if item is _END:
            self._closed = True
            raise StopIteration
        return item  # type: ignore[return-value]

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._close()

    def __enter__(self) -> "InvocationStream":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AsyncInvocationStream:
    def __init__(
        self,
        *,
        invocation: Invocation,
        receive: Callable[[], Awaitable[Event | object]],
        close: Callable[[], Awaitable[None]],
    ) -> None:
        self.invocation = invocation
        self._receive = receive
        self._close = close
        self._closed = False

    def __aiter__(self) -> "AsyncInvocationStream":
        return self

    async def __anext__(self) -> Event:
        if self._closed:
            raise StopAsyncIteration
        item = await self._receive()
        if item is _END:
            self._closed = True
            raise StopAsyncIteration
        return item  # type: ignore[return-value]

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._close()

    async def __aenter__(self) -> "AsyncInvocationStream":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()
