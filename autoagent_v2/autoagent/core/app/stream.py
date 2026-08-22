"""Strict caller-driven streams attached to one Invocation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from typing import Generic, TypeVar, cast


_END = object()
T = TypeVar("T")


class AttachedStream:
    """A zero-backlog rendezvous between Runtime execution and one caller."""

    def __init__(self) -> None:
        self._demand = asyncio.Event()
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=1)
        self._publish_lock = asyncio.Lock()
        self._ack: asyncio.Event | None = None
        self._closed = False
        self._error: BaseException | None = None

    async def publish(self, item: object) -> None:
        await self.publish_created(lambda: item)

    async def publish_created(self, create: Callable[[], object]) -> object:
        """Create and publish one item only after the caller requests it."""

        async with self._publish_lock:
            if self._closed:
                return create()
            await self._demand.wait()
            if self._closed:
                return create()
            self._demand.clear()
            try:
                item = create()
            except BaseException as error:
                self._closed = True
                self._error = error
                await self._queue.put(_END)
                raise
            ack = asyncio.Event()
            self._ack = ack
            await self._queue.put(item)
            await ack.wait()
            return item

    async def receive(self) -> object:
        if self._ack is not None:
            self._ack.set()
            self._ack = None
        self._demand.set()
        item = await self._queue.get()
        if item is _END and self._error is not None:
            raise self._error
        return item

    async def finish(self, error: BaseException | None = None) -> None:
        if self._closed:
            return
        if self._ack is not None:
            await self._ack.wait()
        self._closed = True
        self._error = error
        await self._demand.wait()
        self._demand.clear()
        await self._queue.put(_END)

    def abandon(self) -> None:
        self._closed = True
        self._demand.set()
        if self._ack is not None:
            self._ack.set()
            self._ack = None
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        try:
            self._queue.put_nowait(_END)
        except asyncio.QueueFull:
            pass


def is_stream_end(item: object) -> bool:
    return item is _END


class InvocationStream(Iterator[T], Generic[T]):
    """Synchronous iterator over one caller-driven attached stream."""

    def __init__(
        self,
        *,
        receive: Callable[[], object],
        close: Callable[[], None],
    ) -> None:
        self._receive = receive
        self._close = close
        self._closed = False

    def __iter__(self) -> "InvocationStream[T]":
        return self

    def __next__(self) -> T:
        if self._closed:
            raise StopIteration
        item = self._receive()
        if is_stream_end(item):
            self._closed = True
            raise StopIteration
        return cast(T, item)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._close()

    def __enter__(self) -> "InvocationStream[T]":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


__all__ = ["AttachedStream", "InvocationStream", "is_stream_end"]
