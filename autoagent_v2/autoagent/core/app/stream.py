"""Strict caller-driven streams attached to one Invocation."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from typing import Generic, TypeVar, cast


_END = object()
_NO_TERMINAL_ITEM = object()
T = TypeVar("T")


class AttachedStream:
    """A zero-backlog rendezvous between Runtime execution and one caller."""

    def __init__(self) -> None:
        self._demand = asyncio.Event()
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=1)
        self._publish_lock = asyncio.Lock()
        self._ack: asyncio.Event | None = None
        self._closed = False
        self._abort_publishers = False
        self._error: BaseException | None = None
        self._terminal_delivered = False
        self._terminal_item: object = _NO_TERMINAL_ITEM

    @property
    def attached(self) -> bool:
        """Whether newly created updates still belong to the caller stream."""

        return not self._closed

    async def publish(self, item: object) -> None:
        await self.publish_created(lambda: item)

    async def publish_terminal(self, item: object) -> None:
        """Publish the final Result and make later publishers non-attached."""

        async def create() -> object:
            return item

        await self.publish_async_created(create, terminal=True)

    async def publish_created(self, create: Callable[[], object]) -> object:
        """Create and publish one item only after the caller requests it."""

        async def create_async() -> object:
            return create()

        return await self.publish_async_created(create_async)

    async def publish_async_created(
        self,
        create: Callable[[], Awaitable[object]],
        *,
        terminal: bool = False,
    ) -> object:
        """Await construction after demand, then wait for caller acknowledgement."""

        async with self._publish_lock:
            if self._closed:
                if self._abort_publishers:
                    raise asyncio.CancelledError
                return await create()
            await self._demand.wait()
            if self._closed:
                if self._abort_publishers:
                    raise asyncio.CancelledError
                return await create()
            self._demand.clear()
            try:
                item = await create()
            except asyncio.CancelledError:
                self._closed = True
                self._abort_publishers = True
                self._offer_end()
                raise
            except BaseException as error:
                self._closed = True
                self._abort_publishers = True
                self._error = error
                self._offer_end()
                raise
            ack = asyncio.Event()
            self._ack = ack
            if terminal:
                self._terminal_item = item
            await self._queue.put(item)
            await ack.wait()
            return item

    async def receive(self) -> object:
        if self._closed and self._terminal_delivered:
            return _END
        if self._ack is not None:
            self._ack.set()
            self._ack = None
        self._demand.set()
        item = await self._queue.get()
        if item is self._terminal_item:
            # Receipt of the final Result is the graceful terminal boundary.
            # The caller need not request END before closing the iterator.
            # Publishers already queued behind this item must still commit,
            # but they are no longer attached to the completed Root stream.
            self._terminal_item = _NO_TERMINAL_ITEM
            self._closed = True
            self._abort_publishers = False
            if self._ack is not None:
                self._ack.set()
                self._ack = None
            self._offer_end()
        if item is _END:
            self._terminal_delivered = True
        if item is _END and self._error is not None:
            raise self._error
        return item

    async def finish(self, error: BaseException | None = None) -> None:
        if self._closed:
            return
        if self._ack is not None:
            await self._ack.wait()
        self._closed = True
        self._abort_publishers = error is not None
        self._error = error
        await self._demand.wait()
        self._demand.clear()
        self._offer_end()

    def abandon(self) -> None:
        graceful = self._closed and not self._abort_publishers
        self._closed = True
        self._abort_publishers = not graceful
        self._demand.set()
        if self._ack is not None:
            self._ack.set()
            self._ack = None
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._offer_end()

    def _offer_end(self) -> None:
        """Publish the single terminal sentinel without ever waiting for space."""

        if self._terminal_delivered:
            return
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
        try:
            item = self._receive()
        except BaseException:
            # The channel's terminal error consumes its sentinel.  Mark the
            # iterator closed so a caller that catches the error cannot block
            # forever by asking this exhausted stream for another item.
            self._closed = True
            raise
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
