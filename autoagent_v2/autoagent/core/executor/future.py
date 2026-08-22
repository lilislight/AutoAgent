"""Cancellation-aware, notification-driven Future bridge."""

from __future__ import annotations

import asyncio
import os
import threading
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TypeVar


T = TypeVar("T")


@dataclass(slots=True)
class _Registration:
    waiter: asyncio.Future[None]
    active: bool = True


class _FutureNotifier:
    """Wake one asyncio loop from arbitrary threads through a pipe."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self._reader, self._writer = os.pipe()
        os.set_blocking(self._reader, False)
        os.set_blocking(self._writer, False)
        self._ready: deque[_Registration] = deque()
        self._lock = threading.Lock()
        self.users = 0
        self.loop.add_reader(self._reader, self._drain)

    def notify(self, registration: _Registration) -> None:
        with self._lock:
            if not registration.active:
                return
            self._ready.append(registration)
            try:
                os.write(self._writer, b"\0")
            except BlockingIOError:
                # One unread byte is enough: drain processes the complete queue.
                pass

    def deactivate(self, registration: _Registration) -> None:
        with self._lock:
            registration.active = False

    def close(self) -> None:
        self.loop.remove_reader(self._reader)
        os.close(self._reader)
        os.close(self._writer)

    def _drain(self) -> None:
        try:
            while os.read(self._reader, 4096):
                pass
        except (BlockingIOError, OSError):
            pass
        with self._lock:
            ready = tuple(self._ready)
            self._ready.clear()
        for registration in ready:
            if registration.active and not registration.waiter.done():
                registration.waiter.set_result(None)


_notifiers: dict[asyncio.AbstractEventLoop, _FutureNotifier] = {}
_notifiers_lock = threading.Lock()


def _acquire_notifier(loop: asyncio.AbstractEventLoop) -> _FutureNotifier:
    with _notifiers_lock:
        notifier = _notifiers.get(loop)
        if notifier is None:
            notifier = _FutureNotifier(loop)
            _notifiers[loop] = notifier
        notifier.users += 1
        return notifier


def _release_notifier(notifier: _FutureNotifier) -> None:
    with _notifiers_lock:
        notifier.users -= 1
        if notifier.users:
            return
        if _notifiers.get(notifier.loop) is notifier:
            del _notifiers[notifier.loop]
    notifier.close()


async def await_concurrent_future(future: Future[T]) -> T:
    """Await a concurrent Future without polling and propagate cancellation."""

    loop = asyncio.get_running_loop()
    notifier = _acquire_notifier(loop)
    registration = _Registration(loop.create_future())
    future.add_done_callback(lambda _done: notifier.notify(registration))
    try:
        await registration.waiter
        return future.result()
    except asyncio.CancelledError:
        future.cancel()
        raise
    finally:
        notifier.deactivate(registration)
        _release_notifier(notifier)


__all__ = ["await_concurrent_future"]
