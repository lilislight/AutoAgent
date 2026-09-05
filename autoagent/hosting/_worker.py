"""Loop-neutral single-thread worker used by blocking Host adapters."""

from __future__ import annotations

from concurrent.futures import Future as ThreadFuture
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
import threading
from typing import Awaitable, Callable, Generic, TypeVar

from autoagent.core.executor.future import await_concurrent_future


T = TypeVar("T")


@dataclass(slots=True)
class _Work(Generic[T]):
    function: Callable[..., T]
    arguments: tuple[object, ...]
    future: ThreadFuture[T]


class SerialWorker:
    """Own one blocking resource and bridge it to any caller event loop.

    The worker never binds a database/client to an asyncio loop. Async completion
    is delivered through a platform notifier so the same adapter can be used
    by Core's private RuntimeLoop and by arbitrary Server loops.
    """

    def __init__(self, name: str) -> None:
        self._queue: Queue[_Work[object] | None] = Queue()
        self._lock = threading.Lock()
        self._closed = False
        self._thread = threading.Thread(
            target=self._run,
            name=name,
            daemon=True,
        )
        self._thread.start()

    def call(self, function: Callable[..., T], *arguments: object) -> T:
        return self.submit(function, *arguments).result()

    def call_async(
        self, function: Callable[..., T], *arguments: object
    ) -> Awaitable[T]:
        """Submit immediately, then expose loop-neutral asynchronous waiting."""

        return await_thread_future(self.submit(function, *arguments))

    def submit(
        self, function: Callable[..., T], *arguments: object
    ) -> ThreadFuture[T]:
        """Atomically admit one operation and return its thread Future."""

        future: ThreadFuture[T] = ThreadFuture()
        with self._lock:
            self._ensure_open()
            self._queue.put(
                _Work(function, arguments, future)  # type: ignore[arg-type]
            )
        return future

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._queue.put(None)
        self._thread.join()

    def _run(self) -> None:
        while True:
            work = self._queue.get()
            if work is None:
                return
            if not work.future.set_running_or_notify_cancel():
                continue
            try:
                result = work.function(*work.arguments)
            except BaseException as error:
                if not work.future.done():
                    work.future.set_exception(error)
            else:
                if not work.future.done():
                    work.future.set_result(result)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Serial worker is closed.")


class ConcurrentWorker:
    """Run independent blocking calls concurrently without binding an event loop."""

    def __init__(self, name: str, *, max_workers: int) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be positive.")
        self._lock = threading.Lock()
        self._closed = False
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=name,
        )

    def call(self, function: Callable[..., T], *arguments: object) -> T:
        return self.submit(function, *arguments).result()

    def call_async(
        self, function: Callable[..., T], *arguments: object
    ) -> Awaitable[T]:
        """Submit immediately, then expose loop-neutral asynchronous waiting."""

        return await_thread_future(self.submit(function, *arguments))

    def submit(
        self, function: Callable[..., T], *arguments: object
    ) -> ThreadFuture[T]:
        """Atomically admit one operation and return its thread Future."""

        return self._submit(function, *arguments)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _submit(
        self, function: Callable[..., T], *arguments: object
    ) -> ThreadFuture[T]:
        with self._lock:
            if self._closed:
                raise RuntimeError("Concurrent worker is closed.")
            return self._executor.submit(function, *arguments)


async def await_thread_future(
    future: ThreadFuture[T],
    *,
    cancel_future: bool = True,
) -> T:
    """Await a thread Future on any asyncio event-loop implementation."""

    return await await_concurrent_future(
        future,
        cancel_future=cancel_future,
    )


async def run_in_daemon(
    function: Callable[[], T],
    *,
    name: str,
) -> T:
    """Run one lifecycle call independently from the caller's event loop."""

    future: ThreadFuture[T] = ThreadFuture()

    def run() -> None:
        try:
            result = function()
        except BaseException as error:
            if not future.done():
                future.set_exception(error)
        else:
            if not future.done():
                future.set_result(result)

    thread = threading.Thread(target=run, name=name, daemon=True)
    thread.start()
    return await await_thread_future(future)


__all__ = [
    "ConcurrentWorker",
    "SerialWorker",
    "await_thread_future",
    "run_in_daemon",
]
