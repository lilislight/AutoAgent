from __future__ import annotations

import asyncio
import inspect
from concurrent.futures import Future
from collections.abc import Awaitable, Callable
from threading import Event, Lock, Thread, get_ident
from typing import Any, TypeVar


T = TypeVar("T")


class RuntimeEventLoop:
    """One App-owned event loop shared by its sync and async entrypoints."""

    def __init__(self, *, name: str = "autoagent-runtime") -> None:
        self.name = name
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._thread_id: int | None = None
        self._started = Event()
        self._lock = Lock()

    def start(self) -> None:
        if self._loop is not None:
            return
        with self._lock:
            if self._loop is not None:
                return
            self._thread = Thread(target=self._run, name=self.name, daemon=True)
            self._thread.start()
            self._started.wait()

    def is_current(self) -> bool:
        return self._thread_id == get_ident()

    def submit(self, awaitable: Awaitable[T]) -> Future[T]:
        self.start()
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(awaitable, self._loop)

    def run(self, awaitable: Awaitable[T]) -> T:
        if self.is_current():
            close = getattr(awaitable, "close", None)
            if callable(close):
                close()
            raise RuntimeError("A synchronous App API cannot block its Runtime loop.")
        return self.submit(awaitable).result()

    async def arun(self, awaitable: Awaitable[T]) -> T:
        if self.is_current():
            return await awaitable
        future = self.submit(awaitable)
        # Polling avoids relying on a restricted host's cross-thread self-pipe
        # to wake the caller loop when the concurrent Future completes.
        while not future.done():
            await asyncio.sleep(0.001)
        return future.result()

    def stop(self) -> None:
        loop = self._loop
        thread = self._thread
        if loop is None:
            return
        if self.is_current():
            loop.stop()
            return
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join()
        self._loop = None
        self._thread = None
        self._thread_id = None
        self._started.clear()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._thread_id = get_ident()
        self._started.set()
        # Some restricted runtimes disable asyncio's cross-thread self-pipe.
        # A tiny heartbeat bounds command pickup latency even when
        # call_soon_threadsafe cannot wake the selector directly.
        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(0.001)

        heartbeat_task = loop.create_task(heartbeat())
        try:
            loop.run_forever()
        finally:
            heartbeat_task.cancel()
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.close()


async def invoke_hook_async(hook: Callable[..., Any], *args: Any) -> Any:
    """Call a user hook and await its result only when necessary."""

    result = hook(*args)
    return await result if inspect.isawaitable(result) else result


def run_sync(
    awaitable: Awaitable[T],
    *,
    api_name: str,
    async_api_name: str | None = None,
) -> T:
    """Run an async-first API from synchronous code.

    Blocking an already-running event loop would deadlock and moving the whole
    invocation to a hidden thread would defeat cancellation and task ownership.
    Async callers must therefore use the corresponding async API explicitly.
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)

    close = getattr(awaitable, "close", None)
    if callable(close):
        close()
    replacement = async_api_name or f"a{api_name}"
    raise RuntimeError(
        f"{api_name} cannot be called from a running event loop; "
        f"use await {replacement}(...) instead."
    )
