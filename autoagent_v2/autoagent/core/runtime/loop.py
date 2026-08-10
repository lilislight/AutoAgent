"""One lazily-created Event Loop shared by every App execution API."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar


T = TypeVar("T")


class RuntimeLoop:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._lock = threading.Lock()

    def _ensure_started(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name="autoagent-v2-runtime",
                    daemon=True,
                )
                self._thread.start()
        self._ready.wait()
        assert self._loop is not None
        return self._loop

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        # Publish readiness from inside the first Loop turn. Signalling before
        # ``run_forever`` lets another thread submit while the selector has not
        # started yet; on some event-loop implementations that first wake-up
        # can be missed and the initial App call waits forever.
        loop.call_soon(self._ready.set)
        # Some hardened/embedded selectors can lose the self-pipe wakeup used
        # by ``call_soon_threadsafe``. A small watchdog timer guarantees that
        # cross-thread submissions are observed instead of waiting forever.
        # Five milliseconds is a latency bound, not a sleep in Workflow work.
        async def wake_watchdog() -> None:
            while True:
                await asyncio.sleep(0.005)

        watchdog = loop.create_task(wake_watchdog())
        loop.run_forever()
        watchdog.cancel()
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    def submit(self, coroutine: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        return asyncio.run_coroutine_threadsafe(coroutine, self._ensure_started())

    def run(self, coroutine: Coroutine[Any, Any, T]) -> T:
        if self._thread is threading.current_thread():
            raise RuntimeError("A synchronous App API cannot run on the Runtime Loop.")
        return self.submit(coroutine).result()

    async def await_result(self, coroutine: Coroutine[Any, Any, T]) -> T:
        future = self.submit(coroutine)
        # A hardened caller Loop may lose the cross-thread callback wakeup just
        # like the Runtime Loop. Poll only while this specific Future is
        # outstanding so async App methods cannot wait forever after Core has
        # already completed the work.
        while not future.done():
            await asyncio.sleep(0.005)
        return future.result()

    def close(self) -> None:
        with self._lock:
            loop = self._loop
            thread = self._thread
        if loop is None or thread is None:
            return
        loop.call_soon_threadsafe(loop.stop)
        thread.join()
        with self._lock:
            self._loop = None
            self._thread = None
            self._ready.clear()
