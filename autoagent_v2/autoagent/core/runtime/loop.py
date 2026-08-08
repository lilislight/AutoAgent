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
        # ``call_soon_threadsafe`` is safe before ``run_forever`` and writes to
        # the loop's self-pipe. Publish the initialized loop directly; using a
        # loop callback for readiness creates a race where the caller can wait
        # forever before it gets a chance to submit the first coroutine.
        self._ready.set()
        loop.run_forever()
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
        return await asyncio.wrap_future(self.submit(coroutine))

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
