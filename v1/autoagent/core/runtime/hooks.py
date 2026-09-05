from __future__ import annotations

import asyncio
import inspect
import logging
from concurrent.futures import Future
from collections.abc import Awaitable, Callable
from threading import Event, Lock, Thread, get_ident
from typing import Any, TypeVar


T = TypeVar("T")
logger = logging.getLogger(__name__)


class RuntimeEventLoop:
    """One App-owned event loop shared by its sync and async entrypoints."""

    def __init__(self, *, name: str = "autoagent-runtime") -> None:
        self.name = name
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._thread_id: int | None = None
        self._started = Event()
        self._ready = Event()
        self._lock = Lock()
        self._pending_lock = Lock()
        self._pending_wakeups = 0
        self._polling_waits = 0

    def start(self) -> None:
        owner = False
        with self._lock:
            if self._thread is None:
                self._thread = Thread(target=self._run, name=self.name, daemon=True)
                self._thread.start()
                owner = True
        if owner:
            self._started.wait()
            self._ready.set()
        else:
            self._ready.wait()

    def is_current(self) -> bool:
        return self._thread_id == get_ident()

    def submit(self, awaitable: Awaitable[T]) -> Future[T]:
        self.start()
        assert self._loop is not None
        completed: Future[None] = Future()
        accepted_lock = Lock()
        accepted = False
        self._begin_pending_wakeup()

        def mark_accepted() -> bool:
            nonlocal accepted
            with accepted_lock:
                if accepted:
                    return False
                accepted = True
            self._end_pending_wakeup()
            return True

        async def tracked() -> T:
            mark_accepted()
            try:
                return await awaitable
            finally:
                if not completed.done():
                    completed.set_result(None)

        future = asyncio.run_coroutine_threadsafe(tracked(), self._loop)

        def submission_done(submitted: Future[T]) -> None:
            # Cancellation can win before ``tracked`` starts. In that case no
            # coroutine body exists to acknowledge the wakeup or completion.
            accepted_here = mark_accepted()
            if (
                submitted.cancelled()
                and accepted_here
                and not completed.done()
            ):
                close = getattr(awaitable, "close", None)
                if callable(close):
                    close()
                completed.set_result(None)

        future.add_done_callback(submission_done)
        setattr(future, "_autoagent_completed", completed)
        return future

    def call_soon(self, callback: Callable[..., Any], *args: Any) -> None:
        """Submit one-way work without allocating a caller-visible Future."""

        self.start()
        assert self._loop is not None
        self._begin_pending_wakeup()

        def tracked_callback() -> None:
            self._end_pending_wakeup()
            callback(*args)

        self._loop.call_soon_threadsafe(tracked_callback)

    def _begin_pending_wakeup(self) -> None:
        """Request the low-latency watchdog until submitted work is accepted."""

        with self._pending_lock:
            self._pending_wakeups += 1

    def _end_pending_wakeup(self) -> None:
        with self._pending_lock:
            self._pending_wakeups = max(0, self._pending_wakeups - 1)

    def _has_pending_wakeup(self) -> bool:
        with self._pending_lock:
            return self._pending_wakeups > 0 or self._polling_waits > 0

    def begin_polling_wait(self) -> None:
        """Bound a known external callback wait to the 5 ms watchdog."""

        with self._pending_lock:
            self._polling_waits += 1

    def end_polling_wait(self) -> None:
        with self._pending_lock:
            self._polling_waits = max(
                0,
                self._polling_waits - 1,
            )

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
        try:
            return await _await_concurrent_future(future)
        except asyncio.CancelledError:
            # Cancellation belongs to the invocation, not merely to this
            # caller-side proxy. Forward it to the App runtime loop and wait
            # until WorkflowExecutor has cancelled its workers and committed
            # the terminal Event.
            future.cancel()
            completed = getattr(future, "_autoagent_completed")
            await asyncio.shield(_await_concurrent_future(completed))
            raise

    def stop(self, *, timeout_s: float = 5.0) -> None:
        loop = self._loop
        thread = self._thread
        if loop is None:
            return
        if self.is_current():
            loop.stop()
            return
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            # The wake polling task can take up to 50 ms to recover a
            # lost stop wakeup, so reserve that small cleanup floor even when a
            # deployment configures a shorter application grace period.
            effective_timeout_s = max(0.1, timeout_s)
            thread.join(timeout=effective_timeout_s)
            if thread.is_alive():
                logger.error(
                    "Runtime Event Loop thread %s did not stop within %.3f seconds; "
                    "leaving the daemon thread isolated during process shutdown.",
                    self.name,
                    effective_timeout_s,
                )
                return
        self._loop = None
        self._thread = None
        self._thread_id = None
        self._started.clear()
        self._ready.clear()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._thread_id = get_ident()
        self._started.set()
        # Hardened/embedded hosts can intermittently drop asyncio's selector
        # self-pipe wakeup. Keep a low-frequency recovery pulse, and temporarily
        # shorten it only until cross-thread work has actually entered the loop.
        # Long-running Workflow work therefore creates no periodic 1 ms timer.
        async def wake_polling() -> None:
            while True:
                await asyncio.sleep(
                    0.005 if self._has_pending_wakeup() else 0.05
                )

        pulse_task = loop.create_task(wake_polling())
        try:
            loop.run_forever()
        finally:
            pulse_task.cancel()
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.close()


async def _await_concurrent_future(future: Future[T]) -> T:
    """Await a cross-thread Future with bounded wake polling."""

    wrapped = asyncio.wrap_future(future)
    while not wrapped.done():
        tick = asyncio.create_task(asyncio.sleep(0.05))
        try:
            done, _ = await asyncio.wait(
                (wrapped, tick),
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            if not tick.done():
                tick.cancel()
                await asyncio.gather(tick, return_exceptions=True)
        if wrapped in done:
            break
    return wrapped.result()


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
