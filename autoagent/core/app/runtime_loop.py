"""Lazy owned asyncio loop with a notification-driven submission channel."""

from __future__ import annotations

import asyncio
import os
import threading
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Coroutine, Generic, TypeVar

from ..executor.future import await_concurrent_future


T = TypeVar("T")


class _RuntimeFuture(Future[T]):
    """Bridge result plus the later physical Task-settled boundary."""

    def __init__(self) -> None:
        super().__init__()
        self.settled: Future[None] = Future()


@dataclass(slots=True)
class _Submission(Generic[T]):
    coroutine: Coroutine[object, object, T]
    future: _RuntimeFuture[T]
    task: asyncio.Task[T] | None = None


class RuntimeLoop:
    """Own one event-loop thread without polling or eager thread creation."""

    def __init__(self) -> None:
        # One lock linearizes the complete submit/close lifecycle.  In
        # particular, a submission is placed on the notification queue before
        # close is allowed to enqueue the terminal stop action.
        self._lifecycle_lock = threading.Lock()
        self._actions: deque[tuple[str, object | None]] = deque()
        self._actions_lock = threading.Lock()
        self._ready = threading.Event()
        self._reader: int | None = None
        self._writer: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._closed = False

    @property
    def started(self) -> bool:
        with self._lifecycle_lock:
            return self._thread is not None

    def submit(self, coroutine: Coroutine[object, object, T]) -> Future[T]:
        submission = _Submission(coroutine, _RuntimeFuture())

        def cancelled(future: Future[T]) -> None:
            if future.cancelled():
                self._enqueue("cancel", submission)

        submission.future.add_done_callback(cancelled)
        try:
            with self._lifecycle_lock:
                if self._closed:
                    raise RuntimeError("RuntimeLoop is closed.")
                self._ensure_started_locked()
                self._enqueue_action("submit", submission)
        except BaseException:
            coroutine.close()
            raise
        return submission.future

    def run(self, coroutine: Coroutine[object, object, T]) -> T:
        if self._thread is threading.current_thread():
            coroutine.close()
            raise RuntimeError("RuntimeLoop.run cannot block its own loop thread.")
        return self.submit(coroutine).result()

    async def wait(self, future: Future[T]) -> T:
        try:
            return await await_concurrent_future(future)
        except asyncio.CancelledError:
            # ``Future.cancel()`` completes the cross-thread bridge before the
            # RuntimeLoop Task has handled cancellation.  Join that physical
            # cleanup so an async facade cannot return while Runtime State is
            # still running but its owned Task is already disappearing.
            if isinstance(future, _RuntimeFuture):
                await asyncio.shield(await_concurrent_future(future.settled))
            raise

    def close(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            thread = self._thread
            if thread is threading.current_thread():
                raise RuntimeError("RuntimeLoop cannot join its own thread.")
            self._closed = True
            if thread is None:
                return
            self._enqueue_action("stop", None)
        thread.join()
        with self._lifecycle_lock:
            if self._reader is not None:
                os.close(self._reader)
                self._reader = None
            if self._writer is not None:
                os.close(self._writer)
                self._writer = None
            self._loop = None
            self._thread = None

    def _ensure_started_locked(self) -> None:
        if self._thread is not None:
            return
        if os.name != "nt":
            self._reader, self._writer = os.pipe()
            os.set_blocking(self._reader, False)
            os.set_blocking(self._writer, False)
        self._ready.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="autoagent-runtime",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def _enqueue(self, kind: str, value: object | None) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._enqueue_action(kind, value)

    def _enqueue_action(self, kind: str, value: object | None) -> None:
        """Queue one action while the caller owns ``_lifecycle_lock``."""

        with self._actions_lock:
            self._actions.append((kind, value))
        if self._writer is not None:
            try:
                os.write(self._writer, b"\0")
            except (BlockingIOError, OSError):
                pass
            return
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(self._drain_actions)
            except RuntimeError:
                pass

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        if self._reader is not None:
            loop.add_reader(self._reader, self._drain_pipe)
            self._ready.set()
        else:
            loop.call_soon(self._ready.set)
        loop.run_forever()
        if self._reader is not None:
            loop.remove_reader(self._reader)
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    def _drain_pipe(self) -> None:
        assert self._reader is not None
        try:
            while os.read(self._reader, 4096):
                pass
        except (BlockingIOError, OSError):
            pass
        self._drain_actions()

    def _drain_actions(self) -> None:
        loop = self._loop
        assert loop is not None
        with self._actions_lock:
            actions = tuple(self._actions)
            self._actions.clear()
        for kind, value in actions:
            if kind == "stop":
                loop.stop()
                continue
            submission = value
            assert isinstance(submission, _Submission)
            if kind == "cancel":
                if submission.task is not None:
                    submission.task.cancel()
                continue
            if submission.future.cancelled():
                submission.coroutine.close()
                if not submission.future.settled.done():
                    submission.future.settled.set_result(None)
                continue
            task = loop.create_task(submission.coroutine)
            submission.task = task

            def finished(
                done: asyncio.Task[object],
                target: _Submission[object] = submission,
            ) -> None:
                try:
                    if target.future.done():
                        # A cancelled bridge Future no longer accepts the Task
                        # result, but the Task exception must still be observed.
                        if not done.cancelled():
                            done.exception()
                        return
                    if done.cancelled():
                        target.future.cancel()
                        return
                    error = done.exception()
                    if error is not None:
                        target.future.set_exception(error)
                    else:
                        target.future.set_result(done.result())
                finally:
                    if not target.future.settled.done():
                        target.future.settled.set_result(None)

            task.add_done_callback(finished)


__all__ = ["RuntimeLoop"]
