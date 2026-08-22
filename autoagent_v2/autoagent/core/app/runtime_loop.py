"""Owned asyncio loop with an explicit cross-thread submission channel."""

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


@dataclass(slots=True)
class _Submission(Generic[T]):
    coroutine: Coroutine[object, object, T]
    future: Future[T]
    task: asyncio.Task[T] | None = None


class RuntimeLoop:
    """Run all live Runtime objects on one stable event-loop thread.

    The explicit pipe channel avoids reliance on call_soon_threadsafe wakeups
    in embedded hosts where the loop's internal self-pipe is unreliable.
    """

    def __init__(self) -> None:
        self._reader, self._writer = os.pipe()
        os.set_blocking(self._reader, False)
        os.set_blocking(self._writer, False)
        self._actions: deque[tuple[str, object | None]] = deque()
        self._actions_lock = threading.Lock()
        self._ready = threading.Event()
        self._closed = False
        self._thread = threading.Thread(
            target=self._run_loop,
            name="autoagent-runtime",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def submit(self, coroutine: Coroutine[object, object, T]) -> Future[T]:
        if self._closed:
            coroutine.close()
            raise RuntimeError("RuntimeLoop is closed.")
        submission = _Submission(coroutine, Future())

        def cancelled(future: Future[T]) -> None:
            if future.cancelled():
                self._enqueue("cancel", submission)

        submission.future.add_done_callback(cancelled)
        self._enqueue("submit", submission)
        return submission.future

    def run(self, coroutine: Coroutine[object, object, T]) -> T:
        return self.submit(coroutine).result()

    async def wait(self, future: Future[T]) -> T:
        return await await_concurrent_future(future)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._enqueue("stop", None)
        self._thread.join()
        os.close(self._reader)
        os.close(self._writer)

    def _enqueue(self, kind: str, value: object | None) -> None:
        with self._actions_lock:
            self._actions.append((kind, value))
        try:
            os.write(self._writer, b"\0")
        except (BlockingIOError, OSError):
            # An unread byte already guarantees that the loop will drain every
            # queued action.
            pass

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def drain() -> None:
            try:
                while os.read(self._reader, 4096):
                    pass
            except (BlockingIOError, OSError):
                pass
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
                    continue
                task = loop.create_task(submission.coroutine)
                submission.task = task

                def finished(
                    done: asyncio.Task[object],
                    target: _Submission[object] = submission,
                ) -> None:
                    if target.future.done():
                        return
                    if done.cancelled():
                        target.future.cancel()
                        return
                    error = done.exception()
                    if error is not None:
                        target.future.set_exception(error)
                    else:
                        target.future.set_result(done.result())

                task.add_done_callback(finished)

        loop.add_reader(self._reader, drain)

        self._ready.set()
        loop.run_forever()
        loop.remove_reader(self._reader)
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


__all__ = ["RuntimeLoop"]
