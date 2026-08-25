"""Process-local ownership of live Invocation tasks.

Durable task identity and parent-child links live in Runtime State. This class
contains only objects that cannot be serialized: asyncio Tasks and Events.
"""

from __future__ import annotations

import asyncio


class TaskRuntime:
    """Track, wake and cancel live Invocation control loops."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._wake_events: dict[str, asyncio.Event] = {}

    def track(
        self,
        session_id: str,
        task: asyncio.Task[None],
    ) -> None:
        previous = self._tasks.get(session_id)
        if previous is not None and not previous.done() and previous is not task:
            raise RuntimeError(f"Session {session_id!r} already has a live task.")
        self._tasks[session_id] = task

        def finished(done: asyncio.Task[None]) -> None:
            if self._tasks.get(session_id) is done:
                self._tasks.pop(session_id, None)
            _consume_task_exception(done)

        task.add_done_callback(finished)

    def task(self, session_id: str) -> asyncio.Task[None] | None:
        task = self._tasks.get(session_id)
        return task if task is not None and not task.done() else None

    def is_live(self, session_id: str) -> bool:
        return self.task(session_id) is not None

    def wake_event(self, session_id: str) -> asyncio.Event:
        return self._wake_events.setdefault(session_id, asyncio.Event())

    def wake(self, session_id: str) -> None:
        self.wake_event(session_id).set()

    def release_wake_event(self, session_id: str) -> None:
        self._wake_events.pop(session_id, None)

    def active_sessions(self) -> tuple[str, ...]:
        return tuple(
            session_id
            for session_id, task in self._tasks.items()
            if not task.done()
        )

    async def cancel_all(self) -> None:
        tasks = tuple(
            task for task in self._tasks.values() if not task.done()
        )
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._wake_events.clear()


def _consume_task_exception(task: asyncio.Task[object]) -> None:
    if not task.cancelled():
        task.exception()


__all__ = ["TaskRuntime"]
