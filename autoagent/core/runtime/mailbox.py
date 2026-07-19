from __future__ import annotations

import asyncio
from collections import deque
from typing import Any
from uuid import UUID


class InvocationExecutionMailbox:
    """Process-local execution mailbox owned by one Invocation.

    Tasks cannot be persisted or resumed after process loss, so this object is
    intentionally excluded from Invocation records. NodeExecutor uses it only to
    associate in-flight tasks and completed results with the Invocation
    that submitted them. A restored Invocation receives a new empty mailbox.
    """

    def __init__(self) -> None:
        self._running: dict[asyncio.Task[Any], UUID] = {}
        self._completed: deque[Any] = deque()

    def track(self, task: asyncio.Task[Any], node_execution_id: UUID) -> None:
        self._running[task] = node_execution_id

    def running_tasks(self) -> tuple[asyncio.Task[Any], ...]:
        return tuple(self._running)

    def finish_task(self, task: asyncio.Task[Any], result: Any) -> None:
        if task not in self._running:
            raise KeyError("Task does not belong to this Invocation mailbox.")
        self._running.pop(task)
        self._completed.append(result)

    def node_execution_id_for(self, task: asyncio.Task[Any]) -> UUID:
        return self._running[task]

    def put_completed(self, result: Any) -> None:
        self._completed.append(result)

    def drain_completed(self) -> list[Any]:
        results = list(self._completed)
        self._completed.clear()
        return results

    def has_pending(self) -> bool:
        return bool(self._running) or bool(self._completed)

    async def abandon(self) -> None:
        """Detach all work after fail-fast/cancellation.

        Async operators receive cancellation. A synchronous handler already
        running in the thread pool cannot be force-stopped, but its Task result
        is detached and can no longer reach runtime state.
        """

        tasks = tuple(self._running)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._running.clear()
        self.drain_completed()
