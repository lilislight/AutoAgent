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
        self._messages: deque[Any] = deque()
        self._message_ready: asyncio.Event | None = None

    def track(self, task: asyncio.Task[Any], node_execution_id: UUID) -> None:
        self._running[task] = node_execution_id

    def finish_task(self, task: asyncio.Task[Any], result: Any) -> None:
        if task not in self._running:
            raise KeyError("Task does not belong to this Invocation mailbox.")
        self._running.pop(task)
        self.put_message(result)

    def put_message(self, message: Any) -> None:
        self._messages.append(message)
        if self._message_ready is not None:
            self._message_ready.set()

    def drain_messages(self) -> list[Any]:
        messages = list(self._messages)
        self._messages.clear()
        return messages

    async def wait_for_messages(self) -> list[Any]:
        """Wait until progress or a terminal result reaches this mailbox."""

        messages = self.drain_messages()
        if messages:
            return messages
        if not self._running:
            return []
        if self._message_ready is None:
            self._message_ready = asyncio.Event()
        while True:
            self._message_ready.clear()
            # A producer may have published between the first drain and clear.
            messages = self.drain_messages()
            if messages:
                return messages
            if not self._running:
                return []
            await self._message_ready.wait()
            messages = self.drain_messages()
            if messages:
                return messages

    def has_pending(self) -> bool:
        return bool(self._running) or bool(self._messages)

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
        self.drain_messages()
