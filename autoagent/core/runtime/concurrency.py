from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from threading import Lock


class RuntimeConcurrencyController:
    """Process-local concurrency limits shared by all Invocation mailboxes.

    NodeExecutor owns no counters. It asks this runtime service for a slot keyed
    by `(workflow_id, node_id)` before executing a logical NodeExecution. This
    makes NodePolicy.max_concurrency effective across concurrently invoked
    sessions in the same AutoAgentApp process.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._active: dict[str, int] = {}

    @asynccontextmanager
    async def async_slot(
        self,
        key: str,
        limit: int | None,
    ) -> AsyncIterator[None]:
        """Acquire a process-wide slot without blocking an event loop.

        AutoAgentApp.invoke() may create separate event loops in caller threads,
        while ainvoke() may run many sessions on one loop. A threading-backed
        counter keeps the limit process-wide; cooperative polling avoids binding
        an asyncio synchronization primitive to only one of those loops.
        """

        if limit is None:
            yield
            return

        acquired = False
        try:
            while not acquired:
                with self._lock:
                    active = self._active.get(key, 0)
                    if active < limit:
                        self._active[key] = active + 1
                        acquired = True
                if not acquired:
                    await asyncio.sleep(0.001)
            yield
        finally:
            if acquired:
                with self._lock:
                    remaining = self._active[key] - 1
                    if remaining:
                        self._active[key] = remaining
                    else:
                        self._active.pop(key, None)
