from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from threading import Lock


class _SlotPool:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.semaphore = asyncio.Semaphore(limit)


class RuntimeConcurrencyController:
    """Process-local concurrency limits shared by all Invocation mailboxes.

    NodeExecutor owns no counters. It asks this runtime service for a slot keyed
    by `(workflow_id, node_id)` before executing a logical NodeExecution. This
    makes NodePolicy.max_concurrency effective across concurrently invoked
    sessions in the same AutoAgentApp process.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._pools: dict[str, _SlotPool] = {}

    @asynccontextmanager
    async def async_slot(
        self,
        key: str,
        limit: int | None,
    ) -> AsyncIterator[None]:
        """Acquire an App-runtime slot without polling its Event Loop."""

        if limit is None:
            yield
            return
        if limit < 1:
            raise ValueError("Runtime concurrency limit must be at least 1.")

        with self._lock:
            pool = self._pools.get(key)
            if pool is None:
                pool = _SlotPool(limit)
                self._pools[key] = pool
            elif pool.limit != limit:
                raise ValueError(
                    f"Runtime concurrency key {key!r} was configured with "
                    f"conflicting limits {pool.limit} and {limit}."
                )

        await pool.semaphore.acquire()
        try:
            yield
        finally:
            pool.semaphore.release()
