"""Borrow the active Operator's synchronous executor without owning its lifetime."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar


_current = ContextVar("autoagent_sync_execution", default=None)


async def run_sync(handler, /, *args, **kwargs):
    """Run blocking work on Core workers inside an Operator, or the caller's pool.

    Core retains capacity until submitted work settles, including work whose
    awaiting Task was cancelled. Cancellation cannot stop a running Python
    thread. A scope inherited by a detached Task expires with its Operator.
    """
    scope = _current.get()
    if scope is None:
        return await asyncio.to_thread(handler, *args, **kwargs)
    if not scope.active:
        raise RuntimeError("Operator execution scope has ended.")
    if asyncio.get_running_loop() is not scope.loop:
        raise RuntimeError("Operator synchronous work must be dispatched on its owning event loop.")
    return await scope.runner(handler, args, kwargs)


class _SyncExecutionScope:
    def __init__(self, runner):
        self.runner = runner
        self.loop = asyncio.get_running_loop()
        self.active = True


@contextmanager
def _sync_execution_scope(runner):
    scope = _SyncExecutionScope(runner)
    token = _current.set(scope)
    try:
        yield
    finally:
        scope.active = False
        _current.reset(token)


__all__ = ["run_sync"]
