from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar


T = TypeVar("T")


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
