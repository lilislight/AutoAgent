from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any


InputMapping = Callable[
    ..., Mapping[str, Any] | Awaitable[Mapping[str, Any]]
]
OutputBinding = Callable[..., Any | Awaitable[Any]]
