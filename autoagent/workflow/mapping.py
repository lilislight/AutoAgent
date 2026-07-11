from __future__ import annotations

from collections.abc import Callable
from typing import Any


InputMapping = Callable[..., Any]
OutputBinding = Callable[..., dict[str, Any]]
