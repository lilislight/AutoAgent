"""Persistable streaming reduction contracts."""

from __future__ import annotations

from typing import Protocol


class StreamReducer(Protocol):
    def initial(self, context: object) -> object: ...

    def add(self, context: object, state: object, chunk: object) -> object: ...

    def finish(self, context: object, state: object) -> object: ...


def is_stream_value(value: object) -> bool:
    return hasattr(value, "__next__") or hasattr(value, "__anext__")
