"""Node-owned streaming reduction contracts."""

from __future__ import annotations

from typing import Generic, Protocol, TypeVar


ChunkT = TypeVar("ChunkT")
ResultT = TypeVar("ResultT")


class StreamReducer(Protocol[ChunkT, ResultT]):
    """Incrementally reduce transient chunks to one durable Node result."""

    def add(self, chunk: ChunkT) -> None:
        ...

    def finish(self) -> ResultT:
        ...


def is_stream_value(value: object) -> bool:
    """Return whether a runtime value is a one-shot sync or async stream."""

    return hasattr(value, "__next__") or hasattr(value, "__anext__")
