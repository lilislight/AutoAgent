from __future__ import annotations

from collections.abc import AsyncIterable, Iterable
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar, runtime_checkable


ChunkT = TypeVar("ChunkT")
OutputT = TypeVar("OutputT")


@runtime_checkable
class StreamReducer(Protocol[ChunkT, OutputT]):
    """Incrementally reduce stream chunks into one final Operator output."""

    def add(self, chunk: ChunkT) -> None:
        """Apply one chunk without retaining the complete stream."""

    def finish(self) -> OutputT:
        """Return the final Operator output after the source is exhausted."""


@dataclass(frozen=True)
class StreamingResult(Generic[ChunkT, OutputT]):
    """Explicit one-shot streaming Operator result.

    NodeExecutor consumes ``source`` and applies every item to ``reducer``.
    Only ``reducer.finish()`` becomes the validated and retained Operator
    output. Chunks are transient execution data and never enter Runtime state.
    """

    source: Iterable[ChunkT] | AsyncIterable[ChunkT]
    reducer: StreamReducer[ChunkT, OutputT]

    def __post_init__(self) -> None:
        if not (
            hasattr(self.source, "__iter__")
            or hasattr(self.source, "__aiter__")
        ):
            raise TypeError(
                "StreamingResult source must be an Iterable or AsyncIterable."
            )
        if not callable(getattr(self.reducer, "add", None)):
            raise TypeError("StreamingResult reducer must define add(chunk).")
        if not callable(getattr(self.reducer, "finish", None)):
            raise TypeError("StreamingResult reducer must define finish().")


def streaming_result(
    source: Iterable[ChunkT] | AsyncIterable[ChunkT],
    *,
    reducer: StreamReducer[ChunkT, OutputT],
) -> StreamingResult[ChunkT, OutputT]:
    """Create an explicit streaming result for a custom Operator."""

    return StreamingResult(source=source, reducer=reducer)


def is_raw_stream_result(value: Any) -> bool:
    """Return whether a value is a stream without AutoAgent final-output semantics."""

    return (
        hasattr(value, "__anext__")
        or hasattr(value, "__next__")
        or (
            hasattr(value, "__aiter__")
            and not isinstance(value, StreamingResult)
        )
    )
