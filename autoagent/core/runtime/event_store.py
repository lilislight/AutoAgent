"""Ordered Event storage independent of execution and state projection."""
from __future__ import annotations

from typing import Protocol

from .events import RuntimeEvent
from ..errors import RuntimeTransitionError


class RuntimeEventStore(Protocol):
    async def append(self, event: RuntimeEvent, *, expected_sequence: int) -> None: ...
    async def read(self, session_id: str, *, after_sequence: int = 0,
                   through_sequence: int | None = None) -> tuple[RuntimeEvent, ...]: ...


class InMemoryRuntimeEventStore:
    """Process-local ordered history; append has no suspension point."""
    def __init__(self) -> None:
        self._events: dict[str, list[RuntimeEvent]] = {}
        self._heads: dict[str, int] = {}
        self._ids: dict[str, RuntimeEvent] = {}

    async def append(self, event: RuntimeEvent, *, expected_sequence: int) -> None:
        existing = self._ids.get(event.id)
        if existing is not None:
            if existing == event:
                return
            raise RuntimeTransitionError("EVENT_ID_CONFLICT", "Event identity was reused.")
        head = self._heads.get(event.session_id, 0)
        if head != expected_sequence or event.sequence != expected_sequence + 1:
            raise RuntimeTransitionError("EVENT_SEQUENCE_CONFLICT", "Event Store head changed.")
        self._events.setdefault(event.session_id, []).append(event)
        self._heads[event.session_id] = event.sequence
        self._ids[event.id] = event

    async def read(
        self,
        session_id: str,
        *,
        after_sequence: int = 0,
        through_sequence: int | None = None,
    ) -> tuple[RuntimeEvent, ...]:
        return tuple(
            event for event in self._events.get(session_id, ())
            if event.sequence > after_sequence
            and (through_sequence is None or event.sequence <= through_sequence)
        )

    def anchor_many(self, sequences: dict[str, int]) -> None:
        """Install checkpoint heads atomically without inventing historical Events."""
        for session_id, sequence in sequences.items():
            if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
                raise ValueError("Checkpoint sequence must be non-negative.")
            head = self._heads.get(session_id)
            if head is not None and head != sequence:
                raise RuntimeTransitionError("CHECKPOINT_SESSION_CONFLICT", "Event Store has another head.")
        self._heads.update(sequences)

    def discard(self, session_id: str) -> None:
        for event in self._events.pop(session_id, ()):
            self._ids.pop(event.id, None)
        self._heads.pop(session_id, None)
