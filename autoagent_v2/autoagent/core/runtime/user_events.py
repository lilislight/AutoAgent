"""User-facing observations that do not change canonical Runtime State."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from uuid import uuid4

from .values import DurableValue, freeze, thaw


@dataclass(frozen=True, slots=True)
class UserEvent:
    session_id: str
    invocation_id: str
    sequence: int
    kind: str
    payload: DurableValue
    occurrence_id: str | None = None
    occurred_at_ns: int = field(default_factory=time.time_ns)
    id: str = field(default_factory=lambda: str(uuid4()))

    def __post_init__(self) -> None:
        for name, value in (
            ("id", self.id),
            ("session_id", self.session_id),
            ("invocation_id", self.invocation_id),
            ("kind", self.kind),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"User Event {name} cannot be empty.")
        if self.occurrence_id is not None and (
            not isinstance(self.occurrence_id, str)
            or not self.occurrence_id.strip()
        ):
            raise ValueError("User Event occurrence_id must be non-empty or None.")
        if (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 1
        ):
            raise ValueError("User Event sequence must be positive.")
        if (
            not isinstance(self.occurred_at_ns, int)
            or isinstance(self.occurred_at_ns, bool)
            or self.occurred_at_ns < 0
        ):
            raise ValueError("User Event time cannot be negative.")
        object.__setattr__(self, "payload", freeze(self.payload))

    def to_record(self) -> dict[str, object]:
        return {
            "id": self.id,
            "session_id": self.session_id,
            "invocation_id": self.invocation_id,
            "sequence": self.sequence,
            "kind": self.kind,
            "payload": thaw(self.payload),
            "occurrence_id": self.occurrence_id,
            "occurred_at_ns": self.occurred_at_ns,
        }

    @classmethod
    def from_record(cls, value: dict[str, object]) -> "UserEvent":
        if not isinstance(value, dict):
            raise TypeError("User Event record must be a mapping.")
        return cls(
            id=_string(value, "id"),
            session_id=_string(value, "session_id"),
            invocation_id=_string(value, "invocation_id"),
            sequence=_integer(value, "sequence"),
            kind=_string(value, "kind"),
            payload=_required_value(value, "payload"),
            occurrence_id=(
                _string(value, "occurrence_id")
                if value.get("occurrence_id") is not None
                else None
            ),
            occurred_at_ns=_integer(value, "occurred_at_ns"),
        )


def _string(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str):
        raise TypeError(f"User Event {key} must be a string.")
    return value


def _integer(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"User Event {key} must be an integer.")
    return value


def _required_value(record: dict[str, object], key: str) -> object:
    if key not in record:
        raise KeyError(f"User Event record requires {key}.")
    return record[key]


class InMemoryUserEventJournal:
    """Process-local implementation of the independent UserEvent port."""

    def __init__(self) -> None:
        self._events: dict[str, list[UserEvent]] = {}
        self._sequences: dict[str, int] = {}

    def emit(
        self,
        *,
        session_id: str,
        invocation_id: str,
        kind: str,
        payload: object,
        occurrence_id: str | None,
        occurred_at_ns: int,
    ) -> UserEvent:
        sequence = self._sequences.get(invocation_id, 0) + 1
        event = UserEvent(
            session_id=session_id,
            invocation_id=invocation_id,
            sequence=sequence,
            kind=kind,
            payload=payload,  # type: ignore[arg-type]
            occurrence_id=occurrence_id,
            occurred_at_ns=occurred_at_ns,
        )
        self._events.setdefault(invocation_id, []).append(event)
        self._sequences[invocation_id] = sequence
        return event

    def events(self, invocation_id: str) -> tuple[UserEvent, ...]:
        return tuple(self._events.get(invocation_id, ()))

    def drain(self, invocation_id: str) -> tuple[UserEvent, ...]:
        """Return and release observations already handed to an SDK caller."""

        return tuple(self._events.pop(invocation_id, ()))

    def discard(self, invocation_id: str) -> None:
        self._events.pop(invocation_id, None)
        self._sequences.pop(invocation_id, None)
