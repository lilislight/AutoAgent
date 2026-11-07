"""User-facing events that do not change canonical Runtime State."""

from __future__ import annotations

from .clocks import unix_time_us
from dataclasses import dataclass, field
from uuid import uuid4

from .values import DurableValue, freeze, thaw


@dataclass(frozen=True, slots=True)
class UserEvent:
    session_id: str
    invocation_id: str
    sequence: int | None
    kind: str
    payload: DurableValue
    occurrence_id: str | None = None
    occurred_at_us: int = field(default_factory=unix_time_us)
    id: str = field(default_factory=lambda: str(uuid4()))
    persist: bool = True

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
        if type(self.persist) is not bool:
            raise TypeError("User Event persist must be a boolean.")
        if not self.persist and self.sequence is not None:
            raise ValueError("Transient User Events cannot have a history sequence.")
        if self.persist and (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 1
        ):
            raise ValueError("User Event sequence must be positive.")
        if (
            not isinstance(self.occurred_at_us, int)
            or isinstance(self.occurred_at_us, bool)
            or self.occurred_at_us < 0
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
            "occurred_at_us": self.occurred_at_us,
            "persist": self.persist,
        }

    @classmethod
    def from_record(cls, value: dict[str, object]) -> "UserEvent":
        if not isinstance(value, dict):
            raise TypeError("User Event record must be a mapping.")
        return cls(
            id=_string(value, "id"),
            session_id=_string(value, "session_id"),
            invocation_id=_string(value, "invocation_id"),
            sequence=None if _required_value(value, "sequence") is None else _integer(value, "sequence"),
            kind=_string(value, "kind"),
            payload=_required_value(value, "payload"),
            occurrence_id=(
                _string(value, "occurrence_id")
                if value.get("occurrence_id") is not None
                else None
            ),
            occurred_at_us=_integer(value, "occurred_at_us"),
            persist=value.get("persist", True),
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
        occurred_at_us: int,
        persist: bool = True,
    ) -> UserEvent:
        sequence = self._sequences.get(invocation_id, 0) + 1 if persist else None
        event = UserEvent(
            session_id=session_id,
            invocation_id=invocation_id,
            sequence=sequence,
            kind=kind,
            payload=payload,  # type: ignore[arg-type]
            occurrence_id=occurrence_id,
            occurred_at_us=occurred_at_us,
            persist=persist,
        )
        self._events.setdefault(invocation_id, []).append(event)
        if persist:
            self._sequences[invocation_id] = sequence
        return event

    def events(self, invocation_id: str) -> tuple[UserEvent, ...]:
        return tuple(self._events.get(invocation_id, ()))

    def continue_sequence(self, invocation_id: str, sequence: int) -> None:
        """Seed a restored observation stream without generating historical events."""
        if not isinstance(invocation_id, str) or not invocation_id.strip():
            raise ValueError("Invocation id cannot be empty.")
        if type(sequence) is not int or sequence < 0:
            raise ValueError("User Event frontier must be nonnegative.")
        if sequence < self._sequences.get(invocation_id, 0):
            raise ValueError("User Event frontier cannot move backwards.")
        self._sequences[invocation_id] = sequence

    def drain(self, invocation_id: str) -> tuple[UserEvent, ...]:
        """Return and release User Events after their observation handoff."""

        return tuple(self._events.pop(invocation_id, ()))

    def discard(self, invocation_id: str) -> None:
        self._events.pop(invocation_id, None)
        self._sequences.pop(invocation_id, None)
