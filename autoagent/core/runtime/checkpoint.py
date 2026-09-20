"""Strict point-in-time checkpoints for one Runtime Session."""

from __future__ import annotations

import hashlib
import json
from .clocks import unix_time_us
from dataclasses import dataclass, field
from uuid import uuid4

from .state import RuntimeState, validate_runtime_state


SESSION_CHECKPOINT_SCHEMA_VERSION = 4


@dataclass(frozen=True, slots=True)
class SessionCheckpoint:
    """An immutable Session snapshot, including an incomplete Child admission."""

    session_id: str
    state: RuntimeState
    captured_at_us: int = field(default_factory=unix_time_us)
    id: str = field(default_factory=lambda: str(uuid4()))
    schema_version: int = SESSION_CHECKPOINT_SCHEMA_VERSION
    _digest: str | None = field(default=None, repr=False, compare=False)
    _trusted_runtime_state: bool = field(default=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _non_empty_string(self.session_id, "session_id")
        _non_empty_string(self.id, "id")
        if self.schema_version != SESSION_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported Session Checkpoint schema {self.schema_version}."
            )
        if (
            not isinstance(self.captured_at_us, int)
            or isinstance(self.captured_at_us, bool)
            or self.captured_at_us < 0
        ):
            raise ValueError("Session Checkpoint captured_at_us must be non-negative.")
        if not isinstance(self.state, RuntimeState):
            raise TypeError("Session Checkpoint state must be a RuntimeState.")
        state = self.state
        if self._trusted_runtime_state:
            _validate_state(self.session_id, state)
        else:
            state = RuntimeState.from_record(state.to_record())
            _validate_state(self.session_id, state)
        object.__setattr__(self, "state", state)
        if self._digest is not None:
            _non_empty_string(self._digest, "digest")

    @property
    def sequence(self) -> int:
        return self.state.sequence

    @property
    def digest(self) -> str:
        digest = self._digest
        if digest is None:
            digest = _checkpoint_digest(self._record_without_digest())
            object.__setattr__(self, "_digest", digest)
        return digest

    @classmethod
    def from_state(
        cls,
        state: RuntimeState,
        *,
        captured_at_us: int | None = None,
        id: str | None = None,
    ) -> "SessionCheckpoint":
        """Build an isolated canonical checkpoint from an external State."""
        if not isinstance(state, RuntimeState) or state.session is None:
            raise ValueError("Session Checkpoint State must contain a Session.")
        arguments: dict[str, object] = {
            "session_id": state.session.id,
            "state": state,
        }
        if captured_at_us is not None:
            arguments["captured_at_us"] = captured_at_us
        if id is not None:
            arguments["id"] = id
        return cls(**arguments)  # type: ignore[arg-type]

    @classmethod
    def _from_runtime_state(
        cls,
        session_id: str,
        state: RuntimeState,
        *,
        captured_at_us: int,
    ) -> "SessionCheckpoint":
        """Capture one trusted Reducer-owned immutable State reference."""
        return cls(
            session_id=session_id,
            state=state,
            captured_at_us=captured_at_us,
            _trusted_runtime_state=True,
        )

    def to_record(self) -> dict[str, object]:
        record = self._record_without_digest()
        digest = self._digest
        if digest is None:
            digest = _checkpoint_digest(record)
            object.__setattr__(self, "_digest", digest)
        record["digest"] = digest
        return record

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "SessionCheckpoint":
        """Decode only the exact current Session Checkpoint schema."""
        if not isinstance(record, dict):
            raise TypeError("Session Checkpoint record must be a mapping.")
        expected_fields = {
            "schema_version",
            "id",
            "session_id",
            "captured_at_us",
            "sequence",
            "state",
            "digest",
        }
        if set(record) != expected_fields:
            raise TypeError(
                "Session Checkpoint record contains missing or unknown fields."
            )
        schema_version = _integer(record, "schema_version")
        if schema_version != SESSION_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported Session Checkpoint schema {schema_version}."
            )
        raw_state = record.get("state")
        if not isinstance(raw_state, dict):
            raise TypeError("Session Checkpoint state must be a mapping.")
        if _integer(record, "sequence") != raw_state.get("sequence"):
            raise ValueError("Checkpoint sequence must match Runtime State.")
        expected_digest = _string(record, "digest")
        record_without_digest = {
            key: value for key, value in record.items() if key != "digest"
        }
        if _checkpoint_digest(record_without_digest) != expected_digest:
            raise ValueError("Session Checkpoint digest does not match its content.")
        return cls(
            session_id=_string(record, "session_id"),
            state=RuntimeState.from_record(raw_state),
            captured_at_us=_integer(record, "captured_at_us"),
            id=_string(record, "id"),
            schema_version=schema_version,
            _digest=expected_digest,
            _trusted_runtime_state=True,
        )

    def _record_without_digest(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "id": self.id,
            "session_id": self.session_id,
            "captured_at_us": self.captured_at_us,
            "sequence": self.sequence,
            "state": self.state.to_record(),
        }


def _validate_state(session_id: str, state: RuntimeState) -> None:
    validate_runtime_state(state)
    _validate_state_identity(session_id, state)


def _validate_state_identity(session_id: str, state: RuntimeState) -> None:
    session, invocation = state.session, state.invocation
    if session is None or session.id != session_id:
        raise ValueError("Session Checkpoint State has another Session identity.")
    if invocation is not None and session.latest_invocation_id != invocation.id:
        raise ValueError("Session Checkpoint has inconsistent Invocation identity.")


def _checkpoint_digest(record: dict[str, object]) -> str:
    encoded = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _non_empty_string(value: object, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Session Checkpoint {label} must be a non-empty string.")


def _string(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"Session Checkpoint {key} must be a non-empty string.")
    return value


def _integer(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise TypeError(f"Session Checkpoint {key} must be a non-negative integer.")
    return value


__all__ = ["SESSION_CHECKPOINT_SCHEMA_VERSION", "SessionCheckpoint"]
