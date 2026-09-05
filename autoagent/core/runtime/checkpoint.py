"""Strict point-in-time checkpoints for one Root Runtime graph."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from uuid import uuid4

from .state import RuntimeState, validate_runtime_state


RUNTIME_CHECKPOINT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class RuntimeCheckpointBundle:
    """One Root Session and an immutable snapshot of its current Child closure.

    Journal capture reuses trusted immutable Reducer State references.  Public
    construction canonicalizes external values once so later mutation cannot
    alter a Checkpoint. Encoding and hashing remain lazy.
    """

    root_session_id: str
    states: Mapping[str, RuntimeState]
    captured_at_ns: int = field(default_factory=time.time_ns)
    id: str = field(default_factory=lambda: str(uuid4()))
    schema_version: int = RUNTIME_CHECKPOINT_SCHEMA_VERSION
    _digest: str | None = field(default=None, repr=False, compare=False)
    _trusted_runtime_states: bool = field(default=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _non_empty_string(self.root_session_id, "root_session_id")
        _non_empty_string(self.id, "id")
        if self.schema_version != RUNTIME_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported Runtime Checkpoint schema {self.schema_version}."
            )
        if (
            not isinstance(self.captured_at_ns, int)
            or isinstance(self.captured_at_ns, bool)
            or self.captured_at_ns < 0
        ):
            raise ValueError("Runtime Checkpoint captured_at_ns must be non-negative.")
        if not isinstance(self.states, Mapping) or not self.states:
            raise ValueError("Runtime Checkpoint states cannot be empty.")

        states = dict(self.states)
        if not all(isinstance(state, RuntimeState) for state in states.values()):
            raise TypeError("Runtime Checkpoint values must be RuntimeState instances.")
        if not self._trusted_runtime_states:
            states = {
                session_id: RuntimeState.from_record(state.to_record())
                for session_id, state in states.items()
            }
        _validate_states(self.root_session_id, states)
        object.__setattr__(self, "states", MappingProxyType(states))

        if self._digest is not None:
            _non_empty_string(self._digest, "digest")

    @property
    def digest(self) -> str:
        """Return the lazily materialized canonical content digest."""

        digest = self._digest
        if digest is None:
            digest = _checkpoint_digest(self._record_without_digest())
            object.__setattr__(self, "_digest", digest)
        return digest

    @classmethod
    def from_states(
        cls,
        root_session_id: str,
        states: Mapping[str, RuntimeState],
        *,
        captured_at_ns: int | None = None,
        id: str | None = None,
    ) -> "RuntimeCheckpointBundle":
        """Build an isolated canonical checkpoint from external Runtime States."""

        arguments: dict[str, object] = {
            "root_session_id": root_session_id,
            "states": states,
        }
        if captured_at_ns is not None:
            arguments["captured_at_ns"] = captured_at_ns
        if id is not None:
            arguments["id"] = id
        return cls(**arguments)  # type: ignore[arg-type]

    @classmethod
    def _from_runtime_states(
        cls,
        root_session_id: str,
        states: Mapping[str, RuntimeState],
        *,
        captured_at_ns: int,
    ) -> "RuntimeCheckpointBundle":
        """Capture trusted Reducer-owned immutable State references shallowly."""

        return cls(
            root_session_id=root_session_id,
            states=states,
            captured_at_ns=captured_at_ns,
            _trusted_runtime_states=True,
        )

    def state(self, session_id: str) -> RuntimeState:
        return self.states[session_id]

    def to_record(self) -> dict[str, object]:
        record = self._record_without_digest()
        digest = self._digest
        if digest is None:
            digest = _checkpoint_digest(record)
            object.__setattr__(self, "_digest", digest)
        record["digest"] = digest
        return record

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "RuntimeCheckpointBundle":
        """Decode only the exact current checkpoint schema."""

        if not isinstance(record, dict):
            raise TypeError("Runtime Checkpoint record must be a mapping.")
        expected_fields = {
            "schema_version",
            "id",
            "root_session_id",
            "captured_at_ns",
            "states",
            "digest",
        }
        if set(record) != expected_fields:
            raise TypeError(
                "Runtime Checkpoint record contains missing or unknown fields."
            )
        schema_version = _integer(record, "schema_version")
        if schema_version != RUNTIME_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported Runtime Checkpoint schema {schema_version}."
            )
        raw_states = record.get("states")
        if not isinstance(raw_states, dict):
            raise TypeError("Runtime Checkpoint states must be a mapping.")
        states: dict[str, RuntimeState] = {}
        for session_id, value in raw_states.items():
            if not isinstance(session_id, str) or not session_id:
                raise TypeError("Runtime Checkpoint State keys must be non-empty strings.")
            if not isinstance(value, dict):
                raise TypeError("Runtime Checkpoint State records must be mappings.")
            states[session_id] = RuntimeState.from_record(value)
        expected_digest = _string(record, "digest")
        checkpoint = cls(
            root_session_id=_string(record, "root_session_id"),
            states=states,
            captured_at_ns=_integer(record, "captured_at_ns"),
            id=_string(record, "id"),
            schema_version=schema_version,
            _trusted_runtime_states=True,
        )
        if checkpoint.digest != expected_digest:
            raise ValueError("Runtime Checkpoint digest does not match its content.")
        if checkpoint.to_record() != record:
            raise TypeError("Runtime Checkpoint record is not canonical.")
        return checkpoint

    def _record_without_digest(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "id": self.id,
            "root_session_id": self.root_session_id,
            "captured_at_ns": self.captured_at_ns,
            "states": {
                session_id: state.to_record()
                for session_id, state in sorted(self.states.items())
            },
        }


def _validate_states(root_session_id: str, states: dict[str, RuntimeState]) -> None:
    if root_session_id not in states:
        raise ValueError("Runtime Checkpoint Root State is missing.")

    for session_id, state in states.items():
        _non_empty_string(session_id, "State key")
        if not isinstance(state, RuntimeState):
            raise TypeError("Runtime Checkpoint values must be RuntimeState instances.")
        validate_runtime_state(state)
        session = state.session
        invocation = state.invocation
        if session is None or session.id != session_id:
            raise ValueError(
                f"Runtime Checkpoint State {session_id!r} has another Session identity."
            )
        if invocation is None:
            raise ValueError(
                f"Runtime Checkpoint State {session_id!r} has no current Invocation."
            )
        if session.latest_invocation_id != invocation.id:
            raise ValueError(
                f"Runtime Checkpoint State {session_id!r} has inconsistent Invocation identity."
            )

    visited: set[str] = set()
    visiting: set[str] = set()
    parent_counts: dict[str, int] = {session_id: 0 for session_id in states}

    def visit(session_id: str) -> None:
        if session_id in visiting:
            raise ValueError("Runtime Checkpoint Child graph contains a cycle.")
        if session_id in visited:
            return
        visiting.add(session_id)
        invocation = states[session_id].invocation
        assert invocation is not None
        for creation_id, plan in invocation.child_plans.items():
            if creation_id != plan.creation_id:
                raise ValueError("Runtime Checkpoint Child plan key is inconsistent.")
            for unit_index, unit in enumerate(plan.units):
                if unit_index != unit.unit_index:
                    raise ValueError("Runtime Checkpoint Child unit key is inconsistent.")
                child_state = states.get(unit.session_id)
                if child_state is None:
                    if unit.phase == "planned":
                        continue
                    raise ValueError(
                        f"Runtime Checkpoint Child State {unit.session_id!r} is missing."
                    )
                child_session = child_state.session
                child_invocation = child_state.invocation
                assert child_session is not None and child_invocation is not None
                if (
                    child_invocation.id != unit.invocation_id
                    or child_invocation.workflow_id != plan.workflow_id
                    or child_invocation.workflow_revision_id
                    != plan.workflow_revision_id
                ):
                    raise ValueError(
                        f"Runtime Checkpoint Child {unit.invocation_id!r} does not match its Plan."
                    )
                if unit.phase == "terminal" and not child_invocation.terminal:
                    raise ValueError(
                        f"Runtime Checkpoint Child {unit.invocation_id!r} is "
                        "marked terminal but still active."
                    )
                parent_counts[unit.session_id] += 1
                if parent_counts[unit.session_id] > 1:
                    raise ValueError(
                        f"Runtime Checkpoint Child Session {unit.session_id!r} "
                        "has multiple parents."
                    )
                visit(unit.session_id)
        visiting.remove(session_id)
        visited.add(session_id)

    visit(root_session_id)
    unreachable = set(states) - visited
    if unreachable:
        raise ValueError(
            "Runtime Checkpoint contains orphan States unreachable from its Root: "
            + ", ".join(sorted(unreachable))
        )
    if parent_counts[root_session_id] != 0:
        raise ValueError("Runtime Checkpoint Root cannot also be a Child.")


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
        raise ValueError(f"Runtime Checkpoint {label} must be a non-empty string.")


def _string(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"Runtime Checkpoint {key} must be a non-empty string.")
    return value


def _integer(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise TypeError(f"Runtime Checkpoint {key} must be a non-negative integer.")
    return value


__all__ = ["RUNTIME_CHECKPOINT_SCHEMA_VERSION", "RuntimeCheckpointBundle"]
