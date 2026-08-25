"""Current and capture-boundary Runtime State ownership."""

from __future__ import annotations

from collections.abc import Mapping

from ..errors import RuntimeTransitionError
from .state import RuntimeState


class RuntimeStateStore:
    """Own current States separately from Event capture and history buffers."""

    def __init__(self) -> None:
        self.current: dict[str, RuntimeState] = {}
        self.captured: dict[str, RuntimeState] = {}

    def state(self, session_id: str) -> RuntimeState:
        return self.current.get(session_id, RuntimeState())

    def captured_state(self, session_id: str) -> RuntimeState:
        return self.captured.get(session_id, RuntimeState())

    def install(self, states: Mapping[str, RuntimeState]) -> None:
        """Install multiple external States after complete conflict validation."""

        candidates = _validated_states(states)
        for session_id, state in candidates.items():
            existing = self.current.get(session_id)
            if existing is not None and existing != state:
                raise RuntimeTransitionError(
                    "CHECKPOINT_SESSION_CONFLICT",
                    f"Session {session_id!r} already contains another Runtime State.",
                )
        # No mutation happens before all candidates and conflicts are accepted.
        self.current.update(candidates)
        self.captured.update(candidates)

    def discard(self, session_ids: tuple[str, ...]) -> None:
        for session_id in session_ids:
            self.current.pop(session_id, None)
            self.captured.pop(session_id, None)


def _validated_states(states: Mapping[str, RuntimeState]) -> dict[str, RuntimeState]:
    if not isinstance(states, Mapping) or not states:
        raise ValueError("Checkpoint State installation cannot be empty.")
    candidates = dict(states)
    for session_id, state in candidates.items():
        if not isinstance(session_id, str) or not session_id.strip():
            raise TypeError("Checkpoint State keys must be non-empty strings.")
        if not isinstance(state, RuntimeState):
            raise TypeError("Checkpoint State values must be RuntimeState instances.")
        session = state.session
        if session is None or session.id != session_id:
            raise ValueError(
                f"Checkpoint State {session_id!r} has another Session identity."
            )
        if RuntimeState.from_record(state.to_record()) != state:
            raise TypeError("Checkpoint installation requires canonical Runtime States.")
    return candidates


__all__ = ["RuntimeStateStore"]
