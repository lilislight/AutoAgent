"""Deterministic application of committed Runtime Event deltas."""
from __future__ import annotations

from dataclasses import replace
from .events import RuntimeEvent
from .operations import apply_runtime_delta
from .state import RuntimeState, validate_runtime_state
from ..errors import RuntimeTransitionError


class StateReducer:
    """Replay only mutations; never plan or invoke user computations."""

    def apply(self, state: RuntimeState, event: RuntimeEvent) -> RuntimeState:
        if event.sequence != state.sequence + 1:
            raise RuntimeTransitionError("EVENT_SEQUENCE_GAP", "Runtime Event sequence is not contiguous.")
        if state.session is not None and event.session_id != state.session.id:
            raise RuntimeTransitionError("EVENT_SESSION_MISMATCH", "Runtime Event belongs to another Session.")
        candidate = apply_runtime_delta(state, event.delta) if event.delta is not None else state
        if candidate.sequence != state.sequence or candidate.last_event_id != state.last_event_id:
            raise RuntimeTransitionError("STATE_EVENT_METADATA_MUTATION", "Delta cannot change Event metadata.")
        if candidate.session is None or candidate.session.id != event.session_id:
            raise RuntimeTransitionError("EVENT_SESSION_MISMATCH", "Delta must retain the Event Session identity.")
        if event.invocation_id is not None and (
            candidate.invocation is None or candidate.invocation.id != event.invocation_id
            or candidate.session.latest_invocation_id != event.invocation_id
        ):
            raise RuntimeTransitionError("INVOCATION_MISMATCH", "Delta and Event Invocation identities disagree.")
        return replace(candidate, sequence=event.sequence, last_event_id=event.id)

    def reduce(self, events: tuple[RuntimeEvent, ...], state: RuntimeState | None = None) -> RuntimeState:
        current = state or RuntimeState()
        for event in events:
            current = self.apply(current, event)
        validate_runtime_state(current)
        return current
