"""Process-local Runtime State owner and configurable Event boundary.

State batches are applied immediately. Runtime Events are durable envelopes
created only when ``flush`` is called. ``append`` stages a transition and uses
the configured batch/event boundary to decide when to flush.
"""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

from ..errors import RuntimeTransitionError
from .events import ChildInvocationLinked, RuntimeEvent
from .reducer import StateReducer
from .state import ChildInvocationState, RuntimeState


class InMemoryEventJournal:
    def __init__(
        self,
        reducer: StateReducer | None = None,
        *,
        max_batches_per_event: int = 1,
        flush_event_names: frozenset[str] = frozenset(
            {
                "operator_call.started",
                "node_occurrence.waiting",
                "invocation.waiting",
                "invocation.completed",
                "invocation.failed",
                "invocation.cancelled",
            }
        ),
    ) -> None:
        if (
            not isinstance(max_batches_per_event, int)
            or isinstance(max_batches_per_event, bool)
            or max_batches_per_event < 1
        ):
            raise ValueError("max_batches_per_event must be a positive integer.")
        self._reducer = reducer or StateReducer()
        self._max_batches_per_event = max_batches_per_event
        self._flush_event_names = flush_event_names
        self._events: dict[str, list[RuntimeEvent]] = {}
        self._states: dict[str, RuntimeState] = {}
        self._persisted_states: dict[str, RuntimeState] = {}
        self._pending: dict[str, list[RuntimeEvent]] = {}
        self._event_ids: dict[str, RuntimeEvent] = {}
        self._child_links: dict[str, ChildInvocationState] = {}
        self._children_by_parent: dict[str, list[str]] = {}

    def append(self, event: RuntimeEvent) -> RuntimeState:
        """Stage one transition and flush at the configured capture boundary."""

        if event.from_state_version is not None:
            return self._append_persisted(event)
        self.stage(event)
        if (
            event.event_name in self._flush_event_names
            or len(self._pending.get(event.session_id, ()))
            >= self._max_batches_per_event
        ):
            self.flush(event.session_id)
        return self._states[event.session_id]

    def append_many(self, events: tuple[RuntimeEvent, ...]) -> RuntimeState:
        """Atomically import one contiguous persisted Session Event prefix."""

        if not events:
            raise ValueError("Runtime Event batch cannot be empty.")
        session_id = events[0].session_id
        if any(event.session_id != session_id for event in events):
            raise RuntimeTransitionError(
                "EVENT_SESSION_MISMATCH",
                "One atomic Runtime Event import must target one Session.",
            )
        if self._pending.get(session_id):
            raise RuntimeTransitionError(
                "PENDING_EVENT_EXISTS",
                "Cannot import Runtime Events while local batches are pending.",
            )
        state = self._persisted_states.get(session_id, RuntimeState())
        sealed_events: list[RuntimeEvent] = []
        seen_ids: set[str] = set()
        for event in events:
            identifiers = {event.id}
            identifiers.update(log.id for log in event.logs)
            conflict = next(
                (
                    identifier
                    for identifier in identifiers
                    if identifier in seen_ids or identifier in self._event_ids
                ),
                None,
            )
            if conflict is not None:
                raise RuntimeTransitionError(
                    "EVENT_ID_CONFLICT", f"Runtime Event or Log id {conflict!r} was reused."
                )
            seen_ids.update(identifiers)
            sealed = (
                event
                if event.from_state_version is not None
                else self._reducer.prepare(state, event)
            )
            state = self._reducer.apply(state, sealed)
            sealed_events.append(sealed)

        # No owned collection changes before the whole prefix has reduced.
        self._events.setdefault(session_id, []).extend(sealed_events)
        self._persisted_states[session_id] = state
        self._states[session_id] = state
        for event in sealed_events:
            self._index_event_ids(event)
            self._index_child_links(event)
        return state

    def stage(self, event: RuntimeEvent) -> RuntimeState:
        """Apply one transition now while deferring its Runtime Event envelope."""

        if event.from_state_version is not None:
            raise ValueError("A persisted Runtime Event cannot be staged again.")
        if len(event.logs) != 1 or event.logs[0].id != event.id:
            raise ValueError(
                "A staged Runtime transition must contain exactly one matching Log."
            )
        existing = self._event_ids.get(event.id)
        if existing is not None:
            if _same_draft(existing, event):
                return self._states[event.session_id]
            raise RuntimeTransitionError(
                "EVENT_ID_CONFLICT", f"Event or Runtime Log id {event.id!r} was reused."
            )
        old_state = self._states.get(event.session_id, RuntimeState())
        sealed, new_state = self._reducer.plan(old_state, event)
        self._pending.setdefault(event.session_id, []).append(sealed)
        self._states[event.session_id] = new_state
        self._event_ids[event.id] = sealed
        self._index_child_links(sealed)
        return new_state

    def flush(self, session_id: str) -> RuntimeEvent | None:
        """Seal all pending batches/logs into one contiguous Runtime Event."""

        pending = self._pending.get(session_id)
        if not pending:
            return None
        persisted = self._persisted_states.get(session_id, RuntimeState())
        live = self._states[session_id]
        first = pending[0]
        last = pending[-1]
        batches = tuple(
            batch for item in pending for batch in item.operation_batches
        )
        logs = tuple(log for item in pending for log in item.logs)
        event = replace(
            last,
            id=(last.id if len(pending) == 1 else str(uuid4())),
            sequence=persisted.sequence + 1,
            causation_id=(
                first.causation_id
                if first.causation_id is not None
                else persisted.last_event_id
            ),
            from_state_version=persisted.state_version,
            to_state_version=live.state_version,
            operation_batches=batches,
            logs=logs,
        )
        if event.from_state_version != persisted.state_version:
            raise RuntimeTransitionError(
                "PENDING_STATE_DIVERGED",
                "Pending batches do not start at the persisted State version.",
            )
        committed = self._reducer.commit_event_metadata(live, event)
        # Mutation order matters: the pending buffer is cleared only after the
        # envelope has been accepted into the journal and replayed successfully.
        self._events.setdefault(session_id, []).append(event)
        self._persisted_states[session_id] = committed
        self._states[session_id] = committed
        self._index_event_ids(event)
        del self._pending[session_id]
        return event

    def pending_batches(self, session_id: str):
        """Expose immutable pending batches for hosting-layer durability checks."""

        return tuple(
            batch
            for event in self._pending.get(session_id, ())
            for batch in event.operation_batches
        )

    def state(self, session_id: str) -> RuntimeState:
        return self._states.get(session_id, RuntimeState())

    def events(self, session_id: str) -> tuple[RuntimeEvent, ...]:
        return tuple(self._events.get(session_id, ()))

    def child_link(self, child_invocation_id: str) -> ChildInvocationState | None:
        return self._child_links.get(child_invocation_id)

    def child_links(
        self, parent_invocation_id: str
    ) -> tuple[ChildInvocationState, ...]:
        return tuple(
            self._child_links[child_id]
            for child_id in self._children_by_parent.get(parent_invocation_id, ())
        )

    def _append_persisted(self, event: RuntimeEvent) -> RuntimeState:
        if self._pending.get(event.session_id):
            raise RuntimeTransitionError(
                "PENDING_EVENT_EXISTS",
                "Cannot import a persisted Runtime Event while local batches are pending.",
            )
        existing = self._event_ids.get(event.id)
        if existing is not None:
            if existing == event:
                return self._states[event.session_id]
            raise RuntimeTransitionError(
                "EVENT_ID_CONFLICT", f"Runtime Event id {event.id!r} was reused."
            )
        old_state = self._persisted_states.get(event.session_id, RuntimeState())
        new_state = self._reducer.apply(old_state, event)
        self._events.setdefault(event.session_id, []).append(event)
        self._persisted_states[event.session_id] = new_state
        self._states[event.session_id] = new_state
        self._index_event_ids(event)
        self._index_child_links(event)
        return new_state

    def _index_child_links(self, event: RuntimeEvent) -> None:
        for log in event.logs:
            payload = log.payload
            if not isinstance(payload, ChildInvocationLinked):
                continue
            link = ChildInvocationState(
                payload.parent_occurrence_id,
                payload.child_session_id,
                payload.child_invocation_id,
                payload.workflow_id,
                payload.workflow_revision_id,
            )
            if link.invocation_id in self._child_links:
                continue
            self._child_links[link.invocation_id] = link
            assert log.invocation_id is not None
            self._children_by_parent.setdefault(log.invocation_id, []).append(
                link.invocation_id
            )

    def _index_event_ids(self, event: RuntimeEvent) -> None:
        self._event_ids[event.id] = event
        for log in event.logs:
            self._event_ids[log.id] = event


def _same_draft(sealed: RuntimeEvent, draft: RuntimeEvent) -> bool:
    return replace(
        sealed,
        from_state_version=None,
        to_state_version=None,
        operation_batches=(),
        logs=draft.logs,
    ) == draft
