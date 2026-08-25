"""Process-local Runtime State owner and configurable Event boundary.

State batches are applied immediately. Runtime Events are durable envelopes
created only when ``flush`` is called. ``append`` stages a transition and uses
the configured batch/event boundary to decide when to flush.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from uuid import uuid4

from ..errors import RuntimeTransitionError
from .capture import RuntimeEventCapture
from .checkpoint import RuntimeCheckpointBundle
from .events import RuntimeEvent, StateTransition
from .reducer import StateReducer
from .state import RuntimeState
from .store import RuntimeStateStore


_MANDATORY_FLUSH_EVENT_NAMES = frozenset(
    {
        # Write-ahead boundary before an Operator may perform external work.
        "operator_call.started",
        # Parent ownership must be durable before any Child Session can start.
        "child_invocation.planned",
        # User input and crash-attempt accounting must survive before resumed
        # or replayed work is allowed to perform another external action.
        "wait.resumed",
        "invocation.recovery_requested",
    }
)


@dataclass(frozen=True, slots=True)
class _EventGroupSnapshot:
    """Journal-owned rollback point for one not-yet-durable Event group."""

    state: RuntimeState | None
    pending: tuple[RuntimeEvent, ...]
    pending_ids: frozenset[str]


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
        if not isinstance(flush_event_names, frozenset) or not all(
            isinstance(name, str) and name for name in flush_event_names
        ):
            raise TypeError("flush_event_names must be a frozenset of event names.")
        self._reducer = reducer or StateReducer()
        self._max_batches_per_event = max_batches_per_event
        self._flush_event_names = (
            flush_event_names | _MANDATORY_FLUSH_EVENT_NAMES
        )
        self._state_store = RuntimeStateStore()
        self._capture = RuntimeEventCapture()
        # Private aliases preserve the small Journal implementation while making
        # ownership explicit: State and Event capture are separate concerns.
        self._states = self._state_store.current
        self._persisted_states = self._state_store.captured
        self._events = self._capture.events
        self._pending = self._capture.pending
        self._event_ids = self._capture.event_ids
        self._event_groups: dict[str, _EventGroupSnapshot] = {}

    def apply_transition(self, transition: StateTransition) -> RuntimeState:
        """Apply one internal transition without exposing Event construction."""

        state = self.state(transition.session_id)
        return self.append(transition.to_runtime_event(state.sequence + 1))

    def append(self, event: RuntimeEvent) -> RuntimeState:
        """Stage one transition and flush at the configured capture boundary."""

        if event.from_state_version is not None:
            return self._append_persisted(event)
        self.stage(event)
        if event.session_id not in self._event_groups and (
            event.event_name in self._flush_event_names
            or len(self._pending.get(event.session_id, ()))
            >= self._max_batches_per_event
        ):
            self.flush(event.session_id)
        return self._states[event.session_id]

    def begin_event_group(self, session_id: str) -> None:
        """Defer Event capture for one Session until an atomic boundary is complete."""

        if session_id in self._event_groups:
            raise RuntimeTransitionError(
                "EVENT_GROUP_ACTIVE",
                f"Session {session_id!r} already has an active Event group.",
            )
        pending = tuple(self._pending.get(session_id, ()))
        self._event_groups[session_id] = _EventGroupSnapshot(
            state=self._states.get(session_id),
            pending=pending,
            pending_ids=_draft_ids(pending),
        )

    def commit_event_group(self, session_id: str) -> RuntimeEvent | None:
        """Seal a complete Event group without exposing any partial prefix."""

        if session_id not in self._event_groups:
            return None
        event = self._flush(session_id)
        del self._event_groups[session_id]
        return event

    def abort_event_group(self, session_id: str) -> bool:
        """Roll an uncommitted Event group back to its exact starting State."""

        snapshot = self._event_groups.pop(session_id, None)
        if snapshot is None:
            return False

        current_ids = _draft_ids(tuple(self._pending.get(session_id, ())))
        for identifier in current_ids:
            if identifier not in snapshot.pending_ids:
                self._event_ids.pop(identifier, None)
        if snapshot.pending:
            self._pending[session_id] = list(snapshot.pending)
        else:
            self._pending.pop(session_id, None)
        if snapshot.state is None:
            self._states.pop(session_id, None)
        else:
            self._states[session_id] = snapshot.state
        return True

    def event_group_active(self, session_id: str) -> bool:
        return session_id in self._event_groups

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
        return new_state

    def flush(self, session_id: str) -> RuntimeEvent | None:
        """Seal all pending batches/logs into one contiguous Runtime Event."""

        if session_id in self._event_groups:
            raise RuntimeTransitionError(
                "EVENT_GROUP_ACTIVE",
                "An active Event group can only be sealed through its commit boundary.",
            )
        return self._flush(session_id)

    def _flush(self, session_id: str) -> RuntimeEvent | None:
        """Seal pending batches after the caller has enforced group ownership."""

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
            previous_event_id=persisted.last_event_id,
            previous_event_digest=persisted.last_event_digest,
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

    def drain_events(self, session_id: str) -> tuple[RuntimeEvent, ...]:
        """Transfer captured Events out of Core and release their history memory."""

        events = tuple(self._events.pop(session_id, ()))
        for event in events:
            self._event_ids.pop(event.id, None)
            for log in event.logs:
                self._event_ids.pop(log.id, None)
        return events

    def session_ids(self) -> tuple[str, ...]:
        """Return current in-memory Session identities, not historical Sessions."""

        return tuple(self._states)

    def discard_states(self, session_ids: tuple[str, ...]) -> None:
        """Release terminal State graphs that are no longer owned by Core."""

        if any(session_id in self._event_groups for session_id in session_ids):
            raise RuntimeTransitionError(
                "EVENT_GROUP_ACTIVE",
                "Cannot discard Runtime States with an active Event group.",
            )
        if any(self._pending.get(session_id) for session_id in session_ids):
            raise RuntimeTransitionError(
                "PENDING_EVENT_EXISTS",
                "Cannot discard Runtime States with pending Event batches.",
            )
        if any(self._events.get(session_id) for session_id in session_ids):
            raise RuntimeTransitionError(
                "UNEXPORTED_EVENT_EXISTS",
                "Cannot discard Runtime States with unacknowledged Runtime Events.",
            )
        for session_id in session_ids:
            self.drain_events(session_id)
        self._state_store.discard(session_ids)

    def install_states(self, states: Mapping[str, RuntimeState]) -> None:
        """Atomically install a checkpoint State graph without Event history."""

        conflicts = set(states).intersection(
            session_id for session_id, pending in self._pending.items() if pending
        )
        if conflicts:
            raise RuntimeTransitionError(
                "PENDING_EVENT_EXISTS",
                "Cannot install checkpoint States while local batches are pending.",
            )
        self._state_store.install(states)

    def capture_checkpoint(
        self, root_session_id: str, *, captured_at_ns: int | None = None
    ) -> RuntimeCheckpointBundle:
        """Flush and capture one immutable Root/Child State reference graph."""

        if root_session_id not in self._states:
            raise KeyError(f"Runtime Session {root_session_id!r} does not exist.")
        reachable: list[str] = []
        visited: set[str] = set()

        def visit(session_id: str) -> None:
            if session_id in visited:
                return
            visited.add(session_id)
            if session_id in self._event_groups:
                # Admission is an all-or-nothing durable boundary.  A Child's
                # parent plan remains sufficient to recreate it until the
                # complete grouped Event has been sealed and exported.
                return
            state = self._states.get(session_id)
            if state is None:
                return
            if session_id != root_session_id and state.invocation is None:
                # Child admission is intentionally multi-step.  A concurrent
                # safe boundary may be captured after SessionOpened but before
                # InvocationOpened.  The durable parent plan already contains
                # everything required to recreate that Child, while a
                # Session-only State is not itself a recoverable checkpoint
                # member.  Treat it exactly like a not-yet-opened planned
                # Child and leave its pending batches for the later complete
                # admission boundary.
                return
            invocation = state.invocation
            if invocation is not None:
                for child_session_id in (
                    unit.session_id
                    for plan in invocation.child_plans.values()
                    for unit in plan.units
                    if unit.session_id in self._states
                ):
                    visit(child_session_id)
            # Cross-Session references always point from Parent to Child.  A
            # post-order makes every Child Event durable before any Parent
            # Event that can claim its terminal phase or consume its output.
            reachable.append(session_id)

        visit(root_session_id)
        for session_id in reachable:
            self.flush(session_id)
        states = {session_id: self._states[session_id] for session_id in reachable}
        return RuntimeCheckpointBundle._from_runtime_states(
            root_session_id,
            states,
            captured_at_ns=(time.time_ns() if captured_at_ns is None else captured_at_ns),
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
        return new_state

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
        previous_event_id=draft.previous_event_id,
        previous_event_digest=draft.previous_event_digest,
    ) == draft


def _draft_ids(events: tuple[RuntimeEvent, ...]) -> frozenset[str]:
    return frozenset(
        identifier
        for event in events
        for identifier in (event.id, *(log.id for log in event.logs))
    )
