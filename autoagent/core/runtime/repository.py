"""Commit one semantic boundary before publishing its immutable State."""
from __future__ import annotations

import asyncio
from .clocks import unix_time_us
from ._context_index import ContextRevisionIndex, planning_indexes
from collections.abc import Mapping

from .events import RuntimeEvent, RuntimeEventPayload, RecoveryApplied, NodeCompleted, ChildInvocationPhaseChanged
from .state import RuntimeState
from ._execution_index import ExecutionIndex
from .checkpoint import SessionCheckpoint
from .reducer import StateReducer
from .scheduling import SchedulerDelta
from .transitions import TransitionPlanner
from ..errors import RuntimeInfrastructureError, RuntimeTransitionError
from ..hosting.runtime_events import RuntimeEventSink


class RuntimeRepository:
    """Own a committed State cache and one unresolved append intent per Session.

    A failed or cancelled append can have reached external storage. Its exact
    Event is retained for idempotent retry; it is never replaced by a newly
    generated Event at the same sequence. Cache publication follows append ACK.
    """

    def __init__(
        self,
        *,
        planner: TransitionPlanner | None = None,
        reducer: StateReducer | None = None,
        sink: RuntimeEventSink | None = None,
    ) -> None:
        self.planner = planner or TransitionPlanner()
        self.reducer = reducer or StateReducer()
        self.sink = sink
        self._states: dict[str, RuntimeState] = {}
        self._context_indexes = {}
        self._failed_sessions = set()
        self._locks: dict[str, asyncio.Lock] = {}
        self._execution_indexes: dict[str, ExecutionIndex] = {}
        self._pending: dict[str, tuple[RuntimeEvent, RuntimeState]] = {}

    def state(self, session_id: str) -> RuntimeState:
        state = self._states.get(session_id)
        return RuntimeState() if state is None else state

    def _session_lock(self, session_id: str) -> asyncio.Lock:
        lock = self._locks.get(session_id)
        if lock is None:
            lock = self._locks[session_id] = asyncio.Lock()
        return lock

    async def commit(
        self,
        *,
        session_id: str,
        invocation_id: str | None,
        payload: RuntimeEventPayload,
        occurred_at_us: int | None = None,
        scheduler_delta: SchedulerDelta | None = None,
    ) -> RuntimeEvent:
        async with self._session_lock(session_id):
            pending = self._pending.get(session_id)
            await self._settle_pending(session_id)
            if pending is not None:
                event = pending[0]
                if event.payload == payload and event.invocation_id == invocation_id:
                    return event
            before = self.state(session_id)
            if isinstance(payload, RecoveryApplied) and before.invocation is not None:
                scheduler = before.invocation.scheduler
                payload = RecoveryApplied(
                    tuple(item.id for item in scheduler.occurrences.values() if item.status == "running"),
                    tuple(item.id for item in scheduler.operator_calls.values() if item.status == "running"),
                )
            timestamp = unix_time_us() if occurred_at_us is None else occurred_at_us
            patch = (before.invocation.scheduler.occurrences[payload.occurrence_id].execution.pending_context_patch
                     if isinstance(payload, NodeCompleted) and before.invocation is not None
                     and payload.occurrence_id in before.invocation.scheduler.occurrences else None)
            if patch is not None and (patch.session or patch.invocation):
                with planning_indexes(self._context_lookups(session_id, before)):
                    delta = self.planner.plan(
                        before, payload, occurred_at_us=timestamp,
                        session_id=session_id, invocation_id=invocation_id,
                        scheduler_delta=scheduler_delta,
                    )
            else:
                delta = self.planner.plan(
                    before, payload, occurred_at_us=timestamp,
                    session_id=session_id, invocation_id=invocation_id,
                    scheduler_delta=scheduler_delta,
                )
            event = RuntimeEvent(
                session_id, before.sequence + 1, payload, invocation_id,
                delta=delta, occurred_at_us=timestamp,
            )
            candidate = self.reducer.apply(before, event)
            self._pending[session_id] = event, candidate
            await self._settle_pending(session_id)
            return event

    async def _settle_pending(self, session_id: str) -> None:
        pending = self._pending.get(session_id)
        if pending is None:
            return
        event, candidate = pending
        try:
            # External adapters must idempotently accept a repeated Event id.
            if self.sink is not None:
                await self.sink.append(event)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            raise RuntimeInfrastructureError("Runtime Event append failed.") from error
        index = self._execution_indexes.get(session_id)
        self._execution_indexes[session_id] = (
            ExecutionIndex(candidate) if index is None
            else index.advance(self._states[session_id], candidate, event.delta,
                child_unit=(event.payload.creation_id, event.payload.unit_index)
                if type(self.planner) is TransitionPlanner and isinstance(event.payload, ChildInvocationPhaseChanged)
                else None)
        )
        before = self._states.get(session_id)
        lookups = self._context_indexes.get(session_id, {})
        for name, lookup in tuple(lookups.items()):
            owner = getattr(candidate, name)
            revisions = owner.context_path_revisions if owner is not None else None
            if revisions is lookup.revisions:
                continue
            old_owner = getattr(before, name) if before is not None else None
            if (type(self.planner) is TransitionPlanner and isinstance(event.payload, NodeCompleted)
                    and old_owner is not None and old_owner.context_path_revisions is lookup.revisions):
                occurrence = before.invocation.scheduler.occurrences[event.payload.occurrence_id]
                operations = getattr(occurrence.execution.pending_context_patch, name)
                lookups[name] = lookup.advance(revisions, operations)
            else:
                del lookups[name]
        old_status = before.invocation.status if before is not None and before.invocation is not None else None
        new_status = candidate.invocation.status if candidate.invocation is not None else None
        if old_status != new_status:
            self._update_failure(session_id, candidate)
        self._states[session_id] = candidate
        del self._pending[session_id]

    def _update_failure(self, session_id, state):
        if state.invocation is not None and state.invocation.status in {'failed', 'cancelled'}:
            self._failed_sessions.add(session_id)
        else:
            self._failed_sessions.discard(session_id)

    def has_failed_child(self, plan):
        # Failure convergence remains a scan; successful completion avoids it.
        return bool(self._failed_sessions) and any(
            unit.session_id in self._failed_sessions for unit in plan.units)

    def _context_lookups(self, session_id, state):
        lookups = self._context_indexes.setdefault(session_id, {})
        for name in ('session', 'invocation'):
            owner = getattr(state, name)
            if owner is None:
                lookups.pop(name, None)
                continue
            revisions = owner.context_path_revisions
            existing = lookups.get(name)
            if existing is None or existing.revisions is not revisions:
                lookups[name] = ContextRevisionIndex(revisions)
        return tuple(lookups.values())

    def preview_context_patch(self, session_id, occurrence_id, patch):
        state = self.state(session_id)
        if not patch.session and not patch.invocation:
            return TransitionPlanner().preview_context_patch(state, occurrence_id, patch)
        with planning_indexes(self._context_lookups(session_id, state)):
            return TransitionPlanner().preview_context_patch(state, occurrence_id, patch)

    def execution_index(self, session_id: str) -> ExecutionIndex:
        """Internal derived lookups for the currently acknowledged State."""
        return self._execution_indexes[session_id]

    async def settle(self, session_id: str) -> None:
        """Retry an unresolved append without planning any new execution."""
        async with self._session_lock(session_id):
            await self._settle_pending(session_id)

    def session_ids(self) -> tuple[str, ...]:
        return tuple(self._states)

    def discard_states(self, session_ids: tuple[str, ...]) -> None:
        if any(sid in self._pending for sid in session_ids):
            raise RuntimeTransitionError(
                "UNCOMMITTED_EVENT_EXISTS", "Cannot discard an unresolved append.")
        if any(self._locks.get(sid) is not None and self._locks[sid].locked() for sid in session_ids):
            raise RuntimeTransitionError("SESSION_COMMIT_ACTIVE", "Session commit is active.")
        for sid in session_ids:
            self._failed_sessions.discard(sid)
            self._states.pop(sid, None)
            self._context_indexes.pop(sid, None)
            self._execution_indexes.pop(sid, None)
            self._locks.pop(sid, None)

    def install_states(self, states: Mapping[str, RuntimeState]) -> None:
        """Validate and isolate an entire checkpoint graph before installation."""
        if not isinstance(states, Mapping) or not states:
            raise ValueError("Checkpoint installation requires non-empty States.")
        candidates: dict[str, RuntimeState] = {}
        for sid, state in states.items():
            if not isinstance(state, RuntimeState):
                raise TypeError("Checkpoint values must be RuntimeState.")
            state = RuntimeState.from_record(state.to_record())
            if state.session is None or state.session.id != sid:
                raise ValueError("Checkpoint Session identity mismatch.")
            if sid in self._pending or (sid in self._states and self._states[sid] != state):
                raise RuntimeTransitionError("CHECKPOINT_SESSION_CONFLICT", "Session already exists.")
            candidates[sid] = state
        indexes = {sid: ExecutionIndex(state) for sid, state in candidates.items()}
        for sid, state in candidates.items():
            self._context_indexes.pop(sid, None)
            self._update_failure(sid, state)
        self._states.update(candidates)
        self._execution_indexes.update(indexes)

    def capture_checkpoint(
        self, session_id: str, *, captured_at_us: int | None = None,
    ) -> SessionCheckpoint:
        if session_id in self._pending:
            raise RuntimeTransitionError(
                "CHECKPOINT_BOUNDARY_INCOMPLETE", "Event append has not settled.")
        return SessionCheckpoint._from_runtime_state(
            session_id, self._states[session_id],
            captured_at_us=unix_time_us() if captured_at_us is None else captured_at_us,
        )
