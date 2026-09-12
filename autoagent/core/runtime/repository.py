"""Commit one semantic boundary before publishing its immutable State."""
from __future__ import annotations

import asyncio
from .clocks import unix_time_us
from collections.abc import Mapping

from .events import RuntimeEvent, RuntimeEventPayload, RecoveryApplied
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
            else index.advance(self._states[session_id], candidate, event.delta)
        )
        self._states[session_id] = candidate
        del self._pending[session_id]

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
            self._states.pop(sid, None)
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
