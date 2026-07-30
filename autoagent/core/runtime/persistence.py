from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field, replace
from threading import RLock
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel

from autoagent.core.compiler import WorkflowVersionSnapshot
from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.runtime.user_event import UserEvent
from autoagent.core.runtime.snapshot import ExecutionSnapshot
from autoagent.core.runtime.time import utc_timestamp_ms


PersistenceStatus = Literal[
    "memory_only",
    "pending",
    "durable",
    "degraded",
    "unserializable",
]
PersistenceBackendState = Literal["healthy", "retrying", "unavailable"]
PersistenceKind = Literal[
    "workflow_version",
    "admission",
    "invocation_state",
    "event",
    "user_event_batch",
]


class PersistenceError(RuntimeError):
    """Base class for Runtime durability failures."""


class InvocationPersistenceError(PersistenceError):
    """One Invocation can no longer produce a contiguous durable journal."""

    def __init__(
        self,
        invocation_id: UUID,
        sequence: int,
        message: str,
    ) -> None:
        super().__init__(
            f"Invocation persistence failed at sequence {sequence}: {message}"
        )
        self.invocation_id = invocation_id
        self.sequence = sequence


class UserEventPersistenceError(PersistenceError):
    """One Invocation has a gap in its durable UserEvent journal."""

    def __init__(
        self,
        invocation_id: UUID,
        sequence: int,
        message: str,
    ) -> None:
        super().__init__(
            f"UserEvent persistence failed at sequence {sequence}: {message}"
        )
        self.invocation_id = invocation_id
        self.sequence = sequence


class BackendPersistenceError(PersistenceError):
    """The configured durable sink is currently unavailable."""


class PersistenceAdmissionError(PersistenceError):
    """A new Invocation cannot fit inside the persistence memory budget."""


@dataclass(frozen=True, slots=True)
class PersistenceHealth:
    state: PersistenceBackendState = "healthy"
    last_error: str | None = None
    changed_at_ms: int | None = None
    last_success_at_ms: int | None = None


@dataclass(frozen=True, slots=True)
class PersistencePolicy:
    """Queue admission and memory bounds independent of a concrete sink."""

    queue_high_watermark_bytes: int = 256 * 1024 * 1024
    queue_low_watermark_bytes: int | None = None
    queue_hard_watermark_bytes: int | None = None
    admission_timeout_ms: float = 5_000

    def __post_init__(self) -> None:
        high = self.queue_high_watermark_bytes
        low = high // 2 if self.queue_low_watermark_bytes is None else (
            self.queue_low_watermark_bytes
        )
        hard = high * 2 if self.queue_hard_watermark_bytes is None else (
            self.queue_hard_watermark_bytes
        )
        if high < 1:
            raise ValueError("queue_high_watermark_bytes must be positive.")
        if not 0 <= low < high < hard:
            raise ValueError("Expected low < high < hard byte watermarks.")
        if self.admission_timeout_ms < 0:
            raise ValueError("admission_timeout_ms must be non-negative.")
        object.__setattr__(self, "queue_low_watermark_bytes", low)
        object.__setattr__(self, "queue_hard_watermark_bytes", hard)


@dataclass(frozen=True, slots=True)
class PersistenceEnvelope:
    """An immutable-by-ownership record handed to persistence."""

    kind: PersistenceKind
    session_id: UUID | None
    invocation_id: UUID | None
    estimated_bytes: int
    workflow_snapshot: WorkflowVersionSnapshot | None = None
    execution_snapshot: ExecutionSnapshot | None = None
    workflow_key: tuple[str, str] | None = None
    session_updated_at_ms: int | None = None
    invocation_state: str | None = None
    execution_mode: str | None = None
    invocation_updated_at_ms: int | None = None
    invocation_result: Any | None = None
    invocation_error: dict[str, Any] | None = None
    event: RuntimeEvent | None = None
    user_events: tuple[UserEvent, ...] = ()
    session_record: dict[str, Any] | None = None
    invocation_record: dict[str, Any] | None = None
    recovery_snapshot: ExecutionSnapshot | None = None
    recovery_snapshot_estimated_bytes: int = 0
    force_recovery_checkpoint: bool = False
    id: UUID = field(default_factory=uuid4)


@dataclass(frozen=True, slots=True)
class PersistenceReservation:
    id: UUID
    invocation_id: UUID | None
    estimated_bytes: int


def freeze_workflow_envelope(
    *,
    snapshot: WorkflowVersionSnapshot,
) -> PersistenceEnvelope:
    frozen = snapshot.model_copy(deep=True)
    return PersistenceEnvelope(
        kind="workflow_version",
        session_id=None,
        invocation_id=None,
        estimated_bytes=512 + _estimate_runtime_bytes(
            frozen.model_dump(mode="python")
        ),
        workflow_snapshot=frozen,
    )


def freeze_admission_envelope(
    *,
    session_id: UUID,
    invocation_id: UUID,
    workflow_key: tuple[str, str],
    snapshot: ExecutionSnapshot,
    snapshot_estimated_bytes: int | None = None,
) -> PersistenceEnvelope:
    # Admission built this immutable-by-ownership snapshot specifically for
    # RuntimeStore and persistence. Sharing it avoids copying the genesis
    # Runtime State a second time before it crosses the persistence boundary.
    frozen = snapshot
    return PersistenceEnvelope(
        kind="admission",
        session_id=session_id,
        invocation_id=invocation_id,
        estimated_bytes=1024 + (
            snapshot_estimated_bytes
            if snapshot_estimated_bytes is not None
            else _estimate_runtime_bytes(frozen.state)
        ),
        workflow_key=workflow_key,
        execution_snapshot=frozen,
    )


def freeze_event_envelope(
    *,
    session_id: UUID,
    session_updated_at_ms: int,
    invocation_id: UUID,
    invocation_state: str,
    execution_mode: str,
    invocation_updated_at_ms: int,
    invocation_result: Any | None,
    invocation_error: dict[str, Any] | None,
    event: RuntimeEvent,
    recovery_snapshot: ExecutionSnapshot | None,
    force_recovery_checkpoint: bool,
    recovery_snapshot_estimated_bytes: int = 0,
) -> PersistenceEnvelope:
    """Wrap one already-owned Event for the persistence thread."""

    # RuntimeStore owns Event payloads before they enter its journal. The same
    # immutable-by-ownership object can safely be handed to persistence.
    frozen_event = event
    frozen_recovery = recovery_snapshot
    if frozen_recovery is not None and recovery_snapshot_estimated_bytes <= 0:
        recovery_snapshot_estimated_bytes = _estimate_runtime_bytes(
            frozen_recovery.state
        )
    estimated_bytes = 256 + _estimate_runtime_bytes(
        {
            "payload": frozen_event.payload,
            "timing": frozen_event.timing,
            "input": frozen_event.input,
            "output": frozen_event.output,
            "operations": frozen_event.operations,
            "invocation_result": invocation_result,
            "invocation_error": invocation_error,
        }
    ) + recovery_snapshot_estimated_bytes
    return PersistenceEnvelope(
        kind="event",
        session_id=session_id,
        invocation_id=invocation_id,
        estimated_bytes=estimated_bytes,
        session_updated_at_ms=session_updated_at_ms,
        invocation_state=invocation_state,
        execution_mode=execution_mode,
        invocation_updated_at_ms=invocation_updated_at_ms,
        invocation_result=deepcopy(invocation_result),
        invocation_error=deepcopy(invocation_error),
        event=frozen_event,
        recovery_snapshot=frozen_recovery,
        recovery_snapshot_estimated_bytes=(
            recovery_snapshot_estimated_bytes
            if frozen_recovery is not None
            else 0
        ),
        force_recovery_checkpoint=force_recovery_checkpoint,
    )


def freeze_invocation_state_envelope(
    *,
    session_id: UUID,
    invocation_id: UUID,
    session_record: dict[str, Any],
    invocation_record: dict[str, Any],
) -> PersistenceEnvelope:
    """Freeze the small Invocation projection used by minimal mode."""

    frozen_session = {
        key: deepcopy(value)
        for key, value in session_record.items()
        if key
        in {
            "id",
            "workflow_id",
            "session_key",
            "current_invocation_id",
            "created_at_ms",
            "updated_at_ms",
        }
    }
    frozen_invocation = {
        key: deepcopy(value)
        for key, value in invocation_record.items()
        if key
        in {
            "id",
            "session_id",
            "state",
            "event_mode",
            "input",
            "result",
            "error",
            "created_at_ms",
            "updated_at_ms",
        }
    }
    return PersistenceEnvelope(
        kind="invocation_state",
        session_id=session_id,
        invocation_id=invocation_id,
        estimated_bytes=512 + _estimate_runtime_bytes(frozen_invocation),
        session_record=frozen_session,
        invocation_record=frozen_invocation,
    )


def freeze_user_event_batch_envelope(
    *,
    session_id: UUID,
    invocation_id: UUID,
    events: tuple[UserEvent, ...],
) -> PersistenceEnvelope:
    """Freeze UserEvent payload ownership before crossing the thread boundary."""

    if not events:
        raise ValueError("UserEvent persistence batch cannot be empty.")
    frozen_events = tuple(event.model_copy(deep=True) for event in events)
    return PersistenceEnvelope(
        kind="user_event_batch",
        session_id=session_id,
        invocation_id=invocation_id,
        estimated_bytes=256 + _estimate_runtime_bytes(
            [
                {
                    "type": event.type,
                    "data": event.data,
                    "node_id": event.node_id,
                    "node_execution_id": event.node_execution_id,
                    "operator_call_id": event.operator_call_id,
                }
                for event in frozen_events
            ]
        ),
        user_events=frozen_events,
    )


class PersistenceCoordinator:
    """Thread-safe producer/consumer boundary shared by every durable sink."""

    def __init__(self, policy: PersistencePolicy) -> None:
        self.policy = policy
        self._lock = RLock()
        self._incoming: dict[str, deque[PersistenceEnvelope]] = {}
        self._ready_sessions: deque[str] = deque()
        self._ready_set: set[str] = set()
        self._reservations: dict[UUID, PersistenceReservation] = {}
        self._outstanding_bytes: dict[UUID, int] = {}
        self._outstanding_invocations: dict[UUID, UUID | None] = {}
        self._outstanding_kinds: dict[UUID, PersistenceKind] = {}
        self._pending_bytes = 0
        self._durable_sequences: dict[UUID, int] = {}
        self._durable_admissions: set[UUID] = set()
        self._invocation_errors: dict[UUID, InvocationPersistenceError] = {}
        self._invocation_gaps: dict[UUID, InvocationPersistenceError] = {}
        self._user_event_gaps: dict[UUID, UserEventPersistenceError] = {}
        self._durable_user_event_sequences: dict[UUID, int] = {}
        self._health = PersistenceHealth()
        self._admission_pressure = False
        self._wake_consumer: Callable[[], None] | None = None
        self._status_change_listeners: set[Callable[[], None]] = set()

    def bind_consumer(self, wake_consumer: Callable[[], None]) -> None:
        with self._lock:
            if (
                self._wake_consumer is not None
                and self._wake_consumer != wake_consumer
            ):
                raise RuntimeError("PersistenceCoordinator already has a consumer.")
            self._wake_consumer = wake_consumer

    def subscribe_status_changes(
        self,
        listener: Callable[[], None],
    ) -> Callable[[], None]:
        with self._lock:
            self._status_change_listeners.add(listener)

        def unsubscribe() -> None:
            with self._lock:
                self._status_change_listeners.discard(listener)

        return unsubscribe

    def _notify_status_change(self) -> None:
        with self._lock:
            listeners = tuple(self._status_change_listeners)
        for listener in listeners:
            try:
                listener()
            except Exception:
                # Persistence status listeners are advisory UI wakeups and must
                # never affect durability or Workflow execution.
                continue

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._outstanding_bytes)

    @property
    def pending_bytes(self) -> int:
        with self._lock:
            return self._pending_bytes

    @property
    def health(self) -> PersistenceHealth:
        with self._lock:
            return self._health

    @property
    def admission_paused(self) -> bool:
        with self._lock:
            pending = self._pending_bytes
            high = self.policy.queue_high_watermark_bytes
            low = self.policy.queue_low_watermark_bytes
            assert low is not None
            if pending >= high:
                self._admission_pressure = True
            elif self._admission_pressure and pending <= low:
                self._admission_pressure = False
            return self._admission_pressure

    def invocation_error(
        self,
        invocation_id: UUID,
    ) -> InvocationPersistenceError | None:
        with self._lock:
            return self._invocation_errors.get(invocation_id)

    def invocation_gap(
        self,
        invocation_id: UUID,
    ) -> InvocationPersistenceError | None:
        with self._lock:
            return self._invocation_gaps.get(invocation_id)

    def user_event_gap(
        self,
        invocation_id: UUID,
    ) -> UserEventPersistenceError | None:
        with self._lock:
            return self._user_event_gaps.get(invocation_id)

    async def await_admission(self) -> None:
        timeout_ms = self.policy.admission_timeout_ms
        started = asyncio.get_running_loop().time()
        while True:
            if not self.admission_paused:
                return
            if timeout_ms == 0:
                raise self._admission_error()
            if (
                asyncio.get_running_loop().time() - started
                >= timeout_ms / 1_000
            ):
                raise self._admission_error(timeout_ms=timeout_ms)
            await asyncio.sleep(0.01)

    def try_reserve(
        self,
        envelope: PersistenceEnvelope,
    ) -> PersistenceReservation | None:
        with self._lock:
            if (
                envelope.kind != "user_event_batch"
                and envelope.invocation_id is not None
                and (
                    envelope.invocation_id in self._invocation_errors
                    or envelope.invocation_id in self._invocation_gaps
                )
            ):
                return None
            hard = self.policy.queue_hard_watermark_bytes
            assert hard is not None
            if self._pending_bytes + envelope.estimated_bytes > hard:
                return None
            reservation = PersistenceReservation(
                id=envelope.id,
                invocation_id=envelope.invocation_id,
                estimated_bytes=envelope.estimated_bytes,
            )
            self._reservations[reservation.id] = reservation
            self._outstanding_bytes[reservation.id] = (
                reservation.estimated_bytes
            )
            self._outstanding_invocations[reservation.id] = (
                reservation.invocation_id
            )
            self._outstanding_kinds[reservation.id] = envelope.kind
            self._pending_bytes += reservation.estimated_bytes
            return reservation

    def publish(
        self,
        reservation: PersistenceReservation,
        envelope: PersistenceEnvelope,
    ) -> None:
        with self._lock:
            known = self._reservations.pop(reservation.id, None)
            if known != reservation or envelope.id != reservation.id:
                raise RuntimeError("Unknown persistence reservation.")
            key = (
                str(envelope.session_id)
                if envelope.session_id is not None
                else "__control__"
            )
            queue = self._incoming.setdefault(key, deque())
            if (
                envelope.kind == "event"
                and envelope.recovery_snapshot is not None
                and envelope.invocation_id is not None
            ):
                self._coalesce_queued_recovery_state(
                    queue,
                    invocation_id=envelope.invocation_id,
                )
            queue.append(envelope)
            should_wake = not self._ready_sessions
            if key not in self._ready_set:
                self._ready_sessions.append(key)
                self._ready_set.add(key)
            wake = self._wake_consumer
        if wake is None:
            self.mark_unavailable(
                RuntimeError("Persistence consumer is not initialized.")
            )
            return
        if should_wake:
            try:
                wake()
            except Exception as exc:
                # A stopped sink is a durability failure, not an
                # execution-state rollback.
                self.mark_unavailable(exc)
        self._notify_status_change()

    def _coalesce_queued_recovery_state(
        self,
        queue: deque[PersistenceEnvelope],
        *,
        invocation_id: UUID,
    ) -> None:
        """Keep only the newest queued RecoveryState for one Invocation."""

        for index in range(len(queue) - 1, -1, -1):
            previous = queue[index]
            snapshot = previous.recovery_snapshot
            if (
                previous.invocation_id != invocation_id
                or snapshot is None
            ):
                continue
            removed_bytes = previous.recovery_snapshot_estimated_bytes
            if removed_bytes <= 0:
                removed_bytes = _estimate_runtime_bytes(snapshot.state)
            reduced_bytes = max(256, previous.estimated_bytes - removed_bytes)
            queue[index] = replace(
                previous,
                estimated_bytes=reduced_bytes,
                recovery_snapshot=None,
                recovery_snapshot_estimated_bytes=0,
            )
            tracked = self._outstanding_bytes.get(previous.id)
            if tracked is not None:
                self._outstanding_bytes[previous.id] = reduced_bytes
                self._pending_bytes += reduced_bytes - tracked
            return

    def cancel(self, reservation: PersistenceReservation) -> None:
        with self._lock:
            self._reservations.pop(reservation.id, None)
            self._remove_outstanding(reservation.id)
        self._notify_status_change()

    def take(self, limit: int) -> tuple[PersistenceEnvelope, ...]:
        values: list[PersistenceEnvelope] = []
        with self._lock:
            while self._ready_sessions and len(values) < limit:
                session_id = self._ready_sessions.popleft()
                self._ready_set.discard(session_id)
                queue = self._incoming[session_id]
                envelope = queue.popleft()
                if queue:
                    self._ready_sessions.append(session_id)
                    self._ready_set.add(session_id)
                else:
                    del self._incoming[session_id]
                if (
                    envelope.kind != "user_event_batch"
                    and envelope.invocation_id is not None
                    and envelope.invocation_id in self._invocation_errors
                ):
                    self._remove_outstanding(envelope.id)
                    continue
                values.append(envelope)
        return tuple(values)

    def adjust_size(self, envelope_id: UUID, exact_bytes: int) -> None:
        with self._lock:
            previous = self._outstanding_bytes.get(envelope_id)
            if previous is not None:
                self._outstanding_bytes[envelope_id] = exact_bytes
                self._pending_bytes += exact_bytes - previous
        self._notify_status_change()

    def discard(self, envelope_id: UUID) -> None:
        with self._lock:
            self._remove_outstanding(envelope_id)
        self._notify_status_change()

    def mark_durable(
        self,
        envelope_id: UUID,
        invocation_id: UUID,
        sequence: int,
    ) -> None:
        with self._lock:
            self._remove_outstanding(envelope_id)
            self._durable_sequences[invocation_id] = max(
                self._durable_sequences.get(invocation_id, 0),
                sequence,
            )
        self._notify_status_change()

    def mark_user_events_durable(
        self,
        envelope_id: UUID,
        invocation_id: UUID,
        sequence: int,
    ) -> None:
        with self._lock:
            self._remove_outstanding(envelope_id)
            self._durable_user_event_sequences[invocation_id] = max(
                self._durable_user_event_sequences.get(invocation_id, 0),
                sequence,
            )
        self._notify_status_change()

    def remember_user_events_durable(
        self,
        invocation_id: UUID,
        sequence: int,
    ) -> None:
        with self._lock:
            self._durable_user_event_sequences[invocation_id] = max(
                self._durable_user_event_sequences.get(invocation_id, 0),
                sequence,
            )

    def remember_durable(self, invocation_id: UUID, sequence: int) -> None:
        with self._lock:
            self._durable_sequences[invocation_id] = max(
                self._durable_sequences.get(invocation_id, 0),
                sequence,
            )

    def mark_admission_durable(
        self,
        envelope_id: UUID,
        invocation_id: UUID,
    ) -> None:
        with self._lock:
            self._remove_outstanding(envelope_id)
            self._durable_admissions.add(invocation_id)
        self._notify_status_change()

    def remember_admission_durable(self, invocation_id: UUID) -> None:
        with self._lock:
            self._durable_admissions.add(invocation_id)

    def release_invocation_tracking(self, invocation_id: UUID) -> None:
        """Drop bounded process-local durability metadata after cache eviction."""

        with self._lock:
            if invocation_id in self._outstanding_invocations.values():
                raise RuntimeError(
                    "Cannot release persistence tracking with outstanding records."
                )
            self._durable_sequences.pop(invocation_id, None)
            self._durable_user_event_sequences.pop(invocation_id, None)
            self._durable_admissions.discard(invocation_id)
            self._invocation_errors.pop(invocation_id, None)
            self._invocation_gaps.pop(invocation_id, None)
            self._user_event_gaps.pop(invocation_id, None)

    def fail_invocation(
        self,
        invocation_id: UUID,
        sequence: int,
        error: BaseException,
    ) -> InvocationPersistenceError:
        failure = InvocationPersistenceError(
            invocation_id,
            sequence,
            str(error),
        )
        with self._lock:
            existing = self._invocation_errors.setdefault(
                invocation_id,
                failure,
            )
            retained: dict[str, deque[PersistenceEnvelope]] = {}
            for session_id, queue in self._incoming.items():
                kept: deque[PersistenceEnvelope] = deque()
                for envelope in queue:
                    if (
                        envelope.invocation_id == invocation_id
                        and envelope.kind != "user_event_batch"
                    ):
                        self._remove_outstanding(envelope.id)
                    else:
                        kept.append(envelope)
                if kept:
                    retained[session_id] = kept
            ready_order = tuple(self._ready_sessions)
            self._incoming = retained
            self._ready_sessions = deque(
                session_id
                for session_id in ready_order
                if session_id in retained
            )
            self._ready_set = set(self._ready_sessions)
        self._notify_status_change()
        return existing

    def degrade_invocation(
        self,
        invocation_id: UUID,
        sequence: int,
        message: str,
    ) -> InvocationPersistenceError:
        failure = InvocationPersistenceError(
            invocation_id,
            sequence,
            message,
        )
        with self._lock:
            result = self._invocation_gaps.setdefault(invocation_id, failure)
        self._notify_status_change()
        return result

    def degrade_user_events(
        self,
        invocation_id: UUID,
        sequence: int,
        message: str,
    ) -> UserEventPersistenceError:
        failure = UserEventPersistenceError(
            invocation_id,
            sequence,
            message,
        )
        with self._lock:
            result = self._user_event_gaps.setdefault(invocation_id, failure)
        self._notify_status_change()
        return result

    def mark_retrying(self, error: BaseException) -> None:
        self._set_health("retrying", error)

    def mark_healthy(self) -> None:
        now = utc_timestamp_ms()
        with self._lock:
            self._health = PersistenceHealth(
                state="healthy",
                changed_at_ms=now,
                last_success_at_ms=now,
            )
        self._notify_status_change()

    def mark_unavailable(self, error: BaseException) -> BackendPersistenceError:
        failure = BackendPersistenceError(
            f"Runtime persistence backend is unavailable: {error}"
        )
        self._set_health("unavailable", failure)
        return failure

    def _set_health(
        self,
        state: PersistenceBackendState,
        error: BaseException,
    ) -> None:
        with self._lock:
            self._health = PersistenceHealth(
                state=state,
                last_error=str(error),
                changed_at_ms=utc_timestamp_ms(),
                last_success_at_ms=self._health.last_success_at_ms,
            )
        self._notify_status_change()

    def _admission_error(
        self,
        *,
        timeout_ms: float | None = None,
    ) -> PersistenceAdmissionError:
        health = self.health
        wait = (
            ""
            if timeout_ms is None
            else f" after waiting {timeout_ms:.0f} ms"
        )
        reason = (
            f"; database persistence is {health.state}: {health.last_error}"
            if health.state != "healthy"
            else ""
        )
        return PersistenceAdmissionError(
            "Cannot submit a new Invocation because the persistence backlog "
            f"is above its admission limit{wait}{reason}."
        )

    def _remove_outstanding(self, envelope_id: UUID) -> None:
        size = self._outstanding_bytes.pop(envelope_id, None)
        self._outstanding_invocations.pop(envelope_id, None)
        self._outstanding_kinds.pop(envelope_id, None)
        if size is not None:
            self._pending_bytes -= size

    async def flush(self) -> None:
        while self.pending_count:
            health = self.health
            if health.state == "unavailable":
                raise BackendPersistenceError(
                    health.last_error or "Persistence backend is unavailable."
                )
            await asyncio.sleep(0.001)
        with self._lock:
            invocation_error = next(
                iter(self._invocation_errors.values()),
                None,
            )
        if invocation_error is not None:
            raise invocation_error
        with self._lock:
            invocation_gap = next(
                iter(self._invocation_gaps.values()),
                None,
            )
        if invocation_gap is not None:
            raise invocation_gap
        with self._lock:
            user_event_gap = next(
                iter(self._user_event_gaps.values()),
                None,
            )
        if user_event_gap is not None:
            raise user_event_gap

    def durable_sequence(self, invocation_id: UUID) -> int:
        with self._lock:
            return self._durable_sequences.get(invocation_id, 0)

    def durable_user_event_sequence(self, invocation_id: UUID) -> int:
        with self._lock:
            return self._durable_user_event_sequences.get(invocation_id, 0)

    def status(
        self,
        invocation_id: UUID,
        expected_sequence: int,
    ) -> PersistenceStatus:
        with self._lock:
            if invocation_id in self._invocation_errors:
                return "unserializable"
            if invocation_id in self._invocation_gaps:
                return "degraded"
            durable = self._durable_sequences.get(invocation_id, 0)
            admission_durable = invocation_id in self._durable_admissions
            pending = any(
                candidate == invocation_id
                and self._outstanding_kinds.get(envelope_id)
                != "user_event_batch"
                for envelope_id, candidate
                in self._outstanding_invocations.items()
            )
            health = self._health
        if admission_durable and durable >= expected_sequence and not pending:
            return "durable"
        return "degraded" if health.state == "unavailable" else "pending"

    def user_event_status(
        self,
        invocation_id: UUID,
        expected_sequence: int,
    ) -> PersistenceStatus:
        with self._lock:
            if invocation_id in self._user_event_gaps:
                return "degraded"
            durable = self._durable_user_event_sequences.get(invocation_id, 0)
            pending = any(
                candidate == invocation_id
                and self._outstanding_kinds.get(envelope_id)
                == "user_event_batch"
                for envelope_id, candidate
                in self._outstanding_invocations.items()
            )
            health = self._health
        if durable >= expected_sequence and not pending:
            return "durable"
        return "degraded" if health.state == "unavailable" else "pending"


def _estimate_runtime_bytes(value: Any, seen: set[int] | None = None) -> int:
    """Cheap conservative queue accounting performed during Event freezing."""

    if value is None or isinstance(value, (bool, int, float)):
        return 32
    if isinstance(value, str):
        return 64 + len(value.encode("utf-8"))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return 64 + len(value)
    if isinstance(value, UUID):
        return 64

    tracked = seen if seen is not None else set()
    identity = id(value)
    if identity in tracked:
        return 64
    tracked.add(identity)
    if isinstance(value, dict):
        return 128 + sum(
            _estimate_runtime_bytes(key, tracked)
            + _estimate_runtime_bytes(item, tracked)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return 96 + sum(
            _estimate_runtime_bytes(item, tracked)
            for item in value
        )
    if isinstance(value, BaseModel):
        return 128 + sum(
            _estimate_runtime_bytes(getattr(value, field), tracked)
            for field in type(value).model_fields
        )
    return 256


def estimate_runtime_bytes(value: Any) -> int:
    """Estimate retained queue bytes for one already-owned Runtime value."""

    return _estimate_runtime_bytes(value)
