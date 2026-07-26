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

    queue_high_watermark_bytes: int = 64 * 1024 * 1024
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
    namespace: str
    session_id: UUID | None
    invocation_id: UUID | None
    estimated_bytes: int
    workflow_snapshot: WorkflowVersionSnapshot | None = None
    execution_snapshot: ExecutionSnapshot | None = None
    workflow_key: tuple[str, str, str, str] | None = None
    session_updated_at_ms: int | None = None
    invocation_state: str | None = None
    execution_mode: str | None = None
    invocation_updated_at_ms: int | None = None
    invocation_result: Any | None = None
    invocation_error: dict[str, Any] | None = None
    event: RuntimeEvent | None = None
    session_record: dict[str, Any] | None = None
    invocation_record: dict[str, Any] | None = None
    recovery_snapshot: ExecutionSnapshot | None = None
    force_recovery_checkpoint: bool = False
    id: UUID = field(default_factory=uuid4)


@dataclass(frozen=True, slots=True)
class PersistenceReservation:
    id: UUID
    invocation_id: UUID | None
    estimated_bytes: int


def freeze_workflow_envelope(
    *,
    namespace: str,
    snapshot: WorkflowVersionSnapshot,
) -> PersistenceEnvelope:
    frozen = snapshot.model_copy(deep=True)
    return PersistenceEnvelope(
        kind="workflow_version",
        namespace=namespace,
        session_id=None,
        invocation_id=None,
        estimated_bytes=512 + _estimate_runtime_bytes(
            frozen.model_dump(mode="python")
        ),
        workflow_snapshot=frozen,
    )


def freeze_admission_envelope(
    *,
    namespace: str,
    session_id: UUID,
    invocation_id: UUID,
    workflow_key: tuple[str, str, str, str],
    snapshot: ExecutionSnapshot,
) -> PersistenceEnvelope:
    frozen = snapshot.model_copy(deep=True)
    return PersistenceEnvelope(
        kind="admission",
        namespace=namespace,
        session_id=session_id,
        invocation_id=invocation_id,
        estimated_bytes=1024 + _estimate_runtime_bytes(frozen.state),
        workflow_key=workflow_key,
        execution_snapshot=frozen,
    )


def freeze_event_envelope(
    *,
    namespace: str,
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
) -> PersistenceEnvelope:
    """Copy mutable payload ownership once before crossing a thread boundary."""

    frozen_event = event.model_copy(deep=True)
    # ExecutionSnapshot.capture already detached this state from the live
    # aggregate. It is private to this envelope, so copying it a second time
    # would double the hottest Standard-mode checkpoint cost.
    frozen_recovery = recovery_snapshot
    estimated_bytes = 256 + _estimate_runtime_bytes(
        {
            "payload": frozen_event.payload,
            "timing": frozen_event.timing,
            "input": frozen_event.input,
            "output": frozen_event.output,
            "operations": (
                None
                if frozen_event.operations is None
                else [
                    operation.model_dump(mode="python")
                    for operation in frozen_event.operations
                ]
            ),
            "recovery_state": (
                frozen_recovery.state
                if frozen_recovery is not None
                else None
            ),
            "invocation_result": invocation_result,
            "invocation_error": invocation_error,
        }
    )
    return PersistenceEnvelope(
        kind="event",
        namespace=namespace,
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
        force_recovery_checkpoint=force_recovery_checkpoint,
    )


def freeze_invocation_state_envelope(
    *,
    namespace: str,
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
            "namespace",
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
        namespace=namespace,
        session_id=session_id,
        invocation_id=invocation_id,
        estimated_bytes=512 + _estimate_runtime_bytes(frozen_invocation),
        session_record=frozen_session,
        invocation_record=frozen_invocation,
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
        self._pending_bytes = 0
        self._durable_sequences: dict[UUID, int] = {}
        self._durable_admissions: set[UUID] = set()
        self._invocation_errors: dict[UUID, InvocationPersistenceError] = {}
        self._invocation_gaps: dict[UUID, InvocationPersistenceError] = {}
        self._health = PersistenceHealth()
        self._admission_pressure = False
        self._wake_consumer: Callable[[], None] | None = None

    def bind_consumer(self, wake_consumer: Callable[[], None]) -> None:
        with self._lock:
            if (
                self._wake_consumer is not None
                and self._wake_consumer != wake_consumer
            ):
                raise RuntimeError("PersistenceCoordinator already has a consumer.")
            self._wake_consumer = wake_consumer

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
                envelope.invocation_id is not None
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
            removed_bytes = _estimate_runtime_bytes(snapshot.state)
            reduced_bytes = max(256, previous.estimated_bytes - removed_bytes)
            queue[index] = replace(
                previous,
                estimated_bytes=reduced_bytes,
                recovery_snapshot=None,
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
                    envelope.invocation_id is not None
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

    def discard(self, envelope_id: UUID) -> None:
        with self._lock:
            self._remove_outstanding(envelope_id)

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

    def remember_admission_durable(self, invocation_id: UUID) -> None:
        with self._lock:
            self._durable_admissions.add(invocation_id)

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
                    if envelope.invocation_id == invocation_id:
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
            return self._invocation_gaps.setdefault(invocation_id, failure)

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

    def durable_sequence(self, invocation_id: UUID) -> int:
        with self._lock:
            return self._durable_sequences.get(invocation_id, 0)

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
            pending = invocation_id in self._outstanding_invocations.values()
            health = self._health
        if admission_durable and durable >= expected_sequence and not pending:
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
    try:
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
            return 128 + _estimate_runtime_bytes(
                value.model_dump(mode="python"),
                tracked,
            )
        return 256
    finally:
        tracked.remove(identity)
