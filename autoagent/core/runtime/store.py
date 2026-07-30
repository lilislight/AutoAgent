from __future__ import annotations

import asyncio
from collections import OrderedDict
from copy import deepcopy
import logging
from threading import RLock
from typing import Any, Callable, Protocol
from uuid import UUID, uuid4

from autoagent.core.compiler import WorkflowVersionSnapshot
from autoagent.core.runtime.event import RuntimeEvent, StateOperation
from autoagent.core.runtime.user_event import UserEvent, UserEventSpec
from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.persistence import (
    PersistenceCoordinator,
    PersistenceEnvelope,
    PersistenceHealth,
    PersistencePolicy,
    freeze_admission_envelope,
    freeze_event_envelope,
    freeze_invocation_state_envelope,
    freeze_user_event_batch_envelope,
    freeze_workflow_envelope,
    estimate_runtime_bytes,
)
from autoagent.core.runtime.serialization import JsonRuntimeSerializer
from autoagent.core.runtime.retention import RuntimeRetentionPolicy
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.snapshot import (
    ExecutionSnapshot,
    apply_state_operations,
    build_recovery_state_operations,
    capture_recovery_state,
    capture_execution_state,
    compact_recovery_state,
    reduce_execution_state,
    restore_execution_state,
)
from autoagent.core.runtime.time import utc_timestamp_ms


logger = logging.getLogger(__name__)


def _standard_recovery_point(event: RuntimeEvent) -> bool:
    """Return whether Standard mode must persist a restartable state image."""

    if event.event_type == "recovery":
        return True
    if event.event_name == "wait.created":
        return True
    if event.event_name.startswith("node."):
        return event.status in {
            "completed",
            "failed",
            "cancelled",
            "interrupted",
            "skipped",
        }
    return event.event_name in {"invocation.running"}


def _updated_state_estimate(
    previous: dict[str, Any],
    previous_bytes: int,
    operations: tuple[StateOperation, ...],
) -> int:
    """Update a recovery-root estimate by scanning changed leaves only."""

    estimated = previous_bytes
    for operation in operations:
        if operation.op in {"replace", "remove"}:
            try:
                old_value: Any = previous
                for segment in operation.path:
                    old_value = old_value[segment]
            except (KeyError, IndexError, TypeError):
                old_value = None
            else:
                estimated -= estimate_runtime_bytes(old_value)
        if operation.op in {"add", "replace"}:
            estimated += estimate_runtime_bytes(operation.value)
    return max(256, estimated)


class SessionBusyError(RuntimeError):
    def __init__(self, session: Session, invocation: Invocation) -> None:
        super().__init__(
            "Session already has an active invocation: "
            f"session_id={session.id}, invocation_id={invocation.id}, "
            f"state={invocation.state}"
        )
        self.session_id = session.id
        self.invocation_id = invocation.id
        self.invocation_state = invocation.state


class DurableBackend(Protocol):
    """Optional durable sink/source attached to one ``RuntimeStore``.

    A backend never owns live execution state. It receives immutable persistence
    boundaries from RuntimeStore and may load historical data after a restart.
    """

    def bind(self, store: RuntimeStore) -> None: ...

    async def ainitialize(self) -> None: ...

    async def aclose(self) -> None: ...

    async def aflush(self) -> None: ...

    async def afind_session(
        self,
        *,
        workflow_revision_id: str,
        session_key: str,
    ) -> Session | None: ...

    async def alist_recoverable_invocation_ids(
        self,
        *,
        workflow_revision_ids: tuple[str, ...],
    ) -> tuple[UUID, ...]: ...

    async def aload_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None,
    ) -> ExecutionSnapshot | None: ...

    async def aload_trace_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None,
    ) -> ExecutionSnapshot | None: ...

    async def alist_runtime_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int,
        before_sequence: int | None,
        limit: int,
    ) -> tuple[RuntimeEvent, ...]: ...

    async def alist_trace_runtime_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int,
        before_sequence: int | None,
        limit: int,
    ) -> tuple[RuntimeEvent, ...]: ...

    async def alist_user_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int,
        limit: int,
    ) -> tuple[UserEvent, ...]: ...

    async def alatest_user_event_sequence(
        self,
        invocation_id: UUID,
    ) -> int: ...


_NON_DURABLE_USER_EVENT_TYPES = frozenset(
    {"message_delta", "reasoning_delta", "tool_call_delta"}
)


class RuntimeStore:
    """Authoritative in-memory runtime center with optional durability.

    There is only one Store model. Without a backend it is memory-only; with a
    ``DatabaseBackend`` (or a future remote backend), execution still reads and
    writes this same in-memory aggregate while persistence happens downstream.
    """

    def __init__(
        self,
        *,
        backend: DurableBackend | None = None,
        serializer: JsonRuntimeSerializer | None = None,
        retention_policy: RuntimeRetentionPolicy | None = None,
        persistence_policy: PersistencePolicy | None = None,
    ) -> None:
        self.serializer = serializer or JsonRuntimeSerializer()
        self.backend = backend
        self.retention_policy = retention_policy or RuntimeRetentionPolicy()
        self.persistence = (
            PersistenceCoordinator(persistence_policy or PersistencePolicy())
            if backend is not None
            else None
        )
        self._lock = RLock()
        self.workflow_versions: dict[
            tuple[str, str],
            WorkflowVersionSnapshot,
        ] = {}
        self.sessions: dict[UUID, Session] = {}
        self.session_keys: dict[tuple[str, str | None], UUID] = {}
        self.invocations: dict[UUID, Invocation] = {}
        self.invocation_sessions: dict[UUID, UUID] = {}
        self.runtime_events: dict[UUID, list[RuntimeEvent]] = {}
        # UserEvents form an independent UI/application journal. They are not
        # RuntimeEvents and never participate in execution replay/recovery.
        self.user_events: dict[UUID, list[UserEvent]] = {}
        self._user_event_sequences: dict[UUID, int] = {}
        self._expected_user_event_sequences: dict[UUID, int] = {}
        self._user_event_change_listeners: dict[
            UUID,
            set[Callable[[], None]],
        ] = {}
        self._runtime_change_listeners: dict[
            UUID,
            set[Callable[[], None]],
        ] = {}
        self._session_user_event_change_listeners: dict[
            UUID,
            set[Callable[[], None]],
        ] = {}
        self._session_notified_invocations: dict[UUID, UUID] = {}
        self._workflow_change_listeners: set[Callable[[], None]] = set()
        self._replay_checkpoints: dict[
            tuple[UUID, int],
            ExecutionSnapshot,
        ] = {}
        self._reduced_states: dict[UUID, dict[str, Any]] = {}
        self._reduced_state_estimated_bytes: dict[UUID, int] = {}
        self._pending_admissions: dict[UUID, Invocation] = {}
        self._durable_terminal_lru: OrderedDict[UUID, None] = OrderedDict()
        self._evicted_event_sequences: OrderedDict[UUID, int] = OrderedDict()
        if backend is not None:
            backend.bind(self)

    @property
    def pending_persistence_bytes(self) -> int:
        return self.persistence.pending_bytes if self.persistence is not None else 0

    @property
    def pending_persistence_count(self) -> int:
        return self.persistence.pending_count if self.persistence is not None else 0

    @property
    def admission_paused(self) -> bool:
        return (
            self.persistence.admission_paused
            if self.persistence is not None
            else False
        )

    async def ainitialize(self) -> None:
        if self.backend is not None:
            await self.backend.ainitialize()

    async def alist_recoverable_invocation_ids(
        self,
        *,
        workflow_revision_ids: tuple[str, ...],
    ) -> tuple[UUID, ...]:
        """List active durable Invocations eligible for startup recovery."""

        if not workflow_revision_ids:
            return ()
        revision_id_set = set(workflow_revision_ids)
        with self._lock:
            invocation_ids = [
                invocation.id
                for session in self.sessions.values()
                if session.workflow_revision_id in revision_id_set
                for invocation in [session.get_current_invocation()]
                if invocation is not None
                and invocation.event_mode != "minimal"
                and invocation.state in {"created", "running", "waiting"}
            ]
        if self.backend is not None:
            persisted = await self.backend.alist_recoverable_invocation_ids(
                workflow_revision_ids=workflow_revision_ids,
            )
            invocation_ids.extend(persisted)
        return tuple(dict.fromkeys(invocation_ids))

    async def aclose(self) -> None:
        if self.backend is not None:
            await self.backend.aclose()

    async def aflush(self) -> None:
        if self.backend is not None:
            await self.backend.aflush()
        elif self.persistence is not None:
            await self.persistence.flush()

    def save_workflow_snapshot(
        self,
        snapshot: WorkflowVersionSnapshot,
    ) -> bool:
        key = (
            snapshot.workflow_id,
            snapshot.definition_hash,
        )
        with self._lock:
            existed = key in self.workflow_versions
            self.workflow_versions[key] = snapshot
        created = not existed
        if created:
            self._notify_workflow_change()
            if self.persistence is not None:
                try:
                    envelope = freeze_workflow_envelope(
                        snapshot=snapshot,
                    )
                except Exception as exc:
                    self.persistence.mark_unavailable(exc)
                    logger.exception(
                        "Workflow metadata could not be copied for "
                        "persistence; execution remains available: "
                        "workflow_id=%s",
                        snapshot.workflow_id,
                    )
                else:
                    self._publish_persistence(envelope)
        return created

    def subscribe_workflow_changes(
        self,
        listener: Callable[[], None],
    ) -> Callable[[], None]:
        """Notify a lightweight listener when a local Workflow is registered."""

        with self._lock:
            self._workflow_change_listeners.add(listener)

        def unsubscribe() -> None:
            with self._lock:
                self._workflow_change_listeners.discard(listener)

        return unsubscribe

    def _notify_workflow_change(self) -> None:
        with self._lock:
            listeners = tuple(self._workflow_change_listeners)
        for listener in listeners:
            try:
                listener()
            except Exception:
                logger.exception("Workflow change listener failed.")

    async def asave_workflow_snapshot(
        self,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        self.save_workflow_snapshot(snapshot)

    def load_workflow_snapshot(
        self,
        *,
        workflow_id: str,
        definition_hash: str,
    ) -> WorkflowVersionSnapshot | None:
        with self._lock:
            return self.workflow_versions.get(
                (workflow_id, definition_hash)
            )

    async def aload_workflow_snapshot(self, **kwargs: Any) -> WorkflowVersionSnapshot | None:
        return self.load_workflow_snapshot(**kwargs)

    def get_or_create_session(
        self,
        *,
        workflow_id: str,
        workflow_revision_id: str,
        session_key: str | None,
    ) -> Session:
        key = (workflow_revision_id, session_key)
        with self._lock:
            session_id = self.session_keys.get(key)
            if session_id is not None:
                return self.sessions[session_id]
            session = Session(
                workflow_id=workflow_id,
                workflow_revision_id=workflow_revision_id,
                session_key=session_key,
            )
            self._cache_session(session)
            return session

    async def aget_or_create_session(
        self,
        *,
        workflow_id: str,
        workflow_revision_id: str,
        session_key: str | None,
    ) -> Session:
        if session_key is not None:
            existing = await self.afind_session(
                workflow_revision_id=workflow_revision_id,
                session_key=session_key,
            )
            if existing is not None:
                if existing.workflow_id != workflow_id:
                    raise ValueError(
                        "Workflow id does not match the requested Workflow revision."
                    )
                return existing
        return self.get_or_create_session(
            workflow_id=workflow_id,
            workflow_revision_id=workflow_revision_id,
            session_key=session_key,
        )

    def find_session(
        self,
        *,
        workflow_revision_id: str,
        session_key: str,
    ) -> Session | None:
        with self._lock:
            session_id = self.session_keys.get(
                (workflow_revision_id, session_key)
            )
            return self.sessions.get(session_id) if session_id is not None else None

    async def afind_session(
        self,
        *,
        workflow_revision_id: str,
        session_key: str,
    ) -> Session | None:
        value = self.find_session(
            workflow_revision_id=workflow_revision_id,
            session_key=session_key,
        )
        if value is not None or self.backend is None:
            return value
        value = await self.backend.afind_session(
            workflow_revision_id=workflow_revision_id,
            session_key=session_key,
        )
        if value is not None:
            with self._lock:
                self._cache_session(value)
        return value

    async def aadmit_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> Session:
        with self._lock:
            session = self.sessions.get(session_id)
            if session is None:
                raise KeyError(f"Unknown session: {session_id}")
            if invocation.workflow_revision_id != session.workflow_revision_id:
                raise ValueError(
                    "Invocation workflow revision does not match its Session."
                )
            self._ensure_session_can_admit(session)
        if self.persistence is not None:
            await self.persistence.await_admission()
        with self._lock:
            session = self.sessions.get(session_id)
            if session is None:
                raise KeyError(f"Unknown session: {session_id}")
            self._ensure_session_can_admit(session)
            admitted_at_ms = utc_timestamp_ms()
            session_record = session.to_record()
            session_record["current_invocation_id"] = str(invocation.id)
            session_record["updated_at_ms"] = admitted_at_ms
            invocation_record = invocation.to_record(session.id)
            if invocation.event_mode == "minimal":
                state = {
                    "session": {
                        key: deepcopy(session_record[key])
                        for key in (
                            "id",
                            "workflow_id",
                            "workflow_revision_id",
                            "session_key",
                            "current_invocation_id",
                            "created_at_ms",
                            "updated_at_ms",
                        )
                    },
                    "invocation": {
                        key: deepcopy(invocation_record[key])
                        for key in (
                            "id",
                            "session_id",
                            "workflow_revision_id",
                            "entry_node_id",
                            "state",
                            "execution_mode",
                            "event_mode",
                            "input",
                            "created_at_ms",
                            "updated_at_ms",
                        )
                    },
                    "node_executions": [],
                }
            else:
                raw_state = {
                    "session": session_record,
                    "invocation": invocation_record,
                    "node_executions": [],
                }
                state = (
                    compact_recovery_state(raw_state)
                    if invocation.event_mode == "standard"
                    else deepcopy(raw_state)
                )
            snapshot = ExecutionSnapshot(
                invocation_id=invocation.id,
                through_sequence=0,
                state=state,
            )
            snapshot_estimated_bytes = estimate_runtime_bytes(state)
            self._pending_admissions[session_id] = invocation
        with self._lock:
            pending = self._pending_admissions.get(session_id)
            if pending is not invocation:
                raise RuntimeError("Invocation admission reservation was lost.")
            del self._pending_admissions[session_id]
            if not any(value.id == invocation.id for value in session.invocations):
                session.invocations.append(invocation)
            session.current_invocation_id = invocation.id
            session.updated_at_ms = admitted_at_ms
            self.invocations[invocation.id] = invocation
            self.invocation_sessions[invocation.id] = session.id
            self.runtime_events[invocation.id] = []
            self.user_events[invocation.id] = []
            self._user_event_sequences[invocation.id] = 0
            self._expected_user_event_sequences[invocation.id] = 0
            if invocation.event_mode in {"standard", "full"}:
                self._reduced_states[invocation.id] = state
                self._reduced_state_estimated_bytes[invocation.id] = (
                    snapshot_estimated_bytes
                )
            if invocation.event_mode == "full":
                self._replay_checkpoints[(invocation.id, 0)] = snapshot
        if self.persistence is not None:
            try:
                envelope = freeze_admission_envelope(
                    session_id=session.id,
                    invocation_id=invocation.id,
                    workflow_key=(
                        session.workflow_id,
                        invocation.workflow_definition_hash or "",
                    ),
                    snapshot=snapshot,
                    snapshot_estimated_bytes=snapshot_estimated_bytes,
                )
            except Exception as exc:
                self.persistence.fail_invocation(invocation.id, 0, exc)
                logger.exception(
                    "Invocation genesis could not be copied for persistence; "
                    "execution remains available: invocation_id=%s",
                    invocation.id,
                )
            else:
                self._publish_persistence(envelope)
        return session

    def _ensure_session_can_admit(self, session: Session) -> None:
        pending = self._pending_admissions.get(session.id)
        if pending is not None:
            raise SessionBusyError(session, pending)
        current = session.get_current_invocation()
        if current is not None and current.state in {
            "created",
            "running",
            "waiting",
        }:
            raise SessionBusyError(session, current)

    async def aclaim_waiting_session(
        self,
        *,
        workflow_revision_id: str,
        session_key: str,
        wait_key: str,
        workflow_definition_hash: str | None = None,
    ) -> Session:
        session = await self.afind_session(
            workflow_revision_id=workflow_revision_id,
            session_key=session_key,
        )
        if session is None:
            raise KeyError(f"Unknown session: {session_key}")
        invocation = session.get_current_invocation()
        if invocation is None:
            raise ValueError("Session does not have a current Invocation.")
        if invocation.state != "waiting":
            if invocation.state in {"created", "running"}:
                raise SessionBusyError(session, invocation)
            raise ValueError("Session does not have a waiting Invocation.")
        if (
            workflow_definition_hash is not None
            and invocation.workflow_definition_hash != workflow_definition_hash
        ):
            raise ValueError("Waiting Invocation uses another Workflow definition.")
        if wait_key not in invocation.scheduler.waiting_executions:
            raise KeyError(f"Unknown wait key: {wait_key}")
        return session

    async def arecord_event(
        self,
        session: Session,
        invocation: Invocation,
        event: RuntimeEvent,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
        force_recovery_checkpoint: bool = False,
    ) -> RuntimeEvent:
        if event.invocation_id != invocation.id:
            raise ValueError("RuntimeEvent invocation does not match aggregate.")
        if invocation.event_mode == "minimal":
            raise ValueError("Minimal Invocations do not record RuntimeEvents.")
        if invocation.event_mode == "full" and event.operations is None:
            raise ValueError("Full RuntimeEvent has no StateOperations.")
        if invocation.event_mode == "standard" and event.operations is not None:
            raise ValueError("Standard RuntimeEvent cannot contain StateOperations.")
        # This is the one ownership boundary for RuntimeEvent data. Both the
        # in-memory journal and persistence share this detached object.
        owned_event = event.model_copy(deep=True)
        with self._lock:
            events = self.runtime_events.setdefault(invocation.id, [])
            expected = events[-1].sequence + 1 if events else 1
            if owned_event.sequence != expected:
                raise ValueError(
                    "Expected event sequence "
                    f"{expected}, got {owned_event.sequence}."
                )
            previous = self._reduced_states.get(invocation.id)
            if previous is None:
                raise KeyError(f"Unknown Invocation aggregate: {invocation.id}")
            previous_estimated_bytes = self._reduced_state_estimated_bytes.get(
                invocation.id
            )
            if previous_estimated_bytes is None:
                previous_estimated_bytes = estimate_runtime_bytes(previous)
            if invocation.event_mode == "full":
                assert owned_event.operations is not None
                operations = owned_event.operations
                reduced = apply_state_operations(previous, operations)
                reduced_estimated_bytes = _updated_state_estimate(
                    previous,
                    previous_estimated_bytes,
                    operations,
                )
            elif (
                force_recovery_checkpoint
                or _standard_recovery_point(owned_event)
            ):
                operations = build_recovery_state_operations(
                    previous,
                    session,
                    invocation,
                    node_execution_ids=node_execution_ids,
                )
                reduced = apply_state_operations(previous, operations)
                reduced_estimated_bytes = _updated_state_estimate(
                    previous,
                    previous_estimated_bytes,
                    operations,
                )
            else:
                reduced = previous
                reduced_estimated_bytes = previous_estimated_bytes
        with self._lock:
            events = self.runtime_events.setdefault(invocation.id, [])
            expected = events[-1].sequence + 1 if events else 1
            if owned_event.sequence != expected:
                raise RuntimeError(
                    "Invocation Event sequence changed while persistence "
                    "accepted an Event."
                )
            events.append(owned_event)
            self._reduced_states[invocation.id] = reduced
            self._reduced_state_estimated_bytes[invocation.id] = (
                reduced_estimated_bytes
            )
            self.sessions[session.id] = session
            self.invocations[invocation.id] = invocation
        if self.persistence is not None:
            try:
                recovery_snapshot = (
                    ExecutionSnapshot(
                        invocation_id=invocation.id,
                        through_sequence=owned_event.sequence,
                        state=reduced,
                    )
                    if (
                        force_recovery_checkpoint
                        or (
                            invocation.event_mode == "standard"
                            and _standard_recovery_point(owned_event)
                        )
                    )
                    else None
                )
                envelope = freeze_event_envelope(
                    session_id=session.id,
                    session_updated_at_ms=session.updated_at_ms,
                    invocation_id=invocation.id,
                    invocation_state=invocation.state,
                    execution_mode=invocation.execution_mode,
                    invocation_updated_at_ms=invocation.updated_at_ms,
                    invocation_result=invocation.result,
                    invocation_error=(
                        invocation.error.to_record()
                        if invocation.error is not None
                        else None
                    ),
                    event=owned_event,
                    recovery_snapshot=recovery_snapshot,
                    force_recovery_checkpoint=force_recovery_checkpoint,
                    recovery_snapshot_estimated_bytes=(
                        reduced_estimated_bytes
                        if recovery_snapshot is not None
                        else 0
                    ),
                )
            except Exception as exc:
                self.persistence.fail_invocation(
                    invocation.id,
                    owned_event.sequence,
                    exc,
                )
                logger.exception(
                    "Invocation Event could not be copied for persistence; "
                    "execution remains available: invocation_id=%s sequence=%s",
                    invocation.id,
                    owned_event.sequence,
                )
            else:
                self._publish_persistence(envelope)
        if self.backend is not None:
            self._persistence_advanced(invocation.id)
        self._notify_runtime_change(invocation.id)
        return owned_event

    async def apersist_invocation_state(
        self,
        session: Session,
        invocation: Invocation,
    ) -> None:
        """Publish minimal-mode Invocation state without creating an Event."""

        with self._lock:
            self.sessions[session.id] = session
            self.invocations[invocation.id] = invocation
        if self.persistence is None:
            self._notify_runtime_change(invocation.id)
            return
        try:
            envelope = freeze_invocation_state_envelope(
                session_id=session.id,
                invocation_id=invocation.id,
                session_record=session.to_record(),
                invocation_record=invocation.to_record(session.id),
            )
        except Exception as exc:
            self.persistence.fail_invocation(invocation.id, 0, exc)
            logger.exception(
                "Minimal Invocation state could not be copied for persistence; "
                "execution remains available: invocation_id=%s",
                invocation.id,
            )
        else:
            self._publish_persistence(envelope)
        self._notify_runtime_change(invocation.id)

    def _publish_persistence(self, envelope: PersistenceEnvelope) -> bool:
        persistence = self.persistence
        if persistence is None:
            return False
        if (
            envelope.invocation_id is not None
            and (
                persistence.invocation_error(envelope.invocation_id)
                is not None
                or persistence.invocation_gap(envelope.invocation_id)
                is not None
            )
        ):
            return False
        reservation = persistence.try_reserve(envelope)
        if reservation is None:
            if envelope.invocation_id is not None:
                sequence = envelope.event.sequence if envelope.event else 0
                persistence.degrade_invocation(
                    envelope.invocation_id,
                    sequence,
                    "persistence queue reached its hard memory limit",
                )
            logger.error(
                "Persistence record was not queued because the queue reached "
                "its hard memory limit; execution remains available: "
                "kind=%s invocation_id=%s pending_bytes=%s",
                envelope.kind,
                envelope.invocation_id,
                persistence.pending_bytes,
            )
            return False
        persistence.publish(reservation, envelope)
        return True

    def _restore_live_aggregate(
        self,
        session: Session,
        invocation: Invocation,
        state: dict[str, Any],
    ) -> None:
        restored_session, restored_invocation = restore_execution_state(state)
        mailbox = invocation.execution_mailbox
        invocation.__dict__.clear()
        invocation.__dict__.update(restored_invocation.__dict__)
        invocation.execution_mailbox = mailbox
        session.context = restored_session.context
        session.current_invocation_id = restored_session.current_invocation_id
        session.updated_at_ms = restored_session.updated_at_ms
        self.sessions[session.id] = session
        self.invocations[invocation.id] = invocation

    def reduced_state(self, invocation_id: UUID) -> dict[str, Any]:
        """Return the immutable-by-contract reducer baseline for event creation."""

        with self._lock:
            state = self._reduced_states.get(invocation_id)
            if state is None:
                raise KeyError(f"Unknown Invocation aggregate: {invocation_id}")
            return state

    def cache_replay_checkpoint(
        self,
        snapshot: ExecutionSnapshot,
    ) -> None:
        """Keep one process-local checkpoint created by an explicit replay."""

        with self._lock:
            self._replay_checkpoints[
                (snapshot.invocation_id, snapshot.through_sequence)
            ] = snapshot
            candidates = sorted(
                (
                    sequence
                    for candidate_id, sequence in self._replay_checkpoints
                    if candidate_id == snapshot.invocation_id
                    and sequence != 0
                ),
                reverse=True,
            )
            for sequence in candidates[
                self.retention_policy.max_replay_checkpoints_per_invocation:
            ]:
                del self._replay_checkpoints[
                    (snapshot.invocation_id, sequence)
                ]

    async def aload_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None = None,
    ) -> ExecutionSnapshot | None:
        with self._lock:
            candidates = [
                snapshot
                for (candidate_id, sequence), snapshot
                in self._replay_checkpoints.items()
                if candidate_id == invocation_id
                and (
                    at_or_before_sequence is None
                    or sequence <= at_or_before_sequence
                )
            ]
        if candidates:
            return max(candidates, key=lambda value: value.through_sequence)
        if self.backend is None:
            return None
        snapshot = await self.backend.aload_execution_snapshot(
            invocation_id,
            at_or_before_sequence=at_or_before_sequence,
        )
        if snapshot is not None:
            with self._lock:
                self._replay_checkpoints[
                    (snapshot.invocation_id, snapshot.through_sequence)
                ] = snapshot
        return snapshot

    async def aload_trace_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None = None,
    ) -> ExecutionSnapshot | None:
        """Load a JSON-view snapshot without instantiating user runtime types."""

        with self._lock:
            candidates = [
                snapshot
                for (candidate_id, sequence), snapshot
                in self._replay_checkpoints.items()
                if candidate_id == invocation_id
                and (
                    at_or_before_sequence is None
                    or sequence <= at_or_before_sequence
                )
            ]
        if candidates:
            snapshot = max(
                candidates,
                key=lambda value: value.through_sequence,
            )
            return snapshot.model_copy(
                update={
                    "state": self.serializer.json_view(
                        self.serializer.dumps_unchecked(snapshot.state)
                    )
                },
                deep=True,
            )
        if self.backend is None:
            return None
        loader = getattr(
            self.backend,
            "aload_trace_execution_snapshot",
            None,
        )
        if loader is None:
            return None
        return await loader(
            invocation_id,
            at_or_before_sequence=at_or_before_sequence,
        )

    async def arebuild_execution(
        self,
        invocation_id: UUID,
        *,
        through_sequence: int | None = None,
    ) -> tuple[Session, Invocation]:
        snapshot = await self.aload_execution_snapshot(
            invocation_id,
            at_or_before_sequence=through_sequence,
        )
        if snapshot is None:
            raise KeyError(f"No execution snapshot for Invocation: {invocation_id}")
        events = await self._load_event_range(
            invocation_id=invocation_id,
            after_sequence=snapshot.through_sequence,
            before_sequence=(
                through_sequence + 1
                if through_sequence is not None
                else None
            ),
        )
        snapshot_session, snapshot_invocation = snapshot.restore()
        if snapshot_invocation.event_mode == "standard":
            if through_sequence is not None:
                raise ValueError(
                    "Standard RuntimeEvents cannot rebuild historical state."
                )
            session, invocation = snapshot_session, snapshot_invocation
            if events:
                invocation.event_sequence = events[-1].sequence
        else:
            session, invocation = reduce_execution_state(
                snapshot,
                events,
                through_sequence=through_sequence,
            )
        if through_sequence is not None:
            self.cache_replay_checkpoint(
                ExecutionSnapshot.capture(
                    session,
                    invocation,
                    through_sequence=invocation.event_sequence,
                )
            )
        if through_sequence is None:
            all_events = events
            if (
                snapshot.through_sequence > 0
                and invocation_id not in self.runtime_events
                and self.backend is not None
            ):
                all_events = await self._load_event_range(
                    invocation_id=invocation_id,
                    after_sequence=0,
                    before_sequence=None,
                )
            latest_user_event_sequence = (
                0
                if self.backend is None
                else await self.backend.alatest_user_event_sequence(
                    invocation_id
                )
            )
            with self._lock:
                self._cache_session(session)
                self.invocations[invocation.id] = invocation
                self.invocation_sessions[invocation.id] = session.id
                self.runtime_events[invocation.id] = list(all_events)
                self._user_event_sequences[invocation.id] = (
                    latest_user_event_sequence
                )
                self._expected_user_event_sequences[invocation.id] = (
                    latest_user_event_sequence
                )
                self._evicted_event_sequences.pop(invocation.id, None)
                if invocation.event_mode == "full":
                    self._reduced_states[invocation.id] = (
                        capture_execution_state(session, invocation)
                    )
                elif invocation.event_mode == "standard":
                    self._reduced_states[invocation.id] = (
                        capture_recovery_state(session, invocation)
                    )
                reduced = self._reduced_states.get(invocation.id)
                if reduced is not None:
                    self._reduced_state_estimated_bytes[invocation.id] = (
                        estimate_runtime_bytes(reduced)
                    )
        return session, invocation

    async def alist_runtime_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
    ) -> tuple[RuntimeEvent, ...]:
        if after_sequence < 0 or limit < 1:
            raise ValueError("Invalid RuntimeEvent page.")
        with self._lock:
            known_in_memory = invocation_id in self.runtime_events
            values = [
                event
                for event in self.runtime_events.get(invocation_id, [])
                if event.sequence > after_sequence
                and (
                    before_sequence is None
                    or event.sequence < before_sequence
                )
            ]
        if known_in_memory:
            selected = values[-limit:] if before_sequence is not None else values[:limit]
            return tuple(event.model_copy(deep=True) for event in selected)
        if self.backend is None:
            return ()
        return await self.backend.alist_runtime_events(
            invocation_id=invocation_id,
            after_sequence=after_sequence,
            before_sequence=before_sequence,
            limit=limit,
        )

    async def alist_trace_runtime_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
    ) -> tuple[RuntimeEvent, ...]:
        """Read Events for observation without restoring user runtime types."""

        if after_sequence < 0 or limit < 1:
            raise ValueError("Invalid RuntimeEvent page.")
        with self._lock:
            known_in_memory = invocation_id in self.runtime_events
            values = [
                event
                for event in self.runtime_events.get(invocation_id, [])
                if event.sequence > after_sequence
                and (
                    before_sequence is None
                    or event.sequence < before_sequence
                )
            ]
        if known_in_memory:
            selected = (
                values[-limit:]
                if before_sequence is not None
                else values[:limit]
            )
            return tuple(event.model_copy(deep=True) for event in selected)
        if self.backend is None:
            return ()
        loader = getattr(
            self.backend,
            "alist_trace_runtime_events",
            self.backend.alist_runtime_events,
        )
        return await loader(
            invocation_id=invocation_id,
            after_sequence=after_sequence,
            before_sequence=before_sequence,
            limit=limit,
        )

    def record_user_event(
        self,
        *,
        invocation_id: UUID,
        spec: UserEventSpec,
    ) -> UserEvent:
        """Validate, detach, sequence, and retain one UserEvent."""

        event = self._record_user_events(
            invocation_id=invocation_id,
            specs=(spec,),
        )[0]
        return event.model_copy(deep=True)

    def _record_user_events(
        self,
        *,
        invocation_id: UUID,
        specs: tuple[UserEventSpec, ...],
    ) -> tuple[UserEvent, ...]:
        """Record one internal batch with one serialization and lock pass."""

        if not specs:
            return ()
        with self._lock:
            if invocation_id not in self.invocations:
                raise KeyError(f"Unknown Invocation: {invocation_id}")

        # Serialize before taking the write lock. Serialization detaches every
        # payload from its producer, and a failure leaves the batch unapplied so
        # WorkflowExecutor can isolate the bad spec without partial writes.
        detached_data = tuple(
            self.serializer.json_view(self.serializer.dumps(spec.data))
            for spec in specs
        )
        with self._lock:
            if invocation_id not in self.invocations:
                raise KeyError(f"Unknown Invocation: {invocation_id}")
            values = self.user_events.setdefault(invocation_id, [])
            first_sequence = self._user_event_sequences.get(
                invocation_id,
                0,
            ) + 1
            events = tuple(
                UserEvent.model_construct(
                    id=uuid4(),
                    invocation_id=invocation_id,
                    sequence=first_sequence + index,
                    schema_version=2,
                    type=spec.type,
                    data=data,
                    node_id=spec.node_id,
                    workflow_path=spec.workflow_path,
                    node_execution_id=spec.node_execution_id,
                    operator_call_id=spec.operator_call_id,
                    occurred_at_ms=spec.occurred_at_ms,
                )
                for index, (spec, data) in enumerate(
                    zip(specs, detached_data, strict=True)
                )
            )
            values.extend(events)
            self._user_event_sequences[invocation_id] = events[-1].sequence
        self._publish_user_event_persistence(invocation_id, events)
        self._notify_user_event_change(invocation_id)
        return events

    def _publish_user_event_persistence(
        self,
        invocation_id: UUID,
        events: tuple[UserEvent, ...],
    ) -> None:
        persistence = self.persistence
        if persistence is None:
            return
        durable_events = tuple(
            event
            for event in events
            if event.type not in _NON_DURABLE_USER_EVENT_TYPES
        )
        if not durable_events:
            return
        with self._lock:
            session_id = self.invocation_sessions[invocation_id]
            session = self.sessions[session_id]
        envelope = freeze_user_event_batch_envelope(
            session_id=session_id,
            invocation_id=invocation_id,
            events=durable_events,
        )
        with self._lock:
            self._expected_user_event_sequences[invocation_id] = max(
                self._expected_user_event_sequences.get(invocation_id, 0),
                durable_events[-1].sequence,
            )
        reservation = persistence.try_reserve(envelope)
        if reservation is None:
            persistence.degrade_user_events(
                invocation_id,
                durable_events[0].sequence,
                "persistence queue reached its hard memory limit",
            )
            logger.error(
                "UserEvent batch was not queued because the persistence queue "
                "reached its hard memory limit; execution and RuntimeEvent "
                "durability remain available: invocation_id=%s pending_bytes=%s",
                invocation_id,
                persistence.pending_bytes,
            )
            return
        persistence.publish(reservation, envelope)

    def subscribe_user_event_changes(
        self,
        invocation_id: UUID,
        listener: Callable[[], None],
    ) -> Callable[[], None]:
        """Notify one lightweight listener after UserEvent or terminal changes."""

        with self._lock:
            if invocation_id not in self.invocations:
                raise KeyError(f"Unknown Invocation: {invocation_id}")
            self._user_event_change_listeners.setdefault(
                invocation_id,
                set(),
            ).add(listener)

        def unsubscribe() -> None:
            with self._lock:
                listeners = self._user_event_change_listeners.get(
                    invocation_id
                )
                if listeners is None:
                    return
                listeners.discard(listener)
                if not listeners:
                    self._user_event_change_listeners.pop(
                        invocation_id,
                        None,
                    )

        return unsubscribe

    def subscribe_runtime_changes(
        self,
        invocation_id: UUID,
        listener: Callable[[], None],
    ) -> Callable[[], None]:
        """Notify after RuntimeEvent, Invocation state, or durability changes."""

        with self._lock:
            if (
                invocation_id not in self.invocations
                and invocation_id not in self._evicted_event_sequences
                and self.backend is None
            ):
                raise KeyError(f"Unknown Invocation: {invocation_id}")
            self._runtime_change_listeners.setdefault(
                invocation_id,
                set(),
            ).add(listener)

        def unsubscribe() -> None:
            with self._lock:
                listeners = self._runtime_change_listeners.get(
                    invocation_id
                )
                if listeners is None:
                    return
                listeners.discard(listener)
                if not listeners:
                    self._runtime_change_listeners.pop(
                        invocation_id,
                        None,
                    )

        return unsubscribe

    def subscribe_session_user_event_changes(
        self,
        session_id: UUID,
        listener: Callable[[], None],
    ) -> Callable[[], None]:
        """Notify when any Invocation in one Session changes UserEvents."""

        with self._lock:
            self._session_user_event_change_listeners.setdefault(
                session_id,
                set(),
            ).add(listener)

        def unsubscribe() -> None:
            with self._lock:
                listeners = self._session_user_event_change_listeners.get(
                    session_id
                )
                if listeners is None:
                    return
                listeners.discard(listener)
                if not listeners:
                    self._session_user_event_change_listeners.pop(
                        session_id,
                        None,
                    )

        return unsubscribe

    def notify_user_event_execution_settled(
        self,
        invocation_id: UUID,
    ) -> None:
        """Wake followers after all terminal UserEvents have been recorded."""

        with self._lock:
            invocation = self.invocations.get(invocation_id)
            terminal = (
                invocation is not None
                and invocation.state
                in {"completed", "failed", "cancelled", "interrupted"}
            )
        if terminal:
            self._notify_user_event_change(
                invocation_id,
                session_directory_change=True,
            )

    def _notify_user_event_change(
        self,
        invocation_id: UUID,
        *,
        session_directory_change: bool = False,
    ) -> None:
        with self._lock:
            listeners = tuple(
                self._user_event_change_listeners.get(invocation_id, ())
            )
            session_id = self.invocation_sessions.get(invocation_id)
            notify_session = (
                session_id is not None
                and (
                    session_directory_change
                    or self._session_notified_invocations.get(session_id)
                    != invocation_id
                )
            )
            if notify_session:
                self._session_notified_invocations[session_id] = invocation_id
                session_listeners = tuple(
                    self._session_user_event_change_listeners.get(
                        session_id,
                        (),
                    )
                )
            else:
                session_listeners = ()
        for listener in listeners:
            try:
                listener()
            except Exception:
                logger.exception(
                    "UserEvent change listener failed: invocation_id=%s",
                    invocation_id,
                )
        for listener in session_listeners:
            try:
                listener()
            except Exception:
                logger.exception(
                    "Session UserEvent change listener failed: "
                    "session_id=%s invocation_id=%s",
                    session_id,
                    invocation_id,
                )

    def _notify_runtime_change(self, invocation_id: UUID) -> None:
        with self._lock:
            listeners = tuple(
                self._runtime_change_listeners.get(invocation_id, ())
            )
        for listener in listeners:
            try:
                listener()
            except Exception:
                logger.exception(
                    "Runtime change listener failed: invocation_id=%s",
                    invocation_id,
                )

    def list_user_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int = 0,
        limit: int = 200,
    ) -> tuple[UserEvent, ...]:
        if after_sequence < 0 or limit < 1:
            raise ValueError("Invalid UserEvent page.")
        with self._lock:
            if (
                invocation_id not in self.invocations
                and invocation_id not in self.user_events
            ):
                raise KeyError(f"Unknown Invocation: {invocation_id}")
            values = [
                event
                for event in self.user_events.get(invocation_id, ())
                if event.sequence > after_sequence
            ][:limit]
            return tuple(event.model_copy(deep=True) for event in values)

    def latest_user_event_sequence(self, invocation_id: UUID) -> int:
        with self._lock:
            sequence = self._user_event_sequences.get(invocation_id)
            if sequence is not None:
                return sequence
            if invocation_id not in self.invocations:
                raise KeyError(f"Unknown Invocation: {invocation_id}")
            return 0

    async def alist_user_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int = 0,
        limit: int = 200,
    ) -> tuple[UserEvent, ...]:
        if after_sequence < 0 or limit < 1:
            raise ValueError("Invalid UserEvent page.")
        with self._lock:
            known_in_memory = invocation_id in self.user_events
            memory_values = [
                event
                for event in self.user_events.get(invocation_id, ())
                if event.sequence > after_sequence
            ]
            first_memory_sequence = (
                self.user_events[invocation_id][0].sequence
                if self.user_events.get(invocation_id)
                else None
            )
        needs_durable_prefix = (
            self.backend is not None
            and (
                not known_in_memory
                or (
                    first_memory_sequence is not None
                    and after_sequence < first_memory_sequence - 1
                )
            )
        )
        durable_values: tuple[UserEvent, ...] = ()
        if needs_durable_prefix:
            durable_values = await self.backend.alist_user_events(
                invocation_id=invocation_id,
                after_sequence=after_sequence,
                limit=limit,
            )
            if len(durable_values) >= limit:
                return durable_values
        selected = [
            event
            for event in memory_values
            if (
                not durable_values
                or event.sequence > durable_values[-1].sequence
            )
        ][: limit - len(durable_values)]
        if known_in_memory or durable_values:
            return (
                *durable_values,
                *(event.model_copy(deep=True) for event in selected),
            )
        if self.backend is None:
            if invocation_id not in self.invocations:
                raise KeyError(f"Unknown Invocation: {invocation_id}")
            return ()
        return ()

    async def alatest_user_event_sequence(self, invocation_id: UUID) -> int:
        with self._lock:
            sequence = self._user_event_sequences.get(invocation_id)
            known_in_memory = invocation_id in self.user_events
        if sequence is not None or known_in_memory:
            return sequence or 0
        if self.backend is None:
            if invocation_id not in self.invocations:
                raise KeyError(f"Unknown Invocation: {invocation_id}")
            return 0
        return await self.backend.alatest_user_event_sequence(invocation_id)

    def user_event_persistence_status(self, invocation_id: UUID) -> str:
        if self.backend is None:
            return "memory_only"
        with self._lock:
            expected = self._expected_user_event_sequences.get(
                invocation_id,
                0,
            )
        assert self.persistence is not None
        return self.persistence.user_event_status(invocation_id, expected)

    def persistence_status(self, invocation_id: UUID) -> str:
        if (
            invocation_id not in self.invocations
            and invocation_id not in self._evicted_event_sequences
        ):
            raise KeyError(f"Unknown Invocation: {invocation_id}")
        if self.backend is None:
            return "memory_only"
        expected = self._evicted_event_sequences.get(invocation_id)
        if expected is not None:
            assert self.persistence is not None
            return self.persistence.status(invocation_id, expected)
        invocation = self.invocations[invocation_id]
        assert self.persistence is not None
        return self.persistence.status(
            invocation_id,
            invocation.event_sequence,
        )

    def durable_sequence(self, invocation_id: UUID) -> int:
        return (
            self.persistence.durable_sequence(invocation_id)
            if self.persistence is not None
            else 0
        )

    def persistence_health(self) -> PersistenceHealth | None:
        return self.persistence.health if self.persistence is not None else None

    async def _load_event_range(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int,
        before_sequence: int | None,
        page_size: int = 10_000,
    ) -> tuple[RuntimeEvent, ...]:
        """Load an event range without a silent maximum-invocation limit."""

        values: list[RuntimeEvent] = []
        cursor = after_sequence
        while True:
            page = await self.alist_runtime_events(
                invocation_id=invocation_id,
                after_sequence=cursor,
                before_sequence=before_sequence,
                limit=page_size,
            )
            if not page:
                break
            values.extend(page)
            cursor = page[-1].sequence
            if len(page) < page_size:
                break
        return tuple(values)

    def _cache_session(self, session: Session) -> None:
        self.sessions[session.id] = session
        self.session_keys[
            (
                session.workflow_revision_id,
                session.session_key,
            )
        ] = session.id

    def _persistence_advanced(self, invocation_id: UUID) -> None:
        """Apply retention only after every emitted Event is durable."""

        with self._lock:
            invocation = self.invocations.get(invocation_id)
            if invocation is None or invocation.state not in {
                "completed",
                "failed",
                "cancelled",
                "interrupted",
            }:
                return
            events = self.runtime_events.get(invocation_id, ())
            if invocation.event_mode == "minimal":
                if self.persistence_status(invocation_id) != "durable":
                    return
            else:
                if (
                    not events
                    or events[-1].sequence < invocation.event_sequence
                ):
                    return
                if self.durable_sequence(invocation_id) < invocation.event_sequence:
                    return
            if self.user_event_persistence_status(invocation_id) != "durable":
                return
            self._notify_runtime_change(invocation_id)
            release = getattr(self.backend, "release_invocation_cache", None)
            if release is not None:
                release(invocation_id)
            mode = self.retention_policy.mode
            if mode == "retain_all":
                return
            if mode == "evict_durable_terminal":
                self._evict_invocation(invocation_id)
                return
            self._durable_terminal_lru.pop(invocation_id, None)
            self._durable_terminal_lru[invocation_id] = None
            while (
                len(self._durable_terminal_lru)
                > self.retention_policy.max_terminal_invocations
            ):
                oldest, _ = self._durable_terminal_lru.popitem(last=False)
                self._evict_invocation(oldest)

    def _evict_invocation(self, invocation_id: UUID) -> None:
        invocation = self.invocations.pop(invocation_id, None)
        if invocation is None:
            return
        self._remember_evicted_sequence(
            invocation_id,
            invocation.event_sequence,
        )
        session_id = self.invocation_sessions.pop(invocation_id, None)
        if session_id is not None:
            session = self.sessions.get(session_id)
            if session is not None:
                session.invocations = [
                    value
                    for value in session.invocations
                    if value.id != invocation_id
                ]
                if session.current_invocation_id == invocation_id:
                    session.current_invocation_id = None
                if not session.invocations:
                    self.sessions.pop(session_id, None)
                    session_key = (
                        session.workflow_revision_id,
                        session.session_key,
                    )
                    if self.session_keys.get(session_key) == session_id:
                        self.session_keys.pop(session_key, None)
                    self._session_notified_invocations.pop(
                        session_id,
                        None,
                    )
        self.runtime_events.pop(invocation_id, None)
        self.user_events.pop(invocation_id, None)
        self._user_event_sequences.pop(invocation_id, None)
        self._expected_user_event_sequences.pop(invocation_id, None)
        self._user_event_change_listeners.pop(invocation_id, None)
        self._runtime_change_listeners.pop(invocation_id, None)
        self._reduced_states.pop(invocation_id, None)
        self._reduced_state_estimated_bytes.pop(invocation_id, None)
        for key in [
            key
            for key in self._replay_checkpoints
            if key[0] == invocation_id
        ]:
            del self._replay_checkpoints[key]

    def _remember_evicted_sequence(
        self,
        invocation_id: UUID,
        sequence: int,
    ) -> None:
        self._evicted_event_sequences.pop(invocation_id, None)
        self._evicted_event_sequences[invocation_id] = sequence
        limit = max(1, self.retention_policy.max_terminal_invocations)
        while len(self._evicted_event_sequences) > limit:
            oldest, _ = self._evicted_event_sequences.popitem(last=False)
            if self.persistence is not None:
                self.persistence.release_invocation_tracking(oldest)
