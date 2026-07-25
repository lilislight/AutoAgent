from __future__ import annotations

import asyncio
from collections import OrderedDict
from copy import deepcopy
from threading import RLock
from typing import Any, Protocol
from uuid import UUID

from autoagent.core.compiler import WorkflowVersionSnapshot
from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.serialization import JsonRuntimeSerializer
from autoagent.core.runtime.retention import RuntimeRetentionPolicy
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.snapshot import (
    ExecutionSnapshot,
    StateOperation,
    apply_state_operations,
    capture_execution_state,
    reduce_execution_state,
    restore_execution_state,
)
from autoagent.core.runtime.time import utc_timestamp_ms


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

    @property
    def admission_paused(self) -> bool: ...

    @property
    def persistence_corrupted(self) -> bool: ...

    @property
    def pending_persistence_bytes(self) -> int: ...

    @property
    def pending_persistence_count(self) -> int: ...

    async def ainitialize(self) -> None: ...

    async def aclose(self) -> None: ...

    async def aflush(self) -> None: ...

    async def await_capacity(self) -> None: ...

    def dispatch_event_nowait(
        self,
        session_id: UUID,
        namespace: str,
        session_updated_at_ms: int,
        invocation_id: UUID,
        invocation_state: str,
        execution_mode: str,
        invocation_updated_at_ms: int,
        event: RuntimeEvent,
    ) -> None: ...

    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None: ...

    async def afind_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None: ...

    async def aadmit_invocation(
        self,
        session: Session,
        invocation: Invocation,
        snapshot: ExecutionSnapshot,
    ) -> None: ...

    async def aappend_event(
        self,
        session: Session,
        invocation: Invocation,
        event: RuntimeEvent,
        *,
        durability_barrier: bool,
    ) -> None: ...

    async def aload_execution_snapshot(
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

    def persistence_status(self, invocation_id: UUID) -> str: ...

    def durable_sequence(self, invocation_id: UUID) -> int: ...


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
    ) -> None:
        self.serializer = serializer or JsonRuntimeSerializer()
        self.backend = backend
        self.retention_policy = retention_policy or RuntimeRetentionPolicy()
        self._lock = RLock()
        self.workflow_versions: dict[
            tuple[str, str, str, str],
            WorkflowVersionSnapshot,
        ] = {}
        self.sessions: dict[UUID, Session] = {}
        self.session_keys: dict[tuple[str, str, str | None], UUID] = {}
        self.invocations: dict[UUID, Invocation] = {}
        self.invocation_sessions: dict[UUID, UUID] = {}
        self.runtime_events: dict[UUID, list[RuntimeEvent]] = {}
        self._replay_checkpoints: dict[
            tuple[UUID, int],
            ExecutionSnapshot,
        ] = {}
        self._committed_states: dict[UUID, dict[str, Any]] = {}
        self._pending_admissions: dict[UUID, Invocation] = {}
        self._durable_terminal_lru: OrderedDict[UUID, None] = OrderedDict()
        self._evicted_event_sequences: dict[UUID, int] = {}
        if backend is not None:
            backend.bind(self)

    @property
    def pending_persistence_bytes(self) -> int:
        return (
            self.backend.pending_persistence_bytes
            if self.backend is not None
            else 0
        )

    @property
    def pending_persistence_count(self) -> int:
        return (
            self.backend.pending_persistence_count
            if self.backend is not None
            else 0
        )

    @property
    def admission_paused(self) -> bool:
        return self.backend.admission_paused if self.backend is not None else False

    @property
    def persistence_corrupted(self) -> bool:
        """True when durable persistence is permanently unavailable.

        A ``_drive()`` loop inspects this at the top of every iteration so
        that an async serialization failure on the persistence thread stops
        the invocation within one scheduler boundary instead of silently
        advancing in-memory state past broken durable records.
        """

        return (
            self.backend.persistence_corrupted
            if self.backend is not None
            else False
        )

    async def ainitialize(self) -> None:
        if self.backend is not None:
            await self.backend.ainitialize()

    async def aclose(self) -> None:
        if self.backend is not None:
            await self.backend.aclose()

    async def aflush(self) -> None:
        if self.backend is not None:
            await self.backend.aflush()

    def save_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> bool:
        key = (
            namespace,
            snapshot.workflow_id,
            snapshot.definition_hash,
            snapshot.operator_manifest_hash,
        )
        with self._lock:
            existed = key in self.workflow_versions
            self.workflow_versions[key] = snapshot
        return not existed

    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        created = self.save_workflow_snapshot(namespace, snapshot)
        if created and self.backend is not None:
            await self.backend.asave_workflow_snapshot(namespace, snapshot)

    def load_workflow_snapshot(
        self,
        *,
        namespace: str,
        workflow_id: str,
        definition_hash: str,
        operator_manifest_hash: str | None = None,
    ) -> WorkflowVersionSnapshot | None:
        with self._lock:
            matches = [
                snapshot
                for key, snapshot in self.workflow_versions.items()
                if key[0] == namespace
                and key[1] == workflow_id
                and key[2] == definition_hash
                and (operator_manifest_hash is None or key[3] == operator_manifest_hash)
            ]
        if len(matches) > 1 and operator_manifest_hash is None:
            raise ValueError("operator_manifest_hash is required for this version.")
        return matches[0] if matches else None

    async def aload_workflow_snapshot(self, **kwargs: Any) -> WorkflowVersionSnapshot | None:
        return self.load_workflow_snapshot(**kwargs)

    def get_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        key = (namespace, workflow_id, session_key)
        with self._lock:
            session_id = self.session_keys.get(key)
            if session_id is not None:
                return self.sessions[session_id]
            session = Session(
                namespace=namespace,
                workflow_id=workflow_id,
                session_key=session_key,
            )
            self._cache_session(session)
            return session

    async def aget_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        if session_key is not None:
            existing = await self.afind_session(
                namespace=namespace,
                workflow_id=workflow_id,
                session_key=session_key,
            )
            if existing is not None:
                return existing
        return self.get_or_create_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
        )

    def find_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        with self._lock:
            session_id = self.session_keys.get(
                (namespace, workflow_id, session_key)
            )
            return self.sessions.get(session_id) if session_id is not None else None

    async def afind_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        value = self.find_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
        )
        if value is not None or self.backend is None:
            return value
        value = await self.backend.afind_session(
            namespace=namespace,
            workflow_id=workflow_id,
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
        if self.admission_paused:
            timeout_ms = (
                self.backend.queue_admission_timeout_ms
                if self.backend is not None
                else 0
            )
            if timeout_ms == 0:
                raise RuntimeError(
                    "Runtime persistence backlog is above the admission "
                    "watermark."
                )
            # Poll until admission resumes or the timeout elapses.
            # Multiple callers for the same session may wait concurrently.
            # After the queue drains, the RLock and _pending_admissions dict
            # (just below) serialize admission so only one succeeds and the
            # rest receive SessionBusyError.
            import time as _time
            started = _time.monotonic()
            deadline_s = timeout_ms / 1_000
            while self.admission_paused:
                if _time.monotonic() - started >= deadline_s:
                    raise TimeoutError(
                        "Runtime persistence backlog did not clear "
                        f"within {timeout_ms:.0f} ms."
                    )
                await asyncio.sleep(0.05)
        with self._lock:
            session = self.sessions.get(session_id)
            if session is None:
                raise KeyError(f"Unknown session: {session_id}")
            pending = self._pending_admissions.get(session_id)
            if pending is not None:
                raise SessionBusyError(session, pending)
            current = session.get_current_invocation()
            if current is not None and current.state in {
                "created",
                "running",
                "waiting",
            }:
                raise SessionBusyError(session, current)
            admitted_at_ms = utc_timestamp_ms()
            session_record = session.to_record()
            session_record["current_invocation_id"] = str(invocation.id)
            session_record["updated_at_ms"] = admitted_at_ms
            state = deepcopy(
                {
                    "session": session_record,
                    "invocation": invocation.to_record(session.id),
                    "node_executions": [],
                }
            )
            snapshot = ExecutionSnapshot(
                invocation_id=invocation.id,
                through_sequence=0,
                state=state,
            )
            self._pending_admissions[session_id] = invocation
        cancelled: asyncio.CancelledError | None = None
        try:
            if self.backend is not None:
                admission = asyncio.create_task(
                    self.backend.aadmit_invocation(
                        session,
                        invocation,
                        snapshot,
                    )
                )
                try:
                    await asyncio.shield(admission)
                except asyncio.CancelledError as exc:
                    await admission
                    cancelled = exc
        except BaseException:
            with self._lock:
                if self._pending_admissions.get(session_id) is invocation:
                    del self._pending_admissions[session_id]
            raise
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
            self._committed_states[invocation.id] = state
            self._replay_checkpoints[(invocation.id, 0)] = snapshot
        if cancelled is not None:
            raise cancelled
        return session

    async def aclaim_waiting_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
        wait_key: str,
        workflow_definition_hash: str | None = None,
        workflow_operator_manifest_hash: str | None = None,
    ) -> Session:
        session = await self.afind_session(
            namespace=namespace,
            workflow_id=workflow_id,
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
        if (
            workflow_operator_manifest_hash is not None
            and invocation.workflow_operator_manifest_hash
            != workflow_operator_manifest_hash
        ):
            raise ValueError("Waiting Invocation uses another Operator environment.")
        if wait_key not in invocation.scheduler.waiting_executions:
            raise KeyError(f"Unknown wait key: {wait_key}")
        return session

    async def aapply_event(
        self,
        session: Session,
        invocation: Invocation,
        event: RuntimeEvent,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
        durability_barrier: bool = False,
    ) -> RuntimeEvent:
        del node_execution_ids
        if event.invocation_id != invocation.id:
            raise ValueError("RuntimeEvent invocation does not match aggregate.")
        if "operations" not in event.payload:
            raise ValueError("Boundary RuntimeEvent has no state operations.")
        with self._lock:
            events = self.runtime_events.setdefault(invocation.id, [])
            expected = events[-1].sequence + 1 if events else 1
            if event.sequence != expected:
                raise ValueError(
                    f"Expected event sequence {expected}, got {event.sequence}."
                )
            previous = self._committed_states.get(invocation.id)
            if previous is None:
                raise KeyError(f"Unknown Invocation aggregate: {invocation.id}")
            operations = tuple(
                StateOperation.model_validate(value)
                for value in event.payload["operations"]
            )
            reduced = apply_state_operations(previous, operations)
        cancelled: asyncio.CancelledError | None = None
        try:
            if self.backend is not None:
                await self.backend.await_capacity()
                if durability_barrier:
                    acceptance = asyncio.create_task(
                        self.backend.aappend_event(
                            session,
                            invocation,
                            event,
                            durability_barrier=True,
                        )
                    )
                    try:
                        await asyncio.shield(acceptance)
                    except asyncio.CancelledError as exc:
                        await acceptance
                        cancelled = exc
                else:
                    self.backend.dispatch_event_nowait(
                        session.id,
                        session.namespace,
                        session.updated_at_ms,
                        invocation.id,
                        invocation.state,
                        invocation.execution_mode,
                        invocation.updated_at_ms,
                        event,
                    )
        except BaseException:
            with self._lock:
                self._restore_live_aggregate(
                    session,
                    invocation,
                    previous,
                )
            raise
        with self._lock:
            events = self.runtime_events.setdefault(invocation.id, [])
            expected = events[-1].sequence + 1 if events else 1
            if event.sequence != expected:
                raise RuntimeError(
                    "Invocation Event sequence changed while persistence "
                    "accepted a boundary."
                )
            events.append(event)
            self._committed_states[invocation.id] = reduced
            self.sessions[session.id] = session
            self.invocations[invocation.id] = invocation
        if self.backend is not None:
            self._persistence_advanced(invocation.id)
        if cancelled is not None:
            raise cancelled
        return event

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

    def committed_state(self, invocation_id: UUID) -> dict[str, Any]:
        """Return the immutable-by-contract reducer baseline for event creation."""

        with self._lock:
            state = self._committed_states.get(invocation_id)
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
            with self._lock:
                self._cache_session(session)
                self.invocations[invocation.id] = invocation
                self.invocation_sessions[invocation.id] = session.id
                self.runtime_events[invocation.id] = list(all_events)
                self._committed_states[invocation.id] = capture_execution_state(
                    session,
                    invocation,
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
            return tuple(values[-limit:] if before_sequence is not None else values[:limit])
        if self.backend is None:
            return ()
        return await self.backend.alist_runtime_events(
            invocation_id=invocation_id,
            after_sequence=after_sequence,
            before_sequence=before_sequence,
            limit=limit,
        )

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
            return (
                "durable"
                if self.backend.durable_sequence(invocation_id) >= expected
                else "pending"
            )
        return self.backend.persistence_status(invocation_id)

    def durable_sequence(self, invocation_id: UUID) -> int:
        return (
            self.backend.durable_sequence(invocation_id)
            if self.backend is not None
            else 0
        )

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
            (session.namespace, session.workflow_id, session.session_key)
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
            if (
                not events
                or events[-1].sequence < invocation.event_sequence
            ):
                return
            if self.durable_sequence(invocation_id) < invocation.event_sequence:
                return
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
        self._evicted_event_sequences[invocation_id] = (
            invocation.event_sequence
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
        self.runtime_events.pop(invocation_id, None)
        self._committed_states.pop(invocation_id, None)
        for key in [
            key
            for key in self._replay_checkpoints
            if key[0] == invocation_id
        ]:
            del self._replay_checkpoints[key]
