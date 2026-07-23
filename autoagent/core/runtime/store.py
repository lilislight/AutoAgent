from __future__ import annotations

from threading import RLock
from typing import Any
from uuid import UUID

from autoagent.core.compiler import WorkflowVersionSnapshot
from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.serialization import JsonRuntimeSerializer
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.snapshot import (
    ExecutionSnapshot,
    StateOperation,
    apply_state_operations,
    capture_execution_state,
    diff_execution_state,
    reduce_execution_state,
)


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


class RuntimeStore:
    """Authoritative in-memory aggregate and immutable boundary journal.

    The executor mutates its live aggregate on the App runtime loop, emits one
    sequenced boundary delta, and applies it here. Durable implementations add a
    downstream persistence coordinator; database state is never the live
    execution authority.
    """

    def __init__(
        self,
        *,
        serializer: JsonRuntimeSerializer | None = None,
    ) -> None:
        self.serializer = serializer or JsonRuntimeSerializer()

    async def ainitialize(self) -> None:
        return None

    async def aclose(self) -> None:
        return None

    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        raise NotImplementedError

    async def aload_workflow_snapshot(
        self,
        *,
        namespace: str,
        workflow_id: str,
        definition_hash: str,
        operator_manifest_hash: str | None = None,
    ) -> WorkflowVersionSnapshot | None:
        raise NotImplementedError

    async def aget_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        raise NotImplementedError

    async def afind_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        raise NotImplementedError

    async def aadmit_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> Session:
        raise NotImplementedError

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
        raise NotImplementedError

    async def aapply_event(
        self,
        session: Session,
        invocation: Invocation,
        event: RuntimeEvent,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
        durability_barrier: bool = False,
    ) -> RuntimeEvent:
        raise NotImplementedError

    async def asave_execution_snapshot(
        self,
        snapshot: ExecutionSnapshot,
        *,
        durability_barrier: bool = False,
    ) -> None:
        raise NotImplementedError

    async def aload_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None = None,
    ) -> ExecutionSnapshot | None:
        raise NotImplementedError

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
        events = await self.alist_runtime_events(
            invocation_id=invocation_id,
            after_sequence=snapshot.through_sequence,
            before_sequence=(
                through_sequence + 1
                if through_sequence is not None
                else None
            ),
            limit=1_000_000,
        )
        return reduce_execution_state(
            snapshot,
            events,
            through_sequence=through_sequence,
        )

    async def alist_runtime_events(
        self,
        *,
        invocation_id: UUID,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
    ) -> tuple[RuntimeEvent, ...]:
        raise NotImplementedError


class InMemoryRuntimeStore(RuntimeStore):
    """One-copy runtime aggregates with indexes, snapshots, and event journals."""

    def __init__(
        self,
        *,
        serializer: JsonRuntimeSerializer | None = None,
    ) -> None:
        super().__init__(serializer=serializer)
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
        self.execution_snapshots: dict[
            tuple[UUID, int],
            ExecutionSnapshot,
        ] = {}
        self._committed_states: dict[UUID, dict[str, Any]] = {}

    def save_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        key = (
            namespace,
            snapshot.workflow_id,
            snapshot.definition_hash,
            snapshot.operator_manifest_hash,
        )
        with self._lock:
            self.workflow_versions[key] = snapshot

    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        self.save_workflow_snapshot(namespace, snapshot)

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
            self.sessions[session.id] = session
            self.session_keys[key] = session.id
            return session

    async def aget_or_create_session(self, **kwargs: Any) -> Session:
        return self.get_or_create_session(**kwargs)

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

    async def afind_session(self, **kwargs: Any) -> Session | None:
        return self.find_session(**kwargs)

    async def aadmit_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> Session:
        with self._lock:
            session = self.sessions.get(session_id)
            if session is None:
                raise KeyError(f"Unknown session: {session_id}")
            current = session.get_current_invocation()
            if current is not None and current.state in {
                "created",
                "running",
                "waiting",
            }:
                raise SessionBusyError(session, current)
            session.add_invocation(invocation)
            self.invocations[invocation.id] = invocation
            self.invocation_sessions[invocation.id] = session.id
            self.runtime_events[invocation.id] = []
            state = capture_execution_state(session, invocation)
            self._committed_states[invocation.id] = state
            snapshot = ExecutionSnapshot(
                invocation_id=invocation.id,
                through_sequence=0,
                state=state,
            )
            self.execution_snapshots[(invocation.id, 0)] = snapshot
        await InMemoryRuntimeStore.asave_execution_snapshot(
            self,
            snapshot,
            durability_barrier=True,
        )
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
        session = self.find_session(
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
        del node_execution_ids, durability_barrier
        if event.invocation_id != invocation.id:
            raise ValueError("RuntimeEvent invocation does not match aggregate.")
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
                for value in event.payload.get("operations", [])
            )
            reduced = apply_state_operations(previous, operations)
            live = capture_execution_state(session, invocation)
            if reduced != live:
                divergence = diff_execution_state(reduced, live)
                raise ValueError(
                    f"Boundary reducer diverged from live state at {event.type}: "
                    f"{divergence[:5]}"
                )
            events.append(event)
            self._committed_states[invocation.id] = reduced
            self.sessions[session.id] = session
            self.invocations[invocation.id] = invocation
        return event

    async def asave_execution_snapshot(
        self,
        snapshot: ExecutionSnapshot,
        *,
        durability_barrier: bool = False,
    ) -> None:
        del durability_barrier
        with self._lock:
            self.execution_snapshots[
                (snapshot.invocation_id, snapshot.through_sequence)
            ] = snapshot

    async def aload_execution_snapshot(
        self,
        invocation_id: UUID,
        *,
        at_or_before_sequence: int | None = None,
    ) -> ExecutionSnapshot | None:
        with self._lock:
            candidates = [
                snapshot
                for (candidate_id, sequence), snapshot in self.execution_snapshots.items()
                if candidate_id == invocation_id
                and (
                    at_or_before_sequence is None
                    or sequence <= at_or_before_sequence
                )
            ]
        return (
            max(candidates, key=lambda value: value.through_sequence)
            if candidates
            else None
        )

    async def arebuild_execution(
        self,
        invocation_id: UUID,
        *,
        through_sequence: int | None = None,
    ) -> tuple[Session, Invocation]:
        session, invocation = await super().arebuild_execution(
            invocation_id,
            through_sequence=through_sequence,
        )
        if through_sequence is None:
            with self._lock:
                self.sessions[session.id] = session
                self.invocations[invocation.id] = invocation
                self.invocation_sessions[invocation.id] = session.id
                self.session_keys[
                    (session.namespace, session.workflow_id, session.session_key)
                ] = session.id
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
            values = [
                event
                for event in self.runtime_events.get(invocation_id, [])
                if event.sequence > after_sequence
                and (
                    before_sequence is None
                    or event.sequence < before_sequence
                )
            ]
        if before_sequence is not None:
            return tuple(values[-limit:])
        return tuple(values[:limit])
