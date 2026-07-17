from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any
from threading import RLock
from uuid import UUID

from autoagent.compiler import WorkflowVersionSnapshot
from autoagent.runtime.event import (
    RuntimeEvent,
    RuntimeEventDraft,
    invocation_checkpoint_events,
    operator_call_checkpoint_events,
    session_context_event,
    sort_runtime_event_drafts,
)
from autoagent.runtime.execution import NodeExecution, OperatorCall
from autoagent.runtime.invocation import Invocation
from autoagent.runtime.serialization import JsonRuntimeSerializer
from autoagent.runtime.session import Session


class SessionBusyError(RuntimeError):
    """Raised when a session already owns an unfinished Invocation."""

    def __init__(self, session: Session, invocation: Invocation) -> None:
        super().__init__(
            "Session already has an active invocation: "
            f"session_id={session.id}, invocation_id={invocation.id}, "
            f"state={invocation.state}"
        )
        self.session_id = session.id
        self.invocation_id = invocation.id
        self.invocation_state = invocation.state


class RuntimeStore(ABC):
    """Persistence boundary for sessions, invocations, and execution records.

    RuntimeStore is the only layer that should know whether runtime data lives
    in memory, a database, or a tracing backend. Business objects keep convenient
    methods, while the store serializes them into database-shaped records.

    A durable store must persist enough data to rebuild:
      - Session and its SessionContext.
      - Invocation state, InvocationContext, scheduler queues, and wait entries.
      - NodeExecution records and their OperatorCall trace records.

    Crash recovery starts from store records, not live Python call stacks.
    AutoAgentApp compares the persisted Workflow/Operator environment and either
    asks WorkflowExecutor to replay the whole node or marks the old Invocation
    interrupted. RuntimeStore never executes user code itself.

    Execution-path checkpoints update the Invocation control record, optional
    SessionContext, and only the NodeExecution rows changed in that control-loop
    turn. Materialized state and generated Runtime Events commit atomically.
    """

    serializer: JsonRuntimeSerializer

    async def ainitialize(self) -> None:
        """Initialize backing resources; in-memory stores require no work."""

    async def aclose(self) -> None:
        """Release backing resources; in-memory stores require no work."""

    @abstractmethod
    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        """Persist or confirm one immutable compiled Workflow version."""

        raise NotImplementedError

    @abstractmethod
    async def aload_workflow_snapshot(
        self,
        *,
        namespace: str,
        workflow_id: str,
        definition_hash: str,
        operator_manifest_hash: str | None = None,
    ) -> WorkflowVersionSnapshot | None:
        raise NotImplementedError

    @abstractmethod
    async def asave_session(self, session: Session) -> None:
        raise NotImplementedError

    @abstractmethod
    async def asave_session_context(self, session: Session) -> None:
        """Persist Session fields/context without rewriting invocation history."""

        raise NotImplementedError

    @abstractmethod
    async def aload_session(self, session_id: UUID) -> Session | None:
        raise NotImplementedError

    @abstractmethod
    async def aget_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        raise NotImplementedError

    @abstractmethod
    async def afind_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        """Load an existing session by its external identity without creating it."""

        raise NotImplementedError

    @abstractmethod
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
        """Atomically claim one waiting invocation for resume processing."""

        raise NotImplementedError

    @abstractmethod
    async def asave_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def acheckpoint_invocation(
        self,
        session: Session,
        invocation: Invocation,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
    ) -> None:
        """Atomically persist one execution-loop delta and its Runtime Events.

        The Invocation control record always changes because Scheduler queues,
        InvocationContext, state, result, or error may have changed. Only the
        listed NodeExecution rows and their OperatorCalls are rewritten. The
        supplied Session contributes SessionContext to the same transaction.
        """

        raise NotImplementedError

    @abstractmethod
    async def acheckpoint_operator_call(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        node_execution_id: UUID,
        node_id: str,
        call: OperatorCall,
    ) -> None:
        """Insert a running OperatorCall or update its terminal state atomically."""

        raise NotImplementedError

    @abstractmethod
    async def aadmit_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> Session:
        """Atomically reject an active session or attach and persist invocation."""

        raise NotImplementedError

    @abstractmethod
    async def aload_invocation(self, invocation_id: UUID) -> Invocation | None:
        raise NotImplementedError

    @abstractmethod
    async def alist_active_invocations(self) -> tuple[Invocation, ...]:
        raise NotImplementedError

    @abstractmethod
    async def arecover_interrupted_invocations(self) -> tuple[Invocation, ...]:
        """Force unfinished local work to ``interrupted`` without replay.

        This is a low-level maintenance/testing primitive, not the application
        crash-recovery entrypoint. AutoAgentApp owns compatibility checks and
        asks WorkflowExecutor to replay recoverable work lazily after the
        Workflow and Operator registries are available.
        """

        raise NotImplementedError

    @abstractmethod
    async def alist_workflow_snapshots(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[WorkflowVersionSnapshot, ...]:
        """List compiled graph versions available to observation clients."""

        raise NotImplementedError

    @abstractmethod
    async def alist_sessions(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[Session, ...]:
        raise NotImplementedError

    @abstractmethod
    async def alist_session_invocations(
        self,
        session_id: UUID,
    ) -> tuple[Invocation, ...]:
        raise NotImplementedError

    @abstractmethod
    async def alist_runtime_events(
        self,
        *,
        session_id: UUID | None = None,
        invocation_id: UUID | None = None,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
        visibility: str | None = None,
    ) -> tuple[RuntimeEvent, ...]:
        """Read immutable events in Session sequence order."""

        raise NotImplementedError

    @abstractmethod
    async def aappend_runtime_events(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        drafts: tuple[RuntimeEventDraft, ...],
    ) -> tuple[RuntimeEvent, ...]:
        """Append non-checkpoint events such as user-visible output deltas."""

        raise NotImplementedError

    @abstractmethod
    async def asave_projection_checkpoint(
        self,
        *,
        invocation_id: UUID,
        through_sequence: int,
        projection: dict[str, Any],
    ) -> None:
        """Persist a rebuildable observation projection at an event cursor."""

        raise NotImplementedError

    @abstractmethod
    async def aload_projection_checkpoint(
        self,
        *,
        invocation_id: UUID,
        at_or_before_sequence: int | None = None,
    ) -> tuple[int, dict[str, Any]] | None:
        """Load the newest projection no later than the optional cursor."""

        raise NotImplementedError


class InMemoryRuntimeStore(RuntimeStore):
    """In-memory store backed by database-shaped record tables.

    The dictionaries below intentionally mirror future database tables. Tests
    should assert against this behavior because tracing APIs, database stores,
    and resume logic should all expose the same logical structure.
    """

    def __init__(self, *, serializer: JsonRuntimeSerializer | None = None) -> None:
        self._lock = RLock()
        self.serializer = serializer or JsonRuntimeSerializer()
        self.workflow_versions: dict[
            tuple[str, str, str, str],
            dict[str, Any],
        ] = {}
        # Sessions table keyed by internal session UUID. session_keys is the
        # unique lookup index for `(namespace, workflow_id, external key)`.
        self.sessions: dict[UUID, dict[str, Any]] = {}
        self.session_keys: dict[tuple[str, str, str | None], UUID] = {}

        # Invocations table plus relation index from session to invocation ids.
        self.invocations: dict[UUID, dict[str, Any]] = {}
        self.session_invocations: dict[UUID, list[UUID]] = {}

        # Node execution table plus relation index from invocation to execution
        # ids. Each NodeExecution is a logical node execution, not an operator
        # call attempt.
        self.node_executions: dict[UUID, dict[str, Any]] = {}
        self.invocation_node_executions: dict[UUID, list[UUID]] = {}

        # Operator call table plus relation index from node execution to
        # concrete operator calls. Retry/fallback/map/replication all append
        # rows here.
        self.operator_calls: dict[UUID, dict[str, Any]] = {}
        self.node_operator_calls: dict[UUID, list[UUID]] = {}

        # Immutable event table and per-session sequence index. Events are kept
        # separately from materialized Invocation records so replay cursors and
        # observation clients never depend on mutable latest-state rows.
        self.runtime_events: dict[UUID, dict[str, Any]] = {}
        self.session_runtime_events: dict[UUID, list[UUID]] = {}

        # Rebuildable observation cache keyed by Invocation and event cursor.
        # This is not execution state and may be deleted without data loss.
        self.projection_checkpoints: dict[
            tuple[UUID, int],
            dict[str, Any],
        ] = {}

    def save_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        """Store the same portable record a durable Store writes to its table."""

        key = (
            namespace,
            snapshot.workflow_id,
            snapshot.definition_hash,
            snapshot.operator_manifest_hash,
        )
        with self._lock:
            existing = self.workflow_versions.get(key)
            record = snapshot.model_dump(mode="python")
            if existing is not None and existing != record:
                raise ValueError(
                    "Workflow snapshot identity collision: "
                    f"{snapshot.workflow_id}/{snapshot.definition_hash}"
                )
            self.workflow_versions[key] = deepcopy(record)

    def load_workflow_snapshot(
        self,
        *,
        namespace: str,
        workflow_id: str,
        definition_hash: str,
        operator_manifest_hash: str | None = None,
    ) -> WorkflowVersionSnapshot | None:
        with self._lock:
            if operator_manifest_hash is not None:
                record = self.workflow_versions.get(
                    (
                        namespace,
                        workflow_id,
                        definition_hash,
                        operator_manifest_hash,
                    )
                )
            else:
                matches = [
                    value
                    for key, value in self.workflow_versions.items()
                    if key[:3] == (namespace, workflow_id, definition_hash)
                ]
                if len(matches) > 1:
                    raise ValueError(
                        "operator_manifest_hash is required when a Workflow "
                        "definition has multiple Operator environments."
                    )
                record = matches[0] if matches else None
            return (
                WorkflowVersionSnapshot.model_validate(deepcopy(record))
                if record is not None
                else None
            )

    async def asave_workflow_snapshot(
        self,
        namespace: str,
        snapshot: WorkflowVersionSnapshot,
    ) -> None:
        self.save_workflow_snapshot(namespace, snapshot)

    async def aload_workflow_snapshot(
        self,
        *,
        namespace: str,
        workflow_id: str,
        definition_hash: str,
        operator_manifest_hash: str | None = None,
    ) -> WorkflowVersionSnapshot | None:
        return self.load_workflow_snapshot(
            namespace=namespace,
            workflow_id=workflow_id,
            definition_hash=definition_hash,
            operator_manifest_hash=operator_manifest_hash,
        )

    async def asave_session(self, session: Session) -> None:
        self.save_session(session)

    async def asave_session_context(self, session: Session) -> None:
        self.save_session_context(session)

    async def aload_session(self, session_id: UUID) -> Session | None:
        return self.load_session(session_id)

    async def aget_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        return self.get_or_create_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
        )

    async def afind_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        return self.find_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
        )

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
        return self.claim_waiting_session(
            namespace=namespace,
            workflow_id=workflow_id,
            session_key=session_key,
            wait_key=wait_key,
            workflow_definition_hash=workflow_definition_hash,
            workflow_operator_manifest_hash=workflow_operator_manifest_hash,
        )

    async def asave_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> None:
        self.save_invocation(session_id, invocation)

    async def acheckpoint_invocation(
        self,
        session: Session,
        invocation: Invocation,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
    ) -> None:
        self.checkpoint_invocation(
            session,
            invocation,
            node_execution_ids=node_execution_ids,
        )

    async def acheckpoint_operator_call(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        node_execution_id: UUID,
        node_id: str,
        call: OperatorCall,
    ) -> None:
        self.checkpoint_operator_call(
            session_id=session_id,
            invocation_id=invocation_id,
            node_execution_id=node_execution_id,
            node_id=node_id,
            call=call,
        )

    async def aadmit_invocation(
        self,
        session_id: UUID,
        invocation: Invocation,
    ) -> Session:
        return self.admit_invocation(session_id, invocation)

    async def aload_invocation(self, invocation_id: UUID) -> Invocation | None:
        return self.load_invocation(invocation_id)

    async def alist_active_invocations(self) -> tuple[Invocation, ...]:
        return self.list_active_invocations()

    async def arecover_interrupted_invocations(self) -> tuple[Invocation, ...]:
        return self.recover_interrupted_invocations()

    async def alist_workflow_snapshots(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[WorkflowVersionSnapshot, ...]:
        with self._lock:
            values = [
                WorkflowVersionSnapshot.model_validate(deepcopy(record))
                for key, record in self.workflow_versions.items()
                if (namespace is None or key[0] == namespace)
                and (workflow_id is None or key[1] == workflow_id)
            ]
        values.sort(key=lambda item: (item.workflow_id, item.definition_hash))
        return tuple(values)

    async def alist_sessions(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[Session, ...]:
        with self._lock:
            ids = [
                session_id
                for session_id, record in self.sessions.items()
                if (namespace is None or record["namespace"] == namespace)
                and (workflow_id is None or record["workflow_id"] == workflow_id)
            ]
        values = [
            session
            for session_id in ids
            if (session := self.load_session(session_id)) is not None
        ]
        values.sort(key=lambda item: (item.created_at_ms, str(item.id)))
        return tuple(values)

    async def alist_session_invocations(
        self,
        session_id: UUID,
    ) -> tuple[Invocation, ...]:
        session = self.load_session(session_id)
        return session.list_invocations() if session is not None else ()

    async def alist_runtime_events(
        self,
        *,
        session_id: UUID | None = None,
        invocation_id: UUID | None = None,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
        visibility: str | None = None,
    ) -> tuple[RuntimeEvent, ...]:
        _validate_event_query(
            session_id,
            invocation_id,
            after_sequence,
            before_sequence,
            limit,
        )
        with self._lock:
            records = list(self.runtime_events.values())
        values = [
            RuntimeEvent.model_validate(deepcopy(record))
            for record in records
            if (session_id is None or str(record["session_id"]) == str(session_id))
            and (
                invocation_id is None
                or str(record["invocation_id"]) == str(invocation_id)
            )
            and int(record["sequence"]) > after_sequence
            and (
                before_sequence is None
                or int(record["sequence"]) < before_sequence
            )
            and (visibility is None or record["visibility"] == visibility)
        ]
        values.sort(
            key=lambda item: item.sequence,
            reverse=before_sequence is not None,
        )
        page = values[:limit]
        if before_sequence is not None:
            page.reverse()
        return tuple(page)

    async def aappend_runtime_events(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        drafts: tuple[RuntimeEventDraft, ...],
    ) -> tuple[RuntimeEvent, ...]:
        with self._lock:
            return self._append_event_drafts(session_id, invocation_id, drafts)

    async def asave_projection_checkpoint(
        self,
        *,
        invocation_id: UUID,
        through_sequence: int,
        projection: dict[str, Any],
    ) -> None:
        if through_sequence < 0:
            raise ValueError("through_sequence cannot be negative.")
        with self._lock:
            if invocation_id not in self.invocations:
                raise KeyError(f"Unknown invocation: {invocation_id}")
            self.projection_checkpoints[(invocation_id, through_sequence)] = {
                "projection": deepcopy(projection),
            }

    async def aload_projection_checkpoint(
        self,
        *,
        invocation_id: UUID,
        at_or_before_sequence: int | None = None,
    ) -> tuple[int, dict[str, Any]] | None:
        with self._lock:
            candidates = [
                (sequence, record)
                for (candidate_id, sequence), record in self.projection_checkpoints.items()
                if candidate_id == invocation_id
                and (
                    at_or_before_sequence is None
                    or sequence <= at_or_before_sequence
                )
            ]
            if not candidates:
                return None
            sequence, record = max(candidates, key=lambda value: value[0])
            return sequence, deepcopy(record["projection"])

    def save_session(self, session: Session) -> None:
        with self._lock:
            record = session.to_record()
            self.sessions[session.id] = deepcopy(record)
            self.session_keys[
                (session.namespace, session.workflow_id, session.session_key)
            ] = session.id
            self.session_invocations.setdefault(session.id, [])
            for invocation in session.invocations:
                self.save_invocation(session.id, invocation)

    def save_session_context(self, session: Session) -> None:
        with self._lock:
            previous = self.sessions.get(session.id)
            if previous is None:
                raise KeyError(f"Unknown session: {session.id}")
            self.sessions[session.id] = deepcopy(session.to_record())
            self.session_keys[
                (session.namespace, session.workflow_id, session.session_key)
            ] = session.id
            if (
                previous.get("context") != session.context.to_record()
                and session.current_invocation_id is not None
                and session.current_invocation_id in self.invocations
            ):
                self._append_event_drafts(
                    session.id,
                    session.current_invocation_id,
                    (
                        session_context_event(
                            session_id=session.id,
                            invocation_id=session.current_invocation_id,
                            context=session.context.to_record(),
                            occurred_at_ms=session.updated_at_ms,
                        ),
                    ),
                )

    def load_session(self, session_id: UUID) -> Session | None:
        with self._lock:
            record = self.sessions.get(session_id)
            if record is None:
                return None

            invocations = [
                invocation
                for invocation_id in self.session_invocations.get(session_id, [])
                if (invocation := self.load_invocation(invocation_id)) is not None
            ]
            return Session.from_record(deepcopy(record), invocations=invocations)

    def get_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        with self._lock:
            key = (namespace, workflow_id, session_key)
            session_id = self.session_keys.get(key)
            if session_id is not None:
                session = self.load_session(session_id)
                if session is not None:
                    return session

            session = Session(
                namespace=namespace,
                workflow_id=workflow_id,
                session_key=session_key,
            )
            self.save_session(session)
            return session

    def find_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
    ) -> Session | None:
        with self._lock:
            session_id = self.session_keys.get((namespace, workflow_id, session_key))
            return self.load_session(session_id) if session_id is not None else None

    def claim_waiting_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str,
        wait_key: str,
        workflow_definition_hash: str | None = None,
        workflow_operator_manifest_hash: str | None = None,
    ) -> Session:
        with self._lock:
            session_id = self.session_keys.get((namespace, workflow_id, session_key))
            if session_id is None:
                raise KeyError(f"Unknown session: {session_key}")
            session = self.load_session(session_id)
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
                raise ValueError(
                    "Waiting Invocation belongs to a different Workflow definition."
                )
            if (
                workflow_operator_manifest_hash is not None
                and invocation.workflow_operator_manifest_hash
                != workflow_operator_manifest_hash
            ):
                raise ValueError(
                    "Waiting Invocation belongs to a different Operator manifest "
                    "environment."
                )
            if wait_key not in invocation.scheduler.waiting_executions:
                raise KeyError(f"Unknown wait key: {wait_key}")
            invocation.mark_running()
            self.save_invocation(session.id, invocation)
            return session

    def save_invocation(self, session_id: UUID, invocation: Invocation) -> None:
        with self._lock:
            if session_id not in self.sessions:
                raise KeyError(f"Unknown session: {session_id}")

            previous = self.load_invocation(invocation.id)

            self.invocations[invocation.id] = deepcopy(invocation.to_record(session_id))
            invocation_ids = self.session_invocations.setdefault(session_id, [])
            if invocation.id not in invocation_ids:
                invocation_ids.append(invocation.id)

            self.invocation_node_executions[invocation.id] = []
            for execution in invocation.node_executions:
                self._save_node_execution(invocation.id, execution)
            drafts = invocation_checkpoint_events(previous, invocation)
            self._append_event_drafts(session_id, invocation.id, tuple(drafts))

    def checkpoint_invocation(
        self,
        session: Session,
        invocation: Invocation,
        *,
        node_execution_ids: tuple[UUID, ...] = (),
    ) -> None:
        """Persist one control-loop delta under the Store lock.

        Unlike ``save_invocation``, this method never rebuilds the Invocation's
        child index. Existing historical NodeExecutions remain untouched and
        only explicitly listed rows are inserted or updated.
        """

        with self._lock:
            previous_session = self.sessions.get(session.id)
            if previous_session is None:
                raise KeyError(f"Unknown session: {session.id}")
            previous = self.load_invocation(invocation.id)
            if previous is None:
                raise KeyError(f"Unknown invocation: {invocation.id}")
            if str(self.invocations[invocation.id]["session_id"]) != str(session.id):
                raise ValueError("Invocation does not belong to the supplied session.")

            self.invocations[invocation.id] = deepcopy(
                invocation.to_record(session.id)
            )
            changed_ids = tuple(dict.fromkeys(node_execution_ids))
            for execution_id in changed_ids:
                execution = invocation.get_node_execution(execution_id)
                if execution is None:
                    raise KeyError(f"Unknown NodeExecution: {execution_id}")
                self._save_node_execution(invocation.id, execution)

            session_record = session.to_record()
            self.sessions[session.id] = deepcopy(session_record)
            self.session_keys[
                (session.namespace, session.workflow_id, session.session_key)
            ] = session.id

            drafts = invocation_checkpoint_events(
                previous,
                invocation,
                changed_node_execution_ids=frozenset(map(str, changed_ids)),
            )
            if previous_session.get("context") != session.context.to_record():
                drafts.append(
                    session_context_event(
                        session_id=session.id,
                        invocation_id=invocation.id,
                        context=session.context.to_record(),
                        occurred_at_ms=session.updated_at_ms,
                    )
                )
            sort_runtime_event_drafts(drafts)
            self._append_event_drafts(session.id, invocation.id, tuple(drafts))

    def checkpoint_operator_call(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        node_execution_id: UUID,
        node_id: str,
        call: OperatorCall,
    ) -> None:
        """Persist one call transition without mutating the live Invocation."""

        with self._lock:
            invocation_record = self.invocations.get(invocation_id)
            execution_record = self.node_executions.get(node_execution_id)
            if invocation_record is None:
                raise KeyError(f"Unknown invocation: {invocation_id}")
            if str(invocation_record["session_id"]) != str(session_id):
                raise ValueError("Invocation does not belong to the supplied session.")
            if execution_record is None:
                raise KeyError(f"Unknown NodeExecution: {node_execution_id}")
            if str(execution_record["invocation_id"]) != str(invocation_id):
                raise ValueError("NodeExecution does not belong to the Invocation.")

            previous = self._load_operator_call(call.id)
            self._save_operator_call(node_execution_id, call)
            drafts = operator_call_checkpoint_events(
                previous,
                call,
                node_id=node_id,
            )
            self._append_event_drafts(session_id, invocation_id, tuple(drafts))

    def admit_invocation(self, session_id: UUID, invocation: Invocation) -> Session:
        """Perform session admission and persistence under one store lock.

        `created` is treated as active because another caller must not enter the
        gap between admission and WorkflowExecutor.mark_running(). Interrupted
        invocations are historical recovery outcomes and do not block a new one.
        """

        with self._lock:
            session = self.load_session(session_id)
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
            self.save_session(session)
            return session

    def load_invocation(self, invocation_id: UUID) -> Invocation | None:
        with self._lock:
            record = self.invocations.get(invocation_id)
            if record is None:
                return None

            node_executions = [
                execution
                for execution_id in self.invocation_node_executions.get(invocation_id, [])
                if (execution := self._load_node_execution(execution_id)) is not None
            ]
            node_executions.sort(key=lambda execution: execution.sequence)
            return Invocation.from_record(
                deepcopy(record),
                node_executions=node_executions,
            )

    def list_active_invocations(self) -> tuple[Invocation, ...]:
        active_states = {"created", "running", "waiting"}
        invocations = []
        for invocation_id, record in self.invocations.items():
            if record.get("state") in active_states:
                invocation = self.load_invocation(invocation_id)
                if invocation is not None:
                    invocations.append(invocation)
        return tuple(invocations)

    def recover_interrupted_invocations(self) -> tuple[Invocation, ...]:
        """Restore active invocations and mark running executions interrupted.

        This is the minimal crash recovery primitive. It does not replay Python
        frames. It rebuilds runtime objects from records, turns any in-flight
        NodeExecution into `interrupted`, saves the changed records, and returns
        the affected invocations so WorkflowExecutor can decide the next action.
        """

        recovered = []
        for invocation in self.list_active_invocations():
            interrupted = invocation.recover_interrupted_executions()
            if interrupted:
                session_id = UUID(str(self.invocations[invocation.id]["session_id"]))
                self.save_invocation(session_id, invocation)
                recovered.append(invocation)
        return tuple(recovered)

    def _save_node_execution(
        self,
        invocation_id: UUID,
        execution: NodeExecution,
    ) -> None:
        self.node_executions[execution.id] = deepcopy(
            execution.to_record(invocation_id)
        )
        execution_ids = self.invocation_node_executions.setdefault(invocation_id, [])
        if execution.id not in execution_ids:
            execution_ids.append(execution.id)

        self.node_operator_calls[execution.id] = []
        for call in execution.operator_calls:
            self._save_operator_call(execution.id, call)

    def _load_node_execution(self, execution_id: UUID) -> NodeExecution | None:
        record = self.node_executions.get(execution_id)
        if record is None:
            return None

        operator_calls = []
        for call_id in self.node_operator_calls.get(execution_id, []):
            operator_call = self._load_operator_call(call_id)
            if operator_call is not None:
                operator_calls.append(operator_call)
        operator_calls.sort(key=lambda call: call.call_no)
        return NodeExecution.from_record(
            deepcopy(record),
            operator_calls=operator_calls,
        )

    def _save_operator_call(
        self,
        node_execution_id: UUID,
        call: OperatorCall,
    ) -> None:
        self.operator_calls[call.id] = deepcopy(
            call.to_record(node_execution_id)
        )
        call_ids = self.node_operator_calls.setdefault(
            node_execution_id,
            [],
        )
        if call.id not in call_ids:
            call_ids.append(call.id)

    def _load_operator_call(
        self,
        call_id: UUID,
    ) -> OperatorCall | None:
        record = self.operator_calls.get(call_id)
        if record is None:
            return None
        return OperatorCall.from_record(deepcopy(record))

    def _append_event_drafts(
        self,
        session_id: UUID,
        invocation_id: UUID,
        drafts: tuple[RuntimeEventDraft, ...],
    ) -> tuple[RuntimeEvent, ...]:
        if not drafts:
            return ()
        session_record = self.sessions.get(session_id)
        invocation_record = self.invocations.get(invocation_id)
        if session_record is None:
            raise KeyError(f"Unknown session: {session_id}")
        if invocation_record is None:
            raise KeyError(f"Unknown invocation: {invocation_id}")
        event_ids = self.session_runtime_events.setdefault(session_id, [])
        next_sequence = len(event_ids) + 1
        values: list[RuntimeEvent] = []
        for offset, draft in enumerate(drafts):
            runtime_event = draft.materialize(
                namespace=str(session_record["namespace"]),
                workflow_id=str(session_record["workflow_id"]),
                session_id=session_id,
                invocation_id=invocation_id,
                sequence=next_sequence + offset,
            )
            self.runtime_events[runtime_event.id] = runtime_event.model_dump(
                mode="python"
            )
            event_ids.append(runtime_event.id)
            values.append(runtime_event)
        return tuple(values)


def _validate_event_query(
    session_id: UUID | None,
    invocation_id: UUID | None,
    after_sequence: int,
    before_sequence: int | None,
    limit: int,
) -> None:
    if session_id is None and invocation_id is None:
        raise ValueError("session_id or invocation_id is required.")
    if after_sequence < 0:
        raise ValueError("after_sequence cannot be negative.")
    if before_sequence is not None and before_sequence <= 0:
        raise ValueError("before_sequence must be positive.")
    if before_sequence is not None and after_sequence >= before_sequence:
        raise ValueError("after_sequence must be less than before_sequence.")
    if limit <= 0 or limit > 10_000:
        raise ValueError("limit must be between 1 and 10000.")
