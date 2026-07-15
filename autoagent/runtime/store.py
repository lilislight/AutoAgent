from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any
from uuid import UUID

from autoagent.runtime.execution import NodeExecution, OperatorCall
from autoagent.runtime.invocation import Invocation
from autoagent.runtime.session import Session


class RuntimeStore(ABC):
    """Persistence boundary for sessions, invocations, and execution records.

    RuntimeStore is the only layer that should know whether runtime data lives
    in memory, a database, or a tracing backend. Business objects keep convenient
    methods, while the store serializes them into database-shaped records.

    A durable store must persist enough data to rebuild:
      - Session and its SessionContext.
      - Invocation state, InvocationContext, scheduler queues, and wait entries.
      - NodeExecution records and their OperatorCall trace records.

    Crash recovery starts from store records, not live Python call stacks. A
    running NodeExecution restored after process loss is marked interrupted so
    higher layers can retry, fail, or ask an operator-specific idempotent resume
    path to continue.
    """

    @abstractmethod
    def save_session(self, session: Session) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_session(self, session_id: UUID) -> Session | None:
        raise NotImplementedError

    @abstractmethod
    def get_or_create_session(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_key: str | None,
    ) -> Session:
        raise NotImplementedError

    @abstractmethod
    def save_invocation(self, session_id: UUID, invocation: Invocation) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_invocation(self, invocation_id: UUID) -> Invocation | None:
        raise NotImplementedError

    @abstractmethod
    def list_active_invocations(self) -> tuple[Invocation, ...]:
        raise NotImplementedError

    @abstractmethod
    def recover_interrupted_invocations(self) -> tuple[Invocation, ...]:
        raise NotImplementedError


class InMemoryRuntimeStore(RuntimeStore):
    """In-memory store backed by database-shaped record tables.

    The dictionaries below intentionally mirror future database tables. Tests
    should assert against this behavior because tracing APIs, database stores,
    and resume logic should all expose the same logical structure.
    """

    def __init__(self) -> None:
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

    def save_session(self, session: Session) -> None:
        record = session.to_record()
        self.sessions[session.id] = deepcopy(record)
        self.session_keys[
            (session.namespace, session.workflow_id, session.session_key)
        ] = session.id
        self.session_invocations.setdefault(session.id, [])
        for invocation in session.invocations:
            self.save_invocation(session.id, invocation)

    def load_session(self, session_id: UUID) -> Session | None:
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

    def save_invocation(self, session_id: UUID, invocation: Invocation) -> None:
        if session_id not in self.sessions:
            raise KeyError(f"Unknown session: {session_id}")

        self.invocations[invocation.id] = deepcopy(invocation.to_record(session_id))
        invocation_ids = self.session_invocations.setdefault(session_id, [])
        if invocation.id not in invocation_ids:
            invocation_ids.append(invocation.id)

        self.invocation_node_executions[invocation.id] = []
        for execution in invocation.node_executions:
            self._save_node_execution(invocation.id, execution)

    def load_invocation(self, invocation_id: UUID) -> Invocation | None:
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
