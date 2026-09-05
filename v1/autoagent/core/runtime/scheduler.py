from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from autoagent.core.runtime.status import EdgeResolutionStateValue, NodeExecutionStateValue


@dataclass(frozen=True, order=True)
class LoopIteration:
    """One natural-loop iteration in a NodeExecution's lexical scope."""

    loop_region_id: str
    iteration: int

    def to_record(self) -> dict[str, Any]:
        return {
            "loop_region_id": self.loop_region_id,
            "iteration": self.iteration,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> LoopIteration:
        return cls(
            loop_region_id=str(record["loop_region_id"]),
            iteration=int(record["iteration"]),
        )


ExecutionScope = tuple[LoopIteration, ...]


def execution_scope_key(scope: ExecutionScope) -> str:
    if not scope:
        return "root"
    return "/".join(
        f"{frame.loop_region_id}:{frame.iteration}" for frame in scope
    )


def node_instance_key(node_id: str, scope: ExecutionScope) -> str:
    return node_id if not scope else f"{node_id}@{execution_scope_key(scope)}"


def edge_occurrence_key(edge_id: str, scope: ExecutionScope) -> str:
    return edge_id if not scope else f"{edge_id}@{execution_scope_key(scope)}"


@dataclass(frozen=True)
class EdgeActivation:
    """One selected edge produced by a concrete source NodeExecution.

    Unlike an invocation-level EdgeResolution, an activation is repeatable: a
    loop may traverse the same static edge many times, each time with a different
    source_execution_id. NodeExecutionRequest carries the activation into the
    target execution so input_mapping can read the exact values that triggered
    this execution instead of guessing from latest node outputs.
    """

    edge_id: str
    source_node_id: str
    source_execution_id: UUID

    def to_record(self) -> dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "source_node_id": self.source_node_id,
            "source_execution_id": str(self.source_execution_id),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> EdgeActivation:
        return cls(
            edge_id=str(record["edge_id"]),
            source_node_id=str(record["source_node_id"]),
            source_execution_id=UUID(str(record["source_execution_id"])),
        )


@dataclass(frozen=True)
class EdgeResolution:
    """Final state of one static edge occurrence in an execution scope.

    The same static edge may resolve again in another loop iteration. Missing
    occurrence keys are pending. A selected resolution stores the concrete
    activation consumed by the target; a skipped resolution has no activation.
    """

    edge_id: str
    state: EdgeResolutionStateValue
    activation: EdgeActivation | None = None
    scope: ExecutionScope = ()

    def to_record(self) -> dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "state": self.state,
            "activation": self.activation.to_record() if self.activation else None,
            "scope": [frame.to_record() for frame in self.scope],
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> EdgeResolution:
        activation = record.get("activation")
        return cls(
            edge_id=str(record["edge_id"]),
            state=record["state"],
            activation=(
                EdgeActivation.from_record(activation)
                if activation is not None
                else None
            ),
            scope=tuple(
                LoopIteration.from_record(item)
                for item in record.get("scope", [])
            ),
        )


@dataclass
class NodeExecutionRequest:
    """Request to create one logical NodeExecution for a static workflow node.

    This object lives in SchedulerContext.ready_queue. It is not a Workflow IR
    node and it is not a NodeExecution yet. The scheduler creates this request
    after graph rules say a node can run; the WorkflowExecutor drains a batch of
    requests, creates NodeExecution objects, and passes them to NodeExecutor.

    activations records the exact selected incoming edges and source executions.
    A normal/loop step usually has one; a complete fan-in may have several.
    """

    node_id: str
    activations: tuple[EdgeActivation, ...] = ()
    execution_scope: ExecutionScope = ()
    idempotency_key: str | None = None
    recovery_of_execution_id: UUID | None = None
    recovery_attempt: int = 0

    def to_record(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "activations": [activation.to_record() for activation in self.activations],
            "execution_scope": [
                frame.to_record() for frame in self.execution_scope
            ],
            "idempotency_key": self.idempotency_key,
            "recovery_of_execution_id": (
                str(self.recovery_of_execution_id)
                if self.recovery_of_execution_id is not None
                else None
            ),
            "recovery_attempt": self.recovery_attempt,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> NodeExecutionRequest:
        return cls(
            node_id=str(record["node_id"]),
            activations=tuple(
                EdgeActivation.from_record(item)
                for item in record.get("activations", [])
            ),
            execution_scope=tuple(
                LoopIteration.from_record(item)
                for item in record.get("execution_scope", [])
            ),
            idempotency_key=record.get("idempotency_key"),
            recovery_of_execution_id=(
                UUID(str(record["recovery_of_execution_id"]))
                if record.get("recovery_of_execution_id") is not None
                else None
            ),
            recovery_attempt=int(record.get("recovery_attempt", 0)),
        )


@dataclass
class WaitingExecution:
    """A NodeExecution paused until an external actor resumes it.

    waiting_executions is keyed by wait_key, not node_id. A single static node
    can wait many times across one invocation, especially inside loops. The
    resume API should find the waiting entry by wait_key, then complete or fail
    the stored node_execution_id.

    wait_type is descriptive. Runtime treats human approval, webhook callback,
    timer wakeup, and generic signal as the same wait/resume mechanism; adapters
    and UI can use wait_type/payload to route the external resume action.
    """

    wait_key: str
    node_execution_id: UUID
    node_id: str
    wait_type: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            "wait_key": self.wait_key,
            "node_execution_id": str(self.node_execution_id),
            "node_id": self.node_id,
            "wait_type": self.wait_type,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> WaitingExecution:
        return cls(
            wait_key=str(record["wait_key"]),
            node_execution_id=UUID(str(record["node_execution_id"])),
            node_id=str(record["node_id"]),
            wait_type=record.get("wait_type"),
            payload=dict(record.get("payload", {})),
        )


@dataclass
class NodeExecutionTransition:
    """Completed node execution state that scheduler still needs to process.

    NodeExecutor does not select outgoing edges directly. It marks a
    NodeExecution completed/failed/waiting/interrupted, then WorkflowExecutor
    appends a transition. Scheduler drains transitions in the next scheduling
    pass, evaluates outgoing edges based on the final NodeExecution.output, and
    appends new NodeExecutionRequest objects to ready_queue.

    state should only be a terminal or externally-stable execution state such as
    completed, failed, waiting, interrupted, skipped, or cancelled. Created,
    ready, and running are not useful transition states because they do not
    advance the graph.
    """

    node_execution_id: UUID
    node_id: str
    state: NodeExecutionStateValue

    def to_record(self) -> dict[str, Any]:
        return {
            "node_execution_id": str(self.node_execution_id),
            "node_id": self.node_id,
            "state": self.state,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> NodeExecutionTransition:
        return cls(
            node_execution_id=UUID(str(record["node_execution_id"])),
            node_id=str(record["node_id"]),
            state=record["state"],
        )


class SchedulerContext:
    """Scheduler-owned cursor for one invocation.

    This class is deliberately small: it stores only the current scheduling
    cursor, not historical node outputs or trace data. The scheduler and
    WorkflowExecutor are the only framework layers expected to mutate it.

    ready_queue:
        Ordered NodeExecutionRequest queue. Scheduler appends requests when a
        static workflow node becomes runnable. WorkflowExecutor drains a batch
        and creates one NodeExecution for each request. A batch represents work
        that can be executed concurrently from the workflow-dependency point of
        view; NodeExecutor may still limit concurrency by operator/node policy.

    waiting_executions:
        Mapping from wait_key to WaitingExecution. NodeExecutor or a system
        command adds an entry when a NodeExecution reaches a stable external
        wait. app.resume()/timer/webhook/human UI removes the entry by wait_key.

    transition_queue:
        Ordered NodeExecutionTransition queue. WorkflowExecutor appends a
        transition after a node reaches completed/failed/waiting/interrupted.
        Scheduler drains transitions to evaluate outgoing edges and produce new
        ready requests.
    """

    def __init__(
        self,
        *,
        ready_queue: list[NodeExecutionRequest] | None = None,
        waiting_executions: dict[str, WaitingExecution] | None = None,
        transition_queue: list[NodeExecutionTransition] | None = None,
        edge_resolutions: dict[str, EdgeResolution] | None = None,
        loop_boundary_resolutions: dict[str, EdgeResolution] | None = None,
        scheduled_node_instances: set[str] | None = None,
        skipped_node_instances: set[str] | None = None,
        entered_loop_instances: set[str] | None = None,
        exited_loop_instances: set[str] | None = None,
        entry_paths_initialized: bool = False,
    ) -> None:
        self.ready_queue: deque[NodeExecutionRequest] = deque(ready_queue or [])
        self.waiting_executions: dict[str, WaitingExecution] = dict(
            waiting_executions or {}
        )
        self.transition_queue: deque[NodeExecutionTransition] = deque(
            transition_queue or []
        )
        self.edge_resolutions: dict[str, EdgeResolution] = dict(
            edge_resolutions or {}
        )
        self.loop_boundary_resolutions: dict[str, EdgeResolution] = dict(
            loop_boundary_resolutions or {}
        )
        self.scheduled_node_instances: set[str] = set(
            scheduled_node_instances or set()
        )
        self.skipped_node_instances: set[str] = set(skipped_node_instances or set())
        self.entered_loop_instances: set[str] = set(
            entered_loop_instances or set()
        )
        self.exited_loop_instances: set[str] = set(exited_loop_instances or set())
        self.entry_paths_initialized = entry_paths_initialized

    def enqueue_ready(
        self,
        node_id: str,
        *,
        activations: tuple[EdgeActivation, ...] = (),
        execution_scope: ExecutionScope = (),
        idempotency_key: str | None = None,
        recovery_of_execution_id: UUID | None = None,
        recovery_attempt: int = 0,
    ) -> NodeExecutionRequest:
        request = NodeExecutionRequest(
            node_id=node_id,
            activations=activations,
            execution_scope=execution_scope,
            idempotency_key=idempotency_key,
            recovery_of_execution_id=recovery_of_execution_id,
            recovery_attempt=recovery_attempt,
        )
        self.ready_queue.append(request)
        return request

    def resolve_edge(
        self,
        edge_id: str,
        *,
        state: EdgeResolutionStateValue,
        activation: EdgeActivation | None = None,
        scope: ExecutionScope = (),
    ) -> EdgeResolution:
        """Resolve one target edge occurrence exactly once."""

        key = edge_occurrence_key(edge_id, scope)
        existing = self.edge_resolutions.get(key)
        if existing is not None:
            if existing.state != state or existing.activation != activation:
                raise ValueError(f"Edge already resolved with different state: {edge_id}")
            return existing
        if state == "selected" and activation is None:
            raise ValueError("Selected edge resolution requires an activation.")
        if state == "skipped" and activation is not None:
            raise ValueError("Skipped edge resolution cannot carry an activation.")
        resolution = EdgeResolution(
            edge_id=edge_id,
            state=state,
            activation=activation,
            scope=scope,
        )
        self.edge_resolutions[key] = resolution
        return resolution

    def resolve_loop_boundary(
        self,
        *,
        loop_region_id: str,
        loop_scope: ExecutionScope,
        edge_id: str,
        state: EdgeResolutionStateValue,
        activation: EdgeActivation | None = None,
    ) -> EdgeResolution:
        """Resolve a back/exit edge for one concrete loop iteration."""

        key = (
            f"{loop_region_id}@{execution_scope_key(loop_scope)}:{edge_id}"
        )
        existing = self.loop_boundary_resolutions.get(key)
        if existing is not None:
            if existing.state != state or existing.activation != activation:
                raise ValueError(
                    f"Loop boundary already resolved with different state: {key}"
                )
            return existing
        if state == "selected" and activation is None:
            raise ValueError("Selected loop boundary requires an activation.")
        if state == "skipped" and activation is not None:
            raise ValueError("Skipped loop boundary cannot carry an activation.")
        resolution = EdgeResolution(
            edge_id=edge_id,
            state=state,
            activation=activation,
            scope=loop_scope,
        )
        self.loop_boundary_resolutions[key] = resolution
        return resolution

    def pop_ready(self) -> NodeExecutionRequest | None:
        if not self.ready_queue:
            return None
        return self.ready_queue.popleft()

    def drain_ready(self, limit: int | None = None) -> list[NodeExecutionRequest]:
        requests: list[NodeExecutionRequest] = []
        while self.ready_queue and (limit is None or len(requests) < limit):
            requests.append(self.ready_queue.popleft())
        return requests

    def add_waiting_execution(
        self,
        *,
        wait_key: str,
        node_execution_id: UUID,
        node_id: str,
        wait_type: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WaitingExecution:
        if wait_key in self.waiting_executions:
            raise ValueError(f"Duplicate active wait key: {wait_key}")
        waiting = WaitingExecution(
            wait_key=wait_key,
            node_execution_id=node_execution_id,
            node_id=node_id,
            wait_type=wait_type,
            payload=dict(payload or {}),
        )
        self.waiting_executions[wait_key] = waiting
        return waiting

    def remove_waiting_execution(self, wait_key: str) -> WaitingExecution:
        return self.waiting_executions.pop(wait_key)

    def enqueue_transition(
        self,
        *,
        node_execution_id: UUID,
        node_id: str,
        state: NodeExecutionStateValue,
    ) -> NodeExecutionTransition:
        transition = NodeExecutionTransition(
            node_execution_id=node_execution_id,
            node_id=node_id,
            state=state,
        )
        self.transition_queue.append(transition)
        return transition

    def pop_transition(self) -> NodeExecutionTransition | None:
        if not self.transition_queue:
            return None
        return self.transition_queue.popleft()

    def drain_transitions(
        self,
        limit: int | None = None,
    ) -> list[NodeExecutionTransition]:
        transitions: list[NodeExecutionTransition] = []
        while self.transition_queue and (limit is None or len(transitions) < limit):
            transitions.append(self.transition_queue.popleft())
        return transitions

    def to_record(self) -> dict[str, Any]:
        return {
            "ready_queue": [request.to_record() for request in self.ready_queue],
            "waiting_executions": {
                wait_key: waiting.to_record()
                for wait_key, waiting in self.waiting_executions.items()
            },
            "transition_queue": [
                transition.to_record() for transition in self.transition_queue
            ],
            "edge_resolutions": {
                edge_id: resolution.to_record()
                for edge_id, resolution in self.edge_resolutions.items()
            },
            "loop_boundary_resolutions": {
                key: resolution.to_record()
                for key, resolution in self.loop_boundary_resolutions.items()
            },
            "scheduled_node_instances": sorted(self.scheduled_node_instances),
            "skipped_node_instances": sorted(self.skipped_node_instances),
            "entered_loop_instances": sorted(self.entered_loop_instances),
            "exited_loop_instances": sorted(self.exited_loop_instances),
            "entry_paths_initialized": self.entry_paths_initialized,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any] | None) -> SchedulerContext:
        record = record or {}
        waiting_record = record.get("waiting_executions")
        if waiting_record is None:
            waiting_record = record.get("waiting_nodes", {})
        return cls(
            ready_queue=[
                NodeExecutionRequest.from_record(item)
                for item in record.get("ready_queue", [])
            ],
            waiting_executions={
                wait_key: WaitingExecution.from_record(item)
                for wait_key, item in waiting_record.items()
            },
            transition_queue=[
                NodeExecutionTransition.from_record(item)
                for item in record.get("transition_queue", [])
            ],
            edge_resolutions={
                edge_id: EdgeResolution.from_record(item)
                for edge_id, item in record.get("edge_resolutions", {}).items()
            },
            loop_boundary_resolutions={
                key: EdgeResolution.from_record(item)
                for key, item in record.get(
                    "loop_boundary_resolutions", {}
                ).items()
            },
            scheduled_node_instances=set(
                record.get("scheduled_node_instances", [])
            ),
            skipped_node_instances=set(record.get("skipped_node_instances", [])),
            entered_loop_instances=set(record.get("entered_loop_instances", [])),
            exited_loop_instances=set(record.get("exited_loop_instances", [])),
            entry_paths_initialized=bool(record.get("entry_paths_initialized", False)),
        )
