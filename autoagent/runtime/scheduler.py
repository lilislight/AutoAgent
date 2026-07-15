from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from autoagent.runtime.status import NodeExecutionStateValue


@dataclass
class NodeExecutionRequest:
    """Request to create one logical NodeExecution for a static workflow node.

    This object lives in SchedulerContext.ready_queue. It is not a Workflow IR
    node and it is not a NodeExecution yet. The scheduler creates this request
    after graph rules say a node can run; the WorkflowExecutor drains a batch of
    requests, creates NodeExecution objects, and passes them to NodeExecutor.

    source_execution_ids records which completed upstream executions caused this
    request. It is useful for trace/debug and future join semantics, but normal
    input_mapping should read OutputContext rather than this field.
    """

    node_id: str
    source_execution_ids: tuple[UUID, ...] = ()

    def to_record(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "source_execution_ids": [
                str(execution_id) for execution_id in self.source_execution_ids
            ],
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> NodeExecutionRequest:
        return cls(
            node_id=str(record["node_id"]),
            source_execution_ids=tuple(
                UUID(value) for value in record.get("source_execution_ids", [])
            ),
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
    ) -> None:
        self.ready_queue: deque[NodeExecutionRequest] = deque(ready_queue or [])
        self.waiting_executions: dict[str, WaitingExecution] = dict(
            waiting_executions or {}
        )
        self.transition_queue: deque[NodeExecutionTransition] = deque(
            transition_queue or []
        )

    def enqueue_ready(
        self,
        node_id: str,
        *,
        source_execution_ids: tuple[UUID, ...] = (),
    ) -> NodeExecutionRequest:
        request = NodeExecutionRequest(
            node_id=node_id,
            source_execution_ids=source_execution_ids,
        )
        self.ready_queue.append(request)
        return request

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
        )
