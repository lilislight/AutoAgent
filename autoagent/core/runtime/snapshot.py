from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.runtime.execution import NodeExecution
from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.time import TimestampMs, utc_timestamp_ms


RuntimeBoundary = Literal[
    "node.activation_ready",
    "node.input_ready",
    "node.output_ready",
    "node.committed",
    "routing.committed",
    "wait.committed",
    "resume.committed",
    "recovery.interrupted",
    "invocation.completed",
    "invocation.failed",
    "invocation.cancelled",
]


class StateOperation(BaseModel):
    """One deterministic JSON-tree mutation carried by a boundary event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    op: Literal["add", "replace", "remove"]
    path: tuple[str | int, ...]
    value: Any | None = None


class ExecutionSnapshot(BaseModel):
    """A complete restartable image at sequence zero or a compaction cursor."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID = Field(default_factory=uuid4)
    invocation_id: UUID
    through_sequence: int = Field(ge=0)
    state: dict[str, Any]
    created_at_ms: TimestampMs = Field(default_factory=utc_timestamp_ms)

    @classmethod
    def capture(
        cls,
        session: Session,
        invocation: Invocation,
        *,
        through_sequence: int | None = None,
    ) -> ExecutionSnapshot:
        return cls(
            invocation_id=invocation.id,
            through_sequence=(
                invocation.event_sequence
                if through_sequence is None
                else through_sequence
            ),
            state=capture_execution_state(session, invocation),
        )

    def restore(self) -> tuple[Session, Invocation]:
        return restore_execution_state(self.state)


def capture_execution_state(
    session: Session,
    invocation: Invocation,
) -> dict[str, Any]:
    """Capture only mutable runtime data required for deterministic restart."""

    return deepcopy(
        {
            "session": session.to_record(),
            "invocation": invocation.to_record(session.id),
            "node_executions": [
                execution.to_record(invocation.id)
                for execution in invocation.node_executions
            ],
        }
    )


def restore_execution_state(state: dict[str, Any]) -> tuple[Session, Invocation]:
    node_executions = [
        NodeExecution.from_record(record)
        for record in state.get("node_executions", [])
    ]
    invocation = Invocation.from_record(
        state["invocation"],
        node_executions=node_executions,
    )
    session = Session.from_record(state["session"], invocations=[invocation])
    return session, invocation


def diff_execution_state(
    previous: dict[str, Any],
    current: dict[str, Any],
) -> tuple[StateOperation, ...]:
    operations: list[StateOperation] = []
    _diff_value(previous, current, (), operations)
    return tuple(operations)


def apply_state_operations(
    state: dict[str, Any],
    operations: tuple[StateOperation, ...],
) -> dict[str, Any]:
    result = deepcopy(state)
    for operation in operations:
        _apply_operation(result, operation)
    return result


def reduce_execution_state(
    snapshot: ExecutionSnapshot,
    events: tuple[RuntimeEvent, ...],
    *,
    through_sequence: int | None = None,
) -> tuple[Session, Invocation]:
    """Rebuild runtime state by reducing immutable boundary deltas."""

    state = deepcopy(snapshot.state)
    cursor = snapshot.through_sequence
    for event in sorted(events, key=lambda item: item.sequence):
        if event.sequence <= cursor:
            continue
        if through_sequence is not None and event.sequence > through_sequence:
            break
        if event.role != "boundary":
            continue
        raw_operations = event.payload.get("operations")
        if raw_operations is None:
            raise ValueError(
                f"Boundary event {event.sequence} has no state operations."
            )
        operations = tuple(
            StateOperation.model_validate(value)
            for value in raw_operations
        )
        state = apply_state_operations(state, operations)
        cursor = event.sequence
    return restore_execution_state(state)


def _diff_value(
    previous: Any,
    current: Any,
    path: tuple[str | int, ...],
    operations: list[StateOperation],
) -> None:
    if type(previous) is not type(current):
        operations.append(
            StateOperation(op="replace", path=path, value=deepcopy(current))
        )
        return
    if isinstance(previous, dict):
        previous_keys = set(previous)
        current_keys = set(current)
        for key in sorted(previous_keys - current_keys):
            operations.append(StateOperation(op="remove", path=(*path, key)))
        for key in sorted(current_keys - previous_keys):
            operations.append(
                StateOperation(
                    op="add",
                    path=(*path, key),
                    value=deepcopy(current[key]),
                )
            )
        for key in sorted(previous_keys & current_keys):
            _diff_value(previous[key], current[key], (*path, key), operations)
        return
    if isinstance(previous, list):
        shared = min(len(previous), len(current))
        for index in range(shared):
            _diff_value(previous[index], current[index], (*path, index), operations)
        for index in range(len(previous) - 1, len(current) - 1, -1):
            operations.append(StateOperation(op="remove", path=(*path, index)))
        for index in range(shared, len(current)):
            operations.append(
                StateOperation(
                    op="add",
                    path=(*path, index),
                    value=deepcopy(current[index]),
                )
            )
        return
    if previous != current:
        operations.append(
            StateOperation(op="replace", path=path, value=deepcopy(current))
        )


def _apply_operation(root: dict[str, Any], operation: StateOperation) -> None:
    if not operation.path:
        if operation.op == "remove":
            root.clear()
            return
        if not isinstance(operation.value, dict):
            raise ValueError("Root state replacement must be a mapping.")
        root.clear()
        root.update(deepcopy(operation.value))
        return

    parent: Any = root
    for segment in operation.path[:-1]:
        parent = parent[segment]
    leaf = operation.path[-1]
    if operation.op == "remove":
        if isinstance(parent, list):
            parent.pop(int(leaf))
        else:
            del parent[leaf]
        return
    value = deepcopy(operation.value)
    if isinstance(parent, list):
        index = int(leaf)
        if operation.op == "add":
            parent.insert(index, value)
        else:
            parent[index] = value
    else:
        parent[leaf] = value
