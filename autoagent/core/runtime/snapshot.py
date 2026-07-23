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


_MUTABLE_SESSION_FIELDS = (
    "context",
    "current_invocation_id",
    "updated_at_ms",
)

_MUTABLE_INVOCATION_FIELDS = (
    "state",
    "execution_mode",
    "event_sequence",
    "context",
    "result",
    "scheduler",
    "error",
    "deferred_error",
    "deferred_terminal_state",
    "updated_at_ms",
)

_MUTABLE_NODE_EXECUTION_FIELDS = (
    "state",
    "input",
    "output",
    "error",
    "idempotency_key",
    "recovery_of_execution_id",
    "recovery_attempt",
    "incoming_activations",
    "execution_scope",
    "operator_executions",
    "edge_evaluations",
    "resource_usage",
    "started_at_ms",
    "ended_at_ms",
    "updated_at_ms",
)


def build_boundary_state_operations(
    previous: dict[str, Any],
    session: Session,
    invocation: Invocation,
    *,
    node_execution_ids: tuple[UUID, ...] = (),
) -> tuple[StateOperation, ...]:
    """Build one boundary delta from explicit mutable runtime sections.

    Invocation identity, input, Workflow identity, and creation timestamps are
    immutable after admission and therefore never scanned. NodeExecution
    changes are addressed directly by id instead of diffing the complete
    execution history.
    """

    operations: list[StateOperation] = []
    current_session = session.to_record()
    previous_session = previous["session"]
    _replace_changed_fields(
        operations,
        section="session",
        previous=previous_session,
        current=current_session,
        fields=_MUTABLE_SESSION_FIELDS,
    )

    current_invocation = invocation.to_record(session.id)
    previous_invocation = previous["invocation"]
    _replace_changed_fields(
        operations,
        section="invocation",
        previous=previous_invocation,
        current=current_invocation,
        fields=_MUTABLE_INVOCATION_FIELDS,
    )

    previous_executions = previous.get("node_executions", [])
    previous_indexes = {
        str(record["id"]): index
        for index, record in enumerate(previous_executions)
    }
    seen: set[UUID] = set()
    for execution_id in node_execution_ids:
        if execution_id in seen:
            continue
        seen.add(execution_id)
        execution = invocation.get_node_execution(execution_id)
        if execution is None:
            raise KeyError(
                f"Boundary references unknown NodeExecution: {execution_id}"
            )
        record = execution.to_record(invocation.id)
        previous_index = previous_indexes.get(str(execution_id))
        if previous_index is None:
            operations.append(
                StateOperation(
                    op="add",
                    path=("node_executions", len(previous_executions)),
                    value=deepcopy(record),
                )
            )
            previous_executions = [*previous_executions, record]
            previous_indexes[str(execution_id)] = len(previous_executions) - 1
        elif previous_executions[previous_index] != record:
            _replace_changed_fields(
                operations,
                section=("node_executions", previous_index),
                previous=previous_executions[previous_index],
                current=record,
                fields=_MUTABLE_NODE_EXECUTION_FIELDS,
            )
    return tuple(operations)


def apply_state_operations(
    state: dict[str, Any],
    operations: tuple[StateOperation, ...],
) -> dict[str, Any]:
    if not operations:
        return state
    if not all(_is_boundary_operation(operation) for operation in operations):
        result = deepcopy(state)
        for operation in operations:
            _apply_operation(result, operation)
        return result

    # Boundary operations only replace top-level aggregate fields or fields on
    # one NodeExecution record. Clone those containers once instead of copying
    # the complete Invocation history for every Event.
    result = dict(state)
    if any(operation.path[0] == "session" for operation in operations):
        result["session"] = dict(state["session"])
    if any(operation.path[0] == "invocation" for operation in operations):
        result["invocation"] = dict(state["invocation"])
    node_operations = [
        operation
        for operation in operations
        if operation.path[0] == "node_executions"
    ]
    if node_operations:
        result["node_executions"] = list(state.get("node_executions", []))
        cloned_indexes: set[int] = set()
        for operation in node_operations:
            if len(operation.path) != 3:
                continue
            index = int(operation.path[1])
            if index in cloned_indexes:
                continue
            result["node_executions"][index] = dict(
                result["node_executions"][index]
            )
            cloned_indexes.add(index)
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
        expected = cursor + 1
        if event.sequence != expected:
            raise ValueError(
                "RuntimeEvent journal is not contiguous: "
                f"expected sequence {expected}, got {event.sequence}."
            )
        if event.invocation_id != snapshot.invocation_id:
            raise ValueError(
                "RuntimeEvent invocation does not match ExecutionSnapshot."
            )
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


def _replace_changed_fields(
    operations: list[StateOperation],
    *,
    section: str | tuple[str | int, ...],
    previous: dict[str, Any],
    current: dict[str, Any],
    fields: tuple[str, ...],
) -> None:
    prefix = (section,) if isinstance(section, str) else section
    for field in fields:
        if previous.get(field) == current.get(field):
            continue
        operations.append(
            StateOperation(
                op="replace",
                path=(*prefix, field),
                value=deepcopy(current.get(field)),
            )
        )


def _is_boundary_operation(operation: StateOperation) -> bool:
    path = operation.path
    if operation.op == "replace":
        return (
            len(path) == 2
            and path[0] in {"session", "invocation"}
        ) or (
            len(path) == 3
            and path[0] == "node_executions"
            and isinstance(path[1], int)
        )
    return (
        operation.op == "add"
        and len(path) == 2
        and path[0] == "node_executions"
        and isinstance(path[1], int)
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
