from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.runtime.event import RuntimeEvent, StateOperation
from autoagent.core.runtime.execution import NodeExecution
from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.time import TimestampMs, utc_timestamp_ms


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
            state=(
                capture_recovery_state(session, invocation)
                if invocation.event_mode == "standard"
                else capture_execution_state(session, invocation)
            ),
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


def capture_recovery_state(
    session: Session,
    invocation: Invocation,
) -> dict[str, Any]:
    """Capture restartable Standard state without trace-only intermediate I/O."""

    # Compact the locally owned records before freezing them. Calling
    # capture_execution_state() first would deepcopy the complete trace and
    # compact_recovery_state() would immediately deepcopy it a second time,
    # including operator inputs/outputs that Standard never persists.
    state = {
        "session": session.to_record(),
        "invocation": invocation.to_record(session.id),
        "node_executions": [
            execution.to_record(invocation.id)
            for execution in invocation.node_executions
        ],
    }
    _compact_recovery_state_in_place(state)
    return deepcopy(state)


def compact_recovery_state(state: dict[str, Any]) -> dict[str, Any]:
    """Remove trace-only values from an already captured Runtime State."""

    state = deepcopy(state)
    _compact_recovery_state_in_place(state)
    return state


def _compact_recovery_state_in_place(state: dict[str, Any]) -> None:
    """Compact a private state tree without making another defensive copy."""

    for execution in state["node_executions"]:
        _compact_node_execution_record_in_place(execution)


def _compact_node_execution_record_in_place(
    execution: dict[str, Any],
) -> None:
    """Remove trace-only values from one privately owned Node record."""

    execution["input"] = None
    for operator_call in execution.get("operator_executions", ()):
        operator_call.pop("input", None)
        operator_call.pop("output", None)


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


def build_state_operations(
    previous: dict[str, Any],
    session: Session,
    invocation: Invocation,
    *,
    node_execution_ids: tuple[UUID, ...] = (),
    copy_operation_values: bool = True,
) -> tuple[StateOperation, ...]:
    """Build one state delta from explicit mutable runtime sections.

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
        copy_operation_values=copy_operation_values,
    )

    current_invocation = invocation.to_record(session.id)
    previous_invocation = previous["invocation"]
    _replace_changed_fields(
        operations,
        section="invocation",
        previous=previous_invocation,
        current=current_invocation,
        fields=_MUTABLE_INVOCATION_FIELDS,
        copy_operation_values=copy_operation_values,
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
                f"Event references unknown NodeExecution: {execution_id}"
            )
        record = execution.to_record(invocation.id)
        previous_index = previous_indexes.get(str(execution_id))
        if previous_index is None:
            operations.append(
                StateOperation(
                    op="add",
                    path=("node_executions", len(previous_executions)),
                    value=(
                        deepcopy(record)
                        if copy_operation_values
                        else record
                    ),
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
                copy_operation_values=copy_operation_values,
            )
    return tuple(operations)


def build_recovery_state_operations(
    previous: dict[str, Any],
    session: Session,
    invocation: Invocation,
    *,
    node_execution_ids: tuple[UUID, ...] = (),
) -> tuple[StateOperation, ...]:
    """Build internal Standard-mode deltas without trace-only Node values."""

    operations: list[StateOperation] = []
    current_session = session.to_record()
    _replace_changed_fields(
        operations,
        section="session",
        previous=previous["session"],
        current=current_session,
        fields=_MUTABLE_SESSION_FIELDS,
        copy_operation_values=False,
    )
    current_invocation = invocation.to_record(session.id)
    _replace_changed_fields(
        operations,
        section="invocation",
        previous=previous["invocation"],
        current=current_invocation,
        fields=_MUTABLE_INVOCATION_FIELDS,
        copy_operation_values=False,
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
                f"Event references unknown NodeExecution: {execution_id}"
            )
        record = execution.to_record(invocation.id)
        _compact_node_execution_record_in_place(record)
        previous_index = previous_indexes.get(str(execution_id))
        if previous_index is None:
            operations.append(
                StateOperation(
                    op="add",
                    path=("node_executions", len(previous_executions)),
                    value=record,
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
                copy_operation_values=False,
            )
    return tuple(operations)


def apply_state_operations(
    state: dict[str, Any],
    operations: tuple[StateOperation, ...],
) -> dict[str, Any]:
    if not operations:
        return state

    # Runtime State is immutable from the journal/reducer's perspective. Clone
    # only containers on paths changed by this Event, at most once per path.
    # Unchanged branches are structurally shared with the previous state.
    result = dict(state)
    owned_paths: set[tuple[str | int, ...]] = {()}
    for operation in operations:
        if not operation.path:
            _apply_operation(result, operation)
            owned_paths = {()}
            continue
        parent = _copy_on_write_parent(
            result,
            operation.path,
            owned_paths=owned_paths,
        )
        _apply_operation_to_parent(parent, operation)
    return result


def reduce_execution_state(
    snapshot: ExecutionSnapshot,
    events: tuple[RuntimeEvent, ...],
    *,
    through_sequence: int | None = None,
) -> tuple[Session, Invocation]:
    """Rebuild runtime state by reducing immutable state deltas."""

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
        if event.operations is None:
            raise ValueError(
                f"Full RuntimeEvent {event.sequence} has no state operations."
            )
        state = apply_state_operations(state, event.operations)
        cursor = event.sequence
    return restore_execution_state(state)


def _replace_changed_fields(
    operations: list[StateOperation],
    *,
    section: str | tuple[str | int, ...],
    previous: dict[str, Any],
    current: dict[str, Any],
    fields: tuple[str, ...],
    copy_operation_values: bool,
) -> None:
    prefix = (section,) if isinstance(section, str) else section
    for field in fields:
        _diff_state_value(
            operations,
            path=(*prefix, field),
            previous=previous.get(field),
            current=current.get(field),
            copy_operation_values=copy_operation_values,
        )


def _diff_state_value(
    operations: list[StateOperation],
    *,
    path: tuple[str | int, ...],
    previous: Any,
    current: Any,
    copy_operation_values: bool,
) -> None:
    """Emit leaf-level tree edits while preserving append-only histories."""

    if previous == current:
        return
    if isinstance(previous, dict) and isinstance(current, dict):
        for key in sorted(set(previous) - set(current)):
            operations.append(StateOperation(op="remove", path=(*path, key)))
        for key in sorted(set(current) - set(previous)):
            operations.append(
                StateOperation(
                    op="add",
                    path=(*path, key),
                    value=(
                        deepcopy(current[key])
                        if copy_operation_values
                        else current[key]
                    ),
                )
            )
        for key in sorted(set(previous) & set(current)):
            _diff_state_value(
                operations,
                path=(*path, key),
                previous=previous[key],
                current=current[key],
                copy_operation_values=copy_operation_values,
            )
        return
    if (
        isinstance(previous, list)
        and isinstance(current, list)
        and len(current) >= len(previous)
        and current[: len(previous)] == previous
    ):
        for index in range(len(previous), len(current)):
            operations.append(
                StateOperation(
                    op="add",
                    path=(*path, index),
                    value=(
                        deepcopy(current[index])
                        if copy_operation_values
                        else current[index]
                    ),
                )
            )
        return
    operations.append(
        StateOperation(
            op="replace",
            path=path,
            value=deepcopy(current) if copy_operation_values else current,
        )
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
    _apply_operation_to_parent(parent, operation)


def _copy_on_write_parent(
    root: dict[str, Any],
    path: tuple[str | int, ...],
    *,
    owned_paths: set[tuple[str | int, ...]],
) -> Any:
    parent: Any = root
    prefix: tuple[str | int, ...] = ()
    for segment in path[:-1]:
        child = parent[segment]
        child_path = (*prefix, segment)
        if child_path not in owned_paths:
            if isinstance(child, dict):
                child = dict(child)
            elif isinstance(child, list):
                child = list(child)
            else:
                raise ValueError(
                    "State operation traverses a non-container value at "
                    f"path {child_path!r}."
                )
            parent[segment] = child
            owned_paths.add(child_path)
        parent = child
        prefix = child_path
    return parent


def _apply_operation_to_parent(
    parent: Any,
    operation: StateOperation,
) -> None:
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
