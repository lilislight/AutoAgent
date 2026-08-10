"""Canonical per-Session Runtime State and its only mutation API."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Literal, Sequence
from uuid import UUID

from .serialization import RuntimeValueCodec


PathToken = str | int
OperationKind = Literal["add", "replace", "remove"]
RUNTIME_STATE_SCHEMA_VERSION = 1

_ROOT_KEYS = {
    "schema_version",
    "session",
    "invocation",
    "scheduler",
    "node_executions",
    "waits",
    "pending_advances",
    "counters",
}
_SESSION_KEYS = {
    "id",
    "workflow_id",
    "created_at_ms",
    "updated_at_ms",
    "context",
    "context_path_revisions",
}
_INVOCATION_KEYS = {
    "id",
    "workflow_revision_id",
    "event_mode",
    "state",
    "created_at_ms",
    "updated_at_ms",
    "input",
    "context",
    "context_path_revisions",
    "output",
    "error",
    "runtime_event_sequence",
    "user_event_sequence",
    "recovery_mode",
    "cancel_requested",
    "deferred_error",
}
_SCHEDULER_KEYS = {
    "ready",
    "active_requests",
    "resolutions",
    "skipped",
    "pending_boundaries",
}
_COUNTER_KEYS = {
    "node_executions",
    "operator_attempts",
    "operator_runtime_ns",
}
_INVOCATION_STATES = {
    "created",
    "running",
    "waiting",
    "completed",
    "failed",
    "cancelled",
}
_EVENT_MODES = {"minimal", "standard", "full"}
_NODE_EXECUTION_KEYS = {
    "id",
    "node_id",
    "scope",
    "state",
    "input",
    "output",
    "error",
    "logical_occurrence",
    "idempotency_key",
    "started_state_version",
    "restart_session_context",
    "restart_invocation_context",
}
_NODE_STATES = {
    "pending",
    "ready",
    "running",
    "waiting",
    "completed",
    "failed",
    "skipped",
    "cancelled",
}
_WAIT_KEYS = {"id", "node_execution_id", "request", "payload"}


def context_path_key(path: Sequence[str]) -> str:
    """Encode one Context leaf path as a stable JSON Pointer key."""

    if not path:
        raise ValueError("Context revision path cannot be empty.")
    return "/" + "/".join(
        token.replace("~", "~0").replace("/", "~1") for token in path
    )


def context_path_from_key(value: str) -> tuple[str, ...]:
    if not isinstance(value, str) or not value.startswith("/") or value == "/":
        raise ValueError("Context revision key must be a non-root JSON Pointer.")
    return tuple(
        token.replace("~1", "/").replace("~0", "~")
        for token in value[1:].split("/")
    )


@dataclass(frozen=True, slots=True)
class StateOperation:
    """One ordered JSON-Patch-compatible Runtime State change."""

    op: OperationKind
    path: tuple[PathToken, ...]
    value: Any = None
    _persistent_value: Any = field(default=None, repr=False, compare=False)
    _captured: bool = field(default=False, repr=False, compare=False)

    @classmethod
    def capture(cls, operation: "StateOperation") -> "StateOperation":
        if operation._captured:
            return operation
        if operation.op == "remove":
            return cls("remove", operation.path, _captured=True)
        captured = RuntimeValueCodec.capture(operation.value)
        return cls(
            operation.op,
            operation.path,
            captured.transfer_to_runtime(),
            captured.persistent_value(),
            True,
        )

    def to_record(self) -> dict[str, Any]:
        value = self._persistent_value
        if self.op != "remove" and not self._captured:
            value = RuntimeValueCodec.encode(self.value)
        return {
            "op": self.op,
            "path": list(self.path),
            **({"value": value} if self.op != "remove" else {}),
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "StateOperation":
        operation = str(value["op"])
        if operation not in {"add", "replace", "remove"}:
            raise ValueError(f"Unsupported StateOperation {operation!r}.")
        return cls(
            op=operation,  # type: ignore[arg-type]
            path=tuple(value["path"]),
            value=(
                RuntimeValueCodec.decode(value.get("value"))
                if operation != "remove"
                else None
            ),
            _persistent_value=value.get("value") if operation != "remove" else None,
            _captured=True,
        )


@dataclass(frozen=True, slots=True)
class StateOperationBatch:
    """One atomically-applied, ordered group of State Operations."""

    state_version: int
    operations: tuple[StateOperation, ...]

    def to_record(self) -> dict[str, Any]:
        return {
            "state_version": self.state_version,
            "operations": [operation.to_record() for operation in self.operations],
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "StateOperationBatch":
        return cls(
            state_version=int(value["state_version"]),
            operations=tuple(
                StateOperation.from_record(operation)
                for operation in value.get("operations", [])
            ),
        )


class RuntimeState:
    """The authoritative mutable state for exactly one Session.

    The canonical tree uses mappings and lists so the exact same paths can be
    applied to its persistent JSON tree. Values at leaves may retain supported
    Python runtime types; ``_persistent`` contains their encoded representation.
    """

    def __init__(
        self,
        value: dict[str, Any],
        *,
        state_version: int = 0,
        persistent_value: dict[str, Any] | None = None,
    ) -> None:
        captured = RuntimeValueCodec.capture(value) if persistent_value is None else None
        self._value = (
            captured.transfer_to_runtime() if captured is not None else copy.deepcopy(value)
        )
        self._persistent = (
            captured.persistent_value()
            if captured is not None
            else copy.deepcopy(persistent_value)
        )
        if not isinstance(self._value, dict) or not isinstance(self._persistent, dict):
            raise TypeError("RuntimeState root must be a mapping.")
        _validate_runtime_tree(self._value)
        self._state_version = state_version

    @classmethod
    def create(
        cls,
        *,
        workflow_id: str,
        workflow_revision_id: str,
        session_id: str,
        invocation_id: UUID | str,
        event_mode: str,
        invocation_input: Any,
        session_context: dict[str, Any] | None = None,
        session_created_at_ms: int,
        invocation_created_at_ms: int,
    ) -> "RuntimeState":
        """Create the one canonical Runtime tree for a Session Invocation."""

        if not isinstance(event_mode, str):
            raise TypeError("Runtime State event_mode must be str.")
        normalized_mode = event_mode.strip().lower()
        timestamp = invocation_created_at_ms
        return cls(
            {
                "schema_version": RUNTIME_STATE_SCHEMA_VERSION,
                "session": {
                    "id": session_id,
                    "workflow_id": workflow_id,
                    "created_at_ms": session_created_at_ms,
                    "updated_at_ms": timestamp,
                    "context": session_context or {},
                    "context_path_revisions": {},
                },
                "invocation": {
                    "id": str(invocation_id),
                    "workflow_revision_id": workflow_revision_id,
                    "event_mode": normalized_mode,
                    "state": "created",
                    "created_at_ms": invocation_created_at_ms,
                    "updated_at_ms": timestamp,
                    "input": invocation_input,
                    "context": {},
                    "context_path_revisions": {},
                    "output": None,
                    "error": None,
                    "runtime_event_sequence": 0,
                    "user_event_sequence": 0,
                    "recovery_mode": False,
                    "cancel_requested": False,
                    "deferred_error": None,
                },
                "scheduler": {
                    "ready": [],
                    "active_requests": {},
                    "resolutions": {},
                    "skipped": {},
                    "pending_boundaries": {},
                },
                "node_executions": {},
                "waits": {},
                "pending_advances": {},
                "counters": {
                    "node_executions": {},
                    "operator_attempts": {},
                    "operator_runtime_ns": {},
                },
            }
        )

    @property
    def state_version(self) -> int:
        return self._state_version

    def restore_state_version(self, state_version: int) -> None:
        """Set the durable version while adapting the legacy Checkpoint model.

        This is intentionally not a normal mutation API. It is removed when
        Recovery consumes the canonical Runtime State record directly.
        """

        if not isinstance(state_version, int) or isinstance(state_version, bool):
            raise TypeError("Runtime State version must be int.")
        if state_version < 0:
            raise ValueError("Runtime State version cannot be negative.")
        self._state_version = state_version

    def read(self, *path: PathToken) -> Any:
        """Read a detached value so callers cannot bypass State Operations."""

        return copy.deepcopy(_read_path(self._value, path))

    def isolate(self, *path: PathToken) -> Any:
        return self.read(*path)

    def apply(
        self, operations: Sequence[StateOperation]
    ) -> StateOperationBatch:
        """Capture and atomically apply one ordered Operation batch.

        Both candidate trees are copy-on-write. A bad later Operation therefore
        cannot leave either the Python Runtime or persistent JSON state partially
        modified.
        """

        if not operations:
            raise ValueError("A State Operation batch cannot be empty.")
        captured = tuple(StateOperation.capture(operation) for operation in operations)
        candidate = _apply_copy_on_write(self._value, captured, persistent=False)
        persistent = _apply_copy_on_write(self._persistent, captured, persistent=True)
        _validate_runtime_tree(candidate)
        next_version = self._state_version + 1
        self._value = candidate
        self._persistent = persistent
        self._state_version = next_version
        return StateOperationBatch(next_version, captured)

    def checkpoint_record(self) -> dict[str, Any]:
        return {
            "state_version": self._state_version,
            "state": copy.deepcopy(self._persistent),
        }

    @classmethod
    def from_checkpoint_record(cls, record: dict[str, Any]) -> "RuntimeState":
        persistent = copy.deepcopy(record["state"])
        value = RuntimeValueCodec.decode(persistent)
        if not isinstance(value, dict):
            raise TypeError("Checkpoint Runtime State must decode to a mapping.")
        return cls(
            value,
            state_version=int(record["state_version"]),
            persistent_value=persistent,
        )


def _validate_runtime_tree(value: dict[str, Any]) -> None:
    _require_exact_keys("Runtime State", value, _ROOT_KEYS)
    if value["schema_version"] != RUNTIME_STATE_SCHEMA_VERSION:
        raise ValueError("Unsupported Runtime State schema version.")

    session = _require_mapping("session", value["session"])
    _require_exact_keys("session", session, _SESSION_KEYS)
    _require_nonempty_string("session.id", session["id"])
    _require_nonempty_string("session.workflow_id", session["workflow_id"])
    _require_timestamp("session.created_at_ms", session["created_at_ms"])
    _require_timestamp("session.updated_at_ms", session["updated_at_ms"])
    _require_mapping("session.context", session["context"])
    _require_revision_map(
        "session.context_path_revisions", session["context_path_revisions"]
    )

    invocation = _require_mapping("invocation", value["invocation"])
    _require_exact_keys("invocation", invocation, _INVOCATION_KEYS)
    _require_uuid_string("invocation.id", invocation["id"])
    _require_nonempty_string(
        "invocation.workflow_revision_id", invocation["workflow_revision_id"]
    )
    if invocation["event_mode"] not in _EVENT_MODES:
        raise ValueError("Runtime State invocation.event_mode is invalid.")
    if invocation["state"] not in _INVOCATION_STATES:
        raise ValueError("Runtime State invocation.state is invalid.")
    _require_timestamp("invocation.created_at_ms", invocation["created_at_ms"])
    _require_timestamp("invocation.updated_at_ms", invocation["updated_at_ms"])
    _require_mapping("invocation.context", invocation["context"])
    _require_revision_map(
        "invocation.context_path_revisions",
        invocation["context_path_revisions"],
    )
    for name in ("runtime_event_sequence", "user_event_sequence"):
        count = invocation[name]
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise TypeError(f"Runtime State invocation.{name} must be non-negative int.")
    if not isinstance(invocation["recovery_mode"], bool):
        raise TypeError("Runtime State invocation.recovery_mode must be bool.")
    if not isinstance(invocation["cancel_requested"], bool):
        raise TypeError("Runtime State invocation.cancel_requested must be bool.")
    _require_optional_error("invocation.error", invocation["error"])
    _require_optional_error("invocation.deferred_error", invocation["deferred_error"])

    scheduler = _require_mapping("scheduler", value["scheduler"])
    _require_exact_keys("scheduler", scheduler, _SCHEDULER_KEYS)
    if not isinstance(scheduler["ready"], list) or not all(
        isinstance(item, str) and item for item in scheduler["ready"]
    ):
        raise TypeError("Runtime State scheduler.ready must be list[str].")
    for name in ("active_requests", "resolutions", "skipped", "pending_boundaries"):
        _require_string_key_mapping(f"scheduler.{name}", scheduler[name])

    node_executions = _require_string_key_mapping(
        "node_executions", value["node_executions"]
    )
    for execution_id, item in node_executions.items():
        node = _require_mapping(f"node_executions.{execution_id}", item)
        _require_exact_keys(
            f"node_executions.{execution_id}", node, _NODE_EXECUTION_KEYS
        )
        _require_uuid_string(f"node_executions.{execution_id}.id", node["id"])
        if node["id"] != execution_id:
            raise ValueError("Runtime State Node Execution key must equal its id.")
        _require_nonempty_string(
            f"node_executions.{execution_id}.node_id", node["node_id"]
        )
        if node["state"] not in _NODE_STATES:
            raise ValueError("Runtime State Node Execution state is invalid.")
        if not isinstance(node["scope"], list):
            raise TypeError("Runtime State Node Execution scope must be a list.")
        for frame in node["scope"]:
            if (
                not isinstance(frame, dict)
                or set(frame) != {"loop_region_id", "iteration"}
                or not isinstance(frame["loop_region_id"], str)
                or not isinstance(frame["iteration"], int)
                or isinstance(frame["iteration"], bool)
                or frame["iteration"] < 0
            ):
                raise TypeError("Runtime State Node Execution scope is invalid.")
        for name in ("logical_occurrence", "started_state_version"):
            count = node[name]
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise TypeError(
                    f"Runtime State node_executions.{execution_id}.{name} "
                    "must be non-negative int."
                )

    waits = _require_string_key_mapping("waits", value["waits"])
    for wait_id, item in waits.items():
        wait = _require_mapping(f"waits.{wait_id}", item)
        _require_exact_keys(f"waits.{wait_id}", wait, _WAIT_KEYS)
        _require_uuid_string(f"waits.{wait_id}.id", wait["id"])
        _require_uuid_string(
            f"waits.{wait_id}.node_execution_id", wait["node_execution_id"]
        )
        if wait["id"] != wait_id:
            raise ValueError("Runtime State Wait key must equal its id.")
        _require_mapping(f"waits.{wait_id}.request", wait["request"])

    _require_string_key_mapping("pending_advances", value["pending_advances"])

    counters = _require_mapping("counters", value["counters"])
    _require_exact_keys("counters", counters, _COUNTER_KEYS)
    for name in _COUNTER_KEYS:
        mapping = _require_string_key_mapping(f"counters.{name}", counters[name])
        if not all(
            isinstance(item, int) and not isinstance(item, bool) and item >= 0
            for item in mapping.values()
        ):
            raise TypeError(f"Runtime State counters.{name} values must be non-negative int.")


def _require_mapping(name: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"Runtime State {name} must be a mapping.")
    return value


def _require_string_key_mapping(name: str, value: Any) -> dict[str, Any]:
    mapping = _require_mapping(name, value)
    if not all(isinstance(key, str) and key for key in mapping):
        raise TypeError(f"Runtime State {name} keys must be non-empty str.")
    return mapping


def _require_exact_keys(name: str, value: dict[str, Any], expected: set[str]) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ValueError(
            f"Runtime State {name} keys do not match schema; "
            f"missing={missing}, extra={extra}."
        )


def _require_nonempty_string(name: str, value: Any) -> None:
    if not isinstance(value, str) or not value:
        raise TypeError(f"Runtime State {name} must be non-empty str.")


def _require_uuid_string(name: str, value: Any) -> None:
    _require_nonempty_string(name, value)
    try:
        UUID(value)
    except ValueError as error:
        raise ValueError(f"Runtime State {name} must be a UUID string.") from error


def _require_timestamp(name: str, value: Any) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise TypeError(f"Runtime State {name} must be non-negative int.")


def _require_revision_map(name: str, value: Any) -> None:
    mapping = _require_string_key_mapping(name, value)
    if not all(
        isinstance(item, int) and not isinstance(item, bool) and item >= 0
        for item in mapping.values()
    ):
        raise TypeError(f"Runtime State {name} values must be non-negative int.")


def _require_optional_error(name: str, value: Any) -> None:
    if value is None:
        return
    mapping = _require_mapping(name, value)
    if set(mapping) != {"type", "message"} or not all(
        isinstance(mapping[key], str) for key in ("type", "message")
    ):
        raise TypeError(
            f"Runtime State {name} must be None or {{'type': str, 'message': str}}."
        )


def _apply_copy_on_write(
    root: dict[str, Any],
    operations: tuple[StateOperation, ...],
    *,
    persistent: bool,
) -> dict[str, Any]:
    candidate = root.copy()
    cloned: set[int] = {id(candidate)}
    for operation in operations:
        if not operation.path:
            raise ValueError("StateOperation path cannot be empty.")
        parent, token = _copy_parent(candidate, operation.path, cloned)
        value = operation._persistent_value if persistent else operation.value
        if operation.op == "add":
            _add(parent, token, copy.deepcopy(value) if persistent else value)
        elif operation.op == "replace":
            _replace(parent, token, copy.deepcopy(value) if persistent else value)
        else:
            _remove(parent, token)
    return candidate


def _copy_parent(
    root: dict[str, Any], path: tuple[PathToken, ...], cloned: set[int]
) -> tuple[dict[str, Any] | list[Any], PathToken]:
    current: dict[str, Any] | list[Any] = root
    for token in path[:-1]:
        child = _child(current, token)
        if not isinstance(child, (dict, list)):
            raise TypeError(f"StateOperation traverses non-container at {token!r}.")
        if id(child) not in cloned:
            child_copy = child.copy()
            _assign_existing(current, token, child_copy)
            cloned.add(id(child_copy))
            child = child_copy
        current = child
    return current, path[-1]


def _child(parent: dict[str, Any] | list[Any], token: PathToken) -> Any:
    if isinstance(parent, dict):
        if not isinstance(token, str) or token not in parent:
            raise KeyError(token)
        return parent[token]
    index = _list_index(token, len(parent), allow_end=False)
    return parent[index]


def _assign_existing(
    parent: dict[str, Any] | list[Any], token: PathToken, value: Any
) -> None:
    if isinstance(parent, dict):
        if not isinstance(token, str) or token not in parent:
            raise KeyError(token)
        parent[token] = value
        return
    parent[_list_index(token, len(parent), allow_end=False)] = value


def _add(parent: dict[str, Any] | list[Any], token: PathToken, value: Any) -> None:
    if isinstance(parent, dict):
        if not isinstance(token, str):
            raise TypeError("Mapping StateOperation token must be str.")
        if token in parent:
            raise KeyError(f"StateOperation add target already exists: {token!r}.")
        parent[token] = value
        return
    if token == "-":
        parent.append(value)
        return
    parent.insert(_list_index(token, len(parent), allow_end=True), value)


def _replace(parent: dict[str, Any] | list[Any], token: PathToken, value: Any) -> None:
    _assign_existing(parent, token, value)


def _remove(parent: dict[str, Any] | list[Any], token: PathToken) -> None:
    if isinstance(parent, dict):
        if not isinstance(token, str) or token not in parent:
            raise KeyError(token)
        del parent[token]
        return
    del parent[_list_index(token, len(parent), allow_end=False)]


def _list_index(token: PathToken, size: int, *, allow_end: bool) -> int:
    if not isinstance(token, int):
        raise TypeError("List StateOperation token must be int or '-' for add.")
    upper = size if allow_end else size - 1
    if token < 0 or token > upper:
        raise IndexError(token)
    return token


def _read_path(root: Any, path: Sequence[PathToken]) -> Any:
    current = root
    for token in path:
        current = _child(current, token)
    return current
