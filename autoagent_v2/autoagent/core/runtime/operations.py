"""Fine-grained, durable Runtime State changes.

A State Operation is the smallest replayable mutation.  A batch is the atomic
commit boundary: either every operation is applied in order, or none is.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Literal, TypeAlias
from uuid import uuid4

from ..errors import RuntimeTransitionError
from .values import DurableValue, freeze, thaw


PathToken: TypeAlias = str | int
OperationKind: TypeAlias = Literal["add", "replace", "remove"]


@dataclass(frozen=True, slots=True)
class StateOperation:
    """One ordered change against the canonical Runtime State record."""

    op: OperationKind
    path: tuple[PathToken, ...]
    value: DurableValue = None

    def __post_init__(self) -> None:
        if self.op not in {"add", "replace", "remove"}:
            raise ValueError(f"Unsupported State Operation {self.op!r}.")
        if not self.path:
            raise ValueError("State Operation path cannot be empty.")
        for token in self.path:
            if not isinstance(token, (str, int)) or isinstance(token, bool):
                raise TypeError("State Operation path tokens must be str or int.")
            if isinstance(token, int) and token < 0:
                raise ValueError("State Operation list index cannot be negative.")
        if self.op == "remove":
            if self.value is not None:
                raise ValueError("Remove State Operation cannot carry a value.")
        else:
            object.__setattr__(self, "value", freeze(self.value))

    def to_record(self) -> dict[str, object]:
        record: dict[str, object] = {"op": self.op, "path": list(self.path)}
        if self.op != "remove":
            record["value"] = thaw(self.value)
        return record

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "StateOperation":
        if not isinstance(record, dict):
            raise TypeError("State Operation record must be a mapping.")
        op = record.get("op")
        if op not in {"add", "replace", "remove"}:
            raise ValueError(f"Unsupported State Operation {op!r}.")
        path = record.get("path")
        if not isinstance(path, list):
            raise TypeError("State Operation path must be a list.")
        return cls(
            op,  # type: ignore[arg-type]
            tuple(path),
            record.get("value") if op != "remove" else None,
        )


@dataclass(frozen=True, slots=True)
class StateOperationBatch:
    """One atomically applied group with an exact state-version interval."""

    from_state_version: int
    to_state_version: int
    operations: tuple[StateOperation, ...]
    occurred_at_ns: int
    id: str = field(default_factory=lambda: str(uuid4()))

    def __post_init__(self) -> None:
        for name, value in (
            ("from_state_version", self.from_state_version),
            ("to_state_version", self.to_state_version),
            ("occurred_at_ns", self.occurred_at_ns),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"State Operation Batch {name} must be non-negative.")
        if self.to_state_version != self.from_state_version + 1:
            raise ValueError("A State Operation Batch must advance exactly one version.")
        if not self.operations:
            raise ValueError("A State Operation Batch cannot be empty.")
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("State Operation Batch id cannot be empty.")

    def to_record(self) -> dict[str, object]:
        return {
            "id": self.id,
            "from_state_version": self.from_state_version,
            "to_state_version": self.to_state_version,
            "occurred_at_ns": self.occurred_at_ns,
            "operations": [item.to_record() for item in self.operations],
        }

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "StateOperationBatch":
        if not isinstance(record, dict):
            raise TypeError("State Operation Batch record must be a mapping.")
        operations = record.get("operations")
        if not isinstance(operations, list):
            raise TypeError("State Operation Batch operations must be a list.")
        return cls(
            from_state_version=_integer(record, "from_state_version"),
            to_state_version=_integer(record, "to_state_version"),
            operations=tuple(StateOperation.from_record(item) for item in operations),
            occurred_at_ns=_integer(record, "occurred_at_ns"),
            id=_string(record, "id"),
        )


def diff_runtime_states(before: object, after: object) -> tuple[StateOperation, ...]:
    """Identity-aware diff for immutable typed Runtime State objects.

    Runtime transitions preserve object identity for unchanged branches. This
    avoids serializing and comparing the complete State on every hot-path
    commit while still producing operations against the canonical record.
    """

    operations: list[StateOperation] = []
    _diff_runtime_value(operations, (), before, after)
    return tuple(operations)


def apply_operation_batch(
    record: Mapping[str, object], batch: StateOperationBatch
) -> dict[str, object]:
    """Apply a batch copy-on-write and reject partial or invalid changes."""

    candidate: object = thaw(freeze(record))
    for operation in batch.operations:
        candidate = _apply_operation(candidate, operation)
    if not isinstance(candidate, dict):
        raise RuntimeTransitionError(
            "STATE_ROOT_INVALID", "Runtime State root must remain a mapping."
        )
    return candidate


def _diff_runtime_value(
    operations: list[StateOperation],
    path: tuple[PathToken, ...],
    before: object,
    after: object,
) -> None:
    if before is after or before == after:
        return
    if type(before) is type(after) and is_dataclass(before):
        # Optional RuntimeErrorInfo fields are omitted from its canonical
        # record. Replacing the compact object avoids add/replace ambiguity.
        if type(before).__name__ == "RuntimeErrorInfo":
            operations.append(StateOperation("replace", path, _encode_runtime(after)))
            return
        for item in fields(before):
            if not item.compare and item.name.startswith("_"):
                continue
            if not path and item.name in {
                "state_version",
                "sequence",
                "last_event_id",
                "last_event_digest",
                "last_event_semantic_digest",
            }:
                continue
            _diff_runtime_value(
                operations,
                (*path, item.name),
                getattr(before, item.name),
                getattr(after, item.name),
            )
        return
    if isinstance(before, Mapping) and isinstance(after, Mapping):
        before_items = _runtime_mapping(before)
        after_items = _runtime_mapping(after)
        before_keys = set(before_items)
        after_keys = set(after_items)
        for key in sorted(before_keys - after_keys):
            operations.append(StateOperation("remove", (*path, key)))
        for key in sorted(after_keys - before_keys):
            operations.append(
                StateOperation("add", (*path, key), _encode_runtime(after_items[key]))
            )
        for key in sorted(before_keys & after_keys):
            _diff_runtime_value(
                operations,
                (*path, key),
                before_items[key],
                after_items[key],
            )
        return
    operations.append(StateOperation("replace", path, _encode_runtime(after)))


def _runtime_mapping(value: Mapping[object, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, item in value.items():
        if isinstance(key, str):
            encoded = key
        elif isinstance(key, tuple) and all(isinstance(part, str) for part in key):
            if not key:
                raise ValueError("Runtime State path key cannot be empty.")
            encoded = "/" + "/".join(
                part.replace("~", "~0").replace("/", "~1")
                for part in key
            )
        else:
            raise TypeError("Runtime State mappings require string or string-path keys.")
        result[encoded] = item
    return result


def _encode_runtime(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {
            key: _encode_runtime(item)
            for key, item in _runtime_mapping(value).items()
        }
    if isinstance(value, (tuple, list)):
        return [_encode_runtime(item) for item in value]
    if is_dataclass(value):
        record: dict[str, object] = {}
        for item in fields(value):
            if not item.compare and item.name.startswith("_"):
                continue
            encoded = _encode_runtime(getattr(value, item.name))
            if type(value).__name__ == "RuntimeErrorInfo" and encoded is None:
                continue
            record[item.name] = encoded
        return record
    raise TypeError(f"Unsupported Runtime State value {type(value).__name__}.")


def _apply_operation(root: object, operation: StateOperation) -> object:
    if len(operation.path) == 1:
        parent = root
        token = operation.path[0]
        return _updated_container(parent, token, operation)
    parent_path = operation.path[:-1]
    parent = _read(root, parent_path)
    updated = _updated_container(parent, operation.path[-1], operation)
    return _replace_path(root, parent_path, updated)


def _read(root: object, path: Sequence[PathToken]) -> object:
    current = root
    for token in path:
        if isinstance(current, dict) and isinstance(token, str):
            if token not in current:
                raise RuntimeTransitionError(
                    "STATE_PATH_MISSING", f"State path token {token!r} does not exist."
                )
            current = current[token]
        elif isinstance(current, list) and isinstance(token, int):
            if token >= len(current):
                raise RuntimeTransitionError(
                    "STATE_PATH_MISSING", f"State list index {token} does not exist."
                )
            current = current[token]
        else:
            raise RuntimeTransitionError(
                "STATE_PATH_INVALID", "State Operation path does not match its container."
            )
    return current


def _replace_path(root: object, path: Sequence[PathToken], value: object) -> object:
    if not path:
        return value
    token = path[0]
    if isinstance(root, dict) and isinstance(token, str):
        if token not in root:
            raise RuntimeTransitionError("STATE_PATH_MISSING", f"Missing {token!r}.")
        clone = dict(root)
        clone[token] = _replace_path(root[token], path[1:], value)
        return clone
    if isinstance(root, list) and isinstance(token, int) and token < len(root):
        clone = list(root)
        clone[token] = _replace_path(root[token], path[1:], value)
        return clone
    raise RuntimeTransitionError(
        "STATE_PATH_INVALID", "State Operation path does not match its container."
    )


def _updated_container(
    parent: object, token: PathToken, operation: StateOperation
) -> object:
    value = thaw(operation.value)
    if isinstance(parent, dict) and isinstance(token, str):
        exists = token in parent
        if operation.op == "add" and exists:
            raise RuntimeTransitionError("STATE_PATH_EXISTS", f"Path {token!r} exists.")
        if operation.op in {"replace", "remove"} and not exists:
            raise RuntimeTransitionError("STATE_PATH_MISSING", f"Path {token!r} is missing.")
        clone = dict(parent)
        if operation.op == "remove":
            del clone[token]
        else:
            clone[token] = value
        return clone
    if isinstance(parent, list) and isinstance(token, int):
        clone = list(parent)
        if operation.op == "add":
            if token > len(clone):
                raise RuntimeTransitionError("STATE_PATH_MISSING", "List index is missing.")
            clone.insert(token, value)
        elif token >= len(clone):
            raise RuntimeTransitionError("STATE_PATH_MISSING", "List index is missing.")
        elif operation.op == "remove":
            del clone[token]
        else:
            clone[token] = value
        return clone
    raise RuntimeTransitionError(
        "STATE_PATH_INVALID", "State Operation target is not a compatible container."
    )


def _integer(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{key} must be an integer.")
    return value


def _string(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise TypeError(f"{key} must be a non-empty string.")
    return value
