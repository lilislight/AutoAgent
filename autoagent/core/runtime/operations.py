"""Fine-grained, durable Runtime State changes.

A State Operation is the smallest replayable mutation.  A Delta is the atomic
commit boundary: either every operation is applied in order, or none is.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from typing import Literal, TypeAlias

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
class StateDelta:
    """The ordered, atomic mutations of one Runtime Event."""

    operations: tuple[StateOperation, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.operations, tuple) or not all(
            isinstance(item, StateOperation) for item in self.operations
        ):
            raise TypeError("StateDelta operations must be a tuple of StateOperation.")

    def to_record(self) -> dict[str, object]:
        return {"operations": [item.to_record() for item in self.operations]}

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "StateDelta":
        if not isinstance(record, dict) or set(record) != {"operations"}:
            raise TypeError("Invalid StateDelta record.")
        if not isinstance(record["operations"], list):
            raise TypeError("StateDelta operations must be a list.")
        return cls(tuple(StateOperation.from_record(item) for item in record["operations"]))


def apply_state_delta(
    record: Mapping[str, object], batch: StateDelta
) -> dict[str, object]:
    """Apply a batch with path copy-on-write.

    The input record is treated as an immutable snapshot.  Every operation
    clones only the containers on its path, while operation values are thawed
    into newly owned containers.  This keeps a failed batch atomic without the
    previous full-tree ``freeze``/``thaw`` clone.
    """

    candidate: object = record if isinstance(record, dict) else dict(record)
    for operation in batch.operations:
        candidate = _apply_operation(candidate, operation)
    if not isinstance(candidate, dict):
        raise RuntimeTransitionError(
            "STATE_ROOT_INVALID", "Runtime State root must remain a mapping."
        )
    return candidate


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


def apply_runtime_delta(state, delta: StateDelta):
    """Copy only the typed containers touched by explicit operations."""
    from dataclasses import replace
    from types import MappingProxyType
    from . import state as model

    decoders = {
        ("session",): model._session_from_record,
        ("invocation",): model._invocation_from_record,
    }
    collection_decoders = {
        "occurrences": model._occurrence_from_record,
        "operator_calls": model._call_from_record,
        "waits": model._wait_from_record,
        "resolutions": model._resolution_from_record,
        "boundary_resolutions": model._boundary_from_record,
        "child_plans": model._child_plan_from_record,
    }
    def decode(path, value):
        record = thaw(value)
        if path in decoders:
            return decoders[path](record)
        if len(path) >= 2 and path[-2] in collection_decoders:
            return collection_decoders[path[-2]](record)
        if path[-1] == "execution":
            return model._execution_from_record(record)
        if path[-1] == "error":
            return model._error_from_record(record)
        if path[-1] == "context_path_revisions":
            return MappingProxyType({tuple(part.replace("~1", "/").replace("~0", "~") for part in key[1:].split("/")): revision for key, revision in record.items()})
        return freeze(record)
    def update(container, path, operation):
        token = path[0]
        if is_dataclass(container):
            if not isinstance(token, str) or token not in {item.name for item in fields(container)}:
                raise RuntimeTransitionError("STATE_PATH_INVALID", "Unknown Runtime State field.")
            if len(path) == 1:
                if operation.op != "replace":
                    raise RuntimeTransitionError("STATE_PATH_INVALID", "Typed fields must be replaced.")
                value = decode(operation.path, operation.value)
            else:
                value = update(getattr(container, token), path[1:], operation)
            return replace(container, **{token: value})
        if isinstance(container, Mapping):
            if not isinstance(token, str):
                raise RuntimeTransitionError("STATE_PATH_INVALID", "Mapping path requires a string.")
            exists = token in container
            if (len(path) > 1 or operation.op != "add") and not exists:
                raise RuntimeTransitionError("STATE_PATH_MISSING", "State path is missing.")
            if len(path) == 1 and operation.op == "add" and exists:
                raise RuntimeTransitionError("STATE_PATH_EXISTS", "State path already exists.")
            result = dict(container)
            if len(path) > 1:
                result[token] = update(container[token], path[1:], operation)
            elif operation.op == "remove":
                del result[token]
            else:
                result[token] = decode(operation.path, operation.value)
            return MappingProxyType(result)
        if isinstance(container, tuple) and isinstance(token, int):
            result = list(container)
            if len(path) > 1:
                result[token] = update(container[token], path[1:], operation)
            elif operation.op == "add":
                if token > len(result):
                    raise RuntimeTransitionError("STATE_PATH_MISSING", "Tuple index is missing.")
                result.insert(token, decode(operation.path, operation.value))
            elif operation.op == "remove":
                del result[token]
            else:
                result[token] = decode(operation.path, operation.value)
            return tuple(result)
        raise RuntimeTransitionError("STATE_PATH_INVALID", "Path does not match Runtime State.")
    candidate = state
    for operation in delta.operations:
        if operation.path[0] not in {"session", "invocation"}:
            raise RuntimeTransitionError("STATE_EVENT_METADATA_MUTATION", "Delta cannot mutate Event metadata.")
        candidate = update(candidate, operation.path, operation)
    return candidate
