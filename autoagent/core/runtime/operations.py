"""Fine-grained, durable Runtime State changes.

A State Operation is the smallest replayable mutation.  A Delta is the atomic
commit boundary: either every operation is applied in order, or none is.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass, replace
from functools import lru_cache
from types import MappingProxyType
from typing import Literal, TypeAlias

from ..errors import RuntimeTransitionError
from .values import freeze
from ._chunked import ChunkedUnits, ChunkedMap, MapEdit, runtime_mapping


PathToken: TypeAlias = str | int
OperationKind: TypeAlias = Literal["add", "replace", "remove"]


@dataclass(frozen=True, slots=True)
class StateOperation:
    """One typed State change; record conversion is an explicit codec boundary."""

    op: OperationKind
    path: tuple[PathToken, ...]
    value: object = None

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
            object.__setattr__(self, "value", _decode_operation_value(self.path, self.value))

    @classmethod
    def _from_owned(
        cls, op: OperationKind, path: tuple[PathToken, ...], value: object = None,
    ) -> "StateOperation":
        """Planner-only construction from validated Core-owned immutable objects."""
        operation = object.__new__(cls)
        object.__setattr__(operation, "op", op)
        object.__setattr__(operation, "path", path)
        object.__setattr__(operation, "value", value)
        return operation

    def to_record(self) -> dict[str, object]:
        record: dict[str, object] = {"op": self.op, "path": list(self.path)}
        if self.op != "remove":
            record["value"] = _encode_runtime(self.value)
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
    if isinstance(value, (tuple, list, ChunkedUnits)):
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


def _decode_operation_value(path: tuple[PathToken, ...], value: object) -> object:
    """Decode external operations once; never called by live Planner/Reducer."""
    from types import MappingProxyType
    from . import state as model
    from .events import _patch_from_record, EdgeConditionResult

    decoders = {
        ("session",): model._session_from_record,
        ("invocation",): model._invocation_from_record,
    }
    collections = {
        "occurrences": model._occurrence_from_record,
        "operator_calls": model._call_from_record,
        "waits": model._wait_from_record,
        "resolutions": model._resolution_from_record,
        "boundary_resolutions": model._boundary_from_record,
        "child_plans": model._child_plan_from_record,
    }
    decoder = decoders.get(path)
    if len(path) == 4 and path[:2] == ("invocation", "scheduler") and path[-2] in collections:
        decoder = collections[path[-2]]
    elif len(path) == 3 and path[:2] == ("invocation", "child_plans"):
        decoder = collections["child_plans"]
    # These are domain fields only at their schema paths, not user data keys.
    execution = len(path) >= 5 and path[:3] == ("invocation", "scheduler", "occurrences")
    call = len(path) == 5 and path[:3] == ("invocation", "scheduler", "operator_calls")
    if execution and len(path) == 5 and path[-1] == "execution":
        decoder = model._execution_from_record
    if path == ("invocation", "error") or ((execution or call) and len(path) == 5 and path[-1] == "error"):
        decoder = model._error_from_record
    if execution and len(path) == 6 and path[-2] == "execution":
        if path[-1] == "fault":
            decoder = model._error_from_record
        elif path[-1] == "pending_context_patch":
            decoder = _patch_from_record
        elif path[-1] == "routing":
            return tuple(EdgeConditionResult(**item) for item in _encode_runtime(value))
    if path in (("session", "context_path_revisions"), ("invocation", "context_path_revisions")):
        record = _encode_runtime(value)
        return MappingProxyType({tuple(part.replace("~1", "/").replace("~0", "~") for part in key[1:].split("/")): revision for key, revision in record.items()})
    if decoder is not None:
        return decoder(_encode_runtime(value))
    return freeze(value)


@lru_cache(maxsize=64)
def _field_names(model):
    return tuple(item.name for item in fields(model))


class _PathEdit:
    """Private, unpublished transaction workspace for one changed ancestor.

    Operations execute in their original order. Replacing/removing a parent
    discards previous edits below it; later descendants see the replacement.
    No mutable workspace is ever installed into State or into an Operation.
    """
    __slots__ = ("original", "kind", "values", "dirty")

    def __init__(self, original: object) -> None:
        self.original = original
        self.dirty = set()
        if is_dataclass(original):
            self.kind = "object"
            self.values = {name: getattr(original, name) for name in _field_names(type(original))}
        elif isinstance(original, Mapping):
            self.kind = "mapping"
            self.values = MapEdit(original) if isinstance(original, ChunkedMap) or len(original) >= 512 else dict(original)
        elif isinstance(original, (tuple, ChunkedUnits)):
            self.kind = "tuple"
            self.values = list(original)
        else:
            raise RuntimeTransitionError("STATE_PATH_INVALID", "Path does not match Runtime State.")

    def apply(self, path: tuple[PathToken, ...], operation: StateOperation) -> None:
        token, *rest = path
        if self.kind == "tuple":
            if not isinstance(token, int) or isinstance(token, bool) or token < 0:
                raise RuntimeTransitionError("STATE_PATH_INVALID", "Tuple path requires a non-negative index.")
            exists = token < len(self.values)
        else:
            if not isinstance(token, str):
                raise RuntimeTransitionError("STATE_PATH_INVALID", "Object path requires a string.")
            exists = token in self.values
        if rest:
            if not exists:
                raise RuntimeTransitionError("STATE_PATH_MISSING", "State path is missing.")
            child = self.values[token]
            if not isinstance(child, _PathEdit):
                child = _PathEdit(child)
                self.values[token] = child
                self.dirty.add(token)
            child.apply(tuple(rest), operation)
            return
        if self.kind == "object":
            if not exists or operation.op != "replace":
                raise RuntimeTransitionError("STATE_PATH_INVALID", "Typed fields must be replaced.")
        elif operation.op != "add" and not exists:
            raise RuntimeTransitionError("STATE_PATH_MISSING", "State path is missing.")
        elif self.kind == "mapping" and operation.op == "add" and exists:
            raise RuntimeTransitionError("STATE_PATH_EXISTS", "State path already exists.")
        if self.kind == "tuple" and operation.op == "add":
            if token > len(self.values):
                raise RuntimeTransitionError("STATE_PATH_MISSING", "Tuple index is missing.")
            self.values.insert(token, operation.value)
        elif operation.op == "remove":
            if self.kind == "tuple":
                del self.values[token]
            else:
                del self.values[token]
                self.dirty.discard(token)
        else:
            self.values[token] = operation.value
            self.dirty.add(token)

    def finish(self) -> object:
        if self.kind == "tuple":
            return tuple(value.finish() if isinstance(value, _PathEdit) else value for value in self.values)
        for key in self.dirty:
            value = self.values[key]
            if isinstance(value, _PathEdit):
                self.values[key] = value.finish()
        if self.kind == "object":
            return replace(self.original, **{key: self.values[key] for key in self.dirty})
        return self.values.finish() if isinstance(self.values, MapEdit) else runtime_mapping(self.values)


def apply_runtime_delta(state, delta: StateDelta):
    """Apply ordered typed operations, copying each changed ancestor once."""
    if not delta.operations:
        return state
    candidate = _PathEdit(state)
    for operation in delta.operations:
        if operation.path[0] not in {"session", "invocation"}:
            raise RuntimeTransitionError("STATE_EVENT_METADATA_MUTATION", "Delta cannot mutate Event metadata.")
        candidate.apply(operation.path, operation)
    return candidate.finish()
