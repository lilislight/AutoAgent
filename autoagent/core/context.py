"""Context mutation primitives shared by Workflow hooks and Runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, TypeAlias

from .errors import RuntimeTransitionError


DurableValue: TypeAlias = object


@dataclass(frozen=True, slots=True)
class ContextOperation:
    operation: Literal["set", "delete"]
    path: tuple[str, ...]
    value: object | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.operation, str) or self.operation not in {
            "set",
            "delete",
        }:
            raise ValueError(f"Unknown Context operation {self.operation!r}.")
        if not isinstance(self.path, tuple) or not self.path or any(
            not isinstance(part, str) or not part.strip() for part in self.path
        ):
            raise ValueError("Context path cannot be empty.")
        if self.operation == "delete" and self.value is not None:
            raise ValueError("Delete Context operation cannot carry a value.")

    @classmethod
    def set(
        cls, path: str | tuple[str, ...], value: object
    ) -> "ContextOperation":
        return cls("set", _path(path), value)

    @classmethod
    def delete(cls, path: str | tuple[str, ...]) -> "ContextOperation":
        return cls("delete", _path(path))


@dataclass(frozen=True, slots=True)
class ContextPatch:
    invocation: tuple[ContextOperation, ...] = ()
    session: tuple[ContextOperation, ...] = ()

    def __post_init__(self) -> None:
        for name, operations in (
            ("invocation", self.invocation),
            ("session", self.session),
        ):
            if not isinstance(operations, tuple) or not all(
                isinstance(item, ContextOperation) for item in operations
            ):
                raise TypeError(f"ContextPatch {name} must contain ContextOperation values.")


def apply_context_operation(
    context: DurableValue, operation: ContextOperation
) -> DurableValue:
    """Apply one operation with immutable path-copy semantics."""

    if not isinstance(context, Mapping):
        raise RuntimeTransitionError(
            "CONTEXT_NOT_MAPPING", "Runtime Context must be a mapping."
        )
    return _apply_mapping(context, operation.path, operation)


def _apply_mapping(
    context: Mapping[str, DurableValue],
    path: tuple[str, ...],
    operation: ContextOperation,
) -> DurableValue:
    updated = dict(context)
    key = path[0]
    if len(path) == 1:
        if operation.operation == "set":
            from .runtime.values import freeze

            updated[key] = freeze(operation.value)
        else:
            updated.pop(key, None)
        return MappingProxyType(updated)
    child = context.get(key)
    if child is None:
        child = MappingProxyType({})
    if not isinstance(child, Mapping):
        raise RuntimeTransitionError(
            "CONTEXT_PATH_NOT_MAPPING",
            f"Context path component {key!r} is not a mapping.",
        )
    updated[key] = _apply_mapping(child, path[1:], operation)
    return MappingProxyType(updated)


def _path(value: str | tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(value, str):
        result = tuple(value.split("."))
    elif isinstance(value, tuple):
        result = value
    else:
        raise TypeError("Context path must be a dotted string or tuple of strings.")
    if not result or any(
        not isinstance(part, str) or not part.strip() for part in result
    ):
        raise ValueError("Context path cannot be empty.")
    return result


__all__ = ["ContextOperation", "ContextPatch", "apply_context_operation"]


class _ContextEdit:
    """Private batch workspace; each modified ancestor is materialized once."""
    def __init__(self, context):
        from .runtime._chunked import MapEdit
        if not isinstance(context, Mapping):
            raise RuntimeTransitionError('CONTEXT_NOT_MAPPING', 'Runtime Context must be a mapping.')
        self.values = MapEdit(context)
        self.children = {}

    def apply(self, operation):
        from .runtime.values import freeze
        current = self
        for key in operation.path[:-1]:
            child = current.children.get(key)
            if child is None:
                value = current.values.get(key)
                if value is None:
                    value = MappingProxyType({})
                if not isinstance(value, Mapping):
                    raise RuntimeTransitionError('CONTEXT_PATH_NOT_MAPPING', f'Context path component {key!r} is not a mapping.')
                child = _ContextEdit(value)
                current.children[key] = child
            current = child
        key = operation.path[-1]
        current.children.pop(key, None)
        if operation.operation == 'set':
            current.values[key] = freeze(operation.value)
        else:
            current.values.pop(key, None)

    def finish(self):
        for key, child in self.children.items():
            self.values[key] = child.finish()
        return self.values.finish()
