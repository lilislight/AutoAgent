"""Immutable JSON-compatible values shared by Events and Runtime State."""

from __future__ import annotations

from collections.abc import Mapping
import math
from types import MappingProxyType


DurableValue = (
    None
    | bool
    | int
    | float
    | str
    | tuple["DurableValue", ...]
    | Mapping[str, "DurableValue"]
)


def freeze(value: object) -> DurableValue:
    return _freeze(value, set())


def _freeze(value: object, active: set[int]) -> DurableValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, float) and not math.isfinite(value):
            raise TypeError("Durable floats must be finite.")
        return value
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active:
            raise TypeError("Durable runtime values cannot contain cycles.")
        active.add(identity)
        frozen: dict[str, DurableValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("Durable mappings require string keys.")
            frozen[key] = _freeze(item, active)
        active.remove(identity)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in active:
            raise TypeError("Durable runtime values cannot contain cycles.")
        active.add(identity)
        frozen = tuple(_freeze(item, active) for item in value)
        active.remove(identity)
        return frozen
    raise TypeError(f"Unsupported durable runtime value: {type(value).__name__}.")


def thaw(value: DurableValue) -> object:
    if isinstance(value, Mapping):
        return {key: thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw(item) for item in value]
    return value
