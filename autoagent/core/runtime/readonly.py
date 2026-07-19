from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from types import MappingProxyType
from typing import Any


def to_read_only(value: Any) -> Any:
    """Return an isolated, recursively read-only view of structured user data.

    Standard mutable containers are converted to immutable equivalents. Unknown
    application objects are deep-copied so mutations cannot alter runtime-owned
    records even when the object's type has no generic immutable representation.
    """

    if isinstance(value, Mapping):
        return MappingProxyType(
            {deepcopy(key): to_read_only(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(to_read_only(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(to_read_only(item) for item in value)
    if isinstance(value, bytearray):
        return bytes(value)
    return deepcopy(value)


def to_mutable_record(value: Any) -> Any:
    """Convert read-only hook values back to persistence-friendly containers."""

    if isinstance(value, Mapping):
        return {
            deepcopy(key): to_mutable_record(item)
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return [to_mutable_record(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [to_mutable_record(item) for item in value]
    return deepcopy(value)
