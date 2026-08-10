"""Runtime value ownership and JSON persistence boundaries.

``deepcopy`` and JSON round-tripping solve different problems here:

* ``isolate`` creates an in-process Python value that user code may mutate
  without changing Runtime-owned state.
* ``encode``/``decode`` cross the persistence boundary used by Events,
  Checkpoints, Session snapshots, and later process recovery.

JSON decoding is deliberately never used to create Hook inputs.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from importlib import import_module
import json
import math
from typing import Any
from uuid import UUID


class RuntimeSerializationError(TypeError):
    pass


@dataclass(frozen=True, slots=True)
class CapturedRuntimeValue:
    """One detached Python value plus its reusable persistent representation."""

    _owned: Any
    _encoded: Any

    def clone_for_user(self) -> Any:
        """Return an isolated Python object for a user-defined function."""

        return copy.deepcopy(self._owned)

    def transfer_to_runtime(self) -> Any:
        """Transfer the already-detached value to its single Runtime owner.

        The capture object must not be reused as a mutable owner after this
        internal operation.  Its persistent JSON tree remains reusable.
        """

        return self._owned

    def persistent_value(self) -> Any:
        """Return the immutable-by-contract JSON tree without re-encoding Python."""

        return self._encoded


class RuntimeValueCodec:
    """Shared bottom layer for isolation and durable JSON conversion."""

    @staticmethod
    def isolate(value: Any) -> Any:
        """Detach a Python value for in-process user-code isolation only."""

        return copy.deepcopy(value)

    @staticmethod
    def capture(value: Any) -> CapturedRuntimeValue:
        """Detach once, then encode that detached value once for persistence."""

        owned = copy.deepcopy(value)
        return CapturedRuntimeValue(owned, encode_runtime_value(owned))

    @staticmethod
    def encode(value: Any) -> Any:
        if isinstance(value, CapturedRuntimeValue):
            return value.persistent_value()
        return encode_runtime_value(value)

    @staticmethod
    def decode(value: Any) -> Any:
        return decode_runtime_value(value)


def encode_json_record(value: Mapping[str, Any]) -> bytes:
    """Encode one already-normalized immutable handoff record."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise RuntimeSerializationError("Runtime record is not JSON serializable.") from error


def decode_json_record(value: bytes) -> dict[str, Any]:
    """Decode one immutable handoff record into a fresh mapping."""

    try:
        decoded = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeSerializationError("Runtime record is not valid UTF-8 JSON.") from error
    if not isinstance(decoded, dict):
        raise RuntimeSerializationError("Runtime record must decode to a mapping.")
    return decoded


_TYPE = "__autoagent_runtime_type__"


def encode_runtime_value(value: Any) -> Any:
    """Encode a Checkpoint value while retaining reconstructable Python types."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RuntimeSerializationError("Checkpoint float must be finite.")
        return value
    if isinstance(value, UUID):
        return {_TYPE: "uuid", "value": str(value)}
    if isinstance(value, Enum):
        return {
            _TYPE: "enum",
            "class": _type_name(type(value)),
            "value": encode_runtime_value(value.value),
        }
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return {
            _TYPE: "pydantic",
            "class": _type_name(type(value)),
            "value": encode_runtime_value(value.model_dump(mode="python")),
        }
    if is_dataclass(value) and not isinstance(value, type):
        return {
            _TYPE: "dataclass",
            "class": _type_name(type(value)),
            "value": {
                field.name: encode_runtime_value(getattr(value, field.name))
                for field in fields(value)
            },
        }
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise RuntimeSerializationError("Checkpoint mapping keys must be str.")
        if _TYPE in value:
            raise RuntimeSerializationError(
                f"Checkpoint mapping key {_TYPE!r} is reserved."
            )
        return {key: encode_runtime_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode_runtime_value(item) for item in value]
    if isinstance(value, tuple):
        return {_TYPE: "tuple", "items": [encode_runtime_value(item) for item in value]}
    if isinstance(value, (set, frozenset)):
        return {
            _TYPE: "frozenset" if isinstance(value, frozenset) else "set",
            "items": [encode_runtime_value(item) for item in sorted(value, key=repr)],
        }
    raise RuntimeSerializationError(
        f"Checkpoint value of type {type(value).__name__!r} cannot be reconstructed."
    )


def decode_runtime_value(value: Any) -> Any:
    if isinstance(value, list):
        return [decode_runtime_value(item) for item in value]
    if not isinstance(value, Mapping):
        return value
    kind = value.get(_TYPE)
    if kind is None:
        return {str(key): decode_runtime_value(item) for key, item in value.items()}
    if kind == "uuid":
        return UUID(str(value["value"]))
    if kind == "tuple":
        return tuple(decode_runtime_value(item) for item in value["items"])
    if kind in {"set", "frozenset"}:
        items = (decode_runtime_value(item) for item in value["items"])
        return frozenset(items) if kind == "frozenset" else set(items)
    target = _resolve_type(str(value["class"]))
    decoded = decode_runtime_value(value["value"])
    if kind == "enum":
        return target(decoded)
    if kind == "dataclass":
        return target(**decoded)
    if kind == "pydantic":
        validator = getattr(target, "model_validate", None)
        if not callable(validator):
            raise RuntimeSerializationError(
                f"Checkpoint type {target!r} no longer provides model_validate()."
            )
        return validator(decoded)
    raise RuntimeSerializationError(f"Unknown Checkpoint runtime type marker: {kind!r}")


def _type_name(value: type[Any]) -> str:
    if "<locals>" in value.__qualname__ or value.__module__ == "__main__":
        raise RuntimeSerializationError(
            f"Local type {value.__qualname__!r} cannot be restored across processes."
        )
    return f"{value.__module__}:{value.__qualname__}"


def _resolve_type(value: str) -> type[Any]:
    try:
        module_name, qualname = value.split(":", 1)
        target: Any = import_module(module_name)
        for part in qualname.split("."):
            target = getattr(target, part)
    except (ImportError, AttributeError, ValueError) as error:
        raise RuntimeSerializationError(
            f"Checkpoint runtime type {value!r} cannot be imported."
        ) from error
    if not isinstance(target, type):
        raise RuntimeSerializationError(
            f"Checkpoint runtime type {value!r} does not resolve to a class."
        )
    return target
