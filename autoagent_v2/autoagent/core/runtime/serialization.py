"""Small JSON-boundary normalizer for Events and Checkpoints."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from importlib import import_module
import math
from typing import Any
from uuid import UUID


class RuntimeSerializationError(TypeError):
    pass


def json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RuntimeSerializationError("Runtime float must be finite.")
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return json_value(value.value)
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return json_value(value.model_dump(mode="json"))
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: json_value(getattr(value, field.name)) for field in fields(value)
        }
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise RuntimeSerializationError("Runtime mapping keys must be str.")
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [json_value(item) for item in sorted(value, key=repr)]
    raise RuntimeSerializationError(
        f"Runtime value of type {type(value).__name__!r} is not JSON serializable."
    )


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
