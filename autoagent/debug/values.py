from __future__ import annotations

from collections.abc import Mapping, Sequence
from hashlib import sha256
import json
from typing import Any

from pydantic import BaseModel

from autoagent.core.runtime import ArtifactRef
from autoagent.debug.models import ValueSummary


_SENSITIVE_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "cookie",
        "password",
        "private_key",
        "secret",
        "token",
    }
)
_REDACTED = "[REDACTED]"


def summarize_value(
    value: Any,
    *,
    detail_ref: str | None = None,
    preview_max_bytes: int = 512,
) -> ValueSummary:
    """Build a deterministic bounded summary without exposing sensitive values."""

    normalized = _json_compatible(value)
    redacted, contains_redaction = _redact(normalized)
    encoded = _stable_json(normalized)
    digest_source = _stable_json(redacted) if contains_redaction else encoded
    artifact = _artifact_ref(value, normalized)
    preview = (
        redacted
        if artifact is None and len(_stable_json(redacted)) <= preview_max_bytes
        else None
    )
    return ValueSummary(
        type=_value_type(value),
        preview=preview,
        shape=_shape(normalized),
        serialized_bytes=len(encoded),
        digest=sha256(digest_source).hexdigest(),
        artifact_ref=artifact,
        detail_ref=detail_ref,
        redacted=contains_redaction,
    )


def _json_compatible(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [_json_compatible(item) for item in value]
    return repr(value)


def _redact(value: Any) -> tuple[Any, bool]:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        changed = False
        for key, item in value.items():
            normalized_key = key.lower().replace("-", "_")
            if normalized_key in _SENSITIVE_KEYS or any(
                normalized_key.endswith(f"_{suffix}")
                for suffix in ("password", "secret", "token", "api_key")
            ):
                result[key] = _REDACTED
                changed = True
            else:
                result[key], child_changed = _redact(item)
                changed = changed or child_changed
        return result, changed
    if isinstance(value, list):
        result: list[Any] = []
        changed = False
        for item in value:
            child, child_changed = _redact(item)
            result.append(child)
            changed = changed or child_changed
        return result, changed
    return value, False


def _stable_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _shape(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {
            "key_count": len(value),
            "keys": tuple(sorted(value)[:20]),
        }
    if isinstance(value, list):
        return {"length": len(value)}
    if isinstance(value, str):
        return {"characters": len(value)}
    return {}


def _value_type(value: Any) -> str:
    if isinstance(value, ArtifactRef):
        return "artifact_ref"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return "array"
    return type(value).__name__


def _artifact_ref(value: Any, normalized: Any) -> str | None:
    if isinstance(value, ArtifactRef):
        return str(value.id)
    if isinstance(normalized, dict) and {
        "id",
        "kind",
        "storage",
    }.issubset(normalized):
        if normalized.get("kind") in {"artifact", "runtime_value"}:
            return str(normalized["id"])
    return None
