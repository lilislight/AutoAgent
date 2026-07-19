from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any


Redactor = Callable[[Any], Any]

DEFAULT_SENSITIVE_KEYS = frozenset(
    {
        "api_key",
        "access_token",
        "authorization",
        "cookie",
        "credentials",
        "password",
        "passwd",
        "private_key",
        "refresh_token",
        "secret",
        "set_cookie",
        "token",
    }
)


def redact_sensitive_data(
    value: Any,
    *,
    sensitive_keys: frozenset[str] = DEFAULT_SENSITIVE_KEYS,
    replacement: str = "[REDACTED]",
) -> Any:
    """Return a recursively redacted API value without mutating runtime data."""

    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            result[str(key)] = (
                replacement
                if normalized in sensitive_keys
                else redact_sensitive_data(
                    item,
                    sensitive_keys=sensitive_keys,
                    replacement=replacement,
                )
            )
        return result
    if isinstance(value, tuple):
        return tuple(
            redact_sensitive_data(
                item,
                sensitive_keys=sensitive_keys,
                replacement=replacement,
            )
            for item in value
        )
    if isinstance(value, list):
        return [
            redact_sensitive_data(
                item,
                sensitive_keys=sensitive_keys,
                replacement=replacement,
            )
            for item in value
        ]
    return value
