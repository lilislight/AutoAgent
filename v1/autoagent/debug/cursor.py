from __future__ import annotations

from base64 import urlsafe_b64decode, urlsafe_b64encode
from hashlib import sha256
import json
from typing import Any


def encode_debug_cursor(
    *,
    invocation_id: str,
    query: str,
    through_sequence: int,
    position: int,
    filters: dict[str, Any] | None = None,
) -> str:
    payload = json.dumps(
        {
            "v": 1,
            "invocation_id": invocation_id,
            "query": query,
            "through_sequence": through_sequence,
            "position": position,
            "filters": filters or {},
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    checksum = sha256(b"autoagent-debug-cursor-v1\0" + payload).digest()[:12]
    return f"{_encode(payload)}.{_encode(checksum)}"


def decode_debug_cursor(
    cursor: str,
    *,
    invocation_id: str,
    query: str,
    through_sequence: int | None,
    filters: dict[str, Any] | None = None,
) -> tuple[int, int]:
    try:
        payload_text, checksum_text = cursor.split(".", 1)
        payload = _decode(payload_text)
        checksum = _decode(checksum_text)
        expected = sha256(
            b"autoagent-debug-cursor-v1\0" + payload
        ).digest()[:12]
        if checksum != expected:
            raise ValueError
        value = json.loads(payload)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid or modified debug cursor.") from exc
    if (
        value.get("v") != 1
        or value.get("invocation_id") != invocation_id
        or value.get("query") != query
        or value.get("filters", {}) != (filters or {})
    ):
        raise ValueError("Debug cursor does not belong to this query.")
    cursor_through = int(value["through_sequence"])
    if through_sequence is not None and cursor_through != through_sequence:
        raise ValueError("Debug cursor uses another observed sequence boundary.")
    position = int(value["position"])
    if cursor_through < 0 or position < 0:
        raise ValueError("Debug cursor contains an invalid sequence.")
    return cursor_through, position


def _encode(value: bytes) -> str:
    return urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return urlsafe_b64decode(value + padding)
