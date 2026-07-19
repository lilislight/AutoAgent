from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter_ns, time_ns
from typing import Any, TypeAlias


TimestampMs: TypeAlias = int


def utc_timestamp_ms() -> TimestampMs:
    """Return the current Unix timestamp in UTC milliseconds.

    Runtime records and public observation DTOs use one timezone-neutral integer
    representation. Browsers may render this instant in any local timezone.
    """

    return time_ns() // 1_000_000


def monotonic_timestamp_ns() -> int:
    """Return a process-local monotonic clock value for duration measurement."""

    return perf_counter_ns()


def elapsed_ms(started_ns: int) -> int:
    """Measure elapsed milliseconds without depending on wall-clock changes."""

    return max(0, (perf_counter_ns() - started_ns) // 1_000_000)


def coerce_timestamp_ms(value: Any) -> TimestampMs | None:
    """Read current millisecond timestamps and legacy datetime/ISO records."""

    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("Boolean is not a valid timestamp.")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return int(timestamp.timestamp() * 1000)
