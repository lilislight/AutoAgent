from __future__ import annotations

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
    """Validate one optional Unix timestamp in milliseconds."""

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("Timestamp must be an integer number of milliseconds.")
    return value
