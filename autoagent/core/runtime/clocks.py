"""Wall-clock timestamps use Unix microseconds; durations use monotonic ns."""

import time


def unix_time_us() -> int:
    """Return microseconds since the Unix epoch without float rounding."""
    return time.time_ns() // 1_000
