"""Transient buffers used to form Runtime Event capture envelopes."""

from __future__ import annotations

from .events import RuntimeEvent


class RuntimeEventCapture:
    """Own pending and captured Events without owning Runtime State."""

    def __init__(self) -> None:
        self.events: dict[str, list[RuntimeEvent]] = {}
        self.pending: dict[str, list[RuntimeEvent]] = {}
        self.event_ids: dict[str, RuntimeEvent] = {}


__all__ = ["RuntimeEventCapture"]
