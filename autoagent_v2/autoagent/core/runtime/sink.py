"""The only durable-output dependency owned by V2 Core."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .checkpoint import RecoveryCheckpoint
from .events import Event


@dataclass(frozen=True, slots=True)
class SinkPressure:
    accepting_new_executions: bool
    pending_events: int = 0
    pending_bytes: int = 0
    backend_available: bool = True
    reason: str | None = None


class RuntimeSink(Protocol):
    """One App-wide ownership boundary for Events and latest Checkpoints."""

    async def wait_until_admissible(self, timeout: float | None) -> bool:
        """Wait until a new Invocation may be created."""

    async def submit_events(self, events: tuple[Event, ...]) -> None:
        """Return after accepting ownership of every supplied Event."""

    def offer_checkpoint(self, checkpoint: RecoveryCheckpoint) -> None:
        """Offer a non-blocking latest-wins Checkpoint."""

    def pressure(self) -> SinkPressure:
        """Return an immediate diagnostic snapshot."""
