"""The external acceptance boundary used by V2 Core.

Core owns Event/Checkpoint capture and serialization.  A Sink implementation
normally lives in the future Server/Platform layer and only accepts immutable
records into its own in-process memory.  Database writes, remote delivery,
retry, batching, retention, projections, and health reporting are deliberately
outside this protocol.
"""

from __future__ import annotations

from typing import Protocol

from .checkpoint import SerializedCheckpoint
from .events import SerializedEvent


class RuntimeSink(Protocol):
    """One App-wide in-memory acceptance boundary owned outside Core."""

    async def wait_until_admissible(self) -> None:
        """Return when a new Invocation may be created.

        The caller owns any admission timeout.  Implementations may wait on
        their queue-capacity condition but must not perform durable I/O here.
        """

    async def submit_events(self, events: tuple[SerializedEvent, ...]) -> None:
        """Return after atomically accepting every Event into Sink memory.

        A full Sink queue may suspend this call and therefore pause the current
        Invocation at an Event boundary.  The method must be cancellation-safe:
        cancellation cannot leave Core uncertain whether the batch was accepted.
        Backend delivery failure after return is exclusively Sink-owned.
        """

    def offer_checkpoint(self, checkpoint: SerializedCheckpoint) -> None:
        """Non-blockingly offer a latest-wins Checkpoint to Sink memory."""
