from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import Protocol

from autoagent.core.runtime.event import RuntimeEvent


class RuntimeEventSink(Protocol):
    """Consumer notified after RuntimeStore materializes RuntimeEvents."""

    async def aemit(self, events: Sequence[RuntimeEvent]) -> None:
        """Consume already-persisted events without mutating runtime state."""


class LoggingEventSink:
    """Write RuntimeEvents to Python logging.

    This sink is intentionally lossy and diagnostic-only: the RuntimeStore catches
    sink failures so logging cannot fail workflow execution. Payload logging is
    opt-in because runtime payloads may contain user data.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        level: int = logging.INFO,
        include_payload: bool = False,
    ) -> None:
        self.logger = logger or logging.getLogger("autoagent.runtime.events")
        self.level = level
        self.include_payload = include_payload

    async def aemit(self, events: Sequence[RuntimeEvent]) -> None:
        for event in events:
            extra = {
                "autoagent_event_id": str(event.id),
                "autoagent_event_type": event.type,
                "autoagent_workflow_id": event.workflow_id,
                "autoagent_session_id": str(event.session_id),
                "autoagent_invocation_id": str(event.invocation_id),
                "autoagent_sequence": event.sequence,
                "autoagent_node_id": event.node_id,
                "autoagent_edge_id": event.edge_id,
            }
            if self.include_payload:
                extra["autoagent_payload"] = event.payload
            self.logger.log(
                self.level,
                "autoagent runtime event %s #%s",
                event.type,
                event.sequence,
                extra=extra,
            )


async def emit_to_sinks(
    sinks: Iterable[RuntimeEventSink],
    events: Sequence[RuntimeEvent],
    *,
    logger: logging.Logger | None = None,
) -> None:
    """Best-effort fan-out for diagnostic sinks after store persistence."""

    if not events:
        return
    log = logger or logging.getLogger("autoagent.runtime.events")
    for sink in tuple(sinks):
        try:
            await sink.aemit(events)
        except Exception:
            log.exception("RuntimeEventSink failed; workflow execution continues.")
