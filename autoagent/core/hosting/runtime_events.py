"""Hosting boundary for consuming captured Runtime Events."""

from __future__ import annotations

from typing import Protocol

from ..runtime.events import RuntimeEvent
from ..runtime.user_events import UserEvent


class RuntimeEventSink(Protocol):
    """Persist or forward a captured Event outside Core ownership.

    Core awaits this boundary, so a slow sink applies Runtime backpressure.  Calls
    are ordered within one Session but may overlap across independent Sessions.
    Returning means the Event is durably accepted.  Raising leaves the Event in
    Core for a later export attempt; therefore an implementation may receive the
    same Event id again after a partial external success and must accept it
    idempotently.  Long-term storage, retry scheduling and Outbox transactions
    remain Host responsibilities.
    """

    async def append(self, event: RuntimeEvent) -> None:
        """Durably and idempotently accept one Session-ordered canonical Event."""


class UserEventSink(Protocol):
    """Persist or forward a User Event outside canonical Runtime State.

    User Events have their own Invocation-local sequence and idempotency key.
    A delivery failure must never roll back or rewrite canonical Workflow
    progress; the App records the first failure and stops that sink stream so a
    later Event cannot hide a persistence gap.
    """

    async def append_user_event(self, event: UserEvent) -> None:
        """Accept one ordered User Event independently from Runtime Events."""


__all__ = ["RuntimeEventSink", "UserEventSink"]
