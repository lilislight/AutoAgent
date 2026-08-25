"""Errors raised by Host-owned persistence adapters."""

from __future__ import annotations


class RuntimeEventStoreError(RuntimeError):
    """Base error for a Runtime Event store operation."""


class RuntimeEventConflictError(RuntimeEventStoreError):
    """A durable identity was reused for different canonical content."""


class RuntimeEventSequenceError(RuntimeEventStoreError):
    """A Session Event does not extend its durable chain contiguously."""


class RuntimeEventStoreClosedError(RuntimeEventStoreError):
    """An operation targeted a closed Store."""


class RuntimeSessionNotRootError(RuntimeEventStoreError):
    """Checkpoint recovery targeted a Child Session instead of its Root."""


class RuntimeEventQueryError(ValueError):
    """A read query requested a position outside canonical stored data."""


__all__ = [
    "RuntimeEventConflictError",
    "RuntimeEventQueryError",
    "RuntimeEventSequenceError",
    "RuntimeEventStoreClosedError",
    "RuntimeEventStoreError",
    "RuntimeSessionNotRootError",
]
