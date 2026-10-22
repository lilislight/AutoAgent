"""Transient execution identities, independent of integrations and durable State."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OperatorCallContext:
    """Actual Workflow/Call identities, available on Runtime tasks and workers."""

    workflow_id: str | None = None
    workflow_revision_id: str | None = None
    workflow_version: str | None = None
    session_id: str | None = None
    invocation_id: str | None = None
    occurrence_id: str | None = None
    node_id: str | None = None
    operator_id: str | None = None
    operator_call_id: str | None = None
    unit_index: int | None = None
    attempt: int = 1
    retry_attempt: int = 0

    @classmethod
    def current(cls) -> OperatorCallContext | None:
        return _current.get()


_current: ContextVar[OperatorCallContext | None] = ContextVar("autoagent_operator_call_context", default=None)


@contextmanager
def operator_call_context(context: OperatorCallContext | None):
    token = _current.set(context)
    try:
        yield context
    finally:
        _current.reset(token)


__all__ = ["OperatorCallContext", "operator_call_context"]
