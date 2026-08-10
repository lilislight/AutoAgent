"""Thread-safe Session data and lightweight Invocation Handle."""

from __future__ import annotations

import asyncio
import copy
import threading
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from .checkpoint import RecoveryCheckpoint
from ..errors import InvocationStateError


class InvocationState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {
            InvocationState.COMPLETED,
            InvocationState.FAILED,
            InvocationState.CANCELLED,
        }


@dataclass(frozen=True, slots=True)
class RuntimeErrorInfo:
    type: str
    message: str


@dataclass(frozen=True, slots=True)
class WaitSnapshot:
    id: UUID
    node_id: str
    node_execution_id: UUID
    payload: Any


@dataclass(frozen=True, slots=True)
class InvocationSnapshot:
    id: UUID
    workflow_id: str
    workflow_revision_id: str
    session_id: str
    state: InvocationState
    output: dict[str, Any] | None
    error: RuntimeErrorInfo | None
    latest_checkpoint: RecoveryCheckpoint | None
    waits: tuple[WaitSnapshot, ...]
    created_at_ms: int
    updated_at_ms: int


class Invocation:
    """Compact Handle retained after heavy execution state is released."""

    def __init__(
        self,
        *,
        invocation_id: UUID,
        workflow_id: str,
        workflow_revision_id: str,
        session_id: str,
        created_at_ms: int,
    ) -> None:
        self.id = invocation_id
        self.workflow_id = workflow_id
        self.workflow_revision_id = workflow_revision_id
        self.session_id = session_id
        self.created_at_ms = created_at_ms
        self._updated_at_ms = created_at_ms
        self._state = InvocationState.CREATED
        self._output: dict[str, Any] | None = None
        self._error: RuntimeErrorInfo | None = None
        self._latest_checkpoint: RecoveryCheckpoint | None = None
        self._waits: tuple[WaitSnapshot, ...] = ()
        self._condition = threading.Condition(threading.RLock())

    @property
    def state(self) -> InvocationState:
        with self._condition:
            return self._state

    @property
    def output(self) -> dict[str, Any] | None:
        with self._condition:
            return copy.deepcopy(self._output)

    @property
    def error(self) -> RuntimeErrorInfo | None:
        with self._condition:
            return self._error

    @property
    def latest_checkpoint(self) -> RecoveryCheckpoint | None:
        with self._condition:
            return copy.deepcopy(self._latest_checkpoint)

    def done(self) -> bool:
        return self.state.terminal

    @property
    def waits(self) -> tuple[WaitSnapshot, ...]:
        with self._condition:
            return copy.deepcopy(self._waits)

    def snapshot(self) -> InvocationSnapshot:
        with self._condition:
            return InvocationSnapshot(
                id=self.id,
                workflow_id=self.workflow_id,
                workflow_revision_id=self.workflow_revision_id,
                session_id=self.session_id,
                state=self._state,
                output=copy.deepcopy(self._output),
                error=self._error,
                latest_checkpoint=copy.deepcopy(self._latest_checkpoint),
                waits=copy.deepcopy(self._waits),
                created_at_ms=self.created_at_ms,
                updated_at_ms=self._updated_at_ms,
            )

    def wait(self, timeout: float | None = None) -> "Invocation":
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._state.terminal:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("Invocation did not reach a boundary in time.")
                self._condition.wait(remaining)
        return self

    async def await_done(self, timeout: float | None = None) -> "Invocation":
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self.state.terminal:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Invocation did not reach a boundary in time.")
            await asyncio.sleep(0.005)
        return self

    def result(self) -> dict[str, Any]:
        with self._condition:
            if self._state is InvocationState.COMPLETED:
                return copy.deepcopy(self._output or {})
            if self._state is InvocationState.FAILED:
                message = self._error.message if self._error else "Invocation failed."
                raise InvocationStateError(message)
            if self._state is InvocationState.CANCELLED:
                raise InvocationStateError("Invocation was cancelled.")
            raise InvocationStateError(
                f"Invocation result is unavailable while state is {self._state}."
            )

    def _update(
        self,
        *,
        state: InvocationState | None = None,
        output: dict[str, Any] | None = None,
        error: RuntimeErrorInfo | None = None,
        checkpoint: RecoveryCheckpoint | None = None,
        waits: tuple[WaitSnapshot, ...] | None = None,
        updated_at_ms: int,
    ) -> None:
        with self._condition:
            if state is not None:
                self._state = state
            if output is not None:
                self._output = copy.deepcopy(output)
            if error is not None:
                self._error = error
            if checkpoint is not None:
                self._latest_checkpoint = checkpoint
            if waits is not None:
                self._waits = copy.deepcopy(waits)
            self._updated_at_ms = updated_at_ms
            self._condition.notify_all()

    def _clear_checkpoint(self, *, updated_at_ms: int) -> None:
        with self._condition:
            self._latest_checkpoint = None
            self._updated_at_ms = updated_at_ms
            self._condition.notify_all()


class Session:
    """Compact Session handle projected from the canonical Runtime State."""

    def __init__(
        self,
        *,
        id: str,
        workflow_id: str,
        context: dict[str, Any] | None = None,
        invocation: Invocation | None = None,
        created_at_ms: int = 0,
        updated_at_ms: int = 0,
    ) -> None:
        self.id = id
        self.workflow_id = workflow_id
        self.invocation = invocation
        self.created_at_ms = created_at_ms
        self.updated_at_ms = updated_at_ms
        self._bootstrap_context = copy.deepcopy(context or {})
        self._runtime_state: Any = None

    @property
    def context(self) -> dict[str, Any]:
        if self._runtime_state is None:
            return copy.deepcopy(self._bootstrap_context)
        return self._runtime_state.read("session", "context")

    def _attach_runtime_state(self, runtime_state: Any) -> None:
        self._runtime_state = runtime_state
        self._bootstrap_context = {}
