"""Dependency boundaries owned by the standalone Core application."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol

from ..executor import (
    CallEventHandler,
    NodeExecutionResult,
    StreamChunkHandler,
)
from ..operators import Operator, OperatorRegistration
from ..runtime import (
    NodeOccurrenceCompleted,
    NodeOccurrenceFailed,
    NodeOccurrenceStarted,
    SessionCheckpoint,
    RuntimeEvent,
    RuntimeState,
    SchedulerInitialized,
    StateOperationBatch,
    StateTransition,
    UserEvent,
)
from ..workflow import Capability, EdgeIR, ErrorInfo, NodeIR, WorkflowIR


class RuntimeJournalPort(Protocol):
    """Non-blocking in-process boundary for canonical Runtime Events.

    A database adapter must not implement this port with blocking I/O on the
    Runtime Loop. External persistence consumes emitted Event batches through
    the hosting layer.
    """

    def apply_transition(self, transition: StateTransition) -> RuntimeState: ...

    def append(self, event: RuntimeEvent) -> RuntimeState: ...

    def append_many(self, events: tuple[RuntimeEvent, ...]) -> RuntimeState: ...

    def stage(self, event: RuntimeEvent) -> RuntimeState: ...

    def flush(self, session_id: str) -> RuntimeEvent | None: ...

    def begin_event_group(self, session_id: str) -> None:
        """Start one exclusive group whose staged prefix must remain invisible.

        While active, ``append`` may update live State, but ordinary ``flush``
        must reject the Session and checkpoint capture must omit the group.
        """
        ...

    def commit_event_group(self, session_id: str) -> RuntimeEvent | None:
        """Seal every grouped transition as one Runtime Event, or return None."""
        ...

    def abort_event_group(self, session_id: str) -> bool:
        """Restore pre-group State, pending batches and Event-id ownership."""
        ...

    def event_group_active(self, session_id: str) -> bool:
        """Return whether the Session currently owns an uncommitted group."""
        ...

    def pending_batches(self, session_id: str) -> tuple[StateOperationBatch, ...]: ...

    def state(self, session_id: str) -> RuntimeState: ...

    def events(self, session_id: str) -> tuple[RuntimeEvent, ...]: ...

    def drain_events(self, session_id: str) -> tuple[RuntimeEvent, ...]: ...

    def session_ids(self) -> tuple[str, ...]: ...

    def discard_states(self, session_ids: tuple[str, ...]) -> None:
        """Discard only States with no active group, pending, or unacknowledged Event."""
        ...

    def install_states(self, states: Mapping[str, RuntimeState]) -> None: ...

    def capture_checkpoint(
        self, session_id: str, *, captured_at_ns: int | None = None
    ) -> SessionCheckpoint:
        """Capture one recoverable Runtime Session."""
        ...


class UserEventJournalPort(Protocol):
    """Independent ordered journal for non-canonical User Events."""

    def emit(
        self,
        *,
        session_id: str,
        invocation_id: str,
        kind: str,
        payload: object,
        occurrence_id: str | None,
        occurred_at_ns: int,
    ) -> UserEvent: ...

    def events(self, invocation_id: str) -> tuple[UserEvent, ...]: ...

    def drain(self, invocation_id: str) -> tuple[UserEvent, ...]: ...

    def discard(self, invocation_id: str) -> None: ...


class SchedulerPort(Protocol):
    """Pure planning boundary between the App and graph scheduling."""

    def initialize(
        self, workflow: WorkflowIR, state: RuntimeState
    ) -> SchedulerInitialized: ...

    def start(self, occurrence_id: str) -> NodeOccurrenceStarted: ...

    def complete(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        occurrence_id: str,
        output: object,
        *,
        selected_edge_ids: set[str] | None = None,
    ) -> NodeOccurrenceCompleted: ...

    def fail(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        occurrence_id: str,
        error,
        *,
        selected_edge_ids: set[str] | None = None,
    ) -> NodeOccurrenceFailed: ...


class NodeExecutorPort(Protocol):
    """Transient Operator execution boundary; it never owns Runtime State."""

    @property
    def max_operator_concurrency(self) -> int: ...

    def close(self) -> None: ...

    async def call_hook(
        self, handler: Callable[..., object], *args: object
    ) -> object: ...

    async def map_input(
        self,
        node: NodeIR,
        *,
        invocation_input: object,
        incoming: dict[str, object],
        invocation_context: object,
        session_context: object,
    ) -> object: ...

    async def bind_output(
        self,
        node: NodeIR,
        output: object,
        *,
        invocation_context: object,
        session_context: object,
    ): ...

    async def select_edges(
        self,
        edges: tuple[EdgeIR, ...],
        *,
        source_status: str,
        source_node_id: str,
        output: object | None,
        error: ErrorInfo | None,
        invocation_context: object,
        session_context: object,
    ) -> set[str]: ...

    async def execute(
        self,
        node: NodeIR,
        occurrence_id: str,
        value: object,
        *,
        invocation_context: object = None,
        session_context: object = None,
        on_call_event: CallEventHandler,
        on_stream_chunk: StreamChunkHandler,
    ) -> NodeExecutionResult: ...


class OperatorRegistryPort(Protocol):
    """Runtime-selectable implementations of compiled Capability contracts."""

    def bind_capability(self, capability: Capability) -> None: ...

    def bind_capabilities(self, capabilities: tuple[Capability, ...]) -> None: ...

    def register(
        self,
        operator: Operator,
        *,
        capability_id: str,
        priority: int = 0,
        enabled: bool = True,
        default: bool = False,
    ) -> Operator: ...

    def for_capability(
        self, capability_id: str, *, include_disabled: bool = False
    ) -> tuple[OperatorRegistration, ...]: ...

    def default_for_capability(self, capability_id: str) -> Operator | None: ...

    def set_enabled(self, operator_id: str, enabled: bool) -> None: ...


Clock = Callable[[], int]


__all__ = [
    "Clock",
    "NodeExecutorPort",
    "OperatorRegistryPort",
    "RuntimeJournalPort",
    "SchedulerPort",
    "UserEventJournalPort",
]
