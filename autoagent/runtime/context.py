from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RuntimeContext(BaseModel):
    """User-controlled context persisted by RuntimeStore.

    RuntimeContext is intentionally small. Framework execution state such as
    node transitions, waiting executions, and node outputs must not be stored
    here. `data` is the mutable space exposed to mapping/binding hooks.
    `metadata` is for caller/tooling annotations that should be stored with the
    same lifetime but should not drive scheduler decisions.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    data: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Mutable user data. OutputBindingContext may write it; "
            "InputMappingContext and ConditionContext receive read-only copies."
        ),
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Auxiliary user/tooling metadata with the same lifetime.",
    )

    def to_record(self) -> dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> RuntimeContext:
        return cls(
            data=dict(record.get("data", {})),
            metadata=dict(record.get("metadata", {})),
        )


class SessionContext(RuntimeContext):
    """User data shared by all invocations in one Session.

    Typical use: chat history, long-lived task memory, or tenant-specific
    preferences. Output binding is the normal way to update this object during
    execution. Operators should not receive the mutable object directly.
    """


class InvocationContext(RuntimeContext):
    """User data scoped to one Invocation.

    Typical use: scratch data for one request, temporary routing notes, or
    values that should not survive into the next invocation in the same session.
    """


class InputMappingContext:
    """Read-only view passed to input_mapping functions.

    It can read the invocation input, session/invocation context snapshots, and
    completed node outputs. It must not mutate runtime state. NodeExecutor builds
    this object before calling a node's input_mapping.
    """

    def __init__(
        self,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: Any,
        node_id: str,
    ) -> None:
        self.invocation_input = MappingProxyType(dict(invocation_input))
        self.invocation_context = invocation_context.model_copy(deep=True)
        self.session_context = session_context.model_copy(deep=True)
        self.outputs = outputs
        self.node_id = node_id


class ConditionContext:
    """Read-only view passed to edge condition functions.

    Conditions have the same read environment as input_mapping plus edge/source
    metadata. Scheduler builds this object while processing a completed source
    NodeExecution. A false condition only means this edge is not selected.
    """

    def __init__(
        self,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: Any,
        edge_id: str,
        source_node_id: str,
        target_node_id: str,
        source_output: Any,
    ) -> None:
        self.invocation_input = MappingProxyType(dict(invocation_input))
        self.invocation_context = invocation_context.model_copy(deep=True)
        self.session_context = session_context.model_copy(deep=True)
        self.outputs = outputs
        self.edge_id = edge_id
        self.source_node_id = source_node_id
        self.target_node_id = target_node_id
        self.source_output = source_output


class OutputBindingContext:
    """Mutable user-context view passed to output_binding functions.

    Output binding runs after NodeExecution.output has already been finalized by
    NodeExecutor. It may mutate session_context.data/metadata and
    invocation_context.data/metadata, but it must not change node output,
    scheduler queues, waiting entries, or execution history.
    """

    def __init__(
        self,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: Any,
        node_id: str,
        output: Any,
    ) -> None:
        self.invocation_input = MappingProxyType(dict(invocation_input))
        self.invocation_context = invocation_context
        self.session_context = session_context
        self.outputs = outputs
        self.node_id = node_id
        self.output = output
