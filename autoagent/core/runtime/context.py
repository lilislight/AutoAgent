from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.runtime.output import OutputView


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
        return {
            "data": deepcopy(self.data),
            "metadata": deepcopy(self.metadata),
        }

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


@dataclass(frozen=True, slots=True)
class ContextSnapshot:
    """Isolated context copy for non-writing Workflow hooks.

    The top-level mappings are read-only. Nested values preserve their original
    Python container types and belong to this snapshot, so even accidental
    nested mutation cannot reach the authoritative Runtime Context.
    """

    data: Mapping[str, Any]
    metadata: Mapping[str, Any]

    @classmethod
    def capture(cls, context: RuntimeContext) -> ContextSnapshot:
        return cls(
            data=MappingProxyType(deepcopy(context.data)),
            metadata=MappingProxyType(deepcopy(context.metadata)),
        )


@dataclass(frozen=True, slots=True)
class IncomingOutput:
    """Read-only value carried by one selected incoming edge.

    NodeExecutor builds these records from NodeExecutionRequest activations.
    `edge_id` lets loop input mappings distinguish initial entry from a back
    edge, while `source_execution_id` identifies the exact historical output.
    """

    edge_id: str
    source_node_id: str
    source_execution_id: UUID
    value: Any


@dataclass(frozen=True, slots=True)
class HookContextSnapshot:
    """Common fields shared by every non-writing Workflow hook."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView

    def isolate(self) -> HookContextSnapshot:
        """Create a phase-local copy without copying indexed node outputs."""

        return HookContextSnapshot(
            invocation_input=MappingProxyType(
                deepcopy(dict(self.invocation_input))
            ),
            invocation_context=ContextSnapshot(
                data=MappingProxyType(deepcopy(dict(self.invocation_context.data))),
                metadata=MappingProxyType(
                    deepcopy(dict(self.invocation_context.metadata))
                ),
            ),
            session_context=ContextSnapshot(
                data=MappingProxyType(deepcopy(dict(self.session_context.data))),
                metadata=MappingProxyType(
                    deepcopy(dict(self.session_context.metadata))
                ),
            ),
            outputs=self.outputs,
        )


def capture_hook_context(
    *,
    invocation_input: Mapping[str, Any],
    invocation_context: InvocationContext,
    session_context: SessionContext,
    outputs: OutputView,
) -> HookContextSnapshot:
    return HookContextSnapshot(
        invocation_input=MappingProxyType(deepcopy(dict(invocation_input))),
        invocation_context=ContextSnapshot.capture(invocation_context),
        session_context=ContextSnapshot.capture(session_context),
        outputs=outputs,
    )


@dataclass(frozen=True, slots=True)
class InputMappingContext:
    """Context passed to a Node input mapping."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    node_id: str
    incoming: tuple[IncomingOutput, ...] = ()

    @classmethod
    def create(
        cls,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: OutputView,
        node_id: str,
        incoming: tuple[IncomingOutput, ...] = (),
    ) -> InputMappingContext:
        common = capture_hook_context(
            invocation_input=invocation_input,
            invocation_context=invocation_context,
            session_context=session_context,
            outputs=outputs,
        )
        return cls(
            invocation_input=common.invocation_input,
            invocation_context=common.invocation_context,
            session_context=common.session_context,
            outputs=common.outputs,
            node_id=node_id,
            incoming=tuple(
                IncomingOutput(
                    edge_id=item.edge_id,
                    source_node_id=item.source_node_id,
                    source_execution_id=item.source_execution_id,
                    value=deepcopy(item.value),
                )
                for item in incoming
            ),
        )


@dataclass(frozen=True, slots=True)
class ConditionContext:
    """Context passed to an Edge condition."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    edge_id: str
    source_node_id: str
    target_node_id: str
    source_output: Any

    @classmethod
    def create(
        cls,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: OutputView,
        edge_id: str,
        source_node_id: str,
        target_node_id: str,
        source_output: Any,
    ) -> ConditionContext:
        common = capture_hook_context(
            invocation_input=invocation_input,
            invocation_context=invocation_context,
            session_context=session_context,
            outputs=outputs,
        )
        return cls(
            invocation_input=common.invocation_input,
            invocation_context=common.invocation_context,
            session_context=common.session_context,
            outputs=common.outputs,
            edge_id=edge_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            source_output=deepcopy(source_output),
        )


@dataclass(frozen=True, slots=True)
class MapItemSelectionContext:
    """Context passed to ``MapPolicy.item_selector``."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    node_id: str
    input: Any


@dataclass(frozen=True, slots=True)
class MapAggregationContext:
    """Context passed to a map output aggregator."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    node_id: str
    item_outputs: list[Any]


@dataclass(frozen=True, slots=True)
class ReplicationAggregationContext:
    """Context passed to a replication output aggregator."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    node_id: str
    replica_outputs: list[Any]


@dataclass(frozen=True, slots=True)
class OutputBindingContext:
    """Transactional context passed to a Node output binding."""

    invocation_input: Mapping[str, Any]
    invocation_context: InvocationContext
    session_context: SessionContext
    outputs: OutputView
    node_id: str
    output: Any

    @classmethod
    def create(
        cls,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: OutputView,
        node_id: str,
        output: Any,
    ) -> OutputBindingContext:
        return cls(
            invocation_input=MappingProxyType(deepcopy(dict(invocation_input))),
            invocation_context=invocation_context,
            session_context=session_context,
            outputs=outputs,
            node_id=node_id,
            output=deepcopy(output),
        )
