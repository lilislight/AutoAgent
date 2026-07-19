from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.time import TimestampMs, utc_timestamp_ms


RuntimeEventChannel = Literal["runtime", "output"]
RuntimeEventVisibility = Literal["internal", "user"]
RuntimeEntityType = Literal[
    "invocation",
    "invocation_context",
    "session_context",
    "node_execution",
    "operator_call",
    "edge",
    "output",
]


class RuntimeEvent(BaseModel):
    """Immutable ordered fact consumed by tracing, streaming, and optimization.

    `sequence` is monotonic inside one Session and is the authoritative ordering
    key. `occurred_at_ms` is an absolute UTC instant used only for display and
    duration placement; concurrent events may share the same millisecond.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID = Field(default_factory=uuid4)
    namespace: str
    workflow_id: str
    session_id: UUID
    invocation_id: UUID
    sequence: int = Field(ge=1)
    type: str = Field(min_length=1)
    entity_type: RuntimeEntityType
    entity_id: str | None = None
    node_id: str | None = None
    edge_id: str | None = None
    occurred_at_ms: TimestampMs
    channel: RuntimeEventChannel = "runtime"
    visibility: RuntimeEventVisibility = "internal"
    payload: dict[str, Any] = Field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeEventDraft:
    """Event without Store-owned identity, Session sequence, or scope fields."""

    type: str
    entity_type: RuntimeEntityType
    occurred_at_ms: TimestampMs
    entity_id: str | None = None
    node_id: str | None = None
    edge_id: str | None = None
    channel: RuntimeEventChannel = "runtime"
    visibility: RuntimeEventVisibility = "internal"
    payload: dict[str, Any] = field(default_factory=dict)

    def materialize(
        self,
        *,
        namespace: str,
        workflow_id: str,
        session_id: UUID,
        invocation_id: UUID,
        sequence: int,
    ) -> RuntimeEvent:
        return RuntimeEvent(
            namespace=namespace,
            workflow_id=workflow_id,
            session_id=session_id,
            invocation_id=invocation_id,
            sequence=sequence,
            type=self.type,
            entity_type=self.entity_type,
            entity_id=self.entity_id,
            node_id=self.node_id,
            edge_id=self.edge_id,
            occurred_at_ms=self.occurred_at_ms,
            channel=self.channel,
            visibility=self.visibility,
            payload=self.payload,
        )


def invocation_checkpoint_events(
    previous: Invocation | None,
    current: Invocation,
    *,
    changed_node_execution_ids: frozenset[str] | None = None,
) -> list[RuntimeEventDraft]:
    """Describe externally observable changes in one Invocation checkpoint.

    RuntimeStore calls this before replacing the materialized Invocation state.
    It intentionally derives facts from persisted object changes so concurrent
    NodeExecutor workers never publish events or assign ordering themselves.
    """

    drafts: list[RuntimeEventDraft] = []
    previous_executions = (
        {
            str(value.id): value
            for value in previous.node_executions
            if changed_node_execution_ids is None
            or str(value.id) in changed_node_execution_ids
        }
        if previous is not None
        else {}
    )

    if previous is None:
        drafts.append(
            RuntimeEventDraft(
                type="invocation.created",
                entity_type="invocation",
                entity_id=str(current.id),
                occurred_at_ms=current.created_at_ms,
                payload={"state": "created", "input": current.input},
            )
        )
    if previous is None or previous.state != current.state:
        drafts.append(
            RuntimeEventDraft(
                type="invocation.state_changed",
                entity_type="invocation",
                entity_id=str(current.id),
                occurred_at_ms=current.updated_at_ms,
                payload={
                    "from": previous.state if previous is not None else None,
                    "to": current.state,
                    "error": current.error.to_record() if current.error else None,
                    "result": current.result if current.state == "completed" else None,
                },
            )
        )
        if current.state == "completed":
            drafts.append(
                RuntimeEventDraft(
                    type="invocation.output_published",
                    entity_type="output",
                    entity_id=str(current.id),
                    occurred_at_ms=current.updated_at_ms,
                    channel="output",
                    visibility="user",
                    payload={"result": current.result},
                )
            )
    if previous is not None and previous.context.to_record() != current.context.to_record():
        drafts.append(
            RuntimeEventDraft(
                type="invocation.context_updated",
                entity_type="invocation_context",
                entity_id=str(current.id),
                occurred_at_ms=current.updated_at_ms,
                payload={"context": current.context.to_record()},
            )
        )

    for execution in current.node_executions:
        if (
            changed_node_execution_ids is not None
            and str(execution.id) not in changed_node_execution_ids
        ):
            continue
        old_execution = previous_executions.get(str(execution.id))
        drafts.extend(_node_execution_events(old_execution, execution))

    # A checkpoint can observe several concurrent completions. Wall time places
    # them on the timeline; stable tie breakers keep assigned sequences repeatable.
    sort_runtime_event_drafts(drafts)
    return drafts


def sort_runtime_event_drafts(drafts: list[RuntimeEventDraft]) -> None:
    """Apply one stable ordering to drafts created by the same transaction."""

    drafts.sort(
        key=lambda value: (
            value.occurred_at_ms,
            _event_priority(value.type),
            value.entity_id or "",
            value.type,
        )
    )


def session_context_event(
    *,
    session_id: UUID,
    invocation_id: UUID,
    context: dict[str, Any],
    occurred_at_ms: TimestampMs | None = None,
) -> RuntimeEventDraft:
    return RuntimeEventDraft(
        type="session.context_updated",
        entity_type="session_context",
        entity_id=str(session_id),
        occurred_at_ms=occurred_at_ms or utc_timestamp_ms(),
        payload={"invocation_id": str(invocation_id), "context": context},
    )


def operator_call_checkpoint_events(
    previous: Any | None,
    current: Any,
    *,
    node_id: str,
) -> list[RuntimeEventDraft]:
    """Describe one independently persisted OperatorCall transition."""

    return _operator_call_events(previous, current, node_id)


def _node_execution_events(previous: Any, current: Any) -> list[RuntimeEventDraft]:
    drafts: list[RuntimeEventDraft] = []
    execution_id = str(current.id)
    if previous is None:
        drafts.append(
            RuntimeEventDraft(
                type="node.execution_created",
                entity_type="node_execution",
                entity_id=execution_id,
                node_id=current.node_id,
                occurred_at_ms=current.created_at_ms,
                payload={
                    "sequence": current.sequence,
                    "state": "created",
                    "incoming_activations": [
                        activation.to_record()
                        for activation in current.incoming_activations
                    ],
                },
            )
        )
        if current.started_at_ms is not None:
            drafts.append(_node_state_event(None, current, "running", current.started_at_ms))
        if current.state not in {"created", "ready", "running"}:
            previous_state = "running" if current.started_at_ms is not None else "created"
            drafts.append(
                _node_state_event(
                    previous_state,
                    current,
                    current.state,
                    _node_final_time(current),
                )
            )
    elif previous.state != current.state:
        drafts.append(
            _node_state_event(
                previous.state,
                current,
                current.state,
                current.started_at_ms
                if current.state == "running" and current.started_at_ms is not None
                else _node_final_time(current),
            )
        )

    previous_calls = (
        {str(value.id): value for value in previous.operator_calls}
        if previous is not None
        else {}
    )
    for call in current.operator_calls:
        old_call = previous_calls.get(str(call.id))
        drafts.extend(_operator_call_events(old_call, call, current.node_id))

    previous_evaluations = (
        {str(value.id) for value in previous.edge_evaluations}
        if previous is not None
        else set()
    )
    for evaluation in current.edge_evaluations:
        if str(evaluation.id) in previous_evaluations:
            continue
        drafts.append(
            RuntimeEventDraft(
                type="edge.evaluated",
                entity_type="edge",
                entity_id=evaluation.edge_id,
                node_id=current.node_id,
                edge_id=evaluation.edge_id,
                occurred_at_ms=evaluation.created_at_ms,
                payload={
                    "node_execution_id": execution_id,
                    "target_node_id": evaluation.target_node_id,
                    "state": evaluation.state,
                    "selected": evaluation.selected,
                    "reason": evaluation.reason,
                },
            )
        )
    return drafts


def _node_state_event(
    previous_state: str | None,
    current: Any,
    state: str,
    occurred_at_ms: TimestampMs,
) -> RuntimeEventDraft:
    return RuntimeEventDraft(
        type="node.state_changed",
        entity_type="node_execution",
        entity_id=str(current.id),
        node_id=current.node_id,
        occurred_at_ms=occurred_at_ms,
        payload={
            "from": previous_state,
            "to": state,
            "input": current.input if state == "running" else None,
            "output": current.output if state == "completed" else None,
            "error": current.error.to_record() if current.error else None,
            "resource_usage": current.resource_usage.to_record(),
        },
    )


def _operator_call_events(previous: Any, current: Any, node_id: str) -> list[RuntimeEventDraft]:
    drafts: list[RuntimeEventDraft] = []
    call_id = str(current.id)
    if previous is None:
        drafts.append(
            RuntimeEventDraft(
                type="operator.call_started",
                entity_type="operator_call",
                entity_id=call_id,
                node_id=node_id,
                occurred_at_ms=current.started_at_ms or current.created_at_ms,
                payload={
                    "operator_id": current.operator_id,
                    "call_no": current.call_no,
                    "kind": current.kind,
                    "item_index": current.item_index,
                    "replica_index": current.replica_index,
                    "input": current.input,
                },
            )
        )
        if current.state not in {"created", "running"}:
            drafts.append(_operator_final_event(current, node_id))
    elif previous.state != current.state and current.state not in {"created", "running"}:
        drafts.append(_operator_final_event(current, node_id))
    return drafts


def _operator_final_event(current: Any, node_id: str) -> RuntimeEventDraft:
    return RuntimeEventDraft(
        type="operator.call_finished",
        entity_type="operator_call",
        entity_id=str(current.id),
        node_id=node_id,
        occurred_at_ms=current.ended_at_ms or current.updated_at_ms,
        payload={
            "operator_id": current.operator_id,
            "state": current.state,
            "output": current.output if current.state == "completed" else None,
            "error": current.error.to_record() if current.error else None,
            "resource_usage": current.resource_usage.to_record(),
        },
    )


def _node_final_time(execution: Any) -> TimestampMs:
    return execution.ended_at_ms or execution.updated_at_ms


def _event_priority(event_type: str) -> int:
    order = {
        "invocation.created": 0,
        "node.execution_created": 10,
        "operator.call_started": 20,
        "operator.call_finished": 30,
        "node.state_changed": 40,
        "edge.evaluated": 50,
        "invocation.context_updated": 60,
        "invocation.state_changed": 70,
        "invocation.output_published": 80,
    }
    return order.get(event_type, 100)
