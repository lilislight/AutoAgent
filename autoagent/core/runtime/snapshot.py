from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.runtime.event import RuntimeEvent
from autoagent.core.runtime.execution import NodeExecution, OperatorCall
from autoagent.core.runtime.invocation import Invocation
from autoagent.core.runtime.session import Session
from autoagent.core.runtime.time import TimestampMs, utc_timestamp_ms


RuntimeBoundary = Literal[
    "node.activation_ready",
    "node.input_ready",
    "node.call_outputs_ready",
    "node.output_ready",
    "node.committed",
    "routing.committed",
    "wait.committed",
    "resume.committed",
    "recovery.interrupted",
    "invocation.completed",
    "invocation.failed",
    "invocation.cancelled",
]


class ExecutionSnapshot(BaseModel):
    """A restartable execution image at one Invocation event sequence.

    Sequence zero is the durable Genesis image. Later images are optional
    compaction points; boundary events between images carry reducer state.
    Python tasks, coroutines and locks are intentionally excluded.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID = Field(default_factory=uuid4)
    namespace: str
    workflow_id: str
    session_id: UUID
    invocation_id: UUID
    through_sequence: int = Field(ge=0)
    state: dict[str, Any]
    created_at_ms: TimestampMs = Field(default_factory=utc_timestamp_ms)

    @classmethod
    def capture(
        cls,
        session: Session,
        invocation: Invocation,
        *,
        through_sequence: int | None = None,
    ) -> "ExecutionSnapshot":
        return cls(
            namespace=session.namespace,
            workflow_id=session.workflow_id,
            session_id=session.id,
            invocation_id=invocation.id,
            through_sequence=(
                invocation.event_sequence
                if through_sequence is None
                else through_sequence
            ),
            state=capture_execution_state(session, invocation),
        )

    def restore(self) -> tuple[Session, Invocation]:
        return restore_execution_state(self.state)


def capture_execution_state(
    session: Session,
    invocation: Invocation,
) -> dict[str, Any]:
    """Capture all mutable state required to resume at a stable boundary."""

    node_records: list[dict[str, Any]] = []
    for execution in invocation.node_executions:
        record = execution.to_record(invocation.id)
        record["operator_calls"] = [
            call.to_record(execution.id) for call in execution.operator_calls
        ]
        node_records.append(record)
    return deepcopy(
        {
            "session": session.to_record(),
            "invocation": invocation.to_record(session.id),
            "node_executions": node_records,
        }
    )


def restore_execution_state(state: dict[str, Any]) -> tuple[Session, Invocation]:
    node_executions: list[NodeExecution] = []
    for record in state.get("node_executions", []):
        calls = [
            OperatorCall.from_record(call)
            for call in record.get("operator_calls", [])
        ]
        node_executions.append(NodeExecution.from_record(record, operator_calls=calls))
    invocation = Invocation.from_record(
        state["invocation"],
        node_executions=node_executions,
    )
    session = Session.from_record(state["session"], invocations=[invocation])
    return session, invocation


def reduce_execution_state(
    snapshot: ExecutionSnapshot,
    events: tuple[RuntimeEvent, ...],
    *,
    through_sequence: int | None = None,
) -> tuple[Session, Invocation]:
    """Rebuild a boundary state; observation events are reducer no-ops.

    V1 boundary events contain a complete reducer image. This keeps replay
    deterministic while the event count is deliberately small. Periodic
    snapshots allow a later version to switch individual boundaries to deltas
    without changing the public rebuild contract.
    """

    state = deepcopy(snapshot.state)
    cursor = snapshot.through_sequence
    for event in sorted(events, key=lambda item: item.sequence):
        if event.sequence <= cursor:
            continue
        if through_sequence is not None and event.sequence > through_sequence:
            break
        if event.role == "boundary":
            reducer_state = event.payload.get("reducer_state")
            if reducer_state is None:
                raise ValueError(
                    f"Boundary event {event.sequence} has no reducer_state."
                )
            state = deepcopy(reducer_state)
        cursor = event.sequence
    return restore_execution_state(state)
