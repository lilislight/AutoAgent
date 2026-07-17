from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from autoagent.compiler import WorkflowVersionSnapshot
from autoagent.observer.models import (
    EdgeEvaluationView,
    InvocationDetail,
    InvocationSummary,
    NodeExecutionView,
    ObservationBootstrap,
    OperatorCallView,
    RuntimeEventPage,
    RuntimeProjection,
    SessionSummary,
    TimelineSpan,
    TimelineView,
    WorkflowEdgeView,
    WorkflowGraphView,
    WorkflowNodeView,
    WorkflowSummary,
)
from autoagent.observer.projection import project_runtime_events
from autoagent.observer.security import redact_sensitive_data
from autoagent.runtime import (
    Invocation,
    RuntimeEvent,
    RuntimeSerializer,
    RuntimeStore,
    Session,
)


class ObservationService:
    """Read-only application service shared by HTTP routes and future tooling."""

    def __init__(
        self,
        runtime_store: RuntimeStore,
        *,
        serializer: RuntimeSerializer | None = None,
        bootstrap_event_limit: int = 1000,
        event_page_size: int = 1000,
        redactor: Callable[[Any], Any] | None = None,
    ) -> None:
        if bootstrap_event_limit < 1:
            raise ValueError("bootstrap_event_limit must be positive.")
        if not 1 <= event_page_size <= 9999:
            raise ValueError("event_page_size must be between 1 and 9999.")
        self.runtime_store = runtime_store
        self.serializer = serializer or runtime_store.serializer
        self.bootstrap_event_limit = bootstrap_event_limit
        self.event_page_size = event_page_size
        self.redactor = redactor or redact_sensitive_data

    async def list_workflows(
        self,
        *,
        namespace: str | None = None,
    ) -> tuple[WorkflowSummary, ...]:
        snapshots = await self.runtime_store.alist_workflow_snapshots(
            namespace=namespace
        )
        return tuple(_workflow_summary(snapshot) for snapshot in snapshots)

    async def get_graph(
        self,
        *,
        workflow_id: str,
        definition_hash: str,
        namespace: str | None = None,
        operator_manifest_hash: str | None = None,
    ) -> WorkflowGraphView:
        snapshots = await self.runtime_store.alist_workflow_snapshots(
            namespace=namespace,
            workflow_id=workflow_id,
        )
        matches = [
            value
            for value in snapshots
            if value.definition_hash == definition_hash
            and (
                operator_manifest_hash is None
                or value.operator_manifest_hash == operator_manifest_hash
            )
        ]
        if not matches:
            raise KeyError(
                f"Unknown Workflow snapshot: {workflow_id}/{definition_hash}"
            )
        if operator_manifest_hash is None:
            matches.sort(key=lambda item: item.operator_manifest_hash)
        return _workflow_graph(matches[0])

    async def list_sessions(
        self,
        *,
        namespace: str | None = None,
        workflow_id: str | None = None,
    ) -> tuple[SessionSummary, ...]:
        sessions = await self.runtime_store.alist_sessions(
            namespace=namespace,
            workflow_id=workflow_id,
        )
        return tuple(_session_summary(session) for session in sessions)

    async def list_invocations(
        self,
        session_id: UUID,
    ) -> tuple[InvocationSummary, ...]:
        invocations = await self.runtime_store.alist_session_invocations(session_id)
        return tuple(_invocation_summary(value) for value in invocations)

    async def get_invocation(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
    ) -> InvocationDetail:
        session, invocation = await self._load_scope(session_id, invocation_id)
        del session
        return self._invocation_detail(invocation)

    async def get_timeline(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
    ) -> TimelineView:
        _, invocation = await self._load_scope(session_id, invocation_id)
        return _timeline(invocation)

    async def list_events(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
        visibility: str | None = None,
    ) -> tuple[RuntimeEvent, ...]:
        await self._load_scope(session_id, invocation_id)
        values = await self.runtime_store.alist_runtime_events(
            session_id=session_id,
            invocation_id=invocation_id,
            after_sequence=after_sequence,
            before_sequence=before_sequence,
            limit=limit,
            visibility=visibility,
        )
        return tuple(self._event_view(value) for value in values)

    async def list_event_page(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
        after_sequence: int = 0,
        before_sequence: int | None = None,
        limit: int = 1000,
        visibility: str | None = None,
    ) -> RuntimeEventPage:
        """Read one bounded event page without an extra count query."""

        await self._load_scope(session_id, invocation_id)
        values = await self.runtime_store.alist_runtime_events(
            session_id=session_id,
            invocation_id=invocation_id,
            after_sequence=after_sequence,
            before_sequence=before_sequence,
            limit=limit + 1,
            visibility=visibility,
        )
        selected = values[-limit:] if before_sequence is not None else values[:limit]
        page = tuple(self._event_view(value) for value in selected)
        return RuntimeEventPage(
            events=page,
            next_after_sequence=(
                page[-1].sequence if page else after_sequence
            ),
            previous_before_sequence=(
                page[0].sequence if page else before_sequence
            ),
            has_more=len(values) > limit,
        )

    async def bootstrap(
        self,
        *,
        session_id: UUID,
        invocation_id: UUID,
    ) -> ObservationBootstrap:
        session, invocation = await self._load_scope(session_id, invocation_id)
        stored_checkpoint = await self.runtime_store.aload_projection_checkpoint(
            invocation_id=invocation_id
        )
        checkpoint = (
            RuntimeProjection.model_validate(stored_checkpoint[1])
            if stored_checkpoint is not None
            else project_runtime_events(invocation_id, ())
        )
        original_checkpoint_sequence = checkpoint.through_sequence
        tail_events: list[RuntimeEvent] = []
        cursor = checkpoint.through_sequence
        while True:
            page = await self.runtime_store.alist_runtime_events(
                session_id=session_id,
                invocation_id=invocation_id,
                after_sequence=cursor,
                limit=self.event_page_size,
            )
            if not page:
                break
            cursor = page[-1].sequence
            tail_events.extend(page)
            overflow = len(tail_events) - self.bootstrap_event_limit
            if overflow > 0:
                checkpoint = project_runtime_events(
                    invocation_id,
                    tail_events[:overflow],
                    base_projection=checkpoint,
                )
                del tail_events[:overflow]
            if len(page) < self.event_page_size:
                break

        projection = project_runtime_events(
            invocation_id,
            tail_events,
            base_projection=checkpoint,
        )
        if checkpoint.through_sequence > original_checkpoint_sequence:
            await self.runtime_store.asave_projection_checkpoint(
                invocation_id=invocation_id,
                through_sequence=checkpoint.through_sequence,
                projection=checkpoint.model_dump(mode="python"),
            )
        if invocation.workflow_definition_hash is None:
            raise ValueError("Invocation does not contain a Workflow definition hash.")
        graph = await self.get_graph(
            namespace=session.namespace,
            workflow_id=invocation.workflow_id,
            definition_hash=invocation.workflow_definition_hash,
            operator_manifest_hash=invocation.workflow_operator_manifest_hash,
        )
        return ObservationBootstrap(
            graph=graph,
            session=_session_summary(session),
            invocation=self._invocation_detail(invocation),
            timeline=_timeline(invocation),
            checkpoint=self._projection_view(checkpoint),
            events=tuple(self._event_view(value) for value in tail_events),
            projection=self._projection_view(projection),
        )

    async def _load_scope(
        self,
        session_id: UUID,
        invocation_id: UUID,
    ) -> tuple[Session, Invocation]:
        session = await self.runtime_store.aload_session(session_id)
        if session is None:
            raise KeyError(f"Unknown Session: {session_id}")
        invocation = session.get_invocation(invocation_id)
        if invocation is None:
            raise KeyError(
                f"Invocation {invocation_id} does not belong to Session {session_id}."
            )
        return session, invocation

    def _invocation_detail(self, invocation: Invocation) -> InvocationDetail:
        return InvocationDetail(
            **_invocation_summary(invocation).model_dump(),
            input=self._json_view(invocation.input),
            context=self._json_view(invocation.context.to_record()),
            result=self._json_view(invocation.result),
            error=self._redact(
                invocation.error.to_record() if invocation.error else None
            ),
            node_executions=tuple(
                self._node_execution_view(value)
                for value in invocation.node_executions
            ),
        )

    def _node_execution_view(self, execution: Any) -> NodeExecutionView:
        return NodeExecutionView(
            id=execution.id,
            node_id=execution.node_id,
            sequence=execution.sequence,
            state=execution.state,
            input=self._json_view(execution.input),
            output=self._json_view(execution.output),
            error=self._redact(
                execution.error.to_record() if execution.error else None
            ),
            incoming_activations=tuple(
                value.to_record() for value in execution.incoming_activations
            ),
            edge_evaluations=tuple(
                EdgeEvaluationView(
                    id=value.id,
                    edge_id=value.edge_id,
                    target_node_id=value.target_node_id,
                    state=value.state,
                    selected=value.selected,
                    reason=value.reason,
                    created_at_ms=value.created_at_ms,
                )
                for value in execution.edge_evaluations
            ),
            operator_calls=tuple(
                OperatorCallView(
                    id=value.id,
                    operator_id=value.operator_id,
                    call_no=value.call_no,
                    kind=value.kind,
                    item_index=value.item_index,
                    replica_index=value.replica_index,
                    state=value.state,
                    input=self._json_view(value.input),
                    output=self._json_view(value.output),
                    error=self._redact(
                        value.error.to_record() if value.error else None
                    ),
                    resource_usage=value.resource_usage.to_record(),
                    started_at_ms=value.started_at_ms,
                    ended_at_ms=value.ended_at_ms,
                    created_at_ms=value.created_at_ms,
                    updated_at_ms=value.updated_at_ms,
                )
                for value in execution.operator_calls
            ),
            resource_usage=execution.resource_usage.to_record(),
            started_at_ms=execution.started_at_ms,
            ended_at_ms=execution.ended_at_ms,
            created_at_ms=execution.created_at_ms,
            updated_at_ms=execution.updated_at_ms,
        )

    def _json_view(self, value: Any) -> Any:
        if value is None:
            return None
        return self._redact(
            self.serializer.json_view(self.serializer.dumps(value))
        )

    def _event_view(self, event: RuntimeEvent) -> RuntimeEvent:
        return event.model_copy(update={"payload": self._json_view(event.payload)})

    def _projection_view(self, projection: RuntimeProjection) -> RuntimeProjection:
        return projection.model_copy(
            update={
                "node_executions": {
                    execution_id: execution.model_copy(
                        update={
                            "input": self._json_view(execution.input),
                            "output": self._json_view(execution.output),
                            "error": self._json_view(execution.error),
                        }
                    )
                    for execution_id, execution in projection.node_executions.items()
                }
            }
        )

    def _redact(self, value: Any) -> Any:
        return self.redactor(value)


def _workflow_summary(snapshot: WorkflowVersionSnapshot) -> WorkflowSummary:
    return WorkflowSummary(
        workflow_id=snapshot.workflow_id,
        workflow_version=snapshot.workflow_version,
        definition_hash=snapshot.definition_hash,
        operator_manifest_hash=snapshot.operator_manifest_hash,
        name=snapshot.definition.get("name"),
        description=snapshot.definition.get("description"),
    )


def _workflow_graph(snapshot: WorkflowVersionSnapshot) -> WorkflowGraphView:
    definition = snapshot.definition
    return WorkflowGraphView(
        **_workflow_summary(snapshot).model_dump(),
        nodes=tuple(
            WorkflowNodeView(
                id=str(node["id"]),
                local_id=node.get("local_id"),
                workflow_path=tuple(node.get("workflow_path", [])),
                name=node.get("name"),
                description=node.get("description"),
                capability=dict(node["capability"]),
                entry=bool(node.get("entry", False)),
                exit=bool(node.get("exit", False)),
                policy=node.get("policy"),
                input_contract=dict(node["input_contract"]),
                operator_output_contract=dict(node["operator_output_contract"]),
                output_contract=dict(node["output_contract"]),
            )
            for node in definition.get("nodes", [])
        ),
        edges=tuple(
            WorkflowEdgeView(
                id=str(edge["id"]),
                local_id=edge.get("local_id"),
                workflow_path=tuple(edge.get("workflow_path", [])),
                from_node=str(edge["from_node"]),
                to_node=str(edge["to_node"]),
                order=int(edge["order"]),
                condition=edge.get("condition"),
                policy=edge.get("policy"),
            )
            for edge in definition.get("edges", [])
        ),
        entry_node_ids=tuple(definition.get("entry_node_ids", [])),
        exit_node_ids=tuple(definition.get("exit_node_ids", [])),
        loop_regions=tuple(definition.get("loop_regions", [])),
    )


def _session_summary(session: Session) -> SessionSummary:
    return SessionSummary(
        id=session.id,
        namespace=session.namespace,
        workflow_id=session.workflow_id,
        session_key=session.session_key,
        current_invocation_id=session.current_invocation_id,
        invocation_count=len(session.invocations),
        created_at_ms=session.created_at_ms,
        updated_at_ms=session.updated_at_ms,
    )


def _invocation_summary(invocation: Invocation) -> InvocationSummary:
    return InvocationSummary(
        id=invocation.id,
        workflow_id=invocation.workflow_id,
        workflow_version=invocation.workflow_version,
        definition_hash=invocation.workflow_definition_hash,
        operator_manifest_hash=invocation.workflow_operator_manifest_hash,
        entry_node_id=invocation.entry_node_id,
        state=invocation.state,
        created_at_ms=invocation.created_at_ms,
        updated_at_ms=invocation.updated_at_ms,
    )


def _timeline(invocation: Invocation) -> TimelineView:
    spans: list[TimelineSpan] = []
    for execution in invocation.node_executions:
        node_start = execution.started_at_ms or execution.created_at_ms
        node_duration = (
            execution.resource_usage.duration_ms
            if execution.resource_usage.duration_ms > 0
            else (
                max(0, execution.ended_at_ms - node_start)
                if execution.ended_at_ms is not None
                else None
            )
        )
        spans.append(
            TimelineSpan(
                id=str(execution.id),
                kind="node_execution",
                node_id=execution.node_id,
                label=execution.node_id,
                state=execution.state,
                sequence=execution.sequence * 10_000,
                started_at_ms=node_start,
                ended_at_ms=execution.ended_at_ms,
                duration_ms=node_duration,
            )
        )
        for call in execution.operator_calls:
            call_start = call.started_at_ms or call.created_at_ms
            call_duration = (
                call.resource_usage.duration_ms
                if call.resource_usage.duration_ms > 0
                else (
                    max(0, call.ended_at_ms - call_start)
                    if call.ended_at_ms is not None
                    else None
                )
            )
            spans.append(
                TimelineSpan(
                    id=str(call.id),
                    kind="operator_call",
                    parent_id=str(execution.id),
                    node_id=execution.node_id,
                    label=call.operator_id,
                    state=call.state,
                    sequence=execution.sequence * 10_000 + call.call_no,
                    started_at_ms=call_start,
                    ended_at_ms=call.ended_at_ms,
                    duration_ms=call_duration,
                )
            )
    spans.sort(key=lambda value: (value.started_at_ms, value.sequence, value.id))
    terminal = invocation.state in {
        "completed",
        "failed",
        "cancelled",
        "interrupted",
    }
    return TimelineView(
        invocation_id=invocation.id,
        started_at_ms=invocation.created_at_ms,
        ended_at_ms=invocation.updated_at_ms if terminal else None,
        spans=tuple(spans),
    )
