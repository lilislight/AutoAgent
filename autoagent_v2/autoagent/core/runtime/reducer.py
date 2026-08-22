"""The only Runtime State transition implementation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType

from ..errors import RuntimeTransitionError
from .events import (
    RUNTIME_EVENT_SCHEMA_VERSION,
    InvocationCancelled,
    InvocationCompleted,
    InvocationFailed,
    InvocationOpened,
    InvocationStarted,
    InvocationWaiting,
    InvocationRecoveryRequested,
    ChildInvocationLinked,
    NodeOccurrenceCompleted,
    NodeOccurrenceFailed,
    NodeOccurrenceStarted,
    OperatorCallCompleted,
    OperatorCallFailed,
    OperatorCallStarted,
    NodeOccurrenceWaiting,
    WaitResumed,
    RuntimeEvent,
    SchedulerInitialized,
    SessionOpened,
)
from .scheduling import SchedulerDelta, boundary_key, occurrence_key
from .operations import (
    StateOperationBatch,
    apply_operation_batch,
    diff_runtime_states,
)
from .state import (
    InvocationState,
    ChildInvocationState,
    NodeOccurrenceState,
    OperatorCallState,
    RuntimeState,
    SchedulerState,
    SessionState,
    WaitState,
)
from .values import freeze
from ..context import ContextOperation, apply_context_operation


class StateReducer:
    """Pure reducer for State Operation Batches and persisted Event prefixes.

    Runtime transition objects are planning inputs only. Persisted Events carry
    the resulting operation batches, so replay never needs to reinterpret old
    scheduling or policy code.
    """

    def prepare(self, state: RuntimeState, event: RuntimeEvent) -> RuntimeEvent:
        """Plan an unsealed semantic Event into one atomic operation batch."""

        return self.plan(state, event)[0]

    def plan(
        self, state: RuntimeState, event: RuntimeEvent
    ) -> tuple[RuntimeEvent, RuntimeState]:
        """Return the sealed Event and already-validated candidate State."""

        if event.from_state_version is not None:
            raise ValueError("A persisted Runtime Event cannot be planned again.")
        self._validate_event_header(state, event)
        session, invocation = self._transition(state, event)
        candidate = RuntimeState(
            session=session,
            invocation=invocation,
            state_version=state.state_version,
            sequence=state.sequence,
            last_event_id=state.last_event_id,
            last_event_digest=state.last_event_digest,
            last_event_semantic_digest=state.last_event_semantic_digest,
        )
        operations = diff_runtime_states(state, candidate)
        if not operations:
            return (
                replace(
                    event,
                    from_state_version=state.state_version,
                    to_state_version=state.state_version,
                    logs=tuple(
                        replace(log, state_version=state.state_version)
                        for log in event.logs
                    ),
                ),
                candidate,
            )
        batch = StateOperationBatch(
            from_state_version=state.state_version,
            to_state_version=state.state_version + 1,
            operations=operations,
            occurred_at_ns=event.occurred_at_ns,
            id=f"{event.id}:state:{state.state_version + 1}",
        )
        return (
            replace(
                event,
                from_state_version=batch.from_state_version,
                to_state_version=batch.to_state_version,
                operation_batches=(batch,),
                logs=tuple(
                    replace(log, state_version=batch.to_state_version)
                    for log in event.logs
                ),
            ),
            replace(candidate, state_version=batch.to_state_version),
        )

    def apply(self, state: RuntimeState, event: RuntimeEvent) -> RuntimeState:
        if event.sequence == state.sequence:
            if event.id != state.last_event_id:
                raise RuntimeTransitionError(
                    "EVENT_SEQUENCE_CONFLICT",
                    f"Sequence {event.sequence} already contains another Event.",
                )
            if (
                event.operation_batches
                and _event_digest(event) == state.last_event_digest
            ) or (
                not event.operation_batches
                and _semantic_event_digest(event)
                == state.last_event_semantic_digest
            ):
                return state
            raise RuntimeTransitionError(
                "EVENT_SEQUENCE_CONFLICT",
                f"Sequence {event.sequence} already contains another Event.",
            )
        if event.from_state_version is None:
            event, _candidate = self.plan(state, event)
        self._validate_event_header(state, event)
        digest = _event_digest(event)
        current = state
        for batch in event.operation_batches:
            current = self.apply_batch(current, batch)
        return replace(
            current,
            sequence=event.sequence,
            last_event_id=event.id,
            last_event_digest=digest,
            last_event_semantic_digest=_semantic_event_digest(event),
        )

    def commit_event_metadata(
        self, state: RuntimeState, event: RuntimeEvent
    ) -> RuntimeState:
        """Attach a flushed Event identity to an already-applied live State."""

        if (
            event.from_state_version is None
            or event.to_state_version != state.state_version
        ):
            raise RuntimeTransitionError(
                "EVENT_STATE_VERSION_GAP",
                "Flushed Runtime Event does not end at the live State version.",
            )
        expected = state.sequence + 1
        if event.sequence != expected:
            raise RuntimeTransitionError(
                "EVENT_SEQUENCE_GAP",
                f"Expected Event sequence {expected}, got {event.sequence}.",
            )
        return replace(
            state,
            sequence=event.sequence,
            last_event_id=event.id,
            last_event_digest=_event_digest(event),
            last_event_semantic_digest=_semantic_event_digest(event),
        )

    def apply_batch(
        self, state: RuntimeState, batch: StateOperationBatch
    ) -> RuntimeState:
        """Apply exactly one atomic batch without any Event semantics."""

        if batch.from_state_version != state.state_version:
            raise RuntimeTransitionError(
                "STATE_VERSION_GAP",
                f"Expected State version {state.state_version}, got "
                f"{batch.from_state_version}.",
            )
        record = apply_operation_batch(state.to_record(), batch)
        record["state_version"] = batch.to_state_version
        candidate = RuntimeState.from_record(record)
        if candidate.sequence != state.sequence:
            raise RuntimeTransitionError(
                "STATE_EVENT_METADATA_MUTATION",
                "State Operations cannot mutate Runtime Event sequence metadata.",
            )
        return candidate

    def _validate_event_header(
        self, state: RuntimeState, event: RuntimeEvent
    ) -> None:
        expected = state.sequence + 1
        if event.sequence != expected and event.sequence != state.sequence:
            raise RuntimeTransitionError(
                "EVENT_SEQUENCE_GAP",
                f"Expected Event sequence {expected}, got {event.sequence}.",
            )
        if event.schema_version != RUNTIME_EVENT_SCHEMA_VERSION:
            raise RuntimeTransitionError(
                "EVENT_SCHEMA_UNSUPPORTED",
                f"Unsupported Runtime Event schema {event.schema_version}.",
            )
        if state.session is not None and event.session_id != state.session.id:
            raise RuntimeTransitionError(
                "EVENT_SESSION_MISMATCH", "Runtime Event belongs to another Session."
            )
        if state.session is not None and event.occurred_at_ns < state.session.updated_at_ns:
            raise RuntimeTransitionError(
                "EVENT_TIME_REGRESSION",
                "Runtime Event time cannot move backwards within a Session.",
            )
        if event.from_state_version is not None:
            if event.from_state_version != state.state_version:
                raise RuntimeTransitionError(
                    "EVENT_STATE_VERSION_GAP",
                    f"Expected Event from_state_version {state.state_version}, got "
                    f"{event.from_state_version}.",
                )

    def reduce(self, events: tuple[RuntimeEvent, ...]) -> RuntimeState:
        state = RuntimeState()
        for event in events:
            state = self.apply(state, event)
        return state

    def preview_context_patch(
        self,
        state: RuntimeState,
        occurrence_id: str,
        patch,
        *,
        sequence: int | None = None,
    ) -> tuple[object, object]:
        """Validate one pending patch and return candidate Session/Invocation contexts."""

        session = state.session
        invocation = state.invocation
        if session is None or invocation is None:
            raise RuntimeTransitionError(
                "CONTEXT_STATE_MISSING", "Context Patch requires active Runtime State."
            )
        occurrence = invocation.scheduler.occurrences.get(occurrence_id)
        if occurrence is None or occurrence.status != "running":
            raise RuntimeTransitionError(
                "NODE_OCCURRENCE_NOT_RUNNING",
                "Context Patch requires a running Node Occurrence.",
            )
        candidate_session, candidate_invocation = self._apply_context_patch(
            session,
            invocation,
            occurrence,
            patch,
            sequence or state.state_version + 1,
        )
        return candidate_session.context, candidate_invocation.context

    def _transition(
        self, state: RuntimeState, event: RuntimeEvent
    ) -> tuple[SessionState, InvocationState | None]:
        payload = event.payload
        if isinstance(payload, SessionOpened):
            if state.session is not None:
                raise RuntimeTransitionError(
                    "SESSION_ALREADY_OPEN",
                    f"Session {event.session_id!r} already exists.",
                )
            if not payload.workflow_id:
                raise RuntimeTransitionError(
                    "SESSION_WORKFLOW_REQUIRED", "Session workflow_id cannot be empty."
                )
            context = payload.context
            if not isinstance(context, Mapping):
                raise RuntimeTransitionError(
                    "SESSION_CONTEXT_INVALID", "Session Context must be a mapping."
                )
            return (
                SessionState(
                    id=event.session_id,
                    workflow_id=payload.workflow_id,
                    context=context,
                    created_at_ns=event.occurred_at_ns,
                    updated_at_ns=event.occurred_at_ns,
                ),
                None,
            )

        session = state.session
        if session is None:
            raise RuntimeTransitionError(
                "SESSION_NOT_OPEN", "An Invocation Event requires an open Session."
            )
        if event.invocation_id is None:
            raise RuntimeTransitionError(
                "INVOCATION_ID_REQUIRED", "Invocation Event requires invocation_id."
            )

        if isinstance(payload, InvocationOpened):
            if state.invocation is not None and not state.invocation.terminal:
                raise RuntimeTransitionError(
                    "INVOCATION_ALREADY_ACTIVE",
                    f"Session {session.id!r} already has an active Invocation.",
                )
            if not payload.workflow_revision_id or not payload.entry_node_id:
                raise RuntimeTransitionError(
                    "INVOCATION_DEFINITION_REQUIRED",
                    "Invocation revision and Entry Node cannot be empty.",
                )
            invocation = InvocationState(
                id=event.invocation_id,
                workflow_revision_id=payload.workflow_revision_id,
                entry_node_id=payload.entry_node_id,
                status="created",
                input=payload.input,
                context=freeze({}),
                created_at_ns=event.occurred_at_ns,
            )
            return (
                replace(
                    session,
                    latest_invocation_id=event.invocation_id,
                    updated_at_ns=event.occurred_at_ns,
                ),
                invocation,
            )

        invocation = state.invocation
        if invocation is None or event.invocation_id != invocation.id:
            raise RuntimeTransitionError(
                "INVOCATION_MISMATCH",
                "Runtime Event does not target the Session's latest Invocation.",
            )
        if isinstance(payload, InvocationStarted):
            _require_status(invocation, {"created"}, payload.kind)
            invocation = replace(
                invocation,
                status="running",
                started_at_ns=event.occurred_at_ns,
            )
            return replace(session, updated_at_ns=event.occurred_at_ns), invocation

        if isinstance(payload, InvocationWaiting):
            _require_status(invocation, {"running"}, payload.kind)
            if not any(
                item.status == "waiting"
                for item in invocation.scheduler.waits.values()
            ):
                raise RuntimeTransitionError(
                    "INVOCATION_WAIT_MISSING",
                    "Invocation cannot wait without an active Wait.",
                )
            if any(
                item.status in {"ready", "running"}
                for item in invocation.scheduler.occurrences.values()
            ):
                raise RuntimeTransitionError(
                    "INVOCATION_STILL_RUNNABLE",
                    "Invocation cannot wait while runnable work exists.",
                )
            return session, replace(invocation, status="waiting")

        if isinstance(payload, SchedulerInitialized):
            _require_status(invocation, {"running"}, payload.kind)
            if invocation.scheduler.initialized:
                raise RuntimeTransitionError(
                    "SCHEDULER_ALREADY_INITIALIZED",
                    "Invocation Scheduler is already initialized.",
                )
            scheduler = self._apply_scheduler_delta(
                replace(invocation.scheduler, initialized=True),
                payload.delta,
                event.occurred_at_ns,
            )
            return session, replace(invocation, scheduler=scheduler)

        if isinstance(payload, NodeOccurrenceStarted):
            _require_status(invocation, {"running"}, payload.kind)
            scheduler = invocation.scheduler
            occurrence = scheduler.occurrences.get(payload.occurrence_id)
            if occurrence is None or occurrence.status != "ready":
                raise RuntimeTransitionError(
                    "NODE_OCCURRENCE_NOT_READY",
                    f"Node Occurrence {payload.occurrence_id!r} is not ready.",
                )
            occurrences = dict(scheduler.occurrences)
            occurrences[payload.occurrence_id] = replace(
                occurrence,
                status="running",
                started_at_ns=event.occurred_at_ns,
                started_state_version=state.state_version + 1,
            )
            ready = tuple(
                item for item in scheduler.ready if item != payload.occurrence_id
            )
            return session, replace(
                invocation,
                scheduler=replace(
                    scheduler,
                    ready=ready,
                    occurrences=MappingProxyType(occurrences),
                ),
            )

        if isinstance(payload, (NodeOccurrenceCompleted, NodeOccurrenceFailed)):
            _require_status(invocation, {"running"}, payload.kind)
            scheduler = invocation.scheduler
            occurrence = scheduler.occurrences.get(payload.occurrence_id)
            if occurrence is None or occurrence.status != "running":
                raise RuntimeTransitionError(
                    "NODE_OCCURRENCE_NOT_RUNNING",
                    f"Node Occurrence {payload.occurrence_id!r} is not running.",
                )
            occurrences = dict(scheduler.occurrences)
            occurrences[payload.occurrence_id] = replace(
                occurrence,
                status=(
                    "completed"
                    if isinstance(payload, NodeOccurrenceCompleted)
                    else "failed"
                ),
                output=(payload.output if isinstance(payload, NodeOccurrenceCompleted) else None),
                error=(payload.error if isinstance(payload, NodeOccurrenceFailed) else None),
                completed_at_ns=event.occurred_at_ns,
                metrics=(
                    payload.metrics
                    if isinstance(payload, NodeOccurrenceCompleted)
                    else occurrence.metrics
                ),
            )
            if any(
                resolution.activation is not None
                and resolution.activation.source_occurrence_id
                != payload.occurrence_id
                for resolution in (
                    *payload.delta.resolutions,
                    *payload.delta.boundary_resolutions,
                )
            ):
                raise RuntimeTransitionError(
                    "ACTIVATION_SOURCE_MISMATCH",
                    "Outgoing Activation must reference the completing Node Occurrence.",
                )
            scheduler = replace(
                scheduler, occurrences=MappingProxyType(occurrences)
            )
            if isinstance(payload, NodeOccurrenceCompleted):
                session, invocation = self._apply_context_patch(
                    session,
                    invocation,
                    occurrence,
                    payload.patch,
                    state.state_version + 1,
                )
            scheduler = self._apply_scheduler_delta(
                scheduler, payload.delta, event.occurred_at_ns
            )
            return session, replace(invocation, scheduler=scheduler)

        if isinstance(payload, OperatorCallStarted):
            _require_status(invocation, {"running"}, payload.kind)
            scheduler = invocation.scheduler
            occurrence = scheduler.occurrences.get(payload.occurrence_id)
            if occurrence is None or occurrence.status != "running":
                raise RuntimeTransitionError(
                    "OPERATOR_CALL_OCCURRENCE_NOT_RUNNING",
                    "Operator Call requires a running Node Occurrence.",
                )
            if payload.call_id in scheduler.operator_calls:
                raise RuntimeTransitionError(
                    "OPERATOR_CALL_DUPLICATE",
                    f"Operator Call {payload.call_id!r} already exists.",
                )
            calls = dict(scheduler.operator_calls)
            calls[payload.call_id] = OperatorCallState(
                id=payload.call_id,
                occurrence_id=payload.occurrence_id,
                operator_id=payload.operator_id,
                unit_index=payload.unit_index,
                status="running",
                input=payload.input,
                started_at_ns=event.occurred_at_ns,
                attempt=payload.attempt,
                reason=payload.reason,
            )
            return session, replace(
                invocation,
                scheduler=replace(
                    scheduler, operator_calls=MappingProxyType(calls)
                ),
            )

        if isinstance(payload, (OperatorCallCompleted, OperatorCallFailed)):
            _require_status(invocation, {"running"}, payload.kind)
            scheduler = invocation.scheduler
            call = scheduler.operator_calls.get(payload.call_id)
            if call is None or call.status != "running":
                raise RuntimeTransitionError(
                    "OPERATOR_CALL_NOT_RUNNING",
                    f"Operator Call {payload.call_id!r} is not running.",
                )
            calls = dict(scheduler.operator_calls)
            calls[payload.call_id] = replace(
                call,
                status=(
                    "completed"
                    if isinstance(payload, OperatorCallCompleted)
                    else "failed"
                ),
                output=(payload.output if isinstance(payload, OperatorCallCompleted) else None),
                error=(payload.error if isinstance(payload, OperatorCallFailed) else None),
                completed_at_ns=event.occurred_at_ns,
            )
            return session, replace(
                invocation,
                scheduler=replace(
                    scheduler, operator_calls=MappingProxyType(calls)
                ),
            )

        if isinstance(payload, NodeOccurrenceWaiting):
            _require_status(invocation, {"running"}, payload.kind)
            scheduler = invocation.scheduler
            occurrence = scheduler.occurrences.get(payload.occurrence_id)
            if occurrence is None or occurrence.status != "running":
                raise RuntimeTransitionError(
                    "WAIT_OCCURRENCE_NOT_RUNNING",
                    "Wait requires a running Node Occurrence.",
                )
            if payload.wait_id in scheduler.waits:
                raise RuntimeTransitionError(
                    "WAIT_DUPLICATE", f"Wait {payload.wait_id!r} already exists."
                )
            occurrences = dict(scheduler.occurrences)
            occurrences[payload.occurrence_id] = replace(occurrence, status="waiting")
            waits = dict(scheduler.waits)
            waits[payload.wait_id] = WaitState(
                payload.wait_id,
                payload.occurrence_id,
                "waiting",
                payload.request,
                created_at_ns=event.occurred_at_ns,
            )
            return session, replace(
                invocation,
                scheduler=replace(
                    scheduler,
                    occurrences=MappingProxyType(occurrences),
                    waits=MappingProxyType(waits),
                ),
            )

        if isinstance(payload, WaitResumed):
            _require_status(invocation, {"running", "waiting"}, payload.kind)
            scheduler = invocation.scheduler
            wait = scheduler.waits.get(payload.wait_id)
            if wait is None or wait.status != "waiting":
                raise RuntimeTransitionError(
                    "WAIT_NOT_WAITING", f"Wait {payload.wait_id!r} is not waiting."
                )
            occurrence = scheduler.occurrences.get(wait.occurrence_id)
            if occurrence is None or occurrence.status != "waiting":
                raise RuntimeTransitionError(
                    "WAIT_OCCURRENCE_INVALID", "Wait occurrence is not waiting."
                )
            waits = dict(scheduler.waits)
            waits[payload.wait_id] = replace(
                wait,
                status="resumed",
                response=payload.response,
                resumed_at_ns=event.occurred_at_ns,
            )
            occurrences = dict(scheduler.occurrences)
            occurrences[wait.occurrence_id] = replace(occurrence, status="ready")
            return session, replace(
                invocation,
                status="running",
                scheduler=replace(
                    scheduler,
                    ready=(*scheduler.ready, wait.occurrence_id),
                    occurrences=MappingProxyType(occurrences),
                    waits=MappingProxyType(waits),
                ),
            )

        if isinstance(payload, InvocationRecoveryRequested):
            _require_status(invocation, {"running", "waiting"}, payload.kind)
            scheduler = invocation.scheduler
            calls = {
                key: replace(item, status="lost", completed_at_ns=event.occurred_at_ns)
                if item.status == "running"
                else item
                for key, item in scheduler.operator_calls.items()
            }
            occurrences = dict(scheduler.occurrences)
            recovered: list[str] = []
            for key, item in occurrences.items():
                if item.status == "running":
                    occurrences[key] = replace(
                        item,
                        status="ready",
                        started_at_ns=None,
                        started_state_version=None,
                        recovery_attempts=item.recovery_attempts + 1,
                    )
                    recovered.append(key)
            return session, replace(
                invocation,
                status=("running" if recovered else invocation.status),
                scheduler=replace(
                    scheduler,
                    ready=(*scheduler.ready, *recovered),
                    occurrences=MappingProxyType(occurrences),
                    operator_calls=MappingProxyType(calls),
                ),
            )

        if isinstance(payload, ChildInvocationLinked):
            _require_status(invocation, {"running"}, payload.kind)
            if payload.child_invocation_id in invocation.children:
                raise RuntimeTransitionError(
                    "CHILD_INVOCATION_DUPLICATE",
                    f"Child Invocation {payload.child_invocation_id!r} is already linked.",
                )
            occurrence = invocation.scheduler.occurrences.get(
                payload.parent_occurrence_id
            )
            if occurrence is None or occurrence.status != "running":
                raise RuntimeTransitionError(
                    "CHILD_PARENT_OCCURRENCE_NOT_RUNNING",
                    "Child Invocation requires a running parent Node Occurrence.",
                )
            children = dict(invocation.children)
            children[payload.child_invocation_id] = ChildInvocationState(
                payload.parent_occurrence_id,
                payload.child_session_id,
                payload.child_invocation_id,
                payload.workflow_id,
                payload.workflow_revision_id,
            )
            return session, replace(
                invocation, children=MappingProxyType(children)
            )

        if isinstance(payload, InvocationCompleted):
            _require_status(invocation, {"running"}, payload.kind)
            invocation = replace(
                invocation,
                status="completed",
                output=payload.output,
                completed_at_ns=event.occurred_at_ns,
            )
            return replace(session, updated_at_ns=event.occurred_at_ns), invocation

        if isinstance(payload, InvocationFailed):
            _require_status(invocation, {"created", "running", "waiting"}, payload.kind)
            scheduler = invocation.scheduler
            occurrences = {
                key: replace(
                    item,
                    status="cancelled",
                    completed_at_ns=event.occurred_at_ns,
                )
                if item.status in {"ready", "running", "waiting"}
                else item
                for key, item in scheduler.occurrences.items()
            }
            calls = {
                key: replace(
                    item,
                    status="cancelled",
                    completed_at_ns=event.occurred_at_ns,
                )
                if item.status == "running"
                else item
                for key, item in scheduler.operator_calls.items()
            }
            waits = {
                key: replace(item, status="cancelled")
                if item.status == "waiting"
                else item
                for key, item in scheduler.waits.items()
            }
            invocation = replace(
                invocation,
                status="failed",
                error=payload.error,
                completed_at_ns=event.occurred_at_ns,
                scheduler=replace(
                    scheduler,
                    ready=(),
                    occurrences=MappingProxyType(occurrences),
                    operator_calls=MappingProxyType(calls),
                    waits=MappingProxyType(waits),
                ),
            )
            return replace(session, updated_at_ns=event.occurred_at_ns), invocation

        if isinstance(payload, InvocationCancelled):
            _require_status(invocation, {"created", "running", "waiting"}, payload.kind)
            scheduler = invocation.scheduler
            occurrences = {
                key: replace(
                    item,
                    status="cancelled",
                    completed_at_ns=event.occurred_at_ns,
                )
                if item.status in {"ready", "running", "waiting"}
                else item
                for key, item in scheduler.occurrences.items()
            }
            calls = {
                key: replace(
                    item,
                    status="cancelled",
                    completed_at_ns=event.occurred_at_ns,
                )
                if item.status == "running"
                else item
                for key, item in scheduler.operator_calls.items()
            }
            waits = {
                key: replace(item, status="cancelled")
                if item.status == "waiting"
                else item
                for key, item in scheduler.waits.items()
            }
            invocation = replace(
                invocation,
                status="cancelled",
                cancel_reason=payload.reason,
                completed_at_ns=event.occurred_at_ns,
                scheduler=replace(
                    scheduler,
                    ready=(),
                    occurrences=MappingProxyType(occurrences),
                    operator_calls=MappingProxyType(calls),
                    waits=MappingProxyType(waits),
                ),
            )
            return replace(session, updated_at_ns=event.occurred_at_ns), invocation

        raise RuntimeTransitionError(
            "EVENT_TYPE_UNSUPPORTED",
            f"Unsupported Runtime Event type {type(payload).__name__}.",
        )

    def _apply_scheduler_delta(
        self,
        scheduler: SchedulerState,
        delta: SchedulerDelta,
        occurred_at_ns: int,
    ) -> SchedulerState:
        occurrences = dict(scheduler.occurrences)
        resolutions = dict(scheduler.resolutions)
        boundary_resolutions = dict(scheduler.boundary_resolutions)
        available_resolutions = {**resolutions, **boundary_resolutions}
        ready = list(scheduler.ready)

        for resolution in delta.resolutions:
            if resolution.id in resolutions:
                raise RuntimeTransitionError(
                    "EDGE_RESOLUTION_DUPLICATE",
                    f"Edge Resolution {resolution.id!r} already exists.",
                )
            if (
                resolution.activation is not None
                and resolution.activation.source_occurrence_id not in occurrences
            ):
                raise RuntimeTransitionError(
                    "ACTIVATION_SOURCE_UNKNOWN",
                    "Activation references an unknown source Node Occurrence.",
                )
            resolutions[resolution.id] = resolution
            available_resolutions[resolution.id] = resolution

        for resolution_id in delta.consumed_resolution_ids:
            if resolution_id not in resolutions:
                raise RuntimeTransitionError(
                    "EDGE_RESOLUTION_CONSUME_UNKNOWN",
                    f"Cannot consume unknown Edge Resolution {resolution_id!r}.",
                )
            del resolutions[resolution_id]

        for resolution in delta.boundary_resolutions:
            if resolution.id in boundary_resolutions:
                raise RuntimeTransitionError(
                    "LOOP_BOUNDARY_RESOLUTION_DUPLICATE",
                    f"Loop boundary Resolution {resolution.id!r} already exists.",
                )
            if (
                resolution.activation is not None
                and resolution.activation.source_occurrence_id not in occurrences
            ):
                raise RuntimeTransitionError(
                    "ACTIVATION_SOURCE_UNKNOWN",
                    "Loop boundary Activation references an unknown source occurrence.",
                )
            boundary_resolutions[resolution.id] = resolution
            available_resolutions[resolution.id] = resolution

        for closed in delta.closed_boundaries:
            matching = [
                key
                for key, item in boundary_resolutions.items()
                if boundary_key(item.loop_region_id, item.loop_scope) == closed
            ]
            if not matching:
                raise RuntimeTransitionError(
                    "LOOP_BOUNDARY_UNKNOWN",
                    f"Cannot close unknown Loop boundary {closed!r}.",
                )
            for key in matching:
                del boundary_resolutions[key]

        def activations_for(plan) -> tuple:
            values = []
            for resolution_id in plan.resolution_ids:
                resolution = available_resolutions.get(resolution_id)
                if resolution is None:
                    raise RuntimeTransitionError(
                        "OCCURRENCE_RESOLUTION_UNKNOWN",
                        f"Occurrence plan references unknown Resolution {resolution_id!r}.",
                    )
                if resolution.activation is not None:
                    values.append(resolution.activation)
            return tuple(values)

        ready_ids = {item.id for item in delta.ready}
        skipped_ids = {item.id for item in delta.skipped}
        if ready_ids & skipped_ids:
            raise RuntimeTransitionError(
                "NODE_OCCURRENCE_DELTA_CONFLICT",
                "One Node Occurrence cannot be both ready and skipped.",
            )
        for plan in (*delta.ready, *delta.skipped):
            if plan.id != occurrence_key(plan.node_id, plan.scope):
                raise RuntimeTransitionError(
                    "NODE_OCCURRENCE_ID_INVALID",
                    f"Invalid Node Occurrence id {plan.id!r}.",
                )
            if plan.id in occurrences:
                raise RuntimeTransitionError(
                    "NODE_OCCURRENCE_DUPLICATE",
                    f"Node Occurrence {plan.id!r} already exists.",
                )
            skipped = plan.id in skipped_ids
            occurrences[plan.id] = NodeOccurrenceState(
                id=plan.id,
                node_id=plan.node_id,
                scope=plan.scope,
                status="skipped" if skipped else "ready",
                completed_at_ns=occurred_at_ns if skipped else None,
                activations=activations_for(plan),
            )
            if not skipped:
                ready.append(plan.id)

        for plan in delta.revived:
            occurrence = occurrences.get(plan.id)
            if occurrence is None or occurrence.status != "skipped":
                raise RuntimeTransitionError(
                    "NODE_OCCURRENCE_NOT_SKIPPED",
                    f"Node Occurrence {plan.id!r} cannot be revived.",
                )
            occurrences[plan.id] = replace(
                occurrence,
                status="ready",
                completed_at_ns=None,
                activations=activations_for(plan),
            )
            ready.append(plan.id)

        return SchedulerState(
            initialized=scheduler.initialized,
            ready=tuple(ready),
            occurrences=MappingProxyType(occurrences),
            resolutions=MappingProxyType(resolutions),
            boundary_resolutions=MappingProxyType(boundary_resolutions),
            operator_calls=scheduler.operator_calls,
            waits=scheduler.waits,
        )

    def _apply_context_patch(
        self,
        session: SessionState,
        invocation: InvocationState,
        occurrence: NodeOccurrenceState,
        patch,
        sequence: int,
    ) -> tuple[SessionState, InvocationState]:
        started = occurrence.started_state_version
        if started is None:
            raise RuntimeTransitionError(
                "NODE_OCCURRENCE_START_MISSING",
                "Completed occurrence has no start sequence.",
            )
        session_context, session_revisions = _apply_context_operations(
            session.context,
            session.context_path_revisions,
            patch.session,
            started,
            sequence,
        )
        invocation_context, invocation_revisions = _apply_context_operations(
            invocation.context,
            invocation.context_path_revisions,
            patch.invocation,
            started,
            sequence,
        )
        return (
            replace(
                session,
                context=session_context,
                context_path_revisions=session_revisions,
            ),
            replace(
                invocation,
                context=invocation_context,
                context_path_revisions=invocation_revisions,
            ),
        )


def _require_status(
    invocation: InvocationState, allowed: set[str], event_name: str
) -> None:
    if invocation.status not in allowed:
        raise RuntimeTransitionError(
            "INVOCATION_TRANSITION_INVALID",
            f"{event_name} cannot follow Invocation state {invocation.status!r}.",
        )


def _event_digest(event: RuntimeEvent) -> str:
    encoded = json.dumps(
        event.to_record(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _semantic_event_digest(event: RuntimeEvent) -> str:
    draft = replace(
        event,
        from_state_version=None,
        to_state_version=None,
        operation_batches=(),
        logs=tuple(replace(log, state_version=None) for log in event.logs),
    )
    return _event_digest(draft)


def _apply_context_operations(
    context,
    revisions,
    operations: tuple[ContextOperation, ...],
    started_state_version: int,
    committed_sequence: int,
):
    current = context
    updated_revisions = dict(revisions)
    seen: set[tuple[str, ...]] = set()
    for operation in operations:
        if operation.path in seen:
            raise RuntimeTransitionError(
                "CONTEXT_PATCH_DUPLICATE_PATH",
                f"Context Patch changes path {'.'.join(operation.path)!r} twice.",
            )
        seen.add(operation.path)
        if any(
            revision > started_state_version
            and (_paths_overlap(operation.path, path))
            for path, revision in updated_revisions.items()
        ):
            raise RuntimeTransitionError(
                "CONTEXT_WRITE_CONFLICT",
                f"Context path {'.'.join(operation.path)!r} changed after Node start.",
            )
        current = apply_context_operation(current, operation)
        updated_revisions[operation.path] = committed_sequence
    return current, MappingProxyType(updated_revisions)


def _paths_overlap(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    size = min(len(left), len(right))
    return left[:size] == right[:size]
