"""The only Runtime State transition implementation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
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
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    ChildAwaitReady,
    ChildAwaitSuspended,
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
    StateTransition,
)
from .scheduling import SchedulerDelta, boundary_key, occurrence_key
from .operations import (
    StateOperationBatch,
    apply_operation_batch,
    diff_runtime_states,
)
from .state import (
    InvocationState,
    ChildInvocationPlan,
    ChildUnitState,
    NodeOccurrenceState,
    OperatorCallState,
    RuntimeState,
    SchedulerState,
    SessionState,
    WaitState,
    validate_runtime_state,
)
from .values import freeze
from ..context import ContextOperation, apply_context_operation


@dataclass(frozen=True, slots=True)
class TransitionCommit:
    """Validated compatibility Event and immutable State for one Transition."""

    event: RuntimeEvent
    state: RuntimeState


class StateReducer:
    """Pure reducer for State Operation Batches and persisted Event prefixes.

    Runtime transition objects are planning inputs only. Persisted Events carry
    the resulting operation batches, so replay never needs to reinterpret old
    scheduling or policy code.
    """

    def prepare(self, state: RuntimeState, event: RuntimeEvent) -> RuntimeEvent:
        """Plan an unsealed semantic Event into one atomic operation batch."""

        return self.plan(state, event)[0]

    def transition(
        self, state: RuntimeState, transition: StateTransition
    ) -> TransitionCommit:
        """Plan one internal semantic Transition without exposing Event drafts."""

        if not isinstance(transition, StateTransition):
            raise TypeError("transition must be StateTransition.")
        event, candidate = self.plan(
            state, transition.to_runtime_event(state.sequence + 1)
        )
        return TransitionCommit(event, candidate)

    def plan(
        self, state: RuntimeState, event: RuntimeEvent
    ) -> tuple[RuntimeEvent, RuntimeState]:
        """Return the sealed Event and already-validated candidate State."""

        if event.from_state_version is not None:
            raise ValueError("A persisted Runtime Event cannot be planned again.")
        event = self._attach_previous_event(state, event)
        self._validate_event_header(state, event)
        session, invocation = self._transition(state, event)
        session = replace(session, updated_at_ns=event.occurred_at_ns)
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
        if operations:
            candidate = replace(
                candidate, state_version=state.state_version + 1
            )
        validate_runtime_state(candidate)
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
            candidate,
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
        if (
            current.session is not None
            and current.session.updated_at_ns != event.occurred_at_ns
        ):
            raise RuntimeTransitionError(
                "EVENT_TIME_BOUNDARY_MISMATCH",
                "Runtime Event time must equal its final Session time boundary.",
            )
        return replace(
            current,
            sequence=event.sequence,
            last_event_id=event.id,
            last_event_digest=digest,
            last_event_semantic_digest=_semantic_event_digest(event),
        )

    def validate_sealed(
        self,
        state: RuntimeState,
        event: RuntimeEvent,
    ) -> RuntimeState:
        """Verify sealed operations exactly implement their semantic Runtime Logs."""

        if event.from_state_version is None:
            raise ValueError("validate_sealed requires a sealed Runtime Event.")
        semantic = state
        expected_batches: list[StateOperationBatch] = []
        for log in event.logs:
            transition = StateTransition(
                session_id=event.session_id,
                payload=log.payload,
                invocation_id=log.invocation_id,
                causation_id=log.causation_id,
                occurred_at_ns=log.occurred_at_ns,
                id=log.id,
            )
            commit = self.transition(semantic, transition)
            semantic = commit.state
            expected_batches.extend(commit.event.operation_batches)
            if log.state_version != commit.event.to_state_version:
                raise RuntimeTransitionError(
                    "EVENT_LOG_STATE_MISMATCH",
                    "Runtime Log state version does not match its semantic transition.",
                )
        if (
            event.from_state_version != state.state_version
            or event.to_state_version != semantic.state_version
            or event.operation_batches != tuple(expected_batches)
        ):
            raise RuntimeTransitionError(
                "EVENT_OPERATION_SEMANTIC_MISMATCH",
                "Runtime Event operations do not match its semantic Runtime Logs.",
            )
        return self.commit_event_metadata(semantic, event)

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
        self._validate_previous_event(state, event)
        expected = state.sequence + 1
        if event.sequence != expected:
            raise RuntimeTransitionError(
                "EVENT_SEQUENCE_GAP",
                f"Expected Event sequence {expected}, got {event.sequence}.",
            )
        if (
            state.session is not None
            and state.session.updated_at_ns != event.occurred_at_ns
        ):
            raise RuntimeTransitionError(
                "EVENT_TIME_BOUNDARY_MISMATCH",
                "Runtime Event time must equal its live Session time boundary.",
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
        if (
            state.session is not None
            and batch.occurred_at_ns < state.session.updated_at_ns
        ):
            raise RuntimeTransitionError(
                "EVENT_TIME_REGRESSION",
                "State Operation Batch time cannot move backwards within a Session.",
            )
        record = apply_operation_batch(state.to_record(), batch)
        record["state_version"] = batch.to_state_version
        candidate = RuntimeState.from_record(record)
        if candidate.sequence != state.sequence:
            raise RuntimeTransitionError(
                "STATE_EVENT_METADATA_MUTATION",
                "State Operations cannot mutate Runtime Event sequence metadata.",
            )
        if (
            candidate.session is None
            or candidate.session.updated_at_ns != batch.occurred_at_ns
        ):
            raise RuntimeTransitionError(
                "STATE_TIME_BOUNDARY_MISMATCH",
                "State Operation Batch must advance the Session time boundary.",
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
        self._validate_previous_event(state, event)

    @staticmethod
    def _attach_previous_event(
        state: RuntimeState, event: RuntimeEvent
    ) -> RuntimeEvent:
        if event.previous_event_id is None and event.previous_event_digest is None:
            return replace(
                event,
                previous_event_id=state.last_event_id,
                previous_event_digest=state.last_event_digest,
            )
        return event

    @staticmethod
    def _validate_previous_event(state: RuntimeState, event: RuntimeEvent) -> None:
        if (
            event.previous_event_id != state.last_event_id
            or event.previous_event_digest != state.last_event_digest
        ):
            raise RuntimeTransitionError(
                "EVENT_CHAIN_MISMATCH",
                "Runtime Event previous identity or digest does not match the current State.",
            )

    def reduce(self, events: tuple[RuntimeEvent, ...]) -> RuntimeState:
        """Reconstruct one complete Event prefix.

        This fast path is for sealed Event prefixes already produced by Core
        and accepted by the Host. It keeps one canonical record, applies every
        batch with path copy-on-write, and decodes through the authoritative
        ``RuntimeState`` codec once at the returned prefix boundary. Callers
        validating an untrusted stream incrementally must use ``apply`` (or
        ``apply_batch``) at each acceptance boundary. Unsealed compatibility
        Events still use normal semantic planning.
        """

        if not events:
            return RuntimeState()
        if any(event.from_state_version is None for event in events):
            state = RuntimeState()
            for event in events:
                state = self.apply(state, event)
            return state

        record = RuntimeState().to_record()
        for event in events:
            record = self._apply_persisted_event_record(record, event)
        return _decode_runtime_state(record)

    def _apply_persisted_event_record(
        self,
        record: dict[str, object],
        event: RuntimeEvent,
    ) -> dict[str, object]:
        """Apply one sealed Event to a private canonical replay record."""

        sequence = _record_integer(record, "sequence")
        if event.sequence == sequence:
            if event.id != _record_optional_string(record, "last_event_id"):
                raise RuntimeTransitionError(
                    "EVENT_SEQUENCE_CONFLICT",
                    f"Sequence {event.sequence} already contains another Event.",
                )
            if (
                event.operation_batches
                and _event_digest(event)
                == _record_optional_string(record, "last_event_digest")
            ) or (
                not event.operation_batches
                and _semantic_event_digest(event)
                == _record_optional_string(
                    record, "last_event_semantic_digest"
                )
            ):
                return record
            raise RuntimeTransitionError(
                "EVENT_SEQUENCE_CONFLICT",
                f"Sequence {event.sequence} already contains another Event.",
            )

        self._validate_persisted_event_record_header(record, event)
        candidate: dict[str, object] = record
        state_version = _record_integer(record, "state_version")
        session_record = record.get("session")
        updated_at_ns = (
            _record_integer(session_record, "updated_at_ns")
            if isinstance(session_record, dict)
            else None
        )
        for batch in event.operation_batches:
            if batch.from_state_version != state_version:
                raise RuntimeTransitionError(
                    "STATE_VERSION_GAP",
                    f"Expected State version {state_version}, got "
                    f"{batch.from_state_version}.",
                )
            if updated_at_ns is not None and batch.occurred_at_ns < updated_at_ns:
                raise RuntimeTransitionError(
                    "EVENT_TIME_REGRESSION",
                    "State Operation Batch time cannot move backwards within a Session.",
                )
            candidate = apply_operation_batch(candidate, batch)
            candidate["state_version"] = batch.to_state_version
            state_version = batch.to_state_version
            if _record_integer(candidate, "sequence") != sequence:
                raise RuntimeTransitionError(
                    "STATE_EVENT_METADATA_MUTATION",
                    "State Operations cannot mutate Runtime Event sequence metadata.",
                )
            session_record = candidate.get("session")
            if not isinstance(session_record, dict):
                raise RuntimeTransitionError(
                    "STATE_TIME_BOUNDARY_MISMATCH",
                    "State Operation Batch must retain a Session time boundary.",
                )
            updated_at_ns = _record_integer(session_record, "updated_at_ns")
            if updated_at_ns != batch.occurred_at_ns:
                raise RuntimeTransitionError(
                    "STATE_TIME_BOUNDARY_MISMATCH",
                    "State Operation Batch must advance the Session time boundary.",
                )
        if updated_at_ns != event.occurred_at_ns:
            raise RuntimeTransitionError(
                "EVENT_TIME_BOUNDARY_MISMATCH",
                "Runtime Event time must equal its final Session time boundary.",
            )
        candidate = dict(candidate)
        digest = _event_digest(event)
        semantic_digest = _semantic_event_digest(event)
        candidate["sequence"] = event.sequence
        candidate["last_event_id"] = event.id
        candidate["last_event_digest"] = digest
        candidate["last_event_semantic_digest"] = semantic_digest
        return candidate

    @staticmethod
    def _validate_persisted_event_record_header(
        record: dict[str, object], event: RuntimeEvent
    ) -> None:
        sequence = _record_integer(record, "sequence")
        expected = sequence + 1
        if event.sequence != expected:
            raise RuntimeTransitionError(
                "EVENT_SEQUENCE_GAP",
                f"Expected Event sequence {expected}, got {event.sequence}.",
            )
        if event.schema_version != RUNTIME_EVENT_SCHEMA_VERSION:
            raise RuntimeTransitionError(
                "EVENT_SCHEMA_UNSUPPORTED",
                f"Unsupported Runtime Event schema {event.schema_version}.",
            )
        session = record.get("session")
        if session is not None:
            if not isinstance(session, dict):
                raise TypeError("Runtime State session must be a mapping or None.")
            if event.session_id != _record_string(session, "id"):
                raise RuntimeTransitionError(
                    "EVENT_SESSION_MISMATCH",
                    "Runtime Event belongs to another Session.",
                )
            updated_at_ns = _record_integer(session, "updated_at_ns")
            if event.occurred_at_ns < updated_at_ns:
                raise RuntimeTransitionError(
                    "EVENT_TIME_REGRESSION",
                    "Runtime Event time cannot move backwards within a Session.",
                )
        state_version = _record_integer(record, "state_version")
        if event.from_state_version != state_version:
            raise RuntimeTransitionError(
                "EVENT_STATE_VERSION_GAP",
                f"Expected Event from_state_version {state_version}, got "
                f"{event.from_state_version}.",
            )
        if (
            event.previous_event_id
            != _record_optional_string(record, "last_event_id")
            or event.previous_event_digest
            != _record_optional_string(record, "last_event_digest")
        ):
            raise RuntimeTransitionError(
                "EVENT_CHAIN_MISMATCH",
                "Runtime Event previous identity or digest does not match the current State.",
            )

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
            context = payload.context
            if not isinstance(context, Mapping):
                raise RuntimeTransitionError(
                    "SESSION_CONTEXT_INVALID", "Session Context must be a mapping."
                )
            return (
                SessionState(
                    id=event.session_id,
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
            if (
                not payload.workflow_id
                or not payload.workflow_revision_id
                or not payload.entry_node_id
            ):
                raise RuntimeTransitionError(
                    "INVOCATION_DEFINITION_REQUIRED",
                    "Invocation Workflow, revision and Entry Node cannot be empty.",
                )
            invocation = InvocationState(
                id=event.invocation_id,
                workflow_id=payload.workflow_id,
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
            has_operator_wait = any(
                item.status == "waiting"
                for item in invocation.scheduler.waits.values()
            )
            waiting_occurrence_ids = {
                item.id
                for item in invocation.scheduler.occurrences.values()
                if item.status == "waiting"
            }
            has_child_wait = any(
                plan.mode == "await"
                and plan.parent_occurrence_id in waiting_occurrence_ids
                and any(unit.phase != "terminal" for unit in plan.units)
                for plan in invocation.child_plans.values()
            )
            if not (has_operator_wait or has_child_wait):
                raise RuntimeTransitionError(
                    "INVOCATION_WAIT_MISSING",
                    "Invocation cannot wait without an active Wait or Child Await.",
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
            invalid_activation = next(
                (
                    resolution.activation
                    for resolution in (
                        *payload.delta.resolutions,
                        *payload.delta.boundary_resolutions,
                    )
                    if resolution.activation is not None
                    and (
                        resolution.activation.source_occurrence_id
                        not in occurrences
                        or occurrences[
                            resolution.activation.source_occurrence_id
                        ].status
                        not in {"completed", "failed"}
                    )
                ),
                None,
            )
            if invalid_activation is not None:
                raise RuntimeTransitionError(
                    "ACTIVATION_SOURCE_MISMATCH",
                    "Activation must reference a terminal Node Occurrence.",
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

        if isinstance(payload, ChildInvocationPlanned):
            _require_status(invocation, {"running"}, payload.kind)
            if payload.creation_id in invocation.child_plans:
                raise RuntimeTransitionError(
                    "CHILD_PLAN_DUPLICATE",
                    f"Child Invocation plan {payload.creation_id!r} already exists.",
                )
            occurrence = invocation.scheduler.occurrences.get(
                payload.parent_occurrence_id
            )
            if occurrence is None or occurrence.status != "running":
                raise RuntimeTransitionError(
                    "CHILD_PARENT_OCCURRENCE_NOT_RUNNING",
                    "Child Invocation requires a running parent Node Occurrence.",
                )
            existing_session_ids = {
                unit.session_id
                for plan in invocation.child_plans.values()
                for unit in plan.units
            }
            existing_invocation_ids = {
                unit.invocation_id
                for plan in invocation.child_plans.values()
                for unit in plan.units
            }
            if any(
                unit.child_session_id in existing_session_ids
                or unit.child_invocation_id in existing_invocation_ids
                for unit in payload.units
            ):
                raise RuntimeTransitionError(
                    "CHILD_IDENTITY_DUPLICATE",
                    "Child Session and Invocation identities cannot be reused.",
                )
            units = tuple(
                    ChildUnitState(
                        unit_index=unit.unit_index,
                        session_id=unit.child_session_id,
                        invocation_id=unit.child_invocation_id,
                        input=unit.input,
                    )
                    for unit in payload.units
            )
            plans = dict(invocation.child_plans)
            plans[payload.creation_id] = ChildInvocationPlan(
                creation_id=payload.creation_id,
                parent_occurrence_id=payload.parent_occurrence_id,
                mode=payload.mode,
                workflow_id=payload.workflow_id,
                workflow_revision_id=payload.workflow_revision_id,
                units=units,
            )
            return session, replace(
                invocation, child_plans=MappingProxyType(plans)
            )

        if isinstance(payload, ChildInvocationPhaseChanged):
            _require_status(
                invocation,
                {"running", "waiting", "completed", "failed", "cancelled"},
                payload.kind,
            )
            plan = invocation.child_plans.get(payload.creation_id)
            if plan is None:
                raise RuntimeTransitionError(
                    "CHILD_PLAN_MISSING",
                    f"Child Invocation plan {payload.creation_id!r} does not exist.",
                )
            if payload.unit_index >= len(plan.units):
                raise RuntimeTransitionError(
                    "CHILD_UNIT_MISSING",
                    f"Child unit {payload.unit_index!r} does not exist.",
                )
            unit = plan.units[payload.unit_index]
            expected = {
                "planned": "opened",
                "opened": "accepted",
                "accepted": "terminal",
            }.get(unit.phase)
            if payload.phase != expected:
                raise RuntimeTransitionError(
                    "CHILD_PHASE_INVALID",
                    f"Child unit phase {unit.phase!r} cannot become {payload.phase!r}.",
                )
            units = list(plan.units)
            units[payload.unit_index] = replace(unit, phase=payload.phase)
            plans = dict(invocation.child_plans)
            plans[payload.creation_id] = replace(
                plan, units=tuple(units)
            )
            return session, replace(
                invocation, child_plans=MappingProxyType(plans)
            )

        if isinstance(payload, ChildAwaitSuspended):
            _require_status(invocation, {"running"}, payload.kind)
            plan = invocation.child_plans.get(payload.creation_id)
            if (
                plan is None
                or plan.mode != "await"
                or plan.parent_occurrence_id != payload.parent_occurrence_id
            ):
                raise RuntimeTransitionError(
                    "CHILD_AWAIT_PLAN_MISMATCH",
                    "Child Await suspension does not match an await plan.",
                )
            if any(
                unit.phase not in {"accepted", "terminal"}
                for unit in plan.units
            ):
                raise RuntimeTransitionError(
                    "CHILD_AWAIT_NOT_ACCEPTED",
                    "Child Await cannot suspend before all units are accepted.",
                )
            scheduler = invocation.scheduler
            occurrence = scheduler.occurrences.get(payload.parent_occurrence_id)
            if occurrence is None or occurrence.status != "running":
                raise RuntimeTransitionError(
                    "CHILD_PARENT_OCCURRENCE_NOT_RUNNING",
                    "Child Await suspension requires a running parent occurrence.",
                )
            occurrences = dict(scheduler.occurrences)
            occurrences[occurrence.id] = replace(occurrence, status="waiting")
            still_runnable = any(
                item.id != occurrence.id and item.status in {"ready", "running"}
                for item in scheduler.occurrences.values()
            )
            return session, replace(
                invocation,
                status="running" if still_runnable else "waiting",
                scheduler=replace(
                    scheduler,
                    occurrences=MappingProxyType(occurrences),
                ),
            )

        if isinstance(payload, ChildAwaitReady):
            _require_status(invocation, {"running", "waiting"}, payload.kind)
            plan = invocation.child_plans.get(payload.creation_id)
            if (
                plan is None
                or plan.mode != "await"
                or plan.parent_occurrence_id != payload.parent_occurrence_id
            ):
                raise RuntimeTransitionError(
                    "CHILD_AWAIT_PLAN_MISMATCH",
                    "Child Await readiness does not match an await plan.",
                )
            if any(unit.phase != "terminal" for unit in plan.units):
                raise RuntimeTransitionError(
                    "CHILD_AWAIT_NOT_TERMINAL",
                    "Child Await cannot become ready before every unit is terminal.",
                )
            scheduler = invocation.scheduler
            occurrence = scheduler.occurrences.get(payload.parent_occurrence_id)
            if occurrence is None or occurrence.status != "waiting":
                raise RuntimeTransitionError(
                    "CHILD_PARENT_OCCURRENCE_NOT_WAITING",
                    "Child Await readiness requires a waiting parent occurrence.",
                )
            occurrences = dict(scheduler.occurrences)
            occurrences[occurrence.id] = replace(
                occurrence,
                status="ready",
                started_at_ns=None,
                started_state_version=None,
            )
            ready = (
                scheduler.ready
                if occurrence.id in scheduler.ready
                else (*scheduler.ready, occurrence.id)
            )
            return session, replace(
                invocation,
                status="running",
                scheduler=replace(
                    scheduler,
                    ready=ready,
                    occurrences=MappingProxyType(occurrences),
                ),
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


def _decode_runtime_state(record: dict[str, object]) -> RuntimeState:
    """Single authoritative codec/validation boundary for optimized replay."""

    return RuntimeState.from_record(record)


def _record_integer(record: Mapping[str, object], key: str) -> int:
    value = record.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Runtime State {key} must be an integer.")
    return value


def _record_string(record: Mapping[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise TypeError(f"Runtime State {key} must be a non-empty string.")
    return value


def _record_optional_string(
    record: Mapping[str, object], key: str
) -> str | None:
    value = record.get(key)
    if value is not None and (not isinstance(value, str) or not value):
        raise TypeError(f"Runtime State {key} must be a non-empty string or None.")
    return value


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
