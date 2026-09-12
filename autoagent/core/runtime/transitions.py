"""Live transition planning. Replay never invokes user code or this planner."""
from __future__ import annotations

from collections import ChainMap
from dataclasses import replace
from types import MappingProxyType

from ..errors import RuntimeTransitionError
from ..context import ContextOperation, ContextPatch
from .events import (
    SessionOpened, InvocationStarted, NodeStarted, NodeCompleted, NodeFailed,
    InputMapped, CapabilityResolved, Aggregated, OutputBound, RoutingResolved,
    NodeFaulted, OperatorCallStarted, OperatorCallCompleted, OperatorCallFailed,
    WaitRequested, WaitResumed, RecoveryApplied, InvocationCompleted,
    InvocationFailed, InvocationCancelled, ChildInvocationPlanned,
    ChildInvocationPhaseChanged, ChildAwaitSuspended, ChildAwaitReady,
    validate_payload,
)
from .operations import StateDelta, StateOperation
from .state import (
    RuntimeState, SessionState, InvocationState, SchedulerState,
    NodeOccurrenceState, NodeExecutionState, OperatorCallState, WaitState,
    ChildInvocationPlan, ChildUnitState, _validate_scope,
)
from .scheduling import SchedulerDelta, boundary_key, occurrence_key
from .values import freeze
from ._overlay import PlanningOverlay
from ._chunked import ChunkedUnits, runtime_mapping, child_units

SCHED = ("invocation", "scheduler")


class TransitionPlanner:
    """Generate explicit mutations for a semantic boundary against current State."""

    def plan(self, state, payload, *, occurred_at_us, session_id=None,
             invocation_id=None, scheduler_delta=None):
        validate_payload(payload)
        operations = []
        def put(path, value, op="replace"):
            operations.append(StateOperation._from_owned(op, path, value))
        def occurrence_put(item):
            put((*SCHED, "occurrences", item.id), item)
        def workspace(item, **changes):
            phase = changes.get("phase")
            if phase in {"input_mapped", "capability_resolved", "aggregated", "output_bound", "routing_resolved"}:
                if phase in item.execution.completed_stages and not (
                    phase == "routing_resolved" and changes.get("routing_source_status") == "error"
                    and item.execution.routing_source_status == "complete"
                ):
                    raise RuntimeTransitionError("EXECUTION_STAGE_DUPLICATE", "Execution stage already committed.")
                changes["completed_stages"] = tuple(dict.fromkeys((*item.execution.completed_stages, phase)))
            put((*SCHED, "occurrences", item.id, "execution"),
                replace(item.execution, **changes))
        session = state.session
        if isinstance(payload, SessionOpened):
            if session is not None:
                raise RuntimeTransitionError("SESSION_ALREADY_OPEN", "Session already exists.")
            if not isinstance(payload.context, MappingProxyType) and not hasattr(payload.context, "items"):
                raise TypeError("Session Context must be a mapping.")
            put(("session",), SessionState(session_id, payload.context, occurred_at_us, occurred_at_us))
            return StateDelta(tuple(operations))
        if session is None:
            raise RuntimeTransitionError("SESSION_NOT_OPEN", "Event requires an open Session.")
        if invocation_id is None:
            raise RuntimeTransitionError("INVOCATION_ID_REQUIRED", "Event requires an Invocation identity.")
        put(("session", "updated_at_us"), occurred_at_us)
        inv = state.invocation
        if isinstance(payload, InvocationStarted):
            if inv is not None and not inv.terminal:
                raise RuntimeTransitionError("INVOCATION_ALREADY_ACTIVE", "Invocation is active.")
            new = InvocationState(invocation_id, payload.workflow_id, payload.workflow_revision_id,
                payload.entry_node_id, "running", payload.input, freeze({}), created_at_us=occurred_at_us,
                started_at_us=occurred_at_us)
            if scheduler_delta is None:
                raise ValueError("InvocationStarted requires initial SchedulerDelta.")
            new = replace(new, scheduler=self._apply_scheduler_delta(
                replace(new.scheduler, initialized=True), scheduler_delta, occurred_at_us))
            put(("invocation",), new)
            put(("session", "latest_invocation_id"), invocation_id)
            return StateDelta(tuple(operations))
        if inv is None or inv.id != invocation_id:
            raise RuntimeTransitionError("INVOCATION_MISMATCH", "Event targets another Invocation.")
        if inv.terminal and not isinstance(payload, ChildInvocationPhaseChanged):
            raise RuntimeTransitionError("INVOCATION_TRANSITION_INVALID", "Invocation is already terminal.")
        sched = inv.scheduler
        oid = getattr(payload, "occurrence_id", None)
        occ = sched.occurrences.get(oid)
        def require_occ(status="running"):
            if occ is None or occ.status != status:
                raise RuntimeTransitionError("NODE_OCCURRENCE_NOT_" + status.upper(), "Occurrence has an invalid lifecycle.")
            return occ
        def graph(delta, scheduler=None):
            before = scheduler or sched
            planned = self._apply_scheduler_delta(before, delta, occurred_at_us, temporary=True)
            for item in delta.resolutions:
                if item.id not in delta.consumed_resolution_ids:
                    put((*SCHED, "resolutions", item.id), item, "add")
            for key in delta.consumed_resolution_ids:
                if key in before.resolutions:
                    put((*SCHED, "resolutions", key), None, "remove")
            for item in delta.boundary_resolutions:
                if boundary_key(item.loop_region_id, item.loop_scope) not in delta.closed_boundaries:
                    put((*SCHED, "boundary_resolutions", item.id), item, "add")
            for key, item in before.boundary_resolutions.items():
                if boundary_key(item.loop_region_id, item.loop_scope) in delta.closed_boundaries:
                    put((*SCHED, "boundary_resolutions", key), None, "remove")
            for item in (*delta.ready, *delta.skipped):
                put((*SCHED, "occurrences", item.id), planned.occurrences[item.id], "add")
            for item in delta.revived:
                occurrence_put(planned.occurrences[item.id])
            put((*SCHED, "ready"), planned.ready)
        if isinstance(payload, NodeStarted):
            require_occ("ready")
            if occ.started_sequence is not None:
                raise RuntimeTransitionError("NODE_ALREADY_STARTED", "Continuation cannot start the same occurrence twice.")
            occurrence_put(replace(occ, status="running", started_at_us=occurred_at_us,
                started_sequence=state.sequence+1, execution=replace(occ.execution, phase="started")))
            put((*SCHED, "ready"), tuple(key for key in sched.ready if key != oid))
        elif isinstance(payload, InputMapped):
            require_occ(); workspace(occ, phase="input_mapped", mapped_input=payload.mapped_input)
        elif isinstance(payload, CapabilityResolved):
            require_occ(); workspace(occ, phase="capability_resolved",
                resolved_capability_id=payload.capability_id, resolved_operator_id=payload.operator_id)
        elif isinstance(payload, Aggregated):
            require_occ(); workspace(occ, phase="aggregated", aggregate_output=payload.output)
        elif isinstance(payload, OutputBound):
            require_occ(); workspace(occ, phase="output_bound", pending_context_patch=payload.patch)
        elif isinstance(payload, RoutingResolved):
            require_occ(); workspace(occ, phase="routing_resolved", routing=payload.conditions,
                routing_source_status=payload.source_status)
        elif isinstance(payload, NodeFaulted):
            require_occ(); workspace(occ, phase="faulted", fault=payload.error)
        elif isinstance(payload, OperatorCallStarted):
            require_occ()
            if payload.call_id in sched.operator_calls:
                raise RuntimeTransitionError("OPERATOR_CALL_DUPLICATE", "Operator Call already exists.")
            put((*SCHED, "operator_calls", payload.call_id), OperatorCallState(
                payload.call_id, oid, payload.operator_id, payload.unit_index, "running", payload.input,
                started_at_us=occurred_at_us, queue_duration_ns=payload.queue_duration_ns), "add")
            workspace(occ, phase="executing")
        elif isinstance(payload, (OperatorCallCompleted, OperatorCallFailed)):
            call = sched.operator_calls.get(payload.call_id)
            if call is None or call.status != "running":
                raise RuntimeTransitionError("OPERATOR_CALL_NOT_RUNNING", "Operator Call is not running.")
            success = isinstance(payload, OperatorCallCompleted)
            for name, value in (
                ("status", "completed" if success else "failed"),
                ("output", payload.output if success else None),
                ("error", None if success else payload.error),
                ("completed_at_us", occurred_at_us),
                ("execution_duration_ns", payload.execution_duration_ns),
            ):
                put((*SCHED, "operator_calls", call.id, name), value)
        elif isinstance(payload, (NodeCompleted, NodeFailed)):
            require_occ()
            success = isinstance(payload, NodeCompleted)
            terminal = replace(occ, status="completed" if success else "failed",
                output=payload.output if success else None, error=None if success else payload.error,
                completed_at_us=occurred_at_us, metrics=payload.metrics if success else occ.metrics,
                execution=NodeExecutionState())
            occurrence_put(terminal)
            if success:
                patch = occ.execution.pending_context_patch
                updated_session, updated_inv = self._apply_context_patch(session, inv, occ, patch, state.sequence+1)
                for name, updated in (("session", updated_session), ("invocation", updated_inv)):
                    if getattr(patch, name):
                        put((name, "context"), updated.context)
                        put((name, "context_path_revisions"), updated.context_path_revisions)
            graph(scheduler_delta, replace(sched, occurrences=ChainMap({oid: terminal}, sched.occurrences)))
            remaining = (
                [item for key, item in sched.occurrences.items() if key != oid]
                if not scheduler_delta.ready and not scheduler_delta.revived else ()
            )
            if not scheduler_delta.ready and not scheduler_delta.revived and remaining and all(
                item.status in {"waiting", "completed", "failed", "skipped", "cancelled"} for item in remaining
            ) and any(item.status == "waiting" for item in remaining):
                put(("invocation", "status"), "waiting")
        elif isinstance(payload, WaitRequested):
            require_occ()
            if payload.wait_id in sched.waits:
                raise RuntimeTransitionError("WAIT_DUPLICATE", "Wait already exists.")
            occurrence_put(replace(occ, status="waiting"))
            put((*SCHED, "waits", payload.wait_id), WaitState(payload.wait_id, oid, "waiting", payload.request,
                created_at_us=occurred_at_us), "add")
            if not any(item.id != oid and item.status in {"ready", "running"} for item in sched.occurrences.values()):
                put(("invocation", "status"), "waiting")
        elif isinstance(payload, WaitResumed):
            wait = sched.waits.get(payload.wait_id)
            if wait is None or wait.status != "waiting":
                raise RuntimeTransitionError("WAIT_NOT_WAITING", "Wait is not waiting.")
            put((*SCHED, "waits", wait.id), replace(wait, status="resumed", response=payload.response, resumed_at_us=occurred_at_us))
            occurrence_put(replace(sched.occurrences[wait.occurrence_id], status="running", ready_at_us=occurred_at_us))
            
            put(("invocation", "status"), "running")
        elif isinstance(payload, RecoveryApplied):
            recovered = []
            for item in sched.operator_calls.values():
                if item.status == "running":
                    put((*SCHED, "operator_calls", item.id), replace(item, status="lost", completed_at_us=occurred_at_us))
            for item in sched.occurrences.values():
                if item.status == "running":
                    occurrence_put(replace(item, status="running", ready_at_us=occurred_at_us,
                        recovery_attempts=item.recovery_attempts+1))
                    recovered.append(item.id)
            if recovered:
                
                put(("invocation", "status"), "running")
        elif isinstance(payload, InvocationCompleted):
            _require_status(inv, {"running"}, payload.kind)
            put(("invocation", "status"), "completed")
            put(("invocation", "output"), payload.output)
            put(("invocation", "completed_at_us"), occurred_at_us)
        elif isinstance(payload, (InvocationFailed, InvocationCancelled)):
            _require_status(inv, {"created", "running", "waiting"}, payload.kind)
            failed = isinstance(payload, InvocationFailed)
            put(("invocation", "status"), "failed" if failed else "cancelled")
            put(("invocation", "error" if failed else "cancel_reason"), payload.error if failed else payload.reason)
            put(("invocation", "completed_at_us"), occurred_at_us)
            put((*SCHED, "ready"), ())
            for item in sched.occurrences.values():
                if item.status in {"ready", "running", "waiting"}:
                    occurrence_put(replace(item, status="cancelled", completed_at_us=occurred_at_us))
            for item in sched.operator_calls.values():
                if item.status == "running":
                    put((*SCHED, "operator_calls", item.id), replace(item, status="cancelled", completed_at_us=occurred_at_us))
            for item in sched.waits.values():
                if item.status == "waiting":
                    put((*SCHED, "waits", item.id), replace(item, status="cancelled"))
        elif isinstance(payload, ChildInvocationPlanned):
            if payload.creation_id in inv.child_plans:
                raise RuntimeTransitionError("CHILD_PLAN_DUPLICATE", "Child plan already exists.")
            parent = sched.occurrences.get(payload.parent_occurrence_id)
            if parent is None or parent.status != "running":
                raise RuntimeTransitionError("CHILD_PARENT_OCCURRENCE_NOT_RUNNING", "Child requires a running parent.")
            plan = ChildInvocationPlan(payload.creation_id, payload.parent_occurrence_id, payload.mode,
                payload.workflow_id, payload.workflow_revision_id,
                child_units(tuple(ChildUnitState(u.unit_index, u.child_session_id, u.child_invocation_id, u.input) for u in payload.units)))
            put(("invocation", "child_plans", plan.creation_id), plan, "add")
        elif isinstance(payload, ChildInvocationPhaseChanged):
            plan = inv.child_plans.get(payload.creation_id)
            if plan is None or payload.unit_index >= len(plan.units):
                raise RuntimeTransitionError("CHILD_PLAN_MISSING", "Child plan or unit is missing.")
            unit = plan.units[payload.unit_index]
            if {"planned":"opened", "opened":"accepted", "accepted":"terminal"}.get(unit.phase) != payload.phase:
                raise RuntimeTransitionError("CHILD_PHASE_INVALID", "Invalid Child phase transition.")
            updated = replace(unit, phase=payload.phase)
            if isinstance(plan.units, ChunkedUnits) or len(plan.units) >= 512:
                units = plan.units if isinstance(plan.units, ChunkedUnits) else ChunkedUnits(plan.units)
                units = units.replace_at(payload.unit_index, updated)
            else:
                units = list(plan.units)
                units[payload.unit_index] = updated
                units = tuple(units)
            put(("invocation", "child_plans", plan.creation_id), replace(plan, units=units))
        elif isinstance(payload, (ChildAwaitSuspended, ChildAwaitReady)):
            plan = inv.child_plans.get(payload.creation_id)
            if plan is None or plan.mode != "await" or plan.parent_occurrence_id != payload.parent_occurrence_id:
                raise RuntimeTransitionError("CHILD_AWAIT_PLAN_MISMATCH", "Child Await plan mismatch.")
            item = sched.occurrences[payload.parent_occurrence_id]
            resume = isinstance(payload, ChildAwaitReady)
            if resume and any(unit.phase != "terminal" for unit in plan.units):
                raise RuntimeTransitionError("CHILD_AWAIT_NOT_TERMINAL", "Child units have not settled.")
            occurrence_put(replace(item, status="running" if resume else "waiting", ready_at_us=occurred_at_us))
            if resume:
                
                put(("invocation", "status"), "running")
            elif not any(o.id != item.id and o.status in {"ready", "running"} for o in sched.occurrences.values()):
                put(("invocation", "status"), "waiting")
        else:
            raise RuntimeTransitionError("EVENT_TYPE_UNSUPPORTED", "Unsupported semantic boundary.")
        return StateDelta(tuple(operations))

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
            sequence or state.sequence + 1,
        )
        return candidate_session.context, candidate_invocation.context

    def _apply_scheduler_delta(
        self,
        scheduler: SchedulerState,
        delta: SchedulerDelta,
        occurred_at_us: int,
        *, temporary: bool = False,
    ) -> SchedulerState:
        occurrences = PlanningOverlay(scheduler.occurrences)
        resolutions = PlanningOverlay(scheduler.resolutions)
        boundary_resolutions = PlanningOverlay(scheduler.boundary_resolutions)
        available_resolutions = ChainMap({}, scheduler.boundary_resolutions, scheduler.resolutions)
        ready = list(scheduler.ready)

        for resolution in delta.resolutions:
            _validate_scope(resolution.target_scope, "Edge resolution scope")
            if resolution.id in resolutions:
                raise RuntimeTransitionError(
                    "EDGE_RESOLUTION_DUPLICATE",
                    f"Edge Resolution {resolution.id!r} already exists.",
                )
            if (
                resolution.activation is not None
                and (resolution.activation.source_occurrence_id not in occurrences
                     or occurrences[resolution.activation.source_occurrence_id].status not in {"completed", "failed"})
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
            _validate_scope(resolution.loop_scope, "Loop scope")
            _validate_scope(resolution.source_scope, "Loop source scope")
            if resolution.id in boundary_resolutions:
                raise RuntimeTransitionError(
                    "LOOP_BOUNDARY_RESOLUTION_DUPLICATE",
                    f"Loop boundary Resolution {resolution.id!r} already exists.",
                )
            if (
                resolution.activation is not None
                and (resolution.activation.source_occurrence_id not in occurrences
                     or occurrences[resolution.activation.source_occurrence_id].status not in {"completed", "failed"})
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
            _validate_scope(plan.scope, "Occurrence scope")
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
                completed_at_us=occurred_at_us if skipped else None,
                ready_at_us=None if skipped else occurred_at_us,
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
                ready_at_us=occurred_at_us,
                completed_at_us=None,
                activations=activations_for(plan),
            )
            ready.append(plan.id)

        return SchedulerState(
            initialized=scheduler.initialized,
            ready=tuple(ready),
            occurrences=MappingProxyType(occurrences) if temporary else runtime_mapping(dict(occurrences)),
            resolutions=MappingProxyType(resolutions) if temporary else runtime_mapping(dict(resolutions)),
            boundary_resolutions=MappingProxyType(boundary_resolutions) if temporary else runtime_mapping(dict(boundary_resolutions)),
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
        started = occurrence.started_sequence
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



def _require_status(invocation, allowed, name):
    if invocation.status not in allowed:
        raise RuntimeTransitionError("INVOCATION_TRANSITION_INVALID", f"{name} cannot follow {invocation.status}.")

def _apply_context_operations(
    context,
    revisions,
    operations: tuple[ContextOperation, ...],
    started_sequence: int,
    committed_sequence: int,
):
    if not operations:
        return context, revisions
    from ..context import _ContextEdit
    from ._chunked import MapEdit
    current = _ContextEdit(context)
    updated_revisions = MapEdit(revisions)
    recent = [path for path, revision in revisions.items() if revision > started_sequence]
    seen: set[tuple[str, ...]] = set()
    for operation in operations:
        if operation.path in seen:
            raise RuntimeTransitionError(
                "CONTEXT_PATCH_DUPLICATE_PATH",
                f"Context Patch changes path {'.'.join(operation.path)!r} twice.",
            )
        seen.add(operation.path)
        if any(
            _paths_overlap(operation.path, path) for path in recent
        ):
            raise RuntimeTransitionError(
                "CONTEXT_WRITE_CONFLICT",
                f"Context path {'.'.join(operation.path)!r} changed after Node start.",
            )
        current.apply(operation)
        updated_revisions[operation.path] = committed_sequence
        if committed_sequence > started_sequence:
            recent.append(operation.path)
    return current.finish(), updated_revisions.finish()


def _paths_overlap(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    size = min(len(left), len(right))
    return left[:size] == right[:size]
