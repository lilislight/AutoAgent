"""Live transition planning. Replay never invokes user code or this planner."""
from __future__ import annotations

from ..commands.models import SYSTEM_COMMAND_IDS, INLINE_COMMAND_IDS
from .events import ResumeReceiptReleased, CancellationRequested
from .events import GraphPauseRequested, GraphPaused, GraphResumed
from .signals import SignalAccepted, SignalsReceived, SignalReceiptReleased, signal_delta

from collections import ChainMap
from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType

from ..errors import RuntimeTransitionError
from ..context import ContextOperation
from .events import (
    SessionOpened, InvocationStarted, NodeStarted, NodeCompleted, NodeFailed,
    InputMapped, CapabilityResolved, CommandRetryStarted, CommandsPending, CommandInvoked, CommandReturned, Aggregated, OutputBound, RoutingResolved,
    NodeFaulted, OperatorCallStarted, OperatorCallCompleted, OperatorCallFailed,
    RecoveryPrepared, InvocationReopened,
    CommandAwakened, WaitRequested, WaitResumed, RecoveryApplied, InvocationCompleted, InvocationSettling,
    InvocationFailed, InvocationCancelled, ChildInvocationPlanned,
    ChildInvocationPhaseChanged,
    validate_payload, ChildCompacted,
)
from .operations import StateDelta, StateOperation
from .state import (
    RuntimeState, SessionState, InvocationState, SchedulerState, ChildResult, child_input_digest, release_child_inputs,
    NodeOccurrenceState, NodeExecutionState, OperatorCallState, WaitState,
    ChildInvocationPlan, ChildUnitState, _validate_scope,
)
from .scheduling import SchedulerDelta, boundary_key, occurrence_key
from .values import freeze
from ._overlay import PlanningOverlay
from ._context_index import revision_index, _previews
from ._chunked import ChunkedUnits, runtime_mapping, child_units
from ._execution_index import ExecutionIndex
from ._retention import released_outputs

SCHED = ("invocation", "scheduler")


def _retire_execution(execution):
    # Ownership and cross-Session receipts still reference accepted Command
    # identities after the enclosing Node releases its payload history.
    commands = {key: {**call, "input": None, "output": None}
        for key, call in execution.inline_commands.items()}
    return NodeExecutionState(inline_commands=freeze(commands))


class TransitionPlanner:
    """Generate explicit mutations for a semantic boundary against current State."""

    def plan(self, state, payload, *, occurred_at_us, session_id=None,
             invocation_id=None, scheduler_delta=None, _execution_index=None,
             _output_node_ids=None, recovery_state=None, recovery_plan=None):
        validate_payload(payload)
        operations = []
        def put(path, value, op="replace"):
            operations.append(StateOperation._from_owned(op, path, value))
        def occurrence_put(item):
            put((*SCHED, "occurrences", item.id), item)
        def release_call_values(call):
            for field in ('input', 'output'):
                if getattr(call, field) is not None:
                    put((*SCHED, 'operator_calls', call.id, field), None)
        def workspace(item, **changes):
            phase = changes.get("phase")
            if phase in {"input_mapped", "capability_resolved", "aggregated", "output_bound", "routing_resolved"}:
                if phase in item.execution.completed_stages and not (
                    phase == "routing_resolved" and changes.get("routing_source_status") == "error"
                    and item.execution.routing_source_status == "complete"
                ):
                    raise RuntimeTransitionError("EXECUTION_STAGE_DUPLICATE", "Execution stage already committed.")
                changes["completed_stages"] = tuple(dict.fromkeys((*item.execution.completed_stages, phase)))
            for name, value in changes.items():
                if value is not getattr(item.execution, name) and value != getattr(item.execution, name):
                    put((*SCHED, "occurrences", item.id, "execution", name), value)
        session = state.session
        if isinstance(payload, SessionOpened):
            if session is not None:
                raise RuntimeTransitionError("SESSION_ALREADY_OPEN", "Session already exists.")
            if not isinstance(payload.context, Mapping):
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
        if isinstance(payload, RecoveryPrepared):
            if inv.status not in {'failed', 'cancelled'} or inv.pending_recovery is not None or recovery_plan is None:
                raise RuntimeTransitionError('RECOVERY_PREPARATION_INVALID', 'Recovery requires a settled failed or cancelled Root.')
            put(('invocation', 'pending_recovery'), freeze(recovery_plan))
            return StateDelta(tuple(operations))
        if isinstance(payload, InvocationReopened):
            candidate = recovery_state
            if candidate is None or candidate.session.id != session_id or candidate.invocation.id != inv.id:
                raise RuntimeTransitionError('RECOVERY_STATE_INVALID', 'Recovery must preserve Session and Invocation identities.')
            if (candidate.invocation.workflow_revision_id != inv.workflow_revision_id
                    or candidate.invocation.recovery_count != inv.recovery_count + 1
                    or candidate.invocation.last_recovery_id != payload.request_id
                    or payload.recovery_count != candidate.invocation.recovery_count
                    or payload.previous_status != inv.status or candidate.invocation.status != 'running'
                    or candidate.invocation.pending_recovery is not None
                    or inv.status not in {'failed', 'cancelled'}):
                raise RuntimeTransitionError('RECOVERY_STATE_INVALID', 'Recovery cannot replace another Revision or execution round.')
            put(('session',), replace(candidate.session, updated_at_us=occurred_at_us))
            put(('invocation',), candidate.invocation)
            return StateDelta(tuple(operations))
        if isinstance(payload, (GraphPauseRequested, GraphPaused, GraphResumed)):
            expected = {GraphPauseRequested: {"active"}, GraphPaused: {"pausing"},
                        GraphResumed: {"paused", "pausing"}}[type(payload)]
            if inv.terminal or inv.stopping or inv.control_state not in expected:
                raise RuntimeTransitionError("GRAPH_CONTROL_INVALID", "Invalid graph control transition.")
            value = {GraphPauseRequested: "pausing", GraphPaused: "paused", GraphResumed: "active"}[type(payload)]
            put(("invocation", "control_state"), value)
            return StateDelta(tuple(operations))
        if isinstance(payload, (SignalAccepted, SignalsReceived, SignalReceiptReleased)):
            delta = signal_delta(self, state, payload, occurred_at_us, session_id, invocation_id, _execution_index)
            return StateDelta((*operations, *delta.operations))
        if isinstance(payload, CancellationRequested):
            if inv.terminal or inv.stopping:
                raise RuntimeTransitionError('INVOCATION_STOPPING', 'Cancellation is already decided.')
            if child_input_digest(payload.reason) != payload.origin['reason_digest']:
                raise ValueError('Cancellation reason digest mismatch.')
            delta = self.plan(state, InvocationSettling('cancelled', reason=payload.reason),
                occurred_at_us=occurred_at_us, session_id=session_id, invocation_id=invocation_id,
                _execution_index=_execution_index)
            return StateDelta((*delta.operations, StateOperation._from_owned(
                'replace', ('invocation', 'cancel_origin'), payload.origin)))
        if isinstance(payload, ChildCompacted):
            if isinstance(inv, ChildResult) or not inv.terminal or any(
                u.phase not in {"terminal", "abandoned"} for p in inv.child_plans.values() for u in p.units
            ):
                raise RuntimeTransitionError("CHILD_NOT_COMPACTABLE", "Child must be terminal with settled descendants.")
            result = ChildResult(inv.id, inv.workflow_id, inv.workflow_revision_id, inv.entry_node_id,
                inv.status, inv.output, inv.error, inv.cancel_reason, inv.created_at_us,
                inv.started_at_us, inv.completed_at_us, payload.parent_session_id,
                payload.parent_invocation_id, payload.creation_id, payload.unit_index,
                child_input_digest(inv.input), release_child_inputs(inv.child_plans),
                inv.signals, inv.cancel_origin, inv.resume_receipts, inv.resume_receipt_frontiers,
                inv.recovery_count, inv.last_recovery_id)
            put(("invocation",), result)
            put(("session",), replace(session, context=freeze({}), context_path_revisions=MappingProxyType({}), updated_at_us=occurred_at_us))
            return StateDelta(tuple(operations))
        if isinstance(payload, ResumeReceiptReleased):
            receipt = inv.resume_receipts.get(payload.call_id)
            if receipt is None:
                raise RuntimeTransitionError("RESUME_RECEIPT_MISSING", "Resume receipt was already released.")
            put(("invocation", "resume_receipts", payload.call_id), None, "remove")
            previous = inv.resume_receipt_frontiers.get(receipt['session_id'])
            if previous is not None and previous['invocation_id'] != receipt['invocation_id']:
                raise RuntimeTransitionError('RESUME_CALLER_REPLACED', 'Resume caller identity changed within the graph.')
            sequence = max(payload.caller_sequence, previous['sequence'] if previous else 0)
            put(("invocation", "resume_receipt_frontiers", receipt['session_id']),
                freeze({'invocation_id': receipt['invocation_id'], 'sequence': sequence}),
                "replace" if previous else "add")
            return StateDelta(tuple(operations))
        if inv.terminal and not isinstance(payload, ChildInvocationPhaseChanged):
            raise RuntimeTransitionError("INVOCATION_TRANSITION_INVALID", "Invocation is already terminal.")
        sched = inv.scheduler
        oid = getattr(payload, "occurrence_id", None)
        occ = sched.occurrences.get(oid)
        def require_occ(status="running"):
            allowed = {status} if isinstance(status, str) else status
            if occ is None or occ.status not in allowed:
                label = status if isinstance(status, str) else "_OR_".join(sorted(status))
                raise RuntimeTransitionError("NODE_OCCURRENCE_NOT_" + label.upper(), "Occurrence has an invalid lifecycle.")
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
            return planned
        if isinstance(payload, NodeStarted):
            require_occ("ready")
            if occ.started_sequence is not None:
                raise RuntimeTransitionError("NODE_ALREADY_STARTED", "Continuation cannot start the same occurrence twice.")
            occurrence_put(replace(occ, status="running", started_at_us=occurred_at_us,
                started_sequence=state.sequence+1, raw_input=payload.input,
                execution=replace(occ.execution, phase="started")))
            count = sched.node_executions.get(occ.node_id, 0)
            put((*SCHED, "node_executions", occ.node_id), count + 1,
                "replace" if occ.node_id in sched.node_executions else "add")
            put((*SCHED, "ready"), tuple(key for key in sched.ready if key != oid))
        elif isinstance(payload, InputMapped):
            require_occ(); workspace(occ, phase="input_mapped", mapped_input=payload.mapped_input)
        elif isinstance(payload, CapabilityResolved):
            require_occ()
            if type(payload.unit_index) is not int or payload.unit_index < 0:
                raise ValueError("Capability unit index must be nonnegative.")
            bindings = dict(occ.execution.capability_bindings)
            if str(payload.unit_index) in bindings:
                raise RuntimeTransitionError("CAPABILITY_ALREADY_RESOLVED", "Capability unit is already bound.")
            bindings[str(payload.unit_index)] = {"capability_id": payload.capability_id, "operator_id": payload.operator_id}
            if 'capability_resolved' in occ.execution.completed_stages:
                occurrence_put(replace(occ, execution=replace(occ.execution, capability_bindings=freeze(bindings))))
            else:
                workspace(occ, phase="capability_resolved", capability_bindings=freeze(bindings))
        elif isinstance(payload, CommandsPending):
            require_occ({'running', 'waiting'})
            ids = (_execution_index.calls_by_occurrence.get(oid, ()) if _execution_index is not None
                   else tuple(c.id for c in sched.operator_calls.values() if c.occurrence_id == oid))
            latest = {sched.operator_calls[key].unit_index: sched.operator_calls[key] for key in ids}
            unfinished = [call for call in latest.values() if call.status != 'completed']
            wait_ids = (_execution_index.waits_by_occurrence.get(oid, ()) if _execution_index is not None
                        else tuple(w.id for w in sched.waits.values() if w.occurrence_id == oid))
            if (any(sched.waits[key].status == 'waiting' for key in wait_ids)
                    and all((wait := sched.waits.get(call.id)) is not None and wait.status == 'waiting' for call in unfinished)):
                occurrence_put(replace(occ, status='waiting'))
                if not _other_active(sched, oid, _execution_index):
                    put(('invocation', 'status'), 'waiting')
        elif isinstance(payload, Aggregated):
            require_occ()
            call_ids = (_execution_index.calls_by_occurrence.get(oid, ()) if _execution_index is not None
                        else tuple(c.id for c in sched.operator_calls.values() if c.occurrence_id == oid))
            if any(sched.operator_calls[key].status == "running" for key in call_ids):
                raise RuntimeTransitionError("AGGREGATION_CALLS_ACTIVE", "Aggregation requires settled Operator Calls.")
            workspace(occ, phase="aggregated", aggregate_output=payload.output, mapped_input=None)
            for key in call_ids:
                release_call_values(sched.operator_calls[key])
        elif isinstance(payload, OutputBound):
            require_occ(); workspace(occ, phase="output_bound", pending_context_patch=payload.patch)
        elif isinstance(payload, RoutingResolved):
            require_occ(); workspace(occ, phase="routing_resolved", routing=payload.conditions,
                routing_source_status=payload.source_status)
        elif isinstance(payload, NodeFaulted):
            require_occ(); workspace(occ, phase="faulted", fault=payload.error)
        elif isinstance(payload, CommandInvoked):
            require_occ({"running"})
            parent = sched.operator_calls.get(payload.parent_call_id)
            if (parent is None or parent.status != "running" or parent.occurrence_id != oid
                    or parent.unit_index != payload.unit_index or parent.operator_id in SYSTEM_COMMAND_IDS
                    or payload.operator_id not in INLINE_COMMAND_IDS):
                raise RuntimeTransitionError("COMMAND_PARENT_INVALID", "Nested Command requires its active Operator unit.")
            if payload.call_id in occ.execution.inline_commands:
                raise RuntimeTransitionError("COMMAND_DUPLICATE", "Nested Command already exists.")
            if any(call["unit_index"] == payload.unit_index and call["key"] == payload.key
                   for call in occ.execution.inline_commands.values()):
                raise RuntimeTransitionError("COMMAND_KEY_CONFLICT", "Nested Command key is already assigned in this unit.")
            record = {"id": payload.call_id, "parent_call_id": payload.parent_call_id,
                "operator_id": payload.operator_id, "unit_index": payload.unit_index, "key": payload.key,
                "status": "running", "input": payload.input, "output": None,
                "deadline_at_us": payload.deadline_at_us}
            workspace(occ, inline_commands=freeze({**occ.execution.inline_commands, payload.call_id: record}))
        elif isinstance(payload, CommandReturned):
            require_occ({"running"})
            record = occ.execution.inline_commands.get(payload.call_id)
            if record is None or record["status"] != "running":
                raise RuntimeTransitionError("COMMAND_NOT_RUNNING", "Nested Command is not running.")
            workspace(occ, inline_commands=freeze({**occ.execution.inline_commands,
                payload.call_id: {**record, "status": "completed", "output": payload.output}}))
        elif isinstance(payload, OperatorCallStarted):
            require_occ({"running", "waiting"})
            if payload.call_id in sched.operator_calls:
                raise RuntimeTransitionError("OPERATOR_CALL_DUPLICATE", "Operator Call already exists.")
            put((*SCHED, "operator_calls", payload.call_id), OperatorCallState(
                payload.call_id, oid, payload.operator_id, payload.unit_index, "running", payload.input,
                started_at_us=occurred_at_us, queue_duration_ns=payload.queue_duration_ns,
                retry_attempt=payload.retry_attempt, deadline_at_us=payload.deadline_at_us), "add")
            workspace(occ, phase="executing")
        elif isinstance(payload, CommandRetryStarted):
            call = sched.operator_calls.get(payload.call_id)
            if call is None or call.status != 'failed' or call.retry_at_us is None or call.operator_id not in SYSTEM_COMMAND_IDS:
                raise RuntimeTransitionError('COMMAND_RETRY_INVALID', 'Command retry requires its scheduled failed Call.')
            put((*SCHED, 'operator_calls', call.id), replace(call, status='running', error=None,
                retry_attempt=call.retry_attempt + 1, retry_at_us=None, deadline_at_us=payload.deadline_at_us,
                completed_at_us=None, execution_duration_ns=None))
            occurrence_put(replace(sched.occurrences[call.occurrence_id], status='running', ready_at_us=occurred_at_us))
        elif isinstance(payload, (OperatorCallCompleted, OperatorCallFailed)):
            call = sched.operator_calls.get(payload.call_id)
            if call is None or call.status != "running":
                raise RuntimeTransitionError("OPERATOR_CALL_NOT_RUNNING", "Operator Call is not running.")
            success = isinstance(payload, OperatorCallCompleted)
            if not success and payload.retry_at_us != call.retry_at_us:
                put((*SCHED, "operator_calls", call.id, "retry_at_us"), payload.retry_at_us)
            for name, value in (
                ("status", "completed" if success else "failed"),
                ("output", payload.output if success else None),
                ("error", None if success else payload.error),
                ("completed_at_us", occurred_at_us),
                ("execution_duration_ns", payload.execution_duration_ns),
            ):
                if value is not getattr(call, name):
                    put((*SCHED, "operator_calls", call.id, name), value)
            if not success and (wait := sched.waits.get(call.id)) is not None and wait.status == 'waiting':
                put((*SCHED, 'waits', wait.id), replace(wait, status='cancelled'))
                occurrence_put(replace(sched.occurrences[call.occurrence_id], status='running', ready_at_us=occurred_at_us))
                put(('invocation', 'status'), 'running')
        elif isinstance(payload, (NodeCompleted, NodeFailed)):
            require_occ()
            success = isinstance(payload, NodeCompleted)
            terminal = replace(occ, status="completed" if success else "failed",
                output=payload.output if success else None, error=None if success else payload.error,
                completed_at_us=occurred_at_us, metrics=payload.metrics if success else occ.metrics,
                execution=_retire_execution(occ.execution), raw_input=None if success else occ.raw_input)
            occurrence_put(terminal)
            if success:
                patch = occ.execution.pending_context_patch
                updated_session, updated_inv = self._apply_context_patch(session, inv, occ, patch, state.sequence+1)
                for name, updated in (("session", updated_session), ("invocation", updated_inv)):
                    if getattr(patch, name):
                        put((name, "context"), updated.context)
                        put((name, "context_path_revisions"), updated.context_path_revisions)
            planned = graph(scheduler_delta, replace(sched, occurrences=ChainMap({oid: terminal}, sched.occurrences)))
            retention_index = _execution_index or ExecutionIndex(state)
            for key in retention_index.calls_by_occurrence.get(oid, ()):
                release_call_values(sched.operator_calls[key])
            for key in retention_index.waits_by_occurrence.get(oid, ()):
                wait = sched.waits[key]
                if wait.request is not None or wait.response is not None:
                    put((*SCHED, "waits", key), replace(wait, request=None, response=None))
            for key in released_outputs(sched, planned, scheduler_delta, oid,
                                        _output_node_ids, retention_index):
                put((*SCHED, "occurrences", key, "output"), None)
                put((*SCHED, "occurrences", key, "raw_input"), None)
            if _execution_index is not None:
                # The index describes pre-transition State. Match the original
                # exclusion of this occurrence; ready/revived work prevents waiting.
                if (not scheduler_delta.ready and not scheduler_delta.revived
                        and not _other_active(sched, oid, _execution_index)
                        and _execution_index.occurrence_counts.get('waiting', 0)
                            - (occ.status == 'waiting') > 0):
                    put(("invocation", "status"), "waiting")
            else:
                remaining = (
                    [item for key, item in sched.occurrences.items() if key != oid]
                    if not scheduler_delta.ready and not scheduler_delta.revived else ()
                )
                if not scheduler_delta.ready and not scheduler_delta.revived and remaining and all(
                    item.status in {"waiting", "completed", "failed", "skipped", "cancelled"} for item in remaining
                ) and any(item.status == "waiting" for item in remaining):
                    put(("invocation", "status"), "waiting")
        elif isinstance(payload, CommandAwakened):
            from .waits import awakening_delta
            delta = awakening_delta(self, state, payload, occurred_at_us, session_id, invocation_id, _execution_index)
            return StateDelta((*operations, *delta.operations))
        elif isinstance(payload, WaitRequested):
            require_occ({"running", "waiting"})
            if payload.wait_id in sched.waits:
                raise RuntimeTransitionError("WAIT_DUPLICATE", "Wait already exists.")
            if payload.wait_kind not in {'external', 'signal', 'timer', 'child', 'any'}:
                raise ValueError('Invalid Wait kind.')
            if payload.wait_kind != 'external':
                from .waits import validate_condition
                validate_condition(payload.wait_kind, payload.request)
            # Keep dispatch runnable until CommandsPending closes this pass.
            # A crash between Map registrations must still admit the rest.
            put((*SCHED, "waits", payload.wait_id), WaitState(payload.wait_id, oid, "waiting", payload.request,
                created_at_us=occurred_at_us, kind=payload.wait_kind, registered_sequence=state.sequence + 1,
                deadline_at_us=getattr(sched.operator_calls.get(payload.wait_id), 'deadline_at_us', None)), "add")
        elif isinstance(payload, WaitResumed):
            if payload.origin is not None:
                call_id = payload.origin['call_id']
                if call_id in inv.resume_receipts:
                    raise RuntimeTransitionError("RESUME_CALL_DUPLICATE", "Resume Call already applied.")
                put(("invocation", "resume_receipts", call_id),
                    freeze({**payload.origin, 'wait_id': payload.wait_id}), "add")
            wait = sched.waits.get(payload.wait_id)
            if wait is None or wait.status != "waiting" or wait.kind != "external":
                raise RuntimeTransitionError("WAIT_NOT_WAITING", "Wait is not waiting.")
            call = sched.operator_calls.get(wait.id)
            if call is not None and call.deadline_at_us is not None and occurred_at_us >= call.deadline_at_us:
                raise RuntimeTransitionError("WAIT_EXPIRED", "Wait has reached its deadline.")
            put((*SCHED, "waits", wait.id), replace(wait, status="resumed", response=payload.response, resumed_at_us=occurred_at_us))
            occurrence_put(replace(sched.occurrences[wait.occurrence_id], status="running", ready_at_us=occurred_at_us))

            put(("invocation", "status"), "running")
        elif isinstance(payload, RecoveryApplied):
            recovered = []
            for item in sched.operator_calls.values():
                if item.status == "running" and item.operator_id not in SYSTEM_COMMAND_IDS:
                    put((*SCHED, "operator_calls", item.id), replace(item, status="lost", completed_at_us=occurred_at_us))
            for item in sched.occurrences.values():
                if item.status == "running":
                    occurrence_put(replace(item, status="running", ready_at_us=occurred_at_us,
                        recovery_attempts=item.recovery_attempts+1))
                    recovered.append(item.id)
            if recovered:

                put(("invocation", "status"), "running")
        elif isinstance(payload, (InvocationCompleted, InvocationSettling, InvocationFailed, InvocationCancelled)):
            settling = isinstance(payload, InvocationSettling)
            outcome = (payload.outcome if settling else "completed" if isinstance(payload, InvocationCompleted)
                       else "failed" if isinstance(payload, InvocationFailed) else "cancelled")
            _require_status(inv, {"running", "settling"} if outcome == "completed"
                            else {"created", "running", "waiting", "settling"}, payload.kind)
            if inv.status == "settling" and inv.pending_outcome != outcome:
                if not (settling and inv.pending_outcome == "completed" and outcome in {"failed", "cancelled"}):
                    raise RuntimeTransitionError("OUTCOME_CONFLICT", "Settling outcome is already decided.")
            if inv.status == "settling" and inv.pending_outcome == outcome:
                actual = payload.output if outcome == "completed" else payload.error if outcome == "failed" else payload.reason
                expected = inv.output if outcome == "completed" else inv.error if outcome == "failed" else inv.cancel_reason
                if actual is not expected and actual != expected:
                    raise RuntimeTransitionError("OUTCOME_CONFLICT", "Settling result is already decided.")
            if not settling and any(u.phase not in {"terminal", "abandoned"} for p in inv.child_plans.values() for u in p.units):
                raise RuntimeTransitionError("CHILDREN_NOT_SETTLED", "Terminal status requires settled children.")
            put(("invocation", "status"), "settling" if settling else outcome)
            if settling or inv.pending_outcome is not None:
                put(("invocation", "pending_outcome"), outcome if settling else None)
            if inv.status != "settling" or inv.pending_outcome != outcome:
                put(("invocation", "output"), payload.output if outcome == "completed" else None)
                if outcome == "failed" or inv.error is not None:
                    put(("invocation", "error"), payload.error if outcome == "failed" else None)
                if outcome == "cancelled" or inv.cancel_reason is not None:
                    put(("invocation", "cancel_reason"), payload.reason if outcome == "cancelled" else None)
            if not settling:
                put(("invocation", "completed_at_us"), occurred_at_us)
                if inv.signals['messages']:
                    put(('invocation', 'signals', 'messages'), freeze({}))
                    put(('invocation', 'signals', 'bytes'), 0)
            if outcome != "completed" and inv.status != "settling":
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
            plan = ChildInvocationPlan(payload.creation_id, payload.parent_occurrence_id, payload.entry_node_id,
                payload.workflow_id, payload.workflow_revision_id,
                child_units(tuple(ChildUnitState(u.unit_index, u.child_session_id, u.child_invocation_id, u.input) for u in payload.units)))
            put(("invocation", "child_plans", plan.creation_id), plan, "add")
        elif isinstance(payload, ChildInvocationPhaseChanged):
            plan = inv.child_plans.get(payload.creation_id)
            if plan is None or payload.unit_index >= len(plan.units):
                raise RuntimeTransitionError("CHILD_PLAN_MISSING", "Child plan or unit is missing.")
            unit = plan.units[payload.unit_index]
            if {"planned":"opened", "opened":"accepted", "accepted":"terminal"}.get(unit.phase) != payload.phase and not (inv.stopping and (payload.phase == "terminal" or (payload.phase == "abandoned" and unit.phase == "planned"))):
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
        else:
            raise RuntimeTransitionError("EVENT_TYPE_UNSUPPORTED", "Unsupported semantic boundary.")
        if isinstance(payload, (InvocationCompleted, InvocationFailed, InvocationCancelled)):
            put(("invocation", "control_state"), "active")
        if inv.status != "settling" and isinstance(payload, (InvocationCompleted, InvocationSettling, InvocationFailed, InvocationCancelled)):
            # Terminal Invocations retain their public result and business Context,
            # not execution payload history. Apply after lifecycle status updates.
            for item in sched.occurrences.values():
                if item.output is not None:
                    put((*SCHED, "occurrences", item.id, "output"), None)
                retired = _retire_execution(item.execution)
                if item.execution != retired:
                    put((*SCHED, "occurrences", item.id, "execution"), retired)
            for item in sched.operator_calls.values():
                if item.input is not None:
                    put((*SCHED, "operator_calls", item.id, "input"), None)
                if item.output is not None:
                    put((*SCHED, "operator_calls", item.id, "output"), None)
            for item in sched.waits.values():
                if item.request is not None:
                    put((*SCHED, "waits", item.id, "request"), None)
                if item.response is not None:
                    put((*SCHED, "waits", item.id, "response"), None)
        if isinstance(payload, NodeCompleted):
            for key, plan in inv.child_plans.items():
                if plan.parent_occurrence_id == payload.occurrence_id and any(not u.input_released for u in plan.units):
                    put(("invocation", "child_plans", key), release_child_inputs({key: plan})[key])
        elif isinstance(payload, (InvocationCompleted, InvocationSettling, InvocationFailed, InvocationCancelled)):
            if any(not u.input_released for p in inv.child_plans.values() for u in p.units):
                put(("invocation", "child_plans"), release_child_inputs(inv.child_plans))
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
            if plan.id != scheduler.occurrence_ids.get(occurrence_key(plan.node_id, plan.scope), occurrence_key(plan.node_id, plan.scope)):
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
    cache = _previews.get()
    if cache is not None and cache.closed:
        cache = None
    if cache is not None:
        for old_context, old_revisions, old_operations, start, forward, result in cache:
            if (old_context is context and old_revisions is revisions and old_operations is operations
                    and start == started_sequence and forward == (committed_sequence > started_sequence)):
                updated = MapEdit(revisions)
                for operation in operations:
                    updated[operation.path] = committed_sequence
                return result, updated.finish()
    current = _ContextEdit(context)
    updated_revisions = MapEdit(revisions)
    index = revision_index(revisions)
    recent = ([] if index is not None else
              [path for path, revision in revisions.items() if revision > started_sequence])
    seen: set[tuple[str, ...]] = set()
    for operation in operations:
        if operation.path in seen:
            raise RuntimeTransitionError(
                "CONTEXT_PATCH_DUPLICATE_PATH",
                f"Context Patch changes path {'.'.join(operation.path)!r} twice.",
            )
        seen.add(operation.path)
        if (index is not None and index.conflicts(operation.path, started_sequence)) or any(
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
    result = current.finish()
    if cache is not None:
        cache.append((context, revisions, operations, started_sequence,
                      committed_sequence > started_sequence, result))
    return result, updated_revisions.finish()


def _paths_overlap(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    size = min(len(left), len(right))
    return left[:size] == right[:size]


def _other_active(scheduler, occurrence_id, index):
    """Query committed counts minus the occurrence changed by this transition."""
    if index is None:
        return any(item.id != occurrence_id and item.status in {'ready', 'running'}
                   for item in scheduler.occurrences.values())
    current = scheduler.occurrences[occurrence_id]
    active = index.occurrence_counts.get('ready', 0) + index.occurrence_counts.get('running', 0)
    return active - (current.status in {'ready', 'running'}) > 0
